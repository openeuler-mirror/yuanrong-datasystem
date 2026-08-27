#!/usr/bin/env python3
"""Network / scheduling latency localization analysis for collected k8s node/pod logs.

Implements the 6-step manual analysis workflow as an automated tool:
  1. Scan client INFO logs for [BRPC_RPC_FRAMEWORK_SLOW] lines whose
     network_residual_us exceeds a threshold (default 1000us).
  2. Correlate the problem trace_id across client / worker INFO logs to locate
     the 4 RPC anchors (ClientSend / ServerRecv / ServerSend / ClientRecv) and
     compute the 3 macro segments.
  3. Correlate with per-node bpftrace kernel logs (dscollect_log/bpf-*.log),
     filtered by time window + client/server pod IPs, identify the connection
     4-tuple and rebuild the kernel-level timeline.
  4. Check per-node scheduling latency warnings (latency_warn_log/*) inside the
     problem window; best-effort track the wakeup chain from bpf sched events.
  5. Assemble the full timeline with per-segment durations.
  6. Classify the bottleneck segment and emit a localization conclusion.

Output: a self-contained interactive HTML report.

Usage:
    python3 network_latency_analysis.py <log_root> \
        [--residual-threshold 1000] [--top N] [--trace ID ...] \
        [--window-pad-ms 2] [--sched-pad-ms 10] [-o report.html]
"""

import argparse
import bisect
import html
import heapq
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# ── Constants / thresholds ────────────────────────────────────────────────────

DEFAULT_RESIDUAL_THRESHOLD_US = 1000
DEFAULT_WINDOW_PAD_MS = 2
DEFAULT_SCHED_PAD_MS = 10
DEFAULT_MAX_SCHED_EVENTS = 5000      # 每 (trace, 侧) 保留的调度类事件上限
DEFAULT_MAX_WINDOW_NET_EVENTS = 4000  # 每 (trace, 侧) 窗口全景保留的连接类事件上限
EVENTS_TABLE_MAX_ROWS = 500          # HTML 事件明细表行数上限
INDEX_MAX_TRACES = 2000              # HTML 报告渲染的 trace 数上限
SEEK_SLACK_US = 2 * 1000 * 1000      # bpf seek 模式前后余量(2s)，容忍轻度乱序
SCAN_CHUNK = 16 * 1024 * 1024        # 字节块扫描的块大小
MAX_LINE_BYTES = 8 * 1024 * 1024     # 超长行保护上限

# Per-segment abnormality thresholds (microseconds). None = no fixed threshold.
SEGMENT_DEFS = [
    # (key, start milestone, end milestone, threshold_us, category, description)
    ("client_user_to_kernel", "ClientSend", "ClientTcpSendIn", 100,
     "client_user_to_kernel_delay", "client 用户态发起 → client 内核 tcp 发送入口"),
    ("wire_c2s", "ClientTcpSendIn", "ServerTcpRecvFirst", 200,
     "network_c2s_transmission", "client 内核发送 → server 内核收包（线路传输+软中断）"),
    ("server_kernel_to_user", "ServerTcpRecvLast", "ServerRecv", 100,
     "server_kernel_to_user_delay", "server 内核收包完成 → server 用户态 ServerRecv（唤醒/调度）"),
    ("server_processing", "ServerRecv", "ServerSend", None,
     "server_processing_slow", "server 用户态处理（ServerRecv → ServerSend）"),
    ("server_user_to_kernel", "ServerSend", "ServerTcpSendIn", 100,
     "server_user_to_kernel_delay", "server 用户态 ServerSend → server 内核 tcp 发送入口"),
    ("wire_s2c", "ServerTcpSendIn", "ClientTcpRecvFirst", 200,
     "network_s2c_transmission", "server 内核发送 → client 内核收包（线路传输+软中断）"),
    ("client_kernel_to_user", "ClientTcpRecvLast", "ClientRecv", 1000,
     "client_kernel_to_user_delay", "client 内核收包完成 → client 用户态 ClientRecv（唤醒/调度）"),
]

# Macro (3-segment) thresholds used only when kernel events are missing.
MACRO_THRESHOLDS_US = {"cs_sr": 500, "sr_ss": 500, "ss_cr": 500}

# 全路径时间线点位序（业务 → 协议栈 → 网卡 → 线路 → 网卡 → 协议栈 → 业务）。
# OS/网卡主干仅选取 net.bt 的 5 个探针点位：tcp_sendmsg / tcp_recvmsg /
# net_dev_start_xmit / net_dev_xmit / netif_receive_skb；
# 其余点位（tcp_queue_rcv 入队、sock 唤醒、sched 调度、重传等）在事件明细中展开。
POINT_ORDER = ["ClientSend", "ClientTcpSendIn", "ClientDevStartXmit", "ClientNetDevXmit",
               "ServerNetifRx", "ServerTcpRecvFirst",
               "ServerTcpRecvLast", "ServerRecv", "ServerSend", "ServerTcpSendIn",
               "ServerDevStartXmit", "ServerNetDevXmit", "ClientNetifRx",
               "ClientTcpRecvFirst", "ClientTcpRecvLast", "ClientRecv"]
POINT_LAYERS = {
    "ClientSend": "业务", "ServerRecv": "业务", "ServerSend": "业务", "ClientRecv": "业务",
    "ClientTcpSendIn": "协议栈", "ServerTcpRecvFirst": "协议栈",
    "ServerTcpRecvLast": "协议栈", "ServerTcpSendIn": "协议栈",
    "ClientTcpRecvFirst": "协议栈", "ClientTcpRecvLast": "协议栈",
    "ClientDevStartXmit": "网卡", "ClientNetDevXmit": "网卡", "ServerNetifRx": "网卡",
    "ServerDevStartXmit": "网卡", "ServerNetDevXmit": "网卡", "ClientNetifRx": "网卡",
}

CATEGORY_LABELS = {
    "client_user_to_kernel_delay": "client 用户态发送路径慢",
    "network_c2s_transmission": "client→server 网络传输慢",
    "server_kernel_to_user_delay": "server 收包后唤醒/调度慢",
    "coroutine_schedule_delay": "server 协程调度排队慢（bthread 等待 worker 线程执行）",
    "server_processing_slow": "server 业务处理慢",
    "server_user_to_kernel_delay": "server 发送路径慢",
    "network_s2c_transmission": "server→client 网络传输慢",
    "client_kernel_to_user_delay": "client 收包后唤醒/用户态取包慢",
    "client_to_server_path": "client→server 方向整体异常（含网络/server 内核）",
    "server_to_client_path": "server→client 方向整体异常（含网络/client 内核）",
    "network_c2s_phys_wire_delay": "client→server 物理网卡间传输慢（网卡处理/物理线路，两侧节点内已排除）",
    "network_s2c_phys_wire_delay": "server→client 物理网卡间传输慢（网卡处理/物理线路，两侧节点内已排除）",
    "unknown": "无法定界（证据不足）",
}

# 物理网卡间线路定界（seq 关联）判定阈值：
# 线路耗时超下限 且 占线路段（TcpSendIn→对端 TcpRecvFirst）比例达标 → 判主导
PHYS_WIRE_MIN_US = 1000     # 物理网卡间线路耗时下限(us)
PHYS_WIRE_SHARE_PCT = 70.0  # 占线路段比例阈值(%)

# 物理网卡间传输慢（seq 关联定界）排查建议（c2s/s2c 共用）
_PHYS_WIRE_SUGGESTIONS = [
    "排查物理链路：交换机转发时延/拥塞、光模块/线缆质量、端口错包与丢包计数",
    "检查两侧物理网卡（dev 见网卡链路定界明细）收发处理：ethtool -S 丢包/drop、"
    "中断合并、队列打满、PFC 流控丢包",
    "两侧节点内 veth/协议栈耗时已排除（见网卡链路定界明细）；"
    "跨节点相减注意核对两节点时钟偏差",
]

CATEGORY_SUGGESTIONS = {
    "client_user_to_kernel_delay": [
        "检查 client 进程发送线程 CPU 占用 / brpc 发送队列是否积压",
        "查看 client 节点 bpf 日志中该时段软中断、锁竞争痕迹",
    ],
    "network_c2s_transmission": [
        "检查链路质量（丢包/重传/网卡统计），对比两节点时钟后核实传输耗时",
        "查看 server 节点软中断（recv que 前段）是否延迟",
    ],
    "server_kernel_to_user_delay": [
        "重点核查 server 节点调度：结合 latency_warn 与 sched_waking/wakeup 链",
        "检查 server 进程工作线程是否被抢占或阻塞",
    ],
    "coroutine_schedule_delay": [
        "检查 bRPC worker 线程数与 bthread 并发配置是否充足（worker 线程被前序协程长任务占用）",
        "在 worker 日志中按 bid 检索同窗口其他协程任务，确认是否存在长任务排队",
        "确认 -usercode_in_pthread 运行模式：用户代码阻塞会占住 worker 线程放大排队",
    ],
    "server_processing_slow": [
        "结合 server_req_queue_us / server_exec_us 区分排队与执行",
        "在 worker 日志中按 trace_id 查看业务处理各阶段日志",
    ],
    "server_user_to_kernel_delay": [
        "检查 server 发送线程调度与 brpc 写出路径",
    ],
    "network_s2c_transmission": [
        "检查 server→client 方向链路质量与时钟偏差",
        "查看 client 节点软中断负载",
    ],
    "network_c2s_phys_wire_delay": _PHYS_WIRE_SUGGESTIONS,
    "network_s2c_phys_wire_delay": _PHYS_WIRE_SUGGESTIONS,
    "client_kernel_to_user_delay": [
        "重点核查 client 节点调度：结合 latency_warn 与 wakeup 链证据",
        "检查 client 进程 epoll/brpc 工作线程是否被抢占、CPU 是否被抢占或迁移",
    ],
    "client_to_server_path": [
        "缺少内核日志佐证，建议补充该时段 bpf 采集后重新分析",
    ],
    "server_to_client_path": [
        "缺少内核日志佐证，建议补充该时段 bpf 采集后重新分析",
    ],
    "unknown": [
        "锚点日志不完整，建议检查日志采集覆盖范围",
    ],
}

CONF_HIGH_MARGIN = 5.0  # duration/threshold ratio above which confidence is high

# ── Log line regexes ──────────────────────────────────────────────────────────

# 2026-08-21T21:31:21.060757 | I | file.cpp:12 | 1.2.3.4 | 100:200 | trace | user | msg
INFO_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)"
    r"\s*\|\s*(?P<level>\w+)\s*\|\s*(?P<loc>\S+)\s*\|\s*(?P<host>\S+)"
    r"\s*\|\s*(?P<pid>\d+):(?P<tid>\d+)\s*\|\s*(?P<trace>[^|]*)\|(?P<user>[^|]*)\|(?P<msg>.*)$"
)

SLOW_MARK = "[BRPC_RPC_FRAMEWORK_SLOW]"
KV_RE = re.compile(r"(\w+)=([^\s|]+)")

# 锚点行尾可选扩展（新格式）：`... tid N cpu N bid N`（bid 为 bRPC bthread 协程号）
_ANCHOR_TAIL = r"(?:\s+cpu\s+(?P<cpu>\d+))?(?:\s+bid\s+(?P<bid>\d+))?"
CLIENT_SEND_RE = re.compile(r"ClientSend ts (\d+) tid (\d+)" + _ANCHOR_TAIL)
CLIENT_RECV_RE = re.compile(r"ClientRecv ts (\d+) tid (\d+)" + _ANCHOR_TAIL)
SERVER_RECV_RE = re.compile(r"ServerRecv ts (\d+) tid (\d+)" + _ANCHOR_TAIL)
SERVER_SEND_RE = re.compile(r"ServerSend ts (\d+) tid (\d+)" + _ANCHOR_TAIL)

# bpf log: 21:31:21:060777 <event...>
BPF_TS_RE = re.compile(r"^(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2}):(?P<us>\d{6})\s+(?P<rest>.*)$")

BPF_TCP_RE = re.compile(
    r"^tcp\s+(?P<dir>recv|send)\s+(?P<phase>in|out|que)\s+tid\s+(?P<tid>\d+)\s+"
    r"cpu\s+(?P<cpu>\d+)\s+size\s+(?P<size>-?\d+)(?P<tail>.*)$"
)
BPF_ADDR_RE = re.compile(
    r"(?P<local_ip>\d+\.\d+\.\d+\.\d+):(?P<local_port>\d+)\s*(?P<arrow><-|->)\s*"
    r"(?P<peer_ip>\d+\.\d+\.\d+\.\d+):(?P<peer_port>\d+)"
)
BPF_SOCKReadable_RE = re.compile(
    r"^sock_def_readable,\s*tcp\s+wakeup\s+(?P<n>\d+)\s+tid\s+(?P<tid>\d+)\s+cpu\s+(?P<cpu>\d+)(?P<tail>.*)$"
)
BPF_SCHED_WAKING_RE = re.compile(
    r"^sched_waking\s+tid\s+(?P<tid>\d+)\s+cpu\s+(?P<cpu>\d+)\s+comm\s+(?P<comm>\S+)\s+"
    r"pid\s+(?P<pid>\d+)\s+target_cpu\s+(?P<target_cpu>-?\d+)"
)
BPF_SCHED_WAKEUP_RE = re.compile(
    r"^sched_wakeup\s+tid\s+\d+\s+cur_comm:(?P<cur_comm>\S+)\s+cpu\s+(?P<cpu>\d+)\s+"
    r"comm\s+(?P<comm>\S+)\s+pid\s+(?P<pid>\d+),\s*target_cpu:(?P<target_cpu>-?\d+)"
)
BPF_SCHED_SWITCH_RE = re.compile(
    r"^sched_switch\s+tid\s+\d+\s+cpu\s+(?P<cpu>\d+)\s+prev_comm=(?P<prev_comm>\S+)\s+"
    r"prev_pid=(?P<prev_pid>\d+)\s+next_comm=(?P<next_comm>\S+)\s+next_pid=(?P<next_pid>\d+)"
)
BPF_TCPWAKEUP_OUT_RE = re.compile(r"^tcpwakeup out tid (?P<tid>\d+) cpu (?P<cpu>\d+)")

# 网卡层观测点位（net.bt）：src→dst 方向四元组 + seq/len/dev（net_dev_xmit 另有 rc）
BPF_NIC_RE = re.compile(
    r"^(?P<ev>dev_start_xmit|net_dev_xmit|netif_receive_skb):\s+"
    r"sip:(?P<sip>\d+\.\d+\.\d+\.\d+),\s*sport:(?P<sport>\d+)\s*->\s*"
    r"dip:(?P<dip>\d+\.\d+\.\d+\.\d+),\s*dport:(?P<dport>\d+),\s*"
    r"seq:(?P<seq>\d+),\s*len:(?P<len>-?\d+),\s*dev:(?P<dev>[^,\s]+)"
    r"(?P<tail>.*)$")
# TCP 重传：local->peer 格式（与 tcp 事件同构）+ 发送侧序列信息
BPF_RETRANS_RE = re.compile(
    r"^__tcp_retransmit_skb\s+tid\s+(?P<tid>\d+)\s+cpu\s+(?P<cpu>\d+)\s+"
    r"size\s+(?P<size>-?\d+)\s+tx_seq:\s*(?P<tx_seq>\d+),\s*snd_una:(?P<snd_una>\d+),\s*"
    r"snd_next:\s*(?P<snd_next>\d+)\s+tcb:seq:\s*(?P<seq>\d+),(?P<tail>.*)$")

NIC_KINDS = {"dev_start_xmit": "nic_dev_xmit_start",
             "net_dev_xmit": "nic_dev_xmit",
             "netif_receive_skb": "nic_rx_skb"}

# latency_warn: [88019.268007][2026-08-21 21:31:05:123144] !!! resched_latency_warn Triggered !!!
LW_TRIG_RE = re.compile(
    r"^\[(?P<uptime>[\d.]+)\]\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}:\d{6})\]"
    r"\s+!!!\s*resched_latency_warn Triggered\s*!!!"
)
LW_INFO_RE = re.compile(
    r"Current CPU:\s*(?P<cpu>\d+)\s*\|\s*Task Comm:\s*(?P<comm>\S+)\s*\|"
    r"\s*PID:\s*(?P<pid>\d+),\s*latency:\s*(?P<lat>\d+)"
)

BPF_FILE_RE = re.compile(r"^bpf-(?P<name>.+)-(?P<ip>\d+\.\d+\.\d+\.\d+)\.log$")
WARN_FILE_RE = re.compile(r"^(?P<name>.+)_(?P<ip>\d+\.\d+\.\d+\.\d+)$")
# 辅助日志（均在 dscollect_log/ 下，可选输入，缺失自动降级）：
#   irqoff_latency_<nodeIp>.log  关中断超过 1ms 的记录（块 + 调用栈）
#   nic-<nodeIp>.log             ethtool 属性 + sar 每秒网卡利用率采样
#   <podName>-brpc*.log          bRPC bthread 协程创建/首次调度日志（glog 格式）
IRQOFF_FILE_RE = re.compile(r"^irqoff_latency_(?P<ip>\d+\.\d+\.\d+\.\d+)\.log$")
NIC_FILE_RE = re.compile(r"^nic-(?P<ip>\d+\.\d+\.\d+\.\d+)\.log$")
BRPC_FILE_RE = re.compile(r"^(?P<pod>.+?)-brpc.*\.log$")

# irqoff 块结构：hardirq:/softirq: 切换中断类型，cpu: N 切换 cpu，
# COMMAND 行开一条新记录（其后调用栈行附加到该记录，直到下一条切换/记录行）
IRQOFF_IRQ_RE = re.compile(r"^(hardirq|softirq):\s*$")
IRQOFF_CPU_RE = re.compile(r"^cpu:\s*(?P<cpu>\d+)\s*$")
IRQOFF_HEAD_RE = re.compile(
    r"^\s*COMMAND:\s*(?P<comm>\S+)\s+PID:\s*(?P<pid>\d+)\s+"
    r"LATENCY:\s*(?P<lat>\d+(?:\.\d+)?)\s*(?P<unit>ms|us|s)\s+"
    r"TIMESTAMP:\s*(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)")
# irqoff 全周期统计分桶阈值（us）：bucket[X] = latency ≥ X 的记录数
IRQOFF_BUCKETS_US = [1000, 2000, 5000, 10000, 20000, 50000, 100000, 500000]
IRQOFF_SERIES_MAX = 2000        # 时长曲线（SVG 散点）点数上限

# nic-<ip>.log：ethtool 属性行 + sar 数据行（AM/PM 12 小时制）
NIC_SETTINGS_RE = re.compile(r"^Settings for (?P<dev>\S+):")
NIC_PROP_RE = re.compile(r"^\s*(?P<key>Speed|Duplex|Link detected):\s*(?P<val>.*?)\s*$")
SAR_DATA_RE = re.compile(
    r"^\s*(?P<time>\d{2}:\d{2}:\d{2})\s+(?:(?P<ampm>AM|PM)\s+)?"
    r"(?P<iface>\S+)\s+(?P<rxpck>[\d.]+)\s+(?P<txpck>[\d.]+)\s+"
    r"(?P<rxkB>[\d.]+)\s+(?P<txkB>[\d.]+)\s+(?P<rxcmp>[\d.]+)\s+"
    r"(?P<txcmp>[\d.]+)\s+(?P<rxmcst>[\d.]+)\s+(?P<ifutil>[\d.]+)\s*$")
# %ifutil 判定阈值：≥高阈值提示网卡利用率高；<低阈值输出排除性证据
NIC_HIGH_IFUTIL_PCT = 50.0
NIC_LOW_IFUTIL_PCT = 10.0

# bRPC bthread 日志（glog：I0824 22:32:23.661136  tid  bid  file:line fn] msg）
BTHREAD_CREATED_RE = re.compile(
    r"bthread created:\s*creator_tid=(?P<ctid>\d+)\s+bthread_id=(?P<bid>\d+)\s+"
    r"creation_time_ns=(?P<ctns>\d+)\s+creation_mode=(?P<mode>\w+)\s+"
    r"target_local_pending_tasks=(?P<lp>\d+)\s+target_remote_pending_tasks=(?P<rp>\d+)\s+"
    r"target_pending_tasks=(?P<tp>\d+)")
BTHREAD_SCHED_RE = re.compile(
    r"bthread first scheduled:\s*worker_tid=(?P<wtid>\d+)\s+bthread_id=(?P<bid>\d+)\s+"
    r"fn=(?P<fn>\S+)\s+arg=(?P<arg>\S+)\s+creation_time_ns=(?P<ctns>\d+)\s+"
    r"first_run_time_ns=(?P<frns>\d+)\s+pending_time_us=(?P<ptu>\d+)")

# 辅助日志与 trace 的关联窗口：本侧"收包→用户态取包"段前后余量(ms)
AUX_RECV_PAD_MS = 2


# ── Small helpers ─────────────────────────────────────────────────────────────

def parse_wall_ts(s):
    return datetime.strptime(s[:26], "%Y-%m-%dT%H:%M:%S.%f")


def fmt_dt(dt):
    return dt.strftime("%H:%M:%S.%f") if dt else "-"


def fmt_us(us):
    if us is None:
        return "-"
    if us >= 1000:
        return "%.3f ms" % (us / 1000.0)
    return "%.0f us" % us


def sec_of_day(dt):
    return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6


def _stage(msg):
    """阶段摘要（始终输出到 stderr，stdout 保持干净）。"""
    sys.stderr.write("[analyze] %s\n" % msg)


# ── Fast byte-chunk marker scanning (large-log foundation) ────────────────────
#
# 对 GB 级日志逐行做 Python 层子串/正则判断不可行（100GB ≈ 数亿行）。这里按
# 16MB 二进制块读取，用 bytes.find（C 速度、内存带宽量级）定位稀疏标记，只有
# 命中行才进入 Python 层。块间只保留"最后一个换行之后的不完整行"用于拼接，
# 因此任何完整行恰好只会在其完整的那个块被产出一次。

def _marker_lines_in_block(block, markers, path):
    """产出 block（以 \n 结尾的完整行序列，或文件末尾无换行的 tail）中的命中行。"""
    line_starts = set()
    for mk in markers:
        pos = block.find(mk)
        while pos >= 0:
            line_starts.add(block.rfind(b"\n", 0, pos) + 1)
            pos = block.find(mk, pos + 1)
    for ls in sorted(line_starts):
        le = block.find(b"\n", ls)
        if le < 0:
            le = len(block)
        yield path, block[ls:le].decode("utf-8", errors="replace")


def iter_marker_lines(paths, markers, verbose=False):
    """逐文件按块扫描，yield (path, line)：line 为包含任一 marker 的完整行。

    输出保持文件内原始顺序；Python 层开销正比于命中行数（稀疏）而非总行数。
    """
    for path in paths:
        t0 = time.monotonic()
        hits = 0
        nbytes = 0
        with open(path, "rb") as fh:
            tail = b""
            while True:
                chunk = fh.read(SCAN_CHUNK)
                if not chunk:
                    break
                data = tail + chunk if tail else chunk
                last_nl = data.rfind(b"\n")
                if last_nl < 0:
                    # 当前行尚未结束；超长行截断保护
                    if len(data) > MAX_LINE_BYTES:
                        sys.stderr.write("warn: %s 存在超过 %d 字节的行，已截断\n"
                                         % (path, MAX_LINE_BYTES))
                        data = data[-MAX_LINE_BYTES:]
                    tail = data
                    continue
                body, tail = data[:last_nl + 1], data[last_nl + 1:]
                for _ in _marker_lines_in_block(body, markers, path):
                    hits += 1
                    yield _
            if tail:
                for _ in _marker_lines_in_block(tail, markers, path):
                    hits += 1
                    yield _
            nbytes = fh.tell()
        if verbose:
            dt = time.monotonic() - t0
            rate = nbytes / dt / 1e6 if dt > 0 else 0.0
            sys.stderr.write("  scanned %s (%.1f MB) hits=%d %.2fs %.0fMB/s\n"
                             % (path, nbytes / 1e6, hits, dt, rate))


# ── Parallel file-scan infrastructure ─────────────────────────────────────────
#
# 大日志（TB 级）下单进程顺序扫描受限于单流 IO + 单核 bytes.find。日志天然按
# 文件分布（pod/node），故按文件分派到进程池（fork，Linux 默认）；workers<=1
# 或 Pool 不可用时回退串行，行为与 iter_marker_lines 完全一致。

def _scan_file_job(job):
    """进程池 worker：扫描单个日志文件（参数元组可 pickle）。

    job = (path_str, markers, mode, trace_ids, threshold_us, only_traces,
           verbose, source)
    mode "slow"：返回 ("slow", path_str, None, [SlowRecord...], None)
    mode "anchor_info"：返回 ("anchor_info", path_str, source,
                              {trace: [锚点 info dict...]}, [(trace, line)...])
    """
    (path_str, markers, mode, trace_ids, threshold_us, only_traces,
     verbose, source) = job
    path = Path(path_str)
    if mode == "slow":
        recs = []
        for p, line in iter_marker_lines([path], [SLOW_MARK.encode("ascii")],
                                         verbose=verbose):
            info = parse_info_line(line)
            if not info:
                continue
            kv = parse_slow_kv(info["msg"])
            trace_id = kv.get("trace_id") or info["trace"]
            if not trace_id:
                continue
            if only_traces and not any(t in trace_id for t in only_traces):
                continue
            try:
                residual = int(kv.get("network_residual_us", "0"))
            except ValueError:
                continue
            if residual <= threshold_us:
                continue
            recs.append(SlowRecord(trace_id, info["ts"], kv, str(p),
                                   p.parent.name))
        return ("slow", path_str, None, recs, None)
    # anchor_info 模式：锚点行 + 问题 trace 的全部 INFO 行一并收集
    wanted = set(trace_ids or ())
    anchors = {}
    info_pairs = []
    for p, line in iter_marker_lines([path], markers, verbose=verbose):
        info = parse_info_line(line)
        if not info:
            continue
        t = info["trace"]
        if t not in wanted:
            continue
        msg = info["msg"]
        if (CLIENT_SEND_RE.search(msg) or CLIENT_RECV_RE.search(msg)
                or SERVER_RECV_RE.search(msg) or SERVER_SEND_RE.search(msg)):
            info["_path"] = str(p)
            info["_pod_dir"] = p.parent.name
            anchors.setdefault(t, []).append(info)
        info_pairs.append((t, line.rstrip("\n")))
    return ("anchor_info", path_str, source, anchors, info_pairs)


def run_parallel(jobs, workers, func=None):
    """按文件并行执行 job 列表（保序返回结果）；不可用时回退串行。"""
    func = func or _scan_file_job
    if workers and workers > 1 and len(jobs) > 1:
        try:
            import multiprocessing
            ctx = multiprocessing.get_context("fork")
            with ctx.Pool(min(workers, len(jobs))) as pool:
                return pool.map(func, jobs)
        except Exception as e:  # 非 Linux / 受限环境
            sys.stderr.write("warn: 多进程不可用(%s)，回退串行\n" % e)
    return [func(j) for j in jobs]


def _bpf_scan_job(job):
    """进程池 worker：单节点 bpf 文件窗口扫描。

    job = (path_str, windows, full_scan, max_sched_events, verbose, slack_us)
    返回 (results, truncated, window_results, window_truncated, diag)
    （均可 pickle）。
    """
    path_str, windows, full_scan, max_sched_events, verbose, slack_us = job
    scanner = BpfScanner(path_str, windows, full_scan=full_scan,
                         max_sched_events=max_sched_events, verbose=verbose,
                         slack_us=slack_us)
    res, trunc = scanner.scan()
    return res, trunc, scanner.window_results, scanner.window_truncated, scanner.diag


# ── Parsers (unit-testable pure functions) ───────────────────────────────────

def parse_info_line(line):
    """Parse a pipe-separated INFO log line. Returns dict or None."""
    m = INFO_LINE_RE.match(line.rstrip("\n"))
    if not m:
        return None
    return {
        "ts": parse_wall_ts(m.group("ts")),
        "level": m.group("level"),
        "loc": m.group("loc"),
        "host": m.group("host"),
        "pid": m.group("pid"),
        "tid": m.group("tid"),
        "trace": m.group("trace").strip(),
        "user": m.group("user").strip(),
        "msg": m.group("msg").strip(),
        "raw": line.rstrip("\n"),
    }


def parse_slow_kv(msg):
    """Extract key=value pairs from a BRPC_RPC_FRAMEWORK_SLOW message."""
    return dict(KV_RE.findall(msg))


def match_node(pod_dir_name, node_names):
    """Match a pod directory name to a node name by longest substring."""
    best = None
    for name in node_names:
        if name and name in pod_dir_name:
            if best is None or len(name) > len(best):
                best = name
    return best


def parse_bpf_line(line, day):
    """Parse one bpftrace log line into an event dict (or None)."""
    m = BPF_TS_RE.match(line.rstrip("\n"))
    if not m:
        return None
    h, mi, s, us, rest = int(m.group("h")), int(m.group("m")), int(m.group("s")), int(m.group("us")), m.group("rest")
    if isinstance(day, datetime):
        day = day.date()
    dt = datetime(day.year, day.month, day.day, h, mi, s, us)
    ev = {"ts": dt, "kind": None, "raw": line.rstrip("\n")}

    tm = BPF_TCP_RE.match(rest)
    if tm:
        ev["kind"] = "tcp_%s_%s" % (tm.group("dir"), tm.group("phase"))
        ev["tid"] = int(tm.group("tid"))
        ev["cpu"] = int(tm.group("cpu"))
        ev["size"] = int(tm.group("size"))
        tail = tm.group("tail")
        am = BPF_ADDR_RE.search(tail)
        if am:
            ev["local_ip"] = am.group("local_ip")
            ev["local_port"] = int(am.group("local_port"))
            ev["peer_ip"] = am.group("peer_ip")
            ev["peer_port"] = int(am.group("peer_port"))
            ev["dir_arrow"] = am.group("arrow")
        sm = re.search(r"copied_seq:(\d+)", tail)
        if sm:
            ev["copied_seq"] = int(sm.group(1))
        rn = re.search(r"rcv_nxt:(\d+)", tail)
        if rn:
            ev["rcv_nxt"] = int(rn.group(1))
        return ev

    rm = BPF_SOCKReadable_RE.match(rest)
    if rm:
        ev["kind"] = "sock_readable"
        ev["tid"] = int(rm.group("tid"))
        ev["cpu"] = int(rm.group("cpu"))
        ev["wakeup_n"] = int(rm.group("n"))
        am = BPF_ADDR_RE.search(rm.group("tail"))
        if am:
            ev["local_ip"] = am.group("local_ip")
            ev["local_port"] = int(am.group("local_port"))
            ev["peer_ip"] = am.group("peer_ip")
            ev["peer_port"] = int(am.group("peer_port"))
            ev["dir_arrow"] = am.group("arrow")
        return ev

    nm = BPF_NIC_RE.match(rest)
    if nm:
        ev["kind"] = NIC_KINDS[nm.group("ev")]
        ev["src_ip"] = nm.group("sip")
        ev["src_port"] = int(nm.group("sport"))
        ev["dst_ip"] = nm.group("dip")
        ev["dst_port"] = int(nm.group("dport"))
        ev["seq"] = int(nm.group("seq"))
        ev["len"] = int(nm.group("len"))
        ev["dev"] = nm.group("dev")
        rcm = re.search(r"rc:(-?\d+)", nm.group("tail"))
        if rcm:
            ev["rc"] = int(rcm.group(1))
        return ev

    xm = BPF_RETRANS_RE.match(rest)
    if xm:
        ev["kind"] = "tcp_retransmit"
        ev["tid"] = int(xm.group("tid"))
        ev["cpu"] = int(xm.group("cpu"))
        ev["size"] = int(xm.group("size"))
        ev["tx_seq"] = int(xm.group("tx_seq"))
        ev["snd_una"] = int(xm.group("snd_una"))
        ev["snd_nxt"] = int(xm.group("snd_next"))
        ev["seq"] = int(xm.group("seq"))
        am = BPF_ADDR_RE.search(xm.group("tail"))
        if am:
            ev["local_ip"] = am.group("local_ip")
            ev["local_port"] = int(am.group("local_port"))
            ev["peer_ip"] = am.group("peer_ip")
            ev["peer_port"] = int(am.group("peer_port"))
            ev["dir_arrow"] = am.group("arrow")
        return ev

    wm = BPF_SCHED_WAKING_RE.match(rest)
    if wm:
        ev["kind"] = "sched_waking"
        ev["tid"] = int(wm.group("tid"))
        ev["cpu"] = int(wm.group("cpu"))
        ev["comm"] = wm.group("comm")
        ev["pid"] = int(wm.group("pid"))
        ev["target_cpu"] = int(wm.group("target_cpu"))
        return ev

    um = BPF_SCHED_WAKEUP_RE.match(rest)
    if um:
        ev["kind"] = "sched_wakeup"
        ev["cpu"] = int(um.group("cpu"))
        ev["comm"] = um.group("comm")
        ev["pid"] = int(um.group("pid"))
        ev["target_cpu"] = int(um.group("target_cpu"))
        return ev

    sm = BPF_SCHED_SWITCH_RE.match(rest)
    if sm:
        ev["kind"] = "sched_switch"
        ev["cpu"] = int(sm.group("cpu"))
        ev["prev_comm"] = sm.group("prev_comm")
        ev["prev_pid"] = int(sm.group("prev_pid"))
        ev["next_comm"] = sm.group("next_comm")
        ev["next_pid"] = int(sm.group("next_pid"))
        return ev

    if rest.startswith("tcpwakeup out"):
        tm2 = BPF_TCPWAKEUP_OUT_RE.match(rest)
        if tm2:
            ev["kind"] = "tcpwakeup_out"
            ev["tid"] = int(tm2.group("tid"))
            ev["cpu"] = int(tm2.group("cpu"))
            return ev

    ev["kind"] = "other"
    return ev


def iter_warn_blocks(fh):
    """流式解析 latency_warn 文件，逐块 yield（不整文件驻留内存）。"""
    cur = None
    for line in fh:
        tm = LW_TRIG_RE.match(line.strip())
        if tm:
            if cur:
                yield cur
            try:
                ts = datetime.strptime(tm.group("ts"), "%Y-%m-%d %H:%M:%S:%f")
            except ValueError:
                ts = None
            cur = {"ts": ts, "cpu": None, "comm": None, "pid": None,
                   "latency_us": None, "raw": [line.rstrip("\n")]}
            continue
        if cur is not None:
            cur["raw"].append(line.rstrip("\n"))
            im = LW_INFO_RE.search(line)
            if im:
                cur["cpu"] = int(im.group("cpu"))
                cur["comm"] = im.group("comm")
                cur["pid"] = int(im.group("pid"))
                cur["latency_us"] = int(im.group("lat"))
            if len(cur["raw"]) > 60:  # truncate very long stacks
                cur["raw"] = cur["raw"][:60] + ["..."]
                cur["_truncated"] = True
    if cur:
        yield cur


def parse_latency_warn_blocks(path):
    """全量解析（小文件/测试用）；大文件请用 scan_warn_windows。"""
    try:
        fh = open(path, "r", errors="replace")
    except OSError:
        return []
    with fh:
        return list(iter_warn_blocks(fh))


class _UnsortedWarnLog(Exception):
    pass


def _iter_warn_blocks_kept(fh, keep):
    """iter_warn_blocks 的快进版：keep(ts) 为 False 的块只做行快进，不收集
    raw、不做行级 regex（大文件下窗口外块占绝大多数，此为吞吐关键）。"""
    cur = None
    keeping = False
    for line in fh:
        if line.startswith("["):  # 触发行快速预判（LW_TRIG_RE 以 ^\[ 开头）
            tm = LW_TRIG_RE.match(line.strip())
            if tm:
                if cur is not None and keeping:
                    yield cur
                try:
                    ts = datetime.strptime(tm.group("ts"), "%Y-%m-%d %H:%M:%S:%f")
                except ValueError:
                    ts = None
                keeping = ts is not None and keep(ts)
                cur = {"ts": ts, "cpu": None, "comm": None, "pid": None,
                       "latency_us": None, "raw": [line.rstrip("\n")]} if keeping else None
                continue
        if keeping and cur is not None:
            cur["raw"].append(line.rstrip("\n"))
            im = LW_INFO_RE.search(line)
            if im:
                cur["cpu"] = int(im.group("cpu"))
                cur["comm"] = im.group("comm")
                cur["pid"] = int(im.group("pid"))
                cur["latency_us"] = int(im.group("lat"))
            if len(cur["raw"]) > 60:  # truncate very long stacks
                cur["raw"] = cur["raw"][:60] + ["..."]
                cur["_truncated"] = True
    if cur is not None and keeping:
        yield cur


def _make_warn_keep(trace_windows):
    """合并窗口为不相交区间，返回 keep(ts) 快速判断函数（bisect）。"""
    spans = []
    for s, e in sorted((s, e) for s, e in trace_windows.values()):
        if spans and s <= spans[-1][1]:
            if e > spans[-1][1]:
                spans[-1] = (spans[-1][0], e)
        else:
            spans.append((s, e))
    starts = [s for s, _ in spans]
    ends = [e for _, e in spans]

    def keep(ts):
        i = bisect.bisect_right(starts, ts) - 1
        return i >= 0 and ts <= ends[i]
    return keep


def scan_warn_windows(path, trace_windows):
    """单遍流式扫描 warn 文件，返回 {key: [blocks]}（窗口外块直接丢弃）。

    trace_windows: {key: (start_dt, end_dt)}。块按时间有序（常态）时用扫描线
    O((B+W)logW)；检测到乱序则回退逐块线性匹配（内存换正确性）。
    """
    out = {k: [] for k in trace_windows}
    if not trace_windows:
        return out
    try:
        _warn_sweep(path, trace_windows, out)
    except _UnsortedWarnLog:
        for lst in out.values():
            lst.clear()
        _warn_linear(path, trace_windows, out)
    return out


def _warn_sweep(path, trace_windows, out):
    wins = sorted((s, e, k) for k, (s, e) in trace_windows.items())
    keep = _make_warn_keep(trace_windows)
    i, n = 0, len(wins)
    active = []  # min-heap of (end, key)
    last_ts = None
    with open(path, "r", errors="replace") as fh:
        for block in _iter_warn_blocks_kept(fh, keep):
            ts = block["ts"]
            if ts is None:
                continue
            if last_ts is not None and ts < last_ts:
                raise _UnsortedWarnLog()
            last_ts = ts
            while i < n and wins[i][0] <= ts:
                heapq.heappush(active, (wins[i][1], wins[i][2]))
                i += 1
            while active and active[0][0] < ts:
                heapq.heappop(active)
            for _, k in active:
                out[k].append(block)


def _warn_linear(path, trace_windows, out):
    sys.stderr.write("warn: %s 告警块时间乱序，回退线性匹配\n" % path)
    keep = _make_warn_keep(trace_windows)
    items = list(trace_windows.items())
    with open(path, "r", errors="replace") as fh:
        for block in _iter_warn_blocks_kept(fh, keep):
            ts = block["ts"]
            if ts is None:
                continue
            for k, (s, e) in items:
                if s <= ts <= e:
                    out[k].append(block)


# ── 辅助日志解析：irqoff 关中断 / sar 网卡利用率 / brpc bthread ────────────────

def _irqoff_finalize(rec, stats, wins, blocks):
    """一条 irqoff 记录完成：计入全周期统计 + 命中的 trace 窗口。"""
    stats["total"] += 1
    if rec["irq"] == "hardirq":
        stats["hardirq_n"] += 1
    elif rec["irq"] == "softirq":
        stats["softirq_n"] += 1
    lu = rec["latency_us"]
    stats["max_us"] = max(stats["max_us"], lu)
    stats["total_us"] += lu
    c = stats["by_comm"].setdefault(rec["comm"], {"n": 0, "max_us": 0, "total_us": 0})
    c["n"] += 1
    c["max_us"] = max(c["max_us"], lu)
    c["total_us"] += lu
    cp = stats["by_cpu"].setdefault(rec["cpu"], {"n": 0, "max_us": 0})
    cp["n"] += 1
    cp["max_us"] = max(cp["max_us"], lu)
    for b in IRQOFF_BUCKETS_US:
        if lu >= b:
            stats["buckets"][b] += 1
    stats["series"].append((rec["ts"], lu, rec["comm"], rec["cpu"]))
    for s, e, k in wins:
        if s <= rec["ts"] <= e:
            blocks[k].append(rec)


def scan_irqoff(path, trace_windows):
    """单遍扫描 irqoff_latency_<ip>.log，返回 (stats, blocks)。

    块状态机：hardirq:/softirq: 行切换中断类型，cpu: N 行切换 cpu，
    COMMAND 行开一条新记录（调用栈行附加其后）。每条记录头都计入全周期
    统计（窗口外记录不驻留 raw）；窗口内记录保留完整块（raw 截断 60 行）。
    trace_windows: {key: (start_dt, end_dt)}，blocks: {key: [record]}。
    """
    stats = {"total": 0, "hardirq_n": 0, "softirq_n": 0, "max_us": 0, "total_us": 0,
             "by_comm": {}, "by_cpu": {},
             "buckets": {b: 0 for b in IRQOFF_BUCKETS_US}, "series": []}
    blocks = {k: [] for k in trace_windows}
    wins = sorted((s, e, k) for k, (s, e) in trace_windows.items())
    cur_irq, cur_cpu, cur = None, None, None
    try:
        fh = open(path, "r", errors="replace")
    except OSError:
        return stats, blocks
    with fh:
        for line in fh:
            stripped = line.strip()
            m = IRQOFF_IRQ_RE.match(stripped)
            if m:
                if cur is not None:
                    _irqoff_finalize(cur, stats, wins, blocks)
                cur_irq, cur = m.group(1), None
                continue
            m = IRQOFF_CPU_RE.match(stripped)
            if m:
                if cur is not None:
                    _irqoff_finalize(cur, stats, wins, blocks)
                cur_cpu, cur = int(m.group("cpu")), None
                continue
            m = IRQOFF_HEAD_RE.match(line)
            if m:
                if cur is not None:
                    _irqoff_finalize(cur, stats, wins, blocks)
                lat = float(m.group("lat"))
                if m.group("unit") == "ms":
                    lat_us = lat * 1000.0
                elif m.group("unit") == "s":
                    lat_us = lat * 1000000.0
                else:
                    lat_us = lat
                try:
                    ts = datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    ts = None
                if ts is None:
                    continue
                cur = {"ts": ts, "irq": cur_irq, "cpu": cur_cpu,
                       "comm": m.group("comm"), "pid": int(m.group("pid")),
                       "latency_us": int(lat_us), "raw": [line.rstrip("\n")]}
                continue
            if cur is not None:
                cur["raw"].append(line.rstrip("\n"))
                if len(cur["raw"]) > 60:  # truncate very long stacks
                    cur["raw"] = cur["raw"][:60] + ["..."]
    if cur is not None:
        _irqoff_finalize(cur, stats, wins, blocks)
    # 时长曲线点集：按最长排序截断（全量记录可能很大，SVG 只画 top N）
    stats["series"].sort(key=lambda p: -p[1])
    stats["series"] = stats["series"][:IRQOFF_SERIES_MAX]
    return stats, blocks


def _ampm_to_24h(hms, ampm):
    """sar 12 小时制时间 → 24 小时制（"10:30:48", "PM" → "22:30:48"）。"""
    h, m, s = hms.split(":")
    h = int(h)
    if ampm == "PM" and h < 12:
        h += 12
    elif ampm == "AM" and h == 12:
        h = 0
    return "%02d:%s:%s" % (h, m, s)


def parse_nic_log(path):
    """解析 nic-<ip>.log（ethtool 属性 + sar 采样），返回
    {dev: {"ethtool": {Speed/Duplex/Link detected...}, "samples": [...]}}。

    文件小（每秒一行采样），全量解析一次，供全局统计 + 多 trace 窗口复用。
    sar 时间无日期，存 24h 制 "HH:MM:SS"（hms），关联时再按 trace 日期组合。
    """
    devs = {}
    cur_dev = None
    try:
        fh = open(path, "r", errors="replace")
    except OSError:
        return devs
    with fh:
        for line in fh:
            m = NIC_SETTINGS_RE.match(line)
            if m:
                cur_dev = m.group("dev")
                devs.setdefault(cur_dev, {"ethtool": {}, "samples": []})
                continue
            m = SAR_DATA_RE.match(line)
            if m:
                dev = m.group("iface")
                d = devs.setdefault(dev, {"ethtool": {}, "samples": []})
                d["samples"].append({
                    "hms": _ampm_to_24h(m.group("time"), m.group("ampm")),
                    "rxpck": float(m.group("rxpck")),
                    "txpck": float(m.group("txpck")),
                    "rxkB": float(m.group("rxkB")),
                    "txkB": float(m.group("txkB")),
                    "ifutil": float(m.group("ifutil"))})
                continue
            if cur_dev is not None:
                m = NIC_PROP_RE.match(line)
                if m:
                    devs[cur_dev]["ethtool"][m.group("key")] = m.group("val")
    return devs


def _sar_dt(hms, day):
    """sar 样本时间 "HH:MM:SS" + trace 窗口日期 → datetime。"""
    h, m, s = hms.split(":")
    return day.replace(hour=int(h), minute=int(m), second=int(s), microsecond=0)


def _sar_in_window(samples, win_start, win_end):
    """窗口内的 sar 样本（秒粒度：样本时刻 S 覆盖 [S, S+1s) 区间，与窗口相交即命中）。"""
    out = []
    span = timedelta(seconds=1)
    for s in samples:
        dt = _sar_dt(s["hms"], win_start)
        if dt <= win_end and dt + span > win_start:
            out.append(s)
    return out


def _nic_dev_stats(d):
    """nic log 单 dev 解析结果 → 全周期统计 dict（aux_stats / 概览卡 / JSON 用）。"""
    samples, et = d["samples"], d["ethtool"]
    out = {"n_samples": len(samples), "max_ifutil": None, "avg_ifutil": None,
           "peak_hms": None, "max_rxpck": None}
    if samples:
        out["max_ifutil"] = max(s["ifutil"] for s in samples)
        out["avg_ifutil"] = sum(s["ifutil"] for s in samples) / len(samples)
        out["peak_hms"] = max(samples, key=lambda s: s["ifutil"])["hms"]
        out["max_rxpck"] = max(s["rxpck"] for s in samples)
    out.update(et)  # Speed / Duplex / Link detected（存在时平铺）
    return out


def _parse_bthread_event(line, dt):
    """brpc bthread 日志行 → 事件 dict（不匹配返回 None）。"""
    if "bthread created:" in line:
        m = BTHREAD_CREATED_RE.search(line)
        if not m:
            return None
        return {"ts": dt, "kind": "created", "tid": int(m.group("ctid")),
                "bthread_id": int(m.group("bid")),
                "creation_mode": m.group("mode"),
                "target_pending_tasks": int(m.group("tp")),
                "pending_time_us": None, "raw": line.rstrip("\n")}
    if "bthread first scheduled:" in line:
        m = BTHREAD_SCHED_RE.search(line)
        if not m:
            return None
        return {"ts": dt, "kind": "scheduled", "tid": int(m.group("wtid")),
                "bthread_id": int(m.group("bid")),
                "creation_mode": None, "target_pending_tasks": None,
                "pending_time_us": int(m.group("ptu")),
                "raw": line.rstrip("\n")}
    return None


def scan_bthread_windows(path, windows):
    """单遍扫描 brpc bthread 日志（glog 格式），返回 {key: [events]}。

    行首快速预判（I/W/E + MMDD）后解析时间；glog 行无年份，按各窗口
    日期（±1 年容错跨年）组合候选 datetime 再判窗口归属。窗口外行只做
    行首预判即跳过（不做完整 regex）。
    """
    out = {k: [] for k in windows}
    if not windows:
        return out
    # 窗口按 (年, 月, 日) 建索引：glog 行先定位日期再比对少量窗口
    by_date = {}
    for k, (s, e) in windows.items():
        d = s.date()
        while d <= e.date():
            by_date.setdefault((d.year, d.month, d.day), []).append((s, e, k))
            d += timedelta(days=1)
    years = {y for (y, _m, _d) in by_date}
    years |= {y + 1 for y in years} | {y - 1 for y in years}
    with open(path, "r", errors="replace") as fh:
        for line in fh:
            # glog 行首快速预判：I0824 22:32:23.661136 ...
            if len(line) < 22 or line[0] not in "IWE" or not line[1:5].isdigit():
                continue
            try:
                mon, day = int(line[1:3]), int(line[3:5])
                hh, mm, ss = int(line[6:8]), int(line[9:11]), int(line[12:14])
                us = int(line[15:21])
            except ValueError:
                continue
            for y in years:
                day_wins = by_date.get((y, mon, day))
                if not day_wins:
                    continue
                try:
                    dt = datetime(y, mon, day, hh, mm, ss, us)
                except ValueError:
                    continue
                for s, e, k in day_wins:
                    if s <= dt <= e:
                        ev = _parse_bthread_event(line, dt)
                        if ev:
                            out[k].append(ev)
    return out


# ── Data containers ───────────────────────────────────────────────────────────

class SlowRecord:
    __slots__ = ("trace_id", "ts", "fields", "log_path", "pod_dir")

    def __init__(self, trace_id, ts, fields, log_path, pod_dir):
        self.trace_id = trace_id
        self.ts = ts
        self.fields = fields
        self.log_path = log_path
        self.pod_dir = pod_dir


class TraceContext:
    def __init__(self, slow):
        self.slow = slow
        self.trace_id = slow.trace_id
        self.anchors = {}          # milestone -> {ts, tid, cpu, bid, host, pod_dir, log_path, raw}
        self.client_pod_dir = slow.pod_dir
        self.client_node = None
        self.server_node = None
        self.client_ip = None
        self.server_ip = None
        self.conn = None           # (client_ip, cport, server_ip, sport)
        self.macro = {}            # cs_sr / sr_ss / ss_cr (us)
        self.milestones = {}       # milestone -> datetime
        self.kernel_segments = []  # list of segment dicts
        self.kernel_events = {"client": [], "server": []}
        self.warn_events = {"client": [], "server": []}
        self.wakeup_chain = []     # evidence lines for kernel->user delay
        self.server_wakeup_chain = []   # server 侧：内核收包 → ServerRecv 唤醒链
        self.thread_oncpu_ts = None     # server 协程所在线程上 CPU 时刻（T2 推导）
        self.thread_traces = {}    # 锚点名 -> [sched 事件]（关键线程调度轨迹）
        self.coro_evidence = []    # 协程迁移 / CPU 一致性证据（list[str]）
        self.nic_evidence = []     # 网卡层证据：TCP 重传等（list[str]）
        self.phys_wire = {"s2c": None, "c2s": None}  # 物理网卡间线路定界（seq 关联）
        self.migration = None      # 协程迁移 dict 或 None
        self.missing = []          # evidence-missing notes
        self.conclusion = {}
        self.filtered_events = {"client": [], "server": []}  # 五元组过滤后的事件（展示用）
        self.server_log_path = None  # server 日志文件路径（前序协程查询用）
        self.preceding_trace_lines = {"client": [], "server": []}  # 前序协程轨迹明细行
        # 辅助日志（可选，缺失降级）：irqoff 关中断 / sar 网卡利用率 / bthread 协程事件
        self.irqoff_events = {"client": [], "server": []}
        self.nic_samples = {"client": [], "server": []}
        self.bthread_events = {"client": [], "server": []}
        # 问题窗口全景：窗口内全部连接的 bpf 事件（tcp/nic/sock 全量 + sched
        # 合并，match5t 标注是否属于问题连接五元组），供高亮展示与 cpu 侵占分析
        self.bpf_window_events = {"client": [], "server": []}
        self.cpu_busy = {}          # side -> 问题窗口 cpu 侵占分析结果 dict
        self.cpu_evidence = []      # cpu 侵占证据（list[str]）
        self.cpu_busy_preempt = False  # 业务线程所在 cpu 上发现其他请求处理
        # 慢段窗口：瓶颈段时间窗内该侧全部连接的 bpf 事件（高亮问题五元组 +
        # 过滤选择），bpf 事件明细"慢段时间窗事件"子项数据源
        self.slow_seg = {}


# ── Phase 0: log discovery ────────────────────────────────────────────────────

class LogDiscovery:
    def __init__(self, root):
        self.root = Path(root)
        self.client_logs = []
        self.worker_logs = []
        self.bpf_by_node = {}      # nodeName -> path
        self.warn_by_node = {}     # nodeName -> path
        self.node_by_ip = {}       # nodeIp -> nodeName（bpf/warn 文件名建立）
        self.irqoff_by_node = {}   # nodeName -> path（关中断日志）
        self.irqoff_by_ip = {}     # nodeIp -> path（IP 未能映射到节点时保留，供全局统计）
        self.nic_by_node = {}      # nodeName -> path（sar 网卡利用率日志）
        self.nic_by_ip = {}        # nodeIp -> path（同上）
        self.brpc_by_pod = {}      # podName -> path（bRPC bthread 日志）
        self.aux_stats = {"irqoff": {}, "nic": {}}  # 辅助日志全周期统计
        self._discover()

    def _discover(self):
        root = self.root
        if not root.is_dir():
            raise FileNotFoundError("log root not found: %s" % root)
        for d in ("collected", "collected_worker_logs"):
            base = root / d
            if base.is_dir():
                target = self.client_logs if d == "collected" else self.worker_logs
                for f in sorted(base.rglob("*.log")):
                    if f.is_file():
                        target.append(f)
        dsdir = root / "dscollect_log"
        irqoff_pending, nic_pending = [], []
        if dsdir.is_dir():
            for f in sorted(dsdir.iterdir()):
                if not f.is_file():
                    continue
                m = BPF_FILE_RE.match(f.name)
                if m:
                    self.bpf_by_node[m.group("name")] = f
                    self.node_by_ip.setdefault(m.group("ip"), m.group("name"))
                    continue
                m = IRQOFF_FILE_RE.match(f.name)
                if m:
                    irqoff_pending.append((m.group("ip"), f))
                    continue
                m = NIC_FILE_RE.match(f.name)
                if m:
                    nic_pending.append((m.group("ip"), f))
                    continue
                m = BRPC_FILE_RE.match(f.name)
                if m:
                    self.brpc_by_pod.setdefault(m.group("pod"), f)
        lwdir = root / "latency_warn_log"
        if lwdir.is_dir():
            for f in sorted(lwdir.iterdir()):
                m = WARN_FILE_RE.match(f.name)
                if m and f.is_file():
                    self.warn_by_node[m.group("name")] = f
                    self.node_by_ip.setdefault(m.group("ip"), m.group("name"))
        # irqoff/nic 文件名只含 nodeIp，经 node_by_ip 反查节点（bpf/warn 先建好映射）
        for ip, f in irqoff_pending:
            node = self.node_by_ip.get(ip)
            if node:
                self.irqoff_by_node[node] = f
            else:
                self.irqoff_by_ip[ip] = f
        for ip, f in nic_pending:
            node = self.node_by_ip.get(ip)
            if node:
                self.nic_by_node[node] = f
            else:
                self.nic_by_ip[ip] = f

    def resolve_node(self, pod_dir_name):
        return match_node(pod_dir_name, list(self.bpf_by_node.keys())
                          + list(self.warn_by_node.keys()))


# ── Phase 1: client log scan for slow records ─────────────────────────────────

def scan_slow_records(client_logs, threshold_us, only_traces=None, verbose=False,
                      workers=1):
    """按块扫描 client 日志中的 [BRPC_RPC_FRAMEWORK_SLOW] 行（大日志友好）。

    workers>1 时按文件并行（fork 进程池）；结果与串行完全一致。
    """
    only = tuple(only_traces) if only_traces else None
    jobs = [(str(p), None, "slow", None, threshold_us, only, verbose, None)
            for p in client_logs]
    records = []
    for _, _, _, recs, _ in run_parallel(jobs, workers):
        records.extend(recs)
    records.sort(key=lambda r: -int(r.fields.get("network_residual_us", "0")))
    return records


# ── Phase 2: trace anchors ────────────────────────────────────────────────────

# 锚点行在日志中极稀疏（每 RPC 4 行），用字节块标记扫描定位后做 trace 精确匹配，
# 替代对每行做 O(traces) 次子串比较的旧实现（大日志下不可行）。
ANCHOR_MARKERS = [b"ClientSend ts ", b"ClientRecv ts ",
                  b"ServerRecv ts ", b"ServerSend ts "]


def collect_anchor_lines(client_logs, worker_logs, trace_ids, verbose=False):
    """单遍扫描 client+worker 日志，收集问题 trace 的 4 类锚点行。

    返回 {trace_id: {"client": [...], "worker": [...]}}（各按 ts 升序）。
    下游 build_anchors 只消费锚点标记行，故仅索引这些行即可。
    """
    wanted = set(trace_ids)
    idx = {t: {"client": [], "worker": []} for t in wanted}
    for logs, source in ((client_logs, "client"), (worker_logs, "worker")):
        for path, line in iter_marker_lines(logs, ANCHOR_MARKERS, verbose=verbose):
            info = parse_info_line(line)
            if not info:
                continue
            t = info["trace"]
            if t not in wanted:  # 精确匹配（锚点行 trace 列 == SLOW 行 trace_id）
                continue
            info["_path"] = str(path)
            info["_pod_dir"] = path.parent.name
            idx[t][source].append(info)
    for t in wanted:
        idx[t]["client"].sort(key=lambda x: x["ts"])
        idx[t]["worker"].sort(key=lambda x: x["ts"])
    return idx


def collect_anchor_and_info(client_logs, worker_logs, trace_ids, verbose=False,
                            workers=1):
    """单遍扫描 client+worker 日志，同时收集：
      1. 问题 trace 的 4 类锚点行（等价 collect_anchor_lines 的输出结构）；
      2. 问题 trace 的全部 INFO 行（等价 collect_trace_info_lines 的输出，
         含业务中间行，供 --raw 使用）。

    markers = ANCHOR_MARKERS + trace_id 字节串：锚点行本身含 trace_id，
    业务行以 trace_id 命中——一遍 IO 完成原阶段2+阶段7 两遍扫描的工作。
    workers>1 时按文件并行。返回 (anchor_idx, info_idx)。
    """
    wanted = set(trace_ids)
    markers = list(ANCHOR_MARKERS) + [t.encode("utf-8") for t in wanted]
    anchor_idx = {t: {"client": [], "worker": []} for t in wanted}
    info_idx = {t: [] for t in wanted}
    jobs = [(str(p), markers, "anchor_info", tuple(wanted), None, None,
             verbose, "client") for p in client_logs] \
        + [(str(p), markers, "anchor_info", tuple(wanted), None, None,
            verbose, "worker") for p in worker_logs]
    for _, path_str, source, anchors, info_pairs in run_parallel(jobs, workers):
        for t, infos in anchors.items():
            anchor_idx[t][source].extend(infos)
        for t, line in info_pairs:
            info_idx[t].append((source, path_str, line))
    for t in wanted:
        anchor_idx[t]["client"].sort(key=lambda x: x["ts"])
        anchor_idx[t]["worker"].sort(key=lambda x: x["ts"])
    return anchor_idx, info_idx


def collect_trace_info_lines(client_logs, worker_logs, trace_ids, verbose=False):
    """单遍扫描 client+worker 日志，收集问题 trace 的**全部** INFO 行
    （含业务中间行，不只锚点；供原始日志汇总 --raw 使用）。

    返回 {trace_id: [(source, path, line), ...]}，行序为扫描序
    （文件序 × 文件内时间序）。
    """
    wanted = set(trace_ids)
    markers = [t.encode("utf-8") for t in wanted]
    idx = {t: [] for t in wanted}
    if not wanted:
        return idx
    for logs, source in ((client_logs, "client"), (worker_logs, "worker")):
        for path, line in iter_marker_lines(logs, markers, verbose=verbose):
            info = parse_info_line(line)
            if not info:
                continue
            t = info["trace"]
            if t not in wanted:  # 精确匹配（trace 列 == trace_id）
                continue
            idx[t].append((source, str(path), line.rstrip("\n")))
    return idx


def _pick_anchor(lines, ts_value, regex, want_first=True):
    """Pick anchor line whose ts value matches exactly; else first/last by regex."""
    matches = []
    for info in lines:
        m = regex.search(info["msg"])
        if m and m.group(1) == ts_value:
            matches.append((info, m))
    if matches:
        info, m = matches[0]
        return info, m
    for info in lines:
        m = regex.search(info["msg"])
        if m:
            matches.append((info, m))
            if want_first:
                break
    if not matches:
        return None, None
    return (matches[0] if want_first else matches[-1])


def build_anchors(ctx, client_lines, worker_lines):
    """Fill ctx.anchors / macro segments / node / ip resolution."""
    slow = ctx.slow
    f = slow.fields

    info, m = _pick_anchor(client_lines, f.get("ClientSend", ""), CLIENT_SEND_RE, True)
    if info:
        ctx.anchors["ClientSend"] = _anchor(info, m)
    info, m = _pick_anchor(client_lines, f.get("ClientRecv", ""), CLIENT_RECV_RE, False)
    if info:
        ctx.anchors["ClientRecv"] = _anchor(info, m)

    sr_val, ss_val = f.get("ServerRecv", ""), f.get("ServerSend", "")
    srv_lines = [x for x in worker_lines if SERVER_RECV_RE.search(x["msg"])
                 or SERVER_SEND_RE.search(x["msg"])]
    # locate server log: file containing ServerRecv with matching ts value
    server_path = None
    for info in srv_lines:
        m = SERVER_RECV_RE.search(info["msg"])
        if m and sr_val and m.group(1) == sr_val:
            server_path = info["_path"]
            break
    if server_path is None:
        for info in srv_lines:
            server_path = info["_path"]
            break
    if server_path:
        slines = [x for x in srv_lines if x["_path"] == server_path]
        slines.sort(key=lambda x: x["ts"])
        info, m = _pick_anchor(slines, sr_val, SERVER_RECV_RE, True)
        if info:
            ctx.anchors["ServerRecv"] = _anchor(info, m)
        info, m = _pick_anchor(slines, ss_val, SERVER_SEND_RE, False)
        if info:
            ctx.anchors["ServerSend"] = _anchor(info, m)
        ctx.server_pod_dir = Path(server_path).parent.name
        ctx.server_log_path = server_path
    else:
        ctx.missing.append("worker 日志中未找到该 trace 的 ServerRecv/ServerSend 锚点"
                           "（对应 worker 的日志可能未收集，无法计算 server 侧与跨节点分段）")

    cs = ctx.anchors.get("ClientSend")
    cr = ctx.anchors.get("ClientRecv")
    sr = ctx.anchors.get("ServerRecv")
    ss = ctx.anchors.get("ServerSend")
    if cs:
        ctx.client_ip = cs["host"]
    if sr:
        ctx.server_ip = sr["host"]
    if cs and sr:
        ctx.macro["cs_sr"] = (sr["ts"] - cs["ts"]).total_seconds() * 1e6
    if sr and ss:
        ctx.macro["sr_ss"] = (ss["ts"] - sr["ts"]).total_seconds() * 1e6
    if ss and cr:
        ctx.macro["ss_cr"] = (cr["ts"] - ss["ts"]).total_seconds() * 1e6
    for key, anchor in (("ClientSend", cs), ("ClientRecv", cr),
                        ("ServerRecv", sr), ("ServerSend", ss)):
        if anchor:
            ctx.milestones[key] = anchor["ts"]


def _anchor(info, m):
    """锚点 dict：m 为锚点正则 match（可为 None，向后兼容）；tid/cpu/bid 均可能缺省。"""
    return {"ts": info["ts"], "tid": m.group(2) if m else None,
            "cpu": m.group("cpu") if m else None,
            "bid": m.group("bid") if m else None,
            "host": info["host"],
            "pod_dir": info["_pod_dir"], "log_path": info["_path"],
            "raw": info["raw"]}


# ── Phase 3: bpf correlation ──────────────────────────────────────────────────
#
# 旧实现对每个 (node, day) 把整个 bpf 文件解析成 dict 列表常驻内存（GB 级文件直接
# OOM，且多天会重复解析整文件）。新方案 BpfScanner：
#   * 问题相关行只占 [ClientSend−pad, ClientRecv+pad] 的毫秒级时间窗；
#   * 窗口转"当日时间段(tod)"（bpf 行无日期），跨午夜拆分；
#   * seek 模式（默认）：二分定位每个窗口簇，只读簇内字节（前后各留 10s slack），
#     簇内做单调性自校验，乱序超容忍自动回退整文件流式扫描；
#   * full 模式：顺序读整文件，仅做时间前缀的 bisect 判断（无正则开销）；
#   * 事件按窗口归属到 (trace_id, side)，调度类事件有配额上限。

def tod_us(dt):
    return (dt.hour * 3600 + dt.minute * 60 + dt.second) * 1000000 + dt.microsecond


def us_to_tod(us):
    h, rem = divmod(us, 3600000000)
    m, rem = divmod(rem, 60000000)
    s, u = divmod(rem, 1000000)
    return "%02d:%02d:%02d:%06d" % (h, m, s, u)


def _parse_tod_us(b):
    """解析 b"HH:MM:SS:uuuuuu" → 当日微秒数；格式非法返回 None。"""
    if len(b) != 15 or b[2] != 58 or b[5] != 58 or b[8] != 58:
        return None
    try:
        h = int(b[0:2])
        m = int(b[3:5])
        s = int(b[6:8])
        us = int(b[9:15])
    except ValueError:
        return None
    if h > 23 or m > 59 or s > 59:
        return None
    return (h * 3600 + m * 60 + s) * 1000000 + us


def _fmt_tod(us):
    """当日微秒数 → b"HH:MM:SS:uuuuuu"（定长，字典序 == 时间序）。"""
    h, rem = divmod(us, 3600000000)
    m, rem = divmod(rem, 60000000)
    s, u = divmod(rem, 1000000)
    return ("%02d:%02d:%02d:%06d" % (h, m, s, u)).encode("ascii")


class TraceWindow:
    """单个 trace 在单侧节点上的一个当日时间窗（跨午夜已拆分）。"""

    __slots__ = ("trace_id", "side", "cip", "sip", "base_date",
                 "start_us", "end_us", "start_tod", "end_tod")

    def __init__(self, trace_id, side, start_dt, end_dt, cip, sip):
        self.trace_id = trace_id
        self.side = side
        self.cip = cip
        self.sip = sip
        self.base_date = start_dt.date()
        self.start_us = tod_us(start_dt)
        self.end_us = tod_us(end_dt)
        self.start_tod = us_to_tod(self.start_us)
        self.end_tod = us_to_tod(self.end_us)


def split_window_at_midnight(trace_id, side, start_dt, end_dt, cip, sip):
    """跨午夜的窗口按自然日拆分（bpf 行无日期，需按段绑定日期）。"""
    out = []
    day = start_dt.date()
    while True:
        day_start = datetime.combine(day, datetime.min.time())
        next_day = datetime.combine(day + timedelta(days=1), datetime.min.time())
        seg_start = max(start_dt, day_start)
        seg_end = min(end_dt, next_day - timedelta(microseconds=1))
        if seg_end >= seg_start:
            out.append(TraceWindow(trace_id, side, seg_start, seg_end, cip, sip))
        if seg_end >= end_dt:
            break
        day += timedelta(days=1)
    return out


class _Cluster:
    __slots__ = ("start_us", "end_us", "start_tod", "end_tod", "windows",
                 "win_tods_b", "win_tods_s")

    def __init__(self, w):
        self.start_us = w.start_us
        self.end_us = w.end_us
        self.start_tod = w.start_tod
        self.end_tod = w.end_tod
        self.windows = [w]
        self.win_tods_b = []      # [(lo_bytes, hi_bytes)] 窗口区间（合并去缝隙）
        self.win_tods_s = []      # [(lo_str, hi_str)] 同上，str 版

    def finalize(self):
        """簇定型后调用：把 windows 合并为不相交 tod 区间（行级预过滤用）。"""
        spans = []
        for w in sorted(self.windows, key=lambda x: x.start_us):
            if spans and w.start_us <= spans[-1][1]:
                if w.end_us > spans[-1][1]:
                    spans[-1] = (spans[-1][0], w.end_us)
            else:
                spans.append((w.start_us, w.end_us))
        self.win_tods_b = [(_fmt_tod(s), _fmt_tod(e)) for s, e in spans]
        self.win_tods_s = [(lo.decode("ascii"), hi.decode("ascii"))
                           for lo, hi in self.win_tods_b]

    def head_in_window(self, head):
        """seek 模式：行首 15 bytes 是否落在任一窗口内（闭区间）。"""
        for lo, hi in self.win_tods_b:
            if lo <= head <= hi:
                return True
        return False

    def tod_in_window(self, tod):
        """stream 模式：行首 tod str 是否落在任一窗口内（闭区间）。"""
        for lo, hi in self.win_tods_s:
            if lo <= tod <= hi:
                return True
        return False


def merge_window_clusters(windows, slack_us=SEEK_SLACK_US):
    """把重叠/相邻（≤slack）的 tod 窗口合并成读取簇（簇间互不相交）。"""
    clusters = []
    for w in sorted(windows, key=lambda x: x.start_us):
        if clusters and w.start_us <= clusters[-1].end_us + slack_us:
            c = clusters[-1]
            if w.end_us > c.end_us:
                c.end_us = w.end_us
                c.end_tod = w.end_tod
            c.windows.append(w)
        else:
            clusters.append(_Cluster(w))
    for c in clusters:
        c.finalize()
    return clusters


class _UnsortedBpfLog(Exception):
    pass


class BpfScanner:
    """对单个 bpf 文件按合并时间窗做一次性窗口化扫描（不整文件驻留内存）。

    self.diag 记录零事件诊断所需的计数器（读取行/时间匹配/IP 匹配/样例连接/
    文件首末行时间），供 analyze 阶段3 输出诊断。
    """

    def __init__(self, path, windows, full_scan=False,
                 max_sched_events=DEFAULT_MAX_SCHED_EVENTS, verbose=False,
                 slack_us=SEEK_SLACK_US,
                 max_window_net_events=DEFAULT_MAX_WINDOW_NET_EVENTS):
        self.path = Path(path)
        self.windows = windows
        self.full_scan = full_scan
        self.max_sched_events = max_sched_events
        self.verbose = verbose
        self.slack_us = slack_us
        self.max_window_net_events = max_window_net_events
        # 窗口全景：窗口内全部连接类事件（tcp/nic/sock，不限 IP），供
        # "问题窗口 bpf 事件全景 + cpu 侵占分析"使用（其他请求穿插展示）
        self.window_results = {}      # (trace_id, side) -> [events]
        self.window_counts = {}       # (trace_id, side) -> n（配额）
        self.window_truncated = set()
        self.clusters = merge_window_clusters(windows, slack_us=slack_us)
        self.cluster_starts = [c.start_tod for c in self.clusters]
        self.diag = {"n_read": 0, "n_tod_match": 0, "n_ip_match": 0,
                     "sample_ips": [], "file_first_tod": None, "file_last_tod": None}
        self._seen_ips = set()

    def scan(self):
        """返回 ({(trace_id, side): [events]}, 被配额截断的 (trace_id, side) 集合)。"""
        results, counts, truncated = {}, {}, set()
        t0 = time.monotonic()
        self._read_file_tod_range()
        if self.full_scan:
            self._scan_stream(results, counts, truncated)
        else:
            try:
                self._scan_seek(results, counts, truncated)
            except _UnsortedBpfLog:
                sys.stderr.write("warn: %s 时间乱序超出容忍范围，回退全文件扫描\n" % self.path)
                results.clear()
                counts.clear()
                truncated.clear()
                self.window_results.clear()
                self.window_counts.clear()
                self.window_truncated.clear()
                self._scan_stream(results, counts, truncated)
        for lst in results.values():
            lst.sort(key=lambda e: e["ts"])
        for lst in self.window_results.values():
            lst.sort(key=lambda e: e["ts"])
        if self.verbose:
            n = sum(len(v) for v in results.values())
            sys.stderr.write("  bpf %s: %d 窗口 %d 簇 → %d 事件, %.2fs\n"
                             % (self.path, len(self.windows), len(self.clusters),
                                n, time.monotonic() - t0))
        return results, truncated

    def _read_file_tod_range(self):
        """读取文件首/末有效 tod 行（诊断用，开销可忽略）。

        首/末行可能是 bpftrace BEGIN printf 的无时间戳头（或 END 尾注），
        向后/向前最多回看 64 行找有效 tod。
        """
        try:
            size = self.path.stat().st_size
            if size == 0:
                return
            with open(self.path, "rb") as fh:
                first = None
                for _ in range(64):
                    line = fh.readline()
                    if not line:
                        break
                    if _parse_tod_us(line[:15]) is not None:
                        first = line
                        break
                    if first is None and not self.diag["file_first_tod"]:
                        self.diag["file_first_tod"] = line[:15].decode(
                            "ascii", "replace").strip()
                if first:
                    self.diag["file_first_tod"] = first[:15].decode(
                        "ascii", "replace").strip()
                if size > len(first or b""):
                    fh.seek(max(0, size - 8192))
                    tail_lines = [l for l in fh.read().split(b"\n") if l]
                    for tl in reversed(tail_lines[-64:] if len(tail_lines) > 64
                                        else tail_lines):
                        if _parse_tod_us(tl[:15]) is not None:
                            self.diag["file_last_tod"] = tl[:15].decode(
                                "ascii", "replace").strip()
                            break
                elif first:
                    self.diag["file_last_tod"] = self.diag["file_first_tod"]
        except OSError:
            pass

    # -- full 模式：顺序读整文件，仅时间前缀 bisect 判断 -----------------------
    def _scan_stream(self, results, counts, truncated):
        starts = self.cluster_starts
        clusters = self.clusters
        with open(self.path, "rb") as fh:
            for raw in fh:
                if len(raw) < 16:
                    continue
                self.diag["n_read"] += 1
                tod = raw[:15].decode("ascii", "replace")
                i = bisect.bisect_right(starts, tod) - 1
                if i < 0:
                    continue
                cl = clusters[i]
                if tod > cl.end_tod:
                    continue
                if not cl.tod_in_window(tod):
                    continue  # 簇内缝隙行（窗口外）不进解析
                self._handle_line(raw.decode("utf-8", "replace"),
                                  cl, results, counts, truncated)

    # -- seek 模式：二分定位窗口簇，只读簇内字节 -------------------------------
    def _scan_seek(self, results, counts, truncated):
        size = self.path.stat().st_size
        slack = self.slack_us
        with open(self.path, "rb") as fh:
            for cl in self.clusters:
                target = cl.start_us - slack
                if target < 0:
                    target = 0
                # 定长 tod 字典序 == 时间序：slack 区行仅做 bytes 比较，免逐行 int 解析
                lo_b = _fmt_tod(max(0, cl.start_us - slack))
                hi_b = _fmt_tod(cl.end_us + slack)
                off = self._find_offset(fh, size, target - 1)
                fh.seek(off)
                prev_us = None
                for raw in fh:
                    if len(raw) < 16:
                        continue
                    head = raw[:15]
                    t = _parse_tod_us(head)
                    if t is None:
                        # 无时间戳行（bpftrace BEGIN printf 头等）：
                        # 不参与有序 break 判断，直接跳过
                        continue
                    if head > hi_b:
                        break  # 有序假设下该簇已读完
                    self.diag["n_read"] += 1
                    if head < lo_b:
                        continue  # 二分对齐噪声（slack 边缘）
                    if not cl.head_in_window(head):
                        continue  # slack 区/簇内缝隙行不进解析（解析是大头）
                    if prev_us is not None and t + 1000000 < prev_us:
                        raise _UnsortedBpfLog()  # >1s 乱序
                    prev_us = t
                    self._handle_line(raw.decode("utf-8", "replace"),
                                      cl, results, counts, truncated)

    def _find_offset(self, fh, size, target_us):
        """二分查找第一条 tod > target_us 的行的字节偏移（要求文件大体有序）。"""
        lo, hi = 0, size
        while lo < hi:
            mid = (lo + hi) // 2
            fh.seek(mid)
            fh.readline()  # 丢弃半行，对齐到下一行首
            pos = fh.tell()
            if pos >= size:
                hi = mid
                continue
            line = fh.readline()
            if not line:
                hi = mid
                continue
            t = _parse_tod_us(line[:15])
            if t is None or t <= target_us:
                lo = fh.tell()
            else:
                hi = mid
        return lo

    # -- 事件归属（与旧 _filter_window 语义一致） ------------------------------
    def _handle_line(self, line, cluster, results, counts, truncated):
        ev = parse_bpf_line(line, cluster.windows[0].base_date)
        if ev is None or ev["kind"] == "other":
            return
        ts = ev["ts"]
        t_us = (ts.hour * 3600 + ts.minute * 60 + ts.second) * 1000000 + ts.microsecond
        kind = ev["kind"]
        diag = self.diag
        for w in cluster.windows:
            if not (w.start_us <= t_us <= w.end_us):
                continue
            diag["n_tod_match"] += 1
            attach = False
            if kind.startswith("tcp") or kind == "sock_readable":
                # tcp_retransmit 也以 tcp_ 开头，走 local/peer IP 双向匹配
                lip, pip = ev.get("local_ip"), ev.get("peer_ip")
                if lip and pip and ((lip == w.cip and pip == w.sip)
                                    or (lip == w.sip and pip == w.cip)):
                    attach = True
                    diag["n_ip_match"] += 1
                elif lip and pip and len(diag["sample_ips"]) < 8:
                    key = "%s:%s -> %s:%s" % (lip, ev.get("local_port"),
                                              pip, ev.get("peer_port"))
                    if key not in self._seen_ips:
                        self._seen_ips.add(key)
                        diag["sample_ips"].append(key)
            elif kind.startswith("nic_"):
                # 网卡事件：src→dst IP 双向匹配（与 tcp 同法，仅 IP 不限端口）
                s, d = ev.get("src_ip"), ev.get("dst_ip")
                if s and d and ((s == w.cip and d == w.sip)
                                or (s == w.sip and d == w.cip)):
                    attach = True
                    diag["n_ip_match"] += 1
                elif s and d and len(diag["sample_ips"]) < 8:
                    key = "%s:%s -> %s:%s" % (s, ev.get("src_port"),
                                              d, ev.get("dst_port"))
                    if key not in self._seen_ips:
                        self._seen_ips.add(key)
                        diag["sample_ips"].append(key)
            else:  # sched_* / tcpwakeup_out：窗口内即保留（唤醒链需要），配额截断
                key = (w.trace_id, w.side)
                if counts.get(key, 0) >= self.max_sched_events:
                    truncated.add(key)
                    continue
                counts[key] = counts.get(key, 0) + 1
                attach = True
            # 窗口全景：连接类事件（tcp/nic/sock，不限 IP）全部保留（配额内），
            # 供问题窗口"其他请求穿插"全景展示与 cpu 侵占分析使用
            if kind.startswith("tcp") or kind == "sock_readable" \
                    or kind.startswith("nic_"):
                wkey = (w.trace_id, w.side)
                if self.window_counts.get(wkey, 0) >= self.max_window_net_events:
                    self.window_truncated.add(wkey)
                else:
                    self.window_counts[wkey] = self.window_counts.get(wkey, 0) + 1
                    we2 = dict(ev)
                    we2["ts"] = datetime.combine(w.base_date, ts.time())
                    self.window_results.setdefault(wkey, []).append(we2)
            if attach:
                e2 = dict(ev)
                e2["ts"] = datetime.combine(w.base_date, ts.time())
                results.setdefault((w.trace_id, w.side), []).append(e2)


class BpfCorrelator:
    """bpf 关联的纯算法部分（窗口事件就绪后的连接识别 / 里程碑 / 唤醒链）。"""

    @staticmethod
    def _identify_conn(client_events, client_send_ts, cip, sip):
        # the RPC's kernel send follows the ClientSend log line closely; allow a
        # small 200us negative tolerance but never accept a send that clearly
        # belongs to an earlier RPC on another connection
        eps = timedelta(microseconds=200)
        best = None
        for ev in client_events:
            if ev["kind"] != "tcp_send_in":
                continue
            if ev.get("local_ip") != cip or ev.get("peer_ip") != sip:
                continue
            if ev["ts"] >= client_send_ts - eps:
                if best is None or ev["ts"] < best["ts"]:
                    best = ev
        if best is None:  # fallback: closest send event in window
            for ev in client_events:
                if ev["kind"] == "tcp_send_in" and ev.get("local_ip") == cip \
                        and ev.get("peer_ip") == sip:
                    if best is None or abs((ev["ts"] - client_send_ts).total_seconds()) < \
                            abs((best["ts"] - client_send_ts).total_seconds()):
                        best = ev
        if not best:
            return None
        return (cip, best["local_port"], sip, best["peer_port"])

    @staticmethod
    def _fill_milestone(ms, ev, cip, cport, sip, sport, side):
        # 网卡层里程碑（src→dst 方向四元组，与连接 IP+侧别联合判定方向）
        if ev["kind"].startswith("nic_"):
            s, d = ev.get("src_ip"), ev.get("dst_ip")
            key = None
            if side == "Client" and s == cip and d == sip:    # client 发出
                key = {"nic_dev_xmit_start": "ClientDevStartXmit",
                       "nic_dev_xmit": "ClientNetDevXmit"}.get(ev["kind"])
            elif side == "Server" and s == sip and d == cip:  # server 发出
                key = {"nic_dev_xmit_start": "ServerDevStartXmit",
                       "nic_dev_xmit": "ServerNetDevXmit"}.get(ev["kind"])
            elif side == "Server" and s == cip and d == sip:  # server 收到 client 包
                key = "ServerNetifRx" if ev["kind"] == "nic_rx_skb" else None
            elif side == "Client" and s == sip and d == cip:  # client 收到 server 包
                key = "ClientNetifRx" if ev["kind"] == "nic_rx_skb" else None
            if key and key not in ms:  # first 出现即定格（多 dev 重复取最早）
                ms[key] = ev["ts"]
            return
        if not ev.get("local_ip"):
            return
        is_client_local = ev["local_ip"] == cip and ev["local_port"] == cport \
            and ev["peer_ip"] == sip and ev["peer_port"] == sport
        is_server_local = ev["local_ip"] == sip and ev["local_port"] == sport \
            and ev["peer_ip"] == cip and ev["peer_port"] == cport
        if not (is_client_local or is_server_local):
            return
        # 方向校验：同节点 bpf 文件同时含双向事件，非本侧方向不填里程碑
        # （如 client/worker 同宿主机时，client 事件列表含 server 方向收包）
        dir_ok = (side == "Client" and is_client_local) \
            or (side == "Server" and is_server_local)
        key = None
        if ev["kind"] == "tcp_send_in" and dir_ok:
            key = side + "TcpSendIn"
        elif ev["kind"] == "tcp_recv_in" and dir_ok:
            first, last = (side + "TcpRecvFirst", side + "TcpRecvLast")
            if first not in ms:
                ms[first] = ev["ts"]
            ms[last] = ev["ts"]  # keep updating → last
            return
        elif ev["kind"] == "tcp_recv_que" and dir_ok:
            key = side + "TcpRecvQue"
        elif ev["kind"] == "sock_readable" and dir_ok:
            key = side + "SockReadable"
        if key and key not in ms:
            ms[key] = ev["ts"]

    @staticmethod
    def _wakeup_chain(client_events, ms, client_recv_ts, cip, cport, sip, sport):
        """Reconstruct the kernel→user pickup wakeup chain on the client node:
        sock_def_readable(conn) → sched_waking(same tid, woken pid/comm)
        → sched_wakeup(pid) → sched_switch(next_pid==pid).
        Falls back to all wakeup/sched events in [recv start, ClientRecv]."""
        start = None
        for k in ("ClientTcpRecvQue", "ClientSockReadable", "ClientTcpRecvFirst"):
            if k in ms:
                start = ms[k] if start is None else min(start, ms[k])
        if start is None:
            return []
        win_ev = [e for e in client_events
                  if start - timedelta(milliseconds=1) <= e["ts"] <= client_recv_ts]

        # precise chain: sock_readable for this connection first
        socks = [e for e in win_ev if e["kind"] == "sock_readable"
                 and e.get("local_ip") == cip and e.get("local_port") == cport]
        if socks:
            sock = socks[0]
            chain = [sock]
            wakings = [e for e in win_ev if e["kind"] == "sched_waking"
                       and e.get("tid") == sock.get("tid")
                       and timedelta(0) <= e["ts"] - sock["ts"] <= timedelta(milliseconds=200)]
            if wakings:
                w = wakings[0]
                chain.append(w)
                pid = w.get("pid")
                for e in win_ev:
                    if e["ts"] < w["ts"]:
                        continue
                    if e["kind"] == "sched_wakeup" and e.get("pid") == pid:
                        chain.append(e)
                    elif e["kind"] == "sched_switch" and e.get("next_pid") == pid:
                        chain.append(e)
                        return chain
                return chain
            return chain

        # fallback: context events
        return [e for e in win_ev if e["kind"] in
                ("sock_readable", "tcpwakeup_out", "sched_waking", "sched_wakeup",
                 "sched_switch", "tcp_recv_in")][:200]

    @staticmethod
    def _server_wakeup_chain(server_events, ms, server_recv_ts,
                             cip, cport, sip, sport, sr_tid=None):
        """server 侧唤醒链：内核收包 → ServerRecv（协程开始执行）。

        返回 (chain, thread_oncpu_ts)：
        - chain: sock_readable → sched_waking → sched_wakeup → sched_switch
          （bpf 事件内部 tid 体系链式推导，与 client 侧同法）；
        - thread_oncpu_ts: 协程所在线程上 CPU 时刻。优先用 ServerRecv 锚点 tid
          精确匹配 sched_switch(next_pid==tid, ts≤ServerRecv) 的最近一条（锚点
          tid 与 host pid 同命名空间时最准）；匹配不上退化为推导链末尾 switch 的
          ts；均无则 None。
        """
        start = None
        for k in ("ServerTcpRecvQue", "ServerSockReadable", "ServerTcpRecvFirst"):
            if k in ms:
                start = ms[k] if start is None else min(start, ms[k])
        if start is None:
            return [], None
        win_ev = [e for e in server_events
                  if start - timedelta(milliseconds=1) <= e["ts"] <= server_recv_ts]

        chain = []
        oncpu_ts = None
        # 1) 锚点 tid 精确匹配：协程所在 worker 线程上 CPU 的 switch
        if sr_tid:
            try:
                tid = int(sr_tid)
            except (TypeError, ValueError):
                tid = None
            if tid is not None:
                best = None
                for e in win_ev:
                    if e["kind"] == "sched_switch" and e.get("next_pid") == tid:
                        if best is None or e["ts"] > best["ts"]:
                            best = e
                if best is not None:
                    oncpu_ts = best["ts"]
                    chain.append(best)

        # 2) 推导链：sock_readable(server 端口) → waking(same tid) → wakeup(pid)
        #    → switch(next_pid==pid)
        socks = [e for e in win_ev if e["kind"] == "sock_readable"
                 and e.get("local_ip") == sip and e.get("local_port") == sport]
        if socks:
            sock = socks[0]
            chain.append(sock)
            wakings = [e for e in win_ev if e["kind"] == "sched_waking"
                       and e.get("tid") == sock.get("tid")
                       and timedelta(0) <= e["ts"] - sock["ts"] <= timedelta(milliseconds=200)]
            if wakings:
                w = wakings[0]
                chain.append(w)
                pid = w.get("pid")
                for e in win_ev:
                    if e["ts"] < w["ts"]:
                        continue
                    if e["kind"] == "sched_wakeup" and e.get("pid") == pid:
                        chain.append(e)
                    elif e["kind"] == "sched_switch" and e.get("next_pid") == pid:
                        chain.append(e)
                        if oncpu_ts is None:
                            oncpu_ts = e["ts"]
                        break
        elif not chain:
            # 无 sock_readable 且无精确匹配：退化为窗口内调度事件上下文
            chain = [e for e in win_ev if e["kind"] in
                     ("sock_readable", "tcpwakeup_out", "sched_waking",
                      "sched_wakeup", "sched_switch")][:200]

        # 去重（精确匹配与推导链可能命中同一事件）并按时间排序
        seen, uniq = set(), []
        for e in chain:
            key = (e["ts"], e["kind"], e.get("pid"), e.get("next_pid"), e.get("tid"))
            if key in seen:
                continue
            seen.add(key)
            uniq.append(e)
        uniq.sort(key=lambda e: e["ts"])
        return uniq, oncpu_ts


def _match_conn_5tuple(ev, cip, cport, sip, sport):
    """检查 BPF 事件是否匹配连接五元组（cip:cport ↔ sip:sport）。

    TCP/sock 事件（有 local_ip/peer_ip）按双向匹配；网卡事件（有 src_ip/dst_ip）
    同法匹配；无 IP 的事件（sched 等）默认保留（调度上下文相关）。
    """
    if cip is None or sport is None:
        return True
    # TCP / sock 事件
    if ev.get("local_ip"):
        return ((ev["local_ip"] == cip and ev["local_port"] == cport
                 and ev["peer_ip"] == sip and ev["peer_port"] == sport)
                or (ev["local_ip"] == sip and ev["local_port"] == sport
                    and ev["peer_ip"] == cip and ev["peer_port"] == cport))
    # 网卡层事件
    if ev.get("src_ip"):
        return ((ev["src_ip"] == cip and ev["src_port"] == cport
                 and ev["dst_ip"] == sip and ev["dst_port"] == sport)
                or (ev["src_ip"] == sip and ev["src_port"] == sport
                    and ev["dst_ip"] == cip and ev["dst_port"] == cport))
    # 无 IP 信息的事件（sched 等）保留
    return True


def _thread_sched_trace(events, tid, win_lo, win_hi):
    """锚点线程的调度轨迹：[win_lo, win_hi] 内与该 tid 相关的 sched_* 事件。

    tid 为锚点行线程号；与 bpf sched 事件的 pid 同命名空间时精确命中
    （sched_switch 按 prev_pid/next_pid，waking/wakeup 按 pid），
    命中不了返回空列表（调用方降级）。输出按时间排序。
    """
    if not tid:
        return []
    try:
        t = int(tid)
    except (TypeError, ValueError):
        return []
    out = []
    for e in events:
        k = e.get("kind", "")
        if not k.startswith("sched"):
            continue
        hit = ((k == "sched_switch" and (e.get("prev_pid") == t or e.get("next_pid") == t))
               or (k in ("sched_waking", "sched_wakeup") and e.get("pid") == t))
        if hit and win_lo <= e["ts"] <= win_hi:
            out.append(e)
    out.sort(key=lambda e: e["ts"])
    return out


def _coroutine_evidence(ctx):
    """协程迁移与 CPU 一致性证据（基于锚点 cpu/bid 与 bpf 事件 cpu）。

    写 ctx.coro_evidence（list[str]）与 ctx.migration（dict 或 None）。
    仅依赖锚点字段与已关联的内核事件，任一缺失时对应项自动跳过。
    """
    cs = ctx.anchors.get("ClientSend")
    sr = ctx.anchors.get("ServerRecv")
    ss = ctx.anchors.get("ServerSend")

    # 1) 协程跨线程迁移：同 bid 不同 tid/cpu（bthread yield 后被另一 worker 线程 resume）
    if sr and ss and sr.get("bid") and sr.get("bid") == ss.get("bid"):
        if sr.get("tid") != ss.get("tid") or sr.get("cpu") != ss.get("cpu"):
            ctx.migration = {
                "bid": sr["bid"],
                "recv_tid": sr.get("tid"), "recv_cpu": sr.get("cpu"),
                "send_tid": ss.get("tid"), "send_cpu": ss.get("cpu"),
            }
            ctx.coro_evidence.append(
                "协程 bid=%s 处理期间发生跨线程迁移：ServerRecv(tid %s, cpu %s) → "
                "ServerSend(tid %s, cpu %s)，协程 yield 后被其他 worker 线程 resume"
                % (sr["bid"], sr.get("tid"), sr.get("cpu"),
                   ss.get("tid"), ss.get("cpu")))

    ms = ctx.milestones
    # 2) 发送路径 CPU 交叉：用户态发送 cpu vs 内核 tcp send 入口 cpu
    if cs and cs.get("cpu") and "ClientTcpSendIn" in ms:
        for ev in ctx.kernel_events["client"]:
            if ev["kind"] == "tcp_send_in" and ev.get("ts") == ms["ClientTcpSendIn"] \
                    and ev.get("cpu") is not None and str(ev["cpu"]) != cs["cpu"]:
                ctx.coro_evidence.append(
                    "client 用户态发送在 cpu %s，内核 tcp send 入口在 cpu %s"
                    "（发送路径跨核）" % (cs["cpu"], ev["cpu"]))
                break

    # 3) 收包软中断 cpu vs 业务协程执行 cpu
    if sr and sr.get("cpu") and "ServerTcpRecvFirst" in ms:
        for ev in ctx.kernel_events["server"]:
            if ev["kind"] == "tcp_recv_in" and ev.get("ts") == ms["ServerTcpRecvFirst"] \
                    and ev.get("cpu") is not None and str(ev["cpu"]) != sr["cpu"]:
                ctx.coro_evidence.append(
                    "server 收包软中断在 cpu %s，业务协程 ServerRecv 执行在 cpu %s"
                    "（跨核收包，关注 NUMA/缓存亲和性）" % (ev["cpu"], sr["cpu"]))
                break


# ── 前序协程执行轨迹（client/server 双侧） ────────────────────────────────────

_server_anchors_cache = {}  # server_path -> [(ts, kind, tid, cpu, bid, trace_id, raw), ...]
_client_anchors_cache = {}  # client_path -> 同上


def _scan_side_all_anchors(path, cache, recv_re, send_re, recv_kind, send_kind):
    """扫描单侧日志文件，收集全部收/发锚点（含原始行）。

    返回按 ts 排序的列表，每项为 (ts, kind, tid, cpu, bid, trace_id, raw)。
    结果缓存于 cache，同文件只扫描一次。
    """
    if not path:
        return []
    sp = str(path)
    if sp in cache:
        return cache[sp]
    anchors = []
    try:
        for _, line in iter_marker_lines(
                [Path(path)], [recv_kind.encode(), send_kind.encode()], verbose=False):
            info = parse_info_line(line)
            if not info:
                continue
            m = recv_re.search(info["msg"])
            if m:
                anchors.append((info["ts"], recv_kind,
                                m.group(2), m.group("cpu"), m.group("bid"),
                                info["trace"], info["raw"]))
                continue
            m = send_re.search(info["msg"])
            if m:
                anchors.append((info["ts"], send_kind,
                                m.group(2), m.group("cpu"), m.group("bid"),
                                info["trace"], info["raw"]))
    except OSError:
        return anchors
    anchors.sort(key=lambda x: x[0])
    cache[sp] = anchors
    return anchors


def _scan_server_all_anchors(server_path):
    """server 日志全部 ServerRecv/ServerSend 锚点（含原始行），按 ts 升序。"""
    return _scan_side_all_anchors(server_path, _server_anchors_cache,
                                  SERVER_RECV_RE, SERVER_SEND_RE,
                                  "ServerRecv", "ServerSend")


def _scan_client_all_anchors(client_path):
    """client 日志全部 ClientRecv/ClientSend 锚点（含原始行），按 ts 升序。"""
    return _scan_side_all_anchors(client_path, _client_anchors_cache,
                                  CLIENT_RECV_RE, CLIENT_SEND_RE,
                                  "ClientRecv", "ClientSend")


def _preceding_trace_rows(all_anchors, tid, cur_kind, cur_ts, win_start,
                          max_rows=20):
    """前序协程轨迹明细行：窗口内同 tid 的前序锚点行 + 当前锚点行。

    窗口内无前序行时回看该 tid 最近 2 条（< cur_ts）补充上下文；
    前序行上限 max_rows-1 条，当前锚点行始终为末行（kind 加 "▶" 前缀），
    整体时间升序。
    """
    same = [a for a in all_anchors if a[2] == tid]
    prior = [a for a in same if a[0] < cur_ts]
    rows = [a for a in prior if a[0] >= win_start]
    if not rows:
        rows = prior[-2:]
    cur = [a for a in same if a[1] == cur_kind and a[0] == cur_ts]
    rows = rows[-(max_rows - 1):] + cur
    out = []
    for a in rows:
        ts, kind, t, cpu, bid, trace_id, raw = a
        out.append((ts, ("▶ " + kind) if (cur and a is cur[0]) else kind,
                    t, cpu, bid, trace_id, raw))
    return out


def _preceding_coroutine_evidence(ctx, side):
    """协程调度排队证据（双侧共用）：前序协程执行轨迹 + latency_warn 关联。

    触发条件（>1ms）：
      - server 侧优先用线程上 CPU 时刻（bpf sched 推导）→ ServerRecv；
        无 sched 事件时降级用协议栈收包里程碑 ServerTcpRecvFirst → ServerRecv；
      - client 侧用 ClientTcpRecvFirst → ClientRecv（协议栈已收包但协程很晚执行）。
    触发后在对应侧日志中查找同一 tid 上最近的前序协程锚点作为阻塞证据，
    检查 latency_warn_log 中窗口内的长时间运行任务告警，
    并收集窗口内同 tid 锚点原始行为轨迹明细（ctx.preceding_trace_lines）。
    """
    recv_key = "ServerRecv" if side == "server" else "ClientRecv"
    send_key = "ServerSend" if side == "server" else "ClientSend"
    first_ms_key = "ServerTcpRecvFirst" if side == "server" else "ClientTcpRecvFirst"
    anchor = ctx.anchors.get(recv_key)
    if not anchor or not anchor.get("tid"):
        return
    cur_ts = anchor["ts"]

    # 触发窗口起点：优先线程上 CPU（server 侧）；不可得时降级用协议栈收包里程碑
    win_start, win_label, degraded = None, "", False
    oncpu = ctx.thread_oncpu_ts if side == "server" else None
    if oncpu and oncpu < cur_ts:
        win_start, win_label = oncpu, "上 CPU 后"
    ms_first = ctx.milestones.get(first_ms_key)
    if win_start is None:
        if not ms_first or ms_first >= cur_ts:
            return
        win_start = ms_first
        win_label = "协议栈收包后"
        degraded = True

    pickup_us = (cur_ts - win_start).total_seconds() * 1e6
    if pickup_us <= 1000:  # 阈值 1ms
        return

    tid = anchor["tid"]
    if side == "server":
        all_anchors = _scan_server_all_anchors(ctx.server_log_path)
    else:
        all_anchors = _scan_client_all_anchors(ctx.slow.log_path)

    # 查找同一 tid 上，当前锚点之前最近的前序协程
    prev_send = None
    prev_recv = None
    for ts, kind, t, cpu, bid, trace_id, raw in all_anchors:
        if t != tid:
            continue
        if ts >= cur_ts:
            break
        if kind == send_key:
            prev_send = (ts, bid, cpu, trace_id)
        elif kind == recv_key:
            prev_recv = (ts, bid, cpu, trace_id)

    deg_note = "（无 sched 事件，按协议栈收包→协程执行降级判定）" if degraded else ""
    if prev_send and prev_send[0] > win_start:
        # 前序协程的发送锚点在窗口起点之后 → 前序协程阻塞了当前协程
        gap_us = (cur_ts - prev_send[0]).total_seconds() * 1e6
        ctx.coro_evidence.append(
            "前序协程 bid=%s (trace=%s) 在同一 tid %s cpu %s 上执行，"
            "%s=%s，距当前协程%s %s 才执行（阻塞 %s），"
            "协程调度排队 %s —— 前序协程执行轨迹阻塞%s"
            % (prev_send[1], prev_send[3], tid, prev_send[2],
               send_key, prev_send[0].isoformat(), win_label,
               fmt_us(pickup_us), fmt_us(gap_us), fmt_us(pickup_us), deg_note))
    elif prev_recv:
        gap_us = (cur_ts - prev_recv[0]).total_seconds() * 1e6
        ctx.coro_evidence.append(
            "前序协程 bid=%s (trace=%s) 在同一 tid %s cpu %s 上，"
            "%s=%s（未找到 %s，可能仍在执行中），"
            "距当前协程%s %s（协程调度排队 %s）%s"
            % (prev_recv[1], prev_recv[3], tid, prev_recv[2],
               recv_key, prev_recv[0].isoformat(), send_key,
               win_label, fmt_us(pickup_us), fmt_us(pickup_us), deg_note))

    # latency_warn_log 关联：检查窗口内是否有长时间运行任务告警
    side_tag = "" if side == "server" else "（client）"
    warns = ctx.warn_events.get(side) or []
    for w in warns:
        if w.get("ts") and win_start <= w["ts"] <= cur_ts:
            ctx.coro_evidence.append(
                "latency_warn 告警%s：cpu %s comm=%s pid=%s latency=%s us，"
                "时间 %s 在协程调度排队窗口内"
                % (side_tag, w.get("cpu"), w.get("comm"), w.get("pid"),
                   w.get("latency_us"), w["ts"].isoformat()))

    # 轨迹明细行：窗口内同 tid 锚点原始行（时间升序，当前锚点 ▶ 标记）
    ctx.preceding_trace_lines[side] = _preceding_trace_rows(
        all_anchors, tid, recv_key, cur_ts, win_start)


def _server_preceding_coroutine_evidence(ctx):
    """server 侧前序协程证据（兼容入口，内部委托双侧共用实现）。"""
    _preceding_coroutine_evidence(ctx, "server")


def correlate_kernel(ctx, kernel_results, window_net_results=None):
    """把预扫描的 bpf 窗口事件关联到 ctx（替代旧的整文件解析+过滤）。

    window_net_results：BpfScanner 的窗口全景桶（窗口内全部连接的
    tcp/nic/sock 事件，不限 IP），用于构建问题窗口全景视图。
    """
    cs = ctx.anchors.get("ClientSend")
    cr = ctx.anchors.get("ClientRecv")
    if not cs or not cr:
        ctx.missing.append("缺少 ClientSend/ClientRecv 锚点，跳过内核日志关联")
        return
    if not ctx.client_node:
        ctx.missing.append("client pod 目录 %s 无法映射到 bpf 节点" % ctx.client_pod_dir)
    if ctx.server_pod_dir and not ctx.server_node:
        ctx.missing.append("worker pod 目录 %s 无法映射到 bpf 节点" % ctx.server_pod_dir)
    cip, sip = ctx.client_ip, ctx.server_ip
    if not cip or not sip:
        ctx.missing.append("缺少 client/server pod IP，跳过内核日志关联")
        return

    # 数据层统一按时间排序：HTML 事件表 / JSON / raw 汇总输出顺序一致，
    # 且 _fill_milestone 的"first 出现即定格"语义变为"时间最早"（更准确）
    ctx.kernel_events["client"] = sorted(
        kernel_results.get((ctx.idx, "client"), []), key=lambda e: e["ts"])
    ctx.kernel_events["server"] = sorted(
        kernel_results.get((ctx.idx, "server"), []), key=lambda e: e["ts"])

    # identify connection 4-tuple from client-side tcp send-in after ClientSend
    ctx.conn = BpfCorrelator._identify_conn(ctx.kernel_events["client"], cs["ts"], cip, sip)
    if not ctx.conn:
        ctx.missing.append("client 节点 bpf 日志未找到 ClientSend 后 %s→%s 的 tcp send 事件"
                           % (cip, sip))
        return
    _, cport, _, sport = ctx.conn

    # 五元组过滤：仅保留与当前连接匹配的内核事件用于展示（TCP/NIC 事件按连接过滤，
    # sched 等无IP事件保留；里程碑填充仍用全量 kernel_events）
    ctx.filtered_events = {}
    for side in ("client", "server"):
        ctx.filtered_events[side] = [
            e for e in ctx.kernel_events[side]
            if _match_conn_5tuple(e, cip, cport, sip, sport)
        ]

    # 问题窗口全景事件：窗口内全部连接的 tcp/nic/sock 事件（不限 IP，含其他
    # pod 的流量）+ sched 类事件，按时间排序并标注是否属于问题连接五元组
    # （match5t：True 问题连接 / False 其他连接 / None 无 IP 的调度类事件）
    for side in ("client", "server"):
        net_evs = (window_net_results or {}).get((ctx.idx, side), [])
        sched_evs = [e for e in ctx.kernel_events[side]
                     if e["kind"].startswith("sched")
                     or e["kind"] == "tcpwakeup_out"]
        merged = sorted((dict(e) for e in net_evs + sched_evs),
                        key=lambda e: e["ts"])
        for e in merged:
            if e.get("local_ip") or e.get("src_ip"):
                e["match5t"] = bool(_match_conn_5tuple(e, cip, cport, sip, sport))
            else:
                e["match5t"] = None
        ctx.bpf_window_events[side] = merged

    # milestones from kernel events for this connection
    ms = ctx.milestones
    for ev in ctx.kernel_events["client"]:
        BpfCorrelator._fill_milestone(ms, ev, cip, cport, sip, sport, "Client")
    for ev in ctx.kernel_events["server"]:
        BpfCorrelator._fill_milestone(ms, ev, cip, cport, sip, sport, "Server")

    # wakeup-chain evidence: client side, sock_readable/sched events in
    # [first client tcp recv event, ClientRecv]
    ctx.wakeup_chain = BpfCorrelator._wakeup_chain(
        ctx.kernel_events["client"], ms, cr["ts"], cip, cport, sip, sport)

    # server 侧唤醒链：内核收包 → ServerRecv（协程开始执行）
    sr = ctx.anchors.get("ServerRecv")
    if sr:
        ctx.server_wakeup_chain, ctx.thread_oncpu_ts = \
            BpfCorrelator._server_wakeup_chain(
                ctx.kernel_events["server"], ms, sr["ts"],
                cip, cport, sip, sport, sr.get("tid"))

    # 关键线程调度轨迹：各锚点 tid 在 bpf sched 事件中的上下 CPU 轨迹
    # （锚点 tid 与 host pid 命名空间不一致时自动为空，不影响主流程）
    pad = timedelta(milliseconds=DEFAULT_SCHED_PAD_MS)
    for name, anchor, side in (
            ("ClientSend", cs, "client"), ("ClientRecv", cr, "client"),
            ("ServerRecv", sr, "server"),
            ("ServerSend", ctx.anchors.get("ServerSend"), "server")):
        if anchor and anchor.get("tid"):
            ctx.thread_traces[name] = _thread_sched_trace(
                ctx.kernel_events[side], anchor["tid"],
                anchor["ts"] - pad, anchor["ts"] + pad)
        else:
            ctx.thread_traces[name] = []

    # 唤醒链兼容：bpf 已采集 tcp 事件但无任何 sched_* 事件（该类采集可能已
    # 关闭）——唤醒链证据缺失但不影响内核时间线重建，注明后继续
    if ctx.kernel_events["client"] and not any(
            e["kind"].startswith("sched") for e in ctx.kernel_events["client"]):
        ctx.missing.append("bpf 日志中无 sched_waking/wakeup/switch 唤醒链事件"
                           "（该类采集可能已关闭），无法重建内核→用户态唤醒链")


def build_kernel_segments(ctx):
    ms = ctx.milestones
    f = ctx.slow.fields
    for key, m_start, m_end, thr, cat, desc in SEGMENT_DEFS:
        if m_start not in ms or m_end not in ms:
            continue
        dur = (ms[m_end] - ms[m_start]).total_seconds() * 1e6
        if dur < 0:
            continue
        seg = {"key": key, "start": m_start, "end": m_end, "dur_us": dur,
               "threshold_us": thr, "category": cat, "desc": desc,
               "abnormal": bool(thr is not None and dur > thr)}
        if key == "server_processing":
            try:
                q = int(f.get("server_req_queue_us", "0"))
                e = int(f.get("server_exec_us", "0"))
            except ValueError:
                q, e = 0, 0
            seg["brpc_queue_exec_us"] = q + e
            seg["abnormal"] = dur > max(500, 2 * (q + e))
        ctx.kernel_segments.append(seg)


def _server_pickup_segments(ctx):
    """server 收包→协程执行 细分分段（证据性，threshold=None 不参与异常竞争）。

    server_recvq_to_readable : ServerTcpRecvQue → ServerSockReadable 协议栈收包排队→唤醒
    server_readable_to_oncpu : ServerSockReadable → 线程上 CPU        内核唤醒+线程调度等待
    server_oncpu_to_user     : 线程上 CPU → ServerRecv                 协程调度排队（bthread 等待）
    任一端点缺失则跳过该子段。
    """
    ms = ctx.milestones
    sr = ctx.anchors.get("ServerRecv")
    if not sr:
        return
    oncpu = ctx.thread_oncpu_ts
    pts = [
        ("server_recvq_to_readable", ms.get("ServerTcpRecvQue"),
         ms.get("ServerSockReadable"), "协议栈收包排队→可读唤醒"),
        ("server_readable_to_oncpu", ms.get("ServerSockReadable"), oncpu,
         "内核唤醒→线程上 CPU（调度等待）"),
        ("server_oncpu_to_user", oncpu, sr["ts"], "线程上 CPU→协程执行（协程调度排队）"),
    ]
    for key, lo, hi, desc in pts:
        if lo is None or hi is None or hi < lo:
            continue
        start_name, _, end_name = {
            "server_recvq_to_readable": ("ServerTcpRecvQue", None, "ServerSockReadable"),
            "server_readable_to_oncpu": ("ServerSockReadable", None, "ThreadOnCpu"),
            "server_oncpu_to_user": ("ThreadOnCpu", None, "ServerRecv"),
        }[key]
        ctx.kernel_segments.append({
            "key": key, "start": start_name, "end": end_name,
            "dur_us": (hi - lo).total_seconds() * 1e6,
            "threshold_us": None, "category": "coroutine_pickup_evidence",
            "desc": desc, "abnormal": False, "evidence": True,
            "_start_ts": lo, "_end_ts": hi,
        })


def _nic_segments(ctx):
    """网卡层证据分段（evidence=True，不参与异常竞争）+ TCP 重传证据。

    client_stack_to_nic : ClientTcpSendIn → ClientDevStartXmit 协议栈发送处理（含 qdisc 排队）
    client_nic_xmit     : ClientDevStartXmit → ClientNetDevXmit 驱动发送耗时
    server_nic_to_stack : ServerNetifRx → ServerTcpRecvFirst 网卡收包→协议栈交付（含 veth 转发/排队/唤醒）
    client_nic_to_stack : ClientNetifRx → ClientTcpRecvFirst client 侧同上
    任一端点缺失则跳过。窗口内 TCP 重传写入 ctx.nic_evidence。
    """
    ms = ctx.milestones
    pts = [
        ("client_stack_to_nic", ms.get("ClientTcpSendIn"),
         ms.get("ClientDevStartXmit"), "client 协议栈发送→驱动（含 qdisc 排队）"),
        ("client_nic_xmit", ms.get("ClientDevStartXmit"),
         ms.get("ClientNetDevXmit"), "client 驱动发送耗时"),
        ("server_nic_to_stack", ms.get("ServerNetifRx"),
         ms.get("ServerTcpRecvFirst"),
         "server 网卡收包→协议栈交付（含 veth 转发/排队/唤醒）"),
        ("client_nic_to_stack", ms.get("ClientNetifRx"),
         ms.get("ClientTcpRecvFirst"),
         "client 网卡收包→协议栈交付（含 veth 转发/排队/唤醒）"),
    ]
    for key, lo, hi, desc in pts:
        if lo is None or hi is None or hi < lo:
            continue
        start_name = {"client_stack_to_nic": "ClientTcpSendIn",
                      "client_nic_xmit": "ClientDevStartXmit",
                      "server_nic_to_stack": "ServerNetifRx",
                      "client_nic_to_stack": "ClientNetifRx"}[key]
        end_name = {"client_stack_to_nic": "ClientDevStartXmit",
                    "client_nic_xmit": "ClientNetDevXmit",
                    "server_nic_to_stack": "ServerTcpRecvFirst",
                    "client_nic_to_stack": "ClientTcpRecvFirst"}[key]
        ctx.kernel_segments.append({
            "key": key, "start": start_name, "end": end_name,
            "dur_us": (hi - lo).total_seconds() * 1e6,
            "threshold_us": None, "category": "nic_evidence",
            "desc": desc, "abnormal": False, "evidence": True,
            "_start_ts": lo, "_end_ts": hi,
        })

    # TCP 重传证据：窗口内该连接的重传事件（已在收集层按 IP 过滤）
    rets = {"client": [], "server": []}
    for side in ("client", "server"):
        rets[side] = [e for e in ctx.kernel_events.get(side) or []
                      if e["kind"] == "tcp_retransmit"]
    total = len(rets["client"]) + len(rets["server"])
    if total:
        ctx.nic_evidence.append(
            "窗口内检测到 %d 次 TCP 重传（client 侧 %d / server 侧 %d），"
            "存在丢包/网络质量问题" % (total, len(rets["client"]), len(rets["server"])))
        for e in (rets["client"] + rets["server"])[:3]:
            ctx.nic_evidence.append(
                "  重传样例：seq=%s len=%s dev_tid=%s（%s）"
                % (e.get("seq"), e.get("size"), e.get("tid"), e["raw"][:80]))


def _phys_dir_events(events, src_ip, src_port, dst_ip, dst_port, start):
    """指定方向（src→dst 四元组）、ts≥start 的网卡层事件（时间升序）。"""
    return sorted(
        (e for e in events
         if e["kind"].startswith("nic_")
         and e.get("src_ip") == src_ip and e.get("src_port") == src_port
         and e.get("dst_ip") == dst_ip and e.get("dst_port") == dst_port
         and e["ts"] >= start),
        key=lambda e: e["ts"])


def _phys_wire_evidence(ctx):
    """物理网卡间线路定界（seq 关联）：界定 ServerSend→ClientRecv 耗时长是否由
    物理网卡间传输（网卡处理/物理线路）导致。

    k8s 容器网络一次发送经过多个虚拟网卡（eth0 → cali* → 物理网卡），同一报文
    用 seq 关联两侧链路：
      - 发送侧物理网卡发出 = 同 seq 链上最后一个 nic_dev_xmit（单网卡时即唯一）；
      - 接收侧物理网卡收到 = 同 seq 链上第一个 nic_rx_skb；
      - wire_us = 收到 − 发出；与线路段（TcpSendIn → 对端 TcpRecvFirst）对比得
        占比，并分解两侧节点内耗时（排除性证据）。
    任一端缺失则该方向置 None（旧版 bpf 日志无 nic 点位时自动跳过）；
    s2c / c2s 双向对称。
    """
    if not ctx.conn:
        return
    cip, cport, sip, sport = ctx.conn
    ms = ctx.milestones
    for direction in ("s2c", "c2s"):
        if direction == "s2c":
            sender, receiver = "server", "client"
            send_src, send_dst = (sip, sport), (cip, cport)
            send_in_key, recv_first_key = "ServerTcpSendIn", "ClientTcpRecvFirst"
            fallback_anchor = "ServerSend"
            ms_egress, ms_ingress = "ServerPhysNicXmit", "ClientPhysNicRx"
            seg_key = "wire_s2c_phys"
            seg_desc = "server 物理网卡发出 → client 物理网卡收到（seq 关联线路定界）"
        else:
            sender, receiver = "client", "server"
            send_src, send_dst = (cip, cport), (sip, sport)
            send_in_key, recv_first_key = "ClientTcpSendIn", "ServerTcpRecvFirst"
            fallback_anchor = "ClientSend"
            ms_egress, ms_ingress = "ClientPhysNicXmit", "ServerPhysNicRx"
            seg_key = "wire_c2s_phys"
            seg_desc = "client 物理网卡发出 → server 物理网卡收到（seq 关联线路定界）"
        # 起点：发送侧协议栈发送入口（缺失时降级用业务发送锚点）
        start = ms.get(send_in_key)
        if start is None:
            anchor = ctx.anchors.get(fallback_anchor)
            if not anchor:
                continue
            start = anchor["ts"]
        send_evs = _phys_dir_events(ctx.kernel_events.get(sender) or [],
                                    send_src[0], send_src[1], send_dst[0], send_dst[1],
                                    start)
        if not send_evs:
            continue
        # 关联 seq：发送起点后第一条 xmit 事件（本次发送的首包）
        first_xmit = next((e for e in send_evs
                           if e["kind"] in ("nic_dev_xmit_start", "nic_dev_xmit")), None)
        if not first_xmit or "seq" not in first_xmit:
            continue
        seq = first_xmit["seq"]
        egress_chain = [e for e in send_evs if e.get("seq") == seq]
        xmits = [e for e in egress_chain if e["kind"] == "nic_dev_xmit"]
        if not xmits:
            continue
        egress = xmits[-1]  # 物理网卡发出 = 同 seq 最后一个 xmit
        recv_evs = _phys_dir_events(ctx.kernel_events.get(receiver) or [],
                                    send_src[0], send_src[1], send_dst[0], send_dst[1],
                                    start)
        ingress_chain = [e for e in recv_evs if e.get("seq") == seq]
        rxs = [e for e in ingress_chain if e["kind"] == "nic_rx_skb"]
        if not rxs:
            continue
        ingress = rxs[0]  # 物理网卡收到 = 同 seq 第一个 rx
        if ingress["ts"] < egress["ts"]:
            continue  # 跨节点时钟异常保护
        send_in_ts = ms.get(send_in_key)
        recv_first = ms.get(recv_first_key)
        wire_us = (ingress["ts"] - egress["ts"]).total_seconds() * 1e6
        egress_internal_us = ((egress["ts"] - send_in_ts).total_seconds() * 1e6
                              if send_in_ts and egress["ts"] >= send_in_ts else None)
        ingress_internal_us = ((recv_first - ingress["ts"]).total_seconds() * 1e6
                               if recv_first and recv_first >= ingress["ts"] else None)
        line_us = ((recv_first - send_in_ts).total_seconds() * 1e6
                   if send_in_ts and recv_first and recv_first >= send_in_ts else None)
        share_pct = (wire_us / line_us * 100.0) if line_us and line_us > 0 else None
        dominant = bool(wire_us > PHYS_WIRE_MIN_US
                        and share_pct is not None and share_pct >= PHYS_WIRE_SHARE_PCT)
        ctx.phys_wire[direction] = {
            "seq": seq, "len": first_xmit.get("len"),
            "egress_side": sender, "egress_dev": egress.get("dev"),
            "egress_ts": egress["ts"],
            "ingress_side": receiver, "ingress_dev": ingress.get("dev"),
            "ingress_ts": ingress["ts"],
            "wire_us": wire_us,
            "egress_internal_us": egress_internal_us,
            "ingress_internal_us": ingress_internal_us,
            "line_us": line_us, "share_pct": share_pct, "dominant": dominant,
            "egress_chain": egress_chain, "ingress_chain": ingress_chain,
        }
        ms.setdefault(ms_egress, egress["ts"])
        ms.setdefault(ms_ingress, ingress["ts"])
        ctx.kernel_segments.append({
            "key": seg_key, "start": ms_egress, "end": ms_ingress,
            "dur_us": wire_us, "threshold_us": None, "category": "nic_evidence",
            "desc": seg_desc, "abnormal": False, "evidence": True,
            "_start_ts": egress["ts"], "_end_ts": ingress["ts"],
        })
        line_lbl = {"s2c": "server→client", "c2s": "client→server"}[direction]
        share_txt = ("（占 %s 线路段 %.1f%%）" % (line_lbl, share_pct)
                     if share_pct is not None else "")
        verdict = ("—— 耗时主要在物理网卡间传输（网卡处理/物理线路），两侧节点内耗时已排除"
                   if dominant else
                   "—— 线路段耗时主要在节点内（veth/协议栈），非物理网卡间线路")
        ctx.nic_evidence.append(
            "物理网卡间定界（seq=%s，%s）：%s 物理网卡 %s 发出（%s）→ %s 物理网卡 %s 收到（%s），"
            "线路耗时 %s%s；%s 节点内（协议栈→物理网卡，含 veth 转发）%s + %s 节点内（物理网卡→协议栈）%s %s"
            % (seq, line_lbl,
               sender, egress.get("dev"), fmt_dt(egress["ts"]),
               receiver, ingress.get("dev"), fmt_dt(ingress["ts"]),
               fmt_us(wire_us), share_txt,
               sender, fmt_us(egress_internal_us),
               receiver, fmt_us(ingress_internal_us), verdict))


# ── 辅助日志关联与证据：irqoff / sar nic / brpc bthread ───────────────────────

def _side_recv_window(ctx, side):
    """该侧"收包→用户态取包"窗口（irqoff/bthread 关联用）。

    有该侧 tcp 收包里程碑时用 [TcpRecvFirst − AUX_RECV_PAD, Recv 锚点]；
    否则退化为整个 trace 窗口 [ClientSend, ClientRecv]。返回 None 表示无法确定。
    """
    cs, cr = ctx.anchors.get("ClientSend"), ctx.anchors.get("ClientRecv")
    if not (cs and cr):
        return None
    if side == "client":
        first = ctx.milestones.get("ClientTcpRecvFirst")
        last = ctx.anchors.get("ClientRecv")
    else:
        first = ctx.milestones.get("ServerTcpRecvFirst")
        last = ctx.anchors.get("ServerRecv")
    if first and last and last["ts"] >= first:
        return (first - timedelta(milliseconds=AUX_RECV_PAD_MS), last["ts"])
    return (cs["ts"], cr["ts"])


def _brpc_files_for_pod(brpc_by_pod, pod_dir):
    """pod 目录名（去 _N 后缀）→ 匹配的 brpc bthread 日志文件列表。"""
    base = re.sub(r"_\d+$", "", pod_dir or "")
    if not base:
        return []
    out = []
    for pod, path in sorted(brpc_by_pod.items()):
        if pod == base or base.startswith(pod + "-") or pod.startswith(base + "-"):
            out.append(path)
    return out


def _nic_window_samples(ctx, devs, pad):
    """该 trace 窗口内的 sar 样本（按 dev 分组，标注 dev 字段）。

    dev 优先取物理网卡定界（phys_wire）的收发 dev，无则全部 dev；
    样本为秒粒度，按"样本时刻覆盖 [S, S+1s) 区间"语义匹配窗口。
    """
    cs, cr = ctx.anchors.get("ClientSend"), ctx.anchors.get("ClientRecv")
    if not (cs and cr):
        return []
    win = (cs["ts"] - pad, cr["ts"] + pad)
    pw = getattr(ctx, "phys_wire", None) or {}
    prefer = []
    for d in ("s2c", "c2s"):
        info = pw.get(d)
        if info:
            for k in ("egress_dev", "ingress_dev"):
                if info.get(k):
                    prefer.append(info[k])
    ordered = [d for d in dict.fromkeys(prefer) if d in devs]
    ordered += [d for d in sorted(devs) if d not in ordered]
    out = []
    for dev in ordered:
        for s in _sar_in_window(devs[dev]["samples"], win[0], win[1]):
            out.append(dict(s, dev=dev))
    out.sort(key=lambda s: (s["hms"], s["dev"]))
    return out


def _irqoff_evidence(ctx):
    """irqoff 证据：问题窗口内存在关中断记录 → 可能延迟网卡收包/软中断处理。"""
    for side in ("client", "server"):
        evs = ctx.irqoff_events.get(side) or []
        if not evs:
            continue
        node = ctx.client_node if side == "client" else ctx.server_node
        hard = sum(1 for e in evs if e.get("irq") == "hardirq")
        soft = sum(1 for e in evs if e.get("irq") == "softirq")
        mx = max(evs, key=lambda e: e["latency_us"])
        ctx.nic_evidence.append(
            "%s 节点（%s）问题窗口内检测到 %d 条关中断记录（hardirq %d / softirq %d），"
            "最长：cpu %s 被 %s(pid %s) 关中断 %s（%s，%s）—— "
            "关中断会推迟该 cpu 上的网卡收包/软中断处理，可能是收包慢的直接原因"
            % (side, node or "?", len(evs), hard, soft,
               mx["cpu"], mx["comm"], mx["pid"], fmt_us(mx["latency_us"]),
               mx["irq"], fmt_dt(mx["ts"])))


def _nic_util_evidence(ctx):
    """sar 网卡利用率证据：高利用率提示带宽瓶颈；低利用率输出排除性证据。"""
    for side in ("client", "server"):
        samples = ctx.nic_samples.get(side) or []
        if not samples:
            continue
        node = ctx.client_node if side == "client" else ctx.server_node
        by_dev = {}
        for s in samples:
            by_dev.setdefault(s.get("dev") or "?", []).append(s)
        for dev, ss in sorted(by_dev.items()):
            mx = max(s["ifutil"] for s in ss)
            if mx >= NIC_HIGH_IFUTIL_PCT:
                ctx.nic_evidence.append(
                    "%s 节点（%s）网卡 %s 窗口内 sar 采样 %d 条，%%ifutil 峰值 %.1f%% —— "
                    "网卡利用率高，可能存在带宽/队列瓶颈"
                    % (side, node or "?", dev, len(ss), mx))
            elif mx < NIC_LOW_IFUTIL_PCT:
                ctx.nic_evidence.append(
                    "%s 节点（%s）网卡 %s 窗口内 sar 采样 %d 条，%%ifutil 峰值 %.1f%% —— "
                    "排除网卡带宽打满（问题非网卡利用率导致）"
                    % (side, node or "?", dev, len(ss), mx))


def _bthread_evidence(ctx):
    """bthread 证据：收包窗口内协程创建/排队统计，佐证"协议栈收包后业务执行晚"。"""
    for side in ("client", "server"):
        evs = ctx.bthread_events.get(side) or []
        if not evs:
            continue
        anchor = ctx.anchors.get("ClientRecv" if side == "client" else "ServerRecv")
        tid = (anchor or {}).get("tid")
        created = [e for e in evs if e["kind"] == "created"]
        sched = [e for e in evs if e["kind"] == "scheduled"]
        ptus = [e["pending_time_us"] for e in sched
                if e.get("pending_time_us") is not None]
        tpts = [e["target_pending_tasks"] for e in created
                if e.get("target_pending_tasks") is not None]
        seg = []
        if created:
            seg.append("创建 %d 个协程" % len(created))
        if sched:
            seg.append("首次调度 %d 个" % len(sched))
        if ptus:
            seg.append("pending_time_us 峰值 %dus" % max(ptus))
        if tpts:
            seg.append("target_pending_tasks 峰值 %d" % max(tpts))
        tid_txt = ("线程 tid=%s " % tid) if tid is not None else ""
        ctx.coro_evidence.append(
            "%s 侧%s收包窗口内 bthread 活动：%s —— 协程等待 worker 线程执行，"
            "佐证协议栈收包后业务执行晚" % (side, tid_txt, "，".join(seg)))


def _ev_conn_str(e):
    """bpf 事件的连接串（TCP/sock: local arrow peer；网卡: src -> dst）。"""
    if e.get("local_ip"):
        return "%s:%s %s %s:%s" % (e["local_ip"], e.get("local_port"),
                                   e.get("dir_arrow") or "<->",
                                   e["peer_ip"], e.get("peer_port"))
    if e.get("src_ip"):
        return "%s:%s -> %s:%s" % (e["src_ip"], e.get("src_port"),
                                   e["dst_ip"], e.get("dst_port"))
    return "?"


def _cpu_busy_analysis(ctx):
    """问题窗口 CPU 侵占分析（收包后业务处理开始晚 → 软中断抢占定界）。

    对本侧"内核收包 → 用户态取包"段异常（kernel_to_user 段超阈值）的侧：
      1. 问题窗口 = [本侧收包开始（TcpRecvFirst / NetifRx 较早者），Recv 锚点]，
         取窗口全景事件（全部连接，不限 IP）高亮问题五元组，统计其他请求穿插；
      2. 判断业务线程（Recv 锚点 tid）所在 cpu 上窗口内是否在处理其他请求
         （其他连接的收包/协议栈事件、业务线程被 sched_switch 切出），
         给出"线程被收包软中断抢占"的佐证或排除性结论。

    结果写入 ctx.cpu_busy[side]（渲染/JSON 用）与 ctx.cpu_evidence（结论证据）；
    发现抢占证据时置 ctx.cpu_busy_preempt = True。
    """
    for side, seg_key, first_ms, netif_ms, anchor_name in (
            ("client", "client_kernel_to_user", "ClientTcpRecvFirst",
             "ClientNetifRx", "ClientRecv"),
            ("server", "server_kernel_to_user", "ServerTcpRecvFirst",
             "ServerNetifRx", "ServerRecv")):
        seg = next((s for s in ctx.kernel_segments if s["key"] == seg_key), None)
        if not seg or not seg.get("abnormal"):
            continue
        anchor = ctx.anchors.get(anchor_name)
        if not anchor:
            continue
        win_start = ctx.milestones.get(first_ms) or ctx.milestones.get(seg["start"])
        netif = ctx.milestones.get(netif_ms)
        if netif and win_start and netif < win_start:
            win_start = netif
        win_end = anchor["ts"]
        if not win_start or win_end < win_start:
            continue

        node = ctx.client_node if side == "client" else ctx.server_node
        cpu_val = None
        if anchor.get("cpu"):
            try:
                cpu_val = int(anchor["cpu"])
            except (TypeError, ValueError):
                cpu_val = None
        anchor_tid = anchor.get("tid")
        try:
            anchor_tid_i = int(anchor_tid) if anchor_tid is not None else None
        except (TypeError, ValueError):
            anchor_tid_i = None

        win_evs = [e for e in (ctx.bpf_window_events.get(side) or [])
                   if win_start <= e["ts"] <= win_end]
        mine = [e for e in win_evs if e.get("match5t")]
        others = [e for e in win_evs if e.get("match5t") is False]
        other_conns = {}
        for e in others:
            key = _ev_conn_str(e)
            other_conns[key] = other_conns.get(key, 0) + 1
        other_by_cpu = {}
        for e in others:
            if e.get("cpu") is not None:
                other_by_cpu[e["cpu"]] = other_by_cpu.get(e["cpu"], 0) + 1
        other_on_cpu, switches_on_cpu, switched_out = [], [], []
        if cpu_val is not None:
            other_on_cpu = [e for e in others if e.get("cpu") == cpu_val]
            switches_on_cpu = [e for e in win_evs if e["kind"] == "sched_switch"
                               and e.get("cpu") == cpu_val]
            switched_out = [e for e in switches_on_cpu
                            if anchor_tid_i is not None
                            and e.get("prev_pid") == anchor_tid_i]
        preempt = bool(other_on_cpu or switched_out)
        if preempt:
            ctx.cpu_busy_preempt = True

        dur_us = (win_end - win_start).total_seconds() * 1e6
        win_txt = "%s ~ %s" % (fmt_dt(win_start), fmt_dt(win_end))
        conn_txt = "%s:%s <-> %s:%s" % tuple(ctx.conn) if ctx.conn else "?"

        # 证据 1：cpu 上其他请求处理情况（用户问题："cpu 上是否有在处理其他请求"）
        if cpu_val is None:
            ctx.cpu_evidence.append(
                "%s 节点（%s）问题窗口 [%s，%s] 内共 %d 条内核事件"
                "（问题连接 %d 条 / 其他连接 %d 条），但 %s 锚点行无 cpu 字段，"
                "无法定位业务线程所在 cpu 做软中断抢占分析（见问题窗口全景表）"
                % (side, node or "?", win_txt, fmt_us(dur_us), len(win_evs),
                   len(mine), len(others), anchor_name))
        elif other_on_cpu:
            on_cpu_conns = {}
            for e in other_on_cpu:
                on_cpu_conns[_ev_conn_str(e)] = on_cpu_conns.get(_ev_conn_str(e), 0) + 1
            sample = "、".join("%s×%d" % (k, v)
                               for k, v in sorted(on_cpu_conns.items(),
                                                  key=lambda kv: -kv[1])[:3])
            ctx.cpu_evidence.append(
                "%s 节点（%s）问题窗口 [%s，%s] 内，业务线程（tid %s）所在 cpu %s 上"
                "检测到 %d 条其他连接的收包/协议栈事件（来自 %d 个其他连接：%s）—— "
                "收包软中断在该 cpu 处理其他请求，业务线程可能被软中断抢占，"
                "推迟了取包/业务处理开始时间"
                % (side, node or "?", win_txt, fmt_us(dur_us), anchor_tid, cpu_val,
                   len(other_on_cpu), len(on_cpu_conns), sample))
        else:
            dist_txt = ("，其他连接事件分布在 cpu %s"
                        % "、".join("#%s×%d" % (c, n)
                                    for c, n in sorted(other_by_cpu.items(),
                                                       key=lambda kv: -kv[1])[:4])
                        if other_by_cpu else "，窗口内未发现其他连接的内核事件")
            ctx.cpu_evidence.append(
                "%s 节点（%s）问题窗口 [%s，%s] 内，业务线程（tid %s）所在 cpu %s 上"
                "未发现其他连接的收包/协议栈事件%s —— 该 cpu 被收包软中断"
                "处理其他请求抢占的可能性低"
                % (side, node or "?", win_txt, fmt_us(dur_us), anchor_tid,
                   cpu_val, dist_txt))
        # 证据 2：业务线程被 sched_switch 直接切出（被其他任务抢占）
        if switched_out:
            comms = []
            for e in switched_out:
                c = e.get("next_comm")
                if c and c not in comms:
                    comms.append(c)
            ctx.cpu_evidence.append(
                "%s 节点问题窗口内业务线程（tid %s）在 cpu %s 上被切换出 %d 次"
                "（切至 %s 等）—— 线程被其他任务直接抢占"
                % (side, anchor_tid, cpu_val, len(switched_out),
                   "、".join(comms[:3])))

        ctx.cpu_busy[side] = {
            "seg_key": seg_key, "seg_desc": seg.get("desc"),
            "seg_dur_us": seg.get("dur_us"),
            "window_start": win_start, "window_end": win_end,
            "anchor_name": anchor_name, "anchor_tid": anchor_tid,
            "anchor_cpu": cpu_val, "conn": conn_txt,
            "events": win_evs, "n_mine": len(mine), "n_other": len(others),
            "other_conns": other_conns, "other_by_cpu": other_by_cpu,
            "other_on_cpu": other_on_cpu, "switches_on_cpu": switches_on_cpu,
            "switched_out": switched_out, "preempt": preempt,
        }


# 瓶颈段 key → 涉及侧（client/server/双侧）。未列出的 key 默认双侧展示。
_SLOW_SEG_SIDES = {
    # client 侧段
    "client_user_to_kernel": ("client",),
    "client_kernel_to_user": ("client",),
    "client_stack_to_nic": ("client",),
    "client_nic_xmit": ("client",),
    "client_nic_to_stack": ("client",),
    # server 侧段
    "server_kernel_to_user": ("server",),
    "server_processing": ("server",),
    "server_user_to_kernel": ("server",),
    "server_nic_to_stack": ("server",),
    "server_recvq_to_readable": ("server",),
    "server_readable_to_oncpu": ("server",),
    "server_oncpu_to_user": ("server",),
    # 跨节点线路段 / 宏观三段（涉及双侧）
    "wire_c2s": ("client", "server"),
    "wire_s2c": ("client", "server"),
    "cs_sr": ("client", "server"),
    "sr_ss": ("server",),
    "ss_cr": ("client", "server"),
}

# 宏观三段瓶颈 → 窗口锚点名（bottleneck 无 milestone 名，窗口取锚点）
_MACRO_SEG_ANCHORS = {
    "cs_sr": ("ClientSend", "ServerRecv"),
    "sr_ss": ("ServerRecv", "ServerSend"),
    "ss_cr": ("ServerSend", "ClientRecv"),
}


def _slow_seg_window_analysis(ctx):
    """慢段窗口提取：按定界结论瓶颈段的时间窗过滤该侧全部 bpf 事件。

    根据结论瓶颈段（conclusion.bottleneck，即"慢的具体位置"）确定：
      1. 时间窗 [瓶颈段起点, 终点]（证据分段取 _start_ts/_end_ts，内核段取
         milestone（锚点已并入 milestones），宏观三段取锚点）；
      2. 涉及侧（client 单侧段 / server 单侧段 / 线路段双侧）；
      3. 各涉及侧在瓶颈段时间窗内的全部连接 bpf 事件（从 bpf_window_events
         全景数据过滤，含其他连接，match5t 已标注归属）。

    结果写入 ctx.slow_seg（渲染 / JSON / raw 用）；无瓶颈段或窗口不可得
    时不生成。
    """
    bott = (ctx.conclusion or {}).get("bottleneck")
    if not bott:
        return
    key = bott.get("key")
    if key in _MACRO_SEG_ANCHORS:
        lo_name, hi_name = _MACRO_SEG_ANCHORS[key]
        win_start = (ctx.anchors.get(lo_name) or {}).get("ts")
        win_end = (ctx.anchors.get(hi_name) or {}).get("ts")
        seg_desc = bott.get("label") or key
    else:
        # 内核/证据分段：起点/终点为 milestone 或锚点名（如 ServerRecv 锚点）
        def _point_ts(name):
            ts = ctx.milestones.get(name)
            if ts is None:
                ts = (ctx.anchors.get(name) or {}).get("ts")
            return ts
        win_start = bott.get("_start_ts") or _point_ts(bott.get("start"))
        win_end = bott.get("_end_ts") or _point_ts(bott.get("end"))
        seg_desc = bott.get("desc") or key
    if not win_start or not win_end or win_end < win_start:
        return

    sides = _SLOW_SEG_SIDES.get(key, ("client", "server"))
    sides_out = {}
    for side in sides:
        wevs = (ctx.bpf_window_events or {}).get(side) or []
        if not wevs:
            continue
        in_win = [e for e in wevs if win_start <= e["ts"] <= win_end]
        sides_out[side] = {
            "events": in_win,
            "n_mine": sum(1 for e in in_win if e.get("match5t")),
            "n_other": sum(1 for e in in_win if e.get("match5t") is False),
        }
    if not sides_out:
        return
    ctx.slow_seg = {
        "seg_key": key, "seg_desc": seg_desc,
        "category": (ctx.conclusion or {}).get("category"),
        "window_start": win_start, "window_end": win_end,
        "dur_us": (win_end - win_start).total_seconds() * 1e6,
        "sides": sides_out,
    }


def _side_anchor_tids(ctx, side):
    """该侧关键线程（锚点）tid 集合（int；锚点行无 tid/非数字时忽略）。"""
    names = ("ClientSend", "ClientRecv") if side == "client" \
        else ("ServerRecv", "ServerSend")
    tids = set()
    for n in names:
        t = (ctx.anchors.get(n) or {}).get("tid")
        try:
            tids.add(int(t))
        except (TypeError, ValueError):
            pass
    return tids


def _problem_request_events(ctx, side):
    """问题请求相关事件：问题连接五元组事件 + 关键线程（锚点 tid）调度事件。

    从问题时间窗全景事件（bpf_window_events）提取，排除其他连接的事件与
    无关线程的调度事件——bpf 事件明细"问题请求相关事件"子项数据源。
    调度类事件按 tid/pid/prev_pid/next_pid 任一匹配锚点线程判定相关。
    """
    tids = _side_anchor_tids(ctx, side)
    out = []
    for e in (ctx.bpf_window_events.get(side) or []):
        if e.get("match5t"):
            out.append(e)
            continue
        kind = e.get("kind") or ""
        if not (kind.startswith("sched") or kind == "tcpwakeup_out"):
            continue
        if (e.get("tid") in tids or e.get("pid") in tids
                or e.get("prev_pid") in tids or e.get("next_pid") in tids):
            out.append(e)
    out.sort(key=lambda e: e["ts"])
    return out


def _phys_wire_html(ctx):
    """物理网卡间线路定界（seq 关联）→ HTML 块（无数据时返回空串）。"""
    pw = getattr(ctx, "phys_wire", None) or {}
    parts = []
    for d in ("s2c", "c2s"):
        info = pw.get(d)
        if not info:
            continue
        line_lbl = {"s2c": "server→client", "c2s": "client→server"}[d]
        verdict = ("物理网卡间传输占主导（网卡处理/物理线路），两侧节点内耗时已排除"
                   if info["dominant"] else
                   "线路段耗时主要在节点内（veth/协议栈），非物理网卡间线路")
        share_txt = ("%.1f%%" % info["share_pct"]
                     if info.get("share_pct") is not None else "-")
        cells = tuple(html.escape(v) for v in (
            "%s 物理网卡 %s 发出（%s）" % (info["egress_side"],
                                          info.get("egress_dev") or "-",
                                          fmt_dt(info["egress_ts"])),
            "%s 物理网卡 %s 收到（%s）" % (info["ingress_side"],
                                          info.get("ingress_dev") or "-",
                                          fmt_dt(info["ingress_ts"])),
            fmt_us(info["wire_us"]), share_txt, verdict))
        rows = ("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % cells)
        chain = "\n".join(e["raw"] for e in (info["egress_chain"] + info["ingress_chain"]))
        parts.append(
            '<h4>%s 方向（seq=%s）</h4>'
            '<table><tr><th>物理网卡发出</th><th>物理网卡收到</th><th>线路耗时</th>'
            '<th>占线路段</th><th>判定</th></tr>%s</table>'
            '<details><summary>seq=%s 双侧链路原始事件（%s %d 条 + %s %d 条）</summary>'
            '<pre>%s</pre></details>'
            % (html.escape(line_lbl), info["seq"], rows, info["seq"],
               info["egress_side"], len(info["egress_chain"]),
               info["ingress_side"], len(info["ingress_chain"]),
               html.escape(chain)))
    if not parts:
        return ""
    return ('<h3>网卡链路定界（seq 关联：发送侧物理网卡发出 → 接收侧物理网卡收到）</h3>'
            + "".join(parts))


def _phys_wire_json(info):
    """phys_wire 单方向 dict → JSON dict（ts ISO 微秒，链路事件同 _event_json）。"""
    return {
        "seq": info["seq"], "len": info.get("len"),
        "egress_side": info["egress_side"], "egress_dev": info.get("egress_dev"),
        "egress_ts": info["egress_ts"].isoformat(),
        "ingress_side": info["ingress_side"], "ingress_dev": info.get("ingress_dev"),
        "ingress_ts": info["ingress_ts"].isoformat(),
        "wire_us": info["wire_us"],
        "egress_internal_us": info.get("egress_internal_us"),
        "ingress_internal_us": info.get("ingress_internal_us"),
        "line_us": info.get("line_us"), "share_pct": info.get("share_pct"),
        "dominant": info["dominant"],
        "egress_chain": [_event_json(e) for e in info["egress_chain"]],
        "ingress_chain": [_event_json(e) for e in info["ingress_chain"]],
    }


def _irqoff_html(ctx):
    """窗口内关中断记录 → HTML 块（无数据时返回空串）。"""
    parts = []
    for side in ("client", "server"):
        evs = ctx.irqoff_events.get(side) or []
        if not evs:
            continue
        node = ctx.client_node if side == "client" else ctx.server_node
        rows = "".join(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td></tr>"
            % (fmt_dt(e["ts"]), e.get("irq") or "-",
               e.get("cpu") if e.get("cpu") is not None else "-",
               html.escape(str(e.get("comm"))), e.get("pid"),
               fmt_us(e["latency_us"]))
            for e in evs[:40])
        stacks = "\n\n".join("\n".join(e.get("raw", [])) for e in evs[:10])
        parts.append(
            "<h4>%s 节点（%s）：%d 条</h4>"
            '<table><tr><th>时间</th><th>类型</th><th>cpu</th><th>进程</th>'
            "<th>PID</th><th>关中断时长</th></tr>%s</table>"
            '<details><summary>关中断记录调用栈（前 %d 条原始块）</summary>'
            "<pre>%s</pre></details>"
            % (side, node or "?", len(evs), rows, min(len(evs), 10),
               html.escape(stacks)))
    if not parts:
        return ""
    return "<h3>关中断记录（问题窗口内，>1ms）</h3>" + "".join(parts)


def _sar_html(ctx):
    """窗口内 sar 网卡采样 → HTML 块（无数据时返回空串）。"""
    parts = []
    for side in ("client", "server"):
        samples = ctx.nic_samples.get(side) or []
        if not samples:
            continue
        node = ctx.client_node if side == "client" else ctx.server_node
        rows = "".join(
            "<tr><td>%s</td><td>%s</td><td>%.2f</td><td>%.2f</td><td>%.2f</td>"
            "<td>%.2f</td><td>%.2f</td></tr>"
            % (s["hms"], html.escape(s.get("dev") or "-"), s["rxpck"], s["txpck"],
               s["rxkB"], s["txkB"], s["ifutil"])
            for s in samples[:40])
        parts.append(
            "<h4>%s 节点（%s）：%d 条采样</h4>"
            '<table><tr><th>时间</th><th>网卡</th><th>rxpck/s</th><th>txpck/s</th>'
            "<th>rxkB/s</th><th>txkB/s</th><th>%%ifutil</th></tr>%s</table>"
            % (side, node or "?", len(samples), rows))
    if not parts:
        return ""
    return "<h3>sar 网卡采样（问题窗口内）</h3>" + "".join(parts)


def _bthread_html(ctx):
    """窗口内 bthread 协程事件 → HTML 块（无数据时返回空串）。"""
    parts = []
    for side in ("client", "server"):
        evs = ctx.bthread_events.get(side) or []
        if not evs:
            continue
        anchor = ctx.anchors.get("ClientRecv" if side == "client" else "ServerRecv")
        tid = (anchor or {}).get("tid")
        raw = "\n".join(e["raw"] for e in evs[:30])
        parts.append(
            "<h4>%s 侧（%s）：%d 条</h4>"
            '<details><summary>bthread 原始日志行（前 %d 条）</summary>'
            "<pre>%s</pre></details>"
            % (side, ("线程 tid=%s" % tid) if tid is not None else "全部线程",
               len(evs), min(len(evs), 30), html.escape(raw)))
    if not parts:
        return ""
    return "<h3>bthread 协程事件（问题窗口内）</h3>" + "".join(parts)


def _window_event_row(ev, cpu_val=None):
    """全景事件 → 表格行（归属列 + 问题连接黄底高亮 + 业务 cpu 红色标注）。

    行带 data-o 归属属性（mine/other/none），供慢段时间窗表的 evf 过滤 JS
    做"全部 / 仅问题连接 / 仅其他连接 + 关键字"过滤选择。
    """
    if ev.get("local_ip"):
        addr = "%s:%s %s %s:%s" % (ev["local_ip"], ev.get("local_port"),
                                   ev.get("dir_arrow", ""),
                                   ev["peer_ip"], ev.get("peer_port"))
    elif ev.get("src_ip"):
        addr = "%s:%s -> %s:%s" % (ev["src_ip"], ev.get("src_port"),
                                   ev["dst_ip"], ev.get("dst_port"))
    else:
        addr = ""
    if "comm" in ev:
        extra = "comm=%s pid=%s" % (ev.get("comm"), ev.get("pid"))
    elif "prev_comm" in ev:
        extra = "prev=%s/%s next=%s/%s" % (ev.get("prev_comm"),
                                           ev.get("prev_pid"),
                                           ev.get("next_comm"),
                                           ev.get("next_pid"))
    elif "dev" in ev:
        extra = "dev=%s seq=%s len=%s" % (ev.get("dev"), ev.get("seq"),
                                          ev.get("len"))
    elif "copied_seq" in ev:
        extra = "copied_seq:%s rcv_nxt:%s" % (ev.get("copied_seq"),
                                              ev.get("rcv_nxt"))
    else:
        extra = ""
    match = ev.get("match5t")
    owner = "问题连接" if match else ("其他连接" if match is False else "-")
    cls = ' class="hl5t"' if match else ""
    data_o = ' data-o="%s"' % ("mine" if match
                               else ("other" if match is False else "none"))
    cpu_txt = str(ev.get("cpu", "-"))
    if cpu_val is not None and ev.get("cpu") == cpu_val:
        cpu_txt = '<span class="cpuflag">%s</span>' % cpu_txt
    return ('<tr%s%s><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>'
            "<td>%s</td><td>%s</td></tr>"
            % (cls, data_o, fmt_dt(ev["ts"]), html.escape(str(ev["kind"])),
               ev.get("tid", "-"), cpu_txt, html.escape(addr),
               html.escape(extra), owner))


# 全景事件表列宽（table-layout:fixed，避免大表逐 cell 测宽的布局开销）
_EV_TBL_COLS = ('<colgroup><col style="width:150px"><col style="width:135px">'
                '<col style="width:70px"><col style="width:60px">'
                '<col style="width:270px"><col><col style="width:78px">'
                "</colgroup>")


def _window_events_table(evs, title, cpu_val=None, note=None, with_filter=False):
    """问题窗口全景事件表（全部连接 + sched，含归属列与五元组高亮）。

    包 .table-wrap 滚动容器（参考 skill 风格，兼作大表性能优化）：
    视口外行由 CSS content-visibility 跳过渲染，展开/收起不卡顿。

    with_filter=True 时表头带过滤工具条（全部 / 仅问题连接 / 仅其他连接
    按钮 + 关键字输入框），配合行 data-o 归属属性与报告级 evf 过滤 JS
    （事件委托，无逐表注册）做客户端过滤选择（事件过多时使用）；
    此时不输出 h3 标题（由外层 details summary 承载）。
    """
    if not evs:
        return '<p class="muted">%s：无匹配事件</p>' % html.escape(title)
    total = len(evs)
    shown = evs[:EVENTS_TABLE_MAX_ROWS]
    rows = "".join(_window_event_row(e, cpu_val) for e in shown)
    cap_note = ""
    if total > EVENTS_TABLE_MAX_ROWS:
        cap_note = ('<tr><td colspan="7" class="muted">共 %d 条，仅列前 %d 条'
                    "（按时间序；可用 --trace 缩小分析范围）</td></tr>"
                    % (total, EVENTS_TABLE_MAX_ROWS))
    note_html = '<p class="muted">%s</p>' % note if note else ""
    tbl = ('<div class="table-wrap">'
           '<table class="ev-tbl">%s'
           "<tr><th>时间</th><th>事件</th><th>tid</th><th>cpu</th>"
           "<th>连接</th><th>附加</th><th>归属</th></tr>%s%s</table></div>"
           % (_EV_TBL_COLS, rows, cap_note))
    if not with_filter:
        return '<h3>%s</h3>%s%s' % (html.escape(title), note_html, tbl)
    n_mine = sum(1 for e in shown if e.get("match5t"))
    n_other = sum(1 for e in shown if e.get("match5t") is False)
    n_none = len(shown) - n_mine - n_other
    return ('%s<div class="evf" data-f="all" data-q="">'
            '<div class="evf-bar">'
            '<button class="evf-btn on" data-f="all">全部(%d)</button>'
            '<button class="evf-btn" data-f="mine">仅问题连接(%d)</button>'
            '<button class="evf-btn" data-f="other">仅其他连接(%d)</button>'
            '<input class="evf-input" type="text"'
            ' placeholder="过滤：连接 / 事件 / 关键字">'
            '<span class="evf-count">显示 %d / %d 条（调度类 %d 条不参与归属过滤）</span>'
            "</div>%s</div>"
            % (note_html, len(shown), n_mine, n_other,
               len(shown), len(shown), n_none, tbl))


def _side_events_html(ctx, side):
    """单侧 bpf 事件明细 HTML：问题请求相关 / 慢段时间窗 / 问题时间窗全景。

    - 子项1「问题请求相关事件」（默认展开）：仅问题连接五元组事件 + 关键线程
      （锚点 tid）调度事件（_problem_request_events），其他连接/无关线程排除；
    - 子项2「慢段时间窗事件」（定界出瓶颈段且涉及本侧时）：瓶颈段时间窗内
      该节点全部连接的 bpf 事件（问题五元组高亮 + 过滤选择）；
    - 子项3「问题时间窗全景」：ClientSend→ClientRecv 整窗内全部连接事件；
    - 无全景数据时（无 bpf 日志/关联失败）兜底展示五元组过滤版。
    """
    node = ctx.client_node if side == "client" else ctx.server_node
    wevs = (ctx.bpf_window_events or {}).get(side) or []
    if not wevs:
        return _events_table((ctx.filtered_events or {}).get(side) or [],
                             "%s 节点 bpf 事件" % side)
    parts = []
    # 子项1：问题请求相关事件（仅问题连接五元组 + 关键线程调度）
    req_evs = _problem_request_events(ctx, side)
    if req_evs:
        parts.append(
            "<details open><summary>问题请求相关事件（仅问题连接五元组 + "
            "关键线程调度，%d 条）</summary>%s</details>"
            % (len(req_evs),
               _events_table(req_evs, "%s 侧问题请求相关事件" % side)))
    # 子项2：慢段时间窗事件（瓶颈段窗口内全部连接，高亮 + 过滤选择）
    sw = getattr(ctx, "slow_seg", None) or {}
    sw_side = (sw.get("sides") or {}).get(side)
    if sw_side is not None:
        title = ("慢段时间窗事件（瓶颈段：%s，%s → %s，耗时 %s，共 %d 条："
                 "问题连接 %d / 其他连接 %d）"
                 % (sw.get("seg_desc") or sw.get("seg_key"),
                    fmt_dt(sw["window_start"]), fmt_dt(sw["window_end"]),
                    fmt_us(sw.get("dur_us") or 0),
                    len(sw_side["events"]), sw_side["n_mine"], sw_side["n_other"]))
        note = ("瓶颈段时间窗内该节点全部连接的 bpf 事件（含穿插的其他请求）；"
                "黄底 = 问题连接五元组；事件过多时可用上方按钮 / 关键字过滤选择")
        parts.append(
            "<details><summary>%s</summary>%s</details>"
            % (html.escape(title),
               _window_events_table(sw_side["events"], title, note=note,
                                    with_filter=True)))
    # 子项3：问题时间窗全景（ClientSend→ClientRecv 整窗）
    n_mine = sum(1 for e in wevs if e.get("match5t"))
    n_other = sum(1 for e in wevs if e.get("match5t") is False)
    pano_title = "%s 节点 bpf 事件（%s，问题时间窗全景）" % (side, node or "节点未定位")
    pano_note = ("共 %d 条：问题连接 %d 条 / 其他连接 %d 条（其他请求事件直接混排展示）；"
                 "黄底行 = 问题连接五元组事件；事件过多时可用上方按钮 / 关键字过滤选择"
                 % (len(wevs), n_mine, n_other))
    parts.append("<details><summary>%s</summary>%s</details>"
                 % (html.escape(pano_title),
                    _window_events_table(wevs, pano_title, note=pano_note,
                                         with_filter=True)))
    return "".join(parts)


def _cpu_busy_html(ctx):
    """问题窗口 bpf 事件全景 + cpu 侵占分析 → HTML 块（无数据时返回空串）。

    - 事件表：窗口内全部连接的内核事件，问题五元组行黄底高亮（hl5t），
      业务线程所在 cpu 红色标注（cpuflag）——既可追踪问题请求的事件，
      又能看到问题时间段穿插的其他请求日志；
    - 摘要：窗口/业务线程/事件统计 + 业务 cpu 上其他请求处理结论。
    """
    cpu_busy = getattr(ctx, "cpu_busy", None) or {}
    parts = []
    for side in ("client", "server"):
        info = cpu_busy.get(side)
        if not info:
            continue
        node = ctx.client_node if side == "client" else ctx.server_node
        cpu_val = info["anchor_cpu"]
        evs = info["events"]
        # 摘要卡
        sum_rows = [
            "问题窗口：%s ~ %s（%s，%s）"
            % (fmt_dt(info["window_start"]), fmt_dt(info["window_end"]),
               info.get("seg_desc") or info["seg_key"],
               fmt_us((info["window_end"] - info["window_start"])
                      .total_seconds() * 1e6)),
            "业务线程：tid %s，cpu %s（%s 锚点）"
            % (info["anchor_tid"] if info["anchor_tid"] is not None else "-",
               cpu_val if cpu_val is not None else "未知（锚点行无 cpu 字段）",
               info["anchor_name"]),
            "窗口内内核事件 %d 条：问题连接 %d 条 / 其他连接 %d 条（%d 个其他连接）"
            % (len(evs), info["n_mine"], info["n_other"], len(info["other_conns"])),
        ]
        if cpu_val is not None:
            if info["other_on_cpu"]:
                conns = sorted(info["other_conns"].items(), key=lambda kv: -kv[1])
                sum_rows.append(
                    '<span style="color:#cf222e">业务 cpu %s 上有 %d 条其他连接事件'
                    "（%s）—— 收包软中断在该 cpu 处理其他请求，业务线程可能被抢占</span>"
                    % (cpu_val, len(info["other_on_cpu"]),
                       "、".join("%s×%d" % kv for kv in conns[:3])))
            else:
                sum_rows.append(
                    "业务 cpu %s 上未发现其他连接的收包/协议栈事件"
                    "（软中断处理其他请求的抢占可能性低）" % cpu_val)
            if info["switches_on_cpu"]:
                sum_rows.append("业务 cpu 上 sched_switch %d 次"
                                % len(info["switches_on_cpu"]))
        # 事件表（问题五元组高亮 + 业务 cpu 标注）
        shown = evs[:EVENTS_TABLE_MAX_ROWS]
        rows = "".join(_window_event_row(e, cpu_val) for e in shown)
        cap_note = ""
        if len(evs) > EVENTS_TABLE_MAX_ROWS:
            cap_note = ('<tr><td colspan="7" class="muted">共 %d 条，仅列前 %d 条'
                        "（完整数据见 JSON 输出）</td></tr>"
                        % (len(evs), EVENTS_TABLE_MAX_ROWS))
        parts.append(
            "<h4>%s 节点（%s）：问题窗口 bpf 事件全景</h4>"
            '<div class="winsum">%s</div>'
            '<div class="table-wrap"><table class="ev-tbl">%s'
            '<tr><th>时间</th><th>事件</th><th>tid</th><th>cpu</th>'
            "<th>连接</th><th>附加</th><th>归属</th></tr>%s%s</table></div>"
            '<p class="muted">黄底行 = 问题连接五元组事件（%s）；'
            "红色 cpu = 业务线程所在 cpu</p>"
            % (side, node or "?", "<br>".join(sum_rows), _EV_TBL_COLS,
               rows, cap_note, html.escape(info.get("conn") or "?")))
    if not parts:
        return ""
    return ("<h3>问题窗口 bpf 事件全景与 CPU 侵占分析"
            "（收包后业务处理开始晚 → 软中断抢占定界）</h3>" + "".join(parts))


def _irqoff_svg(series, width=880, height=220):
    """irqoff 时长散点（纯 Python 内联 SVG，无 JS）：x=时间，y=关中断时长。

    series: [(ts, latency_us, comm, cpu)]（已按时长降序截断）。
    """
    pts = [(ts, lu, comm, cpu) for ts, lu, comm, cpu in series
           if ts is not None and lu is not None]
    if not pts:
        return ""
    t_lo = min(p[0] for p in pts)
    t_hi = max(p[0] for p in pts)
    if t_hi <= t_lo:  # 单点/同刻：给 1s 展开区间，避免除零
        t_hi = t_lo + timedelta(seconds=1)
    y_hi = max(p[1] for p in pts) or 1.0
    pad_l, pad_r, pad_t, pad_b = 70, 12, 10, 22
    w = width - pad_l - pad_r
    h = height - pad_t - pad_b

    def X(ts):
        return pad_l + w * (ts - t_lo).total_seconds() / (t_hi - t_lo).total_seconds()

    def Y(lu):
        return pad_t + h * (1 - lu / y_hi)

    grid = []
    for i in range(5):
        val = y_hi * i / 4.0
        y = Y(val)
        grid.append(
            '<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#e1e4e8" '
            'stroke-width="1"/>'
            '<text x="%d" y="%.1f" font-size="10" fill="#57606a" '
            'text-anchor="end">%s</text>'
            % (pad_l, y, width - pad_r, y, pad_l - 6, y + 3, fmt_us(val)))
    dots = "".join(
        '<circle cx="%.1f" cy="%.1f" r="2.5" fill="#d63384">'
        "<title>%s cpu%s %s</title></circle>"
        % (X(ts), Y(lu), fmt_dt(ts), cpu, fmt_us(lu))
        for ts, lu, comm, cpu in pts)
    x_labels = (
        '<text x="%d" y="%d" font-size="10" fill="#57606a">%s</text>'
        '<text x="%d" y="%d" font-size="10" fill="#57606a" text-anchor="end">%s</text>'
        % (pad_l, height - 6, t_lo.strftime("%m-%d %H:%M:%S"),
           width - pad_r, height - 6, t_hi.strftime("%m-%d %H:%M:%S")))
    return ('<svg viewBox="0 0 %d %d" style="max-width:100%%;height:auto" '
            'role="img" aria-label="关中断时长散点">%s%s%s</svg>'
            % (width, height, "".join(grid), dots, x_labels))


def _irqoff_overview_html(aux_stats):
    """关中断统计卡（全采集周期）：分桶直方图 + 按进程 top10 + SVG 散点 + Top20。"""
    stats_by_node = (aux_stats or {}).get("irqoff") or {}
    if not stats_by_node:
        return ""
    parts = []
    for node in sorted(stats_by_node):
        st = stats_by_node[node]
        if not st.get("total"):
            continue
        # 分桶直方图（≥ 阈值累计计数，条宽按最大桶归一）
        buckets = st.get("buckets") or {}
        bmax = max(buckets.values()) if buckets else 0
        hist_rows = "".join(
            "<tr><td>≥%s</td><td>%d</td><td>%s</td></tr>"
            % (fmt_us(b), buckets.get(b, 0),
               ('<div style="background:#0969da;height:10px;width:%.1f%%;'
                'max-width:280px"></div>' % (100.0 * buckets.get(b, 0) / bmax))
               if bmax else "")
            for b in IRQOFF_BUCKETS_US if buckets.get(b))
        comm_rows = "".join(
            "<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>"
            % (html.escape(c), v["n"], fmt_us(v["max_us"]), fmt_us(v["total_us"]))
            for c, v in sorted((st.get("by_comm") or {}).items(),
                               key=lambda kv: -kv[1]["max_us"])[:10])
        top_rows = "".join(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (fmt_dt(ts), fmt_us(lu), html.escape(str(comm)), cpu)
            for ts, lu, comm, cpu in (st.get("series") or [])[:20])
        parts.append(
            "<h3>%s 节点：%d 条（hardirq %d / softirq %d），最长 %s</h3>"
            '<table><tr><th>时长分桶</th><th>记录数</th><th></th></tr>%s</table>'
            '<table><tr><th>进程 top10（按最长）</th><th>次数</th><th>最长</th>'
            "<th>累计</th></tr>%s</table>"
            '<details open><summary>关中断时长散点（x=时间 y=时长，'
            "top %d 条最长记录）</summary>%s</details>"
            '<details><summary>Top20 最长记录</summary>'
            "<table><tr><th>时间</th><th>时长</th><th>进程</th><th>cpu</th></tr>"
            "%s</table></details>"
            % (html.escape(str(node)), st["total"], st["hardirq_n"], st["softirq_n"],
               fmt_us(st["max_us"]), hist_rows, comm_rows,
               len(st.get("series") or []), _irqoff_svg(st.get("series") or []),
               top_rows))
    if not parts:
        return ""
    return ('<div class="card"><h2>关中断统计（全采集周期，&gt;1ms）</h2>'
            '<p class="muted">来源：irqoff_latency_&lt;nodeIp&gt;.log。'
            "定界中断相关问题（如网卡收包慢因关中断导致），"
            "问题时刻的窗口内记录见各 trace 卡。</p>%s</div>" % "".join(parts))


def _nic_overview_html(aux_stats):
    """网卡利用率统计卡（sar，全采集周期）。"""
    nic_by_node = (aux_stats or {}).get("nic") or {}
    if not nic_by_node:
        return ""
    rows = []
    for node in sorted(nic_by_node):
        for dev in sorted(nic_by_node[node]):
            d = nic_by_node[node][dev]
            rows.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
                "<td>%d</td><td>%.2f%%</td><td>%.2f%%</td><td>%s</td>"
                "<td>%.0f</td></tr>"
                % (html.escape(str(node)), html.escape(dev),
                   d.get("Speed") or "-", d.get("Duplex") or "-",
                   d.get("Link detected") or "-", d.get("n_samples", 0),
                   d.get("max_ifutil") or 0.0, d.get("avg_ifutil") or 0.0,
                   d.get("peak_hms") or "-", d.get("max_rxpck") or 0.0))
    if not rows:
        return ""
    return ('<div class="card"><h2>网卡利用率统计（sar，全采集周期）</h2>'
            '<p class="muted">来源：nic-&lt;nodeIp&gt;.log（ethtool 属性 + 每秒采样）。'
            "佐证/排除网卡带宽瓶颈；问题时刻窗口内采样见各 trace 卡。</p>"
            "<table><tr><th>节点</th><th>网卡</th><th>速率</th><th>双工</th>"
            "<th>链路</th><th>采样数</th><th>峰值 %%ifutil</th><th>均值 %%ifutil</th>"
            "<th>峰值时刻</th><th>峰值 rxpck/s</th></tr>%s</table></div>"
            % "".join(rows))


# ── Phase 5/6: conclusion ─────────────────────────────────────────────────────

class ConclusionEngine:
    @staticmethod
    def conclude(ctx):
        f = ctx.slow.fields
        evidence = []
        segs = [s for s in ctx.kernel_segments if s["abnormal"]]
        pool = segs if segs else [s for s in ctx.kernel_segments if s["threshold_us"] is not None]
        if pool:
            bott = max(pool, key=lambda s: s["dur_us"])
            category = bott["category"]
            evidence.append("瓶颈段：%s（%s → %s，耗时 %s，阈值 %s）"
                            % (bott["desc"], bott["start"], bott["end"],
                               fmt_us(bott["dur_us"]),
                               fmt_us(bott["threshold_us"]) if bott["threshold_us"] else "对比BRPC指标"))
            margin = (bott["dur_us"] / bott["threshold_us"]
                      if bott["threshold_us"] else None)
        else:
            # macro-only fallback
            bott = None
            margin = None
            candidates = []
            for key, label, cat in (
                    ("cs_sr", "ClientSend→ServerRecv", "client_to_server_path"),
                    ("sr_ss", "ServerRecv→ServerSend", "server_processing_slow"),
                    ("ss_cr", "ServerSend→ClientRecv", "server_to_client_path")):
                v = ctx.macro.get(key)
                if v is None:
                    continue
                thr = MACRO_THRESHOLDS_US[key]
                candidates.append({"key": key, "dur_us": v, "label": label,
                                   "category": cat, "threshold_us": thr,
                                   "abnormal": v > thr})
            ab = [c for c in candidates if c["abnormal"]]
            pool2 = ab if ab else candidates
            if pool2:
                bott = max(pool2, key=lambda c: c["dur_us"])
                category = bott["category"]
                evidence.append("瓶颈段（宏观三段）：%s，耗时 %s"
                                % (bott["label"], fmt_us(bott["dur_us"])))
                margin = bott["dur_us"] / bott["threshold_us"]
            else:
                category = "unknown"

        # 细分定界：server 内核→用户态瓶颈进一步区分 协程调度排队 vs 内核唤醒/调度。
        # 判据：线程上CPU→ServerRecv（协程排队执行）耗时 > 内核唤醒→线程上CPU（调度等待）
        if category == "server_kernel_to_user_delay":
            pickup = {s["key"]: s for s in ctx.kernel_segments if s.get("evidence")}
            ocu = pickup.get("server_oncpu_to_user")
            roc = pickup.get("server_readable_to_oncpu")
            if ocu and roc and ocu["dur_us"] > roc["dur_us"]:
                category = "coroutine_schedule_delay"
                evidence.append(
                    "细分证据：线程上CPU→ServerRecv（协程排队执行）耗时 %s，"
                    "大于 内核唤醒→线程上CPU（调度等待）耗时 %s —— 瓶颈在协程调度排队"
                    % (fmt_us(ocu["dur_us"]), fmt_us(roc["dur_us"])))

        # 细分定界：传输类瓶颈且物理网卡间线路占主导（seq 关联双侧物理网卡点位）
        # → 明确定界为物理网卡间传输慢（网卡处理/物理线路），两侧节点内耗时已排除
        pw = getattr(ctx, "phys_wire", None) or {}
        for direction, base_cat, refined_cat, line_lbl in (
                ("s2c", "network_s2c_transmission", "network_s2c_phys_wire_delay",
                 "server→client"),
                ("c2s", "network_c2s_transmission", "network_c2s_phys_wire_delay",
                 "client→server")):
            info = pw.get(direction)
            if not info or not info.get("dominant") or category != base_cat:
                continue
            category = refined_cat
            share_txt = ("，占 %s 线路段 %.1f%%" % (line_lbl, info["share_pct"])
                         if info.get("share_pct") is not None else "")
            evidence.append(
                "◆ 细分证据（seq=%s 关联）：%s 物理网卡 %s 发出（%s）→ %s 物理网卡 %s 收到（%s）"
                "耗时 %s%s；%s 节点内 %s + %s 节点内 %s 已排除"
                " —— 瓶颈在物理网卡间传输（网卡处理/物理线路）"
                % (info["seq"],
                   info["egress_side"], info.get("egress_dev"), fmt_dt(info["egress_ts"]),
                   info["ingress_side"], info.get("ingress_dev"), fmt_dt(info["ingress_ts"]),
                   fmt_us(info["wire_us"]), share_txt,
                   info["egress_side"], fmt_us(info.get("egress_internal_us")),
                   info["ingress_side"], fmt_us(info.get("ingress_internal_us"))))

        # corroborating scheduling evidence
        cwarn = ctx.warn_events.get("client") or []
        swarn = ctx.warn_events.get("server") or []
        if cwarn:
            mx = max(w["latency_us"] or 0 for w in cwarn)
            evidence.append("client 节点问题窗口内有 %d 条调度时延告警（最大 latency %d us）"
                            % (len(cwarn), mx))
        if swarn:
            mx = max(w["latency_us"] or 0 for w in swarn)
            evidence.append("server 节点问题窗口内有 %d 条调度时延告警（最大 latency %d us）"
                            % (len(swarn), mx))
        sched_ev = [e for e in ctx.wakeup_chain
                    if e["kind"].startswith("sched") or e["kind"] in ("sock_readable", "tcpwakeup_out")]
        if sched_ev:
            evidence.append("client 侧内核收包→用户态取包之间存在 %d 条唤醒/调度事件（详见唤醒链）"
                            % len(sched_ev))
        if ctx.server_wakeup_chain:
            evidence.append("server 侧内核收包→协程执行之间存在 %d 条唤醒/调度事件"
                            "（详见 server 侧唤醒链）" % len(ctx.server_wakeup_chain))
        for s in (ctx.coro_evidence or []):
            evidence.append("◈ " + s)
        nic_ev = list(ctx.nic_evidence or [])
        for s in nic_ev:
            evidence.append("◆ " + s)
        for s in (getattr(ctx, "cpu_evidence", None) or []):
            evidence.append("◎ " + s)
        if category in ("client_kernel_to_user_delay", "server_kernel_to_user_delay",
                        "coroutine_schedule_delay") \
                and (cwarn or swarn or sched_ev or ctx.server_wakeup_chain
                     or ctx.coro_evidence
                     or ctx.irqoff_events.get("client") or ctx.irqoff_events.get("server")
                     or getattr(ctx, "cpu_busy_preempt", False)):
            # 窗口内有调度告警/唤醒链/协程排队/关中断/软中断抢占证据佐证 → 高置信
            confidence = "高"
        elif category in ("network_c2s_transmission", "network_s2c_transmission",
                          "network_c2s_phys_wire_delay", "network_s2c_phys_wire_delay") \
                and (nic_ev or ctx.milestones.get("ClientNetifRx")
                     or ctx.milestones.get("ServerNetifRx")):
            # 传输类定界且有网卡层点位佐证（收发点位或重传证据）→ 高置信；
            # 物理网卡间定界（seq 关联双侧物理网卡点位）为最强证据
            confidence = "高"
        elif margin is not None and margin >= CONF_HIGH_MARGIN:
            confidence = "高"
        elif ctx.kernel_segments:
            confidence = "中"
        else:
            confidence = "低"
        for note in ctx.missing:
            evidence.append("⚠ " + note)

        suggestions = list(CATEGORY_SUGGESTIONS.get(category, []))
        if category in ("client_kernel_to_user_delay", "server_kernel_to_user_delay") \
                and not sched_ev:
            suggestions.append("本次 bpf 日志未采集 sched_* 唤醒链事件，建议结合 "
                               "latency_warn 告警与线程级 CPU/运行队列监控辅助定界")
        if category in ("client_kernel_to_user_delay", "server_kernel_to_user_delay",
                        "coroutine_schedule_delay") \
                and getattr(ctx, "cpu_busy_preempt", False):
            suggestions.append(
                "业务线程所在 cpu 的收包软中断正在处理其他请求（见问题窗口全景）："
                "考虑调整网卡 RSS/中断亲和性将收包分散到非业务 cpu，"
                "或为业务线程绑定独立 cpu / 调整 CPU 隔离（isolcpus）配置")

        ctx.conclusion = {
            "category": category,
            "label": CATEGORY_LABELS.get(category, category),
            "bottleneck": bott,
            "evidence": evidence,
            "confidence": confidence,
            "suggestions": suggestions,
        }


# ── HTML report ───────────────────────────────────────────────────────────────

CSS = """
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC',
 'Microsoft YaHei',sans-serif;background:#f5f7fa;color:#333;font-size:14px}
.header{background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);color:#fff;
 padding:28px 40px}
.header h1{font-size:26px;font-weight:600}
.header .meta{margin-top:8px;font-size:13px;opacity:.8}
.wrap{max-width:1440px;margin:0 auto;padding:24px}
.toolbar{display:flex;gap:8px;margin:16px 0}
.toolbar button{padding:5px 14px;background:#5470c6;color:#fff;border:none;
 border-radius:4px;cursor:pointer;font-size:13px}
.toolbar button:hover{background:#3a5ba0}
.section-title{font-size:20px;font-weight:600;margin:28px 0 14px;color:#1a1a2e}
h2{font-size:18px;margin:24px 0 10px;color:#1a1a2e}
h3{font-size:15px;font-weight:600;color:#1a1a2e;margin:18px 0 8px;
 border-left:3px solid #5470c6;padding-left:8px}
h4{font-size:13px;font-weight:600;color:#1a1a2e;margin:12px 0 6px}
.card{background:#fff;border-radius:12px;padding:20px;margin:0 0 20px;
 box-shadow:0 2px 8px rgba(0,0,0,.06)}
/* 汇总统计卡 */
.summary-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));
 gap:16px;margin:20px 0}
.summary-cards .card{padding:16px 20px;margin:0}
.summary-cards .label{font-size:13px;color:#999;margin-bottom:6px}
.summary-cards .value{font-size:26px;font-weight:700;color:#1a1a2e}
/* trace 卡片头（参考 group-header 风格） */
.trace-card{padding:0;overflow:hidden;
 content-visibility:auto;contain-intrinsic-size:auto 1000px}
.trace-head{padding:14px 20px;background:linear-gradient(135deg,#f0f4ff 0%,#e8ecf1 100%);
 border-bottom:1px solid #d0d8e8;display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.trace-head:hover{background:linear-gradient(135deg,#e0e8f8 0%,#d8dce8 100%)}
.trace-head .req-index{font-weight:700;font-size:16px;color:#1a1a2e}
.trace-head .trace-id{font-family:monospace;font-size:13px;font-weight:600;color:#333;
 word-break:break-all}
.trace-head .trace-meta{font-size:12px;color:#666;margin-left:auto;white-space:nowrap}
.trace-body{padding:16px 20px}
/* 目录 */
.toc{background:#fff;border-radius:12px;padding:16px 20px;
 box-shadow:0 2px 8px rgba(0,0,0,.06);margin-bottom:20px}
.toc a{color:#5470c6;text-decoration:none}
.toc a:hover{text-decoration:underline}
.toc ol{padding-left:22px;margin-top:6px}
.toc li{padding:2px 0}
table{border-collapse:collapse;width:100%;font-size:13px;margin:8px 0}
th,td{border-bottom:1px solid #eee;padding:7px 10px;text-align:left;vertical-align:top}
th{background:#1a1a2e;color:#fff;font-weight:600}
tr:hover td{background:#f0f4ff}
.num{text-align:right;font-variant-numeric:tabular-nums}
/* 大表性能优化：固定列宽 + 滚动容器 + 视口外行跳过渲染 */
.table-wrap{max-height:520px;overflow:auto;border:1px solid #e0e0e0;
 border-radius:8px;margin:8px 0;background:#fff}
.table-wrap table{margin:0}
.table-wrap th,.table-wrap td{white-space:nowrap}
.ev-tbl{table-layout:fixed}
.ev-tbl td{overflow:hidden;text-overflow:ellipsis}
.ev-tbl tr{content-visibility:auto;contain-intrinsic-size:auto 28px}
tr.hl5t td{background:#fff3bf!important}
tr.hl5t:hover td{background:#ffe98a!important}
.cpuflag{color:#cf222e;font-weight:700}
/* 慢段时间窗事件过滤工具条（事件过多时的过滤选择） */
.evf-bar{display:flex;align-items:center;gap:8px;margin:8px 0;flex-wrap:wrap}
.evf-btn{padding:4px 13px;background:#fff;border:1px solid #c9d4e8;
 border-radius:14px;cursor:pointer;font-size:12px;color:#456}
.evf-btn:hover{border-color:#5470c6;color:#5470c6}
.evf-btn.on{background:#5470c6;border-color:#5470c6;color:#fff}
.evf-input{padding:4px 12px;border:1px solid #c9d4e8;border-radius:14px;
 font-size:12px;outline:none;width:230px}
.evf-input:focus{border-color:#5470c6}
.evf-count{font-size:12px;color:#999}
.badge{display:inline-block;padding:2px 9px;border-radius:10px;font-size:11px;
 font-weight:600}
.b-high{background:#ffebee;color:#c62828}
.b-mid{background:#fff3e0;color:#ef6c00}
.b-low{background:#f5f5f5;color:#757575}
.b-abn{background:#ffebee;color:#c62828}
.b-ok{background:#e8f5e9;color:#2e7d32}
.tl{margin:10px 0}
.tl .bar{display:flex;height:26px;border-radius:4px;overflow:hidden;border:1px solid #d0d8e8}
.tl .seg{position:relative;min-width:2px}
.tl .seg.gap{background-image:repeating-linear-gradient(45deg,rgba(0,0,0,.18) 0 4px,transparent 4px 8px)!important}
.tl .legend{display:flex;flex-wrap:wrap;gap:10px;font-size:12px;margin-top:6px}
.tl .gapwarn{color:#9a6700;margin-left:4px}
.pt-tbl{font-size:12px;margin-top:8px}
.dot{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:4px}
.concl{border-left:4px solid #c62828;background:#fff8f8;padding:10px 14px;
 margin:10px 0;border-radius:0 8px 8px 0}
.concl.ok{border-left-color:#2e7d32;background:#f6fff8}
.concl b{color:#1a1a2e}
.muted{color:#999;font-size:12px}
/* 终端风格原始日志块（参考 log-lines 风格） */
pre{background:#1a1a2e;color:#a8d8a8;padding:12px;border-radius:6px;
 font-family:monospace;font-size:12px;max-height:480px;overflow:auto;
 white-space:pre-wrap;word-break:break-all}
summary{cursor:pointer;font-weight:600;padding:6px 0;color:#1a1a2e;user-select:none}
summary:hover{color:#5470c6}
details{margin:6px 0}
.winsum{background:#f8f9fa;border:1px solid #e0e0e0;border-radius:8px;
 padding:10px 14px;margin:8px 0;font-size:13px;line-height:1.8}
"""


def _seg_color(i):
    palette = ["#79c0ff", "#a5d6ff", "#7ee787", "#ffa657", "#f78166",
               "#d2a8ff", "#ffd8b3", "#56d364"]
    return palette[i % len(palette)]


def _abnormal_badge(ab):
    return ('<span class="badge b-abn">异常</span>' if ab
            else '<span class="badge b-ok">正常</span>')


def _timeline_html(ms, segments):
    """全路径时间线：业务 ↔ 协议栈 ↔ 网卡 ↔ 协议栈 ↔ 业务。

    条形图按"存在的相邻点位"分段；两点之间被跳过的缺失点位在 legend 中以
    `⚠ 缺：…` 标注（段加斜纹 gap 样式）。点位明细表列出全部 18 点位
    （时间/层级/距上一可用点耗时/缺失状态），纯锚点场景也输出。
    """
    order = POINT_ORDER
    idx_of = {k: i for i, k in enumerate(order)}
    pts = [k for k in order if k in ms]
    bar_html = '<p class="muted">可用点位不足 2 个，无法绘制时间线</p>'
    legend = []
    if len(pts) >= 2:
        t0, t1 = ms[pts[0]], ms[pts[-1]]
        total = max((t1 - t0).total_seconds() * 1e6, 1.0)
        spans = []
        for i in range(len(pts) - 1):
            a, b = ms[pts[i]], ms[pts[i + 1]]
            dur = (b - a).total_seconds() * 1e6
            w = max(dur / total * 100, 0.4)
            # 两点之间被跳过的缺失点位（按全路径序）
            skipped = [k for k in order[idx_of[pts[i]] + 1: idx_of[pts[i + 1]]]
                       if k not in ms]
            seg_info = next((s for s in segments
                             if s["start"] == pts[i] and s["end"] == pts[i + 1]), None)
            color = "#d1242f" if (seg_info and seg_info["abnormal"]) else _seg_color(i)
            gap_cls = " gap" if skipped else ""
            spans.append('<div class="seg%s" style="width:%.2f%%;background:%s" '
                         'title="%s→%s: %s%s"></div>'
                         % (gap_cls, w, color, pts[i], pts[i + 1], fmt_us(dur),
                            ("（中间缺 %d 个点位）" % len(skipped)) if skipped else ""))
            legend.append('<span><span class="dot" style="background:%s"></span>%s→%s：%s%s</span>'
                          % (color, pts[i], pts[i + 1], fmt_us(dur),
                             ('<span class="gapwarn">⚠ 缺：%s</span>' % "、".join(skipped))
                             if skipped else ""))
        bar_html = '<div class="bar">%s</div><div class="legend">%s</div>' % (
            "".join(spans), "".join(legend))
    # 点位明细表：全部点位（含缺失），距上一"存在点位"的耗时
    rows = []
    prev_ts = None
    for i, k in enumerate(order):
        present = k in ms
        delta = ""
        if present:
            delta = fmt_us((ms[k] - prev_ts).total_seconds() * 1e6) if prev_ts else "-"
            prev_ts = ms[k]
        rows.append("<tr><td>%d</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                    % (i + 1, k, POINT_LAYERS.get(k, "-"),
                       fmt_dt(ms[k]) if present else "-",
                       delta if present else "-",
                       "" if present else '<span class="badge b-low">缺失</span>'))
    pt_tbl = ('<table class="pt-tbl"><tr><th>#</th><th>点位</th><th>层级</th>'
              '<th>时间</th><th>距上一可用点</th><th>状态</th></tr>%s</table>'
              % "".join(rows))
    return '<div class="tl">%s</div>%s' % (bar_html, pt_tbl)


def _events_table(events, title):
    if not events:
        return '<p class="muted">%s：无匹配事件</p>' % html.escape(title)
    total = len(events)
    if total > EVENTS_TABLE_MAX_ROWS:
        events = events[:EVENTS_TABLE_MAX_ROWS]
    rows = []
    for ev in events:
        addr = ""
        if ev.get("local_ip"):
            arrow = ev.get("dir_arrow", "")
            addr = "%s:%d %s %s:%d" % (ev["local_ip"], ev["local_port"], arrow,
                                       ev["peer_ip"], ev["peer_port"])
        elif ev.get("src_ip"):  # 网卡层事件：方向四元组
            addr = "%s:%d -> %s:%d" % (ev["src_ip"], ev["src_port"],
                                       ev["dst_ip"], ev["dst_port"])
        extra = ""
        if "copied_seq" in ev:
            extra = "copied_seq:%s rcv_nxt:%s" % (ev.get("copied_seq"), ev.get("rcv_nxt"))
        if "comm" in ev:
            extra = "comm=%s pid=%s" % (ev.get("comm"), ev.get("pid"))
        if "dev" in ev:  # 网卡层：dev + seq/len/rc
            extra = "dev=%s seq=%s len=%s" % (ev.get("dev"), ev.get("seq"), ev.get("len"))
            if "rc" in ev:
                extra += " rc=%s" % ev["rc"]
        elif ev["kind"] == "tcp_retransmit":
            extra = "seq=%s tx_seq=%s snd_una=%s snd_nxt=%s" % (
                ev.get("seq"), ev.get("tx_seq"), ev.get("snd_una"), ev.get("snd_nxt"))
        rows.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                    % (fmt_dt(ev["ts"]), html.escape(str(ev["kind"])),
                       ev.get("tid", "-"), ev.get("cpu", "-"), html.escape(addr),
                       html.escape(extra)))
    cap_note = ""
    if total > EVENTS_TABLE_MAX_ROWS:
        cap_note = ('<tr><td colspan="6" class="muted">共 %d 条，仅列前 %d 条</td></tr>'
                    % (total, EVENTS_TABLE_MAX_ROWS))
    return ('<h3>%s</h3><div class="table-wrap"><table class="ev-tbl">'
            '<colgroup><col style="width:150px"><col style="width:135px">'
            '<col style="width:70px"><col style="width:60px">'
            '<col style="width:270px"><col></colgroup>'
            '<tr><th>时间</th><th>事件</th><th>tid</th><th>cpu</th>'
            '<th>连接</th><th>附加</th></tr>%s%s</table></div>'
            % (html.escape(title), "".join(rows), cap_note))


def _trace_html(ctx, idx):
    f = ctx.slow.fields
    c = ctx.conclusion
    conf_cls = {"高": "b-high", "中": "b-mid"}.get(c.get("confidence", "低"), "b-low")
    rows_meta = [
        ("trace_id", ctx.trace_id),
        ("client 日志", ctx.slow.log_path),
        ("client pod / 节点", "%s / %s" % (ctx.client_pod_dir, ctx.client_node or "未知")),
        ("server pod / 节点", "%s / %s" % (getattr(ctx, "server_pod_dir", None) or "未定位",
                                       ctx.server_node or "未知")),
        ("client/server pod IP", "%s / %s" % (ctx.client_ip or "-", ctx.server_ip or "-")),
        ("连接四元组", "%s:%s → %s:%s" % ctx.conn if ctx.conn else "未识别"),
        ("method", f.get("method", "-")),
        ("e2e_us / framework_us", "%s / %s" % (f.get("e2e_us", "-"), f.get("framework_us", "-"))),
        ("network_residual_us", f.get("network_residual_us", "-")),
        ("remote/server queue+exec", "%s / %s+%s" % (f.get("remote_processing_us", "-"),
                                                     f.get("server_req_queue_us", "-"),
                                                     f.get("server_exec_us", "-"))),
    ]
    meta_html = "".join("<tr><th>%s</th><td>%s</td></tr>" % (html.escape(k), html.escape(str(v)))
                        for k, v in rows_meta)

    macro_rows = "".join(
        "<tr><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            {"cs_sr": "ClientSend→ServerRecv", "sr_ss": "ServerRecv→ServerSend",
             "ss_cr": "ServerSend→ClientRecv"}[k],
            fmt_us(ctx.macro.get(k)),
            _abnormal_badge(ctx.macro.get(k, 0) > MACRO_THRESHOLDS_US[k]))
        for k in ("cs_sr", "sr_ss", "ss_cr") if k in ctx.macro)

    seg_rows = "".join(
        "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
        % (html.escape(s["desc"]), s["start"] + " → " + s["end"], fmt_us(s["dur_us"]),
           fmt_us(s["threshold_us"]) if s["threshold_us"] else
           (("BRPC queue+exec=%s" % fmt_us(s.get("brpc_queue_exec_us")))
            if "brpc_queue_exec_us" in s else "证据段（无阈值）"),
           _abnormal_badge(s["abnormal"]))
        for s in ctx.kernel_segments)

    warn_html = ""
    for side, evs in ctx.warn_events.items():
        if evs:
            items = "".join("<li>%s　CPU:%s　comm:%s　PID:%s　latency:%s us</li>"
                            % (fmt_dt(e["ts"]), e["cpu"], html.escape(str(e["comm"])),
                               e["pid"], e["latency_us"])
                            for e in evs[:20])
            warn_html += "<h3>%s 节点调度时延告警（%d 条）</h3><ul>%s</ul>" % (
                side, len(evs), items)

    chain_html = ""
    if ctx.wakeup_chain:
        chain_html = ("<details><summary>唤醒链事件（client 内核收包 → ClientRecv，共 %d 条）</summary><pre>%s</pre></details>"
                      % (len(ctx.wakeup_chain),
                         html.escape("\n".join(e["raw"] for e in ctx.wakeup_chain[:120]))))

    schain_html = ""
    if ctx.server_wakeup_chain:
        schain_html = ("<details><summary>server 侧唤醒链（内核收包 → ServerRecv 协程执行，共 %d 条）</summary><pre>%s</pre></details>"
                      % (len(ctx.server_wakeup_chain),
                         html.escape("\n".join(e["raw"] for e in ctx.server_wakeup_chain[:120]))))

    pcor_html = ""
    for side in ("client", "server"):
        rows = (ctx.preceding_trace_lines or {}).get(side) or []
        if not rows:
            continue
        trs = "".join(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (fmt_dt(ts), html.escape(kind), tid or "-", cpu or "-", bid or "-",
               html.escape(trace_id or "-"), html.escape(raw))
            for ts, kind, tid, cpu, bid, trace_id, raw in rows)
        pcor_html += ("<details><summary>%s 侧前序协程执行轨迹（同 tid %s，%d 条，"
                      "当前锚点 ▶ 标记）</summary>"
                      '<div class="table-wrap"><table class="ev-tbl">'
                      "<tr><th>时间</th><th>锚点</th><th>tid</th><th>cpu</th>"
                      "<th>bid</th><th>trace_id</th><th>日志行</th></tr>%s"
                      "</table></div></details>"
                      % (side, rows[0][2], len(rows), trs))

    trace_parts = []
    for name in ("ClientSend", "ClientRecv", "ServerRecv", "ServerSend"):
        evs = (ctx.thread_traces or {}).get(name) or []
        if evs:
            tid = (ctx.anchors.get(name) or {}).get("tid")
            trace_parts.append("== %s 线程（tid %s）调度轨迹 ==\n%s"
                               % (name, tid, "\n".join(e["raw"] for e in evs[:60])))
    traces_html = ""
    if trace_parts:
        traces_html = ("<details><summary>关键线程调度轨迹（锚点 tid 上下 CPU）</summary>"
                       "<pre>%s</pre></details>"
                       % html.escape("\n\n".join(trace_parts)))

    anchor_html = "".join(
        "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
        % (k, fmt_dt(a["ts"]), a.get("tid") or "-", a.get("cpu") or "-",
           a.get("bid") or "-", html.escape(a["raw"]))
        for k, a in sorted(ctx.anchors.items(), key=lambda kv: kv[1]["ts"]))

    return """
<div class="card trace-card" id="trace%(idx)d">
<div class="trace-head">
<span class="req-index">#%(idx)d</span>
<span class="trace-id">%(trace)s</span>
<span class="badge %(conf_cls)s">置信度：%(conf)s</span>
<span class="badge b-abn">%(label)s</span>
<span class="trace-meta">residual=%(residual)s us　e2e=%(e2e)s us</span>
</div>
<div class="trace-body">
<div class="concl"><b>定界结论：%(label)s</b><ul>%(evid)s</ul>%(sugg)s</div>
<table>%(meta)s</table>
<h3>RPC 宏观分段</h3><table><tr><th>阶段</th><th>耗时</th><th>判定</th></tr>%(macro)s</table>
<h3>全路径时间线与内核级分段（业务 ↔ 协议栈 ↔ 网卡）</h3>
%(timeline)s
%(pwire)s
<table><tr><th>阶段</th><th>区间</th><th>耗时</th><th>阈值</th><th>判定</th></tr>%(segs)s</table>
<h3>RPC 锚点日志</h3><table><tr><th>锚点</th><th>时间</th><th>tid</th><th>cpu</th><th>bid</th><th>日志行</th></tr>%(anchors)s</table>
%(warn)s
%(chain)s
%(schain)s
%(pcor)s
%(irqoff)s
%(sar)s
%(bthread)s
%(cpubusy)s
%(traces)s
<details><summary>client 节点 bpf 事件明细（问题请求相关 / 慢段时间窗 / 问题时间窗全景）</summary>%(cev)s</details>
<details><summary>server 节点 bpf 事件明细（问题请求相关 / 慢段时间窗 / 问题时间窗全景）</summary>%(sev)s</details>
</div>
</div>""" % {
        "idx": idx, "trace": html.escape(ctx.trace_id), "conf_cls": conf_cls,
        "conf": c.get("confidence", "-"), "label": html.escape(c.get("label", "-")),
        "residual": f.get("network_residual_us", "-"),
        "e2e": f.get("e2e_us", "-"),
        "evid": "".join("<li>%s</li>" % html.escape(e) for e in c.get("evidence", [])),
        "sugg": "<b>建议：</b><ul>%s</ul>" % "".join(
            "<li>%s</li>" % html.escape(s) for s in c.get("suggestions", [])),
        "meta": meta_html, "macro": macro_rows, "segs": seg_rows or
        '<tr><td colspan="5" class="muted">内核事件不足，未生成分段</td></tr>',
        "timeline": _timeline_html(ctx.milestones, ctx.kernel_segments),
        "pwire": _phys_wire_html(ctx),
        "anchors": anchor_html, "warn": warn_html, "chain": chain_html,
        "schain": schain_html, "pcor": pcor_html, "traces": traces_html,
        "irqoff": _irqoff_html(ctx), "sar": _sar_html(ctx),
        "bthread": _bthread_html(ctx), "cpubusy": _cpu_busy_html(ctx),
        "cev": _side_events_html(ctx, "client"),
        "sev": _side_events_html(ctx, "server"),
    }


def generate_report(contexts, args, log_root, aux_stats=None):
    total = len(contexts)
    dist = defaultdict(int)
    conf_dist = defaultdict(int)
    for ctx in contexts:
        dist[ctx.conclusion.get("label", "unknown")] += 1
        conf_dist[ctx.conclusion.get("confidence", "低")] += 1
    dist_rows = "".join("<tr><td>%s</td><td class=\"num\">%d</td></tr>"
                        % (html.escape(k), v)
                        for k, v in sorted(dist.items(), key=lambda x: -x[1]))
    idx_note = ""
    if total > INDEX_MAX_TRACES:
        idx_note = ('<p class="muted">共 %d 条 trace，仅列前 %d 条；'
                    '建议使用 --top N 缩小分析范围。</p>' % (total, INDEX_MAX_TRACES))
        contexts = contexts[:INDEX_MAX_TRACES]
    idx_links = "".join('<li><a href="#trace%d">%s　residual=%sus　%s</a></li>'
                        % (i + 1, html.escape(ctx.trace_id),
                           ctx.slow.fields.get("network_residual_us", "-"),
                           html.escape(ctx.conclusion.get("label", "-")))
                        for i, ctx in enumerate(contexts))
    body = "".join(_trace_html(ctx, i + 1) for i, ctx in enumerate(contexts))
    aux_cards = _irqoff_overview_html(aux_stats) + _nic_overview_html(aux_stats)
    # 汇总统计卡（参考 skill summary-cards 风格）
    cards = [
        ("问题请求总数", total, "#1a1a2e"),
        ("高置信结论", conf_dist.get("高", 0), "#c62828"),
        ("中置信结论", conf_dist.get("中", 0), "#ef6c00"),
        ("低置信结论", conf_dist.get("低", 0), "#757575"),
        ("定界结论类别", len(dist), "#5470c6"),
    ]
    cards_html = "".join(
        '<div class="card"><div class="label">%s</div>'
        '<div class="value" style="color:%s">%s</div></div>'
        % (html.escape(lbl), color, val) for lbl, val, color in cards)
    return """<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<title>网络/调度时延定位分析报告</title><style>%(css)s</style></head>
<body>
<div class="header">
<h1>网络/调度时延定位分析报告</h1>
<div class="meta">日志目录：%(root)s　|　阈值 network_residual_us &gt; %(thr)d us　|　生成时间：%(gen)s</div>
</div>
<div class="wrap">
<div class="summary-cards">%(cards)s</div>
<div class="toolbar">
<button onclick="toggleAllDetails(true)">展开全部</button>
<button onclick="toggleAllDetails(false)">收起全部</button>
</div>
<div class="toc">
<h2>概览</h2>
<p>共识别 <b>%(total)d</b> 条问题请求（network_residual_us 超阈值）。</p>
<table><tr><th>定界结论分布</th><th>数量</th></tr>%(dist)s</table>
<h2>问题请求索引</h2>
<ol>%(idx)s</ol>
%(idx_note)s
<p class="muted">注：跨节点耗时基于各节点日志 wall clock 直接相减，若节点间存在时钟偏差，跨节点段耗时仅供参考。</p>
</div>
%(aux)s
%(body)s
</div>
<script>
function toggleAllDetails(open){
  var ds=document.querySelectorAll('details');
  for(var i=0;i<ds.length;i++){ds[i].open=open;}
}
/* 慢段时间窗事件过滤选择（事件委托，全部 evf 表共用一套监听）：
   按钮切归属过滤（all/mine/other），输入框做关键字过滤；
   行按 data-o 归属属性 + 文本内容匹配切换 display。 */
function evfApply(box){
  var f=box.getAttribute('data-f')||'all';
  var q=(box.getAttribute('data-q')||'').toLowerCase();
  var trs=box.querySelectorAll('tr[data-o]');
  var shown=0;
  for(var i=0;i<trs.length;i++){
    var tr=trs[i];
    var ok=(f==='all'||tr.getAttribute('data-o')===f)&&
           (!q||tr.textContent.toLowerCase().indexOf(q)>=0);
    tr.style.display=ok?'':'none';
    if(ok)shown++;
  }
  var c=box.querySelector('.evf-count');
  if(c){c.textContent='显示 '+shown+' / '+trs.length+' 条';}
}
document.addEventListener('click',function(e){
  var b=e.target&&e.target.closest?e.target.closest('.evf-btn'):null;
  if(!b){return;}
  var box=b.closest('.evf');
  if(!box){return;}
  var bs=box.querySelectorAll('.evf-btn');
  for(var i=0;i<bs.length;i++){bs[i].classList.remove('on');}
  b.classList.add('on');
  box.setAttribute('data-f',b.getAttribute('data-f'));
  evfApply(box);
});
document.addEventListener('input',function(e){
  var t=e.target;
  if(!t||!t.classList||!t.classList.contains('evf-input')){return;}
  var box=t.closest('.evf');
  if(!box){return;}
  box.setAttribute('data-q',t.value);
  evfApply(box);
});
</script>
</body></html>""" % {
        "css": CSS, "root": html.escape(str(log_root)), "thr": args.residual_threshold,
        "gen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "total": total,
        "cards": cards_html,
        "dist": dist_rows, "idx": idx_links, "idx_note": idx_note, "body": body,
        "aux": aux_cards,
    }


# ── Structured JSON output（原始结果，供其他 skill/工具二次消费） ──────────────

MACRO_LABELS = {"cs_sr": "ClientSend→ServerRecv",
                "sr_ss": "ServerRecv→ServerSend",
                "ss_cr": "ServerSend→ClientRecv"}


def _event_json(ev):
    """bpf 事件 dict → JSON dict（ts ISO 微秒，保留原始行 raw）。"""
    d = {"ts": ev["ts"].isoformat(), "kind": ev["kind"]}
    if "raw" in ev:
        d["raw"] = ev["raw"]
    for k in ("tid", "cpu", "size", "pid", "comm", "target_cpu",
              "prev_comm", "prev_pid", "next_comm", "next_pid",
              "copied_seq", "rcv_nxt", "wakeup_n",
              "seq", "len", "dev", "rc", "tx_seq", "snd_una", "snd_nxt"):
        if k in ev:
            d[k] = ev[k]
    if ev.get("local_ip"):
        d["local"] = "%s:%d" % (ev["local_ip"], ev["local_port"])
        d["peer"] = "%s:%d" % (ev["peer_ip"], ev["peer_port"])
        d["dir"] = ev.get("dir_arrow", "")
    if ev.get("src_ip"):  # 网卡层事件：方向四元组
        d["src"] = "%s:%d" % (ev["src_ip"], ev["src_port"])
        d["dst"] = "%s:%d" % (ev["dst_ip"], ev["dst_port"])
    return d


def _warn_json(w):
    """latency_warn 告警块 → JSON dict。"""
    return {"ts": w["ts"].isoformat() if w.get("ts") else None,
            "cpu": w.get("cpu"), "comm": w.get("comm"), "pid": w.get("pid"),
            "latency_us": w.get("latency_us"), "raw": w.get("raw", [])}


def _irqoff_json(e):
    """irqoff 关中断记录块 → JSON dict。"""
    return {"ts": e["ts"].isoformat() if e.get("ts") else None,
            "irq": e.get("irq"), "cpu": e.get("cpu"), "comm": e.get("comm"),
            "pid": e.get("pid"), "latency_us": e.get("latency_us"),
            "raw": e.get("raw", [])}


def _nic_sample_json(s):
    """sar 网卡采样（窗口内）→ JSON dict（hms 为 24h 制 HH:MM:SS，无日期）。"""
    return {"hms": s["hms"], "dev": s.get("dev"), "rxpck": s["rxpck"],
            "txpck": s["txpck"], "rxkB": s["rxkB"], "txkB": s["txkB"],
            "ifutil": s["ifutil"]}


def _bthread_json(e):
    """brpc bthread 事件 → JSON dict。"""
    return {"ts": e["ts"].isoformat() if e.get("ts") else None,
            "kind": e.get("kind"), "tid": e.get("tid"),
            "bthread_id": e.get("bthread_id"),
            "pending_time_us": e.get("pending_time_us"),
            "target_pending_tasks": e.get("target_pending_tasks"),
            "creation_mode": e.get("creation_mode"), "raw": e.get("raw")}


def _cpu_busy_json(info):
    """问题窗口 cpu 侵占分析结果 → JSON dict。

    window_events 为问题窗口内全部连接的内核事件（match5t：true 问题连接 /
    false 其他连接 / null 无 IP 调度类事件），软中断抢占定界的原始明细。
    """
    def evj(e):
        d = _event_json(e)
        d["match5t"] = e.get("match5t")
        return d

    return {
        "seg_key": info.get("seg_key"), "seg_desc": info.get("seg_desc"),
        "seg_dur_us": info.get("seg_dur_us"),
        "window_start": info["window_start"].isoformat(),
        "window_end": info["window_end"].isoformat(),
        "anchor_name": info.get("anchor_name"), "anchor_tid": info.get("anchor_tid"),
        "anchor_cpu": info.get("anchor_cpu"), "conn": info.get("conn"),
        "n_mine": info.get("n_mine"), "n_other": info.get("n_other"),
        "other_conns": dict(info.get("other_conns") or {}),
        "other_by_cpu": {str(c): n for c, n in (info.get("other_by_cpu") or {}).items()},
        "other_on_cpu": [evj(e) for e in info.get("other_on_cpu") or []],
        "switches_on_cpu": [evj(e) for e in info.get("switches_on_cpu") or []],
        "switched_out": [evj(e) for e in info.get("switched_out") or []],
        "preempt": bool(info.get("preempt")),
        "window_events": [evj(e) for e in info.get("events") or []],
    }


def _slow_seg_json(sw):
    """慢段窗口分析结果 → JSON dict（无瓶颈段/窗口不可得时为 None）。

    sides.<side>.events 为瓶颈段时间窗内该侧全部连接的内核事件（match5t：
    true 问题连接 / false 其他连接 / null 调度类），慢段定位的原始明细。
    """
    if not sw:
        return None

    def evj(e):
        d = _event_json(e)
        d["match5t"] = e.get("match5t")
        return d

    return {
        "seg_key": sw.get("seg_key"), "seg_desc": sw.get("seg_desc"),
        "category": sw.get("category"),
        "window_start": sw["window_start"].isoformat(),
        "window_end": sw["window_end"].isoformat(),
        "dur_us": sw.get("dur_us"),
        "sides": {side: {"events": [evj(e) for e in d["events"]],
                         "n_mine": d["n_mine"], "n_other": d["n_other"]}
                  for side, d in (sw.get("sides") or {}).items()},
    }


def _irqoff_stats_json(st):
    """irqoff 全周期统计 → JSON dict（series/buckets 键转字符串兼容 JSON）。"""
    return {"total": st.get("total", 0), "hardirq_n": st.get("hardirq_n", 0),
            "softirq_n": st.get("softirq_n", 0), "max_us": st.get("max_us", 0),
            "total_us": st.get("total_us", 0),
            "by_comm": st.get("by_comm") or {},
            "by_cpu": {str(c): v for c, v in (st.get("by_cpu") or {}).items()},
            "buckets": {str(b): n for b, n in (st.get("buckets") or {}).items()},
            "series": [[p[0].isoformat(), p[1], p[2], p[3]]
                       for p in (st.get("series") or [])]}


def generate_json(contexts, args, log_root, aux_stats=None):
    """与 HTML 报告同源的结构化原始结果（JSON，UTF-8，ensure_ascii=False）。

    与 HTML 不同：不做任何行数/条数截断（事件明细、告警、唤醒链全量输出），
    作为其他 skill / 分析工具 / 自定义渲染的输入。
    """
    dist = defaultdict(int)
    for ctx in contexts:
        dist[ctx.conclusion.get("category", "unknown")] += 1
    traces = []
    for i, ctx in enumerate(contexts, 1):
        ms = ctx.milestones
        segs = []
        for s in ctx.kernel_segments:
            d = {k: v for k, v in s.items() if not k.startswith("_")}
            if s.get("evidence"):
                d["start_ts"] = s["_start_ts"].isoformat()
                d["end_ts"] = s["_end_ts"].isoformat()
            else:
                d["start_ts"] = ms[s["start"]].isoformat() if s["start"] in ms else None
                d["end_ts"] = ms[s["end"]].isoformat() if s["end"] in ms else None
            segs.append(d)
        macro = [{"key": k, "label": MACRO_LABELS[k], "dur_us": v,
                  "threshold_us": MACRO_THRESHOLDS_US[k],
                  "abnormal": v > MACRO_THRESHOLDS_US[k]}
                 for k, v in ctx.macro.items()]
        bott = ctx.conclusion.get("bottleneck")
        pw = getattr(ctx, "phys_wire", None) or {}
        traces.append({
            "index": i,
            "trace_id": ctx.trace_id,
            "slow": {"ts": ctx.slow.ts.isoformat(), "log_path": ctx.slow.log_path,
                     "pod_dir": ctx.slow.pod_dir, "fields": ctx.slow.fields},
            "client": {"pod_dir": ctx.client_pod_dir, "node": ctx.client_node,
                       "ip": ctx.client_ip},
            "server": {"pod_dir": getattr(ctx, "server_pod_dir", None),
                       "node": ctx.server_node, "ip": ctx.server_ip},
            "conn": ({"client_ip": ctx.conn[0], "client_port": ctx.conn[1],
                      "server_ip": ctx.conn[2], "server_port": ctx.conn[3]}
                     if ctx.conn else None),
            "anchors": {k: {"ts": a["ts"].isoformat(), "tid": a.get("tid"),
                            "cpu": a.get("cpu"), "bid": a.get("bid"),
                            "host": a.get("host"), "pod_dir": a.get("pod_dir"),
                            "log_path": a.get("log_path"), "raw": a.get("raw")}
                        for k, a in sorted(ctx.anchors.items(),
                                           key=lambda kv: kv[1]["ts"])},
            # 全路径时间线点位（按时间序；缺失点位不在此 dict 中，
            # 完整点位序见 point_order / POINT_ORDER）
            "milestones": {k: ctx.milestones[k].isoformat()
                           for k in POINT_ORDER if k in ctx.milestones},
            "point_order": POINT_ORDER,
            "macro_segments": macro,
            "kernel_segments": segs,
            "kernel_events": {side: [_event_json(e) for e in evs]
                              for side, evs in ctx.filtered_events.items()},
            "wakeup_chain": [_event_json(e) for e in ctx.wakeup_chain],
            "server_wakeup_chain": [_event_json(e) for e in ctx.server_wakeup_chain],
            "thread_oncpu_ts": (ctx.thread_oncpu_ts.isoformat()
                                if ctx.thread_oncpu_ts else None),
            "thread_traces": {k: [_event_json(e) for e in evs]
                              for k, evs in ctx.thread_traces.items()},
            "coro_evidence": list(ctx.coro_evidence),
            "preceding_trace_lines": {
                side: [{"ts": r[0].isoformat(), "kind": r[1], "tid": r[2],
                        "cpu": r[3], "bid": r[4], "trace": r[5], "raw": r[6]}
                       for r in rows]
                for side, rows in (ctx.preceding_trace_lines or {}).items()},
            "nic_evidence": list(ctx.nic_evidence),
            "phys_wire": {d: (_phys_wire_json(pw[d]) if pw.get(d) else None)
                          for d in ("s2c", "c2s")},
            "migration": ctx.migration,
            "sched_warnings": {side: [_warn_json(w) for w in ws]
                               for side, ws in ctx.warn_events.items()},
            "irqoff_events": {side: [_irqoff_json(e) for e in evs]
                              for side, evs in ctx.irqoff_events.items()},
            "nic_samples": {side: [_nic_sample_json(s) for s in ss]
                            for side, ss in ctx.nic_samples.items()},
            "bthread_events": {side: [_bthread_json(e) for e in evs]
                               for side, evs in ctx.bthread_events.items()},
            # 问题窗口全景 + cpu 侵占分析（kernel_to_user 段异常的侧才有）
            "cpu_busy": {side: _cpu_busy_json(info)
                         for side, info in (getattr(ctx, "cpu_busy", None) or {}).items()},
            # 慢段窗口：瓶颈段时间窗内全部连接的 bpf 事件（含其他连接）
            "slow_seg_window": _slow_seg_json(getattr(ctx, "slow_seg", None)),
            "missing_evidence": list(ctx.missing),
            "conclusion": {
                "category": ctx.conclusion.get("category"),
                "label": ctx.conclusion.get("label"),
                "confidence": ctx.conclusion.get("confidence"),
                "bottleneck": bott,
                "evidence": ctx.conclusion.get("evidence", []),
                "suggestions": ctx.conclusion.get("suggestions", []),
            },
        })
    doc = {
        "schema": "ds-network-latency-analysis/result",
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "log_root": str(log_root),
        "residual_threshold_us": args.residual_threshold,
        "total_traces": len(contexts),
        "category_distribution": dict(dist),
        "irqoff_stats": {node: _irqoff_stats_json(st)
                         for node, st in ((aux_stats or {}).get("irqoff") or {}).items()},
        "nic_stats": {node: {dev: dict(d) for dev, d in devs.items()}
                      for node, devs in ((aux_stats or {}).get("nic") or {}).items()},
        "notes": ["跨节点耗时基于各节点日志 wall clock 相减，节点间时钟偏差时仅供参考",
                  "kernel_events/sched_warnings/wakeup_chain 为全量输出（未截断）",
                  "irqoff_stats/nic_stats 为全采集周期统计；irqoff_events/"
                  "nic_samples/bthread_events 为各 trace 问题窗口内明细",
                  "cpu_busy 为问题窗口全景 + cpu 侵占分析（kernel_to_user 段异常的侧），"
                  "window_events 含窗口内全部连接的内核事件（match5t 标注归属）"],
        "traces": traces,
    }
    return json.dumps(doc, ensure_ascii=False, indent=2)


# ── Raw log digest（原始日志汇总，供对照报告查看） ────────────────────────────

def generate_raw(contexts, args, log_root, disc, trace_lines):
    """把每个问题请求相关的原始日志汇总为一份带来源标注的文本。

    每个 trace 一节，包含：
      - client/worker INFO 日志中该 trace 的全部行（按来源文件分组）；
      - 两节点 bpf 日志中该请求时间窗内的内核事件原始行；
      - 两节点 latency_warn 窗口内告警块原始行。
    与 HTML 报告一致：trace 数超过 INDEX_MAX_TRACES 时仅输出前 N 条。
    """
    root = Path(log_root)

    def rel(p):
        try:
            return str(Path(p).relative_to(root))
        except ValueError:
            return str(p)

    out = []
    total = len(contexts)
    if total > INDEX_MAX_TRACES:
        out.append("（共 %d 条 trace，仅输出前 %d 条（与报告一致）；"
                   "建议使用 --top N 缩小范围）" % (total, INDEX_MAX_TRACES))
        out.append("")
        contexts = contexts[:INDEX_MAX_TRACES]

    for i, ctx in enumerate(contexts, 1):
        c = ctx.conclusion
        out.append("=" * 80)
        out.append("#%d trace=%s  residual=%sus"
                   % (i, ctx.trace_id,
                      ctx.slow.fields.get("network_residual_us", "-")))
        out.append("结论：%s（置信度:%s）" % (c.get("label", "-"), c.get("confidence", "-")))
        cs, cr = ctx.anchors.get("ClientSend"), ctx.anchors.get("ClientRecv")
        if cs and cr:
            out.append("窗口：%s ~ %s" % (cs["ts"].isoformat(), cr["ts"].isoformat()))
        if ctx.conn:
            out.append("连接：%s:%d <-> %s:%d" % tuple(ctx.conn))
        if ctx.migration:
            m = ctx.migration
            out.append("协程迁移：bid=%s  ServerRecv(tid %s,cpu %s) → ServerSend(tid %s,cpu %s)"
                       % (m["bid"], m["recv_tid"], m["recv_cpu"],
                          m["send_tid"], m["send_cpu"]))
        # 全路径时间线（业务 ↔ 协议栈 ↔ 网卡；缺失点位显式标注）
        ms = ctx.milestones
        out.append("")
        out.append("全路径时间线（业务 ↔ 协议栈 ↔ 网卡，缺失点位标注[缺失]）：")
        for k in POINT_ORDER:
            out.append("  %-22s %-4s %s" % (k, POINT_LAYERS.get(k, "-"),
                                            fmt_dt(ms[k]) if k in ms else "[缺失]"))

        # 物理网卡间线路定界（seq 关联双侧物理网卡点位，触发时输出）
        pw = getattr(ctx, "phys_wire", None) or {}
        for d in ("s2c", "c2s"):
            info = pw.get(d)
            if not info:
                continue
            line_lbl = {"s2c": "server→client", "c2s": "client→server"}[d]
            share_txt = ("%.1f%%" % info["share_pct"]
                         if info.get("share_pct") is not None else "-")
            out.append("")
            out.append("---- 网卡链路定界（物理网卡间，%s，seq=%s）----" % (line_lbl, info["seq"]))
            out.append("  %s 物理网卡 %s 发出：%s"
                       % (info["egress_side"], info.get("egress_dev") or "-",
                          fmt_dt(info["egress_ts"])))
            out.append("  %s 物理网卡 %s 收到：%s"
                       % (info["ingress_side"], info.get("ingress_dev") or "-",
                          fmt_dt(info["ingress_ts"])))
            out.append("  线路耗时：%s（占 %s 线路段 %s）；%s 节点内 %s + %s 节点内 %s"
                       % (fmt_us(info["wire_us"]), line_lbl, share_txt,
                          info["egress_side"], fmt_us(info.get("egress_internal_us")),
                          info["ingress_side"], fmt_us(info.get("ingress_internal_us"))))
            out.append("  判定：%s" % (
                "物理网卡间传输占主导（网卡处理/物理线路），两侧节点内耗时已排除"
                if info["dominant"] else
                "线路段耗时主要在节点内（veth/协议栈），非物理网卡间线路"))
            out.append("  发送侧链路（%s 节点，seq=%s）：" % (info["egress_side"], info["seq"]))
            out.extend("    " + e["raw"] for e in info["egress_chain"])
            out.append("  接收侧链路（%s 节点，seq=%s）：" % (info["ingress_side"], info["seq"]))
            out.extend("    " + e["raw"] for e in info["ingress_chain"])
        out.append("=" * 80)

        # 前序协程执行轨迹（触发 >1ms 协程排队时收集，当前锚点 ▶ 标记）
        for side in ("client", "server"):
            rows = (ctx.preceding_trace_lines or {}).get(side) or []
            if not rows:
                continue
            out.append("")
            out.append("---- 前序协程执行轨迹（%s 侧，同 tid %s，%d 条）----"
                       % (side, rows[0][2], len(rows)))
            out.extend("[%s] %s" % (r[1], r[6]) for r in rows)

        # INFO 行：按 (来源类型, 文件) 分组
        groups = {}
        for source, path, line in trace_lines.get(ctx.trace_id, []):
            groups.setdefault((source, path), []).append(line)
        for (source, path), lines in groups.items():
            label = "client INFO" if source == "client" else "worker INFO"
            out.append("")
            out.append("---- %s 日志：%s（%d 行）----" % (label, rel(path), len(lines)))
            out.extend(lines)

        # bpf 内核事件（时间窗内，五元组过滤）
        for side, node in (("client", ctx.client_node), ("server", ctx.server_node)):
            evs_all = ctx.kernel_events.get(side) or []
            evs_filt = ctx.filtered_events.get(side) or []
            bpath = disc.bpf_by_node.get(node) if node else None
            src = rel(bpath) if bpath else "（bpf 文件未定位）"
            out.append("")
            filt_note = ""
            if len(evs_all) > len(evs_filt):
                filt_note = "（共 %d 条，过滤后 %d 条匹配当前连接五元组）" % (
                    len(evs_all), len(evs_filt))
            out.append("---- bpf 内核日志（%s 节点 %s，时间窗内）：%s（%d 条）%s----"
                       % (side, node or "?", src, len(evs_filt), filt_note))
            out.extend(e["raw"] for e in evs_filt)

        # 调度时延告警（窗口内）
        for side, node in (("client", ctx.client_node), ("server", ctx.server_node)):
            warns = ctx.warn_events.get(side) or []
            if not warns:
                continue
            wpath = disc.warn_by_node.get(node) if node else None
            src = rel(wpath) if wpath else "（告警文件未定位）"
            out.append("")
            out.append("---- 调度时延告警（%s 节点 %s，窗口内）：%s（%d 块）----"
                       % (side, node or "?", src, len(warns)))
            for w in warns:
                out.extend(w.get("raw", []))
                out.append("")

        # 关中断记录（窗口内，>1ms）
        for side, node in (("client", ctx.client_node), ("server", ctx.server_node)):
            evs = ctx.irqoff_events.get(side) or []
            if not evs:
                continue
            ipath = disc.irqoff_by_node.get(node) if node else None
            src = rel(ipath) if ipath else "（irqoff 文件未定位）"
            out.append("")
            out.append("---- 关中断记录（%s 节点 %s，窗口内）：%s（%d 条）----"
                       % (side, node or "?", src, len(evs)))
            for e in evs:
                out.extend(e.get("raw", []))
                out.append("")

        # sar 网卡采样（窗口内）
        for side, node in (("client", ctx.client_node), ("server", ctx.server_node)):
            samples = ctx.nic_samples.get(side) or []
            if not samples:
                continue
            npath = disc.nic_by_node.get(node) if node else None
            src = rel(npath) if npath else "（nic 文件未定位）"
            out.append("")
            out.append("---- sar 网卡采样（%s 节点 %s，窗口内）：%s（%d 条）----"
                       % (side, node or "?", src, len(samples)))
            for s in samples:
                out.append("  %s  %-14s rxpck/s=%-10.2f txpck/s=%-10.2f "
                           "rxkB/s=%-10.2f txkB/s=%-10.2f %%ifutil=%.2f"
                           % (s["hms"], s.get("dev") or "-", s["rxpck"], s["txpck"],
                              s["rxkB"], s["txkB"], s["ifutil"]))

        # bthread 协程事件（窗口内，按锚点 tid 过滤）
        for side in ("client", "server"):
            evs = ctx.bthread_events.get(side) or []
            if not evs:
                continue
            anchor = ctx.anchors.get("ClientRecv" if side == "client" else "ServerRecv")
            tid = (anchor or {}).get("tid")
            out.append("")
            out.append("---- bthread 协程事件（%s 侧，线程 tid=%s，窗口内）：%d 条 ----"
                       % (side, tid if tid is not None else "全部", len(evs)))
            out.extend(e["raw"] for e in evs)

        # 慢段时间窗 bpf 事件（瓶颈段窗口内全部连接，问题五元组 ▶ 标注）
        sw = getattr(ctx, "slow_seg", None)
        if sw:
            for side in ("client", "server"):
                ssw = (sw.get("sides") or {}).get(side)
                if not ssw:
                    continue
                node = ctx.client_node if side == "client" else ctx.server_node
                bpath = disc.bpf_by_node.get(node) if node else None
                src = rel(bpath) if bpath else "（bpf 文件未定位）"
                out.append("")
                out.append("---- 慢段时间窗 bpf 事件（%s 节点 %s，瓶颈段：%s，[%s ~ %s]，"
                           "问题五元组行 ▶ 标注）：%s（%d 条，问题连接 %d / 其他连接 %d）----"
                           % (side, node or "?", sw.get("seg_desc") or sw.get("seg_key"),
                              fmt_dt(sw["window_start"]), fmt_dt(sw["window_end"]),
                              src, len(ssw["events"]), ssw["n_mine"], ssw["n_other"]))
                for e in ssw["events"]:
                    out.append(("▶ " if e.get("match5t") else "  ") + e["raw"])

        # 问题窗口 bpf 事件全景（高亮问题五元组 ▶ 标注，穿插其他请求日志）
        for side, node in (("client", ctx.client_node), ("server", ctx.server_node)):
            info = (getattr(ctx, "cpu_busy", None) or {}).get(side)
            if not info:
                continue
            bpath = disc.bpf_by_node.get(node) if node else None
            src = rel(bpath) if bpath else "（bpf 文件未定位）"
            out.append("")
            out.append("---- 问题窗口 bpf 事件全景（%s 节点 %s，[%s ~ %s]，"
                       "问题五元组行 ▶ 标注）：%s（%d 条，问题连接 %d / 其他连接 %d）----"
                       % (side, node or "?",
                          fmt_dt(info["window_start"]), fmt_dt(info["window_end"]),
                          src, len(info["events"]),
                          info["n_mine"], info["n_other"]))
            for e in info["events"]:
                out.append(("▶ " if e.get("match5t") else "  ") + e["raw"])
        out.append("")
    return "\n".join(out)


# ── Orchestration ─────────────────────────────────────────────────────────────

def _bpf_zero_event_diag(node, windows, results, diag):
    """零事件自动诊断：该节点存在请求窗口却未产出任何事件时，向 stderr
    输出文件/窗口时间范围与匹配计数，区分时间偏移 / IP 不匹配两类根因。"""
    if not windows:
        return
    n_events = sum(len(v) for v in results.values())
    if n_events > 0:
        return

    def _s(x):
        return x.decode("ascii", "replace") if isinstance(x, bytes) else str(x)

    win_lo = _s(min(w.start_tod for w in windows))
    win_hi = _s(max(w.end_tod for w in windows))
    sys.stderr.write("[diag] bpf %s 零事件诊断:\n" % node)
    sys.stderr.write("  文件时间范围: %s ~ %s\n"
                     % (diag.get("file_first_tod") or "?",
                        diag.get("file_last_tod") or "?"))
    sys.stderr.write("  请求窗口范围: %s ~ %s (%d 窗口)\n"
                     % (win_lo, win_hi, len(windows)))
    sys.stderr.write("  读取行 %d, 时间匹配 %d, IP 匹配 %d\n"
                     % (diag.get("n_read", 0), diag.get("n_tod_match", 0),
                        diag.get("n_ip_match", 0)))
    if diag.get("n_tod_match", 0) == 0:
        if (diag.get("file_first_tod") and diag.get("file_last_tod")
                and (win_hi < diag["file_first_tod"]
                     or win_lo > diag["file_last_tod"])):
            hint = ("  → 窗口时间与文件时间无交集：疑似时钟/时区偏移"
                    "（如 UTC vs CST 差 8h），可尝试 --bpf-time-offset-ms 修正"
                    "（bpf 时间 = 日志时间 + 偏移）\n")
        else:
            hint = ("  → 时间有交集但窗口未命中行：疑似时间偏移或日志乱序，"
                    "可尝试 --bpf-time-offset-ms / --seek-slack-s / --bpf-full-scan\n")
    else:
        hint = "  → 时间匹配但连接 IP 均不匹配；窗口内样例连接:\n"
        for s in diag.get("sample_ips", [])[:8]:
            hint += "    %s\n" % s
        hint += ("  请核对锚点行 host IP 与 bpf 连接 IP"
                 "（可能经 service 转发/多网卡/pod IP 变更）\n")
    sys.stderr.write(hint)


def analyze(log_root, residual_threshold=DEFAULT_RESIDUAL_THRESHOLD_US,
            top=None, only_traces=None, window_pad_ms=DEFAULT_WINDOW_PAD_MS,
            sched_pad_ms=DEFAULT_SCHED_PAD_MS, bpf_full_scan=False,
            max_sched_events=DEFAULT_MAX_SCHED_EVENTS, verbose=False,
            workers=1, seek_slack_s=2.0, bpf_time_offset_ms=0):
    """返回 (disc, contexts, trace_lines)。

    trace_lines: {trace_id: [(source, path, line)...]}（问题 trace 的全部
    INFO 行，阶段2+7 合并扫描的副产物，供 --raw 使用）。
    """
    t_start = time.monotonic()
    disc = LogDiscovery(log_root)
    if not disc.client_logs:
        raise FileNotFoundError("在 %s/collected 下未找到 client 日志" % log_root)

    t0 = time.monotonic()
    records = scan_slow_records(disc.client_logs, residual_threshold, only_traces,
                                verbose=verbose, workers=workers)
    if top:
        records = records[:top]
    _stage("阶段1 慢请求扫描: %d 个 client 日志, 命中 %d 条问题请求, %.1fs"
           % (len(disc.client_logs), len(records), time.monotonic() - t0))
    if not records:
        return disc, [], {}

    trace_ids = list(dict.fromkeys(r.trace_id for r in records))
    t0 = time.monotonic()
    anchor_idx, trace_lines = collect_anchor_and_info(
        disc.client_logs, disc.worker_logs, trace_ids,
        verbose=verbose, workers=workers)
    _stage("阶段2 锚点+INFO 合并扫描: %d 个 client + %d 个 worker 日志, %.1fs"
           % (len(disc.client_logs), len(disc.worker_logs), time.monotonic() - t0))

    # 锚点全部前置构建（bpf/warn 窗口依赖 cs/cr 锚点与节点解析）；
    # 用 ctx 索引做窗口键，精确处理同一 trace_id 出现多条 SLOW 记录的情况
    contexts = []
    for i, rec in enumerate(records):
        ctx = TraceContext(rec)
        ctx.idx = i
        ctx.server_pod_dir = None
        build_anchors(ctx, anchor_idx[rec.trace_id]["client"],
                      anchor_idx[rec.trace_id]["worker"])
        contexts.append(ctx)

    # bpf 窗口化预扫描：每节点文件只读一遍（seek 模式仅读窗口簇字节）；
    # bpf_time_offset_ms：bpf 日志时间 = 应用日志时间 + 偏移（时区/时钟修正）
    pad = timedelta(milliseconds=window_pad_ms)
    bpf_off = timedelta(milliseconds=bpf_time_offset_ms)
    node_windows = {}
    for ctx in contexts:
        cs, cr = ctx.anchors.get("ClientSend"), ctx.anchors.get("ClientRecv")
        if not (cs and cr):
            continue
        ctx.client_node = disc.resolve_node(ctx.client_pod_dir)
        if ctx.server_pod_dir:
            ctx.server_node = disc.resolve_node(ctx.server_pod_dir)
        if not (ctx.client_ip and ctx.server_ip):
            continue
        win = (cs["ts"] + bpf_off - pad, cr["ts"] + bpf_off + pad)
        for side, node in (("client", ctx.client_node), ("server", ctx.server_node)):
            if node:
                node_windows.setdefault(node, []).extend(
                    split_window_at_midnight(ctx.idx, side, win[0], win[1],
                                             ctx.client_ip, ctx.server_ip))

    t0 = time.monotonic()
    kernel_results, truncated_keys = {}, set()
    window_net_results, window_net_truncated = {}, set()
    slack_us = int(seek_slack_s * 1000 * 1000)
    bpf_nodes = [n for n in sorted(node_windows) if disc.bpf_by_node.get(n)]
    bpf_jobs = [(str(disc.bpf_by_node[n]), list(node_windows[n]), bpf_full_scan,
                 max_sched_events, verbose, slack_us) for n in bpf_nodes]
    bpf_out = run_parallel(bpf_jobs, workers, func=_bpf_scan_job)
    for node, (res, trunc, wres, wtrunc, diag) in zip(bpf_nodes, bpf_out):
        kernel_results.update(res)
        truncated_keys |= trunc
        window_net_results.update(wres)
        window_net_truncated |= wtrunc
        _bpf_zero_event_diag(node, node_windows[node], res, diag)
    _stage("阶段3 bpf 窗口扫描: %d 个节点文件, %d 组窗口事件, %.1fs"
           % (len(node_windows), len(kernel_results), time.monotonic() - t0))
    for ctx in contexts:
        for side in ("client", "server"):
            if (ctx.idx, side) in truncated_keys:
                ctx.missing.append("%s 节点 bpf 时间窗内调度类事件超过上限 %d 条，"
                                   "已截断（事件明细可能不全）" % (side, max_sched_events))
            if (ctx.idx, side) in window_net_truncated:
                ctx.missing.append("%s 节点 bpf 时间窗内连接类事件超过全景上限 %d 条，"
                                   "已截断（问题窗口全景可能不全）"
                                   % (side, DEFAULT_MAX_WINDOW_NET_EVENTS))

    # warn 窗口扫描：每节点文件只读一遍，窗口外块直接丢弃
    sched_pad = timedelta(milliseconds=sched_pad_ms)
    ctx_warn_windows = {}
    for ctx in contexts:
        cs, cr = ctx.anchors.get("ClientSend"), ctx.anchors.get("ClientRecv")
        if cs and cr:
            ctx_warn_windows[ctx.idx] = (cs["ts"] - sched_pad, cr["ts"] + sched_pad)
    node_warn_windows = {}
    for ctx in contexts:
        w = ctx_warn_windows.get(ctx.idx)
        if not w:
            continue
        for node in (ctx.client_node, ctx.server_node):
            if node:
                node_warn_windows.setdefault(node, {})[ctx.idx] = w
    t0 = time.monotonic()
    node_warn = {}
    for node in sorted(node_warn_windows):
        path = disc.warn_by_node.get(node)
        if path:
            node_warn[node] = scan_warn_windows(path, node_warn_windows[node])
    _stage("阶段4 调度告警扫描: %d 个节点文件, %.1fs"
           % (len(node_warn_windows), time.monotonic() - t0))

    # 阶段4b 辅助日志扫描（可选输入，缺失自动降级）：
    #   irqoff 关中断 / sar 网卡利用率 / brpc bthread 协程
    # 窗口：本侧"收包→用户态取包"段（irqoff/bthread，收包相关段优先）；
    #       整 trace 窗口（sar，带宽评估看问题时段整体）
    t0 = time.monotonic()
    node_irqoff_wins, node_brpc_wins = {}, {}
    for ctx in contexts:
        for side in ("client", "server"):
            node = ctx.client_node if side == "client" else ctx.server_node
            pod = ctx.client_pod_dir if side == "client" else ctx.server_pod_dir
            if not (node or pod):
                continue
            win = _side_recv_window(ctx, side)
            if win is None:
                continue
            if node:
                node_irqoff_wins.setdefault(node, {})[(ctx.idx, side)] = win
            for bp in _brpc_files_for_pod(disc.brpc_by_pod, pod):
                node_brpc_wins.setdefault(bp, {})[(ctx.idx, side)] = win
    node_irqoff_blocks, node_nic_devs, node_brpc_events = {}, {}, {}
    for node, path in disc.irqoff_by_node.items():
        stats, blocks = scan_irqoff(path, node_irqoff_wins.get(node) or {})
        disc.aux_stats["irqoff"][node] = stats
        node_irqoff_blocks[node] = blocks
    for ip, path in disc.irqoff_by_ip.items():  # IP 未映射到节点的也做全周期统计
        disc.aux_stats["irqoff"].setdefault(
            ip, scan_irqoff(path, {})[0])
    for node, path in disc.nic_by_node.items():
        devs = parse_nic_log(path)
        node_nic_devs[node] = devs
        disc.aux_stats["nic"][node] = {dev: _nic_dev_stats(d)
                                       for dev, d in devs.items()}
    for ip, path in disc.nic_by_ip.items():
        disc.aux_stats["nic"].setdefault(
            ip, {dev: _nic_dev_stats(d) for dev, d in parse_nic_log(path).items()})
    for path, wins in node_brpc_wins.items():
        node_brpc_events[path] = scan_bthread_windows(path, wins)
    _stage("阶段4b 辅助日志扫描: irqoff %d / nic %d / brpc %d 个文件, %.1fs"
           % (len(disc.irqoff_by_node) + len(disc.irqoff_by_ip),
              len(disc.nic_by_node) + len(disc.nic_by_ip),
              len(node_brpc_wins), time.monotonic() - t0))

    for ctx in contexts:
        correlate_kernel(ctx, kernel_results, window_net_results)
        build_kernel_segments(ctx)
        _server_pickup_segments(ctx)
        _coroutine_evidence(ctx)
        _nic_segments(ctx)
        _phys_wire_evidence(ctx)  # 物理网卡间线路定界（seq 关联双侧物理网卡点位）
        _cpu_busy_analysis(ctx)   # 问题窗口全景 + cpu 侵占分析（软中断抢占定界）
        # 辅助日志关联：irqoff 块 / sar 窗口样本 / bthread 事件（按锚点 tid 过滤）
        for side in ("client", "server"):
            node = ctx.client_node if side == "client" else ctx.server_node
            key = (ctx.idx, side)
            if node:
                ctx.irqoff_events[side] = node_irqoff_blocks.get(node, {}).get(key, [])
                devs = node_nic_devs.get(node)
                if devs:
                    ctx.nic_samples[side] = _nic_window_samples(ctx, devs, pad)
            pod = ctx.client_pod_dir if side == "client" else ctx.server_pod_dir
            anchor = ctx.anchors.get("ClientRecv" if side == "client" else "ServerRecv")
            tid = (anchor or {}).get("tid")
            tid_i = None
            if tid is not None:
                try:
                    tid_i = int(tid)
                except (TypeError, ValueError):
                    tid_i = None
            for bp in _brpc_files_for_pod(disc.brpc_by_pod, pod):
                evs = node_brpc_events.get(bp, {}).get(key, [])
                if tid_i is not None:
                    evs = [e for e in evs if e.get("tid") == tid_i]
                ctx.bthread_events[side].extend(evs)
        _irqoff_evidence(ctx)
        _nic_util_evidence(ctx)
        _bthread_evidence(ctx)
        if ctx.idx in ctx_warn_windows:
            for side, node in (("client", ctx.client_node), ("server", ctx.server_node)):
                if node:
                    ctx.warn_events[side] = node_warn.get(node, {}).get(ctx.idx, [])
        _preceding_coroutine_evidence(ctx, "server")  # 前序协程轨迹 + latency_warn 关联（server 侧）
        _preceding_coroutine_evidence(ctx, "client")  # 前序协程轨迹 + latency_warn 关联（client 侧）
        ConclusionEngine.conclude(ctx)
        _slow_seg_window_analysis(ctx)  # 慢段窗口提取（依赖结论瓶颈段）
    _stage("阶段5/6 关联与结论: %d 条问题请求, 总耗时 %.1fs"
           % (len(contexts), time.monotonic() - t_start))
    return disc, contexts, trace_lines


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="网络/调度时延定位分析：network_residual_us 超时请求的定位定界")
    ap.add_argument("log_root", help="日志根目录（含 collected/ 等 4 个子目录）")
    ap.add_argument("--residual-threshold", type=int, default=DEFAULT_RESIDUAL_THRESHOLD_US,
                    help="network_residual_us 判定阈值(us)，默认 %(default)s")
    ap.add_argument("--top", type=int, default=None, help="只分析残余时延最大的前 N 条")
    ap.add_argument("--trace", action="append", default=None,
                    help="只分析指定 trace（子串匹配，可多次）")
    ap.add_argument("--window-pad-ms", type=int, default=DEFAULT_WINDOW_PAD_MS,
                    help="bpf 日志过滤时间窗余量(ms)，默认 %(default)s")
    ap.add_argument("--sched-pad-ms", type=int, default=DEFAULT_SCHED_PAD_MS,
                    help="调度告警时间窗余量(ms)，默认 %(default)s")
    ap.add_argument("--bpf-full-scan", action="store_true", default=False,
                    help="bpf 日志禁用时间窗 seek 定位，整文件扫描（时间乱序严重时用）")
    ap.add_argument("--max-sched-events", type=int, default=DEFAULT_MAX_SCHED_EVENTS,
                    help="每 (trace,侧) 保留的调度类事件上限，默认 %(default)s")
    ap.add_argument("--verbose", action="store_true", default=False,
                    help="stderr 输出逐文件扫描进度（路径/大小/命中/耗时）")
    ap.add_argument("--workers", type=int,
                    default=min(16, os.cpu_count() or 1),
                    help="文件级并行进程数，默认 min(16, CPU核数)（1=串行）")
    ap.add_argument("--seek-slack-s", type=float, default=2.0,
                    help="bpf seek 模式前后余量(秒)，默认 %(default)s（乱序严重时调大）")
    ap.add_argument("--bpf-time-offset-ms", type=int, default=0,
                    help="bpf 日志时间与应用日志时间的偏移(ms)：bpf=日志+偏移"
                         "（时区/时钟修正，如 UTC 日志 vs CST 内核差 8h 传 28800000）")
    ap.add_argument("-o", "--output", default="network_latency_report.html",
                    help="输出 HTML 报告路径")
    ap.add_argument("--json", default=None, dest="json_path",
                    help="输出结构化 JSON 结果路径（原始数据不截断，供其他工具/skill 二次消费）")
    ap.add_argument("--raw", default=None, dest="raw_path",
                    help="输出原始日志汇总路径（每问题请求一节，标注日志来源，供对照报告查看）")
    ns = ap.parse_args(argv)

    disc, contexts, trace_lines = analyze(
        ns.log_root, ns.residual_threshold, ns.top,
        ns.trace, ns.window_pad_ms, ns.sched_pad_ms,
        bpf_full_scan=ns.bpf_full_scan,
        max_sched_events=ns.max_sched_events,
        verbose=ns.verbose,
        workers=ns.workers,
        seek_slack_s=ns.seek_slack_s,
        bpf_time_offset_ms=ns.bpf_time_offset_ms)
    if not contexts:
        print("未发现 network_residual_us > %d 的问题请求" % ns.residual_threshold)
        return 1

    aux_stats = disc.aux_stats if disc is not None else None
    report = generate_report(contexts, ns, ns.log_root, aux_stats=aux_stats)
    out = Path(ns.output)
    out.write_text(report, encoding="utf-8")
    if ns.json_path:
        jout = Path(ns.json_path)
        jout.write_text(generate_json(contexts, ns, ns.log_root,
                                      aux_stats=aux_stats), encoding="utf-8")
    if ns.raw_path:
        # trace_lines 已在阶段2 合并扫描中一并收集（无需再扫一遍日志）
        rout = Path(ns.raw_path)
        rout.write_text(generate_raw(contexts, ns, ns.log_root, disc, trace_lines),
                        encoding="utf-8")

    print("分析完成：%d 条问题请求" % len(contexts))
    for i, ctx in enumerate(contexts, 1):
        c = ctx.conclusion
        print("  #%d %s residual=%sus → %s（置信度:%s）"
              % (i, ctx.trace_id, ctx.slow.fields.get("network_residual_us", "-"),
                 c.get("label"), c.get("confidence")))
    print("报告已生成: %s" % out.resolve())
    if ns.json_path:
        print("JSON 结果已生成: %s" % jout.resolve())
    if ns.raw_path:
        print("原始日志汇总已生成: %s" % rout.resolve())
    return 0


if __name__ == "__main__":
    sys.exit(main())
