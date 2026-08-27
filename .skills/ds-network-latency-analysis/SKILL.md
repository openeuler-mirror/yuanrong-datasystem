---
name: ds-network-latency-analysis
description: >
  Network / scheduling latency localization analysis for k8s collected logs.
  Scans client logs for BRPC_RPC_FRAMEWORK_SLOW requests whose network_residual_us
  exceeds a threshold, correlates trace anchors across client/worker INFO logs with
  per-node bpftrace kernel logs (dscollect_log) and scheduling latency warnings
  (latency_warn_log), rebuilds the per-request full-path timeline (user space →
  TCP stack → NIC driver, incl. net_dev_start_xmit / net_dev_xmit /
  netif_receive_skb / __tcp_retransmit_skb probes), and produces a localization
  conclusion with an interactive HTML report. Also correlates three optional
  auxiliary logs from dscollect_log: irqoff latency (>1ms, interrupt-off
  culprit stacks), sar NIC utilization (ethtool + per-second samples), and
  brpc bthread creation/scheduling logs (coroutine queueing evidence). For
  kernel-to-user receive delays, renders a problem-window bpf event panorama
  (problem 5-tuple highlighted amid interleaved other-connection traffic) and
  detects softirq preemption of the business thread's cpu by other requests'
  receive processing.
  Triggers: network_residual_us, network timeout analysis, RPC segment latency,
  bpf kernel event correlation, scheduling latency, latency_warn, nic driver
  latency, tcp retransmit, irqoff, interrupt disabled, 关中断, sar NIC
  utilization, 网卡利用率, bthread, 协程调度, coroutine queueing,
  softirq preemption, 软中断抢占, 收包后业务处理晚, cpu 侵占,
  定位定界, 网络时延分析, 调度问题定位, 网卡收发耗时.
---

# 网络/调度时延定位分析（ds-network-latency-analysis）

对 k8s 集群收集的 client/worker 用户态日志、bpftrace 内核日志、调度时延告警日志
（及可选的关中断 / sar 网卡利用率 / brpc 协程日志）进行自动化关联分析，定位
`network_residual_us` 超时请求的网络/调度瓶颈段，给出定界结论。

**核心脚本**：`scripts/network_latency_analysis.py`（Python3，无第三方依赖）

---

## 输入日志目录约定

```
<log_root>/
├── collected/                  # client 日志，podName 为子目录名（内含 nodeName）
│   └── <podName>/ds_client_*.INFO.*.log
├── collected_worker_logs/      # worker 日志，podName 为子目录名（内含 nodeName）
│   └── <podName>/kvcache.INFO.*.log
├── dscollect_log/              # bpftrace 内核日志，bpf-$nodeName-$nodeIp.log
│   ├── irqoff_latency_$nodeIp.log   # [可选] 关中断 >1ms 日志（块 + 调用栈）
│   ├── nic-$nodeIp.log              # [可选] ethtool 属性 + sar 每秒网卡采样
│   └── <podName>-brpc_client.log    # [可选] brpc bthread 创建/首次调度日志
└── latency_warn_log/           # 调度时延告警，文件名 $nodeName_$nodeIp
```

三类辅助日志均为**可选输入**（`[可选]` 标注）：文件缺失时自动降级跳过，
不影响既有结论（详见"辅助日志关联分析"一节）。

## 使用方法

```bash
python3 <skill_dir>/scripts/network_latency_analysis.py <log_root> \
    [--residual-threshold 1000] [--top N] [--trace <trace_id>] \
    [--window-pad-ms 2] [--sched-pad-ms 10] \
    [--bpf-full-scan] [--max-sched-events 5000] [--verbose] \
    [--workers N] [--seek-slack-s 2] [--bpf-time-offset-ms 0] \
    [-o network_latency_report.html] [--json result.json] [--raw raw_digest.log]
```

| 参数 | 必填 | 说明 |
|---|---|---|
| `log_root` | 是 | 日志根目录（含上述 4 个子目录） |
| `--residual-threshold` | 否 | `network_residual_us` 判定阈值(us)，默认 1000 |
| `--top` | 否 | 只分析残余时延最大的前 N 条（默认全部） |
| `--trace` | 否 | 只分析指定 trace（子串匹配，可多次指定） |
| `--window-pad-ms` | 否 | bpf 日志过滤时间窗前后余量(ms)，默认 2 |
| `--sched-pad-ms` | 否 | 调度告警时间窗前后余量(ms)，默认 10 |
| `--bpf-full-scan` | 否 | 禁用 bpf 时间窗 seek 定位，整文件扫描（时间乱序严重时用） |
| `--max-sched-events` | 否 | 每 (trace,侧) 保留的调度类事件上限，默认 5000 |
| `--workers` | 否 | 并行扫描进程数（按文件分派），默认 1；TB 级日志建议 `min(16, CPU核数)` |
| `--seek-slack-s` | 否 | bpf 时间窗 seek 前后余量(秒)，默认 2；乱序严重可调大或用 `--bpf-full-scan` |
| `--bpf-time-offset-ms` | 否 | bpf 日志时间相对应用日志的偏移修正(ms)，如 8h 时区差传 `28800000`；用于"bpf 未找到 tcp send 事件"的时钟不同步场景 |
| `--verbose` | 否 | stderr 输出逐文件扫描进度（路径/大小/命中/耗时/MB/s） |
| `-o` | 否 | 输出 HTML 报告路径，默认当前目录 |
| `--json` | 否 | 输出结构化 JSON 结果路径（原始数据不截断，供其他工具/skill 二次消费） |
| `--raw` | 否 | 输出原始日志汇总路径（每问题请求一节，标注日志来源，供对照报告查看） |

测试：`python3 <skill_dir>/scripts/test_network_latency_analysis.py`

---

## 结构化 JSON 输出（原始结果）

`--json result.json` 输出与 HTML 报告同源的结构化结果，供其他 skill / 分析工具 /
自定义渲染消费。与 HTML 不同：**事件明细、调度告警、唤醒链全量输出，不截断**；
时间一律 ISO 8601 微秒精度（如 `2026-08-21T21:31:21.060777`）。

```jsonc
{
  "schema": "ds-network-latency-analysis/result",
  "schema_version": 1,
  "generated_at": "…", "log_root": "…",
  "residual_threshold_us": 1000,
  "total_traces": 6,
  "category_distribution": {"client_kernel_to_user_delay": 1, "unknown": 5},
  "irqoff_stats": {"master": {"total": 12, "hardirq_n": 9, "softirq_n": 3, "max_us": 4200,
                              "total_us": 18200, "by_comm": {"kubelet": {"n": 2, "max_us": 2000, "total_us": 3500}},
                              "by_cpu": {"4": {"n": 2, "max_us": 2000}},
                              "buckets": {"1000": 12, "2000": 3},
                              "series": [["2026-08-24T14:31:34.687803", 2000, "kubelet", 4], …]}},
  "nic_stats":   {"master": {"enp38s0f0np0": {"n_samples": 3600, "max_ifutil": 0.2, "avg_ifutil": 0.01,
                                               "peak_hms": "22:30:50", "max_rxpck": 79.0,
                                               "Speed": "100000Mb/s", "Duplex": "Full", "Link detected": "yes"}}},
  "traces": [
    {
      "index": 2, "trace_id": "…",
      "slow":        {"ts": "…", "log_path": "…", "pod_dir": "…", "fields": {…原样 kv…}},
      "client":      {"pod_dir": "…", "node": "master", "ip": "…"},
      "server":      {"pod_dir": "…", "node": "worker13", "ip": "…"},
      "conn":        {"client_ip": "…", "client_port": 37880, "server_ip": "…", "server_port": 31501},
      "anchors":     {"ClientSend": {"ts": "…", "tid": "…", "cpu": "…", "bid": "…", "host": "…", "raw": "…"}, …},  // 按锚点 ts 升序
      "milestones":  {"ClientSend": "…", "ClientTcpSendIn": "…", …},  // 全路径时间线点位（按时间序；缺失点位不在其中）
      "point_order": ["ClientSend", "ClientTcpSendIn", …],            // 全路径点位序（16 点，判定缺失点位的依据）
      "macro_segments":  [{"key": "cs_sr", "label": "…", "dur_us": 91, "threshold_us": 500, "abnormal": false}, …],
      "kernel_segments": [{"key": "client_kernel_to_user", "desc": "…", "start_milestone": "…",
                           "end_milestone": "…", "start_ts": "…", "end_ts": "…", "dur_us": 15798, "threshold_us": 1000, "brpc_queue_exec_us": null,
                           "category": "…", "abnormal": true},
                          {"key": "server_oncpu_to_user", "evidence": true, "dur_us": 5000, "threshold_us": null, …}, …],
      "kernel_events":   {"client": [{"ts": "…", "kind": "tcp_send_in", "tid": 479093, "cpu": 50,
                                      "local": "…:37880", "peer": "…:31501", "dir": "->",
                                      "raw": "原始行"}, …],
                         "server": […]},  // 网卡事件含 src/dst/seq/len/dev/rc，
                                          // 重传含 tx_seq/snd_una/snd_nxt
      "wakeup_chain":    [事件同上格式…],
      "server_wakeup_chain": [事件同上格式…（内核收包 → ServerRecv）],
      "thread_oncpu_ts": "… 或 null（server 协程所在线程上 CPU 时刻）",
      "thread_traces":   {"ServerRecv": [sched 事件…], …},
      "coro_evidence":   ["协程 bid=… 跨线程迁移…", "前序协程 bid=… 在同一 tid … 阻塞…", …],
      "preceding_trace_lines": {"client": [{"ts": "…", "kind": "▶ ClientRecv", "tid": "…",
                                            "cpu": "…", "bid": "…", "trace": "…", "raw": "…"}, …],
                                "server": [… 或空数组]},  // 前序协程轨迹明细（>1ms 触发时）
      "nic_evidence":    ["窗口内检测到 N 次 TCP 重传（client 侧 N / server 侧 N）…", …],
      "phys_wire":       {"s2c": {"seq": N, "egress_side": "server", "egress_dev": "…",
                                  "egress_ts": "…", "ingress_side": "client", "ingress_dev": "…",
                                  "ingress_ts": "…", "wire_us": 6586, "line_us": 6662,
                                  "share_pct": 98.9, "dominant": true,
                                  "egress_internal_us": 19, "ingress_internal_us": 57,
                                  "egress_chain": [事件…], "ingress_chain": [事件…]},
                         "c2s": … 或 null},  // 物理网卡间线路定界（seq 关联，无 nic 点位时为 null）
      "migration":       {"bid": "…", "recv_tid": "…", "recv_cpu": "…", "send_tid": "…", "send_cpu": "…"} 或 null,
      "sched_warnings":  {"client": [{"ts": "…", "cpu": 61, "comm": "…", "pid": 461221,
                                      "latency_us": 4000850, "raw": [调用栈行…]}], "server": […]},
      "irqoff_events":   {"client": [{"ts": "…", "irq": "hardirq", "cpu": 332, "comm": "kubelet",
                                      "pid": 38557, "latency_us": 3000, "raw": [原始块行…]}],
                          "server": […]},  // 问题窗口内关中断记录（缺失时为空数组）
      "nic_samples":     {"client": [{"hms": "22:32:23", "dev": "enp38s0f0np0", "rxpck": 79.0,
                                      "txpck": 66.0, "rxkB": 16.75, "txkB": 13.88, "ifutil": 0.1}],
                          "server": […]},  // 窗口内 sar 采样（hms 为 24h 制）
      "bthread_events":  {"client": [{"ts": "…", "kind": "scheduled", "tid": 523, "bthread_id": 3693671876360,
                                      "pending_time_us": 4900, "target_pending_tasks": null,
                                      "creation_mode": null, "raw": "原始行"}],
                          "server": […]},  // 窗口内 bthread 事件（按锚点 tid 过滤）
      "cpu_busy":        {"client": {"seg_key": "client_kernel_to_user", "seg_desc": "…",
                                     "seg_dur_us": 15798, "window_start": "…", "window_end": "…",
                                     "anchor_name": "ClientRecv", "anchor_tid": "523", "anchor_cpu": 50,
                                     "conn": "…:37880 <-> …:31501", "n_mine": 8, "n_other": 42,
                                     "other_conns": {"…:33300 <-> …:31501": 30},
                                     "other_by_cpu": {"50": 12, "61": 20},
                                     "other_on_cpu": [{…事件，match5t: false…}],
                                     "switches_on_cpu": [{…sched_switch 事件…}],
                                     "switched_out": [{…}],
                                     "preempt": true,
                                     "window_events": [{"ts": "…", "kind": "tcp_recv_in", "cpu": 50,
                                                        "match5t": true, "raw": "原始行"}, …]}},
                                     // 问题窗口全景 + cpu 侵占分析（kernel_to_user 段异常的侧才有；
                                     // window_events 为窗口内全部连接的内核事件，软中断抢占定界明细）
      "slow_seg_window": {"seg_key": "client_user_to_kernel", "seg_desc": "…",
                          "category": "…", "window_start": "…", "window_end": "…",
                          "dur_us": 15798,
                          "sides": {"client": {"events": [{…事件，match5t: true/false…}],
                                                "n_mine": 8, "n_other": 42}}},
                          // 慢段时间窗（瓶颈段窗口内涉及侧全部连接的 bpf 事件，
                          // match5t 标注归属；无瓶颈段/窗口不可得时为 null）
      "missing_evidence": ["…"],
      "conclusion": {"category": "client_kernel_to_user_delay", "label": "…",
                     "confidence": "高", "bottleneck": {…瓶颈分段…},
                     "evidence": ["…"], "suggestions": ["…"]}
    }
  ]
}
```

`category` 取值见下方"定界分类"表；`schema_version` 变更遵循向后兼容原则。

---

## 原始日志汇总（--raw）

`--raw raw_digest.log` 把每个问题请求相关的**所有原始日志**汇总为一份带来源标注的
文本，方便对照 HTML 报告查看原始信息。每个 trace 一节：

```
================================================================================
#2 trace=getBuffer-...;117c5c4a91c7  residual=15989us
结论：client 收包后唤醒/用户态取包慢（置信度:高）
窗口：2026-08-21T21:31:21.060757 ~ 2026-08-21T21:31:21.077001
连接：192.168.219.138:37880 <-> 192.168.102.161:31501

全路径时间线（业务 ↔ 协议栈 ↔ 网卡，缺失点位标注[缺失]）：
  ClientSend             业务  21:31:21.060757
  ClientTcpSendIn        协议栈 21:31:21.060777
  ClientDevStartXmit     网卡  [缺失]
  ...
================================================================================

---- client INFO 日志：collected/kvchachjpclient-2-master_26/ds_client_....log（16 行）----
<该 trace 在 client 日志中的全部行，含业务中间行，原始内容不变>

---- worker INFO 日志：collected_worker_logs/kvchachjpworker-0-worker13/kvcache....log（9 行）----
<该 trace 在 worker 日志中的全部行>

---- bpf 内核日志（client 节点 master，时间窗内）：dscollect_log/bpf-master-....log（1551 条）----
<该请求时间窗内的 bpf 内核事件原始行>

---- bpf 内核日志（server 节点 worker13，时间窗内）：dscollect_log/bpf-worker13-....log（2044 条）----
...

---- 调度时延告警（client 节点 master，窗口内）：latency_warn_log/master_...（N 块）----
<窗口内告警块原始行（含调用栈）>

---- 关中断记录（client 节点 master，窗口内）：dscollect_log/irqoff_latency_...log（N 条）----
<窗口内关中断记录原始块（COMMAND 头 + 调用栈，>1ms）>

---- sar 网卡采样（client 节点 master，窗口内）：dscollect_log/nic-...log（N 条）----
  22:32:23  enp38s0f0np0  rxpck/s=79.00      txpck/s=66.00      rxkB/s=16.75       txkB/s=13.88       %ifutil=0.10

---- bthread 协程事件（client 侧，线程 tid=523，窗口内）：N 条 ----
<窗口内该线程的 bthread created / first scheduled 原始行>

---- 慢段时间窗 bpf 事件（client 节点 master，瓶颈段：client 发送内核入队→协议栈，
----   [21:31:21.060757 ~ 21:31:21.060777]，问题五元组行 ▶ 标注）：
----   dscollect_log/bpf-master-....log（12 条，问题连接 3 / 其他连接 9）----
▶ 21:31:21.060757 tcp  send in  tid 523 cpu 50 size 16 192.168.219.138:37880 -> 192.168.102.161:31501
  21:31:21.060760 tcp  send in  tid 881 cpu 50 size 64 10.0.0.9:40120 -> 10.0.0.8:31501   ← 瓶颈段窗口内穿插的其他请求
  ...

---- 问题窗口 bpf 事件全景（client 节点 master，[21:31:21.061000 ~ 21:31:21.077001]，
----   问题五元组行 ▶ 标注）：dscollect_log/bpf-master-....log（50 条，问题连接 8 / 其他连接 42）----
▶ 21:31:21.061203 tcp  recv in  tid 523 cpu 50 size 16 192.168.219.138:37880 <- 192.168.102.161:31501
  21:31:21.061350 tcp  recv in  tid 881 cpu 50 size 64 10.0.0.9:40120 <- 10.0.0.8:31501   ← 穿插的其他请求
  ...
```

说明：
- INFO 行按 trace_id 精确匹配收集（相似 trace 不误收），含业务中间行，不只锚点；
- bpf 行为该请求时间窗内的全部内核事件（与报告"内核事件明细"一致）；
- 来源标注为相对 `log_root` 的路径，trace 编号与 HTML 报告索引一一对应；
- trace 数超过 2000 时与报告一致仅输出前 2000 条（提示用 `--top` 缩小范围）。

---

## 分析流程（自动执行用户手工 7 步方法）

1. **问题请求识别**：扫描 client 日志 `[BRPC_RPC_FRAMEWORK_SLOW]` 行，过滤
   `network_residual_us` 超阈值请求，按残余时延降序排列。
2. **RPC 锚点关联**：用 SLOW 行的 `ClientSend=/ClientRecv=/ServerSend=/ServerRecv=`
   单调时钟值精确匹配 client/worker 日志中的
   `yyl9 ClientSend` / `yyl9 ClientRecv` / `yyl3 ServerRecv` / `yyl10 ServerSend`
   锚点行，计算三段宏观耗时（CS→SR、SR→SS、SS→CR），并从锚点行取 pod IP、
   从文件夹名解析 nodeName。
3. **内核日志关联**：在 client/server 节点的 bpf 日志中按
   `[ClientSend−pad, ClientRecv+pad]` 时间窗 + 双 pod IP 过滤；以 ClientSend 后最近的
   `tcp send in` 识别连接四元组（clientIP:ephemeralPort ↔ serverIP:servicePort），
   重建内核级时间线（`tcp send/recv in/out/que`、`sock_def_readable` 等事件）；
   网卡层点位（`dev_start_xmit`/`net_dev_xmit`/`netif_receive_skb`/`__tcp_retransmit_skb`）
   按方向四元组双向匹配接入同一时间线。
4. **调度证据**：解析两节点 latency_warn 告警（`resched_latency_warn Triggered`
   块），过滤问题时间窗；并在 bpf 日志中重建唤醒链
   `sock_def_readable → sched_waking → sched_wakeup → sched_switch`。
5. **辅助日志证据**（可选，缺失自动降级）：关联 irqoff 关中断记录（问题窗口内
   "关中断的人"）、sar 网卡利用率采样（佐证/排除带宽瓶颈）、brpc bthread
   创建/首次调度事件（按锚点 tid 过滤，佐证协程排队）——见"辅助日志关联分析"。
6. **分段耗时**：生成 7 段内核级分段 + 3 段宏观分段（见下表）+ 网卡层/协程调度
   证据分段（不参与异常竞争）。
7. **问题窗口全景与 CPU 侵占分析**：某侧"内核收包 → 用户态取包"段异常时，
   展示该侧问题窗口内**全部连接**的 bpf 事件（问题五元组高亮，可见穿插的
   其他请求），并分析业务线程所在 cpu 是否被收包软中断抢占——见
   "问题窗口全景与 CPU 侵占分析"。
8. **定界结论**：按阈值判定异常段，取最大异常段为瓶颈，叠加调度告警/唤醒链/
   关中断/网卡利用率/协程排队/软中断抢占证据，给出结论、证据链、置信度
   （高/中/低）与排查建议。
9. **慢段窗口提取**：按定界结论的瓶颈段（"慢的具体位置"）确定时间窗与
   涉及侧，提取窗口内全部连接 bpf 事件 + 问题请求相关事件，供 bpf 事件
   明细三子项展示——见"慢段时间窗与问题请求相关事件"。

### 内核级分段与默认阈值

| 分段 | 区间 | 异常阈值 |
|---|---|---|
| client 用户态→内核 | ClientSend → ClientTcpSendIn | > 100 us |
| client→server 线路 | ClientTcpSendIn → ServerTcpRecv(首) | > 200 us |
| server 内核→用户态 | ServerTcpRecv(末) → ServerRecv | > 100 us |
| server 业务处理 | ServerRecv → ServerSend | 对比 BRPC queue+exec（>max(500, 2×)） |
| server 用户态→内核 | ServerSend → ServerTcpSendIn | > 100 us |
| server→client 线路 | ServerTcpSendIn → ClientTcpRecv(首) | > 200 us |
| client 内核→用户态 | ClientTcpRecv(末) → ClientRecv | > 1000 us |

### 定界分类

| 分类 | 含义 |
|---|---|
| client_user_to_kernel_delay | client 用户态发送路径慢 |
| network_c2s_transmission | client→server 网络传输慢 |
| server_kernel_to_user_delay | server 收包后唤醒/调度慢（内核侧主导） |
| coroutine_schedule_delay | server 协程调度排队慢：内核唤醒/线程上 CPU 正常，但 bthread 协程等待 worker 线程执行（细分自 server_kernel_to_user） |
| server_processing_slow | server 业务处理慢（结合 server_req_queue_us/server_exec_us 细分） |
| server_user_to_kernel_delay | server 发送路径慢 |
| network_s2c_transmission | server→client 网络传输慢 |
| network_c2s_phys_wire_delay | client→server 物理网卡间传输慢（网卡处理/物理线路，两侧节点内已排除；细分自 network_c2s_transmission） |
| network_s2c_phys_wire_delay | server→client 物理网卡间传输慢（网卡处理/物理线路，两侧节点内已排除；细分自 network_s2c_transmission） |
| client_kernel_to_user_delay | client 收包后唤醒/用户态取包慢 |
| client_to_server_path / server_to_client_path | 仅有宏观三段时的粗粒度定界 |
| unknown | 证据不足（如 worker 日志未收集） |

---

## 协程调度视角分析（新格式日志：锚点行含 cpu/bid）

client/worker 锚点行新增 `cpu N`（打点时刻所在 CPU）与 `bid N`（bRPC bthread
协程号，进程内唯一）后，自动启用以下增强（旧格式日志自动降级跳过，不影响原结论）：

**server 侧唤醒链**（此前仅 client 侧）：
`tcp recv que → sock_def_readable → sched_waking → sched_wakeup → sched_switch`
重建"协议栈收包 → 协程开始执行"的内核链路，并以 ServerRecv 锚点 tid 精确匹配
`sched_switch(next_pid==tid)` 定位**协程所在线程上 CPU 时刻**（ThreadOnCpu；
容器 PID namespace 与 host 不一致时退化为链式推导）。

**收包→执行 三段细分**（证据性分段，不参与既有 7 段异常竞争）：

| 子段 | 区间 | 含义 |
|---|---|---|
| server_recvq_to_readable | ServerTcpRecvQue → ServerSockReadable | 协议栈收包排队→唤醒 |
| server_readable_to_oncpu | ServerSockReadable → ThreadOnCpu | 内核唤醒+线程调度等待 |
| server_oncpu_to_user | ThreadOnCpu → ServerRecv | **协程调度排队**（bthread 等待 worker 线程） |

当瓶颈段为 server 内核→用户态 且 `协程排队耗时 > 内核唤醒耗时` 时，定界细分改判为
`coroutine_schedule_delay`（区别于内核调度慢的 server_kernel_to_user_delay）。

**协程迁移检测**：ServerRecv 与 ServerSend 同 bid 但 tid/cpu 不同 → 协程处理期间
发生跨线程迁移（bthread yield 后被另一 worker 线程 resume），输出迁移详情。

**CPU 一致性证据**：锚点 cpu 与 bpf 内核事件 cpu 对比——用户态发送 vs 内核 tcp send
入口跨核、收包软中断 vs 业务协程执行跨核（NUMA/缓存亲和性提示）。

**关键线程调度轨迹**：按各锚点 tid 过滤 bpf sched_switch/sched_waking/wakeup，
展示线程在锚点时刻前后 ±10ms 的上下 CPU 轨迹（报告折叠区 + JSON `thread_traces`）。

**前序协程执行轨迹**（协程调度排队 >1ms 时触发，client/server 双侧）：

协议栈已收包但协程任务很晚才被调度执行时，按锚点 tid 查找同一 worker 线程上
正在执行的前序协程任务。触发条件（>1ms）：

- **server 侧**：优先用线程上 CPU 时刻（bpf sched 推导 `thread_oncpu_ts`）→
  ServerRecv；无 sched 事件时**自动降级**用协议栈收包里程碑
  `ServerTcpRecvFirst` → ServerRecv（证据标注"降级判定"）；
- **client 侧**：`ClientTcpRecvFirst` → ClientRecv（协议栈已收响应但协程很晚执行）。

触发后自动进行以下分析：

1. **前序协程查找**：扫描对应侧 INFO 日志中同一 tid 上的全部收/发锚点
   （ServerRecv/ServerSend 或 ClientRecv/ClientSend，含原始行），定位当前
   锚点之前最近的前序协程，输出其 bid、trace_id、cpu、执行时间窗口，
   作为阻塞证据（JSON `coro_evidence`）。

2. **latency_warn_log 关联**：在协程调度排队窗口内查找 `latency_warn_log`
   中是否有对应时间段的长时间运行任务告警，如有则输出告警详情
   （cpu、comm、pid、latency_us），佐证前序协程阻塞。

3. **轨迹明细区块**：收集窗口内同 tid 的锚点原始日志行（时间升序，上限 20 行，
   当前锚点 `▶` 标记），HTML 报告"前序协程执行轨迹"折叠表、`--raw` 汇总段落、
   JSON `preceding_trace_lines` 字段三处输出，还原线程串行执行序列。

当瓶颈段为 server 内核→用户态 且存在前序协程/告警证据时，定界细分改判为
`coroutine_schedule_delay`。

**同节点双向事件方向校验**：client/worker 同宿主机时，单节点 bpf 日志同时含
双向连接事件，里程碑填充按五元组方向过滤（`tcp_recv_in`/`tcp_recv_que`/
`sock_readable`），避免 server 方向收包误填 `ClientTcpRecvFirst` 等里程碑。

---

## 网卡层全路径时间线（net.bt 新增点位）

bpf 日志（net.bt）新增网卡驱动收发观测点位后，时间线扩展为
**业务软件 → 内核协议栈 → 网卡收发** 全路径。旧版 bpf 日志（无这些点位）自动
降级跳过，不影响原结论。

**新增解析点位**（均为 tracepoint/kprobe，按 src→dst 方向四元组过滤，不限端口）：

| bpf 日志事件 | 事件 kind | 含义 |
|---|---|---|
| `dev_start_xmit` | nic_dev_xmit_start | 驱动发送入口（qdisc 排队后） |
| `net_dev_xmit` | nic_dev_xmit | 驱动发送完成（含 rc 返回码） |
| `netif_receive_skb` | nic_rx_skb | 网卡收包入口（含 veth 转发，同包多 dev 触发多次取最早） |
| `__tcp_retransmit_skb` | tcp_retransmit | TCP 重传（丢包/网络质量证据） |

**新增里程碑**（与连接 IP + 侧别联合判定方向，同点位多次出现取最早）：
`ClientDevStartXmit / ClientNetDevXmit / ServerDevStartXmit / ServerNetDevXmit /
ServerNetifRx / ClientNetifRx`，插入 HTML 时间线。

### 全路径全局时间线（缺失点位标注）

每个异常请求的报告渲染"全路径时间线"（业务 ↔ 协议栈 ↔ 网卡 ↔ 协议栈 ↔ 业务），
按固定点位序（JSON `point_order`，共 16 点）呈现。

**主干点位只选取 net.bt 的 5 个探针**，其余点位在事件明细/细分分段中展开：

| net.bt 探针 | 时间线点位 | 层级 |
|---|---|---|
| `kprobe:tcp_sendmsg` | ClientTcpSendIn / ServerTcpSendIn | 协议栈 |
| `kprobe:tcp_recvmsg` | ServerTcpRecv(首/末) / ClientTcpRecv(首/末) | 协议栈 |
| `tracepoint:net:net_dev_start_xmit` | ClientDevStartXmit / ServerDevStartXmit | 网卡 |
| `tracepoint:net:net_dev_xmit` | ClientNetDevXmit / ServerNetDevXmit | 网卡 |
| `tracepoint:net:netif_receive_skb` | ServerNetifRx / ClientNetifRx | 网卡 |

```
ClientSend → ClientTcpSendIn → ClientDevStartXmit → ClientNetDevXmit
    →（线路）→ ServerNetifRx → ServerTcpRecv(首/末) → ServerRecv → ServerSend
    → ServerTcpSendIn → ServerDevStartXmit → ServerNetDevXmit →（线路）
    → ClientNetifRx → ClientTcpRecv(首/末) → ClientRecv
```

非主干点位不进入时间线，在详细分析中展开：
- `tcp_queue_rcv`（recv que 入队）→ 事件明细 + 协程细分分段
  `server_recvq_to_readable`、网卡证据分段终点用 `TcpRecv(首)`（recvmsg 读到）；
- `sock_def_readable/wakeup`、sched 调度事件 → 唤醒链/线程轨迹/协程细分分段；
- `__tcp_retransmit_skb` → 重传证据 + 事件明细。

- **点位明细表**：全部 16 点位逐行列出（序号/点位/层级[业务|协议栈|网卡]/时间/
  距上一可用点耗时/状态），**缺失点位显式标注"缺失" badge**，不再静默跳过；
  纯锚点（无 bpf 事件）场景同样输出。
- **条形图**：相邻"存在点位"分段；两点之间有缺失点位时，该段加斜纹样式并在
  legend 标注 `⚠ 缺：A、B`，形成全局分段视角（可一眼看出哪个层级缺证据）。

**时间排序**：RPC 锚点日志表、bpf 事件明细（HTML/JSON/raw 三种输出）统一按
时间升序——排序在 `correlate_kernel` 数据层完成（单一事实来源），锚点表按
锚点 ts 排序（ClientSend → ServerRecv → ServerSend → ClientRecv）。

**网卡层证据分段**（evidence 段，不参与 7 段异常竞争，无阈值）：

| 子段 | 区间 | 含义 |
|---|---|---|
| client_stack_to_nic | ClientTcpSendIn → ClientDevStartXmit | client 协议栈发送处理（含 qdisc 排队） |
| client_nic_xmit | ClientDevStartXmit → ClientNetDevXmit | client 驱动发送耗时 |
| server_nic_to_stack | ServerNetifRx → ServerTcpRecvFirst | server 网卡收包→协议栈交付（含 veth 转发/排队/唤醒） |
| client_nic_to_stack | ClientNetifRx → ClientTcpRecvFirst | client 网卡收包→协议栈交付（含 veth 转发/排队/唤醒） |

**TCP 重传证据**：窗口内该连接的 `__tcp_retransmit_skb` 事件按 client/server 侧
统计，写入结论证据链（`◆` 前缀，JSON `nic_evidence`），提示丢包/网络质量问题。

### 物理网卡间线路定界（seq 关联）

k8s 容器网络一次发送经过多个虚拟网卡（pod eth0 → cali* veth → 宿主机物理网卡），
同一报文（`seq` 相同）在两侧节点的网卡事件链可精确关联，从而把"线路段"进一步
分解为**物理网卡间传输**与**两侧节点内（veth/协议栈）**：

- **发送侧物理网卡发出** = 发送起点后同 `seq` 链上**最后一个** `net_dev_xmit`
  （多级 veth 串联时物理网卡最后发出；单网卡时即唯一）；
- **接收侧物理网卡收到** = 同 `seq` 链上**第一个** `netif_receive_skb`
  （物理网卡最先收到，再经 veth 转发到 pod）；
- `wire_us = 收到 − 发出`，与线路段（`TcpSendIn → 对端 TcpRecvFirst`）对比得占比，
  并分解两侧节点内耗时（排除性证据）；
- **判定主导**：`wire_us > 1000us` 且占线路段 ≥ 70% → 物理网卡间传输占主导；
  否则标注"耗时主要在节点内（veth/协议栈）"。

**结论改判**：瓶颈段为 `network_s2c/c2s_transmission` 且该方向物理网卡间占主导时，
定界细分改判为 `network_s2c/c2s_phys_wire_delay`（server→client / client→server
物理网卡间传输慢，两侧节点内耗时已排除），置信度高，证据链给出
`◆ 细分证据（seq=…）`：双侧物理网卡 dev、发出/收到时刻、线路耗时、占比、
两侧节点内耗时分解。任一端缺失 nic 点位时该方向自动跳过（不影响原结论）。

**输出位置**：HTML"网卡链路定界（seq 关联）"块（双向汇总表 + 双侧链路原始事件
折叠区，位于全路径时间线之后）、JSON `phys_wire` 字段（s2c/c2s 双向，含
`egress/ingress` 双侧链路事件）、`--raw` "网卡链路定界" 小节（汇总 + 双侧链路
原始行）。新增里程碑 `ServerPhysNicXmit/ClientPhysNicRx/ClientPhysNicXmit/
ServerPhysNicRx` 与证据分段 `wire_s2c_phys/wire_c2s_phys`（evidence 段，不参与
异常竞争）。

**置信度提升**：定界为 `network_c2s/s2c_transmission` 且存在网卡层佐证（收发点位
或重传证据）时，置信度提升为"高"。

**全量日志**：网卡事件随 bpf 事件明细一并进入 HTML 报告、JSON `kernel_events`
（含 `src/dst/seq/len/dev/rc` 字段）与 `--raw` 原始日志汇总。

---

## 辅助日志关联分析（关中断 / sar 网卡利用率 / brpc 协程）

`dscollect_log/` 下三类**可选**辅助日志，用于问题时刻的中断/带宽/协程排队佐证。
文件缺失时自动降级（静默跳过，不产生 missing 噪音），不影响既有结论；
节点 IP 经 `bpf-<node>-<ip>` 文件名建立 nodeIp→nodeName 映射后反查归属。

### 关中断日志（irqoff_latency_$nodeIp.log）

记录哪个进程的调用栈在什么时刻、哪个 cpu 上关中断了多长时间（>1ms），
并区分 hardirq / softirq。用于定界中断相关问题——如网卡收包慢可能是
关中断导致，可据此找到问题时刻"关中断的人"。

**格式**（块状态机：`hardirq:`/`softirq:` 切换中断类型，`cpu: N` 切换 cpu，
`COMMAND:` 行开一条记录，调用栈行附加其后，LATENCY 统一换算为 us）：

```
hardirq:
cpu: 4
    COMMAND: kubelet PID: 38557 LATENCY: 2ms TIMESTAMP: 2026-08-24 14:31:34.687803
    save_trace.isra.0+0x190/0x1d8 [trace_irqoff]
    ...（调用栈）
softirq:
```

**分析输出**：

- **全周期统计**（HTML 概览"关中断统计"卡 + JSON `irqoff_stats`）：记录数 /
  hardirq / softirq / 最长；`[1ms,2ms,5ms,10ms,20ms,50ms,100ms,500ms]` 分桶
  直方图；按进程 top10（次数/最长/累计）；**SVG 时长散点曲线**（x=时间
  y=时长，top 2000 条最长记录）；Top20 最长记录表。
- **问题窗口关联**（trace 卡"关中断记录"块 + JSON `irqoff_events` + raw 段）：
  窗口取该侧"收包→用户态取包"段（`TcpRecvFirst` 前推 2ms → Recv 锚点；
  无 bpf 收包点位时退化为整 trace 窗口），窗口内记录含完整调用栈。
- **证据句**：窗口内存在关中断记录 → `◆ … 检测到 N 条关中断记录（hardirq X /
  softirq Y），最长：cpu C 被 comm(pid) 关中断 T…`；定界为
  `client/server_kernel_to_user_delay` 或 `coroutine_schedule_delay` 时
  作为佐证把置信度提升为"高"。

### sar 网卡利用率日志（nic-$nodeIp.log）

ethtool 属性（Speed/Duplex/Link detected）+ sar 每秒采样（rxpck/s、txpck/s、
rxkB/s、txkB/s、%ifutil）。用于佐证或**排除**网卡带宽瓶颈。

**格式**：`Settings for <dev>:` 开网卡段 + 属性行；sar 数据行含 12 小时制
时间（AM/PM 自动转 24h）；日期缺失，按各 trace 的 ClientSend 锚点日期组合。

**分析输出**：

- **全周期统计**（HTML 概览"网卡利用率统计"卡 + JSON `nic_stats`）：每节点每
  网卡的 Speed / Duplex / Link detected / 采样数 / 峰值与均值 %ifutil /
  峰值时刻 / 峰值 rxpck/s。
- **问题窗口关联**（trace 卡"sar 网卡采样"块 + JSON `nic_samples` + raw 段）：
  窗口为整 trace 窗口（带宽评估看问题时段整体）；dev 优先取物理网卡定界
  （`phys_wire`）的收发 dev；样本按"时刻覆盖 [S, S+1s) 区间"语义匹配。
- **证据句**：窗口内峰值 %ifutil ≥ 50 → "网卡利用率高"佐证；< 10 → 排除性
  证据（"峰值仅 X%，排除网卡带宽打满"）。

### brpc 协程日志（<podName>-brpc_client.log）

bthread 创建与首次调度日志，统计问题时刻线程上有多少协程在排队。用于佐证
"协议栈收包后业务执行晚"的协程调度排队问题（`pending_time_us` = 创建到首次
执行）。

**格式**（glog 行，无年份，按 trace 窗口日期 ±1 年容错组合）：

```
I0824 22:32:23.661136  6267 4294969346 task_group.cpp:520 start_foreground] [WZY] bthread created: creator_tid=6267 bthread_id=3693671876360 creation_time_ns=... creation_mode=foreground target_local_pending_tasks=0 target_remote_pending_tasks=0 target_pending_tasks=0
I0824 22:32:23.661172  6267 3693671876360 task_group.cpp:383 task_runner] [WZY] bthread first scheduled: worker_tid=6267 bthread_id=3693671876360 fn=... arg=... creation_time_ns=... first_run_time_ns=... pending_time_us=37
```

**分析输出**（仅问题窗口内统计，不做全周期统计——日志量大）：

- **问题窗口关联**（trace 卡"bthread 协程事件"块 + JSON `bthread_events` +
  raw 段）：窗口同 irqoff（"收包→用户态取包"段），事件按**锚点 tid 过滤**
  （client 侧 ClientRecv tid / server 侧 ServerRecv tid），窗口外行只做
  行首快速预判即跳过。
- **证据句**（JSON `coro_evidence`）：该线程在窗口内的 created / scheduled
  数、`pending_time_us` max/avg、`target_pending_tasks` max、>1ms 的 top5
  记录 → "线程 tid=N 窗口内创建 X 个协程、Y 次首次调度，最长排队 T…"
  ——佐证"协议栈已收包但协程很晚才执行"。
- **置信度**：定界为 `client/server_kernel_to_user_delay` 或
  `coroutine_schedule_delay` 且有协程排队证据时提升为"高"。

### 输出位置汇总

| 输出 | 内容 |
|---|---|
| HTML 概览 | "关中断统计（全采集周期）"卡（分桶直方图/进程 top10/SVG 散点/Top20）、"网卡利用率统计（sar）"卡 |
| HTML trace 卡 | "关中断记录（问题窗口内）"（表 + 调用栈折叠）、"sar 网卡采样（问题窗口内）"表、"bthread 协程事件（问题窗口内）"折叠块 |
| JSON 顶层 | `irqoff_stats: {node: {total, hardirq_n, softirq_n, max_us, total_us, by_comm, by_cpu, buckets, series}}`、`nic_stats: {node: {dev: {n_samples, max_ifutil, avg_ifutil, peak_hms, max_rxpck, Speed, Duplex, "Link detected"}}}` |
| JSON trace 级 | `irqoff_events: {client/server: [{ts, irq, cpu, comm, pid, latency_us, raw}]}`、`nic_samples: {…: [{hms, dev, rxpck, txpck, rxkB, txkB, ifutil}]}`、`bthread_events: {…: [{ts, kind, tid, bthread_id, pending_time_us, target_pending_tasks, creation_mode, raw}]}` |
| `--raw` | 每 trace 追加"关中断记录 / sar 网卡采样 / bthread 协程事件"三段（标注来源文件与条数，irqoff 含原始调用栈块） |

---

## 五元组过滤（内核事件展示）

节点内核 bpftrace 日志中通常包含大量与当前连接无关的其他连接事件（同一节点上
多个 pod 共享内核探针）。分析器按**连接五元组**（`客户端IP:端口 ↔ 服务端IP:端口`）
做双向匹配（TCP/sock 按 local↔peer，网卡按 src↔dst），调度事件无连接信息默认保留。

过滤生效范围：
- **JSON 输出**：`kernel_events` 字段使用过滤后事件；
- **raw 输出**：`--raw` 原始日志汇总中 bpf 段标注"共 M 条，过滤后 N 条匹配当前连接五元组"；
- **HTML 报告**：事件明细已升级为**三子项结构**（问题请求相关 / 慢段时间窗 /
  问题时间窗全景，见下节），问题五元组行黄底高亮、其他连接事件直接混排
  展示；无全景数据时（无 bpf 日志/关联失败）兜底展示 `filtered_events`
  过滤版。

注意：里程碑填充（`_fill_milestone`）仍使用全量 `kernel_events`，确保时间线
点位不因过滤而遗漏。

---

## bpf 事件明细：三子项结构（问题请求相关 / 慢段时间窗 / 问题时间窗全景）

HTML trace 卡中"client/server 节点 bpf 事件明细"按**三个子项**分层展示，
从"只看问题请求"到"看瓶颈段时间窗"再到"看整个问题时间段"逐层放宽：

| 子项 | 数据范围 | 用途 |
|---|---|---|
| ① 问题请求相关事件 | 仅**问题连接五元组**事件 + **关键线程**（锚点 tid）调度事件 | 单独追踪问题请求自身的日志（无其他连接干扰） |
| ② 慢段时间窗事件 | **瓶颈段**时间窗内全部连接事件 | 聚焦"慢的具体位置"——client/server 在哪一段请求收包慢，就看那一段窗口内的所有 bpf 日志 |
| ③ 问题时间窗全景 | ClientSend→ClientRecv 整窗全部连接事件 | 看问题时间段穿插的其他请求（全局视角） |

- **问题五元组高亮**：三个子项的问题连接行均**黄底高亮**（`hl5t`），归属列
  标注"问题连接 / 其他连接 / -（调度类）"，表头注"共 N 条：问题连接 M 条 /
  其他连接 K 条"——既可追踪问题请求相关日志，又能看到问题时间段穿插的
  其他请求日志；
- **过滤选择**：事件过多时子项②③表头带**过滤工具条**——"全部 / 仅问题
  连接 / 仅其他连接"按钮（归属过滤）+ 关键字输入框（文本过滤），实时
  切换行显示并显示"显示 X / N 条"计数（纯前端，事件委托实现，多表共用
  一套监听）；
- **上限**：HTML 每表展示前 500 条（按时间序，超限标注总数，建议用
  `--trace` 缩小范围）；全景数据层上限 4000 条连接类事件/（trace, 侧）；
- **兜底**：无全景数据时（无 bpf 日志/关联失败）退回 `filtered_events`
  过滤版单表展示。

---

## HTML 报告风格与渲染性能

### 报告风格（对齐 ds-log-deep-analysis 参考风格）

- **头部横幅**：深色渐变 header（标题 + 日志目录/阈值/生成时间）；
- **汇总统计卡**：问题请求总数 / 高中低置信结论数 / 结论类别数；
- **展开收起工具条**：全局"展开全部/收起全部"按钮（原生 `<details>` 切换）；
- **trace 卡片**：卡片头（#序号 + trace_id + 置信度/定界徽章 + residual/e2e
  指标，渐变底色）+ 卡片体；表格深色表头、行 hover；原始日志块为终端风格
  （深底绿字等宽）。

### 渲染性能（大报告展开/收起卡顿优化）

大报告（数百 trace × 多张 500 行事件表）此前展开/收起明显卡顿，根因是
整页大 DOM 全量布局。现从渲染层优化（不改分析逻辑）：

| 优化 | 说明 |
|---|---|
| `content-visibility:auto` | trace 卡片与事件表行跳过视口外渲染（CSS 层），展开 details 只布局可见行 |
| `contain-intrinsic-size` | 跳过渲染的卡片/行预留占位高度，避免滚动条跳动 |
| `table-layout:fixed` + `<colgroup>` | 事件表固定列宽，免去逐 cell 测宽的自动布局开销 |
| `.table-wrap` 滚动容器 | 大表限高 520px 内部滚动，展开不触发整页超长布局 |
| 原生 `<details>/<summary>` | 不用 JS 重排 DOM；全局展开/收起为一次性 `open` 属性切换 |

---

## 问题窗口全景与 CPU 侵占分析（软中断抢占定界）

有一类问题可以定界：**网络收包后，业务处理时间开始得比较晚**——可能是业务
线程被收包软中断抢占。当某侧"内核收包 → 用户态取包"段（`client/server_
kernel_to_user`）超阈值异常时，自动针对该侧触发两项分析：

### 1. 问题窗口 bpf 事件全景（问题五元组高亮）

节点内核 bpf 日志中混杂同节点其他 pod 的流量。此前事件明细按五元组过滤后，
看不到"问题时间段穿插的其他请求"；现在**按问题时间段展示全量内核事件并
高亮问题五元组**，既方便追踪问题请求相关日志，又能看到穿插的其他请求：

- **问题窗口** = [本侧收包开始（`TcpRecvFirst` / `NetifRx` 较早者），
  Recv 锚点时刻]（比 trace 卡事件明细的 ClientSend→ClientRecv 整窗更窄，
  聚焦瓶颈段）；
- **全景事件**：窗口内**全部连接**的 tcp/nic/sock 事件（不限 IP，含其他
  pod 流量）+ sched 类事件（唤醒链/线程轨迹已按 tid 过滤），按时间排序，
  每条标注 `match5t`（true 问题连接 / false 其他连接 / null 调度类事件）；
- **高亮方式**：HTML 全景表问题连接行**黄底高亮**、归属列标注"问题连接/
  其他连接"、业务线程所在 cpu 红色标注；`--raw` 输出问题连接行加 `▶` 前缀；
- **上限**：每 (trace, 侧) 全景保留 4000 条连接类事件（超限截断并在
  `missing_evidence` 标注"问题窗口全景可能不全"，建议用 `--trace` 缩小范围）。

### 2. 问题时间段 CPU 侵占分析

回答"**该时间段业务线程所在 cpu 上是否有在处理其他请求**"：

- **业务线程定位**：Recv 锚点行的 tid（线程）与 cpu（锚点行无 `cpu N`
  字段时提示无法定位，仅输出全景表）；
- **判定抢占**（`preempt = true`，满足其一）：
  - 业务 cpu 上出现**其他连接**的收包/协议栈事件（收包软中断在该 cpu
    处理其他请求）；
  - 业务线程在业务 cpu 上被 `sched_switch` 切出（`prev_pid == tid`，
    被其他任务直接抢占）；
- **排除性结论**：业务 cpu 上无其他连接事件时，明确输出"软中断处理其他
    请求的抢占可能性低"，并给出窗口内其他连接事件的 cpu 分布供参考。

### 证据与结论影响

| 项 | 说明 |
|---|---|
| 证据句 | `◎` 前缀写入结论证据链（JSON `conclusion.evidence`）：窗口/事件统计、业务 cpu 上其他连接事件数（含 top3 连接）、线程被切出次数与切向 |
| 置信度 | 定界为 `client/server_kernel_to_user_delay` 或 `coroutine_schedule_delay` 且存在抢占证据（`cpu_busy_preempt`）时提升为"高" |
| 排查建议 | 抢占成立时追加：调整网卡 RSS/中断亲和性将收包分散到非业务 cpu、业务线程绑核 / isolcpus 隔离 |

### 输出位置

| 输出 | 内容 |
|---|---|
| HTML trace 卡 | "问题窗口 bpf 事件全景与 CPU 侵占分析"块：摘要卡（窗口/业务线程/事件统计/cpu 结论）+ 全景事件表（黄底 = 问题五元组行，红色 cpu = 业务线程所在 cpu） |
| JSON trace 级 | `cpu_busy: {client/server: {seg_key, seg_desc, seg_dur_us, window_start, window_end, anchor_name, anchor_tid, anchor_cpu, conn, n_mine, n_other, other_conns, other_by_cpu, other_on_cpu, switches_on_cpu, switched_out, preempt, window_events[]}}`（`window_events` 每条含 `match5t` 标注，全量不截断） |
| `--raw` | 每 trace 追加"问题窗口 bpf 事件全景"段（标注窗口区间/来源文件/条数，问题连接行 `▶` 前缀，穿插展示其他请求原始日志行） |

---

## 慢段时间窗与问题请求相关事件（bpf 事件明细子项数据源）

对应"bpf 事件明细：三子项结构"中子项①②的数据来源，回答两个问题：

1. **"只显示问题请求相关的日志"** → 问题请求相关事件提取；
2. **"根据 client/server 在哪一段请求收包慢的具体位置，过滤出这段时间
   窗口内的所有 bpf 日志，高亮问题五元组，太多时支持过滤选择"** →
   慢段时间窗事件提取。

### 1. 问题请求相关事件（`_problem_request_events`）

从问题时间窗全景事件（`bpf_window_events`）中筛选：

- **问题连接五元组事件**（`match5t == true`）：该请求自身经手的全部
  tcp/nic/sock 内核事件；
- **关键线程调度事件**：sched 类事件按 tid/pid/prev_pid/next_pid 任一
  匹配该侧锚点线程（client 取 ClientSend/ClientRecv tid，server 取
  ServerRecv/ServerSend tid）判定相关——问题请求业务线程的唤醒/切换轨迹；
- 排除其他连接事件与无关线程调度事件，按时间排序。

### 2. 慢段时间窗事件（`_slow_seg_window_analysis`）

按**定界结论瓶颈段**（`conclusion.bottleneck`，即"慢的具体位置"）过滤：

- **时间窗** = [瓶颈段起点, 终点]：
  - 证据分段取 `_start_ts`/`_end_ts`，内核段取 milestone/锚点时刻
    （起点/终点可为锚点名，如 ServerRecv）；
  - 宏观三段（cs_sr/sr_ss/ss_cr）取对应锚点时刻；
- **涉及侧**：client 单侧段只看 client 节点，server 单侧段只看 server 节点，
  跨节点线路段（wire_c2s/wire_s2c 等）双侧都看——"在哪一段慢就看哪一侧"；
- **窗口内全部连接**的内核事件（含穿插的其他请求），每条带 `match5t`
  归属标注；无瓶颈段或窗口不可得时不生成。

### 3. 客户端过滤选择（事件过多时）

子项②③的事件表头带过滤工具条（纯前端，无请求）：

- **归属过滤按钮**：`全部` / `仅问题连接` / `仅其他连接`——按行
  `data-o` 归属属性切换显示；
- **关键字过滤输入框**：按行文本匹配（不区分大小写），如输入端口号 /
  事件类型 / tid；
- **计数反馈**：过滤后实时显示"显示 X / N 条"；实现为事件委托 + 单次
  遍历切 `display`，多表共用一套监听，无性能开销。

### 输出位置

| 输出 | 内容 |
|---|---|
| HTML trace 卡 | "client/server 节点 bpf 事件明细"三个 `<details>` 子项：①问题请求相关事件（默认展开）②慢段时间窗事件 ③问题时间窗全景（②③带过滤工具条） |
| JSON trace 级 | `slow_seg_window: {seg_key, seg_desc, category, window_start, window_end, dur_us, sides: {client/server: {events[]（每条含 match5t）, n_mine, n_other}}}`（无瓶颈段时为 null，events 全量不截断） |
| `--raw` | 每 trace 追加"慢段时间窗 bpf 事件"段（标注瓶颈段/窗口区间/来源文件/条数/问题连接占比，问题连接行 `▶` 前缀，穿插其他连接原始行） |

---

## 日志格式参考

**client/worker INFO 日志**（管道分隔）：
`wall_ts | I | file:line | hostIP | pid:tid | trace_id | user | msg`

锚点行 msg（新格式在 tid 后追加 `cpu N bid N`，旧格式无此后缀仍兼容）：
`yyl9 ClientSend ts <单调时钟> tid N cpu N bid N`
`yyl3 ServerRecv ts <单调时钟> tid N cpu N bid N`（bid 为 bRPC bthread 协程号）

**bpf 日志**（无日期，取 client 日志日期组合）：
`HH:MM:SS:usec tcp  send|recv in|out|que tid N cpu N size N local:port ->|<- peer:port[, copied_seq..]`
另有 `sock_def_readable`、`sched_waking/sched_wakeup/sched_switch`、`tcpwakeup out`
（其中 sched_* 唤醒链事件量极大、可选采集，关闭后分析自动降级，见注意事项 5）。

**网卡层点位**（net.bt tracepoint，按端口过滤后输出方向四元组）：
`HH:MM:SS:usec dev_start_xmit|net_dev_xmit|netif_receive_skb: sip:S, sport:P -> dip:D, dport:Q, seq:N, len:L, dev:NAME[, rc:R]`
`HH:MM:SS:usec __tcp_retransmit_skb  tid N cpu N size N tx_seq: N, snd_una:N, snd_next: N tcb:seq: N,local:port -> peer:port`
（网卡点位可选采集；无这些点位时网卡层分段/证据自动降级跳过，见"网卡层全路径时间线"）

**latency_warn 日志**：
`[uptime][YYYY-MM-DD HH:MM:SS:usec] !!! resched_latency_warn Triggered !!!` +
`Current CPU: N | Task Comm: X | PID: N, latency: N` + 内核调用栈。

**关中断日志**（irqoff_latency_$nodeIp.log，可选；块 + 调用栈）：
`hardirq:` / `softirq:` / `cpu: N` + `COMMAND: X PID: N LATENCY: 2ms TIMESTAMP: YYYY-MM-DD HH:MM:SS.usec` + 调用栈。

**sar 网卡利用率日志**（nic-$nodeIp.log，可选）：
`Settings for <dev>:` + `Speed:/Duplex:/Link detected:` 属性 +
`HH:MM:SS AM|PM IFACE rxpck/s txpck/s rxkB/s txkB/s rxcmp/s txcmp/s rxmcst/s %ifutil`。

**brpc 协程日志**（<podName>-brpc_client.log，可选；glog 格式）：
`IMMDD HH:MM:SS.usec tid N ... start_foreground] [WZY] bthread created: creator_tid=... bthread_id=... creation_mode=... target_pending_tasks=...` /
`... task_runner] [WZY] bthread first scheduled: worker_tid=... bthread_id=... pending_time_us=...`。

---

## 注意事项

1. **时钟域**：跨节点耗时一律用日志行 wall clock 相减；BRPC 单调时钟 ts
   （ClientSend= 等）仅用于锚点行的精确匹配，不可跨节点相减。若节点间存在时钟
   偏差，跨节点段耗时仅供参考（报告中有提示）。
2. **node 关联**：podIP 与 nodeIP 通常不同网段，只能通过 pod 文件夹名包含
   nodeName 做子串匹配（最长匹配优先，避免 worker1 误配 worker13）。
3. **并发连接**：一个 trace 可能含多次 RPC（如 metadata 查询 + 数据查询），
   分析对象仅为 SLOW 行对应的那次 RPC（按 ts 值匹配锚点，而非仅按 trace_id）。
4. **证据缺失降级**：worker 日志未收集 / bpf 无匹配事件 / 无调度告警时，
   相应分段标注缺失，不中断整体分析（样例中 6 条问题请求仅 1 条有完整 worker 日志，
   其余正确标注"证据不足"）。
5. **唤醒链事件可选（sched_* 兼容）**：`sched_waking/sched_wakeup/sched_switch`
   事件量极大，采集端可关闭。bpf 日志仅含 tcp 事件时：内核时间线/分段/定界结论
   照常输出（置信度按超阈值倍数判定），证据链注明"唤醒链事件缺失（采集可能已
   关闭）"，建议中给出 latency_warn / 线程级 CPU 监控等替代手段。
6. **大规模日志设计**：全流程面向 100GB 级日志设计（见下节），无任何文件被完整
   载入内存；bpf 按 (node, 文件) 只读一遍，warn 告警按窗口流式提取。
7. **辅助日志可选（关中断 / sar 网卡 / brpc 协程）**：三类辅助日志均位于
   `dscollect_log/`，缺失时自动降级静默跳过（不产生 missing 提示噪音）。
   irqoff 全周期统计窗口外记录只计统计不驻留 raw；brpc 协程日志仅做问题窗口内
   统计（日志量大），窗口外行只做行首快速预判即跳过。
8. **问题窗口全景事件上限**：每 (trace, 侧) 全景保留 4000 条连接类事件
   （请求时间窗内连接类事件即收集，扫描阶段一次完成，正常请求不额外扫描）；
   超限截断并写入 `missing_evidence`（"问题窗口全景可能不全"），建议用
   `--trace` 缩小范围后重跑。
9. **慢段时间窗**：慢段窗口从问题时间窗全景数据（`bpf_window_events`）二次
   过滤而来，仅瓶颈段涉及侧生成（client 单侧段不生成 server 侧数据）；
   无瓶颈段（`bottleneck` 为空，如"证据不足"结论）或窗口不可得时
   `slow_seg_window` 为 null、HTML 无"慢段时间窗事件"子项，属正常降级。
   "问题请求相关事件"子项依赖锚点行 tid 字段，锚点行无 tid 时仅展示
   问题连接五元组事件。

---

## 大规模日志性能（面向 ~100GB）

**扫描策略**（内存占用与日志总量无关，仅与"命中数 × 窗口内事件数"相关）：

| 阶段 | 策略 | 说明 |
|---|---|---|
| 慢请求/锚点扫描 | 16MB 字节块 + `bytes.find` 标记定位 | 仅 marker（`[BRPC_RPC_FRAMEWORK_SLOW]`、trace_id）命中的行进入 Python 层，扫描为 C 速度；锚点扫描与 INFO 行汇总合并为单遍 IO |
| bpf 内核日志 | 时间窗 seek 定位 | 按问题请求时间窗合并成簇，二分查找仅读取窗口簇字节（+`--seek-slack-s` 乱序余量，默认 2s）；乱序超限自动回退全扫（`--bpf-full-scan` 可强制） |
| latency_warn | 流式窗口过滤 | 告警块逐块解析，窗口外块直接丢弃，调用栈仅保留关键行 |
| 全阶段 | 文件级多进程并行 | `--workers N` 按 (文件, 阶段) 分派进程池（fork），client/worker/bpf 日志并行扫描 |

**内存与报告上限**：

- 每 (trace, 侧) 调度类事件上限 `--max-sched-events`（默认 5000），超出截断并提示；
- HTML 事件明细表超过 500 行只渲染前 500 行（标注"共 M 条，仅列前 N 条"）；
- 报告索引/正文超过 2000 条 trace 只渲染前 2000 条，提示用 `--top` 缩小范围。

**大日志使用建议**：

- 先 `--top 20 --workers 16 --verbose` 快速定位最严重请求，再 `--trace <id>` 深挖；
- bpf 时间乱序告警频繁时加 `--bpf-full-scan`（牺牲时间换正确性）；
- 阶段进度始终输出到 stderr（`--verbose` 附逐文件 MB/s 吞吐）。

**bpf 零事件自动诊断**：某节点窗口事件为 0 时自动输出诊断（文件/请求窗口时间范围、
读取行数、时间匹配数、IP 匹配数、样例连接），用于区分两类根因：
- 时间匹配为 0 → 节点时钟/时区偏移，用 `--bpf-time-offset-ms` 修正（如
  bpf 日志为 UTC 而应用日志为 CST(+8) 时传 `28800000`）；
- 时间匹配但 IP 匹配为 0 → pod IP 未被 bpf 采集覆盖或经 NAT/代理，核对连接四元组。

**实测吞吐**（合成 2GB 日志，单机单线程，仅供参考）：

- 慢请求/锚点标记扫描：200~600 MB/s（bytes.find 主导，内存带宽量级）；
- bpf 窗口扫描：与日志总量无关，正比于窗口数（2GB 日志 10 窗口 < 1s；
  seek 二分定位 + 行首 tod bytes 预过滤，slack 区行不进解析）；
- warn 流式窗口过滤：~30 MB/s（窗口外块快进跳过，不做 regex）；
- 整体 2.02GB → 约 11s，峰值内存 130MB（与日志总量无关）；
- 外推 100GB（磁盘顺序读 ~250MB/s 下限估计）：约 8~12 分钟，内存不变。

---

## 样例验证

对 `/home/wcy/log/mini_log` 样例的分析结果与手工分析完全一致
（trace `getBuffer-25487-00004775;117c5c4a91c7`）：

```
ClientSend .060757
 │ 20 us     client 用户态→内核（正常）
 │ 45 us     线路传输（正常）
 │ 21 us     server 内核→用户态（正常）
 │ 248 us    server 处理（正常）
 │ 4 us      server 用户态→内核（正常）
 │ 95 us     线路传输（正常）
 │ 15.798 ms ← 主要异常：client 内核收包完成→用户态取包
ClientRecv .077001
结论：client 收包后唤醒/用户态取包慢（置信度：高）
```
