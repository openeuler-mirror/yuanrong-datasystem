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
  conclusion with an interactive HTML report. Supports in-place extraction of
  tar.gz bpf log archives (multi-archive parallel via ThreadPoolExecutor,
  concurrency = min(archive count, --workers), per-archive
  tar --use-compress-program="pigz -p N" with N = workers // concurrency,
  with Python tarfile fallback) and multi-layer pod-to-node
  mapping for node association when pod dirs are simplified to worker_<podIp>:
  env files (pod_ip= / *HOST_IP=), in-log "Host ID is <ip> from env ..." lines
  (fallback when env is missing, e.g. SDK direct-connection pods), directory-IP
  lookup, and node-name substring matching.
  Also correlates three optional auxiliary logs from dscollect_log: irqoff
  latency (>1ms, interrupt-off culprit stacks), sar NIC utilization (ethtool +
  per-second samples), and brpc bthread creation/scheduling/completion logs
  (coroutine queueing and execution evidence), plus per-machine NUMA memory
  access monitoring logs (numafast/memory/perf in
  <ip>-<ip>-data_<ts> subdirectories). OS-resource periodic monitoring
  (irqoff / sar NIC / NUMA memory access; future periodic metrics will be
  added here too) is rendered in a dedicated standalone self-contained
  os_monitor_report.html (node info table + interactive explorers:
  irqoff scatter by process and NUMA single-chart with node dropdown,
  metric-group checkboxes with group batch toggle, hover tooltip with raw
  values, and drag-to-zoom time selection like the latency trend charts).
  NUMA metrics cover the full numafast report (NUMA score, SRC→DST
  access matrix pairs with traffic/distance on hover, per-NID
  %RMA/RMA_Die/RMA_Skt/LMA/MEM_all/MEM_free/%MEM/%CPU, top-1 process
  ACCESS%/%RMA/%CPU/LMA by memory access with command shown on hover),
  memory log (cache miss rates, DDR bandwidth, L1/L2/TLB access bandwidth +
  hit rates, L3 read bandwidth + hit rate per NODE), and perf log
  (all perf stat events dynamically discovered via the "==== 事件: ... ===="
  header + generic count lines — new counters such as dTLB-loads or
  ummu_pmcg_*/tcu_* are automatically included in the report's perf metric
  group, collapsed by default and expandable; known events dTLB/iTLB misses
  and ummu_pmcg TBU TLB hit rates keep default visibility; metric labels
  always match the raw event names verbatim (trailing slash included,
  zero-value counters included as full series); `#` comments ride along as
  tooltip extras); common metrics are shown
  by default while the rest are collapsed behind an expand toggle —
  auto-generated whenever any periodic monitoring logs are
  collected, no longer embedded in the localization report; --os-monitor-only
  analyzes ONLY periodic monitoring without client logs or slow-request
  analysis. The localization report still prints the worker/client host IP
  (reverse-looked up from bpf/warn file names) in each trace card and the
  JSON client/server host_ip field.
  When the client node has no bpf
  logs (e.g. SDK direct-connection node not collected), falls back to
  identifying the connection 5-tuple from server-side events. Business anchor
  logs (ServerRecv/ServerSend) are optional: when missing, the server pod is
  recovered from worker business INFO lines (kvcache*.log or access.log) and
  the link is inferred from bpf kernel events (ServerTcpRecvFirst≈request
  delivery, ServerTcpSendIn≈response emission, ◇-prefixed inference
  evidence), with a client-only single-sided fallback (request egress NIC
  point → response ClientNetifRx, network vs server processing
  indistinguishable). New log formats that no longer emit anchor lines are
  fully supported: the SLOW line's embedded ns monotonic timestamps
  (ClientSend=/ClientRecv=/ServerRecv=/ServerSend=) are converted to wall
  clock (client via SLOW-line write time ≈ ClientRecv; server via URMA
  trace_us observed offset, falling back to the first worker business line,
  ±ms) to synthesize all four anchors — structurally identical to native
  anchors (downstream windowing/segmentation/attribution unchanged), marked
  synth=true in JSON with ◇ evidence. bpf logs with a sequence-number prefix
  ("16438196 18:03:09:028931 dev_start_xmit: ...") are parsed transparently
  (seek binary-search / full-scan / probe paths all compat). Connection
  identification gains two response-direction fallbacks for UB/URMA
  transports where request-direction events are absent from bpf: client_resp
  (client-side response receive events, tcp recv_que/in → nic rx, closest to
  ClientRecv) and server_nic (server-side response-direction nic xmit closest
  to ServerSend). Distinct root causes:
  business recv slow (TcpRecvQue→TcpRecvFirst: stack queued the packet but the
  business recv lagged) and node-internal NIC↔stack ingress/egress delay
  (≥1000us and ≥70% of the wire segment rewrites the network_* category). For kernel-to-user receive delays, renders a problem-window bpf event panorama
  (problem 5-tuple highlighted amid interleaved other-connection traffic, with
  problem packet sequence numbers — seq/tp_rcv_nxt/copied_seq/rcv_nxt and
  retransmit tx_seq/snd_una/snd_nxt — highlighted in red across nic/tcp layer
  events for cross-event tracking of the same problem packet) and
  detects softirq preemption of the business thread's cpu by other requests'
  receive processing, including softirq probe events (softirq_raise→entry /
  softirq_entry→exit latency >1ms with vec, kstack and timercnt) for further
  receive-latency localization. When a slow-softirq log's cpu matches the
  receiving cpu or its SMT sibling (cpu^1) within a 50ms lookback window
  before the receive point, directly identifies the preempting task (comm)
  and renders its full kstack as a definitive localization conclusion
  (kernel_to_user and wire modes), prominently displayed via a red
  root-cause banner at the top of the trace card plus a head badge and a
  TOC marker; the next step is analyzing why that task ran on that cpu.
  Log discovery is directory-name agnostic: it scans the whole tree and
  classifies files by name patterns (ds_client*/kvcache*/bpf-*/irqoff_*/
  nic-*/-brpc*/node_ip/env/tar.gz) with content sniffing fallback for
  unknown *.log files. When pod directory names are unrecognizable (no
  node substring/IP, no env/Host ID), a bpf probe fallback resolves the
  node: the pod IP appears as local_ip only in the bpf log of the node
  hosting the pod (client/server sides probed independently).
  Triggers: network_residual_us, network timeout analysis, RPC segment latency,
  bpf kernel event correlation, scheduling latency, latency_warn, nic driver
  latency, tcp retransmit, irqoff, interrupt disabled, 关中断, sar NIC
  utilization, 网卡利用率, bthread, 协程调度, coroutine queueing,
  softirq preemption, 软中断抢占, 收包后业务处理晚, cpu 侵占,
  业务收包慢, business recv slow, 节点内定界, node internal delay,
  定位定界, 网络时延分析, 调度问题定位, 网卡收发耗时,
  numa, 访存, numa score, remote memory access, DDR 带宽, ddrc,
  TLB miss, cache miss, dTLB, iTLB, 宿主机 IP, host ip,
  OS 资源周期监控, os monitor, 周期监控报告.
---

# 网络/调度时延定位分析（ds-network-latency-analysis）

对 k8s 集群收集的 client/worker 用户态日志、bpftrace 内核日志、调度时延告警日志
（及可选的关中断 / sar 网卡利用率 / brpc 协程 / NUMA 访存监控日志）进行自动化
关联分析，定位 `network_residual_us` 超时请求的网络/调度瓶颈段，给出定界结论。

**核心脚本**：`scripts/network_latency_analysis.py`（Python3，无第三方依赖）

---

## 输入日志发现（目录名无关）

**发现逻辑只认文件，不依赖目录名**——采集目录名（`collected/`、
`collected_worker_logs/`、`dscollect_log/`、`latency_warn_log/` 及 pod 目录名）
后续变化不影响分析。对日志根目录**全树递归扫描**，按文件名模式分类：

| 文件名模式 | 类型 |
|---|---|
| `ds_client*.log` | client 应用日志（glog） |
| `kvcache*.log` / `access.log` | worker 应用日志（glog；access.log 为访问日志，含 trace 行，兜底恢复 server pod IP） |
| `bpf-<nodeName>-<nodeIp>.log` | bpftrace 内核日志（新格式行首带序号：`16438196 18:03:09:028931 dev_start_xmit: ...`，自动兼容） |
| `irqoff_latency_<nodeIp>.log` | [可选] 关中断 >1ms 日志（块 + 调用栈） |
| `nic-<nodeIp>.log` | [可选] ethtool 属性 + sar 每秒网卡采样 |
| `numafast_<ts>.log` / `memory_<ts>.log` / `perf_<ts>.log` | [可选] NUMA 访存监控（父目录名含节点 IP，如 `<ip>-<ip>-data_<ts>/`） |
| `<podName>-brpc*.log` | [可选] brpc bthread 创建/首次调度/完成日志（可在任意目录） |
| `<nodeName>_<nodeIp>`（无扩展名） | 调度时延告警 |
| `env` | [可选] pod_ip= / *HOST_IP= 映射 |
| `*.tar.gz` / `*.tgz` | [可选] 归档（任意目录，发现阶段就地解压后重扫） |

**未知名 `*.log` 内容嗅探兜底**：读文件头 1MB，含 `ClientSend ts `/`ClientRecv ts `
/`[BRPC_RPC_FRAMEWORK_SLOW]` 标记 → client；含 `ServerRecv ts `/`ServerSend ts `
→ worker；无标记 → 跳过。工具自身的 `--raw` 汇总输出（首行 80 个 `=` 分隔线）
被排除，防止输出落在日志根目录时自污染。

典型目录布局（仅为示例，目录名可任意变化）：

```
<log_root>/
├── collected/                  # client 日志，podName 为子目录名（内含 nodeName）
│   └── <podName>/ds_client_*.INFO.*.log
├── collected_worker_logs/      # worker 日志，podName 为子目录名（内含 nodeName）
│   ├── <podName>/kvcache.INFO.*.log
│   └── <podName>/env                # [可选] pod_ip= / *HOST_IP= 映射（pod 目录名
│                                    #   简化为 worker_<podIp> 时，靠它定位宿主机节点）
├── dscollect_log/              # bpftrace 内核日志，bpf-$nodeName-$nodeIp.log
│   ├── bpf-*.log.tar.gz             # [可选] 超大 bpf 日志归档（发现阶段就地解压）
│   ├── irqoff_latency_$nodeIp.log   # [可选] 关中断 >1ms 日志（块 + 调用栈）
│   ├── nic-$nodeIp.log              # [可选] ethtool 属性 + sar 每秒网卡采样
│   ├── <ip>-<ip>-data_<ts>/         # [可选] 每物理机 NUMA 访存监控
│   │   ├── numafast_<ts>.log        #   NUMA score + 按 NID %RMA/%CPU/%MEM
│   │   ├── memory_<ts>.log          #   L1D/L1I/L2D/L2I cache miss + DDR 带宽
│   │   └── perf_<ts>.log            #   perf stat 全事件动态识别（1s/轮）
│   ├── <podName>-brpc_client.log    # [可选] brpc bthread 创建/首次调度/完成日志
│   └── <podName>-brpc_server.log    # [可选] server 侧 brpc bthread 日志
└── latency_warn_log/           # 调度时延告警，文件名 $nodeName_$nodeIp
```

**tar.gz 归档**：任意目录下的 `*.tar.gz`（超大 bpf 日志归档）在发现阶段
**就地解压**后按普通文件匹配。pigz 解压单个归档近似单线程，**多归档并行才是
主要收益**：ThreadPoolExecutor 并发解压多个归档，并发数 = min(归档数, workers)，
单归档 `tar -x -C <dir> --use-compress-program="pigz -p N" -f <archive>` 的
**N = workers // 并发数**（workers 即 `--workers` 参数，默认 min(16, CPU核数)；
单归档场景 N = workers，行为不变）；pigz/tar 不可用或命令失败时自动回退
Python tarfile。解压失败（坏包/权限）告警跳过，不影响其余日志发现。

**env 节点映射**：任意目录下的 `env` 文件含 `pod_ip=...` 与
`JD_HOST_IP=...`（或其它 `*HOST_IP=` 键）时建立 podIP→宿主机 IP 映射——pod 目录名
被采集脚本简化成 `worker_<podIp>`（不含 nodeName）时，靠该映射经
`bpf-<node>-<nodeIp>` 文件名反查宿主机 bpf/辅助日志节点。

**日志正文 Host ID 映射（env 缺失时兜底）**：client/worker 应用日志正文中的
启动期一次性行——`Host ID is <ip> from env HOST_IP`（client，service_discovery.cpp）
/ `Host id is <ip> from env JD_HOST_IP`（worker，ds_coordination_backend.cpp）——
在阶段 2 合并扫描时一并提取（pod IP 取日志所在目录名），env 已有映射时不覆盖。
典型场景：`collected/SDK_<podIp>/` 无 env 文件，宿主机 IP 只在 ds_client 日志正文。

**bpf 探测节点兜底（目录名不可识别时）**：pod 目录名无节点子串/IP、又无
env/Host ID 行时（目录名被完全改掉），用 trace 时间窗（前后各扩 2s）探测
各节点 bpf 文件——**pod IP 作为 local_ip 只会出现在 pod 所在节点的 bpf
日志**（对端节点上它是 peer_ip），命中 local_ip==podIP 的节点即回填为该侧
节点，client/server 两侧独立探测，后续 bpf/告警/辅助日志关联照常。只认
local_ip 天然排除对端节点误绑；探测命中按 pod IP 缓存，每 pod 只探测一次。

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
    [--os-monitor-report os_monitor_report.html] [--os-monitor-only]
```

| 参数 | 必填 | 说明 |
|---|---|---|
| `log_root` | 是 | 日志根目录（全树扫描按文件名/内容识别，不依赖目录名） |
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
| `--os-monitor-report` | 否 | OS 资源周期监控报告输出路径（默认 -o 同目录 `os_monitor_report.html`，采集到周期监控日志即自动生成） |
| `--os-monitor-only` | 否 | 只分析 OS 资源周期监控（irqoff/sar nic/NUMA 访存），跳过问题请求定界分析（可无 client 日志），输出周期监控报告即退出 |

测试：`python3 <skill_dir>/scripts/test_network_latency_analysis.py`

---

## 结构化 JSON 输出（原始结果）

`--json result.json` 输出与 HTML 报告同源的结构化结果，供其他 skill / 分析工具 /
自定义渲染消费。与 HTML 不同：**事件明细、调度告警、唤醒链全量输出，不截断**；
时间一律 ISO 8601 微秒精度（如 `2026-08-21T21:31:21.060777`）。

```jsonc
{
  "schema": "ds-network-latency-analysis/result",
  "schema_version": 2,
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
      "conn":        {"client_ip": "…", "client_port": 37880, "server_ip": "…", "server_port": 31501,
                     "source": "client_tcp|client_nic|server_tcp|client_port"},
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
                                      "creation_mode": null, "execution_time_us": null,
                                      "lifetime_time_us": null, "cpu": 187, "raw": "原始行"},
                                     {"ts": "…", "kind": "completed", "tid": 523, "bthread_id": …,
                                      "execution_time_us": 19, "lifetime_time_us": 47, …}],
                          "server": […]},  // 窗口内 bthread 事件（按锚点 tid 过滤；
                                           // kind: created/scheduled/completed）
      "cpu_busy":        {"client": {"seg_key": "client_kernel_to_user", "seg_desc": "…",
                                     "seg_dur_us": 15798, "window_start": "…", "window_end": "…",
                                     "anchor_name": "ClientRecv", "anchor_tid": "523", "anchor_cpu": 50,
                                     "conn": "…:37880 <-> …:31501", "n_mine": 8, "n_other": 42,
                                     "other_conns": {"…:33300 <-> …:31501": 30},
                                     "other_by_cpu": {"50": 12, "61": 20},
                                     "other_on_cpu": {"n": 12, "ts_list": ["…", …]},
                                     "switches_on_cpu": {"n": 3, "ts_list": ["…", …]},
                                     "switched_out": {"n": 1, "ts_list": ["…"]},
                                     "softirq_raise_on_cpu": {"n": 1, "ts_list": ["…"]},
                                     "softirq_exit_on_cpu": {"n": 0, "ts_list": []},
                                     "preempt": true,
                                     "softirq_localization": {"mode": "kernel_to_user|wire",
                                                              "comm": "ubctl", "kstack": "…完整调用栈…",
                                                              "latency_us": 5044, "vec": 3, "vec_txt": "3(NET_RX)",
                                                              "cpu": 44, "anchor_cpu": 50, "smt": false,
                                                              "ts": "…", "recv_ts": "…", "n_candidates": 1,
                                                              "wakeup_trace": {"wakeup_ts": "…", "waker_cpu": 44,
                                                                               "switch_in_ts": "…", "prev_comm": "…",
                                                                               "trigger_kind": "…", "delta_to_recv_us": 512}},
                                     "window_events": {"n": 50, "first_ts": "…", "last_ts": "…"}}},
                                     // 问题窗口全景 + cpu 侵占分析（kernel_to_user 段异常的侧才有）。
                                     // schema v2 去重：事件明细（含 raw）仅在 kernel_events 全量存一份，
                                     // 本结构内各事件列表改 {n, ts_list} 引用（按 ts 与 kernel_events
                                     // 对齐取明细/raw），window_events 改 {n, first_ts, last_ts} 摘要；
                                     // softirq_localization 为收包慢直接定位结论（命中时才有，wakeup_trace
                                     // 为占用任务唤醒者回溯；wire 模式下 kernel_to_user 段不异常也会补最小信息）
      "slow_seg_window": {"seg_key": "client_user_to_kernel", "seg_desc": "…",
                          "category": "…", "window_start": "…", "window_end": "…",
                          "dur_us": 15798,
                          "sides": {"client": {"n_mine": 8, "n_other": 42,
                                                "by_kind": {"tcp_send_in": 3, …}}}},
                          // 慢段时间窗（窗口定义 + 各侧事件归属计数；事件明细 =
                          // kernel_events 按窗口过滤，ts 对齐；无瓶颈段/窗口不可得时为 null）
      "missing_evidence": ["…"],
      "conclusion": {"category": "client_kernel_to_user_delay", "label": "…",
                     "confidence": "高", "bottleneck": {…瓶颈分段…},
                     "evidence": ["…"], "suggestions": ["…"],
                     "preemptor_origin": null}
                     // preemptor_origin：softirq 定位命中且唤醒者回溯成功时的
                     // 唤醒链文本（任务被谁唤醒/何时切入 cpu/距收包点多久），
                     // 直接回答"该任务为何在该 cpu 运行"；未命中为 null
    }
  ]
}
```

**schema v2 事件去重**（v1 → v2 变更，下游消费方注意）：

- 事件明细（含 `raw` 原始行）**仅在 `kernel_events` 全量存一份**（唯一明细源）；
- `cpu_busy` 内各事件列表（`other_on_cpu` / `switches_on_cpu` / `switched_out` /
  `softirq_raise_on_cpu` / `softirq_exit_on_cpu`）改 `{n, ts_list}` 计数+ts 引用，
  `window_events` 改 `{n, first_ts, last_ts}` 摘要；
- `slow_seg_window.sides` 改归属计数摘要（`n_mine` / `n_other` / `by_kind`）；
- 顶层 `softirq_events` 字段已删除（`cpu_busy.softirq_*` 摘要 +
  `kernel_events` 明细 + `softirq_localization` 定位证据覆盖）；
- 下游按 `ts`（ISO 8601 微秒）+ `kind` 与 `kernel_events` 对齐即可取到完整明细
  与原始行，**不丢任何信息**，JSON 体积大幅下降（实测均条 -80%+）。

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
   **预热阶段 trace 过滤**：UUID 格式 trace_id（如
   `d855850b-54c6-4968-8cc2-1d4b974d88bc`）为预热请求，不计入业务 p99，
   默认跳过不分析（正常业务 trace 为 `setStringView-`/`getBuffer-` 等
   前缀格式）；`--trace` 显式指定 UUID 子串时不过滤（用户主动要求）。
2. **RPC 锚点关联**：用 SLOW 行的 `ClientSend=/ClientRecv=/ServerSend=/ServerRecv=`
   单调时钟值精确匹配 client/worker 日志中的
   `yyl9 ClientSend` / `yyl9 ClientRecv` / `yyl3 ServerRecv` / `yyl10 ServerSend`
   锚点行，计算三段宏观耗时（CS→SR、SR→SS、SS→CR），并从锚点行取 pod IP、
   从文件夹名解析 nodeName。
   **锚点日志为可选收集**：`ServerRecv/ServerSend` 锚点行缺失时（worker 日志
   级别未打/轮转/采样，或新日志格式不再输出锚点行），若 worker 日志中存在该
   trace 的任意业务 INFO 行（kvcache*.log / access.log），从其 host 列恢复
   server pod IP/pod 目录，server 节点解析与 bpf 窗口构建照常进行，server 侧
   链路按 bpf 内核事件推测（见"锚点缺失时的推测链路"）。
   **新格式锚点合成（锚点行全缺）**：新版本 SDK/worker 日志不再输出
   `ClientSend ts ...` 锚点行，但 SLOW 行本身内嵌四个 ns 单调钟时间戳
   （`ClientSend=.../ClientRecv=.../ServerRecv=.../ServerSend=...`）——
   - client 侧：SLOW 行写出时刻 ≈ ClientRecv 墙钟（±日志写出延迟），
     ClientSend 墙钟 = 其减 `(ClientRecv_ns − ClientSend_ns)/1000`（差值精确）；
   - server 侧：优先从 worker URMA 日志 `trace_us:{...}` 取已完成事件的 us
     单调钟值求 offset（`行墙钟 − observed`），无 URMA 行时用 worker 首个
     trace 业务行墙钟近似（±ms 级），再加 ns 差值得 ServerRecv/ServerSend 墙钟
     （server 处理 SR→SS 为两 ns 值精确差值）；
   - 合理性校验：合成 ServerRecv 必须落在 `[ClientSend−60s, ClientRecv+60s]`，
     否则放弃（避免换算参考错误生成离谱锚点）；
   - 合成锚点结构与原生锚点行完全一致（下游窗口/分段/定界引擎零改动），
     JSON 中带 `"synth": true` 标记，证据带 ◇ 前缀说明合成方法与精度。
3. **内核日志关联**：在 client/server 节点的 bpf 日志中按
   `[ClientSend−pad, ClientRecv+pad]` 时间窗 + 双 pod IP 过滤；连接四元组识别
   （clientIP:ephemeralPort ↔ serverIP:servicePort）按优先级：
   - **client tcp**（确证）：ClientSend 后最近的 `tcp send in`；
   - **client nic**（推测）：tcp 层探针丢失时，用 client 侧请求方向
     （src=clientIP → dst=serverIP）`dev_start_xmit`/`net_dev_xmit`（raw 含完整
     五元组，取 ClientSend−200us 后最早）推测识别，missing 注明"tcp 层 bpf
     事件丢失，已按 nic 层事件推测连接五元组"，client 侧事件表加"推测"badge；
   - **server tcp**（回退）：client 节点无 bpf 日志时（如 SDK 直连节点未采集），
     用 server 侧窗口事件识别（请求方向收包事件 `tcp recv_que/in`（local=server,
     peer=client，时间 ≥ ServerRecv−200us 中最早）优先，其次响应方向发送事件
     `tcp send_in`），missing 注明"已从 server 侧回退识别连接，client 侧内核
     事件缺失"；
   - **client port**（端口+时间配对推测）：server pod IP 未知（worker pod 日志
     未收集）但已知 server 服务端口时（server 角色端口固定，由同 run 内完整
     trace 识别出的连接提供，按 client IP 归组），在 client 侧窗口事件中筛选
     目标端口的请求发送事件（邻近 ClientSend）与响应接收事件（邻近
     ClientRecv），按 client 临时端口配对成连接，取
     「|req−ClientSend| + |rsp−ClientRecv|」总分最小者（同端口的干扰连接因
     响应时间远离 ClientRecv 而被排除，非目标端口连接直接排除）；missing 注明
     "已按已知服务端口 + 双向时间配对推测连接五元组"，事件表加"推测"badge，
     置信度封顶"中"；
   - **client resp**（响应方向兜底）：请求方向事件全缺时（如 UB 传输：RPC
     请求走 verbs/URMA，bpf 仅观测到响应方向数据），用 client 侧响应方向收包
     事件识别——`tcp recv_que/in`（local=client & peer=client 对端=server）
     优先，无则 nic 层 `netif_receive_skb`（dst=client & src=server），取离
     ClientRecv 最近的一条；
   - **server nic**（server 侧 nic 层兜底）：server 侧 tcp 探针丢失（URMA
     写仅有 nic 层发送事件）时，用响应方向 `dev_start_xmit`/`net_dev_xmit`
     （src=server → dst=client）取离 ServerSend 最近的一条。

   **server pod IP 未知时**（worker pod 日志未收集）：不再跳过内核关联——client 侧
   窗口按 client pod IP 单侧匹配照常扫描，事件明细/全景照常输出；连接五元组优先
   走上方 **client port** 端口+时间配对推测（server IP 一并从请求事件 dst IP 推出，
   五元组过滤/里程碑/唤醒链照常走准确路径）；推测不出时标注"未能识别"，
   client 侧里程碑（ClientTcpSendIn/DevStartXmit/NetDevXmit/NetifRx/
   TcpRecvFirst/Last）按时间邻近推测（窗口内该 pod 最早同向事件，多连接时有混淆
   风险，missing 注明），client 段照常产出，结论置信度封顶"中"。

   随后重建内核级时间线（`tcp send/recv in/out/que`、
   `sock_def_readable` 等事件）；
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
| server 业务收包 | ServerTcpRecvQue → ServerTcpRecv(首) | > 100 us |
| server 内核→用户态 | ServerTcpRecv(末) → ServerRecv | > 100 us |
| server 业务处理 | ServerRecv → ServerSend | 对比 BRPC queue+exec（>max(500, 2×)） |
| server 用户态→内核 | ServerSend → ServerTcpSendIn | > 100 us |
| server→client 线路 | ServerTcpSendIn → ClientTcpRecv(首) | > 200 us |
| client 业务收包 | ClientTcpRecvQue → ClientTcpRecv(首) | > 1000 us |
| client 内核→用户态 | ClientTcpRecv(末) → ClientRecv | > 1000 us |

**业务收包分段说明**：`TcpRecvQue`（协议栈收包入队）→ `TcpRecvFirst`
（业务调用 recvmsg）耗时超阈值即定界为业务收包慢——协议栈已收包入队，
业务迟迟未调用 recv 取包（收包线程被长任务/前序协程占用、锁/IO 阻塞）。
阻塞收包场景 First 先于 Que（recvmsg 先阻塞等待，包后到），分段耗时为负
自动跳过，不误报。

### 定界分类

| 分类 | 含义 |
|---|---|
| client_user_to_kernel_delay | client 用户态发送路径慢 |
| network_c2s_transmission | client→server 网络传输慢 |
| server_business_recv_slow | server 业务收包慢（协议栈已收包入队，业务迟迟未调用 recv 取包） |
| client_business_recv_slow | client 业务收包慢（协议栈已收包入队，业务迟迟未调用 recv 取包） |
| server_kernel_to_user_delay | server 收包后唤醒/调度慢（内核侧主导） |
| coroutine_schedule_delay | server 协程调度排队慢：内核唤醒/线程上 CPU 正常，但 bthread 协程等待 worker 线程执行（细分自 server_kernel_to_user） |
| server_processing_slow | server 业务处理慢（结合 server_req_queue_us/server_exec_us 细分） |
| server_user_to_kernel_delay | server 发送路径慢 |
| network_s2c_transmission | server→client 网络传输慢 |
| network_c2s_phys_wire_delay | client→server 物理网卡间传输慢（网卡处理/物理线路，两侧节点内已排除；细分自 network_c2s_transmission） |
| network_s2c_phys_wire_delay | server→client 物理网卡间传输慢（网卡处理/物理线路，两侧节点内已排除；细分自 network_s2c_transmission） |
| server_node_ingress_delay | server 节点内收包→协议栈交付慢（veth 转发/软中断/排队；细分自 network_c2s_transmission，节点内段占 wire 段 ≥70% 且 ≥1000us 时改写） |
| client_node_ingress_delay | client 节点内收包→协议栈交付慢（同上，细分自 network_s2c_transmission） |
| client_node_egress_delay | client 节点内协议栈发送→驱动慢（qdisc 排队/协议栈处理；细分自 network_c2s_transmission） |
| server_node_egress_delay | server 节点内协议栈发送→驱动慢（同上，细分自 network_s2c_transmission） |
| client_kernel_to_user_delay | client 收包后唤醒/用户态取包慢 |
| interrupt_off_delay | 问题窗口内关中断定量归因命中（cpu 匹配的最长关中断 ≥ max(2ms, 50% 段耗时)，见"窗口级定量归因"） |
| client_to_server_path / server_to_client_path | 仅有宏观三段时的粗粒度定界 |
| unknown | 证据不足（如 worker 日志未收集） |

### 锚点缺失时的推测链路（锚点日志为可选收集）

`ClientSend→ServerRecv`、`ServerRecv→ServerSend`、`ServerSend→ClientRecv`
的业务锚点日志为**可选收集**（worker 日志级别未打/轮转/采样时缺失）。
缺失时按已有日志（业务行 + bpf 内核事件）推测链路，`◇` 前缀写入结论
证据链（JSON `conclusion.evidence`），尽量减少落入 `unknown`：

1. **worker 业务行恢复 server pod**：worker 日志存在该 trace 的任意业务
   INFO 行 → host 列即 worker pod IP，恢复 server pod/节点，server 侧 bpf
   窗口照常扫描。
2. **内核事件推测宏观三段**：`ServerTcpRecvFirst` ≈ 请求交付业务、
   `ServerTcpSendIn` ≈ 响应发出，等价计算 CS→SR / SR→SS / SS→CR 三段
   耗时（供宏观回退定界）；推测文本注明依据，出现负值时提示"连接推测
   配对可能有误或节点间时钟偏差"。
3. **client_only 补扫**：worker 日志完全未收集、连接五元组经端口+时间
   配对推测识别后，用推测 server IP 探测其所在 bpf 节点（pod IP 作为
   local_ip 只出现在所在节点日志），命中则补扫该节点窗口，回填 server 侧
   事件/里程碑/全景并生成推测链路证据。
4. **client 单侧兜底推测**：server 侧完全无 bpf 证据时，用 client 单侧
   内核点位推测方向——request 发出（`ClientTcpSendIn`，tcp 探针未启用时
   回退 nic 层 `ClientDevStartXmit`/`ClientNetDevXmit`）→ response 到达
   client 网卡（`ClientNetifRx`）耗时覆盖 网络传输 + server 业务处理
   （无法区分二者，如实注明），另给 client 网卡→业务取包耗时与 TCP 重传
   次数（提示线路丢包）。

### 节点内细分定界（网卡↔协议栈）

传输类瓶颈（`network_c2s/s2c_transmission`）命中时，进一步检查瓶颈侧
**节点内段**（bpf 直接证据，排除物理线路）：

| wire 段 | 节点内证据段（`_nic_segments`） | 改写根因 |
|---|---|---|
| wire_c2s（client→server） | server `NetifRx → TcpRecvFirst`（收包→协议栈交付） | server_node_ingress_delay |
| wire_c2s | client `TcpSendIn → DevStartXmit`（协议栈发送→驱动，含 qdisc） | client_node_egress_delay |
| wire_s2c（server→client） | client `NetifRx → TcpRecvFirst` | client_node_ingress_delay |
| wire_s2c | server `TcpSendIn → DevStartXmit` | server_node_egress_delay |

节点内段耗时 ≥ 1000us 且占 wire 段 ≥ 70% 时改写根因（取耗时最大者），
证据链追加"细分定界：… 物理线路已排除"；节点内定界同样要求网卡层点位
佐证才给高置信度。未达标时维持 `network_*` 传输分类，交由物理网卡间
seq 关联定界（`network_*_phys_wire_delay`）进一步细分。

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

- **全周期统计**（独立 `os_monitor_report.html`"关中断统计"交互探索卡 +
  JSON `irqoff_stats`；主报告仅保留一行摘要）：记录数 /
  hardirq / softirq / 最长；`[1ms,2ms,5ms,10ms,20ms,50ms,100ms,500ms]` 分桶
  直方图；按进程 top10（次数/最长/累计）；交互散点（节点下拉/按进程勾选/
  悬浮/缩放）；Top20 最长记录表。
- **问题窗口关联**（trace 卡"关中断记录"块 + JSON `irqoff_events` + raw 段）：
  窗口取该侧"收包→用户态取包"段（`TcpRecvFirst` 前推 2ms → Recv 锚点；
  无 bpf 收包点位时退化为整 trace 窗口），窗口内记录含完整调用栈。
- **证据句**：窗口内存在关中断记录 → `◆ … 检测到 N 条关中断记录（hardirq X /
  softirq Y），最长：cpu C 被 comm(pid) 关中断 T…`；定界为
  `client/server_kernel_to_user_delay` 或 `coroutine_schedule_delay` 时
  作为佐证把置信度提升为"高"；定量命中时见"窗口级定量归因"
  （改写 `interrupt_off_delay`）。

### sar 网卡利用率日志（nic-$nodeIp.log）

ethtool 属性（Speed/Duplex/Link detected）+ sar 每秒采样（rxpck/s、txpck/s、
rxkB/s、txkB/s、%ifutil）。用于佐证或**排除**网卡带宽瓶颈。

**格式**：`Settings for <dev>:` 开网卡段 + 属性行；sar 数据行含 12 小时制
时间（AM/PM 自动转 24h）；日期缺失，按各 trace 的 ClientSend 锚点日期组合。

**分析输出**：

- **全周期统计**（独立 `os_monitor_report.html`"网卡利用率统计"卡 +
  JSON `nic_stats`；主报告仅保留一行摘要）：每节点每
  网卡的 Speed / Duplex / Link detected / 采样数 / 峰值与均值 %ifutil /
  峰值时刻 / 峰值 rxpck/s。
- **问题窗口关联**（trace 卡"sar 网卡采样"块 + JSON `nic_samples` + raw 段）：
  窗口为整 trace 窗口（带宽评估看问题时段整体）；dev 优先取物理网卡定界
  （`phys_wire`）的收发 dev；样本按"时刻覆盖 [S, S+1s) 区间"语义匹配。
- **证据句**：窗口内峰值 %ifutil ≥ 50 → "网卡利用率高"佐证；< 10 → 排除性
  证据（"峰值仅 X%，排除网卡带宽打满"）。

### brpc 协程日志（<podName>-brpc_client.log / <podName>-brpc_server.log）

bthread 创建 / 首次调度 / 执行完成日志，统计问题时刻线程上有多少协程在排队、
协程内业务执行多慢。用于佐证"协议栈收包后业务执行晚"的协程调度排队问题
（`pending_time_us` = 创建到首次执行），并区分"排队慢"与"业务本身慢"
（`execution_time_us` = 协程内执行耗时）。

**文件关联**（pod 目录名 → brpc 文件，优先级递减）：
1. pod 名精确/前后缀匹配（目录名沿用 pod 名）；
2. brpc 文件 pod 名以宿主机节点名为独立段（目录名被简化成 `worker_<podIp>` 时，
   经 env 映射出节点名仍可关联）；
3. 同角色文件全局唯一时兜底（文件名含 `-brpc_client` / `-brpc_server` 且该角色
   文件只有一个；多个时不猜）。

**格式**（glog 行，无年份，按 trace 窗口日期 ±1 年容错组合）：

```
I0824 22:32:23.661136  6267 4294969346 task_group.cpp:520 start_foreground] [WZY] bthread created: creator_tid=6267 bthread_id=3693671876360 creation_time_ns=... creation_mode=foreground target_local_pending_tasks=0 target_remote_pending_tasks=0 target_pending_tasks=0
I0824 22:32:23.661172  6267 3693671876360 task_group.cpp:383 task_runner] [WZY] bthread first scheduled: worker_tid=6267 cpu_id=92 bthread_id=3693671876360 fn=... arg=... creation_time_ns=... first_run_time_ns=... pending_time_us=37
I0824 22:32:23.661190  6267 3693671876360 task_group.cpp:422 task_runner] [WZY] bthread completed: worker_tid=6267 cpu_id=92 bthread_id=3693671876360 fn=... arg=... completion_time_ns=... execution_time_us=33 lifetime_time_us=60
```

（`cpu_id` 为可选字段，新格式 first scheduled / completed 行含；旧格式无则置 null。）

**分析输出**（仅问题窗口内统计，不做全周期统计——日志量大）：

- **问题窗口关联**（trace 卡"bthread 协程事件"块 + JSON `bthread_events` +
  raw 段）：窗口同 irqoff（"收包→用户态取包"段），事件按**锚点 tid 过滤**
  （client 侧 ClientRecv tid / server 侧 ServerRecv tid），窗口外行只做
  行首快速预判即跳过。
- **证据句**（JSON `coro_evidence`）：该线程在窗口内的 created / scheduled /
  completed 数、`pending_time_us` max/avg、`execution_time_us` 峰值、
  `lifetime_time_us` 峰值、`target_pending_tasks` max →
  "线程 tid=N 窗口内创建 X 个协程、Y 次首次调度，最长排队 T…"
  ——佐证"协议栈已收包但协程很晚才执行"；`execution_time_us` 峰值高则
  提示业务本身慢（非"处理开始晚"），`lifetime_time_us` = 创建→完成全生命周期。
- **置信度**：定界为 `client/server_kernel_to_user_delay` 或
  `coroutine_schedule_delay` 且有协程排队证据时提升为"高"。

### NUMA 访存监控（numafast / memory / perf）

每物理机一个 `<ip>-<ip>-data_<ts>/` 子目录（节点身份取父目录名第一个 IPv4；
已解析到节点用规范节点名，未解析到直接用 IP 作键，与 irqoff/nic 一致），
包含三类日志，全周期统计后以 **SVG 折线图**（自包含，风格同各阶段时延趋势图：
网格线 + Y 轴刻度 + X 轴首尾时间标签 + 每点 tooltip + 图例）呈现：

| 日志 | 块结构 | 提取指标 |
|---|---|---|
| `numafast_<ts>.log` | `NUMAFAST Report-N ... Time:YYYYmmdd-HHMMSS` 块 | `System's numa score`（0-1）、按 NID 的 `%RMA`（远程访存占比）/`%CPU`/`%MEM` |
| `memory_<ts>.log` | `Memory Summary Report-N  Time:YYYY/mm/dd HH:MM:SS` 块 | L1D/L1I/L2D/L2I cache miss %、`ddrc_read`/`ddrc_write`（MB/s） |
| `perf_<ts>.log` | `==== perf stat round N 开始时间 YYYY-mm-dd HH:MM:SS ====` 块（1s/轮） | **全部事件动态识别**（`==== 事件: ... ====` 头注册事件目录 + 通用数据行，新增指标自动纳入，如 `dTLB-loads`、`ummu_pmcg_*/tcu_cntx_cache_miss_num/`、`tcu_pptw_req_num/`）；`#` 注释进序列附加信息（悬浮窗展示），行尾 `(xx.xx%)` 缩放标注忽略 |

**图表**（OS 资源周期监控报告"NUMA 访存监控（全采集周期，交互探索）"卡，
**单张交互图**，原生 JS 离线自包含，物理机多/指标多不再逐节点平铺）：
- **节点下拉选择**：按需查看某个 worker（物理机）；**切换节点保留已勾选
  指标**（勾选是用户选择的完整记忆，不随节点重置；跨节点对比无需重新
  勾选，切到无该指标的节点自动隐藏、切回即恢复，新节点与勾选完全无交集
  时并集补默认）；
- **指标分组勾选**：numafast（NUMA score(×100) / 各 NID %RMA）、
  memory（ddrc_read/ddrc_write MB/s、L1D/L1I/L2D/L2I miss %）、
  perf（**事件动态识别**：dTLB/iTLB misses、ummu_pmcg_0/1 TBU TLB 命中率
  为常用直接展示；新增事件如 `dTLB-loads`、`ummu_pmcg_*/tcu_*` 自动进
  perf 分组，默认收起、点"展开"后勾选，报告完整体现不丢指标；**指标名
  与原始事件名完全一致**（含尾斜杠，如 `ummu_pmcg_0/tcu_cntx_cache_miss_num/`），
  恒 0 指标也如实展示完整序列），
  勾选色块与图中序列颜色一致；
- **悬浮透明提示窗**：鼠标悬停显示竖直参考线 + 各序列最近点，
  窗口内呈现该时刻各指标原始值 + 单位；
- **时间轴缩放**：图上横向拖拽选择时间段（10 分钟级跨度可放大到秒级，
  类似 kvcache 各阶段时延趋势的时间段选择），双击或"重置缩放"恢复全量；
- **分组批量选中/取消**：分组标题自带组级复选框，一次勾选/取消整组指标
  （部分勾选显示半选状态；全部取消时图区提示重新勾选）；
- 混合单位同图时各序列归一化 0-100（Y 轴标注"相对幅度"，
  悬浮窗始终显示原始值）；同单位则直接按原始值共轴。

用途：与问题时刻对照，佐证/排除访存类瓶颈（不直接参与定界结论）。
原始时序数据全量在 JSON `numa_stats`。

### OS 资源周期监控独立报告（os_monitor_report.html）

OS 资源类周期监控（关中断 / sar 网卡利用率 / NUMA 访存，后续新增周期监控
指标统一在此扩展）**单独输出独立报告**，不再进入定界主报告：

- **默认自动生成**：采集到任一类周期监控日志时，正常分析模式自动输出到
  `-o` 同目录 `os_monitor_report.html`（可用 `--os-monitor-report` 自定义路径）；
- **独立分析模式**：`--os-monitor-only` 只分析周期监控（跳过慢请求定界分析，
  可无 client 日志，仅需 dscollect 周期监控日志），输出周期监控报告即退出；
- 内容：节点信息表（节点 / 宿主机 IP / 各类日志可用性）+ 关中断统计
  交互探索卡 + 网卡利用率统计卡 + NUMA 访存监控交互探索卡，自包含单文件 HTML。

### 关中断统计交互探索（与 NUMA 访存监控同风格）

OS 资源周期监控报告的"关中断统计（全采集周期，&gt;1ms，交互探索）"卡
采用与 NUMA 访存监控相同的通用交互探索器（**散点模式**），
多物理机不再逐节点平铺：

- **节点下拉选择**：切换物理机重绘（**已勾选进程保留**，同 NUMA 探索器
  的勾选记忆语义）；
- **按进程（comm）序列勾选**：每条序列 = 一个进程的关中断时长散点
  （x=时间 y=关中断时长 us，默认勾选累计时长 top5 进程；面板最多展示
  top20 进程，其余合并为"其他进程"），组级复选框支持整组批量选中/取消；
- **悬浮透明提示窗**：鼠标悬停显示竖直参考线 + 各序列最近点，
  窗口内呈现时间 + 关中断时长 + cpu；
- **时间轴缩放**：图上横向拖拽选择时间段，双击或"重置缩放"恢复全量；
- 图下方保留跨节点汇总统计表：概览表（记录数 / hardirq / softirq /
  最长 / 累计）+ 折叠的时长分桶（按节点）/ 进程 top10（跨节点）/
  Top20 最长记录（跨节点）。

### 窗口级定量归因（irqoff / sar nic / NUMA DDR 参与推理）

三类周期监控数据此前的角色是"问题窗口内展示 + 置信度修饰"，现在升级为
**切片到问题慢段时间窗做定量对比，直接参与定界推理**（`_window_quant_attribution`，
结论前自动执行，命中写入 `quant_evidence` 证据链）：

| 数据源 | 命中条件（问题窗口内） | 推理影响 |
|---|---|---|
| irqoff 关中断 | cpu 匹配（业务 cpu 或其 SMT 姊妹核 `cpu^1`）的最长关中断 ≥ **max(2000us, 50% × 段耗时)** | **改写 category 为 `interrupt_off_delay`**，置信度高，证据含占用占比（如"占段耗时 62.5%"） |
| sar 网卡利用率 | 窗口内某 dev `%ifutil` 峰值 ≥ **80%** 且窗口内存在 TCP 重传 | 为 `network_*_transmission` / `phys_wire` 类定界补充高置信度定量证据（"带宽/队列瓶颈与丢包重传并发"） |
| NUMA DDR 带宽 | 问题窗口 DDR 带宽均值 > **基线（窗口外样本均值 + 2σ；样本不足用全周期）** 且 > 均值 ×1.2（窗口内 ≥2 采样、全周期 ≥8 采样才评估） | 为 `server_processing_slow` 等补充"问题窗口内存带宽饱和"定量证据 |

- 时间窗 = 定界结论瓶颈段区间；irqoff/numa 数据不足或窗口无采样时静默跳过
  （不影响既有结论）；
- 命中详情存 trace 级 `irqoff_quant` / `nic_quant` / `numa_quant`（内部结构），
  文本证据以 `◆ 定量归因（…）` 前缀进入 `conclusion.evidence`。

### 唤醒者回溯（抢占任务从哪来）

softirq 定位结论命中（占用 cpu 的任务 comm 已知）后，自动在问题窗口 bpf 事件流中
回溯 `sched_wakeup → sched_switch` 切入链（`_preemptor_wakeup_trace`），直接回答
**"该任务为何在该 cpu 运行"**——替代原人工"下一步分析调度来源"建议：

- 输出：任务被唤醒时刻/唤醒者所在 cpu（`waker_cpu`）、从哪个任务切上该 cpu
  （`prev_comm`）、切入时刻距收包点多久（`delta_to_recv_us`）、唤醒前同 cpu
  相邻事件（`trigger_kind`，触发源提示）；
- 结论写入 `conclusion.preemptor_origin`（唤醒链文本）与
  `cpu_busy.softirq_localization.wakeup_trace`（结构化），建议直接指向来源分析
  （内核 worker / 其他进程、亲和性配置）；
- 无 wakeup/switch 事件时降级为原人工建议，不影响既有结论。

### 输出位置汇总

| 输出 | 内容 |
|---|---|
| OS 监控报告（`os_monitor_report.html`） | "节点信息"表（节点 / 宿主机 IP / bpf / 调度告警 / irqoff / nic / NUMA 可用性）、"关中断统计（全采集周期，交互探索）"卡（单张散点交互图：节点下拉/按进程勾选/分组批量选中取消/悬浮提示/拖拽缩放 + 跨节点汇总统计表）、"网卡利用率统计（sar）"卡、"NUMA 访存监控（全采集周期，交互探索）"卡（单张折线交互图，同款交互） |
| HTML trace 卡 | meta 表含 "client/server 宿主机 IP" 行（bpf/warn 文件名反查）；"关中断记录（问题窗口内）"（表 + 调用栈折叠）、"sar 网卡采样（问题窗口内）"表、"bthread 协程事件（问题窗口内）"折叠块 |
| JSON 顶层 | `irqoff_stats: {node: {total, hardirq_n, softirq_n, max_us, total_us, by_comm, by_cpu, buckets, series}}`、`nic_stats: {node: {dev: {n_samples, max_ifutil, avg_ifutil, peak_hms, max_rxpck, Speed, Duplex, "Link detected"}}}`、`numa_stats: {node: {numafast: [{ts, score, nids: {NID: {%RMA, %CPU, %MEM}}}], memory: [{ts, l1d/l1i/l2d/l2i_miss_pct, ddrc_read_mb_s, ddrc_write_mb_s}], perf: [{ts, values: {事件名: 计数（全事件动态识别）}, comments: {事件名: 注释}, dtlb_load_misses, itlb_load_misses（已知事件旧字段兼容）}]}}` |
| JSON trace 级 | `client/server` 对象含 `host_ip`（节点名反查的宿主机 IP，未知为 null）；`irqoff_events: {client/server: [{ts, irq, cpu, comm, pid, latency_us, raw}]}`、`nic_samples: {…: [{hms, dev, rxpck, txpck, rxkB, txkB, ifutil}]}`、`bthread_events: {…: [{ts, kind(created/scheduled/completed), tid, bthread_id, pending_time_us, target_pending_tasks, creation_mode, execution_time_us, lifetime_time_us, cpu, raw}]}` |
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
点位不因过滤而遗漏。其中 **TCP 收包类里程碑**（TcpRecvFirst/Last、TcpRecvQue、
SockReadable）额外限定在当前请求锚点时间窗内（client 侧 `[ClientSend−50ms,
ClientRecv+50ms]`，server 侧 `[ClientSend−200ms, max(ServerRecv,ServerSend)+50ms]`），
避免同五元组长连接上**其他请求**的收包事件覆盖本请求里程碑、把真正慢的 seq
事件挤出慢段时间窗；窗口过滤后首点位缺失时自动回退全量填充。

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
- **问题包序号红色高亮**（`seqhl`）：问题连接事件的 TCP 序号族字段
  （nic 层 `seq`，tcp 层 `rcv_nxt`/`copied_seq`，重传 `tx_seq`/`snd_una`/
  `snd_nxt`）值收集为问题序号集合；事件任一序号字段命中时该事件全部
  序号字段红色标注（"问题窗口全景与 CPU 侵占分析"块同样生效）——同一
  问题包在 `dev_start_xmit` / `netif_receive_skb` / `tcp recv que/in` 等
  不同层事件中一眼可追踪（序号集合按 trace 两侧窗口事件合并收集，跨层
  跨侧高亮）；
- **过滤选择**：事件过多时子项②③表头带**过滤工具条**——"全部 / 仅问题
  连接 / 仅其他连接"按钮（归属过滤）+ 关键字输入框（文本过滤），实时
  切换行显示并显示"显示 X / N 条"计数（纯前端，事件委托实现，多表共用
  一套监听）；
- **上限**：HTML 每表展示前 500 条（按时间序，超限标注总数，建议用
  `--trace` 缩小范围）；全景数据层上限 4000 条连接类事件/（trace, 侧）；
- **去重**：该侧已有 cpu_busy 分析（kernel_to_user 段异常）时，子项③
  不再单独输出——"问题窗口全景与 CPU 侵占分析"块的窄窗口全景表已覆盖
  该侧问题窗口（含归属过滤），无 cpu_busy 时保留子项③兜底；
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

### 报告结构收敛（去冗余）

- **宏观三段并入内核分段表**：trace 卡原独立的宏观三段表取消，宏观段作为
  组头行（`seg-grp` 样式）并入"内核级分段"表，一张表看全部分段；
- **整窗全景去重**：该侧已有 cpu_busy 分析时不再重复输出"问题时间窗全景"
  子项（cpu_busy 的窄窗口全景表已覆盖，含归属过滤）；
- **行渲染合一**：事件明细表（6 列）与全景表（7 列，带归属高亮列）共用
  统一行渲染函数，推测关联（"推测"badge）与业务 cpu 红色标注两表一致；
- **主报告周期监控摘要化**：irqoff / sar nic 全周期统计在主报告收敛为
  一行结论摘要卡（节点数/记录数/最长、峰值 %ifutil），全量交互探索
  （分桶/进程 top/散点/趋势）统一在独立 `os_monitor_report.html`
  （问题时刻窗口内记录仍在各 trace 卡）；
- **探索器 JS 公共化**：os_monitor_report.html 的交互探索器（NUMA /
  关中断）提取报告级公共工厂函数（单份 JS/CSS），卡内仅保留初始化调用，
  报告体积更小。

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
  - 业务 cpu 上出现 **softirq 处理超长**事件（`softirq_exit_delay`，
    entry→exit >1ms，软中断执行期间业务线程无法在该 cpu 运行，见第 3 节）；
- **排除性结论**：业务 cpu 上无其他连接事件时，明确输出"软中断处理其他
  请求的抢占可能性低"，并给出窗口内其他连接事件的 cpu 分布供参考。

### 3. softirq 探针定界（收包慢的进一步定位）

bpftrace 增加 softirq 三点位（`softirq_raise` → `softirq_entry` →
`softirq_exit`），超过 1ms 时在 bpf 日志打印两类事件行，工具解析后结合
问题窗口做网卡收包慢定界（仅 kernel_to_user 段异常的侧触发）：

| 事件行（bpf 日志原始格式） | kind | 定界含义 |
|---|---|---|
| `HH:MM:SS:uuuuuu high irq-to-softirq  vec=N latency: X usec (Y ms) on CPU:C comm:NAME kstack:STACK` | `softirq_raise_delay` | **raise→entry >1ms**：软中断发起到开始执行期间被其他任务抢占/延迟，收包协议栈处理（NetifRx→TcpRecv）被推迟（kstack 为当时调用栈） |
| `HH:MM:SS:uuuuuu slow softirq! cpu: C \| Type: N \| Latency: X us, timercnt:a/b` | `softirq_exit_delay` | **entry→exit >1ms**：软中断本身处理时间太长，执行期间业务线程无法在该 cpu 运行（软中断不可被调度抢占） |

- **关联方式**：事件时间戳落在问题窗口内且 `cpu` 等于业务线程所在 cpu
  时生成"◎"证据（其他 cpu 上的 softirq 事件仍进全景表但不构成证据）；
- **vec 含义**：向量号→名称映射（`3(NET_RX)` 收包 / `1(TIMER)` 定时器 /
  `7(SCHED)` 调度等）；`softirq_exit_delay` 附带 `timercnt`（窗口内 timer
  软中断次数/超长次数），可用于判断是否 TIMER 软中断拖长 NET_RX 处理；
- **置信度影响**：`softirq_exit_delay` 在业务 cpu 上 → 计入抢占证据
  （`cpu_busy_preempt`，可提升高置信）；`softirq_raise_delay` 仅作证据句
  （软中断被延迟≠业务线程被软中断占用），不改变 preempt 判定；
- **输入可选**：bpf 日志无 softirq 探针行时静默降级，不影响既有分析。

### 4. softirq 定位结论（收包慢直接定界）

在探针定界基础上更进一步：若收包慢时间段内、**收包时间往前回溯 50ms**
（`SOFTIRQ_LOOKBACK_MS`）窗口内存在 softirq 慢日志（`softirq_raise_delay`），
且事件 cpu 与**收包 cpu 相同或为其 SMT 姊妹核**（如 0/1 互为 SMT 核对应
一个物理核，`cpu^1` 相邻配对），则**直接定位**抢占 cpu 的任务（`comm`）并
呈现其完整调用栈（`kstack`）——收包慢问题即定界；命中后自动执行
**唤醒者回溯**（见"唤醒者回溯"小节）回答"该任务为何在该 cpu 运行"。
分两种模式：

| 模式 | 触发条件 | 回溯匹配规则 |
|---|---|---|
| `kernel_to_user` | 该侧"内核收包 → 用户态取包"段异常 | 收包点（NetifRx 里程碑，缺则窗口起点）前 50ms 内，cpu = 业务 cpu 或其 SMT 姊妹核的 `softirq_raise_delay` 事件；优先 NET_RX（vec=3），其次延迟最大 |
| `wire` | 该侧接收线路段（`wire_s2c_phys`/`wire_c2s_phys`）异常 | "线路慢"实为收包软中断被占用：仅 NET_RX（vec=3），entry 不晚于收包点（该 softirq 即执行本次收包的上下文），raise 时间不早于对端物理网卡发出（排除更早一批包的软中断）；取 entry 最紧邻收包点者 |

- **定位输出**：`comm`（占用 cpu 的任务名）+ 完整 `kstack`（多行续行已
  解析合并）+ `latency_us`（raise→entry 延迟）+ `vec_txt` + 事件 cpu 与
  业务 cpu（`anchor_cpu`）+ 是否 SMT 姊妹核（`smt`）；
- **醒目呈现**：命中时 trace 卡**顶部红色高亮横幅**（`loc-banner`，
  "根因已定位 —— 收包慢直接定界"，含占用任务/cpu/延迟/模式 + 完整调用栈，
  无需展开下方明细）+ trace 头部红色 badge（"根因已定位"，未展开卡片即可见）
  + 概览索引"已定位"标记 + 证据链"【已定位】"条目红色加粗；
- **结论呈现**：命中时证据链追加"【已定位】…收包慢根因"（含占用任务、
  cpu、延迟、raise/entry 时刻与完整调用栈），结论建议直接指向占用任务
  （分析其调度来源/绑核与亲和性配置/触发路径），置信度提升为"高"；
- **wire 模式补充**：该侧 kernel_to_user 段不异常时无既有 cpu_busy 信息，
  工具会补最小信息（窗口即 wire 段区间）使渲染/JSON 走同一通道；
- **负例**：回溯窗内无匹配事件 / cpu 不匹配 / vec 不符 → 不生成定位
  结论，不影响既有分析。

### 证据与结论影响

| 项 | 说明 |
|---|---|
| 证据句 | `◎` 前缀写入结论证据链（JSON `conclusion.evidence`）：窗口/事件统计、业务 cpu 上其他连接事件数（含 top3 连接）、线程被切出次数与切向、softirq 发起延迟/处理超长（含 vec 与最大延迟）；softirq 定位命中时追加"【已定位】…收包慢根因"（含占用任务 comm/cpu/延迟/完整调用栈） |
| 置信度 | 定界为 `client/server_kernel_to_user_delay` 或 `coroutine_schedule_delay` 且存在抢占证据（`cpu_busy_preempt`）时提升为"高"；softirq 定位命中（传输类定界 + 网卡点位佐证）亦为"高" |
| 排查建议 | 抢占成立时追加：调整网卡 RSS/中断亲和性将收包分散到非业务 cpu、业务线程绑核 / isolcpus 隔离；softirq 定位命中时建议直接附**唤醒者回溯结论**（`preemptor_origin`：被谁唤醒/何时切入/距收包点多久），并指向占用任务的来源分析与亲和性配置 |

### 输出位置

| 输出 | 内容 |
|---|---|
| HTML trace 卡 | **根因已定位醒目呈现**（命中时）：trace 卡顶部红色高亮横幅（loc-banner，含占用任务/cpu/延迟/完整调用栈 + 唤醒者回溯结论）+ trace 头部"根因已定位"badge + 概览索引"已定位"标记；"问题窗口 bpf 事件全景与 CPU 侵占分析"块：摘要卡（窗口/业务线程/事件统计/cpu 结论/softirq 结论）+ 全景事件表（黄底 = 问题五元组行，红色 cpu = 业务线程所在 cpu，softirq 行附加列含 vec/延迟/kstack）+ softirq 定位结论块（命中时：定位说明 + 占用任务 comm + raise→entry 延迟 + 完整调用栈，wire 模式区分展示"线路段慢实为收包软中断被抢占"） |
| JSON trace 级 | `cpu_busy: {client/server: {seg_key, seg_desc, seg_dur_us, window_start, window_end, anchor_name, anchor_tid, anchor_cpu, conn, n_mine, n_other, other_conns, other_by_cpu, other_on_cpu/switches_on_cpu/switched_out/softirq_raise_on_cpu/softirq_exit_on_cpu（均为 {n, ts_list} 引用）, preempt, softirq_localization?（含 wakeup_trace 唤醒者回溯）, window_events（{n, first_ts, last_ts} 摘要）}}`（schema v2：明细经 ts 与 `kernel_events` 对齐；顶层 `softirq_events` 已删除）；`conclusion.preemptor_origin` 为唤醒链文本（命中时） |
| `--raw` | 每 trace 追加"问题窗口 bpf 事件全景"段（标注窗口区间/来源文件/条数，问题连接行 `▶` 前缀，穿插展示其他请求原始日志行）+ softirq 定位结论段（命中时含占用任务与完整调用栈，wire 模式含线路段区间说明） |

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
| JSON trace 级 | `slow_seg_window: {seg_key, seg_desc, category, window_start, window_end, dur_us, sides: {client/server: {n_mine, n_other, by_kind}}}`（无瓶颈段时为 null；schema v2 事件明细 = `kernel_events` 按窗口 ts 过滤） |
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

**brpc 协程日志**（<podName>-brpc_client.log / -brpc_server.log，可选；glog 格式）：
`IMMDD HH:MM:SS.usec tid N ... start_foreground] [WZY] bthread created: creator_tid=... bthread_id=... creation_mode=... target_pending_tasks=...` /
`... task_runner] [WZY] bthread first scheduled: worker_tid=... cpu_id=N bthread_id=... pending_time_us=...` /
`... task_runner] [WZY] bthread completed: worker_tid=... cpu_id=N bthread_id=... execution_time_us=... lifetime_time_us=...`。

---

## 注意事项

1. **时钟域**：跨节点耗时一律用日志行 wall clock 相减；BRPC 单调时钟 ts
   （ClientSend= 等）仅用于锚点行的精确匹配，不可跨节点相减。若节点间存在时钟
   偏差，跨节点段耗时仅供参考（报告中有提示）。
2. **node 关联**：podIP 与 nodeIP 通常不同网段，映射优先级为
   **env 文件（pod_ip= / *HOST_IP=）→ 日志正文 Host ID is/id is 行 →
   目录名 IP 直查 node_by_ip → 节点名子串匹配**（最长匹配优先，避免 worker1
   误配 worker13；同一 IP 多命名时别名收敛到先注册的规范名）。宿主机 IP 已知
   但该节点无 bpf 文件时，missing 明确提示"宿主机 X 的 bpf 日志未采集"
   （区别于完全无法映射）。client pod 目录无法映射到 bpf 节点且 server 侧
   回退识别连接成功时，仅 client 侧内核事件缺失，server 侧分析照常
   （missing 中注明回退）。
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

对 `/home/wcy/minilog` 实际日志（141.62 网段三节点 bpf 共 5.5GB、tar.gz 归档 +
已解压双份、worker 目录名 `worker_<podIp>`、client 为 `SDK_<podIp>` 目录无 env
文件）验证通过：client 51/51 经**日志正文 Host ID 行**（`Host ID is 141.62.33.21
from env HOST_IP`）解析到宿主机 worker22 节点；server 侧经 env 映射定位 worker12
（141.62.32.59）；命中的 trace 双侧 bpf 事件恢复（client 9 + server 13 条），
连接五元组 192.168.49.66:53896 ↔ 192.168.210.192:31402 从 client 侧 tcp send
正常识别，定界"server 收包后唤醒/调度慢"（高置信）。其余 50 条问题请求因对应
worker pod 的 kvcache 日志未收集，正确标注"证据不足"（提示补充采集）。
