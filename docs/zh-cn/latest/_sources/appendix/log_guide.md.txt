# openYuanrong datasystem 日志

## 日志类别

openYuanrong datasystem 的日志分为以下类型：

1. **运行日志**：记录客户端、服务端运行时的日志信息，包括 INFO、WARNING、ERROR、FATAL；更细粒度调试可通过 `VLOG` 与 gflags（如 `--v`）控制。
2. **访问日志**：记录每一次访问客户端/服务端的请求，每个请求一条日志，用于定界上游是否访问客户端/服务端（需开启 `log_monitor`）。
3. **请求第三方日志（request_out）**：记录 Worker 访问第三方组件的请求，每个请求一条日志，可用于定界 openYuanrong datasystem 是否成功访问该外部组件；当前实现中 **主要接入 ETCD gRPC**（需开启 `log_monitor`）。
4. **资源日志**：定时输出 Worker 运行时关键资源信息，包括共享内存、Spill 磁盘、线程池、队列、流缓存相关统计等（需 `log_monitor` 且 `log_monitor_exporter=harddisk`）。
5. **流缓存指标日志（sc_metrics）**：流缓存运行数据（需开启 `log_monitor`）。
6. **容器/进程相关日志**：容器运行日志，管理和监控worker进程的生命周期。
7. **操作审计日志（operation）**：记录进程启动/停止、配置初始化及运行时动态配置变更（`UpdateConfig` API 或 `monitor_config_file` 热更新），便于运维追溯配置操作历史。
8. **资源快照日志（kv_resource）**：`resource.log` 同源字段的 JSON-Lines 快照，每行一个完整 JSON 对象，顶层含 `time`/`pod_name`/`cluster_name`，`metrics` 为按 `resource_json_schema` 配置的 metric 组及子字段（需 `json_log_monitor` 且 `log_monitor_exporter=harddisk`；与 `resource.log` 复用同一采集线程和周期，写入由 `json_log_monitor` 独立控制）。
9. **指标摘要日志（kv_metrics）**：`datasystem_worker.INFO.log` 中 `metrics_summary` 摘要的 JSON-Lines 并行输出，每行一个完整 JSON 对象，顶层含 `time`/`pod_name`/`cluster_name`，body 与 INFO.log 逐字一致（需 `json_log_monitor`；INFO.log 原 `metrics_summary` 仍由 `log_monitor` 控制）。

日志目录一般为配置项 `log_dir`（如部署下的 `yr_datasystem/logs`）；下文 `/path/yr_datasystem/logs` 表示该目录。

### 不同模块日志文件

| 序号 | 模块 | 日志路径及文件名 | 含义、用途 |
|---|------|-----------------------------------------|-----------------------------------------|
| 1 | datasystem_worker | `/path/yr_datasystem/logs/{log_filename}.INFO.log`（及 `.WARNING`、`.ERROR` 等轮转文件） | Worker 运行日志；`log_filename` 由部署配置决定，示例常为 `datasystem_worker` |
| 2 | datasystem_worker | `/path/yr_datasystem/logs/access.log` | 访问 Worker POSIX 等接口的日志 |
| 3 | datasystem_worker | `/path/yr_datasystem/logs/resource.log` | Worker 资源使用日志；默认关闭，由 `log_monitor` 控制 |
| 4 | datasystem_worker | `/path/yr_datasystem/logs/request_out.log` | 访问第三方（当前主要为 ETCD）接口日志 |
| 5 | datasystem_worker | `/path/yr_datasystem/logs/sc_metrics.log` | 流缓存运行数据；由 `log_monitor` 控制是否开启 |
| 6 | datasystem_worker | `/path/yr_datasystem/logs/container.log` | 容器运行日志，管理和监控worker进程的生命周期 |
| 7 | datasystem_worker | `/path/yr_datasystem/logs/{log_filename}_operation.log` | 操作审计日志；记录 Init/Shutdown、配置初始化快照及运行时 flag 变更（`UpdateConfig` 或配置文件热更新） |
| 8 | datasystem_worker | `/path/yr_datasystem/logs/kv_resource.log` | 资源快照 JSON-Lines；`resource.log` 同源字段的纯 JSON 输出，顶层 `time`/`pod_name`/`cluster_name`，`metrics` 按 schema 输出已配置的 metric 组；由 `json_log_monitor` 且 `log_monitor_exporter=harddisk` 控制 |
| 9 | datasystem_worker | `/path/yr_datasystem/logs/kv_metrics.log` | 指标摘要 JSON-Lines；INFO.log 中 `metrics_summary` 的并行输出，body 逐字一致，额外前置 `time`/`pod_name`/`cluster_name`；由 `json_log_monitor` 控制（INFO.log 原输出仍由 `log_monitor` 控制） |
| 10 | Client | `/path/client/ds_client.INFO.log`（及 `.WARNING`、`.ERROR` 等；关闭 `DATASYSTEM_CLIENT_LOG_WITHOUT_PID` 后恢复为 `/path/client/ds_client_{pid}.INFO.log`） | SDK 运行日志；基名可由启动参数与环境变量 `DATASYSTEM_CLIENT_LOG_NAME` 覆盖 |
| 11 | Client | `/path/client/ds_client_operation.log`（关闭 `DATASYSTEM_CLIENT_LOG_WITHOUT_PID` 后恢复为 `/path/client/ds_client_{pid}_operation.log`） | Client 操作审计日志；记录 Init/Shutdown 及 `UpdateConfig` 动态配置变更 |
| 12 | Client | `/path/client/ds_client_access.log`（关闭 `DATASYSTEM_CLIENT_LOG_WITHOUT_PID` 后恢复为 `/path/client/ds_client_access_{pid}.log`） | SDK 接口访问日志；基名可由 `DATASYSTEM_CLIENT_ACCESS_LOG_NAME` 覆盖 |

---

## 日志格式

### 格式总览

| 序号 | 日志 | 日志格式 |
|-----|-----|------------------------|
| 1 | 运行日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| message |
| 2 | 访问日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| status_code \| action \| cost \| data size \| request param\| response param |
| 3 | 访问第三方日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| status_code \| action \| cost \| data size \| request param\| response param |
| 4 | 资源日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| shm info \| spill disk info \| client nums \| object nums \| object total datasize \| WorkerOcService threadpool \| WorkerWorkerOcService threadpool \| MasterWorkerOcService threadpool \| MasterOcService threadpool \| write ETCD queue \| ETCDrequest success rate \| OBSrequest success rate \| Master AsyncTask threadpool \| stream nums \| ClientWorkerSCService threadpool \| WorkerWorkerSCService threadpool \| MasterWorkerSCService threadpool \| MasterSCService threadpool \| remote stream push success rate \| shared disk info \| scLocalCache info \| Cache Hit Info \| brpc stream leak count \| deferred cleanup queue size |
| 5 | 流缓存数据日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| sc_metric |
| 6 | 容器运行日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| message |

### 日志字段

| 字段 | 长度 Byte | 描述
|-------|---------------|-------|
| time | 26 | ISO8601格式的时间戳，示例: 2023-06-02T14:58:32.081156
| level | 1 | 日志级别 (debug, info, warn等)
| filename | 128 | 输出该条日志的函数所在文件及行号，最大长度128Byte，超出则截断。示例：oc_metadata_manager.cpp:733 |
| pod_name | 128 | 输出当前worker所属的POD名称，超出长度则截断。示例：ds-worker-hs5qm |
| pid:tid | 11 | 该日志所属的进程ID和线程ID。进程号最大值为32757，该字段最大长度11.示例：9:177 |
| trace_id | 36 | 请求的trace_id |
| cluster_name | 128 | 输出日志的组件名，最大长度为128，超出长度则截断。示例：ds-worker |
| Message | 1024 | 自定义消息内容 |
| status_code | 5 | 该请求的状态，不同消息类型状态值不一样。SDK/datasystem_worker 访问日志，0表示成功，其他表示失败 |
| action | 64 | 表示该请求所访问的接口名称。约定前缀：SDK接口：DS_KV_CLIENT、DS_OBJECT_CLIENT，Worker接口：DS_OBJECT_POSIX，ETCD：DS_ETCD，HTTP请求：POST {url path} |
| cost | 16 | 记录该请求所花费的时间。单位：us |
| datasize | 16 | 记录Publish请求接收到的Payload大小 |
| request param | 2560 | 记录该请求的关键请求参数，最大长度2048。请参考“关键请求参数”表格 |
| response param | 1024 | 记录该请求的响应信息。最大长度为1024 Byte，超出则截断 |
| shm info | 47 | 记录共享内存使用信息，单位为Byte，按照1T限制大小，每个长度 13 Byte，格式为：memoryUsage/physicalMemoryUsage/totalLimit/rate/scMemoryUsage/scMemoryLimit<br>1) memoryUsage	已分配的内存大小，是已缓存的对象大小总和。注意：由于系统中使用jemalloc管理内存，实际分配的内存会按size class对齐，因此memoryUsage通常比实际对象大小的总和高一些，包含一定的内存对齐开销。<br>2) physicalMemoryUsage	已分配的物理内存大小。<br>3) totalLimit	共享内存总大小。<br>4) Rate	共享内存使用率，memoryUsage/totalLimit, 保留3位有效数字，单位: % <br>5) scMemoryUsage 流缓存共享内存使用大小，单位：byte <br>6) scMemoryLimit 流缓存的共享内存限制，单位：byte | 
| spill disk info | 47 | 记录Spill磁盘使用信息。单位为Byte，按照1T限制大小，每个长度 13 Byte，格式为：spaceUsage/physicalSpaceUsage/totalLimit/rate<br>1) spaceUsage	已使用的磁盘大小，是已Spill的对象大小总和。<br>2) physicalSpaceUsage	已使用的物理磁盘大小。<br>3) totalLimit	Spill磁盘总大小。<br>4) Rate	Spill磁盘使用率，spaceUsage /totalLimit, 保留3位有效数字，单位: % | 
| client nums | 5 | 记录已和worker成功建立连接的Client数。最大值为10000 | 
| object nums | 9 | 记录worker已缓存对象数。按照1亿对象限制数量 | 
| object total datasize | 13 | 记录worker已缓存对象的大小。按照1T限制大小，长度 13 Byte | 
| WorkerOcService threadpool | 21 | WorkerOcService threadpool使用信息，线程数限制最大128；格式为：idleNum/currentTotalNum/MaxThreadNum/waitingTaskNum/rate<br>1) idleNum	空闲线程数；<br>2) currentTotalNum	当前正在运行任务的线程数；<br>3) MaxThreadNum	 threadpool最大可申请的线程数；<br>4) waitingTaskNum	正在等待的任务数。<br>5) rate	线程利用率，currentTotalNum/ MaxThreadNum,单位：%，保留3位有效数字 | 
| WorkerWorkerOcService threadpool | 21 | threadpool使用信息 | 
| MasterWorkerOcService threadpool | 21 | threadpool使用信息 | 
| MasterOcService threadpool | 21 | threadpool使用信息 |
| write ETCD queue | 15 | 队列使用信息 | 
| ETCDrequest success rate | 6 | 请求成功率，单位 %,保留3位有效数字 | 
| OBSrequest success rate | 6 | 请求成功率，单位 %,保留3位有效数字 | 
| Master AsyncTask threadpool | 21 | threadpool使用信息，格式为：idleNum/currentTotalNum/MaxThreadNum/waitingTaskNum/rate |
| stream nums | 9 | 记录本节点流的个数。本节点上的流：在本节点存在producer或者consumer |
| ClientWorkerSCService threadpool | 21 | 线程池使用信息，格式为：idleNum/currentTotalNum/MaxThreadNum/waitingTaskNum/rate |
| WorkerWorkerSCService threadpool | 21 | 线程池使用信息，格式为：idleNum/currentTotalNum/MaxThreadNum/waitingTaskNum/rate |
| MasterWorkerSCService threadpool | 21 | 线程池使用信息，格式为：idleNum/currentTotalNum/MaxThreadNum/waitingTaskNum/rate |
| MasterSCService threadpool | 21 | 线程池使用信息，格式为：idleNum/currentTotalNum/MaxThreadNum/waitingTaskNum/rate |
| remote stream push success rate | 6 | 请求成功率，单位 %,保留3位有效数字 |
| shared disk info | 47 | 记录共享磁盘使用信息，单位为Byte，按照1T限制大小，每个长度 13 Byte，格式为：usage/physicaleUsage/totalLimit/rate<br>1) usage  已使用的磁盘大小，是已缓存的对象大小总和<br>2) physicaleUsage 已使用的物理磁盘大小<br>3) totalLimit     共享磁盘总大小<br>4) rate   共享磁盘使用率，usage/totalLimit, 保留3位有效数字，单位: % |
| scLocalCache info | 47 | 记录scLocalCache使用信息，单位为Byte，按照1T限制大小，每个长度 13 Byte，格式为：usedSize/reservedSize/totalLimit/usedRate |
|Cache Hit Info | 9 | 缓存命中统计,格式为:memHitNum/diskHitNum/l2HitNum/remoteHitNum/missNum,<br>1) memHitNum	本地内存命中次数。<br>2) diskHitNum	本地磁盘命中次数. <br>3) l2HitNum	二级缓存命中次数。<br>4) remoteHitNum 远端worker命中次数。<br>5) missNum 未命中次数。
| brpc stream leak count | 9 | brpc stream 关闭超时后为避免 UAF 而有意泄漏的 `brpc::Controller` 累计次数（进程启动以来单调递增）。正常应为 0 |
| deferred cleanup queue size | 9 | 当前待延迟清理的 `brpc::Controller` 队列深度；stream 关闭超时时若配置了 `closeNotifier`，Controller 会入队等待 reaper 线程释放 |
| sc_metric | 1024 | 流缓存运行数据(sc_stream_metric)。worker上一个stream的的流缓存数据，格式：streamName ["exit"]/numLocalProd/numRemoteProd/numLocalCon/numRemoteCon/sharedMemUsed/localMemUsed/numEleSent/numEleRecv/numEleAck/numSendReq/numRecvReq/numPagesCreated/numPagesReleased/numPagesInUse/numPagesCached/numBigPagesCreated/numBigPagesReleased/numLocalProdBlocked/numRemoteProdBlocked/numRemoteConBlocking/retainData/streamState/numProdMaster/numConMaster<br>1) streamName ["exit"] stream名字，带有" exit"表示stream正要关闭<br>2) numLocalProd 本地producer数量<br>3) numRemoteProd - Number of remote workers with atleast one producewill be 0, if no local consumers)<br>4) numLocalCon 本地consumer数量<br>5) numRemoteCon 远端consumer数量<br>6) sharedMemUsed stream使用共享内存大小，单位: Byte<br>7) localMemUsed stream使用本地内存大小，单位: Byte<br>8) numEleSent - Total number of elements produced by all local producers<br>9) numEleRecv - Total number of elements received by all local consumers (value will be 0, if no local consumers)<br>10) numEleAck element acked数量<br>11) numSendReq client调用producer.send()次数<br>12) numRecvReq client调用consumer.receive()次数<br>13) numPagesCreated page创建次数<br>14) numPagesReleased page释放次数<br>15) numPagesInUse page in use数量<br>16) numPagesCached page cached数量<br>17) numBigPagesCreated big element page创建次数<br>18) numBigPagesReleased big element page释放次数<br>19) numLocalProdBlocked 本地producer blocked数量<br>20) numRemoteProdBlocked 远端producer blocked数量<br>21) numRemoteConBlocking 远端consumer blocking数量<br>22) retainData retain data state<br>23) streamState stream state<br>24) numProdMaster master上producer数量<br>25) numConMaster master上consumer数量<br>- 如果worker不是stream的master，24-25会没有数据。如果worker只有master数据，2-23会没有据 |

### SDK 与 Worker 访问日志关键请求参数

| 关键请求参数 | 长度 Byte | 描述 |
|------------|-------------|---------|
| Object_key | 1024 | 对象的ID。长度：1024Byte； |
| object_keys | 1024 | 多个对象 KEY。单个 Item 最长 1024 Byte，展示总长上限约 1KB；超出时仅显示总数等缩写。示例 JSON：`{"object_keys":["id1xx","id2xx","id3xx","id4xx"],"total":100}` |
| Nested_keys | 1024 | 嵌套引用的对象KEY。单个Item长度：1024Byte，全部长度：1KB； |
| keep | 1 | 是否手动管理对象生命周期。取值：true/false, 长度：5Byte |
| Write_mode | 1 | 写数据的模式，影响数据可靠性。取值：int32 |
| consistency_type | 1 | 数据一致性模式。取值：uint32 |
| is_seal | 1 | 数据是否不可修改。取值：0/1, 0表示false |
| is_retry | 1 | 是否为重试场景。取值：0/1 |
| ttl_second | 32 | TTL时间配置。取值：uint32 |
| existence | 1 | 配置Key存在时是否允许继续操作，默认允许。取值：int |
| sub_timeout | 32 | Get请求订阅时间。取值：int64 |
| timeout | 32 | 接口超时时间。取值：int32 |

### 访问第三方日志关键请求参数

| 外部组件 | 请求类型 | 关键请求参数 | 描述 |
|------|--------|----------|------|
| ETCD | GRPC | key | 将Key字段获取并打印。|

### 资源快照日志（kv_resource.log）字段

每行一个完整 JSON 对象，采集周期与 `resource.log` 相同（`log_monitor_interval_ms`，默认 10s）。

**顶层字段**：`time`（ISO8601，与文本日志前缀 time 格式一致）、`pod_name`、`cluster_name`、`event`（固定 `resource_snapshot`）、`version`（当前 `v0`）、`metrics`（object）。

**`metrics` 内已输出的 metric 组**（与 `resource.log` 管道字段同源；字段映射真源：`resource_json_schema`）：

| JSON 组名 | 子字段 | 对应 resource.log 语义 |
|-----------|--------|------------------------|
| `shared_memory` | `memory_usage`、`physical_memory_usage`、`total_limit`、`worker_share_memory_usage` | 共享内存使用 |
| `spill_hard_disk` | `space_usage`、`physical_space_usage`、`total_limit`、`worker_spill_hard_disk_usage` | Spill 磁盘使用 |
| `active_client_count` / `object_count` / `object_size` | （标量） | Client 数 / 对象数 / 对象总大小 |
| `worker_oc_service_thread_pool` 等 OC 线程池组 | 线程池五元组 | 线程池状态 |
| `etcd_queue` / `etcd_request_success_rate` | 见 schema | ETCD 队列 / 成功率 |
| `master_async_tasks_thread_pool` | 线程池五元组 | Master 异步任务线程池 |
| `oc_hit_num` | 命中统计五元组 | 缓存命中统计 |
| `brpc_stream_leak_count` | （标量） | brpc stream 有意泄漏计数 |
| `deferred_cleanup_queue_size` | （标量） | 延迟清理队列深度 |

---

## 日志采样

LogSampler 提供统一随机哈希阈值采样，替代旧的 `log_rate_limit` first-N 限速机制。

### 工作原理

- 三类采样率：`request_sample_rate`（请求主采样）、`access_sample_rate`（access 补采样）、`diagnostic_sample_rate`（diagnostic 补采样）（各 [0.0–1.0]，默认 1.0=全量保留）
- 采样粒度是"请求（traceId）"，不是"单条日志"
- 请求采样决策随 RPC 元数据传播（LogSampleState），跨 client/worker 保持同一 trace 的一致结果
- request sampled-in 时，该请求的 INFO/VLOG/ERROR/WARNING/SLOW_LOG 和 access 日志都直接输出（请求日志完整性优先）
- request reject 只直接阻断 INFO/VLOG；diagnostic/access 不把 reject 当作直接丢弃条件，继续各自补采样
- 仅 SDK 请求 trace 参与采样；后台线程日志不受本方案控制，始终全量输出
- 配置权威源：worker；client 通过 register/heartbeat 接收 worker 下发的 `LogSampleConfigPb`

### 参数语义与 OR 规则

三个参数不是完全独立的——`access_sample_rate` 和 `diagnostic_sample_rate` 是**补采样率**，仅在请求未被 `request_sample_rate` 采中时生效。请求一旦采中，该请求的所有 access 和 diagnostic 日志**无条件强制输出**。

实际日志保留率公式：

- **access 保留率** = `request_sample_rate` + (1 − `request_sample_rate`) × `access_sample_rate`
- **diagnostic 保留率** = `request_sample_rate` + (1 − `request_sample_rate`) × `diagnostic_sample_rate`

> **注意**：`access_sample_rate=0.3` 不是"30% 的 access 日志被保留"，而是"未被 request 采中的请求中，30% 的 access 日志作为补充被保留"。实际保留率通常高于此值。

**配置示例与实际保留率对照**：

| 配置 | request采中率 | access补采样率 | **access实际保留率** | diagnostic补采样率 | **diagnostic实际保留率** |
|------|--------------|--------------|--------------------|--------------------|----------------------|
| `request=0.5, access=0.3, diagnostic=0.4` | 50% | 30% | **65%** (0.5+0.5×0.3) | 40% | **70%** (0.5+0.5×0.4) |
| `request=1.0, access=0.0, diagnostic=0.0` | 100% | 0% | **100%** (采中→强制输出) | 0% | **100%** (采中→强制输出) |
| `request=0.0, access=0.5, diagnostic=0.5` | 0% | 50% | **50%** (全靠补采样) | 50% | **50%** (全靠补采样) |
| 仅 `request=0.2`（派生） | 20% | 60%(3r派生) | **68%** (0.2+0.8×0.6) | 80%(4r派生) | **84%** (0.2+0.8×0.8) |
| `request=0.5, access=0.0` | 50% | 0% | **50%** (仅采中请求输出) | 100%(4r派生) | **100%** (0.5+0.5×1.0) |

> `request=1.0` 时无论 access/diagnostic 设多少，实际保留率都是 100%（所有请求采中→强制输出）。此时 `access=0.0` 仅影响 access log 中的 `logSampled` 标记，不影响日志输出。

### 派生规则（request-only derivation）

当仅显式配置 `request_sample_rate`，而未显式设置 `access_sample_rate` 或 `diagnostic_sample_rate` 时，两者自动按以下公式派生：

- `access_sample_rate = min(1.0, request_sample_rate × 3)` （简称 3r 规则）
- `diagnostic_sample_rate = min(1.0, request_sample_rate × 4)` （简称 4r 规则）

**示例：** 仅设置 `request_sample_rate=0.2` → effective `access=0.6`, `diagnostic=0.8`

### 显式 1.0 阻止派生

即使 `access_sample_rate=1.0` 与默认值相同，显式设置也意味着"用户明确要求全量保留 access 日志"，**阻止自动派生**。这与"未设置时默认 1.0"有本质区别：

| 配置 | access effective 值 | 派生行为 |
|------|---------------------|----------|
| 仅 `request_sample_rate=0.2` | 0.6 (派生) | 派生生效 |
| `request_sample_rate=0.2` + `access_sample_rate=1.0`(显式) | 1.0 (显式) | 派生被阻止 |
| `request_sample_rate=0.2` + `access_sample_rate=0.3`(显式) | 0.3 (显式) | 派生被阻止 |

### Sticky Explicit 行为

一旦某个采样率被显式设置（如 `access_sample_rate=0.3`），后续仅修改 `request_sample_rate` 的动态更新不会覆盖已显式设置的值。这防止了配置漂移导致的意外日志量变化。

**示例：**
1. 首次配置 `request=0.2` → access 派生 0.6
2. 动态更新显式 `access=0.3` → access 固定 0.3，标记为 ever-explicit
3. 再次更新 `request=0.1` → access 仍为 0.3（不会被派生覆盖为 0.3×3=0.3，而是保持显式值）

### 配置方式

| 场景 | 配置方式 | 示例 |
|------|----------|------|
| Worker 命令行 | `--request_sample_rate=0.5`（仅 request 触发派生） | `./datasystem_worker --request_sample_rate=0.5` |
| Worker 命令行（阻止派生） | `--request_sample_rate=0.5 --access_sample_rate=1.0` | access=1.0（显式），阻止 3r 派生 |
| K8s Helm | `values.yaml` 中仅设置 `requestSampleRate: 0.5`（access/diagnostic 留空） | 自动派生 |
| K8s Helm（阻止派生） | `values.yaml` 中设置 `accessSampleRate: 1.0` | access=1.0（显式） |
| dscli | `dscli start --request_sample_rate 0.2`（仅传 request） | 自动派生 |
| Embedded Worker | `config.RequestSampleRate(0.5)`（仅 request 触发派生） | `config.RequestSampleRate(0.5)` |
| Embedded Worker（阻止派生） | `config.RequestSampleRate(0.5).AccessSampleRate(1.0)` | access=1.0（显式） |
| 运行时动态修改 | 修改 `datasystem.config` 中 `request_sample_rate` 等 | — |

默认值均为 `1.0`（全量保留），完全向后兼容。

---

## 慢日志与 latencySummary

当请求的处理或 RPC 阶段时延超过配置阈值时，access log 中会额外输出 `latencySummary` 字段，包含各阶段耗时明细，便于快速定位慢请求瓶颈。

### 配置参数

| 参数 | 类型 | 默认值 | 格式与示例 | 描述 |
|------|------|--------|------------|------|
| `slow_log_process_slower_than` | uint64 | `2000` | 正整数，单位微秒；例如 `1000` 表示 1ms 阶段 | 处理阶段时延阈值（微秒）。默认2000μs(2ms)；设为0可禁用；启用后，当处理阶段的耗时（不含跨进程 RPC）超过此阈值时输出 latencySummary。`process` 指处理耗时，等于总耗时减去子 RPC 耗时。支持热更新。 |
| `slow_log_rpc_slower_than` | uint64 | `5000` | 正整数，单位微秒；例如 `2000` 表示 2ms 阶段 | 跨进程 RPC 阶段时延阈值（微秒）。默认5000μs(5ms)；设为0可禁用；启用后，当 RPC 子阶段耗时超过此阈值时输出 latencySummary。支持热更新。 |
| `client_slow_log_process_slower_than` | uint64 | `2000` | 正整数，单位微秒 | Client 侧处理阶段慢日志门限（微秒）。默认2000μs(2ms)；设为0禁用。通过 `DATASYSTEM_CLIENT_CONFIG_PATH` 配置文件或 `UpdateConfig` API 热更新。详见 [Client环境变量](client_env_guide.md)。 |
| `client_slow_log_rpc_slower_than` | uint64 | `5000` | 正整数，单位微秒 | Client 侧 RPC 阶段慢日志门限（微秒）。默认5000μs(5ms)；设为0禁用。通过 `DATASYSTEM_CLIENT_CONFIG_PATH` 配置文件或 `UpdateConfig` API 热更新。详见 [Client环境变量](client_env_guide.md)。 |

> **"process"含义说明**：`process` 阶段代表本进程中的处理与等待时间，而非端到端整链路耗时。
> `client.process.get` 是 Get 总耗时扣除已单独记录的 Get 子阶段后的剩余时间；直读模式还会扣除 route、
> query-and-get、get-data 和 materialize 子阶段。

### latencySummary 格式

latencySummary 字段以预计算字符串形式写入 access log 的 request param 区域，格式示例：

```
latencySummary:{client.process.get:200,client.rpc.get:896,worker.process.get:768,worker.rpc.query_meta:512}
```

各阶段耗时以 `{phase:duration}` 形式输出，多个阶段用 `,` 分隔，`phase` 与 `duration` 之间用 `:` 分隔，整体用 `{}` 包裹，单位为微秒。如有 tick 因 buffer 溢出被丢弃，尾部附加 `tick_dropped:N`；如有 phase 因数组溢出被丢弃，尾部附加 `phase_dropped:N`。

可选 phase 名称如下：

> **命名规则**：`process` = 处理耗时（扣除子 RPC/跨进程调用），非 IP 地址；`rpc` = 跨进程 RPC 耗时；`urma`/`ub` = 用户态 RDMA（Urma-based）相关传输耗时。

| phase 名称 | 含义 |
|------------|------|
| `client.process.get` | 客户端 Get 中未被其他 Get 子阶段覆盖的处理耗时 |
| `client.rpc.get` | 客户端→worker Get RPC 耗时 |
| `client.process.direct_route` | 直读 Get 加载路由并按元数据 owner 分组的耗时 |
| `client.rpc.direct_query_and_get` | 直读 Get 查询元数据阶段的父线程墙钟耗时，包含任务调度/等待、连接与锁等待、RPC 及重试 |
| `client.rpc.direct_get_data` | 直读 Get 数据读取阶段的父线程墙钟耗时，包含连接与锁等待、数据传输、备副本尝试及重试 |
| `client.process.direct_materialize` | 直读结果校验并转换为 SDK Buffer 的耗时 |
| `client.process.set` | 客户端 Set/Put 处理耗时（总耗时扣除子 RPC） |
| `client.rpc.create` | 客户端→worker Create RPC 耗时（Set/Put 的 Create 子阶段） |
| `client.process.memory_copy` | 客户端 Set/Put 数据拷贝到共享内存耗时 |
| `client.rpc.publish` | 客户端→worker Publish RPC 耗时 |
| `client.urma.ub_transfer` | 客户端 UB（Urma-based 用户态 RDMA）数据传输耗时 |
| `client.process.create` | 客户端 Create 处理耗时（总耗时扣除 RPC） |
| `client.process.exist` | 客户端 Exist 处理耗时（总耗时扣除 RPC） |
| `client.rpc.exist` | 客户端→worker Exist RPC 耗时 |
| `worker.process.get` | worker Get 处理耗时（总耗时扣除子 RPC，与 worker IP 无关） |
| `worker.rpc.query_meta` | worker→master QueryMeta RPC 耗时 |
| `worker.rpc.remote_get` | worker→远端 worker RemoteGet RPC 耗时 |
| `worker.urma.urma_total` | worker URMA 协议栈总耗时（含 RDMA 数据搬运、远端响应等） |
| `worker.process.l2cache_read` | worker L2 缓存读取耗时 |
| `worker.process.create` | worker Create 处理耗时 |
| `worker.process.publish` | worker Publish 处理耗时（总耗时扣除子 RPC） |
| `worker.rpc.create_meta` | worker→master CreateMeta RPC 耗时 |
| `worker.rpc.update_meta` | worker→master UpdateMeta RPC 耗时 |
| `worker.process.exist` | worker Exist 处理耗时（总耗时扣除 QueryMeta RPC） |
| `master.process.query_meta` | master QueryMeta 处理耗时 |
| `master.process.create_meta` | master CreateMeta 处理耗时 |
| `master.process.update_meta` | master UpdateMeta 处理耗时 |
| `worker.process.remote_get` | 远端数据 worker RemoteGet 处理耗时 |

### 配置方式

| 场景 | 配置方式 | 示例 |
|------|----------|------|
| Worker 命令行 | `--slow_log_process_slower_than=1000 --slow_log_rpc_slower_than=2000` | 处理>1ms或RPC>2ms时输出 |
| K8s Helm | `values.yaml` 中设置 `slowLogProcessSlowerThan: 1000` | 同上 |
| dscli | `dscli start --slow_log_process_slower_than 1000` | 同上 |
| Embedded Worker | `config.SlowLogProcessSlowerThan(1000).SlowLogRpcSlowerThan(2000)` | 同上 |
| Client 配置文件 | `DATASYSTEM_CLIENT_CONFIG_PATH` 指定文件中设置 `--client_slow_log_process_slower_than=1000` | Client 侧处理>1ms时输出 |
| Client API | `UpdateConfig(R"({"client_slow_log_process_slower_than":"1000"})")` | 运行时动态修改 Client 侧门限 |
| 运行时动态修改 | 修改 `datasystem.config` 中对应参数 | — |

默认值为 `2000`/`5000`（进程内2ms/RPC 5ms），完全向后兼容，零开销。
