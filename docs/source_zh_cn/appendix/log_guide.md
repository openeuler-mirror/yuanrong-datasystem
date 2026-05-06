# openYuanrong datasystem 日志

## 日志类别

openYuanrong datasystem 的日志分为以下类型：

1. **运行日志**：记录客户端、服务端运行时的日志信息，包括 INFO、WARNING、ERROR、FATAL；更细粒度调试可通过 `VLOG` 与 gflags（如 `--v`）控制。
2. **访问日志**：记录每一次访问客户端/服务端的请求，每个请求一条日志，用于定界上游是否访问客户端/服务端（需开启 `log_monitor`）。
3. **请求第三方日志（request_out）**：记录 Worker 访问第三方组件的请求，每个请求一条日志，可用于定界 openYuanrong datasystem 是否成功访问该外部组件；当前实现中 **主要接入 ETCD gRPC**（需开启 `log_monitor`）。
4. **资源日志**：定时输出 Worker 运行时关键资源信息，包括共享内存、Spill 磁盘、线程池、队列、流缓存相关统计等（需 `log_monitor` 且 `log_monitor_exporter=harddisk`）。
5. **流缓存指标日志（sc_metrics）**：流缓存运行数据（需开启 `log_monitor`）。
6. **容器/进程相关日志**：容器运行日志，管理和监控worker进程的生命周期。

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
| 7 | Client | `/path/client/ds_client_{pid}.INFO.log`（及 `.WARNING`、`.ERROR` 等；`{pid}` 为进程号） | SDK 运行日志；基名可由启动参数与环境变量 `DATASYSTEM_CLIENT_LOG_NAME` 覆盖 |
| 8 | Client | `/path/client/ds_client_access_{pid}.log` | SDK 接口访问日志；基名可由 `DATASYSTEM_CLIENT_ACCESS_LOG_NAME` 覆盖 |

---

## 日志格式

### 格式总览

| 序号 | 日志 | 日志格式 |
|-----|-----|------------------------|
| 1 | 运行日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| message |
| 2 | 访问日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| status_code \| action \| cost \| data size \| request param\| response param |
| 3 | 访问第三方日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| status_code \| action \| cost \| data size \| request param\| response param |
| 4 | 资源日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| shm info \| spill disk info \| client nums \| object nums \| object total datasize \| WorkerOcService threadpool \| WorkerWorkerOcService threadpool \| MasterWorkerOcService threadpool \| MasterOcService threadpool \| write ETCD queue \| ETCDrequest success rate \| OBSrequest success rate \| Master AsyncTask threadpool \| stream nums \| ClientWorkerSCService threadpool \| WorkerWorkerSCService threadpool \| MasterWorkerSCService threadpool \| MasterSCService threadpool \| remote stream push success rate \| shared disk info \| scLocalCache info \| Cache Hit Info |
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
| shm info | 47 | 记录共享内存使用信息，单位为Byte，按照1T限制大小，每个长度 13 Byte，格式为：memoryUsage/physicalMemoryUsage/totalLimit/rate<br>1) memoryUsage	已分配的内存大小，是已缓存的对象大小总和。注意：由于系统中使用jemalloc管理内存，实际分配的内存会按size class对齐，因此memoryUsage通常比实际对象大小的总和高一些，包含一定的内存对齐开销。<br>2) physicalMemoryUsage	已分配的物理内存大小。<br>3) totalLimit	共享内存总大小。<br>4) Rate	共享内存使用率，memoryUsage/totalLimit, 保留3位有效数字，单位: % | 
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
| sc_metric | 1024 | 流缓存运行数据(sc_stream_metric)。worker上一个stream的的流缓存数据，格式：streamName ["exit"]/numLocalProd/numRemoteProd/numLocalCon/numRemoteCon/sharedMemUsed/localMemUsed/numEleSent/numEleRecv/numEleAck/numSendReq/numRecvReq/numPagesCreated/numPagesReleased/numPagesInUse/numPagesCached/numBigPagesCreated/numBigPagesReleased/numLocalProdBlocked/numRemoteProdBlocked/numRemoteConBlocking/retainData/streamState/numProdMaster/numConMaster<br>1) streamName ["exit"] stream名字，带有" exit"表示stream正要关闭<br>2) numLocalProd 本地producer数量<br>3) numRemoteProd - Number of remote workers with atleast one producewill be 0, if no local consumers)<br>4) numLocalCon 本地consumer数量<br>5) numRemoteCon 远端consumer数量<br>6) sharedMemUsed stream使用共享内存大小，单位: Byte<br>7) localMemUsed stream使用本地内存大小，单位: Byte<br>8) numEleSent - Total number of elements produced by all local producers<br>9) numEleRecv - Total number of elements received by all local consumers (value will be 0, if no local consumers)<br>10) numEleAck element acked数量<br>11) numSendReq client调用producer.send()次数<br>12) numRecvReq client调用consumer.receive()次数<br>13) numPagesCreated page创建次数<br>14) numPagesReleased page释放次数<br>15) numPagesInUse page in use数量<br>16) numPagesCached page cached数量<br>17) numBigPagesCreated big element page创建次数<br>18) numBigPagesReleased big element page释放次数<br>19) numLocalProdBlocked 本地producer blocked数量<br>20) numRemoteProdBlocked 远端producer blocked数量<br>21) numRemoteConBlocking 远端consumer blocking数量<br>22) retainData retain data state<br>23) streamState stream state<br>24) numProdMaster master上producer数量<br>25) numConMaster master上consumer数量<br>- 如果worker不是stream的master，24-25会没有数据。如果worker只有master数据，2-23会没有据 |

### SDK 与 Worker 访问日志关键请求参数

| 关键请求参数 | 长度 Byte | 描述 |
|------------|-------------|---------|
| Object_key | 255 | 对象的ID。长度：255Byte； |
| object_keys | 1024 | 多个对象 KEY。单个 Item 最长 255 Byte，展示总长上限约 1KB；超出时仅显示总数等缩写。示例 JSON：`{"object_keys":["id1xx","id2xx","id3xx","id4xx"],"total":100}` |
| Nested_keys | 1024 | 嵌套引用的对象KEY。单个Item长度：255Byte，全部长度：1KB； |
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

---

## 日志采样

大流量场景下，可通过 `log_rate_limit` 参数控制每秒采样请求数，避免高频请求日志产生过多磁盘 I/O。

### 工作原理

- 采样粒度是“请求（traceId）”，不是“单条日志”
- `log_rate_limit=N` 表示每秒最多采样 `N` 个新请求（trace）
- 对采样到的请求：链路日志完整打印（INFO/WARNING/ERROR/FATAL）
- 对未采样请求：仅保留 ERROR/FATAL，非ERROR日志丢弃
- 仅 SDK 请求 trace 参与采样；后台线程日志即使带 traceId 也不参与采样
- 采样决策会随 RPC 元数据传播，跨 client/worker 保持同一 trace 的一致采样结果
- 不带 traceId 的后台日志（例如 worker/client 后台线程日志）不参与该采样逻辑

### 配置方式

| 场景 | 配置方式 | 示例 |
|------|----------|------|
| Worker 命令行 | `--log_rate_limit=N` 启动参数 | `./datasystem_worker --log_rate_limit=20` |
| Embedded Worker | `EmbeddedConfig::LogRateLimit(N)` | `config.LogRateLimit(20)` |
| Standalone Client | 环境变量 `DATASYSTEM_LOG_RATE_LIMIT` | `export DATASYSTEM_LOG_RATE_LIMIT=20` |
| 运行时动态修改 | 修改 `datasystem.config` 中 `log_rate_limit` 值 | — |

默认值为 `0`（不限速），完全向后兼容。
