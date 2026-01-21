# openYuanrong datasystem日志

## 日志类别

openYuanrong datasystem的日志分为四类：

1. 运行日志: 记录客户端、服务端运行时的日志信息，包括DEBUG、INFO、WARNING、ERROR级别；
2. 访问日志：记录每一个访问客户端/服务端请求，而且每个请求仅打印一条日志，可用于定界定位上游应用是否访问客户端/服务端；
3. 请求第三方日志：记录datasystem_worker每一个访问第三方组件日志的请求，而且每个请求仅打印一条日志，可用于定界定位openYuanrong datasystem是否有成功访问该外部组件；
4. 资源日志：定时输出datasystem_worker运行时关键资源信息，包括共享内存使用量、Spill磁盘使用量、 threadpool及队列使用量等等信息。

openYuanrong datasystem 不同模块日志分类如下表所示：

| 序号 | 模块 | 日志路径及文件名 | 含义、用途 |
|---|------|-----------------------------------------|-----------------------------------------|
| 1 | datasystem_worker | /path/yr_datasystem/logs/datasystem_worker.INFO.log | worker运行日志 |
| 2 | datasystem_worker | /path/yr_datasystem/logs/access.log | 访问worker POSIX接口的日志 |
| 3 | datasystem_worker | /path/yr_datasystem/logs/resource.log | Worker资源使用日志，默认关闭，通过log_monitor开关控制是否开启 |
| 4 | datasystem_worker | /path/yr_datasystem/logs/requestout.log | 访问ETCD接口日志 |
| 5 | datasystem_worker | /path/yr_datasystem/logs/container.log | 容器运行日志，管理和监控worker进程的生命周期 |
| 6 | Client | /path/client/ds_client_\<pid\>.log | SDK运行日志 |
| 7 | Client | /path/client/ds_client_access_\<pid\>.log | SDK接口访问日志 |

## 日志格式

### 格式介绍

| 序号 | 日志 | 日志格式 |
|-----|-----|------------------------|
| 1 | 运行日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| message |
| 2 | 访问日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| status_code \| action \| cost \| data size \| request param\| response param
| 3 | 访问第三方日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| status_code \| action \| cost \| data size \| request param\| response param
| 4 | 资源日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| shm_info \| spill_disk_info \| client nums \| object nums \| object total datasize \| WorkerOcService threadpool \| WorkerWorkerOcService threadpool \| MasterWorkerOcService threadpool \| MasterOcService threadpool \| write ETCD queue \| ETCDrequest success rate \| OBSrequest success rate \| Master AsyncTask threadpool \|Cache Hit Info \|
| 5 | 容器运行日志 | Time \| level \| filename \| pod_name \| pid:tid \| trace_id \| cluster_name \| message |

### 日志字段

| 字段 | 长度 Byte | 描述
|-------|---------------|-------|
| time | 26 | ISO8601格式的时间戳，示例: 2023-06-02T14:58:32.081156
| level | 1 | 日志级别 (debug, info, warn等)
| filename | 128 | 输出该条日志的函数所在文件及行号，最大长度128Byte，超出则截断。示例：oc_metadata_manager.cpp:733 |
| pod_name | 128 | 输出当前worker所属的POD名称，超出长度则截断。示例：ds-worker-hs5qm |
| pid:tid | 11 | 该日志所属的进程ID和线程ID。进程号最大值为32757，该字段最大长度11.示例：9:177 |
| trace_id | 36 | 请求的trace_id。 |
| cluster_name | 128 | 输出日志的组件名，最大长度为128，超出长度则截断。示例：ds-worker。 |
| Message | 1024 | 自定义消息内容 |
| status_code | 5 | 该请求的状态，不同消息类型状态值不一样。SDK/datasystem_worker 访问日志，0表示成功，其他表示失败。 |
| action | 64 | 表示该请求所访问的接口名称。约定前缀：SDK接口：DS_KV_CLIENT、DS_OBJECT_CLIENT，Worker接口：DS_OBJECT_POSIX，ETCD：DS_ETCD，HTTP请求：POST {url path} |
| cost | 16 | 记录该请求所花费的时间。单位：us |
| datasize | 16 | 记录Publish请求接收到的Payload大小。 |
| request param | 2560 | 记录该请求的关键请求参数，最大长度2048。请参考“关键请求参数”表格。 |
| response param | 1024 | 记录该请求的响应信息。最大长度为1024 Byte，超出则截断。 |
| shm_info | 47 | 记录共享内存使用信息，单位为Byte，按照1T限制大小，每个长度 13 Byte，格式为：memoryUsage/physicalMemoryUsage/totalLimit/rate<br>1) memoryUsage	已分配的内存大小，是已缓存的对象大小总和。注意：由于系统中使用jemalloc管理内存，实际分配的内存会按size class对齐，因此memoryUsage通常比实际对象大小的总和高一些，包含一定的内存对齐开销。<br>2) physicalMemoryUsage	已分配的物理内存大小。<br>3) totalLimit	共享内存总大小。<br>4) Rate	共享内存使用率，memoryUsage/totalLimit, 保留3位有效数字，单位: %. | 
| spill_disk_info | 47 | 记录Spill磁盘使用信息。单位为Byte，按照1T限制大小，每个长度 13 Byte，格式为：spaceUsage/physicalSpaceUsage/totalLimit/rate<br>1) spaceUsage	已使用的磁盘大小，是已Spill的对象大小总和。<br>2) physicalSpaceUsage	已使用的物理磁盘大小。<br>3) totalLimit	Spill磁盘总大小。<br>4) Rate	Spill磁盘使用率，spaceUsage /totalLimit, 保留3位有效数字，单位: %. | 
| client nums | 5 | 记录已和worker成功建立连接的Client数。最大值为10000. | 
| object nums | 9 | 记录worker已缓存对象数。按照1亿对象限制数量。| 
| object total datasize | 13 | 记录worker已缓存对象的大小。按照1T限制大小，长度 13 Byte | 
| WorkerOcService threadpool | 21 | WorkerOcService threadpool使用信息，线程数限制最大128；格式为：idleNum/currentTotalNum/MaxThreadNum/waitingTaskNum/rate<br>1) idleNum	空闲线程数；<br>2) currentTotalNum	当前正在运行任务的线程数；<br>3) MaxThreadNum	 threadpool最大可申请的线程数；<br>4) waitingTaskNum	正在等待的任务数。<br>5) rate	线程利用率，currentTotalNum/ MaxThreadNum,单位：%，保留3位有效数字。| 
| WorkerWorkerOcService threadpool | 21 | threadpool使用信息 | 
| MasterWorkerOcService threadpool | 21 |  threadpool使用信息 | 
| MasterOcService threadpool | 21 | threadpool使用信息 |
| write ETCD queue | 15 | 队列使用信息 | 
| ETCDrequest success rate | 6 | 请求使用率，单位 %,保留3位有效数字 | 
| OBSrequest success rate | 6 | 请求使用率，单位 %,保留3位有效数字 | 
| Master AsyncTask threadpool | 21 |  threadpool使用信息，格式为：idleNum/currentTotalNum/MaxThreadNum/waitingTaskNum/rate |
|Cache Hit Info | 9 | 缓存命中统计,格式为:memHitNum/diskHitNum/l2HitNum/remoteHitNum/missNum,<br>1) memHitNum	本地内存命中次数。<br>2) diskHitNum	本地磁盘命中次数. <br>3) l2HitNum	二级缓存命中次数。<br>4) remoteHitNum 远端worker命中次数。<br>5) missNum 未命中次数。

#### SDK与worker访问日志关键请求参数

|关键请求参数 | 长度 Byte | 描述 |
|------------|-------------|---------|
| Object_key | 255 | 对象的ID。长度：255Byte； |
| object_keys | 1024 | 多个对象KEY。单个Item长度：255Byte，全部长度：1KB；超出的对象KEY，只显示出总数。示例： {“object_keys”:[”id1xx”,”id2xx”,”id3xx”,”id4xx”, total: 100]} |
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

#### 访问第三方日志关键请求参数

|外部组件 | 请求类型 | 关键请求参数 | 描述 |
|------|--------|----------|------|
| ETCD | GRPC | key | 将Key字段获取并打印。|
