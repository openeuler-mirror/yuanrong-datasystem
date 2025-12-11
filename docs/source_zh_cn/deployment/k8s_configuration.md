# openYuanrong datasystem Kubernetes配置项

<!-- TOC -->

- [最小化配置项](#最小化配置项)
- [详细配置项](#详细配置项)
    - [镜像相关配置](#镜像相关配置)
    - [命名空间相关配置](#命名空间相关配置)
    - [资源相关配置](#资源相关配置)
    - [IPC/RPC相关配置](#ipcrpc相关配置)
    - [ETCD相关配置](#etcd相关配置)
    - [Spill相关配置](#spill相关配置)
    - [日志与可观测相关配置](#日志与可观测相关配置)
    - [二级缓存相关配置](#二级缓存相关配置)
    - [多集群相关配置](#多集群相关配置)
    - [元数据相关配置](#元数据相关配置)
    - [可靠性相关配置](#可靠性相关配置)
    - [优雅退出相关配置](#优雅退出相关配置)
    - [性能相关配置](#性能相关配置)
    - [AK/SK相关配置](#aksk相关配置)
    - [Kubernetes亲和性相关配置](#kubernetes亲和性相关配置)
    - [挂载路径相关配置](#挂载路径相关配置)
    - [混合部署相关配置](#混合部署相关配置)
    - [Kubernetes精细化控制配置](#kubernetes精细化控制配置)

<!-- /TOC -->

本文档描述了openYuanrong datasystem Kubernetes快速启动以及详细配置项说明。

## 最小化配置项

本节描述了在Kubernetes部署openYuanrong datasystem的必须配置项，最小化配置项如下表所示：

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| [global.imageRegistry](#镜像相关配置) | string | `""` | 镜像仓库 url |
| [global.images.datasystem](#镜像相关配置) | string | `"yr_datasystem:v0.1"` | 镜像名称和镜像标签 |
| [global.etcd.etcdAddress](#etcd相关配置) | string | `""` | ETCD 服务端访问地址，openYuanrong datasystem的集群管理依赖ETCD |

**样例**：
```yaml
global:
  # 镜像：openyuanrong_datasystem:0.5.0
  imageRegistry: ""

  images:
    datasystem: "openyuanrong-datasystem:0.5.0"
  
  etcd:
    etcdAddress: "127.0.0.1:2379"
```

部署openYuanrong datasystem请参考：[openYuanrong datasystem Kubernetes部署](../deployment/deploy.md#openyuanrong-datasystem-kubernetes部署)。

默认情况下，每个 openYuanrong datasystem DaemonSet 最大可使用 2GB 共享内存空间用于缓存数据，如果需要调整该值，可以通过 [global.resources.datasystemWorker.sharedMemory](#资源相关配置) 调整。

> **注意事项：**
>
> [global.resources.datasystemWorker.sharedMemory](#资源相关配置) 的大小必须小于 [global.resources.requests.memory](#资源相关配置)

```yaml
global:
  # 镜像：openyuanrong_datasystem:0.5.0
  imageRegistry: ""

  images:
    datasystem: "openyuanrong-datasystem:0.5.0"
  
  etcd:
    etcdAddress: "127.0.0.1:2379"

  resources:
    datasystemWorker:
      limits:
        cpu: "3"
        # 该值必须大于等于 requests.memory
        memory: "5Gi"
      requests:
        cpu: "3"
        # 该值必须大于 shreadMemory
        memory: "5Gi"
      # 将共享内存最大使用容量调整为4GB
      sharedMemory: 4096
```

> **注意事项：**
>
> - openYuanrong datasystem依赖ETCD进行集群管理，请确保部署前ETCD服务处于可用状态。
> - openYuanrong datasystem默认占用端口号 `31501`，请确保Kubernetes集群中每个节点的31501端口处于空闲状态，或者通过[global.port.datasystemWorker](#ipcrpc相关配置)更换默认端口号。
> - 请确保Kubernetes集群中每个节点拥有至少3核4G的资源剩余，或者通过[资源相关配置](#资源相关配置)减少默认的资源需求。
> - openYuanrong datasystem Pod内容器默认会挂载以下宿主机目录，请确保容器运行用户具备以下目录的可读可写可访问权限（rwx）：
>   | 宿主机目录 | 配置项 | 作用 |
>   |-----------|--------|------|
>   | /home/sn/datasystem/logs | [global.log.logDir](#日志与可观测相关配置) | 日志持久化目录 |
>   | /home/uds | [global.ipc.udsDir](#ipcrpc相关配置) | Unix Domain Socket目录，用于启用共享内存免拷贝能力 |
>   | /home/sn/datasystem/rocksdb | [global.metadata.rocksdbStoreDir](#元数据相关配置) | 元数据持久化目录 |
>   | /dev/shm | - | 共享内存目录 |

## 详细配置项

### 镜像相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.imageRegistry | string | `""` | 镜像仓库 url |
| global.images.datasystem | string | `"yr_datasystem:v0.1"` | 镜像名称和镜像标签 |

**样例**：
```yaml
global:
  # 镜像：openyuanrong_datasystem:0.5.0
  imageRegistry: ""
  images:
    datasystem: "openyuanrong-datasystem:0.5.0"
```

### 命名空间相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.namespace | string | `"default"` | 命名空间名称 |
| global.autoCreatedNamespace | bool | `false` | 是否自动创建不存在的命名空间 |

### 资源相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.resources.datasystemWorker.limits.cpu | string | `"3"` | openYuanrong datasystem单个DaemonSet占用的最大CPU核数 |
| global.resources.datasystemWorker.limits.memory | string | `"4Gi"` | openYuanrong datasystem单个DaemonSet占用的最大内存量 |
| global.resources.datasystemWorker.requests.cpu | string | `"3"` | openYuanrong datasystem单个DaemonSet初始化时需要的CPU核数，该值的大小必须小于等于 `global.resources.datasystemWorker.limits.cpu` |
| global.resources.datasystemWorker.requests.memory | string | `"3Gi"` | openYuanrong datasystem单个DaemonSet初始化时需要的内存量，该值的大小必须小于等于 `global.resources.datasystemWorker.limits.memory` |
| global.resources.datasystemWorker.maxClientNum | int | `200` | openYuanrong datasystem单个DaemonSet最大可同时连接的客户端数 |
| global.resources.datasystemWorker.sharedMemory | int | `2048` | openYuanrong datasystem单个DaemonSet可使用的共享内存资源大小（以MB为单位），该值的大小必须小于 `global.resources.datasystemWorker.requests.memory` |

### IPC/RPC相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.ipc.ipcThroughSharedMemory | bool | `true` | datasystem-worker共享内存启用开关 |
| global.ipc.udsDir | string | `"/home/uds"` | Unix Domain Socket (UDS) 文件存储目录配置，UDS文件该在该路径下产生，路径最大长度不能超过80个字符。该目录会被自动挂载到宿主机同名目录上，请确保容器具备宿主机同名目录的操作权限 |
| global.port.datasystemWorker | int | `31501` | openYuanrong datasystem DaemonSet占用的主机端口号 |
| global.rpc.enableCurveZmq | bool | `false` | 是否开启服务端组件间认证鉴权功能 |
| global.rpc.curveKeyDir | string | `"/home/sn/datasystem/curve_key_dir"` | 用于查找 ZMQ Curve 密钥文件的目录，启用 ZMQ 认证时必须指定该路径 |
| global.rpc.curveZmqKey.clientPublicKey | string | `""` | 客户端公钥 |
| global.rpc.curveZmqKey.workerPublicKey | string | `""` | datasystem-worker的公钥 |
| global.rpc.curveZmqKey.workerSecretKey | string | `""` | datasystem-worker的私钥 |
| global.rpc.ocWorkerWorkerDirectPort | int | `0` | 对象/KV缓存datasystem-worker之间用于数据传输的TCP通道，0表示禁用该功能；当指定为一个非0值时，datasystem-worker将会建立一条单独用于数据传输的TCP通道，用于加速节点间数据的传输速度，降低数据传输时延 |
| global.rpc.ocWorkerWorkerPoolSize | int | `3` | datasystem-worker间用于数据传输的并行连接数，用于提升节点间数据传输的吞吐量，只有当 `ocWorkerWorkerDirectPort` 指定为非0值时该配置才生效 |
| global.rpc.payloadNocopyThreshold | string | `"104857600"` | datasystem-worker间数据传输时免数据拷贝的阈值（以字节为单位） |
| global.rpc.rpcThreadNum | int | `128` | 配置服务端的RPC线程数，必须为大于0的数 |
| global.rpc.ocThreadNum | int | `64` | 配置服务端用于处理对象/KV缓存的业务线程数 |
| global.rpc.zmqServerIoContext | int | `5` | ZMQ服务端性能优化参数，其数值与系统吞吐量正相关，取值范围：[1, 32] |
| global.rpc.zmqClientIoContext | int | `5` | ZMQ客户端性能优化参数，其数值与系统吞吐量正相关，取值范围：[1, 32] |
| global.rpc.zmqChunkSz | int | `1048576` | 并行负载分块大小配置（以字节为单位） |
| global.rpc.maxRpcSessionNum | int | `2048` | 单个datasystem-worker最大可缓存会话数，取值范围：[512, 10,000] |
| global.rpc.streamIdleTimes | int | `300` | 配置流的空闲时间。默认值为300秒（5分钟） |
| global.rpc.remoteSendThreadNum | int | `8` | 配置服务端用于将元素发送到远程工作线程的线程数量 |

**样例**：
配置一个Unix Domain Socket路径为 "/home/uds"，并使用31501作为openYuanrong datasystem DaemonSet的监听端口号
```yaml
global:
  port:
    datasystemWorker: 31501

  ipc:
    ipcThroughSharedMemory: true
    udsDir: /home/uds
```

### ETCD相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.etcd.etcdAddress | string | `""` | ETCD 服务端访问地址 |
| global.etcd.enableEtcdAuth | bool | `false` | 是否启用 ETCD 认证 |
| global.etcd.etcdCa | string | `""` | CA明文证书，使用Base64编码 |
| global.etcd.etcdCert | string | `""` | 客户端明文证书，使用Base64转码 |
| global.etcd.etcdKey | string | `""` | 客户端私钥。需进行Base64转码，是否加密取决于是否设置了密码短语. |
| global.etcd.etcdCertDir | string | `"/home/sn/datasystem/etcd_cert_dir"` | 已加密 etcd 证书的挂载路径，`global.etcd.enableEtcdAuth` 为 `true` 必须指定此参数 |
| global.etcd.passphraseValue | string | `""` | 密码短语的值，需加密并进行Base64转码。 |
| global.etcd.etcdMetaPoolSize | int | `8` | ETCD元数据异步队列大小，用于将KV接口 `WRITE_BACK_L2_CACHE` 可靠性配置的key的元数据异步写入ETCD持久化 |
| global.etcd.etcdTargetNameOverride | string | `""` | 设置用于SSL主机名校验的ETCD目标名称覆盖。该配置值应与TLS证书的Subject Alternate Names（主题备用名称）中的DNS内容保持一致。 |

**样例**：
```yaml
global:
  # 连接一个无需认证鉴权的ETCD服务
  etcd:
    etcdAddress: "127.0.0.1:2379"
    enableEtcdAuth: false
    etcdTargetNameOverride: ""
    etcdCa: ""
    etcdCert: ""
    etcdKey: ""
    etcdCertDir: ""
    passphraseValue: ""
    etcdMetaPoolSize: 8
```

### Spill相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.spill.spillDirectory | string | `""` | 配置缓存溢出功能的本地磁盘路径，为空表示禁用缓存溢出功能。当配置该路径后，溢出的缓存数据将保存在该路径下的 `datasystem_spill_data` 目录下 |
| global.spill.spillSizeLimit | string | `"0"` | 配置缓存溢出的最大容量（以字节为单位） |
| global.spill.spillThreadNum | int | `8` | 表示溢出数据写文件的最大并行度，线程数越多会消耗越多的CPU和I/O资源 |
| global.spill.spillFileMaxSizeMb | int | `200` | 单个溢出文件的最大大小（以MB为单位）；对于小于此值的对象，会聚合存储于同一个文件中；对于超过此值的对象，将以单个对象单独存为一个文件 |
| global.spill.spillFileOpenLimit | int | `512` | 溢出文件的最大打开文件描述符数量。若已打开文件数超过此值，系统将临时关闭部分文件以防止超出系统最大限制。在系统资源有限的情况下，应适当调低此数值 |
| global.spill.spillEnableReadahead | bool | `true` | 是否启用磁盘预读功能，当预读功能被禁用时，可以缓解KV语义 `Read` 接口偏移读取导致的读放大问题 |
| global.spill.evictionThreadNum | int | `1` | 后台驱逐线程池大小，用于将缓存数据从共享内存驱逐到溢出队列中等到溢出到磁盘 |

- **样例1**：

    Spill目录为 "/opt/spill/yr_datasystem_spill"，大小为10GB。

    ```yaml
    global:
      spill:
        spillDirectory: "/opt/spill/yr_datasystem_spill"
        spillSizeLimit: "10737418240"
        spillThreadNum: 8
        spillFileMaxSizeMb: 200
        spillFileOpenLimit: 512
        spillEnableReadahead: true
        evictionThreadNum: 1
    ```

- **样例2**：

    Spill目录为 "/opt/spill/yr_datasystem_spill"，需要使用宿主机的SSD盘提升Spill性能（宿主机的SSD路径假设为："/data/ssd"），大小同样为10GB。这种场景下需要配合[挂载路径相关配置](#挂载路径相关配置)一起使用。

    ```yaml
    global:
      spill:
        spillDirectory: "/opt/spill/yr_datasystem_spill"
        spillSizeLimit: "10737418240"
        spillThreadNum: 8
        spillFileMaxSizeMb: 200
        spillFileOpenLimit: 512
        spillEnableReadahead: true
        evictionThreadNum: 1

    mount:
      # 宿主机挂载目录，该场景下即为宿主机的SSD路径
    - hostPath: "/data/ssd/yr_datasystem_spill"
      # 容器内挂载目录，需要和 `spillDirectory` 保持一致
      mountPath: "/opt/spill/yr_datasystem_spill"
    ```


### 日志与可观测相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.log.logDir | string | `"/home/sn/datasystem/logs"` | 日志目录，服务端产生的日志将保存到此目录中，该路径会被自动挂载到宿主机同名路径下 |
| global.log.logLevel | int | `0` | 冗余日志级别，0表示不开启冗余日志，取值范围：[0, 3] |
| global.log.logAsync | bool | `true` | 是否开启异步刷新日志功能 |
| global.log.logAsyncQueueSize | int | `65536` | 异步日志的消息队列大小 |
| global.log.logCompress | bool | `true` | 是否开启日志压缩功能，当开启时会将历史日志压缩为gzip格式 |
| global.log.logBufSecs | int | `10` | 最多缓冲这么多秒的日志消息 |
| global.log.logFilename | string | `""` | 日志前缀名，当值为空时前缀名为 `datasystem_worker` |
| global.log.logRetentionDay | int | `0` | 日志保留天数，当该值大于0时，最后修改时间早于 `logRetentionDay` 的日志文件将会被删除；当该值为0时表示禁用该功能 |
| global.log.maxLogFileNum | int | `25` | 最大日志文件个数，当日志文件个数超过该值时，会将最旧的日志文件删除，通过日志滚动机制保证日志文件最大个数小于等于该值 |
| global.log.maxLogSize | int | `400` | 单个日志文件最大大小（以MB为单位） |
| global.observability.logMonitor | bool | `true` | 是否开启接口性能与资源观测日志 |
| global.observability.logMonitorExporter | string | `"harddisk"` | 指定观测日志导出类型，当前仅支持按 `harddisk` 类型导出观测数据，即将观测数据保存到 `logDir` 路径下 |
| global.observability.logMonitorIntervalMs | int | `10000` | 观测日志收集导出的间隔时间（以毫秒为单位） |
| global.log.minLogLevel | int | `0` | 设置记录冗余日志的最低级别，低于这个级别的日志不会被记录 |

**样例**：
```yaml
global:
  # 配置日志路径为 '/home/sn/datasystem/openeuler_datasystem/logs'，日志前缀名为 'openeuler_datasystem'
  log:
    logDir: /home/sn/datasystem/openeuler_datasystem/logs
    logAsyncQueueSize: 65536
    logLevel: 0
    maxLogSize: 400
    maxLogFileNum: 25
    logRetentionDay: 0
    logAsync: true
    logBufSecs: 10
    # 开启日志压缩功能，历史日志将被压缩成gzip格式节约磁盘空间
    logCompress: true
    # 日志前缀名，配置为 'openeuler_datasystem'
    logFilename: "openeuler_datasystem"
```

### 二级缓存相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.l2Cache.l2CacheType | string | `"none"` | 配置二级缓存类型，`none` 表示不配置二级缓存，可选择二级缓存类型：[`obs`, `sfs`] |
| global.l2Cache.l2CacheDeleteThreadNum | int | `32` | 配置二级缓存异步删除线程池大小，增大该值可以提升二级缓存删除并行度，同时也会提升worker的CPU消耗 |
| global.l2Cache.ocIoFromL2CacheNeedMetadata | bool | `true` | 配置L2缓存守护进程的数据读写是否依赖元数据，设置为false，则表示元数据不存储在etcd中 |

::::{tab-set}

:::{tab-item} OBS

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.l2Cache.obs.obsAccessKey | string | `""` | 对象存储服务(OBS) AK/SK认证的访问密钥(Access Key) |
| global.l2Cache.obs.obsSecretKey | string | `""` | 对象存储服务(OBS) AK/SK认证的密钥(Secret Key) |
| global.l2Cache.obs.obsEndpoint | string | `""` | 对象存储服务(OBS) 访问域名 |
| global.l2Cache.obs.obsBucket | string | `""` | 对象存储服务(OBS) 桶的名称 |
| global.l2Cache.obs.obsHttpsEnabled | bool | `false` | 是否启用HTTPS连接对象存储服务（OBS），默认为HTTP |
| global.l2Cache.obs.cloudServiceTokenRotation.enable | bool | `false` | 是否使用CCMS凭据轮换模式访问OBS，默认值为 `false`。若启用此模式，则至少需指定 `iamHostName`（IAM服务地址）、`identityProvider`（身份提供商）、`projectId`（项目ID）及 `regionId`（区域ID）。 |
| global.l2Cache.obs.cloudServiceTokenRotation.iamHostName | string | `""` | 需要获取的IAM token的域名。例如：iam.cn-beijing-4.myhuaweicloud.com |
| global.l2Cache.obs.cloudServiceTokenRotation.identityProvider | string | `""` | 为 openYuanrong datasystem 提供权限的 Provider。例如：csms-datasystem |
| global.l2Cache.obs.cloudServiceTokenRotation.projectId | string | `""` | 对象存储服务(OBS) 的项目ID。例如：fb6a00ff7ae54a5fbb8ff855d0841d00。 |
| global.l2Cache.obs.cloudServiceTokenRotation.regionId | string | `""` | 对象存储服务(OBS) 的区域ID。例如：cn-north-7 |
| global.l2Cache.obs.cloudServiceTokenRotation.enableTokenByAgency | bool | `false` | 是否通过委托代理访问其他账号的 对象存储服务(OBS) ，默认值为 `false`。若设为 `true`，需指定 `tokenAgencyName` 和 `tokenAgencyDomain`。 |
| global.l2Cache.obs.cloudServiceTokenRotation.tokenAgencyDomain | string | `""` | 用于代理访问其他账号的委托名称。例如：obs_access |
| global.l2Cache.obs.cloudServiceTokenRotation.tokenAgencyName | string | `""` | 用于代理访问其他账号的委托方域名。例如：op_svc_cff |

:::

:::{tab-item} SFS

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.l2Cache.sfsTurbo.endpoint | string | `""` | sfs-turbo 的接入点，例如：127.0.0.1 |
| global.l2Cache.sfsTurbo.subPath | string | `""` | 挂载到 datasystem-worker 的 sfs-turbo 子路径。若未指定该参数，默认挂载 sfs-turbo 的根目录 "/" |
| global.l2Cache.sfsTurbo.id | string | `"0"` | 指定 sfs-turbo 的 ID，该 ID 可在 sfs-turbo 页面查看 |
| global.l2Cache.sfsTurbo.projectId | string | `"0"` | 指定sfs-turbo的企业项目ID，该ID可在sfs-turbo页面查看 |
| global.l2Cache.sfsTurbo.capacity | string | `"500Gi"` | 指定 sfs-turbo 的最大可用容量，该值必须要小于 sfs-turbo 的实际可用大小 |

:::

::::

### 多集群相关配置
注: 多集群模式为实验性质特性， 某些场景下可能会有问题，详见：[多集群模式FAQ](../FAQ/clusterFAQ.md)

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.clusterName | string | `"AZ1"` | 可用区的名称 |
| global.crossAz.otherClusterNames | string | `""` | 指定其他可用区的名称，如果需要指定多个可用区通过','进行分隔 |
| global.crossAz.crossAzGetDataFromWorker | bool | `true` | 是否优先尝试从其他可用区的datasystem-worker获取数据。如果为 `false`，则将直接从二级缓存中检索数据 |
| global.crossAz.crossAzGetMetaFromWorker | bool | `false` | 是否从其他可用区的datasystem-worker获取元数据，如果为 `false`，则从本地可用区获取元数据 |

**样例**：
```yaml
global:
  azName: "az1"

  crossAz:
    otherAzNames: "az2,az3,az4"
    crossAzGetDataFromWorker: true
    crossAzGetMetaFromWorker: false
```

### 元数据相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.metadata.enableMetaReplica | bool | `false` | 是否开启元数据多副本功能 |
| global.metadata.rocksdbStoreDir | string | `"/home/sn/datasystem/rocksdb"` | 配置元数据持久化目录，元数据通过RocksDB持久化在磁盘中 |
| global.metadata.rocksdbBackgroundThreads | int | `16` | RocksDB的后台线程数，用于元数据的刷盘和压缩 |
| global.metadata.rocksdbMaxOpenFile | int | `128` | RocksDB可使用的最大打开文件个数 |
| global.metadata.rocksdbWriteMode | string | `async` | 配置元数据写入RocksDB的方式，支持不写、同步和异步写入，默认值为`async`。可选值包括：'none'（不写）、'sync'（同步）、'async'（异步） |


### 可靠性相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.reliability.clientReconnectWaitS | int | `5` | 客户端断链重连最大等待时间（单位为秒） |
| global.reliability.clientDeadTimeoutS | int | `120` | 客户端存活检测最大时间间隔（单位为秒） |
| global.reliability.heartbeatIntervalMs | int | `1000` | 服务端与ETCD的心跳间隔时间（单位为毫秒） |
| global.reliability.nodeTimeoutS | int | `60` | 服务端节点超时最大时间间隔（单位为秒） |
| global.reliability.nodeDeadTimeoutS | int | `300` | 服务端节点存活检测最大时间间隔（单位为秒），当节点超过存活检测最大时间间隔后仍未恢复心跳，会被标记为死亡节点，该值必须大于 `nodeTimeoutS` |
| global.reliability.enableReconciliation | bool | `true` | 当节点重启时是否启用对账功能 |
| global.reliability.enableHashRingSelfHealing | bool | `false` | 是否启用哈希环自愈功能，如果该值为 `true`，当哈希环状态异常时会启用自愈修复哈希环 |
| global.reliability.livenessProbeTimeoutS | int | `150` | Kubernetes 存活探针超时时间配置 |
| global.reliability.addNodeWaitTimeS | int | `60` | 新节点加入哈希环的等待超时时间 |
| global.reliability.autoDelDeadNode | bool | `true` | 是否启用死亡节点自动清理功能，当该值为 `true` 时，会将死亡节点剔除出集群管理，并触发被动缩容 |
| global.reliability.enableDistributedMaster | bool | `true` | 是否启用分布式主节点，默认值为true |
| global.reliability.enableStreamDataVerification | bool | `false` | 是否验证生产者数据乱序，默认值为false |

### 优雅退出相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.gracefulShutdown.scaleInTaint | string | `"datasystem/offline=true:NoExecute"` | 识别优雅退出的污点，格式为 `key=value:effect`。当节点打上与 `scaleInTaint` 相匹配的污点时，该节点上的 datasystem-worker 会触发主动缩容优雅退出 |
| global.gracefulShutdown.enableLosslessDataExitMode | bool | `false` | 是否启用无损数据退出模式，当该值为 `true` 时，在节点退出时则会以优雅退出的方式，迁移数据和元数据，保证数据和元数据不丢失 |
| global.gracefulShutdown.checkAsyncQueueEmptyTimeS | int | `15` | datasystem-worker检测异步队列为空的时间，单位为秒 |
| global.gracefulShutdown.dataMigrateRateLimitMb | int | `40` | 配置优雅退出数据迁移的流控（以MB/s为单位） |
| global.gracefulShutdown.livenessProbeTerminationGracePeriodSeconds | int | `0` | 优雅退出的最大处理时间，0表示无限时间；当该值大于0时，如果优雅退出时间超过该值，Kubernetes会强制清除datasystem-worekr Pod |

### 性能相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.performance.enableHugeTlb | bool | `false` | 是否开启共享内存大页内存功能，它可以提高内存访问，减少页表的开销 |
| global.performance.enableFallocate | bool | `true` | 由于Kubernetes(k8s)的资源计算策略，共享内存有时会被计算两次，这可能会导致客户端OOM崩溃。为了解决这个问题，我们使用了fallocate来链接客户端和工作节点的共享内存，从而纠正内存计算错误。缺省情况下，fallocate是使能的。启用fallocate会降低内存分配的效率 |
| global.performance.sharedMemoryPopulate | bool | `false` | 是否开启共享内存预热功能，启用该功能可以加速应用运行期间的共享内存拷贝速度，但是在datasystem_worker进程启动时也会由于预热导致启动速度变慢（取决于sharedMemory的配置）。如果开启该功能，'arenaPerTenant'必须设置为1，'enableFallocate'必须设置为false |
| global.performance.enableThp | bool | `false` | 是否启用透明大页（Transparent Huge Page,THP）功能，启用透明大页可以提高性能，减少页表开销，但也可能导致 Pod 内存使用增加 |
| global.performance.arenaPerTenant | int | `16` | 每个租户的共享内存分配器数量。多分配器可以提高第一次分配共享内存的性能，但每个分配器会多使用一个fd，导致fd资源使用量上升。取值范围：[1, 32] |
| global.performance.memoryReclamationTimeSecond | int | `600` | 释放后的内存回收时间，未回收的内存可以提供给下次分配复用，提升分配效率 |
| global.performance.asyncDelete | bool | `false` | 是否异步删除对象，如果设置为 `true` 时，删除对象数据是个异步的过程，客户端不需要等待所有数据副本删除完成即可返回 |
| global.performance.enableP2pTransfer | bool | `false` | 是否开启异构对象传输协议支持点对点传输 |
| global.performance.enableWorkerWorkerBatchGet | bool | `false` | 是否开启worker到worker的对象数据批量获取，默认值为false |
| global.performance.ocShmTransferThresholdKB  | int | `500` | 在客户端和worker之间通过共享内存传输对象数据的阈值，单位为KB |
| global.performance.enableUrma | bool | `false` | 是否开启Urma以实现对象worker之间的数据传输 |
| global.performance.urmaMode | string | `UB` | 是否开用Urma通过IB或UB运行，默认情况下，UB将与URMA一起运行 |
| global.performance.urmaPollSize | int | `8` | 一次可轮询的完整记录数量，该设备最多可轮询16条记录 |
| global.performance.urmaRegisterWholeArena | bool | `true` | 是否在初始化时将整个arena注册为一个段，如果设置为`false`，将每个对象分别注册为一个段 |
| global.performance.urmaConnectionSize | int | `16` | jfs和jfr对的数量 |
| global.performance.urmaEventMode | bool | `false` | 是否使用中断模式轮询完成事件 |
| global.performance.sharedDiskDirectory | string | `""` | 磁盘缓存数据存放目录，默认为空，表示未启用磁盘缓存 |
| global.performance.sharedDiskSize | int | `0` | 共享磁盘的大小上限，单位为MB，默认为0，表示未启用磁盘缓存 |
| global.performance.sharedDiskArenaPerTenant  | int | `8` | 每个租户的磁盘缓存区域数量，多个区域可以提高首次共享磁盘分配的性能，但每个区域会多占用一个文件描述符（fd）。取值范围：[0, 32] |
| global.performance.enableRdma | bool | `false` | 是否为OC工作节点之间的数据传输启用RDMA |
| global.performance.rdmaRegisterWholeArena | bool | `true` | 是否在初始化时将整个arena注册为一个段，false时将每个对象分别注册为一个段 |
| global.performance.ocWorkerAggregateMergeSize | int | `2097152` | worker响应的目标批量大小，默认值为2MB |
| global.performance.ocWorkerAggregateSingleMax | int | `65536` | 批量处理worker最大单个项目大小，默认为64KB |
| global.performance.ocWorkerWorkerParallelMin | int | `100` | 并行工作线程批处理响应的最小数据计数，默认值为100 |
| global.performance.ocWorkerWorkerParallelNums | int | `16` | worker并行数量，0表示无限制 |

### AK/SK相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.akSk.systemAccessKey | string | `""` | 系统访问密钥 |
| global.akSk.systemSecretKey | string | `""` | 系统密钥 |
| global.akSk.systemDataKey | string | `""` | 系统数据秘钥 |
| global.akSk.tenantAccessKey | string | `""` | 租户访问密钥 |
| global.akSk.tenantSecretKey | string | `""` | 租户密钥 |
| global.akSk.requestExpireTimeS | int | `300` | 请求过期时间，单位为秒，最大值为300 |

### Kubernetes亲和性相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.affinity.nodeSelector | object | `{}` | 亲和标签，控制Pod调度到符合特定条件的Node节点 |
| global.affinity.nodeAffinity | object | `{}` | 亲和标签，精确控制Pod调度到符合特定条件的Node节点，相比 `nodeSelector` 支持更强大的高级调度策略 |
| global.affinity.tolerations | list | `[]` | 污点容忍标签，Pod被动接受节点的排斥，不会被驱逐 |

### 挂载路径相关配置

挂载路径相关配置一般与[Spill相关配置](#spill相关配置)相配合使用。

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.mount.hostPath | string | `""` | 主机路径 |
| global.mount.mountPath | string | `""` | 容器内挂载路径 |

**样例**：
```yaml
global:
  mount:
    # Spill挂载路径1
    - hostPath: "/data/ssd1/yr_datasystem_spill"
      mountPath: "/opt/spill/yr_datasystem_spill1"
    # Spill挂载路径2
    - hostPath: "/data/ssd2/yr_datasystem_spill"
      mountPath: "/opt/spill/yr_datasystem_spill2"
```


### 混合部署相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.multiSpec | list | `[]` | 混合部署支持，默认情况下所有openYuanrong datasystem的DaemonSet Pod都是相同规格的，如果需要配置不同规格的Pod，需要使用该配置，该配置需要与节点亲和性标签一起配合使用 |

**样例**：
```yaml
global:
  # ...
  multiSpec:
  # 在亲和性标签为 `mid` 的节点部署第一种规格的Pod
  - name: "ds-worker-mid"
    affinityLabel: "mid"
    resources:
      limits:
        cpu: "3"
        memory: "4Gi"
      requests:
        cpu: "3"
        memory: "4Gi"
    workerResources:
      sharedMemory: 1024
  # 在亲和性标签为 `small` 的节点部署第一种规格的Pod
  - name: "ds-worker-small"
    affinityLabel: "small"
    resources:
      limits:
        cpu: "2"
        memory: "3Gi"
      requests:
        cpu: "1"
        memory: "2Gi"
    workerResources:
      sharedMemory: 1024
  # 在亲和性标签为 `big` 的节点部署第一种规格的Pod
  - name: "ds-worker-big"
    affinityLabel: "big"
    resources:
      limits:
        cpu: "5"
        memory: "6Gi"
      requests:
        cpu: "4"
        memory: "5Gi"
    workerResources:
      sharedMemory: 2048
```


### Kubernetes精细化控制配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.annotations | object | `{}` | Kubernetes 元注解 |
| global.enableNonPreemptive | bool | `false` | 配置priorityClass。如果该值为false，则默认priorityClass为system-cluster-key。如果为true，则会创建一个preemptionPolicy Never的priorityClass |
| global.fsGid | string | `"1002"` | fsGroup配置。容器的所有进程也是附加组ID的一部分 |
| global.rollingUpdateTimeoutS | int | `1800` | 滚动升级的最大持续时间，默认值为1800秒 |
| global.security.scEncryptSecretKey | string | `1800` | 流缓存的加密密钥，密钥长度最多为1024字节，解密后必须为32字节 |