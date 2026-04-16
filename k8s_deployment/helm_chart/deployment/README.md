# 部署操作文档

## 环境准备

1) 需要在Linux系统中安装好Docker。（[docker安装指导](https://docs.docker.com/engine/install/)）.

2) 本地下载基础的Linux docker镜像。需要是下面的系统：

| 系统        |版本|
|-----------|---|
| openeuler |24.03 SP3|

基础镜像由客户提供，下面仅以指定镜像为例：

```shell
已有镜像镜像：docker.io/library/datasystem-64k-vfe:latest
```

## 构建镜像

通过`docker build`命令执行构建。

构建示例如下：

```shell
# 在k8s/docker/dockerfile目录下构建为例：
docker build --build-arg no_proxy \
          --build-arg DS_BASE_IMAGE=docker.io/library/datasystem-64k-vfe:latest \
          --build-arg USER_NAME=jd \
          --build-arg UID=1002 \
          -t datasystem:0.7.0 \
          -f datasystem-kvcache.Dockerfile .
```

查看构建结果：
```shell
# 通过下方命令可以看到已构建的镜像：datasystem:0.7.0
docker images
```

镜像打包：
```shell
# 如需将构建的镜像打包，可执行下方命令：
docker save -o datasystem_0.7.0.tar datasystem:0.7.0
```
在执行命令目录下输出 `datasystem_0.7.0.tar` docker镜像包。

## 加载镜像

通过 `docker load` 命令可以加载镜像到docker中。

```shell
docker load -i datasystem_0.7.0.tar
```

### 集群部署

kvcache通过 [k8s/helm_chart/deployment/values.yaml]文件进行集群相关配置，其中必配项如下：

```yaml
global:
  # 镜像名字和镜像tag，<VERSION>需要替换为对应的版本号
  images:
    datasystem: "kvcache:0.7.0"
  # ETCD相关配置
  etcd:
    etcdAddress: "127.0.0.1:2379"
```

配置完成后，通过 helm 命令即可轻松完成部署，命令如下：

```bash
helm install kvcache k8s/helm_chart/deployment
# NAME: kvcache
# LAST DEPLOYED: Tue Apr 22 14:31:34 2025
# NAMESPACE: default
# STATUS: deployed
# REVISION: 1
# TEST SUITE: None
```

部署后可以通过 kubectl 命令查看集群状态：

```bash
kubectl get pods -o wide
```

当Pod处于Running状态时说明Pod处于就绪状态，可以正常对外提供服务，部署成功。

### 快速验证

kvcache会以deployment按照replicaNum的数量在相应数量的节点部署一个 `kvcache-worker` Pod，默认监听 `<PODIP>:31501`，可通过如下 Python 脚本快速验证：

```python
from yr.datasystem.ds_client import DsClient

client = DsClient("127.0.0.1", 31501)
client.init()
```

当脚本执行未发生异常时说明kvcache的客户端能正常连接上当前节点的 `kvcache-worker` Pod，部署成功。

### 集群卸载

集群通过 helm 命令即可轻松完成卸载，卸载命令如下：

```bash
helm uninstall kvcache
```

执行完命令后通过 kubectl 命令再次查看集群状态：

```bash
kubectl get pods -o wide
```

当kvcache所有的kvcache-worker Pod都退出时说明集群卸载成功。

## 部署参数

### 最小化配置项

本节描述了在Kubernetes部署kvcache的必须配置项，最小化配置项如下表所示：

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| [global.images.datasystem] | string | `"kvcache:0.7.0"` | 镜像名称和镜像标签 |
| [global.etcd.etcdAddress] | string | `""` | ETCD 服务端访问地址，kvcache的集群管理依赖ETCD |

**样例**：
```yaml
global:
  images:
    datasystem: "kvcache:0.7.0"

  etcd:
    etcdAddress: "127.0.0.1:2379"
```

### k8s相关功能化配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.namespace | string | `"default"` | 命名空间名称(非default命名空间需确保k8s集群已存在所配置的namespace) |
| global.replicaNum | int | `2` | deployment副本数 |
| global.fsGid | int | `1002` | 容器运行用户groupID（需与构建镜像时传入的UID保持一致） |
| global.terminationGracePeriodSeconds | int | `1800` | k8s pod优雅退出时间（以秒为单位） |
| global.healthCheckPath | string | `"/home/jd/kvcache/health/healthy"` | k8s pod健康检查路径 |
| global.readyCheckPath | string | `"/home/jd/kvcache/health/ready"` | k8s pod就绪检查路径 |
| global.livenessProbePath | string | `"/home/jd/kvcache/health/liveness"` | k8s pod存活检查路径 |
| global.statusFilePath | string | `"/home/jd/kvcache/logs/worker/worker-status"` | 数据系统worker状态记录文件 |
| global.reliability.livenessProbeTimeoutS | int | `150` | k8s pod存活探针超时时间（秒） |
| global.dockerConfigPath | string | `"/home/jd/kvcache/docker"` | 容器运行配置文件路径 |
| global.hostNetwork | bool | `true` | 是否使用主机网络 |
| global.dnsPolicy | string | `ClusterFirstWithHostNet` | k8s pod DNS策略 |

### 资源相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.resources.datasystemWorker.limits.cpu | string | `"3"` | kvcache单个deployment占用的最大CPU核数 |
| global.resources.datasystemWorker.limits.memory | string | `"4Gi"` | kvcache单个deployment占用的最大内存量 |
| global.resources.datasystemWorker.limits.urmaVfe | int | `1` | kvcache单个deployment占用的最大urma vfe数（仅在容器网络模式下需要配置） |
| global.resources.datasystemWorker.requests.cpu | string | `"3"` | kvcache单个deployment初始化时需要的CPU核数，该值的大小必须小于等于 `global.resources.datasystemWorker.limits.cpu` |
| global.resources.datasystemWorker.requests.memory | string | `"3Gi"` | kvcache单个deployment初始化时需要的内存量，该值的大小必须小于等于 `global.resources.datasystemWorker.limits.memory` |
| global.resources.datasystemWorker.requests.urmaVfe | int | `1` | kvcache单个deployment初始化时需要的urma vfe数（仅在容器网络模式下需要配置） |
| global.resources.datasystemWorker.sharedMemory | int | `2048` | kvcache单个deployment可使用的共享内存资源大小（以MB为单位），该值的大小必须小于 `global.resources.datasystemWorker.requests.memory` |

**样例**：
```yaml
# 如果是容器网络时需要在worke_deployment.yaml中resources字段下额外配置unifiedbus.com/urma.vfe
# 当前主机网络下无上述字段
resources:
  limits:
    cpu: {{ $.Values.global.resources.datasystemWorker.limits.cpu }}
    memory: {{ $.Values.global.resources.datasystemWorker.limits.memory }}
    unifiedbus.com/urma.vfe: {{ $.Values.global.resources.datasystemWorker.limits.urmaVfe }}
  requests:
    cpu: {{ $.Values.global.resources.datasystemWorker.requests.cpu }}
    memory: {{ $.Values.global.resources.datasystemWorker.requests.memory }}
    unifiedbus.com/urma.vfe: {{ $.Values.global.resources.datasystemWorker.requests.urmaVfe }}
```

### IPC/RPC相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.ipc.ipcThroughSharedMemory | bool | `false` | datasystem-worker共享内存启用开关 |
| global.ipc.udsDir | string | `"/home/uds"` | Unix Domain Socket (UDS) 文件存储目录。UDS文件在该路径下产生，路径最大长度不能超过80个字符。该目录将挂载到宿主机同名目录上，请确保容器具备宿主机同名目录的操作权限 |
| global.port.datasystemWorker | int | `31501` | kvcache deployment占用的主机端口号 |
| global.rpc.rpcThreadNum | int | `128` | 配置服务端的RPC线程数，必须为大于0的数 |
| global.rpc.ocThreadNum | int | `64` | 配置服务端用于处理对象/KV缓存的业务线程数 |

**样例**：
配置一个Unix Domain Socket路径为 "/home/uds"，并使用31501作为kvcache deployment的监听端口号
```yaml
global:
  port:
    datasystemWorker: 31501

  ipc:
    ipcThroughSharedMemory: false
    udsDir: /home/uds
```

### 日志与可观测相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.log.logDir | string | `"/home/jd/kvcache/logs"` | 日志目录，服务端产生的日志将保存到此目录中 |
| global.log.logMonitor | bool | `true` | 是否开启接口性能与资源观测日志 |
| global.log.minLogLevel | int | `0` | 设置记录冗余日志的最低级别，低于这个级别的日志不会被记录 |

**样例**：
```yaml
global:
  # 配置日志路径为 '/home/jd/kvcache/openeuler_datasystem/logs'
  log:
    logDir: /home/jd/kvcache/openeuler_datasystem/logs
    minLogLevel: 0
    logMonitor: true
```

### 元数据相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.metadata.rocksdbStoreDir | string | `"/home/jd/kvcache/rocksdb"` | 配置元数据持久化目录，元数据通过RocksDB持久化在磁盘中 |
| global.metadata.rocksdbWriteMode | string | `none` | 配置元数据写入RocksDB的方式，支持不写、同步和异步写入，默认值为`async`。可选值包括：'none'（不写）、'sync'（同步）、'async'（异步） |
| global.metadata.enableRedirect | bool | `false` | 是否开启元数据持久化目录重定向，如果开启，元数据将被持久化到指定的目录中 |
| global.metadata.enableDataReplication | bool | `false` | 是否开启元数据数据复制，如果开启，元数据将被复制到指定的目录中 |
| global.metadata.clusterName | string | `AZ1` | 配置ETCD表前缀，值只能包含英文字母（a-zA-Z）和数字（0-9）,用于逻辑隔离集群 |

**样例**：
```yaml
global:
  metadata:
    rocksdbStoreDir: /home/jd/kvcache/rocksdb
    rocksdbWriteMode: "none"
    enableRedirect: false
    enableDataReplication: false
    clusterName: "AZ1"
```

### 性能相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| global.performance.enableHugeTlb | bool | `false` | 是否开启共享内存大页内存功能，它可以提高内存访问，减少页表的开销 |
| global.performance.enableThp | bool | `false` | 是否启用透明大页（Transparent Huge Page,THP）功能，启用透明大页可以提高性能，减少页表开销，但也可能导致 Pod 内存使用增加 |
| global.performance.enableWorkerWorkerBatchGet | bool | `true` | 是否开启worker到worker的对象数据批量获取 |
| global.performance.enableUrma | bool | `true` | 是否开启Urma以实现对象worker之间的数据传输 |
| global.performance.urmaPollSize | int | `8` | 一次可轮询的完整记录数量，该设备最多可轮询16条记录 |
| global.performance.urmaRegisterWholeArena | bool | `true` | 是否在初始化时将整个arena注册为一个段，如果设置为`false`，将每个对象分别注册为一个段 |
| global.performance.urmaConnectionSize | int | `0` | [已废弃] 仅为兼容旧配置而保留，内部已忽略。当前 JFS/JFR 按连接独占创建 |
| global.performance.evictionReserveMemThresholdMB | int | `10240` | 内存驱逐阈值（MB），由 min(shared_memory_size_mb*0.1, eviction_reserve_mem_threshold_mb) 决定。当内存低于此阈值时，驱逐操作开始。有效范围为 100-102400。 |
