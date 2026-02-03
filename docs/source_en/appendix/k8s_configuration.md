# openYuanrong datasystem Kubernetes Configuration

<!-- TOC -->
- [Minimal Configurations](#minimal-configurations)
- [Detailed Configurations](#detailed-configurations)
    - [Image Configurations](#image-configurations)
    - [Namespace Configurations](#namespace-configurations)
    - [Resource Configurations](#resource-configurations)
    - [IPC/RPC Configurations](#ipcrpc-configurations)
    - [ETCD Configurations](#etcd-configurations)
    - [Spill Configurations](#spill-configurations)
    - [Log Configurations](#log-configurations)
    - [L2 cache Configurations](#l2-cache-configurations)
    - [AZ Configurations](#az-configurations)
    - [Metadata Configurations](#metadata-configurations)
    - [Reliability Configurations](#reliability-configurations)
    - [Graceful Shutdown Configurations](#graceful-shutdown-configurations)
    - [Performance Configurations](#performance-configurations)
    - [AK/SK Configurations](#aksk-configurations)
    - [Kubernetes Affinity Configurations](#kubernetes-affinity-configurations)
    - [Mount Path Configurations](#mount-path-configurations)
    - [Hybrid Deployment Configurations](#hybrid-deployment-configurations)
    - [Kubernetes Precision Control Configurations](#kubernetes-precision-control-configurations)

<!-- /TOC -->

This document describes openYuanrong datasystem Kubernetes configuration items.

## Minimal Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| [global.imageRegistry](#image-configurations) | string | `""` | image registry url |
| [global.images.datasystem](#image-configurations) | string | `"yr_datasystem:v0.1"` | image name and image tag |
| [global.etcd.etcdAddress](#etcd-configurations) | string | `""` | ETCD endpoints，openYuanrong datasystem cluster management relies on ETCD |

**Example**:
```yaml
global:
  # image：openyuanrong_datasystem:0.5.0
  imageRegistry: ""

  images:
    datasystem: "openyuanrong-datasystem:0.5.0"
  
  etcd:
    etcdAddress: "127.0.0.1:2379"
```

To deploy openYuanrong datasystem, refer to: [openYuanrong datasystem Kubernetes Deployment](../getting-started/deploy.md#openYuanrong-datasystem-kubernetes部署).

Each openYuanrong datasystem DaemonSet can use a maximum of 2GB shared memory space for data caching by default. If you need to adjust this value, you can configure it via [global.resources.datasystemWorker.sharedMemory](#resource-configurations).

> **Notes:**
>
> The size of [global.resources.datasystemWorker.sharedMemory](#resource-configurations) must be less than [global.resources.requests.memory](#resource-configurations).

```yaml
global:
  # openyuanrong_datasystem:0.5.0
  imageRegistry: ""

  images:
    datasystem: "openyuanrong-datasystem:0.5.0"
  
  etcd:
    etcdAddress: "127.0.0.1:2379"

  resources:
    datasystemWorker:
      limits:
        cpu: "3"
        # Limit 
        memory: "5Gi"
      requests:
        cpu: "3"
        # Request memory must greater than shreadMemory
        memory: "5Gi"
      # Set the shared memory limit to 4GB
      sharedMemory: 4096
```

> **Important Notes:**
> 
> - openYuanrong datasystem relies on ETCD for cluster management. Ensure the ETCD service is available before deployment.
> - openYuanrong datasystem uses port `31501` by default. Ensure this port is available on all Kubernetes nodes, or specify a custom port via [global.port.datasystemWorker](#ipcrpc-configurations).
> - Ensure each node in the Kubernetes cluster has at least 3 CPU cores and 4GB memory available, or reduce default resource requirements via [Resource Configurations](#resource-configurations).
> - openYuanrong datasystem Pod containers mount the following host directories by default. Ensure the container runtime user has read-write-execute (rwx) permissions for these directories:
>   | Host Path | Configuration | Function |
>   |-----------|--------|------|
>   | /home/sn/datasystem/logs | [global.log.logDir](#log-configurations) | Log persistence directory |
>   | /home/uds | [global.ipc.udsDir](#ipcrpc-configurations) | Unix Domain Socket file path for establishing shared memory mapping between datasystem_worker and client |
>   | /home/sn/datasystem/rocksdb | [global.metadata.rocksdbStoreDir](#metadata-configurations) | Metadata persistence directory |
>   | /dev/shm | - | Shared memory directory |

## Detailed Configurations

### Image Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.imageRegistry | string | `""` | Image prefix. Docker registry URL. Empty means using Docker Hub |
| global.images.datasystem | string | `"yr_datasystem:v0.1"` | Image name and tag in 'name:tag' format |

**Example**:
```yaml
global:
  # image：openyuanrong_datasystem:0.5.0
  imageRegistry: ""
  images:
    datasystem: "openyuanrong-datasystem:0.5.0"
```

### Namespace Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.namespace | string | `"default"` | datasystem-worker Pod namespace in Kubernetes |
| global.autoCreatedNamespace | bool | `false` | If the namespace is not "default" or does not exist, set this configuration to true to enable automatic creation |

### Resource Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.resources.datasystemWorker.limits.cpu | string | `"3"` | Maximum CPU cores occupied by a openYuanrong datasystem DaemonSet |
| global.resources.datasystemWorker.limits.memory | string | `"4Gi"` | Maximum memory occupied by a openYuanrong datasystem DaemonSet |
| global.resources.datasystemWorker.requests.cpu | string | `"3"` | CPU cores required for initializing a single openYuanrong datasystem DaemonSet. The value must be less than or equal to `global.resources.datasystemWorker.limits.cpu` |
| global.resources.datasystemWorker.requests.memory | string | `"4Gi"` | Memory required for initializing a single openYuanrong datasystem DaemonSet. The value must be less than or equal to `global.resources.datasystemWorker.limits.memory` |
| global.resources.datasystemWorker.maxClientNum | int | `200` | Maximum number of clients that can be connected to a worker |
| global.resources.datasystemWorker.sharedMemory | int | `2048` | Upper limit of the shared memory, the default unit for shared memory is MB. The value must be less than `global.resources.datasystemWorker.requests.memory` |

### IPC/RPC Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.ipc.ipcThroughSharedMemory | bool | `true` | Determines whether the shared memory feature is enabled |
| global.ipc.udsDir | string | `"/home/uds"` | Unix domain socket (UDS) file directory with 80-character path limit |
| global.port.datasystemWorker | int | `31501` | Port value (suggested range: 30000-32767) |
| global.rpc.enableCurveZmq | bool | `false` | Whether to enable the authentication function between datasystem_worker |
| global.rpc.curveKeyDir | string | `"/home/sn/datasystem/curve_key_dir"` | The directory to find ZMQ curve key files. This path must be specified when zmq authentication is enabled |
| global.rpc.curveZmqKey.clientPublicKey | string | `""` | Client's public key in the curve encryption environment |
| global.rpc.curveZmqKey.workerPublicKey | string | `""` | Datasystem-worker's public key in the curve encryption environment |
| global.rpc.curveZmqKey.workerSecretKey | string | `""` | Datasystem-worker's private key in the curve encryption environment |
| global.rpc.ocWorkerWorkerDirectPort | int | `0` | A direct TCP/IP port for worker-to-worker scenarios to improve latency. Acceptable value:0, or some positive integer. 0 means disabled |
| global.rpc.ocWorkerWorkerPoolSize | int | `3` | Number of parallel connections from worker to worker scenarios to improve throughput. `ocWorkerWorkerDirectPort` must be enabled to take effect |
| global.rpc.payloadNocopyThreshold | string | `"104857600"` | Minimum payload size in bytes to trigger direct write into shared memory. May incur extra network cost |
| global.rpc.rpcThreadNum | int | `128` | Config rpc server thread number, must be greater than 0 |
| global.rpc.ocThreadNum | int | `64` | The number of worker service for object cache |
| global.rpc.zmqServerIoContext | int | `5` | Optimize the performance of the customer. Default server is 5. The higher the throughput, the higher the value, but should be in range [1, 32] |
| global.rpc.zmqClientIoContext | int | `5` | Optimize the performance of the client stub. Default value is 5. The higher the throughput, the higher the value, but should be in range [1, 32] |
| global.rpc.zmqChunkSz | int | `1048576` | Parallel payload split chunk size. Default to 1048756 bytes |
| global.rpc.maxRpcSessionNum | int | `2048` | Maximum number of sessions that can be cached, must be within [512, 10'000] |
| global.rpc.streamIdleTimes | int | `300` | stream idle time. default 300s (5 minutes) |
| global.rpc.remoteSendThreadNum | int | `8` | The num of threads used to send elements to remote worker |

**Example**:

Configure a Unix Domain Socket path as "/home/uds" and use port 31501 as the listening port for the openYuanrong datasystem DaemonSet.

```yaml
global:
  port:
    datasystemWorker: 31501

  ipc:
    ipcThroughSharedMemory: true
    udsDir: /home/uds
```

### ETCD Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.etcd.etcdAddress | string | `""` | Configure ETCD server address. Should not be empty |
| global.etcd.enableEtcdAuth | bool | `false` | Whether to enable ETCD auth |
| global.etcd.etcdCa | string | `""` | The CA certificate. No encryption required. Base64 encoding is required |
| global.etcd.etcdCert | string | `""` | The client cert. No encryption required, base64 transcoding required |
| global.etcd.etcdKey | string | `""` | The client private key. Base64 transcoding is required, and whether encryption is required depends on whether passphrase is applied |
| global.etcd.etcdCertDir | string | `"/home/sn/datasystem/etcd_cert_dir"` | The path where the encrypted etcd certificate is mounted. This must be specified when etcd authentication is enabled |
| global.etcd.passphraseValue | string | `""` | The value of passphrase. Encryption is required, base64 transcoding required |
| global.etcd.etcdMetaPoolSize | int | `8` | ETCD metadata async operation pool size. If key size is large under WRITE_BACK_L2_CACHE mode, you may need to increase this value |
| global.etcd.etcdTargetNameOverride | string | `""` | Set ETCD target name override for SSL host name checking. The configuration value should be consistent with the DNS content of the Subject Alternate Names of the TLS certificate |

**Example**:

```yaml
global:
  # Connect to an ETCD service without auth.
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

### Spill Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.spill.spillDirectory | string | `""` | The path of the spilling, empty means local_dick spill disabled. It will create a new subdirectory("datasystem_spill_data") under the `spillDirectory` to store the spill file |
| global.spill.spillSizeLimit | string | `"0"` | Maximum amount of spilled data that can be stored in the spill directory. If spill is enable and spillSizeLimit is 0, spillSizeLimit will be set to 95% of the spill directory. Unit for spillSizeLimit is Bytes |
| global.spill.spillThreadNum | int | `8` | It represents the maximum parallelism of writing files, more threads will consume more CPU and I/O resources |
| global.spill.spillFileMaxSizeMb | int | `200` | The size limit of single spill file, spilling objects which lager than that value with one object per file. If there are some big objects, you can increase this value to avoid run out of inodes quickly. The valid range is 200-10240. |
| global.spill.spillFileOpenLimit | int | `512` | The maximum number of open file descriptors about spill. If opened file exceed this value, some files will be temporarily closed to prevent exceeding the maximum system limit. You need reduce this value if your system resources are limited. The valid range is greater than or equal to 8. |
| global.spill.spillEnableReadahead | bool | `true` | Disable readahead can mitigate the read amplification problem for offset read, default is true |
| global.spill.evictionThreadNum | int | `1` | Thread number of eviction for object cache |
| global.spill.spillToRemoteWorker | bool | `false` | It indicates that when node resources are insufficient, it supports spilling memory to the memory of other nodes. When enabled, if local node memory reaches the high watermark, the system attempts to migrate objects to other workers' shared memory. If no worker has available memory, objects spill to disk. |

- **Example1**:

    Spill directory is "/opt/spill/yr_datasystem_spill"，the maximum size is 10GB.

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

- **Example2**:

    The spill directory is "/opt/spill/yr_datasystem_spill". To enhance spill performance, mount the host machine's SSD (assumed path: "/data/ssd") with 10GB capacity. This scenario requires coordination with [Mount Path Configurations](#mount-path-configurations).

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
      # SSD path in host.
    - hostPath: "/data/ssd/yr_datasystem_spill"
      # The mounted directory inside the container must match the `spillDirectory`.
      mountPath: "/opt/spill/yr_datasystem_spill"
    ```
- **Example3**:

    Spill directory is "/opt/spill/yr_datasystem_spill"，the maximum size is 10GB. When spillToRemoteWorker is true, and the local node memory reaches the high watermark, it attempts to migrate objects to the shared memory of other workers. If no worker has available memory, objects spill to disk.

    ```yaml
    global:
      spill:
        spillDirectory: "/opt/spill/yr_datasystem_spill"
        spillSizeLimit: "10737418240"
        spillThreadNum: 8
        spillToRemoteWorker: true
        spillFileMaxSizeMb: 200
        spillFileOpenLimit: 512
        spillEnableReadahead: true
        evictionThreadNum: 1
    ```
### Log Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.log.logDir | string | `"/home/sn/datasystem/logs"` | The directory where log files are stored |
| global.log.logLevel | int | `0` | Vlog level, a larger value indicates more detailed logs. The value is between 0-3 |
| global.log.logAsync | bool | `true` | Flush log files with async mode |
| global.log.logAsyncQueueSize | int | `65536` | Size of async logger's message queue |
| global.log.logBufSecs | int | `10` | Buffer log messages for at most this many seconds |
| global.log.logCompress | bool | `true` | Compress old log files in .gz format. This parameter takes effect only when the size of the generated log is greater than max log size |
| global.log.logFilename | string | `""` | Prefix of log filename, default is program invocation short name. Use standard characters only |
| global.log.logRetentionDay | int | `0` | If log-retention-day is greater than 0, any log file from your project whose last modified time is greater than log-retention-day days will be unlinked. If log-retention-day is equal 0, will not unlink log file by time |
| global.log.maxLogFileNum | int | `25` | Maximum number of log files to retain per severity level. And every log file size is limited by max log size |
| global.log.maxLogSize | int | `400` | The maximum log file size (in MB), which must be greater than 0 |
| global.observability.logMonitor | bool | `true` | Record performance and resource logs |
| global.observability.logMonitorExporter | string | `"harddisk"` | Specify the type of exporter, [harddisk]. Takes effect only when logMonitor is true |
| global.observability.logMonitorIntervalMs | int | `10000` | The sleep time between iterations of observability collector scan |
| global.log.minLogLevel | int | `0` | Log messages below this level will not actually be recorded anywhere |

```yaml
global:
  # Log path is '/home/sn/datasystem/yr_datasystem/logs' and log name prefix is 'yr_datasystem'
  log:
    logDir: /home/sn/datasystem/yr_datasystem/logs
    logAsyncQueueSize: 65536
    logLevel: 0
    maxLogSize: 400
    maxLogFileNum: 25
    logRetentionDay: 0
    logAsync: true
    logBufSecs: 10
    # Enable log compress
    logCompress: true
    # Log filename prefix
    logFilename: "yr_datasystem"
```

### L2 cache Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.l2Cache.l2CacheType | string | `"none"` | Config the l2 cache type support obs, none by default. Optional value: 'obs', 'sfs', 'none' |
| global.l2Cache.l2CacheDeleteThreadNum | int | `32` | l2 cache delete threads number, it affects the parallelism of L2 data deletion. Increasing it will also cause CPU usage to increase |
| global.l2Cache.ocIoFromL2CacheNeedMetadata | bool | `true` | Whether data read and write from the L2 cache daemon depend on metadata. Note: If set to false, it indicates that the metadata is not stored in etcd |

::::{tab-set}

:::{tab-item} OBS

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.l2Cache.obs.obsAccessKey | string | `""` | The access key for obs AK/SK authentication |
| global.l2Cache.obs.obsSecretKey | string | `""` | The secret key for obs AK/SK authentication |
| global.l2Cache.obs.obsEndpoint | string | `""` |  OBS endpoint. Example: "xxx.hwcloudtest.cn" |
| global.l2Cache.obs.obsBucket | string | `""` | OBS bucket name |
| global.l2Cache.obs.obsHttpsEnabled | bool | `false` | Whether to enable the https in obs. false: use HTTP (default), true: use HTTPS |
| global.l2Cache.obs.cloudServiceTokenRotation.enable | bool | `false` | Whether to use ccms credential rotation mode to access OBS, default is false. If it is enabled, need to specify `iamHostName`, `identityProvider`, `projectId`, `regionId` at least. In addition, obsEndpoint and obsBucket need to be specified |
| global.l2Cache.obs.cloudServiceTokenRotation.iamHostName | string | `""` |  Domain name of the IAM token to be obtained. Example:  iam.example.com |
| global.l2Cache.obs.cloudServiceTokenRotation.identityProvider | string | `""` | Provider that provides permissions for the ds-worker. Example: csms-datasystem |
| global.l2Cache.obs.cloudServiceTokenRotation.projectId | string | `""` | Project id of the OBS to be accessed. Example: fb6a00ff7ae54a5fbb8ff855d0841d00 |
| global.l2Cache.obs.cloudServiceTokenRotation.regionId | string | `""` | Region id of the OBS to be accessed. Example: cn-beijing-4 |
| global.l2Cache.obs.cloudServiceTokenRotation.enableTokenByAgency | bool | `false` | Whether to access OBS of other accounts by agency, default is false. If is true, need to specify `tokenAgencyName` and `tokenAgencyDomain` |
| global.l2Cache.obs.cloudServiceTokenRotation.tokenAgencyDomain | string | `""` | Agency name for proxy access to other accounts. Example: obs_access |
| global.l2Cache.obs.cloudServiceTokenRotation.tokenAgencyName | string | `""` | Agency domain for proxy access to other accounts. Example: op_svc_cff |

:::

:::{tab-item} SFS

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.l2Cache.sfsTurbo.endpoint | string | `""` | Endpoint of sfs-turbo, which is used to concatenate the shared path in sfs-turbo |
| global.l2Cache.sfsTurbo.subPath | string | `""` | Sfs-turbo sub-path mounted to ds-worker. If this parameter is not specified, the root directory '/' of sfs-turbo is mounted by default |
| global.l2Cache.sfsTurbo.id | string | `"0"` | Specifies the sfs-turbo ID, which can be viewed on the sfs-turbo page |
| global.l2Cache.sfsTurbo.projectId | string | `"0"` | Specifies the sfs-turbo enterprise project ID, which can be viewed on the sfs-turbo page |
| global.l2Cache.sfsTurbo.capacity | string | `"500Gi"` | Specifies the capacity of using sfs-turbo. Note that the size must be smaller than the size of sfs-turbo |

:::

::::

### AZ Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.clusterName | string | `"AZ1"` | Available Zone name |
| global.crossAz.otherClusterNames | string | `""` | Specify other az names using the same etcd. Only split by ',' |
| global.crossAz.crossAzGetDataFromWorker | bool | `true` | Control whether to try to get data from other cluster's worker first. If false, data will be retrieved directly from the L2 cache |
| global.crossAz.crossAzGetMetaFromWorker | bool | `false` | Control whether to get meta data from other cluster's worker, if false then get meta data from local cluster |

**Example**:
```yaml
global:
  clusterName: "az1"

  crossAz:
    otherClusterNames: "az2,az3,az4"
    crossAzGetDataFromWorker: true
    crossAzGetMetaFromWorker: false
```

### Metadata Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.metadata.enableMetaReplica | bool | `false` | Controls whether to enable multiple meta replica |
| global.metadata.rocksdbStoreDir | string | `"/home/sn/datasystem/rocksdb"` | Config MASTER back store directory and must specify in rocksdb scenario. The rocksdb database is used to persistently store the metadata stored in the master so that the metadata before the restart can be re-obtained when the master restarts |
| global.metadata.rocksdbBackgroundThreads | int | `16` | Number of background threads rocksdb can use for flushing and compacting |
| global.metadata.rocksdbMaxOpenFile | int | `128` | Number of open files that can be used by the rocksdb |
| global.metadata.rocksdbWriteMode | string | `async` | Config the rocksdb support none, sync or async, async by default. Optional value: 'none', 'sync', 'async'. This represents the method of writing metadata to rocksdb |

### Reliability Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.reliability.clientReconnectWaitS | int | `5` | Client reconnect wait seconds |
| global.reliability.clientDeadTimeoutS | int | `120` | Maximum time interval for the worker to determine client death, value range: [15, UINT64_MAX) |
| global.reliability.heartbeatIntervalMs | int | `1000` | Time interval between worker and etcd heartbeats |
| global.reliability.nodeTimeoutS | int | `60` | Maximum time interval before a node is considered lost, the unit is second. (Must be greater than 5s) |
| global.reliability.nodeDeadTimeoutS | int | `300` | Maximum time interval for the etcd to determine node death, the unit is second. (Must be greater than nodeTimeoutS) |
| global.reliability.enableReconciliation | bool | `true` | Control whether to enable reconciliation |
| global.reliability.enableHashRingSelfHealing | bool | `false` | Whether to support self-healing when the hash ring is in an abnormal state |
| global.reliability.livenessProbeTimeoutS | int | `150` | Timeout interval of kubernetes liveness probe |
| global.reliability.addNodeWaitTimeS | int | `60` | Time to wait for the first node that wants to join a working hash ring |
| global.reliability.autoDelDeadNode | bool | `true` | Indicate dead nodes marked in the hash ring can be removed or not |
| global.reliability.enableDistributedMaster | bool | `true` | Whether to support distributed master, default is true |
| global.reliability.enableStreamDataVerification | bool | `false` | Option to verify if data from a producer is out of order |

### Graceful Shutdown Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.gracefulShutdown.scaleInTaint | string | `"datasystem/offline=true:NoExecute"` | Scale in taint, format is `key=value:effect` |
| global.gracefulShutdown.enableLosslessDataExitMode | bool | `false` | Decide whether to migrate data to other nodes or not when current node exits. If this is the only node in the cluster, exits directly and the data will be lost |
| global.gracefulShutdown.checkAsyncQueueEmptyTimeS | int | `1` | The worker ensures a certain period of time that the asynchronous queues for sending messages to ETCD and L2 cache remain empty before it can exit properly |
| global.gracefulShutdown.dataMigrateRateLimitMb | int | `40` | Data migration rate limit for every node when scaling down |
| global.gracefulShutdown.livenessProbeTerminationGracePeriodSeconds | int | `0` | Overwrite the global default terminationGracePeriodSeconds when liveness probe failed, enable when greater than 0 |

### Performance Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.performance.enableHugeTlb | bool | `false` | This is controlled by the flag of mmap(MAP_HUGETLB) which can improve memory access and reducing the overhead of page table |
| global.performance.enableFallocate | bool | `true` | Due to Kubernetes' (k8s) resource calculation policies, shared memory is sometimes counted twice, which can lead to client crashes. To address this issue, fallocate is employed to link the client and worker nodes for shared memory, thus correcting the memory calculation errors. By default, fallocate is enabled. Enabling fallocate will lower the efficiency of memory allocation |
| global.performance.sharedMemoryPopulate | bool | `false` | Avoiding page faults during copying improves runtime performance but may result in longer worker startup times (depending on sharedMemory). If set to true, it must be ensured that 'arenaPerTenant' is 1 and 'enableFallocate' is false |
| global.performance.enableThp | bool | `false` | Control this process by enabling transparent huge pages, default is disabled. Enabling Transparent Huge Pages (THP) can enhance performance and reduce page table overhead, but it may also lead to increased memory usage, leading to worker being terminated by the OOM Killer |
| global.performance.arenaPerTenant | int | `16` | The arena count for each tenant. Multiple arenas can improve the performance of share memory allocation for the first time, but each arena will use one more fd, value range: [1, 32] |
| global.performance.memoryReclamationTimeSecond | int | `600` | The memory reclamation time after free |
| global.performance.memoryAlignment | int | `64` | The memory alignment size used by jemalloc for all memory allocations (in bytes). Larger alignment can improve performance but may increase memory consumption due to fragmentation. |
| global.performance.asyncDelete | bool | `false` | Set whether to delete object asynchronously. If set to true, master will notify workers to delete objects asynchronously. Client doesn't need to wait for all workers to delete objects. |
| global.performance.enableP2pTransfer | bool | `false` | Heterogeneous object transfer protocol Enables p2p transfer |
| global.performance.enableWorkerWorkerBatchGet | bool | `false` | Enable worker->worker OC batch get, default false |
| global.performance.ocShmTransferThresholdKB  | int | `500` | The data threshold to transfer obj data between client and worker via shm, unit is KB |
| global.performance.enableUrma | bool | `false` | Option to turn on urma for OC worker to worker data transfer, default false |
| global.performance.urmaMode | string | `UB` | Option to enable URMA over IB or UB, default UB to run with URMA over UB |
| global.performance.urmaPollSize | int | `8` | Number of complete record to poll at a time, 16 is the max this device can poll |
| global.performance.urmaRegisterWholeArena | bool | `true` | Register the whole arena as segment during init, otherwise, register each object as a segment |
| global.performance.urmaConnectionSize | int | `16` | Number of jfs and jfr pair |
| global.performance.urmaEventMode | bool | `false` | Uses interrupt mode to poll completion events |
| global.performance.sharedDiskDirectory | string | `""` | Disk cache data placement directory, default value is empty, indicating that disk cache is not enabled |
| global.performance.sharedDiskSize | int | `0` | Upper limit of the shared disk, the unit is mb |
| global.performance.sharedDiskArenaPerTenant  | int | `8` | The number of disk cache Arena for each tenant. Multiple arenas can improve the performance of shared disk allocation for the first time, but each arena will use one more fd. The valid range is 0 to 32 |
| global.performance.enableRdma | bool | `false` | Option to turn on rdma for OC worker to worker data transfer, default false |
| global.performance.rdmaRegisterWholeArena | bool | `true` | Register the whole arena as segment during init, otherwise, register each object as a segment |
| global.performance.ocWorkerAggregateMergeSize | int | `2097152` | Target batch size for worker worker responses, default is 2MB |
| global.performance.ocWorkerAggregateSingleMax | int | `65536` | Max single item size for batching worker worker batch rsp, default is 64KB |
| global.performance.ocWorkerWorkerParallelMin | int | `100` | Min data count for parallel worker worker batch rsp, default is 100 |
| global.performance.ocWorkerWorkerParallelNums | int | `16` | Worker worker batch rsp control nums, 0 means unlimited |

### AK/SK Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.akSk.systemAccessKey | string | `""` | The access key used by the system |
| global.akSk.systemSecretKey | string | `""` | The secret key used by the system |
| global.akSk.systemDataKey | string | `""` | The data key for system encrypte and decrypt secert key |
| global.akSk.tenantAccessKey | string | `""` | The access key used by the tenant |
| global.akSk.tenantSecretKey | string | `""` | The secret key used by the tenant |
| global.akSk.requestExpireTimeS | int | `300` | Request expiration time in seconds, the maximum value is 300s |

### Kubernetes Affinity Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.affinity.nodeSelector | object | `{}` | Affinity configuration, which is used to control pod scheduling to nodes that meet specific conditions |
| global.affinity.nodeAffinity | object | `{}` | Affinity configuration are used to accurately control pod scheduling to nodes that meet specific conditions. Compared with nodeSelector, more powerful advanced scheduling policies are supported. |
| global.affinity.tolerations | list | `[]` | Taint tolerance. Pods are passively rejected by nodes and will not be expelled |

### Mount Path Configurations

The mount path configuration is typically used in conjunction with the [Spill Configurations](#spill-configurations)

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.mount.hostPath | string | `""` | host path |
| global.mount.mountPath | string | `""` | mount path in container |

**Example**:
```yaml
global:
  mount:
    # Spill mount path 1
    - hostPath: "/data/ssd1/yr_datasystem_spill"
      mountPath: "/opt/spill/yr_datasystem_spill1"
    # Spill mount path 2
    - hostPath: "/data/ssd2/yr_datasystem_spill"
      mountPath: "/opt/spill/yr_datasystem_spill2"
```

### Hybrid Deployment Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.multiSpec | list | `[]` | Support hybrid deployment. |

**Example**:
```yaml
global:
  # ...
  multiSpec:
  # Deploy Pods of the first specification on nodes with the affinity label `mid`
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
  # Deploy Pods of the second specification on nodes with the affinity label `small`
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
  # Deploy Pods of the third specification on nodes with the affinity label `big`
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

### Kubernetes Precision Control Configurations

| Configuration | Type | Default | Description |
|-----|------|---------|-------------|
| global.annotations | object | `{}` | Kubernetes meta annotation |
| global.enableNonPreemptive | bool | `false` | Configure priorityClass. If the value is false, the default priorityClass is system-cluster-critical. If the value is true, a priorityClass with preemptionPolicy Never is created |
| global.fsGid | string | `"1002"` | fsGroup configuratio. All processes of the container are also part of the supplementary group ID |
| global.rollingUpdateTimeoutS | int | `1800` | Maximum duration of the rolling upgrade, default value is 1800 seconds |
| global.security.scEncryptSecretKey | string | `1800` | The encrypted secret key for stream cache. The key length is up to 1024 bytes and must be 32 bytes after decryption |