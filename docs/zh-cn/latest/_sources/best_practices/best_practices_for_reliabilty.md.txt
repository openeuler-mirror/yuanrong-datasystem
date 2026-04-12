# 可靠性最佳实践

本文档介绍 openYuanrong datasystem 在可靠性场景下的二级缓存配置方法。当前承担数据可靠性能力的
`l2_cache_type` 包括 `distributed_disk`、`sfs` 和 `obs` 三种模式。

## 可靠性能力的生效条件

可靠性能力同时依赖集群侧配置和业务侧写入模式。

1. 集群侧需要配置二级缓存类型，即将 `l2_cache_type` 或 `l2CacheType` 设置为
   `distributed_disk`、`sfs` 或 `obs`。
2. 业务侧写入数据时，需要使用具备二级缓存语义的写模式。

具备可靠性语义的写模式包括：

- `WRITE_THROUGH_L2_CACHE`
- `WRITE_BACK_L2_CACHE`
- `WRITE_BACK_L2_CACHE_EVICT`

其中：

- `WRITE_THROUGH_L2_CACHE` 表示同步写入二级缓存，可靠性语义最直接。
- `WRITE_BACK_L2_CACHE` 表示异步写入二级缓存，在故障窗口内可能存在尚未落入二级缓存的数据。
- `WRITE_BACK_L2_CACHE_EVICT` 表示异步写入二级缓存，并允许对象在缓存容量不足时被提前淘汰。

以下写模式不提供二级缓存可靠性：

- `NONE_L2_CACHE`
- `NONE_L2_CACHE_EVICT`

## 二级缓存类型说明

| 类型 | 依赖介质 | 配置特点 | 典型场景 |
| --- | --- | --- | --- |
| `distributed_disk` | 共享 NFS 目录 | 通过 slot 存储在共享目录上持久化数据 | 集群内所有 worker 已挂载同一份 NFS 目录 |
| `sfs` | SFS Turbo | 依赖共享文件系统参数 | 已使用 SFS Turbo 承载可靠性数据 |
| `obs` | 对象存储服务 | 依赖 OBS 认证信息和桶配置 | 需要通过对象存储提供可靠性能力 |

## 不同部署方式的配置入口

不同部署方式对应的可靠性配置入口如下：

| 部署方式 | 配置入口 | 参数形式 |
| --- | --- | --- |
| Kubernetes DaemonSet | `k8s/helm_chart/datasystem/values.yaml` | `global.l2Cache.*` |
| Kubernetes Deployment | `k8s_deployment/helm_chart/worker.config` | worker 级 flag，例如 `l2_cache_type` |
| `dscli` 进程部署 | `worker_config.json` 或 `dscli start` 命令行参数 | worker 级 flag，例如 `l2_cache_type` |

对于 Kubernetes DaemonSet 模式，Helm chart 会将 `values.yaml` 中的 `global.l2Cache.*` 转换为实际的 worker
启动参数。对于 Kubernetes Deployment 模式和 `dscli` 进程部署模式，可靠性相关配置均直接以 worker flag 的
形式生效。

## 配置方法

:::::{tab-set}

:::{tab-item} distributed_disk

**模式说明**

`distributed_disk` 模式由 slot 存储承担可靠性能力。系统会基于 `distributed_disk_path` 构建 slot 根目录，并在
该目录下持久化对象版本数据。该路径并不是各 worker 独占的本地磁盘路径，而应部署在**所有 worker 均可访问的
同一份共享 NFS 目录**上。

生产环境中使用 `distributed_disk` 时，应满足以下条件：

- 所有 worker 节点均已挂载同一份 NFS 目录。
- 所有 worker 对该目录具有一致的读写可见性。
- `distributed_disk_path` 在所有 worker 上最终指向同一份共享存储。

**配置入口概览**

| 部署方式 | 配置入口 | 关键参数 |
| --- | --- | --- |
| Kubernetes DaemonSet | `k8s/helm_chart/datasystem/values.yaml` | `global.l2Cache.l2CacheType`、`global.l2Cache.distributedDisk.*` |
| Kubernetes Deployment | `k8s_deployment/helm_chart/worker.config` | `l2_cache_type`、`distributed_disk_path`、`distributed_disk_*` |
| `dscli` 进程部署 | `worker_config.json` 或 `dscli start` 参数 | `l2_cache_type`、`distributed_disk_path`、`distributed_disk_*` |

该模式下需要重点关注：

- `distributed_disk_path` 应配置为所有 worker 均可访问的共享 NFS 路径。
- DaemonSet Helm chart 会将宿主机路径挂载到容器内固定目录
  `/home/yuanrong/datasystem/distributed_disk`，并以
  `-distributed_disk_path=/home/yuanrong/datasystem/distributed_disk`
  的形式传递给 worker。

**必选配置项与可选配置项**

- 必选配置项：`l2_cache_type=distributed_disk`、`distributed_disk_path`
- 可选配置项：`distributed_disk_max_data_file_size_mb`、`distributed_disk_sync_interval_ms`、
  `distributed_disk_sync_batch_bytes`、`distributed_disk_compact_interval_s`、`enableL2CacheFallback`
- `distributed_disk_path` 用于指定所有 worker 共享可见的 NFS 目录，是该模式能够正常工作的前提。
- 其余可选配置项主要用于调优文件大小、刷盘节奏和 compact 周期；如无特殊吞吐或时延诉求，可先使用默认值。

**Kubernetes DaemonSet**

```yaml
global:
  l2Cache:
    # 必选：启用 distributed_disk 二级缓存
    l2CacheType: "distributed_disk"

    distributedDisk:
      # 必选：宿主机上所有 worker 共享可见的 NFS 挂载路径
      path: "/mnt/datasystem-nfs"
      # 可选：单个 data file 的大小上限，单位 MB
      maxDataFileSizeMb: 1024
      # 可选：group commit 的最长刷盘间隔，单位毫秒
      syncIntervalMs: 1000
      # 可选：group commit 的批量刷盘阈值，单位字节
      syncBatchBytes: 33554432
      # 可选：后台 compact 执行周期，单位秒
      compactIntervalS: 3600
```

DaemonSet 模式下，用户真正需要提供的是宿主机侧的共享 NFS 路径 `global.l2Cache.distributedDisk.path`。
Helm chart 会负责将该路径挂载到容器内固定目录，并把对应 worker flag 自动传入进程。

**Kubernetes Deployment**

```javascript
{
  // 必选：启用 distributed_disk 二级缓存
  "l2_cache_type": {
    "value": "distributed_disk"
  },
  // 必选：所有 worker 共享可见的 NFS 路径
  "distributed_disk_path": {
    "value": "/mnt/datasystem-nfs"
  },
  // 可选：单个 data file 的大小上限，单位 MB
  "distributed_disk_max_data_file_size_mb": {
    "value": "1024"
  },
  // 可选：group commit 的最长刷盘间隔，单位毫秒
  "distributed_disk_sync_interval_ms": {
    "value": "1000"
  },
  // 可选：group commit 的批量刷盘阈值，单位字节
  "distributed_disk_sync_batch_bytes": {
    "value": "33554432"
  },
  // 可选：后台 compact 执行周期，单位秒
  "distributed_disk_compact_interval_s": {
    "value": "3600"
  }
}
```

Kubernetes Deployment 模式下，worker 直接读取 `worker.config` 中的 flag 配置，因此最小可运行配置是
`l2_cache_type` 和 `distributed_disk_path`；其他 `distributed_disk_*` 选项均属于可选调优参数。

**dscli 进程部署**

`dscli` 进程部署建议分两步配置：先配置必选项，确认系统已经使用共享 NFS 目录工作；再根据实际吞吐和刷盘压力补充可选项。

命令行最小示例：

```bash
dscli start -w \
  --worker_address "127.0.0.1:31501" \
  --etcd_address "127.0.0.1:2379" \
  --l2_cache_type "distributed_disk" \
  --distributed_disk_path "/mnt/datasystem-nfs"
```

命令行补充可选调优项后的示例：

```bash
dscli start -w \
  --worker_address "127.0.0.1:31501" \
  --etcd_address "127.0.0.1:2379" \
  --l2_cache_type "distributed_disk" \
  --distributed_disk_path "/mnt/datasystem-nfs" \
  --distributed_disk_max_data_file_size_mb 1024 \
  --distributed_disk_sync_interval_ms 1000 \
  --distributed_disk_sync_batch_bytes 33554432 \
  --distributed_disk_compact_interval_s 3600
```

必选配置项：

- `--l2_cache_type distributed_disk`
- `--distributed_disk_path /path/to/shared-nfs`

可选配置项：

- `--distributed_disk_max_data_file_size_mb`
- `--distributed_disk_sync_interval_ms`
- `--distributed_disk_sync_batch_bytes`
- `--distributed_disk_compact_interval_s`

`worker_config.json` 示例：

```javascript
{
  // 必选：启用 distributed_disk 二级缓存
  "l2_cache_type": {
    "value": "distributed_disk"
  },
  // 必选：所有 worker 共享可见的 NFS 路径
  "distributed_disk_path": {
    "value": "/mnt/datasystem-nfs"
  },
  // 可选：单个 data file 的大小上限，单位 MB
  "distributed_disk_max_data_file_size_mb": {
    "value": "1024"
  },
  // 可选：group commit 的最长刷盘间隔，单位毫秒
  "distributed_disk_sync_interval_ms": {
    "value": "1000"
  },
  // 可选：group commit 的批量刷盘阈值，单位字节
  "distributed_disk_sync_batch_bytes": {
    "value": "33554432"
  },
  // 可选：后台 compact 执行周期，单位秒
  "distributed_disk_compact_interval_s": {
    "value": "3600"
  }
}
```

**使用建议**

- 当业务已经具备统一的 NFS 挂载体系时，`distributed_disk` 是优先推荐的配置方式。
- 该模式依赖共享目录的一致可见性，不适合将每个 worker 指向彼此独立的本地磁盘目录。

:::

:::{tab-item} sfs

**模式说明**

`sfs` 模式依赖 SFS Turbo 作为二级缓存存储介质。worker 通过挂载的共享文件系统路径读写二级缓存数据。

**配置入口概览**

| 部署方式 | 配置入口 | 关键参数 |
| --- | --- | --- |
| Kubernetes DaemonSet | `k8s/helm_chart/datasystem/values.yaml` | `global.l2Cache.l2CacheType`、`global.l2Cache.sfsTurbo.*` |
| Kubernetes Deployment | `k8s_deployment/helm_chart/worker.config` | `l2_cache_type`、`sfs_path` |
| `dscli` 进程部署 | `worker_config.json` 或 `dscli start` 参数 | `l2_cache_type`、`sfs_path` |

该模式下，Helm chart 会在容器内使用固定的 `sfs_path`，并由 PVC 将 SFS Turbo 挂载到 worker。

**Kubernetes DaemonSet**

```yaml
global:
  l2Cache:
    l2CacheType: "sfs"
    enableL2CacheFallback: true

    sfsTurbo:
      endpoint: "127.0.0.1"
      subPath: "/datasystem"
      id: "0"
      projectId: "0"
      capacity: "500Gi"
```

**Kubernetes Deployment**

```json
{
  "l2_cache_type": {
    "value": "sfs"
  },
  "sfs_path": {
    "value": "/opt/data/datasystem/sfs-turbo"
  }
}
```

**dscli 进程部署**

`worker_config.json` 示例：

```json
{
  "l2_cache_type": {
    "value": "sfs"
  },
  "sfs_path": {
    "value": "/opt/data/datasystem/sfs-turbo"
  }
}
```

命令行示例：

```bash
dscli start -w \
  --worker_address "127.0.0.1:31501" \
  --etcd_address "127.0.0.1:2379" \
  --l2_cache_type "sfs" \
  --sfs_path "/opt/data/datasystem/sfs-turbo"
```

**使用建议**

- 当集群已经使用 SFS Turbo 作为共享文件系统时，优先考虑 `sfs`。
- 需要确保所有 worker 均能访问相同的 SFS 路径。

:::

:::{tab-item} obs

**模式说明**

`obs` 模式依赖对象存储服务承载可靠性数据，适用于已经具备 OBS 访问与认证体系的场景。

**配置入口概览**

| 部署方式 | 配置入口 | 关键参数 |
| --- | --- | --- |
| Kubernetes DaemonSet | `k8s/helm_chart/datasystem/values.yaml` | `global.l2Cache.l2CacheType`、`global.l2Cache.obs.*` |
| Kubernetes Deployment | `k8s_deployment/helm_chart/worker.config` | `l2_cache_type`、`obs_access_key`、`obs_secret_key`、`obs_endpoint`、`obs_bucket` |
| `dscli` 进程部署 | `worker_config.json` 或 `dscli start` 参数 | `l2_cache_type`、`obs_access_key`、`obs_secret_key`、`obs_endpoint`、`obs_bucket` |

如果集群使用临时凭据模式，DaemonSet 模式下还应继续配置
`global.l2Cache.obs.cloudServiceTokenRotation.*`。

**Kubernetes DaemonSet**

```yaml
global:
  l2Cache:
    l2CacheType: "obs"
    enableL2CacheFallback: true

    obs:
      obsAccessKey: ""
      obsSecretKey: ""
      obsEndpoint: "obs.example.com"
      obsBucket: "datasystem-l2"
      obsHttpsEnabled: true
```

**Kubernetes Deployment**

```json
{
  "l2_cache_type": {
    "value": "obs"
  },
  "obs_access_key": {
    "value": ""
  },
  "obs_secret_key": {
    "value": ""
  },
  "obs_endpoint": {
    "value": "obs.example.com"
  },
  "obs_bucket": {
    "value": "datasystem-l2"
  },
  "obs_https_enabled": {
    "value": "true"
  }
}
```

**dscli 进程部署**

`worker_config.json` 示例：

```json
{
  "l2_cache_type": {
    "value": "obs"
  },
  "obs_access_key": {
    "value": ""
  },
  "obs_secret_key": {
    "value": ""
  },
  "obs_endpoint": {
    "value": "obs.example.com"
  },
  "obs_bucket": {
    "value": "datasystem-l2"
  },
  "obs_https_enabled": {
    "value": "true"
  }
}
```

命令行示例：

```bash
dscli start -w \
  --worker_address "127.0.0.1:31501" \
  --etcd_address "127.0.0.1:2379" \
  --l2_cache_type "obs" \
  --obs_access_key "<ak>" \
  --obs_secret_key "<sk>" \
  --obs_endpoint "obs.example.com" \
  --obs_bucket "datasystem-l2" \
  --obs_https_enabled true
```

**使用建议**

- 当业务侧已经具备 OBS 认证、桶管理与网络连通能力时，`obs` 是合适的二级缓存模式。
- 需要确保认证参数、桶名和 endpoint 与实际云环境一致。

:::

:::::

## 使用建议

1. 首次接入可靠性能力时，应先明确部署方式，再选择对应的配置入口。
2. `distributed_disk` 模式下，最关键的前提是共享 NFS 目录的一致可见性。
3. 无论使用哪种二级缓存类型，业务侧都应同步审视 `writeMode`，避免仅配置集群侧参数而未启用可靠性写模式。

## 参考文档

- [KV 示例中的可靠性章节](../api_reference/example/kv.md#数据可靠性)
- [Kubernetes 配置说明](../deployment/k8s_configuration.md#二级缓存相关配置)
- [dscli 部署参数说明](../deployment/dscli.md#二级缓存相关配置)
