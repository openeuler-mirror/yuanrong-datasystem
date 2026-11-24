# 可靠性最佳实践

本文档介绍 openYuanrong datasystem 在可靠性场景下的二级缓存配置方法。当前承担数据可靠性能力的
`l2_cache_type` 包括 `sfs` 和 `obs` 两种模式。

## 可靠性能力的生效条件

可靠性能力同时依赖集群侧配置和业务侧写入模式。

1. 集群侧需要配置二级缓存类型，即将 `l2_cache_type` 或 `l2CacheType` 设置为
   `sfs` 或 `obs`。
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
| `sfs` | SFS Turbo | 依赖共享文件系统参数 | 已使用 SFS Turbo 承载可靠性数据 |
| `obs` | 对象存储服务 | 依赖 OBS 认证信息和桶配置 | 需要通过对象存储提供可靠性能力 |

## 不同部署方式的配置入口

不同部署方式对应的可靠性配置入口如下：

| 部署方式 | 配置入口 | 参数形式 |
| --- | --- | --- |
| Kubernetes DaemonSet | `k8s/helm_chart/datasystem/values.yaml` | `global.l2Cache.*` |
| `dscli` 进程部署 | `worker_config.json` 或 `dscli start` 命令行参数 | worker 级 flag，例如 `l2_cache_type` |

对于 Kubernetes DaemonSet 模式，Helm chart 会将 `values.yaml` 中的 `global.l2Cache.*` 转换为实际的 worker
启动参数；对于 `dscli` 进程部署模式，可靠性相关配置直接以 worker flag 的形式生效。

## 配置方法

:::::{tab-set}

:::{tab-item} sfs

**模式说明**

`sfs` 模式依赖 SFS Turbo 作为二级缓存存储介质。worker 通过挂载的共享文件系统路径读写二级缓存数据。

**配置入口概览**

| 部署方式 | 配置入口 | 关键参数 |
| --- | --- | --- |
| Kubernetes DaemonSet | `k8s/helm_chart/datasystem/values.yaml` | `global.l2Cache.l2CacheType`、`global.l2Cache.sfsTurbo.*` |
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
2. 无论使用哪种二级缓存类型，业务侧都应同步审视 `writeMode`，避免仅配置集群侧参数而未启用可靠性写模式。

## 参考文档

- [KV 示例中的可靠性章节](../api_reference/example/kv.md#数据可靠性)
- [Kubernetes 配置说明](../deployment/k8s_configuration.md#二级缓存相关配置)
- [dscli 部署参数说明](../deployment/dscli.md#二级缓存相关配置)
