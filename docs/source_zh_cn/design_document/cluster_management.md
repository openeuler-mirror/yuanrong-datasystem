# 集群管理

## 概述

集群管理是 openYuanrong datasystem 的核心能力之一，负责实现节点发现、健康检测、故障恢复及在线扩缩容等功能。通过集群管理，datasystem worker 能够注册到集群中，实现元数据的分布式管理，支持系统水平线性扩展和高可用。

openYuanrong datasystem 目前支持三种集群管理方式：

- **ETCD**：基于外部 ETCD 服务的分布式元数据存储方案
- **Metastore**：内置在 datasystem worker 中的元数据存储服务，无需部署外部 ETCD
- **Coordinator**：通过独立的 datasystem Coordinator 服务管理集群信息，Worker 使用 `coordinator_address` 接入

## 集群管理方式对比
### 基于 ETCD 的集群管理

ETCD 是一个高可用的键值存储系统，专门用于配置共享和服务发现。在 ETCD 架构中：

- **外部依赖**：需要单独部署和管理 ETCD 集群
- **架构分离**：ETCD 作为独立组件运行，datasystem worker 通过 gRPC 与 ETCD 通信
- **功能完整**：ETCD 提供完整的分布式键值存储、Watch、Lease 等功能
- **运维复杂**：需要额外维护 ETCD 集群的健康性和可用性

**部署架构**：

<img src="../images/cluster-management-etcd.png" alt="ETCD 部署架构" width="500">

**适用场景**：
- **已有 ETCD 集群**：环境中已经部署了 ETCD 集群，可以复用现有基础设施
- **多集群共享元数据**：多个 datasystem 集群需要共享同一个 ETCD 作为元数据中心
- **高可用要求高**：需要跨数据中心的高可用部署，ETCD 提供更成熟的高可用方案
- **独立运维需求**：希望将元数据存储与业务组件解耦，便于独立运维和监控

### 基于 Metastore 的集群管理

Metastore 是集成在 datasystem worker 中的元数据存储服务，提供与 ETCD 兼容的 gRPC 接口。在 Metastore 架构中：

- **内置服务**：Metastore 作为 worker 进程的一部分启动
- **架构简化**：无需外部依赖，worker 自管理元数据
- **接口兼容**：完全兼容 ETCD 的 gRPC 接口协议
- **运维简化**：减少了组件数量，降低了部署复杂度

**部署架构**：

<img src="../images/cluster-management-metastore.png" alt="Metastore 部署架构" width="400" >


其中 `Worker 0` 作为主节点，启动 Metastore 服务，其他 worker 通过 `metastore_address` 连接到主节点的 Metastore 服务。

**适用场景**：
- **快速部署**：需要快速搭建 datasystem 集群，不想额外部署 ETCD
- **资源受限**：生产环境资源有限，希望减少组件数量
- **简化运维**：减少组件数量，降低运维复杂度

### 基于 Coordinator 的集群管理

Coordinator 作为独立服务运行，Worker 通过 `coordinator_address` 连接 Coordinator，并从 Coordinator 获取
集群管理所需的信息。客户端可以使用 `CoordinatorServiceDiscovery` 从 Coordinator 查询就绪 Worker，
再按节点亲和性策略选择连接目标。

该方式具有以下特点：

- **独立服务**：需要单独启动和管理 `datasystem_coordinator` 进程
- **统一入口**：Worker 和 SDK 均通过 Coordinator 获取集群信息
- **无需 ETCD**：使用 Coordinator 管理集群时，不需要同时配置 ETCD 或 Worker 内置 Metastore
- **当前地址约束**：SDK 的 Coordinator 服务发现当前要求配置且仅配置一个 Coordinator 地址

**适用场景**：

- 希望将集群管理能力从 Worker 进程中独立部署
- Worker 和客户端需要通过统一的 Coordinator 查询集群信息
- 希望通过 `dscli` 在同一节点统一启停 Coordinator 和 Worker


## 部署方式

### 基于 ETCD 部署

ETCD 的部署需要先部署外部 ETCD 服务。ETCD 的安装部署可参考：[安装并部署ETCD](../deployment/deploy.md#安装并部署etcd)。

**快速部署**：

```bash
dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
# [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
```

**集群部署**：

1. 生成配置文件：

```bash
dscli generate_config -o ./
# [INFO] Configuration file cluster_config.json has been generated to /home/user/cluster_config.json
# [INFO] Configuration file worker_config.json has been generated to /home/user/worker_config.json
# [INFO] Configuration file coordinator_config.json has been generated to /home/user/coordinator_config.json
# [INFO] Configuration generation completed successfully
```

2. 编辑 `cluster_config.json`，配置集群节点信息：

```json
{
    "ssh_auth": {
        "ssh_private_key": "~/.ssh/id_rsa",
        "ssh_user_name": "root"
    },
    "worker_config_path": "./worker_config.json",
    "worker_nodes": [
        "192.168.1.1",
        "192.168.1.2"
    ],
    "worker_port": 31501
}
```

> **注意事项**：
>
> - 多机集群部署依赖多机之间配置 SSH 互信，SSH 互信配置可参考：[SSH互信配置](../deployment/deploy.md#ssh互信配置)
> - 所有待部署的机器上都需要安装 dscli，dscli 安装可参考：[dscli安装教程](../deployment/dscli.md#dscli安装教程)

3. 编辑 `worker_config.json`，配置 ETCD 地址：

```json
{
    "etcd_address": {
        "value": "127.0.0.1:2379"
    }
}
```

4. 部署集群：

```bash
dscli up -f ./cluster_config.json
# [INFO] Start worker service @ 192.168.1.1:31501 success.
# [INFO] Start worker service @ 192.168.1.2:31501 success.
```

### 基于 Metastore 部署

Metastore 部署无需外部 ETCD 服务，由 worker 内置提供元数据存储功能。多机部署时需要指定主节点，主节点的 worker 启动 Metastore 服务，其他 worker 连接到主节点的 Metastore 服务。

**配置说明**：

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| metastore_head_node | string | "" | 指定启动 Metastore 服务的主节点 IP，必须在集群配置文件 `cluster_config.json` 的 `worker_nodes` 中（仅 `dscli up` 使用） |
| start_metastore_service | bool | `false` | 是否启用 Metastore 代替ETCD，若需要启用，仅需主节点worker设为`true`，从节点worker设为`false` |
| metastore_address | string | `""` | 主节点worker的Metastore Service访问地址，与 `start_metastore_service` 搭配一起使用，主从节点均需填写，格式为：ip:port，例如：127.0.0.1:2379 |

**快速部署**：

```bash
# 主节点
dscli start -w --worker_address "192.168.1.1:31501" \
               --start_metastore_service true \
               --metastore_address "192.168.1.1:2379"
# [INFO] [  OK  ] Start worker service @ 192.168.1.1:31501 success

# 从节点
dscli start -w --worker_address "192.168.1.2:31501" \
               --start_metastore_service false \
               --metastore_address "192.168.1.1:2379"
# [INFO] [  OK  ] Start worker service @ 192.168.1.2:31501 success
```

**集群部署**：

1. 生成配置文件：

```bash
dscli generate_config -o ./
```

2. 编辑 `cluster_config.json`，指定 `metastore_head_node`：

```json
{
    "ssh_auth": {
        "ssh_private_key": "~/.ssh/id_rsa",
        "ssh_user_name": "root"
    },
    "worker_config_path": "./worker_config.json",
    "worker_nodes": [
        "192.168.1.1",
        "192.168.1.2"
    ],
    "worker_port": 31501,
    "metastore_head_node": "192.168.1.1"
}
```

> **注意事项**：
>
> - 多机集群部署依赖多机之间配置 SSH 互信，SSH 互信配置可参考：[SSH互信配置](../deployment/deploy.md#ssh互信配置)
> - 所有待部署的机器上都需要安装 dscli，dscli 安装可参考：[dscli安装教程](../deployment/dscli.md#dscli安装教程)

3. 编辑 `worker_config.json`，配置 Metastore 相关参数：

```json
{
    "etcd_address": {
        "value": ""
    },
    "metastore_address": {
        "value": "192.168.1.1:2379"
    }
}
```

4. 部署集群：

```bash
dscli up -f ./cluster_config.json

# [INFO] Modified config - metastore_address
# [INFO] Starting metastore head node: 192.168.1.1
# [INFO] Setting start_metastore_service=true for node: 192.168.1.1
# [INFO] Start worker service @ 192.168.1.1:31501 success.
# [INFO] Starting other worker nodes in parallel: ['192.168.1.2']
# [INFO] Setting start_metastore_service=false for node: 192.168.1.2
# [INFO] Start worker service @ 192.168.1.2:31501 success.
```

也可以直接通过命令行参数指定 Metastore 头节点，无需修改 `cluster_config.json`：

```bash
dscli up --metastore_head_node 192.168.1.1 -f ./cluster_config.json
```

### 基于 Coordinator 部署

使用 Coordinator 时，需要先启动 Coordinator，再启动配置了 `coordinator_address` 的 Worker。

```bash
# 分别启动 Coordinator 和 Worker
dscli start --coordinator_args --coordinator_address "127.0.0.1:31511"
dscli start --worker_args --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"
```

也可以通过一条命令依次启动两个服务：

```bash
dscli start --coordinator_worker_args --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"
```

使用配置文件时，`coordinator_config.json` 中的 `service_type` 为 `coordinator`，`worker_config.json` 中的
`service_type` 为 `worker`。同时需要将 `worker_config.json` 中的 `coordinator_address` 配置为 Coordinator
服务地址；Worker 的 `coordinator_address`、`etcd_address` 和 `metastore_address` 必须且只能配置一个。

```json
{
    "coordinator_address": {
        "value": "127.0.0.1:31511"
    }
}
```

`dscli start -f` 和 `dscli stop -f` 根据 `service_type` 选择对应服务；字段缺失或值为空时按 Worker 处理。

```bash
# 使用配置文件分别启动服务
dscli start -f ./coordinator_config.json
dscli start -f ./worker_config.json

# 先停止 Worker，再停止 Coordinator
dscli stop --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"
```

完整的 Coordinator 和 Worker 启停命令参见
[使用Coordinator部署](../deployment/dscli.md#openyuanrong-datasystem集群使用coordinator部署)。
