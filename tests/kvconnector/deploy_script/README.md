# vLLM & YuanRong 智能部署管理工具 (vllm\_yuanrong\_cli)

## 1\. 工具简介

`vllm_yuanrong_cli` 是一个功能强大的命令行工具，旨在简化和自动化 vLLM 与 YuanRong 分布式KV缓存系统的部署和管理。它将所有零散的脚本整合为一个统一的入口，通过命令行参数即可支持 **PD合并** 和 **PD分离** 两种核心部署架构，极大提升了部署、调试和运维效率。

## 2\. 核心功能

  * **架构感知**：通过简单的命令行参数即可在 `PD合并` 和 `PD分离` 模式间切换。
  * **角色驱动**：清晰地定义了 `merged`, `primary`, `secondary` 三种节点角色，自动执行匹配该角色的任务。
  * **配置与逻辑分离**：将所有环境配置集中在 `deploy_env.sh` 文件中，主脚本 `vllm_yuanrong_cli.sh` 负责执行逻辑，使配置更清晰，脚本更稳定。
  * **生命周期管理**：提供 `start`, `stop`, `restart`, `clean`, `status` 等完整的服务生命周期管理命令。
  * **一键式操作**：简化了多服务（etcd, ds-worker, vLLM, proxy-server）的启动和停止顺序，避免手动操作的复杂性和易错性。

## 3\. 文件结构

本工具由两个核心文件组成，请确保它们始终位于同一目录下。

```
/your/workspace/
├── vllm_yuanrong_cli.sh   # 主执行脚本 (CLI Tool)
└── deploy_env.sh         # 环境变量配置文件 (Configuration)
```

  * **`vllm_yuanrong_cli.sh`**: 命令行接口工具。**用户直接执行此脚本**。
  * **`deploy_env.sh`**: 存储所有部署所需的环境变量，如IP地址、端口、模型路径等。**用户在此文件中进行部署前的配置**。

## 4\. 快速开始

1.  **放置文件**：将 `vllm_yuanrong_cli.sh` 和 `deploy_env.sh` 放置在部署节点的同一目录下。
2.  **配置环境**：根据您的环境，详细编辑 `deploy_env.sh` 文件。请参考下面的配置说明。
3.  **授予权限**：在终端中为CLI工具添加可执行权限。
    ```bash
    chmod +x vllm_yuanrong_cli.sh
    ```
4.  **执行命令**：根据您的部署需求，执行相应的命令。例如，在主节点上部署PD分离模式：
    ```bash
    ./vllm_yuanrong_cli.sh start primary
    ```

## 5\. 配置说明 (`deploy_env.sh`)

此文件是部署的核心，请在执行任何命令前仔细配置。所有变量都使用**前缀**来区分其所属的角色。

| 变量前缀 | 适用角色 | 说明 |
| :--- | :--- | :--- |
| `MERGED_*` | `merged` | 用于PD合并模式的配置，如 `MERGED_HOST_IP`。 |
| `PRIMARY_*` | `primary` | 用于PD分离模式下的主节点，如 `PRIMARY_HOST_IP`。 |
| `SECONDARY_*` | `secondary` | 用于PD分离模式下的从节点，如 `SECONDARY_HOST_IP`。 |

**关键配置项：**

  * **`*_HOST_IP`**:  指定该角色所在节点的IP地址。
  * **`SECONDARY_ETCD_IP_FOR_CLIENT`**: **[极其重要]** 从节点连接etcd所用的IP，**必须设置为Primary节点的IP地址**。
  * **`*_KV_CONFIG`**: 为不同角色的vLLM实例指定 `kv_transfer_config`。
      * `merged` -\> `"kv_both"`
      * `primary` -\> `"kv_producer"`
      * `secondary` -\> `"kv_consumer"`
  * **通用参数**: 文件下方的参数（如 `MODEL_PATH`, `VISIBLE_DEVICES`等）被所有角色共享，请根据实际情况修改。

## 6\. 命令行用法

### 命令语法

工具采用标准 `command <action> <role>` 语法结构。

```bash
./vllm_yuanrong_cli.sh [action] [role]
```

### Actions (动作)

| Action | 描述 |
| :--- | :--- |
| `start` | **启动**指定角色的所有相关服务。 |
| `stop` | **停止**指定角色的所有相关服务。 |
| `restart` | **重启**指定角色的服务（先执行`stop`再执行`start`）。|
| `clean` | **停止**所有服务并**清理**产生的日志和临时文件。 |
| `status` | **检查**并显示当前节点上相关服务的运行状态。 |

### Roles (角色)

| Role | 描述 | 启动的服务 |
| :--- | :--- | :--- |
| `merged` | **PD合并模式**。单节点部署，同时处理prefill和decode。| `etcd`, `ds-worker`, `vLLM(kv_both)` |
| `primary` | **PD分离模式下的主节点**。负责prefill、服务发现和请求代理。| `etcd`, `ds-worker`, `vLLM(kv_prefill)`, `proxy-server` |
| `secondary` | **PD分离模式下的从节点**。负责decode。 | `ds-worker`, `vLLM(kv_decode)` |

## 7\. 部署场景示例

### 场景一：PD合并模式

**目标**：在单台机器（例如 `10.170.27.24`）上部署一个完整的服务。

1.  **配置**：打开 `deploy_env.sh`，确保 `MERGED_HOST_IP` 设置正确。
    ```bash
    # deploy_env.sh
    export MERGED_HOST_IP="10.170.27.24"
    ```
2.  **执行**：在该机器上运行以下命令启动服务。
    ```bash
    ./vllm_yuanrong_cli.sh start merged
    ```
3.  **检查**：
    ```bash
    ./vllm_yuanrong_cli.sh status merged
    ```
4.  **停止**：
    ```bash
    ./vllm_yuanrong_cli.sh stop merged
    ```

### 场景二：PD分离模式

**目标**：在两台机器上部署分离的服务。

  * 主节点 (Primary): `10.170.27.24`
  * 从节点 (Secondary): `10.170.27.144`

#### 步骤 1: 在主节点 (Primary) 上操作

1.  **配置**：打开 `deploy_env.sh`，确保 `PRIMARY_*` 和 `PROXY_*` 相关IP配置正确。
    ```bash
    # deploy_env.sh on Primary Node
    export PRIMARY_HOST_IP="10.170.27.24"
    export PROXY_PREFILL_HOST="10.170.27.24"
    export PROXY_DECODE_HOST="10.170.27.144" # 指向从节点
    ```
2.  **执行**：启动主节点服务。
    ```bash
    ./vllm_yuanrong_cli.sh start primary
    ```
3.  **检查**：
    ```bash
    ./vllm_yuanrong_cli.sh status primary
    ```

#### 步骤 2: 在从节点 (Secondary) 上操作

1.  **配置**：打开 `deploy_env.sh`，确保 `SECONDARY_*` 相关IP配置正确，**尤其是`SECONDARY_ETCD_IP_FOR_CLIENT`必须指向主节点**。
    ```bash
    # deploy_env.sh on Secondary Node
    export SECONDARY_HOST_IP="10.170.27.144"
    export SECONDARY_ETCD_IP_FOR_CLIENT="10.170.27.24" # 指向主节点
    ```
2.  **执行**：启动从节点服务。
    ```bash
    ./vllm_yuanrong_cli.sh start secondary
    ```
3.  **检查**：
    ```bash
    ./vllm_yuanrong_cli.sh status secondary
    ```

## 8\. 日志与排错

  * **vLLM 日志**：服务启动日志和运行状态默认输出到 `vllm_log.txt` 文件中。
  * **Proxy Server 日志**：代理服务日志默认输出到 `proxy_server_log.txt`。
  * **YuanRong 客户端日志**：日志目录由 `DATASYSTEM_CLIENT_LOG_DIR` 变量定义，默认为 `./client_logs`。
  * **权限问题**：如果执行时提示 "Permission denied"，请确保已使用 `chmod +x vllm_yuanrong_cli.sh` 命令授予脚本执行权限。
  * **命令未找到**：确保 `dscli`, `etcd`, `vllm` 等依赖的程序已正确安装并在系统的 `PATH` 环境变量中。