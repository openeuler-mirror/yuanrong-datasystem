# dscli命令行工具

<!-- TOC -->
- [环境准备](#环境准备)
    - [安装Python](#安装python)
    - [安装并部署ETCD](#安装并部署etcd)
    - [SSH互信配置](#ssh互信配置)
- [dscli安装教程](#dscli安装教程)
    - [pip安装](#pip安装)
    - [安装自定义版本](#安装自定义版本)
    - [源码编译安装](#源码编译安装)
- [dscli使用教程](#dscli使用教程)
    - [openYuanrong datasystem集群使用ETCD部署](#openyuanrong-datasystem集群使用etcd部署)
    - [openYuanrong datasystem集群使用Coordinator部署](#openyuanrong-datasystem集群使用coordinator部署)
    - [生成Helm Chart模板](#生成helm-chart模板)
    - [生成Cpp样例代码](#生成cpp样例代码)
    - [日志收集](#日志收集)
    - [多机命令运行](#多机命令运行)
- [命令行参数说明](#命令行参数说明)
    - [dscli start](#dscli-start)
    - [dscli stop](#dscli-stop)
    - [dscli up](#dscli-up)
    - [dscli down](#dscli-down)
    - [dscli runscript](#dscli-runscript)
    - [dscli generate_helm_chart](#dscli-generate_helm_chart)
    - [dscli generate_cpp_template](#dscli-generate_cpp_template)
    - [dscli generate_config](#dscli-generate_config)
    - [dscli collect_log](#dscli-collect_log)
    - [dscli query](#dscli-query)
- [配置项说明](#配置项说明)
    - [集群配置项](#集群配置项)
    - [命令行参数配置项](#命令行参数配置项)


<!-- /TOC -->

本文档介绍dscli集群管理工具的使用方法、命令行参数以及配置项说明。

## 环境准备

dscli 系统环境依赖如下：

| 软件名称 | 版本 | 作用 |
|-------|----|----|
| openEuler | 22.03 | 运行openYuanrong datasystem的操作系统 |
| [Python](#安装python) | 3.9-3.11 | openYuanrong datasystem dscli的使用依赖Python环境 |
| [ETCD](#安装并部署etcd) | 3.5 | 使用 ETCD 集群管理方式时的依赖组件；使用 Coordinator 或 Metastore 时无需部署 |
| [SSH互信配置](#ssh互信配置) |- | 仅多机部署需要，配置SSH互信用于机器间互相访问 |

下面给出以上依赖的安装方法。

### 安装Python

[Python](https://www.python.org/)可通过Conda进行安装。

安装Miniconda：
```bash
cd /tmp
curl -O https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-py311_23.10.0-1-Linux-$(arch).sh
bash Miniconda3-py311_23.10.0-1-Linux-$(arch).sh -b
cd -
. ~/miniconda3/etc/profile.d/conda.sh
conda init bash
```

安装完成后，可以为Conda设置清华源加速下载，参考[此处](https://mirrors.tuna.tsinghua.edu.cn/help/anaconda/)。

创建虚拟环境，以Python 3.11.4为例：

```bash
conda create -n py311 python=3.11.4 -y
conda activate py311
```

可以通过以下命令查看Python版本。

```bash
python --version
```

### 安装并部署ETCD

本节仅适用于使用 ETCD 进行集群管理的场景。使用 Coordinator 进行集群管理时无需安装或部署 ETCD，可跳过本节。

1. 下载 ETCD 二进制文件

    从 [ETCD GitHub Releases](https://github.com/etcd-io/etcd/releases) 下载最新版本的二进制包：

    ```bash
    ETCD_VERSION="v3.5.12"
    wget https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz
    ```

2. 解压并安装

    ```bash
    tar -xvf etcd-${ETCD_VERSION}-linux-amd64.tar.gz
    cd etcd-${ETCD_VERSION}-linux-amd64
    # 复制可执行文件到系统路径
    sudo cp etcd etcdctl /usr/local/bin/
    ```
3. 验证安装

    ```bash
    etcd --version
    etcdctl version
    ```

    如果能输出版本号说明安装成功。

4. 部署ETCD

    > **注意事项：**
    >
    > 此章节仅给出了最简单的ETCD部署方式，更多ETCD部署方式请参考[ETCD官网部署教程](https://etcd.io/docs/current/op-guide/clustering/)

    以部署一个端口号为 2379 的单节点ETCD为例，运行命令如下：

    ```bash
    etcd \
    --name etcd-single \
    --data-dir /tmp/etcd-data \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://0.0.0.0:2379 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --initial-advertise-peer-urls http://0.0.0.0:2380 \
    --initial-cluster etcd-single=http://0.0.0.0:2380 &
    ```

    参数说明：

    - --name：节点名称，单机可以随便命名。
    - --data-dir：数据存储目录。
    - --listen-client-urls：监听客户端请求的地址（0.0.0.0 允许所有 IP 访问）。
    - --advertise-client-urls：对外暴露的客户端访问地址。
    - --listen-peer-urls：监听其他 etcd 节点通信的 URL（集群内部端口）。
    - --initial-advertise-peer-urls：向其他节点宣告的访问地址（集群内部通信地址）。
    - --initial-cluster：初始集群成员列表（格式：节点名=节点peerURL）。

    部署完成之后通过 `etcdctl` 命令访问 ETCD 集群验证：

    ```bash
    etcdctl --endpoints "127.0.0.1:2379" put key "value"
    etcdctl --endpoints "127.0.0.1:2379" get key
    ```

    命令行操作能正常写入/读取数据说明部署ETCD成功。

### SSH互信配置

SSH互信可以让服务器之间无需密码即可登录，多机集群部署必须配置SSH互信，步骤如下：

1. 安装SSH服务

    确保所有需要配置互信的服务器上都已经安装并运行了SSH服务：

    ```bash
    sudo yum install openssh-server
    sudo systemctl start sshd
    sudo systemctl enable sshd
    ```

2. 生成SSH密钥对（在所有节点上执行）

    ```bash
    ssh-keygen -t rsa -b 4096
    ```

    按Enter接受默认位置（~/.ssh/id_rsa），然后设置空密码（直接按Enter两次）。

3. 将公钥复制到所有节点

    在主节点上查看公钥：

    ```bash
    cat ~/.ssh/id_rsa.pub
    ```

    将显示的公钥内容添加到所有节点的`~/.ssh/authorized_keys`文件中：

    ```bash
    mkdir -p ~/.ssh
    chmod 700 ~/.ssh
    touch ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
    echo "公钥内容" >> ~/.ssh/authorized_keys
    ```

4. 测试SSH互信

    从一个节点尝试SSH到其他节点，不需要输入密码：

    ```bash
    ssh username@hostname
    ```

    当无需密码能够正常跳转到其他节点时说明SSH互信配置成功。

## dscli安装教程

dscli命令集成在 openYuanrong datasystem 的 wheel 包中，因此在环境中安装 openYuanrong datasystem 完成后即可使用 dscli 命令行工具。

### pip安装

安装PyPI上的版本：

```bash
pip install openYuanrong-datasystem
```

### 安装自定义版本

指定好yr_datasystem以及配套的Python版本，运行如下命令安装openYuanrong datasystem包：

```bash
# 指定yr_datasystem版本为0.1
export version="0.1"
# 指定Python版本为3.11
export py_version="311"
pip install https://ms-release.obs.cn-north-4.myhuaweicloud.com/${version}/yr_datasystem/any/openyuanrong_datasystem-${version}-cp${py_version}-cp${py_version}-linux_x86_64.whl --trusted-host ms-release.obs.cn-north-4.myhuaweicloud.com -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 源码编译安装

源码编译安装 openYuanrong datasystem 详细说明请参考：[源码编译方式安装openYuanrong datasystem](../installation/build_guide/cmake_build.md#源码编译安装)。


## dscli使用教程

本节包含 dscli 的使用场景与教程。

### openYuanrong datasystem集群使用ETCD部署

本节介绍使用 ETCD 进行集群管理时的单机和多机部署方式。部署 Worker 前请确保 ETCD 集群可用，具体操作参见[安装并部署ETCD](#安装并部署etcd)。

#### 单机部署

openYuanrong datasystem 单机部署依赖 [dscli start](#dscli-start) 命令。

- 快速部署：

    [dscli start --worker_args](#dscli-start) 命令可在后面添加 datasystem_worker 初始化启动参数。使用 ETCD 时至少需要指定以下两个参数：

    | 命令行参数 | 类型 | 说明 |
    |-----------|------|------|
    | [--worker_address](#ipcrpc相关配置) | string | datasystem_worker IP 地址与监听端口号 |
    | [--etcd_address](#etcd相关配置) | string | ETCD 服务端访问地址 |

    ```bash
    dscli start --worker_args --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
    ```

    输出 OK 说明部署成功。默认情况下，datasystem_worker 最大可使用 1GB 共享内存空间用于缓存数据；如需调整，可以指定 `--shared_memory_size_mb`：

    ```bash
    dscli start --worker_args --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379" --shared_memory_size_mb 4096
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
    ```

    更多 datasystem_worker 参数说明参见[命令行参数配置项](#命令行参数配置项)。

- 通过配置项部署：

    在当前目录生成配置模板：

    ```bash
    dscli generate_config -o ./
    # [INFO] Configuration file cluster_config.json has been generated to /home/sn/cluster_config.json
    # [INFO] Configuration file worker_config.json has been generated to /home/sn/worker_config.json
    # [INFO] Configuration file coordinator_config.json has been generated to /home/sn/coordinator_config.json
    # [INFO] Configuration generation completed successfully
    ```

    修改 `worker_config.json`，配置 ETCD 地址，并确保 `coordinator_address` 和 `metastore_address` 为空：

    ```json
    {
      "worker_address": {
          "value": "127.0.0.1:31501",
          "description": "Address of worker in 'host:port' format (e.g., \"127.0.0.1:31501\"). The value cannot be empty."
      },
      "etcd_address": {
          "value": "127.0.0.1:2379",
          "description": "Address of ETCD server, (such as \"192.168.0.1:10001,192.168.0.2:10001,192.168.0.3:10001\")."
      }
    }
    ```

    启动 Worker：

    ```bash
    dscli start --worker_config_path ./worker_config.json
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
    ```

    输出 OK 说明部署成功。默认情况下，datasystem_worker 最大可使用 1GB 共享内存空间用于缓存数据；如需调整，可以修改 `shared_memory_size_mb`：

    ```json
    {
        "shared_memory_size_mb": {
            "value": "4096",
            "description": "Upper limit of the shared memory, the unit is mb, must be greater than 0."
        }
    }
    ```

    更多 `worker_config.json` 配置说明参见[命令行参数配置项](#命令行参数配置项)。

#### 单机卸载

openYuanrong datasystem 单机卸载依赖 [dscli stop](#dscli-stop) 命令。

- 卸载通过快速部署方式部署的 Worker：

    此种方式需要通过 `--worker_address` 命令行参数指定 datasystem_worker 的 IP 地址。

    ```bash
    dscli stop --worker_address "127.0.0.1:31501"
    # [INFO] [  OK  ] Stop worker service @ 127.0.0.1:31501 normally, PID: 38100
    ```

    输出OK说明卸载成功。

- 卸载通过配置项部署方式部署的 Worker：

    此种方式需要指定 `worker_config.json` 文件。

    ```bash
    dscli stop --worker_config_path ./worker_config.json
    # [INFO] [  OK  ] Stop worker service @ 127.0.0.1:31501 normally, PID: 38100
    ```

输出 OK 说明卸载成功。

#### 多机部署

- 通过执行 [dscli up](#dscli-up) 命令部署：

    1. 在当前目录生成配置模板：

        ```bash
        dscli generate_config -o ./
        # [INFO] Configuration file cluster_config.json has been generated to /home/sn/cluster_config.json
        # [INFO] Configuration file worker_config.json has been generated to /home/sn/worker_config.json
        # [INFO] Configuration file coordinator_config.json has been generated to /home/sn/coordinator_config.json
        # [INFO] Configuration generation completed successfully
        ```

    2. 编辑 `cluster_config.json`：

        ```json
        {
            // ssh互信
            "ssh_auth": {
                "ssh_private_key": "~/.ssh/id_rsa",
                "ssh_user_name": "sn"
            },
            // 配置datasystem_worker命令行参数配置项文件
            "worker_config_path": "./worker_config.json",
            "worker_nodes": [
                "127.0.0.1",
                "127.0.0.2"
            ],
            "worker_port": 31501
        }
        ```

        `cluster_config.json` 用于配置 SSH 互信和 Worker 部署节点，更多说明参见[集群配置项](#集群配置项)。

    3. 编辑 `worker_config.json`，配置所有 Worker 使用的 ETCD 地址，并确保 `coordinator_address` 和 `metastore_address` 为空：

        ```json
        {
            "etcd_address": {
                "value": "127.0.0.1:2379",
                "description": "Address of ETCD server, (such as \"192.168.0.1:10001,192.168.0.2:10001,192.168.0.3:10001\")."
            },
            "shared_memory_size_mb": {
                "value": "4096",
                "description": "Upper limit of the shared memory, the unit is mb, must be greater than 0."
            }
        }
        ```

        `worker_config.json` 用于配置 datasystem_worker 启动参数，更多说明参见[命令行参数配置项](#命令行参数配置项)。

        > **注意事项：**
        >
        > `cluster_config.json` 中的 `worker_nodes` 和 `worker_port` 会覆盖 `worker_config.json` 中的 `worker_address`。

    4. 部署 Worker 集群：

        ```bash
        dscli up -f ./cluster_config.json
        # [INFO] Start worker service @ 127.0.0.1:31501 success.
        # [INFO] Start worker service @ 127.0.0.2:31501 success.
        ```

        当输出如上信息时，说明集群部署成功。

    > **注意事项：**
    >
    > - 多机部署依赖节点间的 SSH 互信，具体操作参见[SSH互信配置](#ssh互信配置)。
    > - 所有待部署节点都需要安装 dscli，安装方法参见[dscli安装教程](#dscli安装教程)。

- 通过在多个节点执行 [dscli start](#dscli-start) 命令部署。以下示例在 127.0.0.1 和 127.0.0.2 部署两个 Worker：

    1. 在 127.0.0.1 节点执行：

        ```bash
        dscli start --worker_args --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
        # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
        ```

    2. 在 127.0.0.2 节点执行：

        ```bash
        dscli start --worker_args --worker_address "127.0.0.2:31501" --etcd_address "127.0.0.1:2379"
        # [INFO] [  OK  ] Start worker service @ 127.0.0.2:31501 success, PID: 38101
        ```

    > **注意事项：**
    >
    > - 所有 Worker 必须连接同一个 ETCD，即 `--etcd_address` 的值必须一致。
    > - 如果指定 `--cluster_name`，所有 Worker 的 `--cluster_name` 也必须一致。

#### 多机卸载

- 通过执行 [dscli down](#dscli-down) 命令卸载 Worker 集群：

    ```bash
    dscli down -f ./cluster_config.json
    # [INFO] Stop worker service @ 127.0.0.1:31501 success.
    # [INFO] Stop worker service @ 127.0.0.2:31501 success.
    ```

    当输出如上信息时说明集群卸载成功。

- 通过执行 [dscli stop](#dscli-stop) 命令分别停止 Worker：

    ```bash
    dscli stop --worker_address "127.0.0.1:31501"
    dscli stop --worker_address "127.0.0.2:31501"
    ```

当所有 Worker 均停止后，说明集群卸载成功。

#### 部署问题汇总

1. 默认路径过长导致 `dscli start` 执行失败

    `dscli start` 默认使用当前目录作为 home 目录。如果当前目录路径过长，可能导致 datasystem_worker 启动失败。

    使用 `--datasystem_home_dir`（短参数：`-d`）明确指定较短的 datasystem home 目录：

    ```bash
    dscli start --worker_config_path ~/worker_config.json --datasystem_home_dir /home/usr1/dscli
    dscli start --datasystem_home_dir /home/usr1/dscli --worker_args --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
    ```

### openYuanrong datasystem集群使用Coordinator部署

使用 Coordinator 进行集群管理时无需安装或部署 ETCD。部署 Worker 前，需要先启动一个所有 Worker 均可访问的 Coordinator。

#### Coordinator部署

Coordinator 支持快速部署和通过配置项部署两种方式。

- 快速部署：

    ```bash
    dscli start --coordinator_args --coordinator_address "127.0.0.1:31511"
    # [INFO] [  OK  ] Start coordinator service @ 127.0.0.1:31511 success, PID: 38100
    ```

    应在所有 Worker 停止后再停止 Coordinator：

    ```bash
    dscli stop --coordinator_address "127.0.0.1:31511"
    ```

- 通过配置项部署：

    在当前目录生成配置模板：

    ```bash
    dscli generate_config -o ./
    # [INFO] Configuration file cluster_config.json has been generated to /home/sn/cluster_config.json
    # [INFO] Configuration file worker_config.json has been generated to /home/sn/worker_config.json
    # [INFO] Configuration file coordinator_config.json has been generated to /home/sn/coordinator_config.json
    # [INFO] Configuration generation completed successfully
    ```

    按需修改 `coordinator_config.json` 中的 Coordinator 服务地址：

    ```json
    {
      "coordinator_address": {
        "value": "127.0.0.1:31511",
        "description": "Address of coordinator in 'host:port' format (e.g., \"127.0.0.1:31511\"). The value cannot be empty."
      }
    }
    ```

    启动 Coordinator：

    ```bash
    dscli start --coordinator_config_path ./coordinator_config.json
    # [INFO] [  OK  ] Start coordinator service @ 127.0.0.1:31511 success, PID: 38100
    ```

    应在所有 Worker 停止后，使用同一配置文件停止 Coordinator：

    ```bash
    dscli stop --coordinator_config_path ./coordinator_config.json
    ```

更多 Coordinator 配置说明参见[Coordinator配置项](#coordinator配置项)。

#### Coordinator 多节点部署

`dscli` 支持通过静态 peers 启动多个 Coordinator 并启用 Raft 选主。每个节点都需要独立启动一个 Coordinator 进程，要求：

- 所有 Coordinator 节点的 `coordinator_raft_initial_peers` 配置完全相同，格式为逗号分隔的 `host:port` 列表。
- 每个节点的 `coordinator_address` 必须是本机对其他 Coordinator 和 Worker 可访问的地址，并且必须包含在 `coordinator_raft_initial_peers` 中。
- 每个节点的 `coordinator_raft_data_dir` 必须是该节点独占的本地目录，不能在多个 Coordinator 进程之间共享。
- `coordinator_raft_initial_peers` 为空时按单节点无选主模式启动。
- Worker 侧 `coordinator_address` 仍配置为可访问的 Coordinator 地址；如果需要客户端/Worker 自动发现多个 Coordinator，应使用支持多地址的 Coordinator Discovery 接入方式。

三节点示例的静态 peers 列表如下：

```text
192.0.2.10:31511,192.0.2.11:31511,192.0.2.12:31511
```

节点 1 的 `coordinator_config.json` 关键配置示例：

```json
{
  "coordinator_address": {
    "value": "192.0.2.10:31511"
  },
  "coordinator_raft_initial_peers": {
    "value": "192.0.2.10:31511,192.0.2.11:31511,192.0.2.12:31511"
  },
  "coordinator_raft_data_dir": {
    "value": "./datasystem/coordinator_raft_node1"
  }
}
```

节点 2 和节点 3 使用相同的 `coordinator_raft_initial_peers`，但分别修改本机地址和 Raft 数据目录：

```json
{
  "coordinator_address": {
    "value": "192.0.2.11:31511"
  },
  "coordinator_raft_initial_peers": {
    "value": "192.0.2.10:31511,192.0.2.11:31511,192.0.2.12:31511"
  },
  "coordinator_raft_data_dir": {
    "value": "./datasystem/coordinator_raft_node2"
  }
}
```

```json
{
  "coordinator_address": {
    "value": "192.0.2.12:31511"
  },
  "coordinator_raft_initial_peers": {
    "value": "192.0.2.10:31511,192.0.2.11:31511,192.0.2.12:31511"
  },
  "coordinator_raft_data_dir": {
    "value": "./datasystem/coordinator_raft_node3"
  }
}
```

首次启动三节点 Coordinator 时，应在三个节点上并发或近同时执行启动命令，避免只启动单个节点后长时间等待：

```bash
dscli start --coordinator_config_path ./coordinator_config.json
```

停止时应先停止所有 Worker，再在每个 Coordinator 节点使用对应配置文件停止本机进程：

```bash
dscli stop --coordinator_config_path ./coordinator_config.json
```

> 注意：首次启动时，各节点应使用同一组静态 peers，并发或近同时完成启动。已有 Raft 数据目录的节点会优先从本地 Raft 状态恢复，不应通过删除 `coordinator_raft_data_dir` 强行重建成员关系。

#### 单机部署

Coordinator 启动后，可以单独启动 Worker，也可以使用一条命令依次启动 Coordinator 和 Worker。

- 快速部署 Worker：

    ```bash
    dscli start --worker_args --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38101
    ```

    默认情况下，datasystem_worker 最大可使用 1GB 共享内存空间；如需调整，可以指定 `--shared_memory_size_mb`：

    ```bash
    dscli start --worker_args --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511" --shared_memory_size_mb 4096
    ```

- 统一启动 Coordinator 和 Worker：

    如果尚未启动 Coordinator，可以通过一条命令先启动 Coordinator，再启动 Worker：

    ```bash
    dscli start --coordinator_worker_args --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"
    ```

    如果 Worker 启动失败，`dscli` 会停止本次命令已经启动的 Coordinator。

- 通过配置项部署 Worker：

    如果尚未生成配置模板，先执行 `dscli generate_config -o ./`。修改 `worker_config.json`，配置 Coordinator 地址，并确保 `etcd_address` 和 `metastore_address` 为空：

    ```json
    {
      "worker_address": {
        "value": "127.0.0.1:31501",
        "description": "Address of worker in 'host:port' format (e.g., \"127.0.0.1:31501\"). The value cannot be empty."
      },
      "coordinator_address": {
        "value": "127.0.0.1:31511",
        "description": "Address of datasystem coordinator service. Empty means coordinator mode is disabled."
      },
      "shared_memory_size_mb": {
        "value": "4096",
        "description": "Upper limit of the shared memory, the unit is mb, must be greater than 0."
      }
    }
    ```

    启动 Worker：

    ```bash
    dscli start --worker_config_path ./worker_config.json
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38101
    ```

更多 datasystem_worker 参数说明参见[命令行参数配置项](#命令行参数配置项)。

#### 单机卸载

- 通过地址停止 Worker 和 Coordinator。两个地址同时指定时，`dscli` 会先停止 Worker，再停止 Coordinator：

    ```bash
    dscli stop --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"
    ```

- 或分别停止（需要先停止 Worker，再停止 Coordinator ）：

    ```bash
    dscli stop --worker_address 127.0.0.1:31501
    dscli stop --coordinator_address 127.0.0.1:31511
    ```

- 使用配置文件时，先停止 Worker，再停止 Coordinator：

    ```bash
    dscli stop --worker_config_path ./worker_config.json
    dscli stop --coordinator_config_path ./coordinator_config.json
    ```

当 Worker 和 Coordinator 均停止后，说明单机集群卸载成功。

#### 多机部署

多机部署只批量启动 Worker。请先按照[Coordinator部署](#coordinator部署)启动一个所有 Worker 均可访问的 Coordinator。

- 通过执行 [dscli up](#dscli-up) 命令部署：

    1. 如果尚未生成配置模板，执行：

        ```bash
        dscli generate_config -o ./
        # [INFO] Configuration file cluster_config.json has been generated to /home/sn/cluster_config.json
        # [INFO] Configuration file worker_config.json has been generated to /home/sn/worker_config.json
        # [INFO] Configuration file coordinator_config.json has been generated to /home/sn/coordinator_config.json
        # [INFO] Configuration generation completed successfully
        ```

    2. 编辑 `cluster_config.json`：

        ```json
        {
            "ssh_auth": {
                "ssh_private_key": "~/.ssh/id_rsa",
                "ssh_user_name": "sn"
            },
            "worker_config_path": "./worker_config.json",
            "worker_nodes": [
                "127.0.0.1",
                "127.0.0.2"
            ],
            "worker_port": 31501
        }
        ```

    3. 编辑 `worker_config.json`，配置所有 Worker 使用的 Coordinator 地址，并确保 `etcd_address` 和 `metastore_address` 为空：

        ```json
        {
            "coordinator_address": {
                "value": "127.0.0.1:31511",
                "description": "Address of datasystem coordinator service. Empty means coordinator mode is disabled."
            }
        }
        ```

        > **注意事项：**
        >
        > `cluster_config.json` 中的 `worker_nodes` 和 `worker_port` 会覆盖 `worker_config.json` 中的 `worker_address`。

    4. 部署 Worker 集群：

        ```bash
        dscli up -f ./cluster_config.json
        # [INFO] Start worker service @ 127.0.0.1:31501 success.
        # [INFO] Start worker service @ 127.0.0.2:31501 success.
        ```

    > **注意事项：**
    >
    > - `dscli up` 只部署 Worker，不会启动 Coordinator。
    > - 多机部署依赖节点间的 SSH 互信，具体操作参见[SSH互信配置](#ssh互信配置)。
    > - 所有待部署节点都需要安装 dscli，安装方法参见[dscli安装教程](#dscli安装教程)。

- 通过在多个节点执行 [dscli start](#dscli-start) 命令部署。以下示例在 127.0.0.1 和 127.0.0.2 部署两个 Worker：

    ```bash
    # 在 127.0.0.1 节点执行
    dscli start --worker_args --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"

    # 在 127.0.0.2 节点执行
    dscli start --worker_args --worker_address "127.0.0.2:31501" --coordinator_address "127.0.0.1:31511"
    ```

    所有 Worker 必须连接同一个 Coordinator。如果指定 `--cluster_name`，所有 Worker 的 `--cluster_name` 也必须一致。

#### 多机卸载

卸载时先停止所有 Worker，最后停止 Coordinator。

- 通过执行 [dscli down](#dscli-down) 命令停止 Worker，然后停止 Coordinator：

    ```bash
    dscli down -f ./cluster_config.json
    dscli stop --coordinator_address "127.0.0.1:31511"
    ```

- 通过地址分别停止 Worker，然后停止 Coordinator：

    ```bash
    dscli stop --worker_address "127.0.0.1:31501"
    dscli stop --worker_address "127.0.0.2:31501"
    dscli stop --coordinator_address "127.0.0.1:31511"
    ```

当所有 Worker 和 Coordinator 均停止后，说明集群卸载成功。

#### 部署问题汇总

1. 默认路径过长导致 `dscli start` 执行失败

    `dscli start` 默认使用当前目录作为 home 目录。如果当前目录路径过长，可能导致 datasystem_worker 启动失败。

    使用 `--datasystem_home_dir`（短参数：`-d`）明确指定较短的 datasystem home 目录：

    ```bash
    dscli start --worker_config_path ~/worker_config.json --datasystem_home_dir /home/usr1/dscli
    dscli start --datasystem_home_dir /home/usr1/dscli --worker_args --worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"
    ```

### 生成Helm Chart模板

通过 [dscli generate_helm_chart](#dscli-generate_helm_chart) 命令在当前目录生成 openYuanrong datasystem 的 Helm Chart 包：

```bash
dscli generate_helm_chart
# [INFO] Helm chart generated successfully at: /home/sn/datasystem-helm-chart
# [INFO] You can now use this chart for deployment with Helm.
```

可通过 `datasystem-helm-chart/datasystem/values.yaml` 配置 openYuanrong datasystem Kubernetes 部署所需的配置项，Kubernetes 配置项详细说明请参考：[openYuanrong datasystem Kubernetes配置项](./k8s_configuration.md)。

配置完成后即可通过如下命令拉起 openYuanrong datasystem 集群：

```bash
helm install yr_datasystem /home/sn/datasystem-helm-chart/datasystem
```

卸载集群命令：

```bash
helm uninstall yr_datasystem
```

更多 openYuanrong datasystem Kubernetes 部署详细信息请参考：[openYuanrong datasystem Kubernetes部署](../deployment/deploy.md#kubernetes部署worker)。

更多关于 dscli generate_helm_chart 命令的使用请参考：[dscli generate_helm_chart](#dscli-generate_helm_chart)。

### 生成Cpp样例代码

通过 [dscli generate_cpp_template](#dscli-generate_cpp_template) 命令可快速生成 openYuanrong datasystem 的 C++ 样例代码：

```bash
dscli generate_cpp_template
# [INFO] C++ code template generated successfully at: /home/sn/datasystem-helm-chart/cpp_template
# [INFO] You can now use this template for your C++ development.
```

`cpp_template` 目录中包含以下文件/目录：
```text
cpp_template
├── CMakeLists.txt          // CMake配置文件
├── include                 // openYuanrong datasystem SDK头文件
├── kv_cache_example.cpp    // 样例代码
├── lib                     // openYuanrong datasystem SDK动态库及其依赖库
├── README.md               // 说明文档
└── run.sh                  // 编译&运行脚本
```

编译并运行样例代码：

```bash
bash run.sh <worker_address>
```

运行前需要先拉起 openYuanrong datasystem 集群。

### 日志收集

通过 [dscli collect_log](#dscli-collect_log) 命令可快速收集集群日志：

```bash
dscli collect_log --cluster_config_path ./cluster_config.json
```

更多关于 dscli collect_log 命令的使用请参考：[dscli collect_log](#dscli-collect_log)。

### 多机命令运行

通过 [dscli runscript](#dscli-runscript) 命令可快速在多个节点上执行脚本命令。以在多个机器上安装 dscli 命令行工具为例：

1. 编写数据系统安装脚本：
    ```bash
    cat << EOF > install.sh
    conda create --name python39
    conda activate python39
    pip install openyuanrong-datasystem
    EOF
    ```

2. 设置集群配置信息 `cluster_config.json`，可通过 [dscli generate_config](#dscli-generate_config) 命令生成并修改，其中 `worker_nodes` 即为要执行脚本的机器：

    ```json
    {
        "worker_nodes" : ["127.0.0.1", "127.0.0.2"],
        "ssh_auth": {
            "ssh_user_name": "sn",
            "ssh_private_key": "~/.ssh/id_rsa"
            }
    },
    ```

3. 使用 dscli runscript 命令执行脚本：

    ```bash
    dscli runscript -f ./cluster_config.json install.sh
    ```

更多关于 dscli runscript 命令的使用请参考：[dscli runscript](#dscli-runscript)。


## 命令行参数说明

本节包含用于管理 openYuanrong datasystem 集群的命令。

### dscli start

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--timeout &lt;SECONDS&gt;| -t &lt;SECONDS&gt; | 等待服务就绪的最大时间（默认：90秒） |
|--worker_config_path &lt;FILE&gt; / --config_path &lt;FILE&gt;|-W &lt;FILE&gt; / -f &lt;FILE&gt;| 使用 Worker 配置文件（JSON格式）启动 Worker，配置文件可通过 generate_config 命令生成 |
|--coordinator_config_path &lt;FILE&gt;|-C &lt;FILE&gt;| 使用 Coordinator 配置文件（JSON格式）启动 Coordinator，配置文件可通过 generate_config 命令生成 |
|--worker_args &lt;...&gt;        |-w &lt;...&gt; | 使用参数启动数据系统worker, 以 "--<Args>  <Value>"为格式。比如--worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"<br>**注意**：此选项必须是命令行的最后一个参数选项，其后的所有内容都会被解析为 worker 参数|
|--coordinator_args &lt;...&gt;|-c &lt;...&gt;| 使用参数启动coordinator，比如--coordinator_address "127.0.0.1:31511"。此选项必须是命令行的最后一个参数选项 |
|--coordinator_worker_args &lt;...&gt;|-a &lt;...&gt;| 先启动coordinator再启动worker，比如--worker_address "127.0.0.1:31501" --coordinator_address "127.0.0.1:31511"。此选项必须是命令行的最后一个参数选项。参数会按 `coordinator_config.json` 和 `worker_config.json` 分别过滤后传给对应进程 |
|--datasystem_home_dir |-d &lt;DIR&gt; | 指定基础路径，将配置文件中的相对路径转换为绝对路径。例如当配置中包含 './yr_datasystem/log_dir'，其中的 '.' 将被替换为 `datasystem_home_dir` 的值 |
|--cpunodebind|-N | numactl选项，仅允许进程在指定 NUMA 节点所属的 CPU 上运行，支持多个节点 |
|--physcpubind|无 | 按物理 CPU 编号将进程绑定到指定核心 |
|--interleave|-i | 设置内存交错策略，按编号顺序在指定 NUMA 节点间轮询分配页面 |
|--preferred|-p | 设定优先 NUMA 节点。内核首先尝试在该节点分配内存；若内存不足，则退至其他节点 |
|--membind|-m | 强制仅允许从指定 NUMA 节点分配内存；若这些节点内存不足，分配将失败  |
|--localalloc|-l | 将内存分配限制在当前 CPU 所在的 NUMA 节点（本地节点），若本地节点内存不足，内核会退至邻近节点 |
|--enable_ums| 无 | 启用ums后，datasystem worker之间的rpc消息将通过 ub 传输 |
|--jemalloc_prof_conf &lt;CONF&gt;|无|设置 Worker 子进程的 jemalloc profiling 配置。仅适用于使用 `build.sh -x on` 构建的安装包，且必须位于 `--worker_args`、`--coordinator_args` 或 `--coordinator_worker_args` 之前|

常用启动方式的短参数和长参数等价示例如下：

```bash
# 使用命令行参数启动 Worker
dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
dscli start --worker_args --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"

# 使用配置文件启动 Worker
dscli start -W worker_config.json
dscli start --worker_config_path worker_config.json
dscli start -f worker_config.json
dscli start --config_path worker_config.json

# 使用命令行参数启动 Coordinator
dscli start -c --coordinator_address "127.0.0.1:31511"
dscli start --coordinator_args --coordinator_address "127.0.0.1:31511"

# 使用配置文件启动 Coordinator
dscli start -C coordinator_config.json
dscli start --coordinator_config_path coordinator_config.json
```

> **配置文件启动注意事项**：
>
> - `dscli start --worker_config_path` / `dscli start --config_path` 使用 Worker 配置文件启动 Worker。
> - `dscli start --coordinator_config_path` 使用 Coordinator 配置文件启动 Coordinator。
> - 配置文件中不再需要 `service_type` 字段；服务类型由 `dscli start` 的启动选项决定。

> **绑核配置项注意事项**：
>
> - 使用绑核功能前请确保机器上已安装numactl命令。
> - 绑核配置项（cpunodebind，physcpubind，interleave，preferred，membind，localalloc）需要位于 `--worker_args`、`--coordinator_args` 或 `--coordinator_worker_args` 参数之前。
>
> 例子：
> ```bash
> # 绑定到 NUMA 节点 0 的 CPU，并优先在 NUMA 节点 1 分配内存
> dscli start --cpunodebind 0 --preferred 1 --worker_args --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
>
> # 在 NUMA 节点 0-7 间交错分配内存（多 NUMA 节点、大共享内存场景，如 KV Pool）
> dscli start --interleave 0-7 --worker_args --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
> ```
>
> `--interleave` 的取值遵循 numactl 节点语法，支持单个节点（`0`）、列表（`0,1,2`）、范围（`0-7`）以及 `all`。跨全部 NUMA 节点交错分配可避免大块共享内存集中占用单个节点导致内存不均，建议在 Worker 共享内存较大（如 `shared_memory_size_mb` 取值较高）时使用。
>
> **启用ums注意事项**：
>
> - 启用ums功能请确保机器已安装ums相关rpm包
> - 配置项 enable_ums 需要位于 `--worker_args` 或 `--coordinator_worker_args` 参数之前。
> ums安装参考
> ```bash
> yum install umdk-ums.aarch64
> yum install umdk-ums-tools.aarch64
> modprobe ums
> ```
>
> 例子：
> ```bash
> dscli start --enable_ums --worker_args --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
> ```

> **jemalloc profiling 注意事项**：
>
> - `--jemalloc_prof_conf` 是 dscli 参数，不是 Worker 启动参数，因此不写入 `worker_config.json`。
> - 该参数只能用于启动 Worker，并要求安装包通过 `build.sh -x on` 构建。普通构建传入该参数会在 Worker 启动前报错并提示重新构建。
> - dscli 会自动补充 `prof:true`。如果未指定 `prof_prefix`，默认前缀为 `<log_dir>/jemalloc/datasystem_worker`。
> - 显式指定 `prof_prefix` 时完全使用该前缀，不再使用默认日志路径。`prof_prefix` 是文件名前缀而不是目录，例如 `prof_prefix:/data/heap/worker` 会生成 `/data/heap/worker.<pid>...heap`。
> - dscli 不会自动配置 `prof_final`、`prof_gdump` 或 `lg_prof_interval`。heap 文件也不参与日志轮转、压缩或清理，用户需要自行选择 dump 策略并管理磁盘空间。
>
> 例子：
> ```bash
> # heap 文件默认写入 log_dir/jemalloc
> dscli start --jemalloc_prof_conf "prof_final:true,lg_prof_sample:20" -W worker_config.json
>
> # 使用用户指定的文件名前缀
> dscli start --jemalloc_prof_conf "prof_final:true,prof_prefix:/data/heap/worker" -W worker_config.json
> ```



### dscli stop

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--worker_config_path &lt;FILE&gt;|-W &lt;FILE&gt;|--config_path &lt;FILE&gt;|-f &lt;FILE&gt;| 通过使用 Worker 配置文件（JSON格式）仅停止 Worker |
|--coordinator_config_path &lt;FILE&gt;|-C &lt;FILE&gt;| 通过使用 Coordinator 配置文件（JSON格式）仅停止 Coordinator |
|--worker_address <ADDR>    |-w &lt;...&gt; | 通过指定worker地址（IP:PORT格式，如127.0.0.1:31501）来停止worker |
|--coordinator_address <ADDR>|无| 通过指定coordinator地址（IP:PORT格式，如127.0.0.1:31511）来停止coordinator。与 `--worker_address` 同时指定时会先停止worker，再停止coordinator |
|--timeout &lt;SECONDS&gt;|-t &lt;SECONDS&gt;| 覆盖动态计算的停止超时时间（有效范围：大于零）。适用于已知数据已迁移完成或测试环境无数据的快速停止场景 |

常用停止方式的短参数和长参数等价示例如下：

```bash
# 通过地址停止 Worker
dscli stop -w 127.0.0.1:31501
dscli stop --worker_address 127.0.0.1:31501

# 通过地址停止 Coordinator
dscli stop -c 127.0.0.1:31511
dscli stop --coordinator_address 127.0.0.1:31511

# 同时停止 Coordinator 和 Worker
dscli stop --worker_address 127.0.0.1:31501 --coordinator_address 127.0.0.1:31511

# 通过配置文件停止 Worker
dscli stop -f worker_config.json
dscli stop --config_path worker_config.json
dscli stop -W worker_config.json
dscli stop --worker_config_path worker_config.json

# 通过配置文件停止 Coordinator
dscli stop -C coordinator_config.json
dscli stop --coordinator_config_path coordinator_config.json

# 指定超时时间快速停止（跳过动态超时计算）
dscli stop -w 127.0.0.1:31501 -t 10
dscli stop --worker_address 127.0.0.1:31501 --timeout 10
```

`dscli stop` 会先向服务进程发送 `SIGTERM`，等待超时后再发送 `SIGKILL`。Coordinator 的等待默认超时时间为180秒，Worker 等待超时时间默认按以下公式动态计算（单位：秒）：

`180 + shared_memory_size_mb / data_migrate_rate_limit_mb`

可通过 `-t`/`--timeout` 参数覆盖此动态超时。

### dscli up

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--timeout &lt;SECONDS&gt;| -t &lt;SECONDS&gt; | 等待 worker 服务就绪的最大时间（默认：90秒） |
|--cluster_config_path &lt;FILE&gt;|-f &lt;FILE&gt;| 指定集群配置文件的路径，配置文件（JSON格式）可通过generate_config生成 |
|--datasystem_home_dir &lt;DIR&gt;  |-d &lt;DIR&gt; | 替换配置文件中当前路径的目录为`datasystem_home_dir`的值 |
|--cpunodebind|-N | 仅允许进程在指定 NUMA 节点所属的 CPU 上运行，支持多个节点 |
|--physcpubind|-C | 按物理 CPU 编号将进程绑定到指定核心 |
|--interleave|-i | 设置内存交错策略，按编号顺序在指定 NUMA 节点间轮询分配页面 |
|--preferred|-p | 设定优先 NUMA 节点。内核首先尝试在该节点分配内存；若内存不足，则退至其他节点 |
|--membind|-m | 强制仅允许从指定 NUMA 节点分配内存；若这些节点内存不足，分配将失败 |
|--localalloc|-l | 将内存分配限制在当前 CPU 所在的 NUMA 节点（本地节点），若本地节点内存不足，内核会退至邻近节点 |
|--enable_ums| 无 | 启用ums后，datasystem worker之间的rpc消息将通过 ub 传输 |
|--jemalloc_prof_conf &lt;CONF&gt;|无|将 jemalloc profiling 配置转发到每个远端 Worker；每个节点都必须使用 `build.sh -x on` 构建的安装包|
|--metastore_head_node <NODE_IP> | 无 | 指定<NODE_IP>节点的worker启动Metastore取代ETCD |


> **绑核配置项注意事项**：
>
> - 使用绑核功能前请确保机器上已安装numactl命令。
>
> 例子：
> ```bash
> # 启动集群并绑核
> dscli up --cpunodebind 0 --preferred 1 -f ./cluster_config.json
>
> # 在 NUMA 节点 0-7 间交错分配内存启动集群（多 NUMA 节点、大共享内存场景，如 KV Pool）
> dscli up --interleave 0-7 -f ./cluster_config.json
> ```
>
> `--interleave` 的取值遵循 numactl 节点语法，支持单个节点（`0`）、列表（`0,1,2`）、范围（`0-7`）以及 `all`。跨全部 NUMA 节点交错分配可避免大块共享内存集中占用单个节点导致内存不均，建议在 Worker 共享内存较大（如 `shared_memory_size_mb` 取值较高）时使用。
>
> **启用ums注意事项**：
>
> - 启用ums功能请确保机器已安装ums相关rpm包
>
> 例子：
> ```bash
> dscli up --enable_ums -f ./cluster_config.json
> ```

> `dscli up --jemalloc_prof_conf <CONF>` 会把同一配置安全转发到每个远端 `dscli start`。是否支持 profiling 由各远端安装包独立检查。默认和显式 `prof_prefix` 的规则与 `dscli start` 相同。

### dscli down

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--cluster_config_path &lt;FILE&gt; |-f &lt;FILE&gt; | 指定集群配置文件的路径 |

### dscli runscript

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--cluster_config_path &lt;FILE&gt; |-f &lt;FILE&gt; | 集群配置文件（JSON格式）的路径 |

### dscli generate_helm_chart

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--output_path &lt;OUTPUT_PATH&gt; |-o &lt;OUTPUT_PATH&gt; | 指定生成helm chart的存放路径，默认存放路径为当前目录 |

### dscli generate_cpp_template

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--output_path &lt;OUTPUT_PATH&gt; |-o &lt;OUTPUT_PATH&gt; | 指定生成C++代码模板文件的存放路径，默认存放路径为当前目录 |

### dscli generate_config

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--output_path &lt;OUTPUT_PATH&gt; |-o &lt;OUTPUT_PATH&gt; | 指定生成配置文件的存放路径，默认存放路径为当前目录。会生成 `cluster_config.json`、`worker_config.json` 和 `coordinator_config.json` |

### dscli collect_log

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--cluster_config_path &lt;FILE&gt; |-f &lt;FILE&gt; | 集群配置文件（JSON）的路径                                           |
|--log_path &lt;PATH&gt;            |-d &lt;PATH&gt; | 对于每个需要采集日志的集群节点，指定存放日志的远程路径                  |
|--output_path &lt;...&gt;          |-o &lt;...&gt;  | 存储日志的本地目录，默认路径为当前目录下名为'log_\<timestamp\>'的文件夹 |

### dscli query

`dscli query` 通过只读接口查询协调后端，并在命令行进程本地完成拓扑解码、健康状态派生、
哈希区间计算和 key 路由。命令不会读取 `worker_config.json`，也不会自动探测后端类型；
用户必须显式传入 ETCD 或 Coordinator 地址。
Coordinator 使用与服务部署相同的基础地址，查询客户端经 brpc 传输与 Coordinator 通信。
`--coordinator_address` 支持逗号分隔的 `host:port` 列表，用于多实例 Coordinator 部署场景；查询客户端会经由
Coordinator Discovery 解析全部地址并路由到当前 Leader，格式与 Worker 侧 `coordinator_address` 一致。
整条命令使用固定 5 秒超时，stdout 只输出一个 JSON 对象。
当前 ETCD 查询仅支持未启用认证和 TLS 的集群。

查询 ETCD 集群节点：

```bash
dscli query cluster \
  --etcd_address 192.0.2.10:2379,192.0.2.11:2379 \
  --cluster_name cluster-a
```

查询多实例 Coordinator 部署的集群节点（传入全部 Coordinator 节点地址）：

```bash
dscli query cluster \
  --coordinator_address 192.0.2.10:31511,192.0.2.11:31511,192.0.2.12:31511 \
  --cluster_name cluster-a
```

成功输出示例：

```json
{
  "schema_version": "1.0",
  "cluster_name": "cluster-a",
  "status": "OK",
  "topology_version": 7,
  "nodes": [
    {
      "worker_address": "192.0.2.20:31501",
      "health": "HEALTHY",
      "state": "ACTIVE",
      "hash_ranges": ["(3000000000,1000000000]"]
    },
    {
      "worker_address": "192.0.2.21:31501",
      "health": "DRAINING",
      "state": "PRE_LEAVING",
      "hash_ranges": ["(1000000000,3000000000]"]
    }
  ]
}
```

哈希区间采用左开右闭 `(start,end]`。当 `start > end` 时表示跨越 `UINT32_MAX` 到 `0` 的环回区间；
单 token 全环固定输出 `[0,4294967295]`。`INITIAL`、`JOINING` 和 `FAILED` 节点不拥有 committed 区间。

查询一批 key 当前路由到的 Worker（多实例 Coordinator 部署时传入全部节点地址，客户端自动路由到 Leader）：

```bash
dscli query route \
  --coordinator_address 192.0.2.10:31511,192.0.2.11:31511,192.0.2.12:31511 \
  --keys key-1 key-2 key-1
```

成功输出按 Worker 地址分组，同一组内保留输入顺序和重复 key：

```json
{
  "schema_version": "1.0",
  "cluster_name": "",
  "status": "OK",
  "topology_version": 7,
  "routes": [
    {
      "worker_address": "192.0.2.20:31501",
      "keys": ["key-1", "key-1"],
      "health": "HEALTHY"
    },
    {
      "worker_address": "192.0.2.21:31501",
      "keys": ["key-2"],
      "health": "HEALTHY"
    }
  ]
}
```

一次最多 100000 个 key，单 key 最大 1 MiB，总 key 数据最大 4 MiB。

失败时不输出不完整的节点或路由，只输出最小错误对象并返回退出码 1：

```json
{
  "schema_version": "1.0",
  "cluster_name": "",
  "status": "RPC unavailable",
  "error": "failed to connect to coordination backend"
}
```

查询不会猜测或输出部分节点、部分路由。只有 `status=OK` 且退出码为 0 的结果可用于自动化。

## 配置项说明

### 集群配置项

集群相关配置项位于 `cluster_config.json` 文件中，如下表所示：

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| ssh_auth.ssh_private_key | string | `"~/.ssh/id_rsa"` | 否 | 用于免密登录远程服务器的私钥文件 |
| ssh_auth.ssh_user_name | string | `"root"` | 否 | SSH登录时的远程用户名 |
| worker_config_path | string | `"./worker_config.json"` | 否 | datasystem_worker配置项路径 |
| worker_nodes | list | `"127.0.0.1"` | 否 | 部署openYuanrong datasystem的节点 |
| worker_port | int | `31501` | 否 | datasystem_worker占用的端口号 |

### 命令行参数配置项

服务启动配置项分别位于 `worker_config.json` 和 `coordinator_config.json`。配置文件只包含对应服务进程的启动参数；服务类型由 `dscli start` 的启动选项决定，例如使用 `--worker_config_path` 启动 Worker，使用 `--coordinator_config_path` 启动 Coordinator。

#### Coordinator配置项

`coordinator_config.json` 包含 Coordinator 启动参数。可以使用 `dscli start --coordinator_config_path ./coordinator_config.json` 启动，
使用 `dscli stop --coordinator_config_path ./coordinator_config.json` 停止。

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| coordinator_address | string | `"127.0.0.1:31511"` | 否 | Coordinator 服务地址，格式为 `host:port`，不能为空 |
| coordinator_rpc_stub_cache_size | int | `4096` | 否 | Coordinator RPC Stub 缓存数量上限 |
| coordinator_topology_max_active_clusters | int | `8` | 否 | Coordinator 同时承载的集群拓扑上限，取值范围为 `[2, 32]`；超限时拒绝新集群准入，修改后需重启 Coordinator |
| coordinator_raft_initial_peers | string | `""` | 否 | Coordinator Raft 静态初始成员列表，格式为逗号分隔的 `host:port`。为空时按单节点无选主模式启动；配置多个成员时启用静态 peers Raft 选主，各节点必须配置相同列表 |
| coordinator_raft_data_dir | string | `"./datasystem/coordinator_raft"` | 否 | Coordinator本地braft状态目录；启用选主时不能为空，且每个Coordinator节点必须独占一个目录 |
| coordinator_raft_heartbeat_interval_ms | int | `100` | 否 | Raft Leader发送心跳的时间间隔，单位为毫秒；取值范围为`[10, 10000]` |
| coordinator_raft_election_timeout_ms | int | `1000` | 否 | Raft选举超时时间，单位为毫秒；必须是`coordinator_raft_heartbeat_interval_ms`的整数倍，且倍数在`[5, 10]`范围内 |
| coordinator_member_failure_grace_ms | uint32 | `10000` | 否 | 成员持续失败宽限时间，单位为毫秒；必须大于内部健康检查间隔 |
| coordinator_discovery_retry_interval_ms | uint32 | `5000` | 否 | Discovery候选重试的最小间隔，单位为毫秒；必须大于`0` |
| watch_event_dispatch_thread | int | `4` | 否 | Coordinator 分发 Watch 事件的线程数 |
| rpc_thread_num | int | `64` | 否 | Coordinator RPC 服务线程数 |
| log_dir | string | `"./datasystem/logs"` | 否 | Coordinator 日志目录 |
| log_filename | string | `"datasystem_coordinator"` | 否 | Coordinator 日志文件名前缀，非空时仅允许英文字母、数字和下划线 |
| minloglevel | int | `0` | 是 | 最低日志级别，低于该级别的日志不会被记录 |
| log_async_queue_size | int | `2048` | 否 | 异步日志消息队列最大容量 |
| max_log_size | int | `400` | 否 | 单个日志文件最大大小，单位为 MB |
| max_log_file_num | int | `25` | 是 | 每个日志级别最多保留的日志文件数；为 `0` 时不限制数量 |
| log_retention_day | int | `0` | 否 | 日志保留天数；为 `0` 时不按时间删除日志 |
| log_async | bool | `true` | 否 | 是否异步写入日志文件 |
| log_compress | bool | `false` | 是 | 是否将滚动生成的历史日志压缩为 gzip 格式 |
| logbufsecs | int | `0` | 否 | 日志消息最大缓冲时长，单位为秒 |
| logfile_mode | int | `416` | 否 | 日志文件模式/权限 |
| log_only_write_info_file | bool | `true` | 否 | 是否只生成 INFO 日志文件；INFO 文件始终包含所有级别的日志 |
| log_monitor | bool | `true` | 是 | 是否开启 Coordinator 指标摘要；当 JSON exporter 未启用或未创建时，摘要输出到 Coordinator INFO 作为 fallback |
| json_log_monitor | bool | `true` | 是 | 是否将 Coordinator 指标摘要输出到 `kv_metrics.log` JSON-Lines 文件。开启且 exporter 创建成功后，每周期完整记录，Coordinator INFO 不再重复输出 `metrics_summary` |
| log_monitor_interval_ms | int | `10000` | 否 | Coordinator 指标摘要的采集和输出间隔，单位为毫秒 |
| brpc_server_num_threads | int | `64` | 否 | brpc Server 工作线程数 |
| brpc_max_concurrency | int | `128` | 否 | 每个 brpc Server 允许的最大并发 RPC 数；为 `0` 时不限制，且不能小于 `brpc_server_num_threads` |
| request_sample_rate | double | `1.0` | 是 | 请求日志主采样率，取值范围为 `[0.0, 1.0]` |
| access_sample_rate | double | `1.0` | 是 | Access 日志补采样率，取值范围为 `[0.0, 1.0]` |
| diagnostic_sample_rate | double | `1.0` | 是 | Diagnostic 日志补采样率，取值范围为 `[0.0, 1.0]` |

Coordinator 日志和采样配置的详细语义参见[日志与可观测相关配置](#日志与可观测相关配置)。

`coordinator_raft_initial_peers` 为空时，Coordinator 按单节点无选主模式启动；配置多个静态 peers 时，
Coordinator 按该成员列表启动 Raft 选主。启用选主后，`coordinator_raft_data_dir`、
`coordinator_raft_heartbeat_interval_ms`、`coordinator_raft_election_timeout_ms`、
`coordinator_member_failure_grace_ms` 和 `coordinator_discovery_retry_interval_ms` 用于配置本节点 Raft 状态目录
和选举时序。配置 `coordinator_raft_initial_peers` 是启用选主的必要条件。

生产部署中，一个 Coordinator 配置文件对应一个 Coordinator 进程。多节点部署时，每个节点应使用自己的
`coordinator_address` 和独占的 `coordinator_raft_data_dir`，并配置相同的 `coordinator_raft_initial_peers`。配置
文件解析或参数校验失败时，Coordinator 启动失败；修改 Raft 成员、地址或数据目录类参数后，应停止相关节点并
按部署规划重新启动。

`worker_config.json` 包含 datasystem_worker 的命令行参数相关配置项。启动 Worker 时，必须在
`coordinator_address`、`etcd_address`、`metastore_address` 中配置且仅配置一个集群管理后端。

> **注意事项：**
>
> 单机快速部署 `dscli start --worker_args` 该命令支持直接传入 datasystem_worker 的参数进行配置。表格中的配置项需要映射为命令行长参数。以配置项 `max_client_num` 为例，如需在启动时配置该项，应当写为 `dscli start --worker_args --max_client_num 200`。

#### 资源相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| max_client_num | int | `200` | 否 | openYuanrong datasystem单个DaemonSet可同时接入的共享内存客户端数量上限；仅对走共享内存（SHM）路径的客户端生效 |
| shared_memory_size_mb | int | `1024` | 否 | openYuanrong datasystem单个DaemonSet可使用的共享内存资源大小（以MB为单位） |

#### IPC/RPC相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| ipc_through_shared_memory | bool | `true` | 否 | datasystem-worker共享内存启用开关 |
| shared_memory_worker_port | int | `0` | 否 | 指定用于共享内存 FD 传输的 SCMTCP 监听端口；当值为非 0 时，datasystem-worker 会在该端口监听连接 ，要求内核支持 SCMTCP 且端口未被占用，否则 worker 启动失败。客户端与 worker 在同一节点时，客户端会使用该通道传递共享内存描述符。 |
| unix_domain_socket_dir | string | `"./datasystem/uds"` | 否 | 配置 Unix Domain Socket (UDS) 文件的存储目录。UDS 路径总长度受内核限制，建议目录路径不超过80个字符。该目录将挂载到宿主机同名目录，请确保容器具备宿主机同名目录的操作权限 |
| worker_address | string | `"127.0.0.1:31501"` | 否 | datasystem_worker IP地址，格式为：ip:port, 例如：127.0.0.1:31501 |
| coordinator_address | string | `""` | 否 | Coordinator 服务地址，格式为 `host:port`；使用 Coordinator 集群管理方式时必须配置 |
| kv_events_config | string | `""` | 否 | KV event publisher JSON 配置。为空表示关闭该功能；非空时需要填写 JSON 对象字符串，字段说明参见[表1](#table_kv_events_config)。例如：`{"bind_endpoint":"tcp://0.0.0.0:5557","backend_id":"worker-0"}` |
| oc_worker_worker_direct_port | int | `0` | 否 | 对象/KV缓存datasystem-worker之间用于数据传输的TCP通道，0表示禁用该功能；当指定为一个非0值时，datasystem-worker将会建立一条单独用于数据传输的TCP通道，用于加速节点间数据的传输速度，降低数据传输时延 |
| oc_worker_worker_pool_size | int | `3` | 否 | datasystem-worker间用于数据传输的并行连接数，用于提升节点间数据传输的吞吐量，只有当 `ocWorkerWorkerDirectPort` 指定为非0值时该配置才生效 |
| payload_nocopy_threshold | string | `"104857600"` | 否 | datasystem-worker间数据传输时免数据拷贝的阈值（以字节为单位） |
| rpc_thread_num | int | `16` | 否 | 配置服务端的RPC线程数，必须为大于0的数 |
| oc_thread_num | int | `32` | 否 | 配置服务端用于处理对象/KV缓存的业务线程数 |
| io_thread_nice | int | `0` | 否 | 指定部分 IO 线程的 nice 值，取值范围：[-20, 19]。0 表示跳过 nice 调整并保留线程继承的 nice 值；仅非 0 值调用 `setpriority`；设置负值通常需要相应权限 |
| enable_sched_runtime | bool | `true` | 否 | 是否允许指定 worker 线程设置 Linux 调度 runtime。当前仅用于 URMA JFC poll 线程；设置为 `false` 时保留线程继承的调度参数且不调用 `sched_setattr`。 |
| brpc_event_dispatcher_num | int | `0` | 否 | brpc 进程级事件分发器数量。0 表示不覆盖 brpc 默认值 1；大于 1 时按 fd 哈希分摊 socket 读写事件，可缓解大量连接同时活跃时的首包读取排队，但不会并行化单个监听 fd 的 accept。仅在进程首次初始化 brpc 前生效，修改后需要重启 Worker |
| max_rpc_session_num | int | `2048` | 否 | 单个datasystem-worker最大可缓存会话数，取值范围：[512, 10,000] |
| remote_send_thread_num | int | `8` | 否 | 配置服务端用于将元素发送到远程工作线程的线程数量 |
| stream_idle_time_s | int | `300` | 否 | 配置流的空闲时间。默认值为300秒（5分钟） |

`brpc_event_dispatcher_num` 与 `rpc_thread_num`、`oc_thread_num` 的职责不同。仅当大量连接在同一时段发送首包或恢复读写，TCP 建连已完成但 RPC handler 入场仍明显延迟，且业务线程池与 CPU 尚未饱和时，才建议从 2 或 4 开始灰度验证；可结合 `event_dispatcher_read_latency` 与 handler 入场时序判断。业务处理、锁竞争、服务线程池饱和或单监听 fd 的 accept 是瓶颈时，增大该值通常无效。

> **升级提示：** `io_thread_nice` 的新默认值 `0` 仅影响新生成的配置文件，或启动时未显式传入该参数的场景。已有 `worker_config.json` 中的 `-15` 仍会作为显式配置在后续启动时使用；如需停用 nice 调整，请在启动 worker 前手动将其改为 `0`。

**表1** `kv_events_config` JSON 字段说明<a id="table_kv_events_config"></a>

| JSON 字段 | 类型 | 默认值 | 是否必填 | 描述 |
|---|---|---|---|---|
| bind_endpoint | string | `""` | 是 | ZMQ PUB 套接字的绑定地址，必须为非空字符串，例如 `tcp://0.0.0.0:5557`。 |
| model_name | string | `""` | 否 | 事件中的模型名称；空字符串在事件中编码为 `null`。 |
| backend_id | string | `""` | 是 | 事件中的后端实例标识，必须为非空字符串。 |
| tenant_id | string | `"default"` | 否 | 事件中的租户标识；当对象 key 未携带租户前缀时使用该值。 |
| additional_salt | string | `""` | 否 | 事件中的附加 salt；空字符串在事件中编码为 `null`。 |
| lora_name | string | `""` | 否 | 事件中的 LoRA 名称；空字符串在事件中编码为 `null`。 |
| block_size | uint32 | `0` | 否 | 事件中的 KV block 大小；值为 `0` 时在事件中编码为 `null`。 |
| dp_rank | uint32 | `0` | 否 | 数据并行 rank，同时写入事件和批次载荷。 |
| emit_legacy_compat_fields | bool | `true` | 否 | 是否输出兼容字段 `type`、`block_hashes` 和 `parent_block_hash`。 |
| queue_capacity | uint32 | `65536` | 否 | 待发布事件队列的最大事件数，必须大于 `0`；队列满时新增事件会被丢弃。 |

`kv_events_config` 为空时 KV event publisher 保持关闭。配置为非空 JSON 对象时，字段类型、必填字段和取值校验全部通过后自动启用；校验失败时保持关闭并记录错误日志。`enabled` 是内部派生状态，不是 JSON 配置字段。

#### ETCD相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| etcd_address | string | `""` | 否 | ETCD 服务端访问地址，格式为：ip:port, 例如：127.0.0.1:23456 |
| enable_etcd_auth | bool | `false` | 否 | 是否启用 ETCD 认证 |
| etcd_ca | string | `""` | 否 | CA明文证书，使用Base64编码 |
| etcd_cert | string | `""` | 否 | 客户端明文证书，使用Base64转码 |
| etcd_key | string | `""` | 否 | 客户端私钥。需进行Base64转码，是否加密取决于是否设置了密码短语 |
| etcd_username | string | `""` | 否 | ETCD鉴权用户名。仅在ETCD开启用户名/密码鉴权时需要配置 |
| etcd_password | string | `""` | 否 | ETCD鉴权密码。仅在ETCD开启用户名/密码鉴权时需要配置 |
| etcd_passphrase_path | string | `""` | 否 | 密码短语的值，需加密并进行Base64转码 |
| etcd_meta_pool_size | int | `8` | 否 | ETCD元数据异步队列大小，用于将KV接口 `WRITE_BACK_L2_CACHE` 可靠性配置的key的元数据异步写入ETCD持久化 |
| etcd_target_name_override | string | `""` | 否 | 设置用于SSL主机名校验的ETCD目标名称覆盖。该配置值应与TLS证书的Subject Alternate Names（主题备用名称）中的DNS内容保持一致 |
| host_id_env_name | string | `""` | 否 | 用于读取当前节点 `host_id` 的环境变量名。配置后，worker 在注册到 ETCD 时会同时上报 `host_id`，供客户端按同节点策略优先选择 worker |
| start_metastore_service | bool | `false` | 否 | 是否启用 Metastore 代替ETCD，若需要启用，仅需主节点worker设为`true`，从节点worker设为`false` |
| metastore_address | string | `""` | 否 | 主节点worker的Metastore Service访问地址，与`start_metastore_service`搭配一起使用，主从节点均需填写，格式为：ip:port, 例如：127.0.0.1:23456 |

#### Spill相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| spill_directory | string | `""` | 否 | 配置缓存溢出功能的本地磁盘路径，为空表示禁用缓存溢出功能。当配置该路径后，溢出的缓存数据将保存在该路径下的 `datasystem_spill_data` 目录下 |
| spill_size_limit | int | `0` | 是 | 配置缓存溢出的最大容量（以字节为单位） |
| spill_thread_num | int | `8` | 否 | 表示溢出数据写文件的最大并行度，线程数越多会消耗越多的CPU和I/O资源 |
| spill_io_mode | string | `"buffered"` | 否 | Spill写盘模式。`buffered`使用缓冲I/O；`direct_io_uring`使用异步Direct I/O，不能直接使用Direct I/O的对象会自动回退到缓冲I/O |
| spill_file_max_size_mb | int | `200` | 是 | 单个溢出文件的最大大小（以MB为单位）；对于小于此值的对象，会聚合存储于同一个文件中；对于超过此值的对象，将以单个对象单独存为一个文件 |
| spill_file_open_limit | int | `512` | 是 | 溢出文件的最大打开文件描述符数量。若已打开文件数超过此值，系统将临时关闭部分文件以防止超出系统最大限制。在系统资源有限的情况下，应适当调低此数值 |
| spill_enable_readahead | bool | `true` | 否 | 是否启用磁盘预读功能，当预读功能被禁用时，可以缓解KV语义 `Read` 接口偏移读取导致的读放大问题 |
| eviction_reserve_mem_threshold_mb | int | `10240` | 否 | 内存预留阈值（MB），实际取值 min(shared_memory_size_mb × 0.1, eviction_reserve_mem_threshold_mb)；与 eviction_high_watermark_ratio 共同决定驱逐触发线。有效范围 100-102400 |
| eviction_high_watermark_ratio | double | `0.9` | 否 | 内存占用率高水位（比例 0.0-1.0，相对可用共享内存）。当占用内存达到 max(比例 × 共享内存, 共享内存 - eviction_reserve_mem_threshold_mb) 时触发驱逐。有效范围 0.02-1.0，须大于 eviction_low_watermark_ratio |
| eviction_low_watermark_ratio | double | `0.8` | 否 | 内存占用率低水位（比例 0.0-1.0），后台驱逐运行直至占用率降至该比例及以下。有效范围 0.01-0.99，须小于 eviction_high_watermark_ratio |
| spill_high_watermark_ratio | double | `0.8` | 否 | Spill 目录占用率高水位（相对 spill_size_limit 的比例 0.0-1.0）。有效范围 0.02-1.0，须大于 spill_low_watermark_ratio |
| spill_low_watermark_ratio | double | `0.6` | 否 | Spill 目录占用率低水位（相对 spill_size_limit 的比例 0.0-1.0）。有效范围 0.01-0.99，须小于 spill_high_watermark_ratio |

#### Memory Rebalance相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| enable_memory_rebalance | bool | `false` | 否 | 是否开启由 master 调度的内存均衡能力。开启后，master 会将高共享内存使用率 worker 上的对象迁移到低使用率 worker，以均衡集群内 object cache 的共享内存压力 |
| rebalance_source_usage_percent | uint32 | `80` | 否 | source worker 的共享内存使用率阈值（百分比）。使用率达到或超过该值的 ready worker 才会被选为内存均衡迁移源。该参数为双重语义：同一百分比同时作为 target worker 的迁移上限，当 target 投影使用率达到该值时停止后续迁移批次。默认值 80% 与 eviction 低水位一致；调高该值会让 target 上限向 eviction 高水位（默认 90%）靠拢，更易触发驱逐。取值范围：1-100 |
| rebalance_usage_gap_percent | uint32 | `20` | 否 | source 与 target worker 之间的最小共享内存使用率差值（百分比）。仅当差值不小于该值时才下发迁移任务。取值范围：1-100 |
| rebalance_task_report_grace_ms | uint32 | `30000` | 否 | 均衡任务上报的宽限时间（毫秒），超过该时间未上报则任务视为超时并触发重试 |

> 兼容性说明：`rebalance_cooldown_s` 与 `rebalance_max_migrate_bytes_per_round` 已移除，冷却时长固定为 60s。单次任务迁移上限由原 1GB/轮改为 300MB/批的连续反馈回路：master 在 30s 调度周期内钉死总迁移预算（`min(usageGap/2, headroomToWatermark, targetAvail)`），worker 每完成一批 300MB 迁移即上报目标剩余内存，master 据此决策继续发放下一批或停止（预算耗尽或目标达水位线，即 `rebalance_source_usage_percent`，默认 80%）。升级前请从 worker 启动参数（如 `workerGflagParams`/`extraArgs`）和自定义 `worker_config.json` 中移除这两个配置项，否则 worker 会因识别到未知 flag 而启动失败。

#### 日志与可观测相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| log_dir | string | `"./datasystem/logs"` | 否 | 日志目录，服务端产生的日志将保存到此目录中，该路径会被自动挂载到宿主机同名路径下 |
| v | int | `0` | 是 | 冗余日志级别，0表示不开启冗余日志，取值范围：[0, 3] |
| log_async | bool | `true` | 否 | 是否开启异步刷新日志功能 |
| log_async_queue_size | int | `65536` | 否 | 异步日志的消息队列最大容量（消息条数） |
| log_compress | bool | `false` | 是 | 控制是否启用日志压缩功能。启用时，历史日志将自动压缩为gzip格式存储 |
| logbufsecs | int | `10` | 否 | 日志消息最多缓冲时长（以秒为单位） |
| log_filename | string | `""` | 否 | 日志前缀名，非空时仅允许英文字母、数字和下划线；为空时前缀名为 `datasystem_worker` |
| log_retention_day | int | `0` | 否 | 日志保留天数，当该值大于0时，最后修改时间早于 `logRetentionDay` 的日志文件将会被删除；当该值为0时表示禁用该功能 |
| max_log_file_num | int | `5` | 是 | 最大日志文件个数，当日志文件个数超过该值时，会将最旧的日志文件删除，通过日志滚动机制保证日志文件最大个数小于等于该值 |
| max_log_size | int | `400` | 否 | 单个日志文件最大大小（以MB为单位） |
| request_sample_rate | double | `1.0` | 是 | 请求日志主采样率（[0.0–1.0]）。1.0=全量保留（所有请求采中→access/diagnostic也强制输出）；0.0=丢弃请求级 INFO/VLOG，请求级 ERROR/WARNING/SLOW_LOG 由 diagnostic补采样率 控制 |
| access_sample_rate | double | `1.0` (未显式设置时支持派生) | 是 | access日志**补采样率**（[0.0–1.0]）。仅在请求未被request采中时生效；请求采中时access日志无条件输出。实际保留率=request采中率+(1−采中率)×此值，通常高于此值。1.0=未采中请求全量保留 |
| diagnostic_sample_rate | double | `1.0` (未显式设置时支持派生) | 是 | diagnostic日志**补采样率**（[0.0–1.0]）。仅在请求未sampled-in时生效，控制请求级 ERROR/WARNING/SLOW_LOG的补充采样；请求采中时diagnostic无条件输出。FATAL/CHECK无条件保留不受此参数影响。实际保留率=request采中率+(1−采中率)×此值。1.0=未采中请求全量保留 |
| slow_log_process_slower_than | uint64 | `2000` | 是 | 处理阶段时延阈值（微秒），用于慢日志和latencySummary输出。默认2000μs(2ms)。填写正整数（如 `1000` 表示 1ms 阶段）。设为0可禁用。启用后，处理阶段耗时（总耗时减去子RPC耗时）超过此阈值的请求将在access log中输出latencySummary |
| slow_log_rpc_slower_than | uint64 | `5000` | 是 | 跨进程RPC阶段时延阈值（微秒），用于慢日志和latencySummary输出。默认5000μs(5ms)。填写正整数（如 `2000` 表示 2ms 阶段）。设为0可禁用。启用后，RPC子阶段耗时超过此阈值的请求将在access log中输出latencySummary |

> **派生规则说明**：当仅显式设置 `request_sample_rate`，且 `access_sample_rate` 和 `diagnostic_sample_rate` 未被显式指定（包括在运行过程中也未曾被显式设置过），则：
> - `access_sample_rate = min(1.0, request_sample_rate × 3)`
> - `diagnostic_sample_rate = min(1.0, request_sample_rate × 4)`
> 一旦 `access_sample_rate` 或 `diagnostic_sample_rate` 被显式设置，派生规则即失效，此后使用各自独立的值。显式设置 `1.0` 与默认 `1.0` 具有不同语义：前者阻止派生，后者允许派生。
>
> **补采样率与整体保留率的区别**：`access_sample_rate` 和 `diagnostic_sample_rate` 是补采样率，不是该类日志的整体保留率。
> 例如 `request=0.5, access=0.3` 时，access实际保留率是 65%（50%采中强制输出 + 50%未采中×30%补采样），远高于 `access_sample_rate=0.3`。
| log_only_write_info_file | bool | `true` | 否 | INFO日志文件始终写入所有级别日志。该值为`true`时不额外生成WARNING/ERROR日志文件；为`false`时会额外生成WARNING/ERROR日志文件，高级别日志会按等级写入多个日志文件。 |
| log_monitor | bool | `true` | 是 | 是否开启接口性能与资源观测日志 |
| json_log_monitor | bool | `true` | 是 | 是否开启 `kv_resource.log` 与 `kv_metrics.log` JSON-Lines 观测日志。开启后 `kv_metrics.log` 每周期完整记录 `metrics_summary`，主 INFO 不再重复输出 `metrics_summary`；JSON exporter 未启用或未创建时保持 `log_monitor` 控制的 INFO 输出 |
| monitor_config_file | string | `./datasystem/config/datasystem.config` | 否 | 配置worker监控配置文件的路径 |
| log_monitor_exporter | string | `"harddisk"` | 否 | 指定观测日志导出类型，当前仅支持按 `harddisk` 类型导出观测数据，即将观测数据保存到 `logDir` 路径下 |
| log_monitor_interval_ms | int | `10000` | 否 | 观测日志收集导出的间隔时间（以毫秒为单位） |
| minloglevel | int | `0` | 是 | 设置记录冗余日志的最低级别，低于这个级别的日志不会被记录 |
| logfile_mode | int | `416` | 否 | 设置日志文件模式/权限，值为八进制数 |
| enable_perf_trace_log | bool | `false` | 否 | 是否打开性能trace日志 |

> **升级提示**：日志压缩默认关闭后，新部署或升级集群的历史日志不会再自动压缩，日志磁盘占用可能增加且滚动更快。如需保持历史压缩行为，请显式配置 `log_compress` 为 `true`；同时建议根据不压缩场景重新评估 `max_log_file_num` 和 `max_log_size` 的容量设置。

#### 二级缓存相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| l2_cache_type | string | `"none"` | 否 | 配置二级缓存类型，`none` 表示不配置二级缓存，可选择二级缓存类型：[`obs`, `sfs`, `distributed_disk`] |
| l2_cache_delete_thread_num | int | `32` | 否 | 配置二级缓存异步删除线程池大小，增大该值可以提升二级缓存删除并行度，同时也会提升worker的CPU消耗 |
| l2_cache_async_write_queue_size | int | `10000` | 否 | 用于写入二级缓存的每线程异步队列大小。共有 8 个线程，支持的最大总容量为 8 * l2_cache_async_write_queue_size 个待写入的键值对 |
| l2_cache_async_write_rate_limit_mb | int | `200` | 否 | 配置写入二级缓存异步任务速率(以MB/s为单位) |
| obs_access_key | string | `""` | 否 | 对象存储服务(OBS) AK/SK认证的访问密钥(Access Key) |
| obs_secret_key | string | `""` | 否 | 对象存储服务(OBS) AK/SK认证的密钥(Secret Key) |
| obs_endpoint | string | `""` | 否 | 对象存储服务(OBS) 访问域名 |
| obs_bucket | string | `""` | 否 | 对象存储服务(OBS) 桶的名称 |
| obs_https_enabled | bool | `false` | 否 | 是否启用HTTPS连接对象存储服务（OBS），默认为HTTP |
| sfs_path | string | `""` | 否 | 挂载的SFS路径 |
| distributed_disk_path | string | `""` | 否 | distributed_disk 模式下的根路径。当 `l2_cache_type=distributed_disk` 时，slot root path 基于该路径构建，不再使用 `sfs_path` |
| distributed_disk_max_data_file_size_mb | uint32 | `1024` | 否 | 单个 distributed_disk data 文件的最大大小，单位 MB |
| distributed_disk_sync_interval_ms | uint32 | `1000` | 否 | distributed_disk group commit 的最长刷盘间隔，单位毫秒 |
| distributed_disk_sync_batch_bytes | uint64 | `33554432` | 否 | distributed_disk group commit 的批量刷盘阈值，单位字节 |
| distributed_disk_compact_interval_s | uint32 | `3600` | 否 | distributed_disk 后台 compact 的固定执行周期，单位秒；生产构建最小值为 60，测试构建在 `WITH_TESTS` 下最小值为 1 |
| enable_cloud_service_token_rotation | bool | `false` | 否 | 启用OBS客户端使用临时令牌访问OBS，令牌过期后，获取新的令牌并重新连接OBS |

#### 元数据相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| rocksdb_store_dir | string | `"./datasystem/rocksdb"` | 否 | 配置元数据持久化目录，元数据通过RocksDB持久化在磁盘中 |
| rocksdb_background_threads | int | `16` | 否 | RocksDB的后台线程数，用于元数据的刷盘和压缩 |
| rocksdb_max_open_file | int | `128` | 否 | RocksDB可使用的最大打开文件个数 |
| rocksdb_write_mode | string | `async` | 否 | 配置元数据写入RocksDB的方式，支持不写、同步和异步写入，默认值为`async`。可选值包括：'none'（不写）、'sync'（同步）、'async'（异步） |
| enable_meta_replica | bool | `false` | 否 | 已废弃的兼容参数，当前配置值会被忽略 |
| enable_metadata_recovery | bool | `false` | 否 | 是否在 worker 重启清理阶段将本地元数据回补到 master |
| enable_data_replication | bool | `true` | 否 | 实验性参数。是否允许跨 worker 读取到的数据在本 worker 缓存为本地热副本，并向 master 注册副本位置；关闭后 remote get 获取的数据通常仅服务当前请求，不作为普通本地副本保留。该参数仅用于热副本性能优化，不作为数据可靠性机制，不保障数据可靠性或可用性 |

#### 可靠性相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| client_reconnect_wait_s | int | `10` | 是 | 客户端断链重连最大等待时间（单位为秒） |
| client_dead_timeout_s | int | `120` | 否 | 客户端存活检测最大时间间隔（单位为秒）, client_dead_timeout_s >= 3 |
| heartbeat_interval_ms | int | `1000` | 是 | 服务端与ETCD的心跳间隔时间（单位为毫秒） |
| node_timeout_s | int | `60` | 否 | 服务端节点超时最大时间间隔（单位为秒）, node_timeout_s >= 1 |
| node_dead_timeout_s | int | `300` | 是 | 服务端节点存活检测最大时间间隔（单位为秒），当节点超过存活检测最大时间间隔后仍未恢复心跳，会被标记为死亡节点，该值必须大于 `node_timeout_s` |
| hash_ring_tokens_per_member | uint32 | `4` | 否 | 首次创建哈希环时为每个 Worker 分配的 token 数，取值范围为 `1`～`4096`。该值会写入集群 topology；后续扩容和重规划使用 topology 中的集群值，修改本地配置不会改变已有集群的 token 数。 |
| enable_reconciliation | bool | `false` | 否 | 当节点重启时是否启用全局引用计数对账功能；仅使用 KVClient 的场景无需开启，使用 ObjectClient 全局引用计数时需显式设置为 `true` |
| add_node_wait_time_s | int | `60` | 是 | 新节点加入哈希环的等待超时时间 |
| auto_del_dead_node | bool | `true` | 是 | 是否启用死亡节点自动清理功能，当该值为 `true` 时，会将死亡节点剔除出集群管理，并触发被动缩容 |
| enable_distributed_master | bool | `true` | 否 | 是否启用分布式主节点，默认值为true |

#### 优雅退出相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| enable_lossless_data_exit_mode | bool | `false` | 是 | 是否启用无损数据退出模式，当该值为 `true` 时，在节点退出时则会以优雅退出的方式，迁移数据和元数据，保证数据和元数据不丢失 |
| check_async_queue_empty_time_s | int | `1` | 否 | datasystem-worker检测异步队列为空的时间，单位为秒 |
| data_migrate_rate_limit_mb | int | `40` | 否 | 配置优雅退出数据迁移的流控（以MB/s为单位） |
| data_migrate_urma_transport_mode | string | `write` | 否 | 配置后台迁移启用 URMA 时的数据迁移传输模式。可选值：`write` 表示使用 URMA write 路径，`read` 表示使用 URMA read 路径。仅在 `enable_urma=true` 时生效 |

#### 性能相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| enable_huge_tlb | bool | `false` | 是否开启共享内存大页内存功能，它可以提高内存访问，减少页表的开销 |
| enable_fallocate | bool | `true` | 由于Kubernetes(k8s)的资源计算策略，共享内存有时会被计算两次，这可能会导致客户端OOM崩溃。为了解决这个问题，我们使用了fallocate来链接客户端和工作节点的共享内存，从而纠正内存计算错误。缺省情况下，fallocate是使能的。启用fallocate会降低内存分配的效率 |
| shared_memory_populate | bool | `false` | 是否开启共享内存预热功能，启用该功能可以加速应用运行期间的共享内存拷贝速度，但是在datasystem_worker进程启动时也会由于预热导致启动速度变慢（取决于sharedMemory的配置）。如果开启该功能，'arena_per_tenant'必须设置为1，'enable_fallocate'必须设置为false |
| enable_thp | bool | `false` | 是否启用透明大页（Transparent Huge Page,THP）功能，启用透明大页可以提高性能，减少页表开销，但也可能导致 Pod 内存使用增加 |
| arena_per_tenant | int | `16` | 每个租户的共享内存分配器数量。多分配器可以提高第一次分配共享内存的性能，但每个分配器会多使用一个fd，导致fd资源使用量上升。取值范围：[1, 32] |
| memory_reclamation_time_second | int | `600` | 释放后的内存回收时间控制日志消息的最大缓冲时间（以秒为单位），未回收的内存可以提供给下次分配复用，提升分配效率 |
| async_delete | bool | `false` | 是否异步删除对象，如果设置为 `true` 时，删除对象数据是个异步的过程，客户端不需要等待所有数据副本删除完成即可返回 |
| enable_p2p_transfer | bool | `false` | 是否开启异构对象传输协议支持点对点传输 |
| enable_worker_worker_batch_get | bool | `false` | 是否开启worker到worker的对象数据批量获取，默认值为false |
| enable_urma | bool | `false` | 是否开启Urma以实现对象worker之间的数据传输，开启后worker启动时会自动预热URMA worker-worker连接 |
| enable_ub_numa_affinity | bool | `false` | 是否开启 UB NUMA 亲和优化。仅在 `enable_urma=true` 且 `urma_register_whole_arena=true` 时生效。Worker 集群必须保持一致；client 从首个返回 UB 配置的 worker 固化该值，后续不一致仅记录 WARNING，不会修改 client 进程配置。 |
| ub_transport_arena_num | int | `4` | UB 传输内存池拆分的等大 Arena 数量，取值范围为 `[1, 32]`。开启 UB NUMA 亲和时，优先在不同芯片间轮询分配 Arena，再复用同一芯片内的 NUMA 节点。使用多个 Arena 时，任一 NUMA 绑定失败都会导致 client 初始化失败。 |
| ub_numa_rr_type | int | `1` | UB NUMA 源芯片选择粒度。`0` 表示关闭源芯片选择，保持发送内存所在芯片；`1` 表示每次逻辑写选择一次，`2` 表示每个 WR Post 选择一次。Worker 集群必须保持一致；client 固化首个返回 UB 配置的 worker 的取值。 |
| ub_numa_src_chip_policy | int | `1` | UB NUMA 源芯片策略。`0` 表示轮询；`1` 表示以轮询为基线，仅当发送内存的亲和芯片接收本次逻辑写的预计 WR 后仍不比轮询候选更忙时，才覆盖轮询结果。配置 `0` 可用于消融实验和快速回滚。旧 worker 未返回该字段时，新 client 按 `0` 兼容。Worker 集群必须保持一致；client 固化首个返回 UB 配置的 worker 的取值。 |
| ub_numa_inflight_wr_diff_threshold | int | `15` | 两个源芯片 inflight WR 数量的最大允许差值。差值严格大于该值时，后续逻辑写改用 inflight 较低的芯片；`0` 表示关闭深度纠偏和机会式亲和覆盖，保留纯轮询。Worker 集群必须保持一致；client 固化首个返回 UB 配置的 worker 的取值。 |
| shared_memory_distribution_policy | string | `none` | 共享内存在 NUMA 上的分布策略。可选值：`none`、`interleave_all_numa`、`interleave_affinity_numa`。仅在 `enable_urma=true` 且 `urma_register_whole_arena=true` 时生效。 |
| urma_connection_size | int | `0` | [已废弃] 仅为兼容旧配置而保留，内部已忽略。当前 JFS/JFR 按连接独占创建 |
| urma_event_mode | bool | `false` | 是否使用中断模式轮询完成事件 |
| urma_poll_size | int | `8` | 一次可轮询的完整记录数量，该设备最多可轮询16条记录 |
| urma_max_write_size_mb | int | `4` | URMA 单次写入大小上限，单位为 MB，取值范围：[1, 2048] |
| urma_register_whole_arena | bool | `true` | 是否在初始化时将整个arena注册为一个段，如果设置为`false`，将每个对象分别注册为一个段 |
| enable_rdma | bool | `false` | 是否开启RDMA以实现对象worker之间的数据传输 |
| rdma_register_whole_arena | bool | `true` | 是否在RDMA初始化时将整个arena注册为一个段，如果设置为`false`，将每个对象分别注册为一个段 |
| oc_shm_transfer_threshold_kb | int | `500` | 在客户端和worker之间通过共享内存传输对象数据的阈值，单位为KB |
| shared_disk_arena_per_tenant | int | `8` | 每个租户的磁盘缓存区域数量，多个区域可以提高首次共享磁盘分配的性能，但每个区域会多占用一个文件描述符（fd）。取值范围：[0, 32] |
| shared_disk_directory | string | `""` | 磁盘缓存数据存放目录，默认为空，表示未启用磁盘缓存 |
| shared_disk_size_mb | int | `0` | 共享磁盘的大小上限，单位为MB，默认为0，表示未启用磁盘缓存 |
| memory_alignment | int | `64` | jemalloc分配内存使用的字节对齐大小。更大的对齐可能提升性能，但也会因碎片化而增加内存占用。 |
| oc_metadata_header | bool | `true` | 是否为对象缓存共享内存分配元数据头，如果设置为`false`则关闭此功能（此时shm latch/可见性将不可用）|

`shared_memory_distribution_policy` 策略说明：

1. `none`： 不做 NUMA 分布
2. `interleave_all_numa`： 在所有可用 NUMA 节点上均匀分布
3. `interleave_affinity_numa`： 在当前进程绑定的 NUMA 节点上均匀分布。

#### AK/SK相关配置

| 配置项 | 类型 | 默认值 | 是否支持动态修改 | 描述 |
|-----|------|---------|-----|-------------|
| system_access_key | string | `""` | 否 | 系统访问密钥 |
| system_secret_key | string | `""` | 否 | 系统密钥 |
| tenant_access_key | string | `""` | 否 | 租户访问密钥 |
| tenant_secret_key | string | `""` | 否 | 租户密钥 |
| request_expire_time_s | int | `300` | 否 | 请求过期时间，单位为秒，最大值为300 |
