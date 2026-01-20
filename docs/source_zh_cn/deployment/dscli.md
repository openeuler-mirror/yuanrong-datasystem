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
    - [openYuanrong datasystem集群部署](#openyuanrong-datasystem集群部署)
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
| [Python](#安装python) | 3.10-3.11 | openYuanrong datasystem dscli的使用依赖Python环境 |
| [ETCD](#安装并部署etcd) | 3.5 | openYuanrong datasystem集群管理依赖组件 |
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

源码编译安装 openYuanrong datasystem 详细说明请参考：[源码编译方式安装openYuanrong datasystem](../installation/installation_linux.md#源码编译安装)。 


## dscli使用教程

本节包含 dscli 的使用场景与教程。

### openYuanrong datasystem集群部署

openYuanrong datasystem集群管理依赖ETCD，在部署openYuanrong datasystem集群前请确保ETCD集群处于可用状态。ETCD部署教程可参考：[ETCD集群部署](../deployment/deploy.md#安装并部署etcd)。

#### 单机部署

openYuanrong datasystem单机部署依赖 [dscli start](#dscli-start) 命令：

- 快速部署：

    [dscli start -w](#dscli-start) 命令可在后面添加 datasystem_worker 初始化启动命令行参数，快速部署至少需要指定以下两个命令行参数：

    | 命令行参数 | 类型 | 说明 |
    |-----------|------|------|
    | [--worker_address](#ipcrpc相关配置) | string | datasystem_worker IP地址与监听端口号 |
    | [--etcd_address](#etcd相关配置) | string | ETCD 服务端访问地址 |

    ```bash
    dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
    ```

    输出OK说明部署成功。默认情况下，datasystem_worker 最大可使用 1GB 共享内存空间用于缓存数据，如果需要调整共享内存大小，可以通过 `--shared_memory_size_mb` 参数进行调整：

    ```bash 
    dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379" --shared_memory_size_mb 4096
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
    ```

    更多 datasystem_worker 命令行参数详细说明请参考：[命令行参数配置项](#命令行参数配置项)。

- 通过配置项部署：

    在当前目录生成配置模板文件：

    ```bash
    dscli generate_config -o ./
    # [INFO] Cluster configuration file has been generated to /home/sn/cluster_config.json
    # [INFO] Worker configuration file has been generated to /home/sn/worker_config.json
    # [INFO] Configuration generation completed successfully
    ```

    `worker_config.json` 内置了 datasystem_worker 相关的命令行参数配置项，无需修改任何配置项即可部署，默认会占用端口号 31501：

    ```bash
    dscli start -f ./worker_config.json
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
    ```

    输出OK说明部署成功。默认情况下，datasystem_worker 最大可使用 1GB 共享内存空间用于缓存数据，如果需要调整共享内存大小，可以通过 `shared_memory_size_mb` 配置项进行调整：

    ```json
    // worker_config.json
    {
        "shared_memory_size_mb": {
            "value": "4096",
            "description": "Upper limit of the shared memory, the unit is mb, must be greater than 0."
        }
    }
    ```

    更多 `worker_config.json` 的详细说明请参考：[命令行参数配置项](#命令行参数配置项)。

#### 单机卸载

openYuanrong datasystem单机卸载依赖 [dscli stop](#dscli-stop) 命令：

- 卸载通过快速部署方式部署的集群：

    此种方式需要通过 `--worker_address` 命令行参数指定 datasystem_worker 的 IP 地址。

    ```bash
    dscli stop --worker_address "127.0.0.1:31501"
    # [INFO] [  OK  ] Stop worker service @ 127.0.0.1:31501 normally, PID: 38100
    ```

    输出OK说明卸载成功。

- 卸载通过配置项部署方式部署的集群：

    此种方式需要指定 `worker_config.json` 文件。

    ```bash
    dscli stop -f ./worker_config.json
    # [INFO] [  OK  ] Stop worker service @ 127.0.0.1:31501 normally, PID: 38100
    ```

    输出OK说明卸载成功。

#### 多机部署

- 通过执行 [dscli up](#dscli-up) 命令部署：

    1. 在当前目录生成 `worker_config.json` 和 `cluster_config.json` 文件：

        ```bash
        dscli generate_config -o ./
        # [INFO] Cluster configuration file has been generated to /home/sn
        # [INFO] Worker configuration file has been generated to /home/sn
        # [INFO] Configuration generation completed successfully
        ```

        编辑 `cluster_config.json` 文件：

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

        `cluster_config.json` 用于配置SSH互信以及 datasystem_worker 部署节点等信息，更多详细说明请参考：[集群配置项](#集群配置项)。

        编辑 `worker_config.json` 文件，默认情况下 datasystem_worker 最大可使用 1GB 共享内存空间用于缓存数据，如果需要调整共享内存大小，可以通过 `shared_memory_size_mb` 配置项进行调整：

        ```json
        {
            "shared_memory_size_mb": {
                "value": "4096",
                "description": "Upper limit of the shared memory, the unit is mb, must be greater than 0."
            }
        }
        ```

        `worker_config.json` 用于配置 datasystem_worker 相关的命令行参数配置项，更多详细说明请参考：[命令行参数配置项](#命令行参数配置项)。

        > **注意事项：**
        > 
        > `cluster_config.json` 文件中的 `worker_nodes` 以及 `worker_port` 配置项会覆盖 `worker_config.json` 中 `worker_address` 配置项。

    2. 部署集群：

        ```bash
        dscli up -f ./cluster_config.json                     
        # [INFO] Start worker service @ 127.0.0.1:31501 success.
        # [INFO] Start worker service @ 127.0.0.2:31501 success.
        ```

        当输出如上信息时说明集群部署成功。

    > 注意事项：
    > 
    > - 多机集群部署依赖多机之间配置SSH互信，请参考：[SSH互信配置](#ssh互信配置)。
    > - 所有待部署的机器上都需要安装dscli，dscli安装可参考：[dscli安装教程](#dscli安装教程)。

- 通过在多个机器执行 [dscli start](#dscli-start) 命令部署，假设在 127.0.0.1 和 127.0.0.2 部署一个两节点的数据系统集群：

    1. 在 127.0.0.1 机器上执行：

        ```bash
        dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
        # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
        ```

    2. 在 127.0.0.2 机器上执行：

        ```bash
        dscli start -w --worker_address "127.0.0.2:31501" --etcd_address "127.0.0.1:2379"
        # [INFO] [  OK  ] Start worker service @ 127.0.0.2:31501 success, PID: 38101
        ```

    > **注意事项**：
    >
    > - 在不同节点执行 dscli start 命令时，需要保证连接的是同一个ETCD，即 `--etcd_address` 的值需要保持一致。
    > - 如果涉及到需要指定 cluster name 的情况，即需要传 `--cluster_name` 参数，那么 `--cluster_name` 的值也需要保持一致。

#### 多机卸载

- 通过执行 [dscli down](#dscli-down) 命令卸载集群：

    ```bash
    dscli down -f ./cluster_config.json                     
    # [INFO] Stop worker service @ 127.0.0.1:31501 success.
    # [INFO] Stop worker service @ 127.0.0.2:31501 success.
    ```

    当输出如上信息时说明集群卸载成功。

- 通过执行 [dscli stop](#dscli-stop) 命令卸载集群，假设在 127.0.0.1 和 127.0.0.2 部署了一个两节点的数据系统集群需要卸载：

    1. 在 127.0.0.1 机器上执行：

        ```bash
        dscli stop --worker_address "127.0.0.1:31501"
        # [INFO] [  OK  ] Stop worker service @ 127.0.0.1:31501 normally, PID: 38100
        ```

    2. 在 127.0.0.2 机器上执行：

        ```bash
        dscli stop --worker_address "127.0.0.2:31501"
        # [INFO] [  OK  ] Stop worker service @ 127.0.0.2:31501 normally, PID: 38101
        ```

#### 部署问题汇总

1. 默认路径过长导致 dscli start 执行失败

    dscli start 默认使用当前目录作为 home 目录。如果当前目录路径过长，可能导致 datasystem_worker 进程启动失败。

    解决方案：

    使用 `--datasystem_home_dir`（短参数：`-d`）明确指定 datasystem home 目录，以规避路径过长问题：

    ```bash
    dscli start -f ~/worker_config.json -d /home/usr1/dscli

    dscli start -d /home/usr1/dscli -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379
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

更多 openYuanrong datasystem Kubernetes 部署详细信息请参考：[openYuanrong datasystem Kubernetes部署](../deployment/deploy.md#openyuanrong-datasystem-kubernetes部署)。

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

更多关于 dscli generate_helm_chart 命令的使用请参考：[dscli collect_log](#dscli-collect_log)。

### 多机命令运行

通过 [dscli runscript](#dscli-runscript) 命令可快速在多个节点上执行脚本命令。以在多个机器上安装 dscli 命令行工具为例：

1. 编写数据系统安装脚本：
    ```bash
    cat << EOF > install.sh
    conda create env --name python39
    conda activate python39
    pip install yr_datasystem
    EOF
    ```

2. 设置集群配置信息 `cluster_config.json`，可通过 [dscli generate_config](#dscli-generate_config) 命令生成并修改，其中 `worker_nodes` 即为要执行脚本的机器：

    ```json
    {
        "worker_nodes" : ["127.0.0.1", "127.0.0.2"],
        "ssh_auth": {
            "ssh_user_name": "sn",
            "ssh_ssh_private_key": "~/.ssh/id_rsa"
            }
    },
    ```

3. 使用 dscli runscript 命令执行脚本：

    ```bash
    dscli runscript -f ./cluster.config install.sh
    ```

更多关于 dscli runscript 命令的使用请参考：[dscli runscript](#dscli-runscript)。


## 命令行参数说明

本节包含用于管理 openYuanrong datasystem 集群的命令。

### dscli start

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--timeout &lt;SECONDS&gt;| -t &lt;SECONDS&gt; | 等待 worker 服务就绪的最大时间（默认：90秒） |
|--worker_config_path &lt;FILE&gt;|-f &lt;FILE&gt;| 使用配置文件（JSON格式）启动worker，配置文件可通过generate_config命令生成 |
|--worker_args &lt;...&gt;        |-w &lt;...&gt; | 使用参数启动数据系统worker, 以 "--<Args>  <Value>"为格式。比如--worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"<br>**注意**：此选项必须是命令行的最后一个参数选项，其后的所有内容都会被解析为 worker 参数|
|--datasystem_home_dir |-d &lt;DIR&gt; | 指定基础路径，将配置文件中的相对路径转换为绝对路径。例如当配置中包含 './yr_datasystem/log_dir'，其中的 '.' 将被替换为 datasystem_home_dir 的值 |
|--cpunodebind|-N | numactl选项，仅允许进程在指定 NUMA 节点所属的 CPU 上运行，支持多个节点 |
|--physcpubind|-C | 按物理 CPU 编号将进程绑定到指定核心 |
|--interleave|-i | 设置内存交错策略，按编号顺序在指定 NUMA 节点间轮询分配页面 |
|--preferred|-p | 设定优先 NUMA 节点。内核首先尝试在该节点分配内存；若内存不足，则退至其他节点 |
|--membind|-m | 强制仅允许从指定 NUMA 节点分配内存；若这些节点内存不足，分配将失败  |
|--localalloc|-l | 将内存分配限制在当前 CPU 所在的 NUMA 节点（本地节点），若本地节点内存不足，内核会退至邻近节点 |
|--enable_ums| 无 | 启用ums后，datasystem worker之间的rpc消息将通过 ub 传输 |

> **绑核配置项注意事项**：
> 
> - 使用绑核功能前请确保机器上已安装numactl命令。
> - 绑核配置项（cpunodebind，physcpubind，interleave，preferred，membind，localalloc）需要位于-w worker参数之前。
> 
> 例子：
> ```bash
> # 绑定到 NUMA 节点 0 的 CPU，并优先在 NUMA 节点 1 分配内存
> dscli start --cpunodebind 0 --preferred 1 -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
> ```
>
> **启用ums注意事项**：
> 
> - 启用ums功能请确保机器已安装ums相关rpm包
> - 配置项 enable_ums 需要位于-w worker参数之前。
> ums安装参考
> ```bash
> yum install umdk-ums.aarch64
> yum install umdk-ums-tools.aarch64
> modprobe ums
> ```
>
> 例子：
> ```bash
> dscli start --enable_ums -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
> ```



### dscli stop

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--worker_config_path &lt;FILE&gt;|-f &lt;FILE&gt;| 通过使用配置文件（JSON格式）停止worker |
|--worker_address <ADDR>    |-w &lt;...&gt; | 通过指定worker地址（IP:PORT格式，如127.0.0.1:31501）来停止worker |

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


> **绑核配置项注意事项**：
> 
> - 使用绑核功能前请确保机器上已安装numactl命令。
> 
> 例子：
> ```bash
> # 启动集群并绑核
> dscli up --cpunodebind 0 --preferred 1 -f ./cluster_config.json 
> ```
>
> **启用ums注意事项**：
> 
> - 启用ums功能请确保机器已安装ums相关rpm包
>
> 例子：
> ```bash
> dscli up --enable_ums -f ./cluster_config.json 
> ```

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
|--output_path &lt;OUTPUT_PATH&gt; |-o &lt;OUTPUT_PATH&gt; | 指定生成C++代码模板文件存放路径，默认存放路径为当前目录 |

### dscli generate_config

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--output_path &lt;OUTPUT_PATH&gt; |-o &lt;OUTPUT_PATH&gt; | 指定生成配置文件的存放路径，默认存放路径为当前目录 |

### dscli collect_log

|选项                         |等效短参数  |说明     |
|-----------------------------|-----------|--------|
|--cluster_config_path &lt;FILE&gt; |-f &lt;FILE&gt; | 集群配置文件（JSON）的路径                                           |
|--log_path &lt;PATH&gt;            |-d &lt;PATH&gt; | 对于每个需要采集日志的集群节点，指定存放日志的远程路径                  |
|--output_path &lt;...&gt;          |-o &lt;...&gt;  | 存储日志的本地目录，默认路径为当前目录下名为'log_\<timestamp\>'的文件夹 |

## 配置项说明

### 集群配置项

集群相关配置项位于 `cluster_config.json` 文件中，如下表所示：

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| ssh_auth.ssh_private_key | string | `"~/.ssh/id_rsa"` | 用于免密登录远程服务器的私钥文件 |
| ssh_auth.ssh_user_name | string | `"root"` | SSH登录时的远程用户名 |
| worker_config_path | string | `"./worker_config.json"` | datasystem_worker配置项路径 |
| worker_nodes | list | `"127.0.0.1"` | 部署openYuanrong datasystem的节点 |
| worker_port | int | `31501` | datasystem_worker占用的端口号 |

### 命令行参数配置项

集群相关配置项位于 `worker_config.json` 文件中，包含 datasystem_worker 的命令行参数相关的配置项

> **注意事项：**
>
> 单机快速部署 `dscli start -w` 该命令支持直接传入 datasystem_worker 的参数进行配置。表格中的配置项需要映射为命令行长参数。以配置项 `max_client_num` 为例，如需在启动时配置该项，应当写为 `dscli start -w --max_client_num 200`。

#### 资源相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| max_client_num | int | `200` | openYuanrong datasystem单个DaemonSet最大可同时连接的客户端数 |
| shared_memory_size_mb | int | `1024` | openYuanrong datasystem单个DaemonSet可使用的共享内存资源大小（以MB为单位） |

#### IPC/RPC相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| ipc_through_shared_memory | bool | `true` | datasystem-worker共享内存启用开关 |
| unix_domain_socket_dir | string | `"./datasystem/uds"` | 配置 Unix Domain Socket (UDS) 文件的存储目录。UDS 路径总长度受内核限制，建议目录路径不超过80个字符。该目录将挂载到宿主机同名目录，请确保容器具备宿主机同名目录的操作权限 |
| worker_address | string | `"127.0.0.1:31501"` | datasystem_worker IP地址，格式为：ip:port, 例如：127.0.0.1:31501 |
| enable_curve_zmq | bool | `false` | 是否开启服务端组件间认证鉴权功能 |
| curve_key_dir | string | `""` | 用于查找 ZMQ Curve 密钥文件的目录，启用 ZMQ 认证时必须指定该路径 |
| oc_worker_worker_direct_port | int | `0` | 对象/KV缓存datasystem-worker之间用于数据传输的TCP通道，0表示禁用该功能；当指定为一个非0值时，datasystem-worker将会建立一条单独用于数据传输的TCP通道，用于加速节点间数据的传输速度，降低数据传输时延 |
| oc_worker_worker_pool_size | int | `3` | datasystem-worker间用于数据传输的并行连接数，用于提升节点间数据传输的吞吐量，只有当 `ocWorkerWorkerDirectPort` 指定为非0值时该配置才生效 |
| payload_nocopy_threshold | string | `"104857600"` | datasystem-worker间数据传输时免数据拷贝的阈值（以字节为单位） |
| rpc_thread_num | int | `16` | 配置服务端的RPC线程数，必须为大于0的数 |
| oc_thread_num | int | `32` | 配置服务端用于处理对象/KV缓存的业务线程数 |
| zmq_server_io_context | int | `5` | ZMQ服务端性能优化参数，其数值与系统吞吐量正相关，取值范围：[1, 32] |
| zmq_client_io_context | int | `5` | ZMQ客户端性能优化参数，其数值与系统吞吐量正相关，取值范围：[1, 32] |
| zmq_chunk_sz | int | `1048576` | 并行负载分块大小配置（以字节为单位） |
| max_rpc_session_num | int | `2048` | 单个datasystem-worker最大可缓存会话数，取值范围：[512, 10,000] |
| remote_send_thread_num | int | `8` | 配置服务端用于将元素发送到远程工作线程的线程数量 |
| stream_idle_time_s | int | `300` | 配置流的空闲时间。默认值为300秒（5分钟） |

#### ETCD相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| etcd_address | string | `""` | ETCD 服务端访问地址，格式为：ip:port, 例如：127.0.0.1:23456 |
| enable_etcd_auth | bool | `false` | 是否启用 ETCD 认证 |
| etcd_ca | string | `""` | CA明文证书，使用Base64编码 |
| etcd_cert | string | `""` | 客户端明文证书，使用Base64转码 |
| etcd_key | string | `""` | 客户端私钥。需进行Base64转码，是否加密取决于是否设置了密码短语 |
| etcd_passphrase_path | string | `""` | 密码短语的值，需加密并进行Base64转码 |
| etcd_meta_pool_size | int | `8` | ETCD元数据异步队列大小，用于将KV接口 `WRITE_BACK_L2_CACHE` 可靠性配置的key的元数据异步写入ETCD持久化 |
| etcd_target_name_override | string | `""` | 设置用于SSL主机名校验的ETCD目标名称覆盖。该配置值应与TLS证书的Subject Alternate Names（主题备用名称）中的DNS内容保持一致 |

#### Spill相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| spill_directory | string | `""` | 配置缓存溢出功能的本地磁盘路径，为空表示禁用缓存溢出功能。当配置该路径后，溢出的缓存数据将保存在该路径下的 `datasystem_spill_data` 目录下 |
| spill_size_limit | int | `0` | 配置缓存溢出的最大容量（以字节为单位） |
| spill_thread_num | int | `8` | 表示溢出数据写文件的最大并行度，线程数越多会消耗越多的CPU和I/O资源 |
| spill_file_max_size_mb | int | `200` | 单个溢出文件的最大大小（以MB为单位）；对于小于此值的对象，会聚合存储于同一个文件中；对于超过此值的对象，将以单个对象单独存为一个文件 |
| spill_file_open_limit | int | `512` | 溢出文件的最大打开文件描述符数量。若已打开文件数超过此值，系统将临时关闭部分文件以防止超出系统最大限制。在系统资源有限的情况下，应适当调低此数值 |
| spill_enable_readahead | bool | `true` | 是否启用磁盘预读功能，当预读功能被禁用时，可以缓解KV语义 `Read` 接口偏移读取导致的读放大问题 |
| eviction_thread_num | int | `1` | 后台驱逐线程池大小。用于将缓存数据从共享内存中驱逐，经由溢出队列异步写入到磁盘中 |

#### 日志与可观测相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| log_dir | string | `"./datasystem/logs"` | 日志目录，服务端产生的日志将保存到此目录中，该路径会被自动挂载到宿主机同名路径下 |
| v | int | `0` | 冗余日志级别，0表示不开启冗余日志，取值范围：[0, 3] |
| log_async | bool | `true` | 是否开启异步刷新日志功能 |
| log_async_queue_size | int | `65536` | 异步日志的消息队列最大容量（消息条数） |
| log_compress | bool | `true` | 控制是否启用日志压缩功能。启用时，历史日志将自动压缩为gzip格式存储 |
| logbufsecs | int | `10` | 最多缓冲这么多秒的日志消息 |
| log_filename | string | `""` | 日志前缀名，当值为空时前缀名为 `datasystem_worker` |
| log_retention_day | int | `0` | 日志保留天数，当该值大于0时，最后修改时间早于 `logRetentionDay` 的日志文件将会被删除；当该值为0时表示禁用该功能 |
| max_log_file_num | int | `5` | 最大日志文件个数，当日志文件个数超过该值时，会将最旧的日志文件删除，通过日志滚动机制保证日志文件最大个数小于等于该值 |
| max_log_size | int | `400` | 单个日志文件最大大小（以MB为单位） |
| log_monitor | bool | `true` | 是否开启接口性能与资源观测日志 |
| monitor_config_file | string | `./datasystem/config/datasystem.config` | 配置worker监控配置文件的路径 |
| log_monitor_exporter | string | `"harddisk"` | 指定观测日志导出类型，当前仅支持按 `harddisk` 类型导出观测数据，即将观测数据保存到 `logDir` 路径下 |
| log_monitor_interval_ms | int | `10000` | 观测日志收集导出的间隔时间（以毫秒为单位） |
| minloglevel | int | `0` | 设置记录冗余日志的最低级别，低于这个级别的日志不会被记录 |
| logfile_mode | int | `416` | 设置日志文件模式/权限，值为八进制数 |

#### 二级缓存相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| l2_cache_type | string | `"none"` | 配置二级缓存类型，`none` 表示不配置二级缓存，可选择二级缓存类型：[`obs`, `sfs`] |
| l2_cache_delete_thread_num | int | `32` | 配置二级缓存异步删除线程池大小，增大该值可以提升二级缓存删除并行度，同时也会提升worker的CPU消耗 |
| obs_access_key | string | `""` | 对象存储服务(OBS) AK/SK认证的访问密钥(Access Key) |
| obs_secret_key | string | `""` | 对象存储服务(OBS) AK/SK认证的密钥(Secret Key) |
| obs_endpoint | string | `""` | 对象存储服务(OBS) 访问域名 |
| obs_bucket | string | `""` | 对象存储服务(OBS) 桶的名称 |
| obs_https_enabled | bool | `false` | 是否启用HTTPS连接对象存储服务（OBS），默认为HTTP |
| sfs_path | string | `""` | 挂载的SFS路径 |
| enable_cloud_service_token_rotation | bool | `false` | 启用OBS客户端使用临时令牌访问OBS，令牌过期后，获取新的令牌并重新连接OBS |

#### 多集群相关配置
注: 多集群模式为实验性质特性， 某些场景下可能会有问题，详见：[多集群模式FAQ](../FAQ/clusterFAQ.md)

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| other_cluster_names | string | `""` | 指定其他可用区的名称，如果需要指定多个可用区通过','进行分隔 |
| cross_cluster_get_data_from_worker | bool | `true` | 是否优先尝试从其他可用区的datasystem-worker获取数据。如果为 `false`，则将直接从二级缓存中检索数据 |
| cross_cluster_get_meta_from_worker | bool | `false` | 是否从其他可用区的datasystem-worker获取元数据，如果为 `false`，则从本地可用区获取元数据 |

#### 元数据相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| rocksdb_store_dir | string | `"./datasystem/rocksdb"` | 配置元数据持久化目录，元数据通过RocksDB持久化在磁盘中 |
| rocksdb_background_threads | int | `16` | RocksDB的后台线程数，用于元数据的刷盘和压缩 |
| rocksdb_max_open_file | int | `128` | RocksDB可使用的最大打开文件个数 |
| rocksdb_write_mode | string | `async` | 配置元数据写入RocksDB的方式，支持不写、同步和异步写入，默认值为`async`。可选值包括：'none'（不写）、'sync'（同步）、'async'（异步） |
| enable_meta_replica | bool | `false` | 控制是否启用多个元数据副本 |

#### 可靠性相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| client_reconnect_wait_s | int | `5` | 客户端断链重连最大等待时间（单位为秒） |
| client_dead_timeout_s | int | `120` | 客户端存活检测最大时间间隔（单位为秒） |
| heartbeat_interval_ms | int | `1000` | 服务端与ETCD的心跳间隔时间（单位为毫秒） |
| node_timeout_s | int | `60` | 服务端节点超时最大时间间隔（单位为秒） |
| node_dead_timeout_s | int | `300` | 服务端节点存活检测最大时间间隔（单位为秒），当节点超过存活检测最大时间间隔后仍未恢复心跳，会被标记为死亡节点，该值必须大于 `node_timeout_s` |
| enable_reconciliation | bool | `true` | 当节点重启时是否启用对账功能 |
| enable_hash_ring_self_healing | bool | `false` | 是否启用哈希环自愈功能，如果该值为 `true`，当哈希环状态异常时会启用自愈修复哈希环 |
| add_node_wait_time_s | int | `60` | 新节点加入哈希环的等待超时时间 |
| auto_del_dead_node | bool | `true` | 是否启用死亡节点自动清理功能，当该值为 `true` 时，会将死亡节点剔除出集群管理，并触发被动缩容 |
| enable_distributed_master | bool | `true` | 是否启用分布式主节点，默认值为true |

#### 优雅退出相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| enable_lossless_data_exit_mode | bool | `false` | 是否启用无损数据退出模式，当该值为 `true` 时，在节点退出时则会以优雅退出的方式，迁移数据和元数据，保证数据和元数据不丢失 |
| check_async_queue_empty_time_s | int | `1` | datasystem-worker检测异步队列为空的时间，单位为秒 |
| data_migrate_rate_limit_mb | int | `40` | 配置优雅退出数据迁移的流控（以MB/s为单位） |

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
| enable_urma | bool | `false` | 是否开启Urma以实现对象worker之间的数据传输 |
| urma_connection_size | int | `16` | jfs和jfr对的数量 |
| urma_event_mode | bool | `false` | 是否使用中断模式轮询完成事件 |
| urma_poll_size | int | `8` | 一次可轮询的完整记录数量，该设备最多可轮询16条记录 |
| urma_register_whole_arena | bool | `true` | 是否在初始化时将整个arena注册为一个段，如果设置为`false`，将每个对象分别注册为一个段 |
| enable_rdma | bool | `false` | 是否开启RDMA以实现对象worker之间的数据传输 |
| rdma_register_whole_arena | bool | `true` | 是否在RDMA初始化时将整个arena注册为一个段，如果设置为`false`，将每个对象分别注册为一个段 |
| oc_shm_transfer_threshold_kb | int | `500` | 在客户端和worker之间通过共享内存传输对象数据的阈值，单位为KB |
| shared_disk_arena_per_tenant | int | `8` | 每个租户的磁盘缓存区域数量，多个区域可以提高首次共享磁盘分配的性能，但每个区域会多占用一个文件描述符（fd）。取值范围：[0, 32] |
| shared_disk_directory | sting | `""` | 磁盘缓存数据存放目录，默认为空，表示未启用磁盘缓存 |
| shared_disk_size_mb | int | `0` | 共享磁盘的大小上限，单位为MB，默认为0，表示未启用磁盘缓存 |

#### AK/SK相关配置

| 配置项 | 类型 | 默认值 | 描述 |
|-----|------|---------|-------------|
| system_access_key | string | `""` | 系统访问密钥 |
| system_secret_key | string | `""` | 系统密钥 |
| tenant_access_key | string | `""` | 租户访问密钥 |
| tenant_secret_key | string | `""` | 租户密钥 |
| request_expire_time_s | int | `300` | 请求过期时间，单位为秒，最大值为300 |
