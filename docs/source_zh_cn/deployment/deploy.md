# 部署 openYuanrong datasystem

<!-- TOC -->
- [openYuanrong datasystem进程部署](#openyuanrong-datasystem进程部署)
    - [部署环境准备](#部署环境准备)
    - [集群部署](#集群部署)
        - [单机部署](#单机部署)
        - [多机部署](#多机部署)
    - [快速验证](#快速验证)
    - [集群卸载](#集群卸载)
        - [单机卸载](#单机卸载)
        - [多机卸载](#多机卸载)
- [openYuanrong datasystem Kubernetes部署](#openyuanrong-datasystem-kubernetes部署)
    - [部署环境准备](#部署环境准备-1)
    - [集群部署](#集群部署-1)
    - [快速验证](#快速验证-1)
    - [集群卸载](#集群卸载-1)

<!-- /TOC -->

本文档介绍如何将openYuanrong datasystem通过裸进程或者Kubernetes的方式进行部署。

## openYuanrong datasystem进程部署

### 部署环境准备
openYuanrong datasystem进程部署所需的系统环境依赖如下：
|软件名称|版本|作用|
|-------|----|----|
| openEuler |22.03|运行openYuanrong datasystem的操作系统|
|[CANN](#安装cann)|8.2.rc1|运行异构相关特性的依赖库|
|[Python](#安装python)|3.9-3.11|openYuanrong datasystem dscli的使用依赖Python环境|
|[dscli](#安装dscli)|-|用于部署openYuanrong datasystem的命令行工具|
|[ETCD](#安装并部署etcd)|3.5|openYuanrong datasystem集群管理依赖组件|
|[SSH互信配置](#ssh互信配置)|-|仅多机部署需要，配置SSH互信用于机器间互相访问|

下面给出以上依赖的安装方法。

### 安装CANN
CANN的安装依赖Python环境，请确保您在开始安装CANN之前环境中的Python已经就绪。

在[Ascend官网](https://www.hiascend.com/developer/download/community/result?module=cann&cann=8.2.RC1)下载CANN run包，安装 run 包：
```bash
chmod +x ./Ascend-cann-toolkit_<version>_linux-<arch>.run
./Ascend-cann-toolkit_<version>_linux-<arch>.run --install --quiet
```

安装完成后，若显示如下信息，则说明软件安装成功：
```bash
Toolkit:  Ascend-cann-toolkit_<version>_linux-<arch> install success
```

如果用户未指定安装路径，则软件会安装到默认路径下，默认安装路径如下：
| 用户身份   | 默认路径                |
| ------ | ------------------- |
| root   | `/usr/local/Ascend` |
| 非 root | `$HOME/Ascend`     |

加载环境变量（非 root 示例）：
```bash
source ${HOME}/Ascend/ascend-toolkit/set_env.sh
```

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
conda create -n openYuanrong datasystem_py311 python=3.11.4 -y
conda activate openYuanrong datasystem_py311
```

可以通过以下命令查看Python版本。

```bash
python --version
```

### 安装dscli
dscli命令行工具集成在openYuanrong datasystem的wheel包 `openyuanrong_datasystem-<version>-cp311-cp311-manylinux_2_34_<arch>.whl`中，安装openYuanrong datasystem请参考[安装openYuanrong datasystem](../installation/installation_linux.md)。

安装完成后，运行如下命令：
```bash
dscli -h
```
如果输出dscli帮助文档说明安装成功。

### 安装并部署ETCD
#### 1. 下载 ETCD 二进制文件
从 [ETCD GitHub Releases](https://github.com/etcd-io/etcd/releases) 下载最新版本的二进制包：
```bash
ETCD_VERSION="v3.5.12"  
wget https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz
```

#### 2. 解压并安装
```bash
tar -xvf etcd-${ETCD_VERSION}-linux-amd64.tar.gz
cd etcd-${ETCD_VERSION}-linux-amd64
# 复制可执行文件到系统路径
sudo cp etcd etcdctl /usr/local/bin/
```

#### 3. 验证安装
```bash
etcd --version
etcdctl version
```
如果能输出版本号说明安装成功。

#### 4. 部署ETCD
> 注意：
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

#### 1. 安装SSH服务

确保所有需要配置互信的服务器上都已经安装并运行了SSH服务：
```bash
sudo yum install openssh-server     
sudo systemctl start sshd
sudo systemctl enable sshd
```

#### 2. 生成SSH密钥对（在所有节点上执行）

```bash
ssh-keygen -t rsa -b 4096
```
按Enter接受默认位置（~/.ssh/id_rsa），然后设置空密码（直接按Enter两次）。

#### 3. 将公钥复制到所有节点

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

#### 4. 测试SSH互信

从一个节点尝试SSH到其他节点，不需要输入密码：
```bash
ssh username@hostname
```
当无需密码能够正常跳转到其他节点时说明SSH互信配置成功。

### 集群部署

openYuanrong datasystem集群依赖ETCD，部署前需要先部署ETCD，部署ETCD可参考：[安装并部署ETCD](#安装并部署etcd)。

#### 单机部署

单机部署有两种方式：快速部署以及通过配置项部署

- 快速部署
    ```bash
    dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
    ```
    输出OK说明部署成功。

    更多快速部署的使用方式请参考：[dscli单机快速部署](../deployment/dscli.md#单机部署)

- 通过配置项部署
    
    需要指定配置项部署时，先获取配置模板文件：
    ```bash
    dscli generate_config -o ./
    # [INFO] Worker configuration file has been generated to /home/test
    ```
    配置模板文件完成后，通过如下命令部署集群：
    ```bash
    dscli start -f ./worker_config.json
    # [INFO] [  OK  ] Start worker service @ 127.0.0.1:31501 success, PID: 38100
    ```
    输出OK说明部署成功。

    更多配置项部署的使用方式请参考：[dscli单机配置项部署](../deployment/dscli.md#单机部署)

#### 多机部署

在当前目录生成worker_config.json和cluster_config.json文件：

```bash
dscli generate_config -o ./
# [INFO] Cluster configuration file has been generated to /home/test
# [INFO] Worker configuration file has been generated to /home/test
# [INFO] Configuration generation completed successfully
```

编辑cluster_config.json文件：

```json
{
    // ssh互信
    "ssh_auth": {
        "ssh_private_key": "~/.ssh/id_rsa",
        "ssh_user_name": "test_user"
    },
    // 集群配置项文件，在第一步生成
    "worker_config_path": "./worker_config.json",
    "worker_nodes": [
        "127.0.0.1",
        "127.0.0.2"
    ],
    "worker_port": 31501
}
```

部署集群：

```bash
dscli up -f ./cluster_config.json                     
# [INFO] Start worker service @ 127.0.0.1:31501 success.
# [INFO] Start worker service @ 127.0.0.2:31501 success.
```

当输出如上信息时说明集群部署成功。

> 注意事项：
> 
> - openYuanrong datasystem集群依赖ETCD，部署前需要先部署ETCD，部署ETCD可参考：[安装并部署ETCD](#安装并部署etcd)。
> - 多机集群部署依赖多机之间配置SSH互信，请参考：[SSH互信配置](#ssh互信配置)。
> - 需要部署的机器上都已安装dscli，dscli安装可参考：[安装dscli](#安装dscli)。
>

更多配置项部署的使用方式请参考：[dscli多机部署](../deployment/dscli.md#多机部署)

### 快速验证

Python 脚本快速验证：

```python
from yr.datasystem.ds_client import DsClient

client = DsClient("127.0.0.1", 31501)
client.init()
```

当脚本执行未发生异常时说明openYuanrong datasystem的客户端能正常连接上当前节点的ds-worker，部署成功。

### 集群卸载

#### 单机卸载

单机集群卸载有两种方式：快速卸载以及通过配置项卸载

- 卸载快速部署的集群

    ```bash
    # 卸载指定ds-worker IP的集群
    dscli stop --worker_address "127.0.0.1:31501"
    # [INFO] [  OK  ] Stop worker service @ 127.0.0.1:31501 normally, PID: 38100
    ```

    当命令输出的日志为OK时说明卸载成功。

- 卸载配置项部署的集群

    ```bash
    # 卸载使用配置项部署的集群
    dscli stop -f ./worker_config.json
    # [INFO] [  OK  ] Stop worker service @ 127.0.0.1:31501 normally, PID: 38100
    ```

    输出OK说明卸载成功。

#### 多机卸载

```bash
dscli down -f ./cluster_config.json                     
# [INFO] Stop worker service @ 127.0.0.1:31501 success.
# [INFO] Stop worker service @ 127.0.0.2:31501 success.
```

当输出如上信息时说明集群卸载成功。

## openYuanrong datasystem Kubernetes部署

### 部署环境准备

openYuanrong datasystem Kubernetes部署所需的依赖如下：

|软件名称|推荐版本|作用|
|--------|-------|----|
|openEuler |22.03|支持运行Kubernetes与Docker的操作系统|
|[kubectl](#安装kubectl)|-|运行异构相关特性的依赖库|
|[Kubernetes](#安装kubernetes)|-|Kubernetes集群，用于编排和管理openYuanrong datasystem的容器|
|[Helm](#安装helm)|-|openYuanrong datasystem dscli的使用依赖Python环境|
|[Docker](#安装docker)|-|提供容器化平台，支持openYuanrong datasystem容器化部署和运行|
|[ETCD](#安装并部署etcd)|3.5|openYuanrong datasystem集群管理依赖组件|
|[openYuanrong datasystem镜像](#获取openyuanrong-datasystem镜像)|-|openYuanrong datasystem服务端组件镜像|
|[openYuanrong datasystem helm chart](#获取openyuanrong-datasystem-helm-chart包)|-|openYuanrong datasystem helm chart包|

下面给出以上软件的获取及安装方法。

#### 安装kubectl

安装详情请参考官方文档：[Install and Set Up kubectl on Linux](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)

#### 安装Kubernetes

安装详情请参考官方文档：[使用部署工具安装 Kubernetes](https://kubernetes.io/zh-cn/docs/setup/production-environment/tools/)

#### 安装Helm

安装详情请参考官方文档：[Installing Helm](https://helm.sh/docs/intro/install/)

#### 安装Docker

安装详情请参考官方文档：[Install Docker Engine](https://docs.docker.com/engine/install/)

#### 安装并部署ETCD

安装详情请参考 [安装并部署ETCD](#安装并部署etcd) 章节。

#### 获取openYuanrong datasystem镜像

- 通过镜像仓获取镜像：

    ```bash
    docker pull openeuler/yuanrong-datasystem:<version>
    ```

- 源码编译构建镜像

    如果需要从源码构建镜像，需要先完成 [源码编译](../installation/installation_linux.md)。源码编译完成之后执行如下命令：

    ```bash
    cd yuanrong-datasystem/k8s/docker
    bash docker_build.sh \
        -b <base_image_name>:<base_image_tag> \
        -n <image_name> \
        -t <image_tag>
    ```

    参数说明：
    - base_image_name: 基础镜像名
    - base_image_tag: 基础镜像Tag
    - image_name: 生成的镜像名
    - image_tag: 生成的镜像Tag

    执行完成之后会在openYuanrong datasystem/docker目录下生成一个build目录，build目录中会生成一个 `datasystem.tar` 文件，即为镜像压缩文件；与此同时本地的docker仓库中也会保存 `<image_name>:<image_tag>` 的镜像。

#### 获取openYuanrong datasystem helm chart包

- 通过 dscli 命令行工具获取：

    ```bash
    dscli generate_helm_chart -o /tmp
    ```

    请注意命令运行成功后会在"/tmp"目录下生成helm chart目录。

- 通过源码获取：

    ```bash
    git clone -b ${version} https://gitee.com/openeuler/yuanrong-datasystem.git
    cp -r yuanrong-datasystem/docker/chart/datasystem /tmp
    ```

### 集群部署

openYuanrong datasystem通过 [/tmp/datasystem/values.yaml](#获取openYuanrong datasystem-helm-chart包) 文件进行集群相关配置，其中必配项如下：

```yaml
global:
  # 其他配置项...

  # 镜像仓地址，不涉及可以留空
  imageRegistry: ""
  # 镜像名字和镜像tag，<VERSION>需要替换为对应的版本号
  images:
    datasystem: "openyuanrong-datasystem:0.5.0"
  
  etcd:
    # ETCD集群地址
    etcdAddress: "127.0.0.1:2379"
```

> 注意事项：
> 
> - 镜像仓地址与镜像名称获取请参考：[获取openYuanrong datasystem镜像](#获取openYuanrong datasystem镜像)。
> - ETCD集群的部署与IP地址获取请参考：[安装并部署ETCD](#安装并部署etcd)。

配置完成后，通过 helm 命令即可轻松完成部署，命令如下：

```bash
helm install yr_datasystem /tmp/datasystem
# NAME: yr_datasystem
# LAST DEPLOYED: Tue Apr 22 14:31:34 2025
# NAMESPACE: default
# STATUS: deployed
# REVISION: 1
# TEST SUITE: None
```

部署后可以通过 kubectl 命令查看集群状态：

```bash
kubectl get pods -o wide
# NAME                   READY   STATUS       RESTARTS      AGE    IP           NODE 
# ...
# ds-worker-5cw42        1/1     Running      1 (2s ago)    13s   127.0.0.1   Running
# ds-worker-4wv63        1/1     Running      1 (10s ago)   23s   127.0.0.2   Running
```

当Pod处于Running状态时说明Pod处于就绪状态，可以正常对外提供服务，部署成功。

更多部署参数配置请参考：[Kubernetes配置项](../deployment/k8s_configuration.md)

### 快速验证

openYuanrong datasystem会默认以DamonSet的方式在每个节点都部署一个 `ds-worker` Pod，默认监听 `<主机IP>:31501`，可通过如下 Python 脚本快速验证：

```python
from yr.datasystem.ds_client import DsClient

client = DsClient("127.0.0.1", 31501)
client.init()
```

当脚本执行未发生异常时说明openYuanrong datasystem的客户端能正常连接上当前节点的 `ds-worker` Pod，部署成功。

### 集群卸载

集群通过 helm 命令即可轻松完成卸载，卸载命令如下：

```bash
helm uninstall yr_datasystem
```

执行完命令后通过 kubectl 命令再次查看集群状态：

```bash
kubectl get pods -o wide
# NAME                   READY   STATUS       RESTARTS      AGE    IP           NODE 
# Pod列表中不存在ds-worker Pod
```

当openYuanrong datasystem所有的ds-worker Pod都退出时说明集群卸载成功。