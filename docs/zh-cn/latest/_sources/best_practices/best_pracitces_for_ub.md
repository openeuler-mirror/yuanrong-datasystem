# UB最佳实践

[灵衢](https://www.unifiedbus.com/zh)（UnifiedBus，UB）是一种面向**超节点**的互联协议，将 IO、内存访问和各类处理单元间的通信统一在同一互联技术体系，实现高性能数据搬移、资源统一管理、资源灵活组合、处理单元间高效协同和高效编程。

openYuanrong datasystem 现已集成对 UB 的支持，实现了分布式缓存在超节点内的硬件级加速。基于 UB 构建的全局缓存抽象，透明化了下层拓扑，使得应用能够以极简的编程方式，在超节点拓扑中实现数据直通，充分释放底层硬件性能。

## 源码编译安装

源码编译安装前请确保编译环境中具备如下软件依赖：

|软件名称|版本|作用|
|-|-|-|
|openEuler|24.03|UB环境依赖的操作系统|
|UB|2.0|UB软件依赖|
|Python|3.9-3.11|openYuanrong datasystem的编译依赖Python环境|
|GCC|7.5.0+|用于编译openYuanrong datasystem的C编译器|
|G++|7.5.0+|用于编译openYuanrong datasystem的C++编译器|
|libtool|-|编译构建openYuanrong datasystem的工具|
|git|-|openYuanrong datasystem使用的源代码管理工具|
|Make|-|openYuanrong datasystem使用的编译构建工具|
|CMake|3.18.3+|编译构建openYuanrong datasystem的工具|
|patch|2.5+|openYuanrong datasystem使用的源代码补丁工具|

### 下载源码

```bash
git clone https://gitcode.com/openeuler/yuanrong-datasystem.git
```

### 编译

默认配置下数据系统会启用异构能力的编译，需要编译环境中具备CANN依赖，如无需异构能力支持，可禁用异构能力。

::::{tab-set}

:::{tab-item}  启用 UB 支持

```bash
bash build.sh -M on
```

:::

:::{tab-item} 启用 UB，禁用异构能力

```bash
bash build.sh -M on -X off
```

:::

::::

编译成功后，会在output目录下产生如下编译产物：

```text
output/
├── openyuanrong_datasystem-x.x.x-cp311-cp311-manylinux_2_34_x86_64.whl
└── yr-datasystem-vx.x.x.tar.gz
```

### 安装

```bash
pip install output/openyuanrong_datasystem-*.whl
```


## 部署指南

部署前，请确保满足以下必要条件：
1. 节点准备：须至少在 2 个 已配备 UB 硬件并安装相应软件的节点上部署服务端组件。
2. 集群依赖：openYuanrong datasystem 的集群管理功能依赖于 ETCD，因此需预先搭建并确保一个稳定可用的 ETCD 集群。

ETCD部署命令：

```bash
etcd --listen-client-urls http://0.0.0.0:2379 \
     --advertise-client-urls http://0.0.0.0:2379 &
```

::::{tab-set}

:::{tab-item}  进程部署

openYuanrong datasystem 进程部署主要通过dscli工具，在使用前请确保在两个节点中已安装 openYuanrong datasystem wheel 包。

> **容器内进程部署注意事项**
>
> 🔔 **重要提示**：在拉起容器前，请挂载如下宿主机目录，确保容器内的URMA二进制文件与宿主机版本一致：
> | 宿主机路径 | 容器挂载路径 |
> |--|--|
> | /usr/bin/urma_admin | /usr/bin/urma_admin |
> | /usr/bin/urma_perftest | /usr/bin/urma_perftest |
> | /usr/bin/urma_sample | /usr/bin/urma_sample |
> | /lib64/urma/ | /lib64/urma/ |

分别在两个节点执行如下命令：

```bash
dscli start -w --worker_address "${node_address}" --etcd_address "${etcd_address}" --enable_urma true
```
参数说明：
- `node_address`：当前节点的通信地址与端口。格式为 `IP:Port`，例如：`192.168.0.1:31501`。
- `etcd_address`：ETCD集群的访问地址列表。格式为多个 `IP:Port` 的逗号分隔字符串，例如：`192.168.1.100:2379,192.168.1.101:2379,192.168.1.102:2379`。

:::

:::{tab-item} K8S部署



```bash
# 通过dscli获取helm chart包
dscli generate_helm_chart -o /tmp

# 通过源码获取helm chart包
git clone -b ${version} https://gitcode.com/openeuler/yuanrong-datasystem.git
cp -r yuanrong-datasystem/docker/chart/datasystem /tmp
```

命令运行成功后会在"/tmp"目录下生成helm chart目录。

编辑 `/tmp/datasystem/values.yaml` 对集群启动项进行配置：

```yaml
global:
  # 其他配置项...

  imageRegistry: ""
  images:
    datasystem: "openyuanrong-datasystem:0.6.0"
  
  etcd:
    # ETCD集群地址
    etcdAddress: "192.168.1.100:2379,192.168.1.101:2379,192.168.1.102:2379"
  
  performance:
    # 开启UB能力
    enableUrma: true
  
  # 挂载URMA二进制文件，确保Pod容器内的URMA二进制文件与宿主机版本一致：
  mount:
    - hostPath: "/usr/bin/urma_admin"
      mountPath: "/usr/bin/urma_admin"
      type: FileOrCreate
    - hostPath: "/usr/bin/urma_perftest"
      mountPath: "/usr/bin/urma_perftest"
      type: FileOrCreate
    - hostPath: "/usr/bin/urma_sample"
      mountPath: "/usr/bin/urma_sample"
      type: FileOrCreate
    - hostPath: "/lib64/urma/"
      mountPath: "/lib64/urma/"
```

部署集群：

```bash
helm install datasystem /tmp/datasystem
```

部署后可以通过 kubectl 命令查看集群状态：

```bash
kubectl get pods -o wide
# NAME                   READY   STATUS       RESTARTS      AGE    IP           NODE 
# ...
# ds-worker-5cw42        1/1     Running      1 (2s ago)    13s   127.0.0.1   Running
# ds-worker-4wv63        1/1     Running      1 (10s ago)   23s   127.0.0.2   Running
```

:::

::::


## 快速验证

通过跨节点拉取数据的样例可快速验证UB的能力。

在节点1执行以下Python脚本：

```python
from datasystem import KVClient

client = KVClient("192.168.0.1:31501", 31501)
client.init()
key = "key"
expected_val = b"value"
client.set(key, expected_val)
print("[OK] Set value")
```



在节点2执行以下Python脚本：

```python
from datasystem import KVClient

client = KVClient("192.168.0.2:31501", 31501)
client.init()
key = "key"
expected_val = b"value"
val = client.kv().get([key])
assert val[0] == expected_val
print("[OK] Get value")
```

> 注意：
> 
> 脚本中初始化KVClient的入参需要替换为节点1/节点2服务端组件的IP和端口号。

当脚本执行完均打印OK时说明验证成功。


## 推荐配置

为确保灵衢（UB）组件在生产环境中达到最佳性能与稳定性，请参考以下配置建议。


### 关闭LPI

LPI 用于优化功耗管理与资源分配，建议在 BIOS 启动时禁用该功能以提升 CPU 性能，配置步骤如下：

```text
BIOS -> Advanced -> Power And Performance Configuration -> CPU PM Control
```

### 开启大页内存

开启大页内存可有效提升内存的分配与拷贝性能，开启大页内存可参考附录文档：[大页内存配置指南](../appendix/hugepage_guide.md)。

运行环境开启大页内存之后，启动数据系统服务端组件时需要启用大页内存配置项：

::::{tab-set}

:::{tab-item}  进程部署

```bash
dscli start -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --enable_urma true \
    --enable_huge_tlb true
```

:::

:::{tab-item}  K8s部署

```yaml
global:
  # 其他配置项...

  imageRegistry: ""
  images:
    datasystem: "openyuanrong-datasystem:0.6.0"
  
  etcd:
    # ETCD集群地址
    etcdAddress: "192.168.1.100:2379,192.168.1.101:2379,192.168.1.102:2379"
  
  performance:
    # 开启UB能力
    enableUrma: true
    # 开启大页内存
    enableHugeTlb: true
```


:::

::::

### 绑定NUMA节点

绑定NUMA节点可减少远程内存访问，提升缓存访问性能，进程部署时绑定NUMA节点命令如下：

```bash
dscli start \
    --cpunodebind 0 \
    --localalloc \
    -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --enable_urma true \
    --enable_huge_tlb true
    
```

表示绑定到 NUMA 节点 0 的 CPU，并在节点 NUMA 0 分配内存。
更多 dscli绑定numa节点 部署详细信息请参考：[dscli命令参数说明](../deployment/dscli.md#命令行参数说明)。
