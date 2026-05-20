# RDMA最佳实践

远程直接内存访问（Remote Direct Memory Access，RDMA）是一种面向大规模数据中心与并行计算的互联技术，将网络传输与内存访问深度融合，通过旁路内核与硬件卸载，实现跨节点间高带宽、极低时延的数据直接搬移。RDMA 统一了分布式系统中的通信与存储访问路径，大幅降低了处理器的协议栈开销，支持异构计算单元间的高效协同，是构建高性能计算、AI 训练及分布式数据库的关键底层基础设施。

openYuanrong datasystem 现已集成对 RDMA 的支持，实现了分布式缓存在物理节点之间的硬件级加速。基于 RDMA 构建的全局缓存抽象，透明化了下层拓扑，使得应用能够以极简的编程方式，在跨节点通信场景中实现数据直通，充分释放底层硬件性能。

## 源码编译安装

源码编译安装前请确保编译环境中具备如下软件依赖：

|软件名称|版本|作用|
|-|-|-|
|openEuler|22.03|运行openYuanrong datasystem的操作系统|
|rdma-core-devel|41.0+|RDMA开发者软件依赖，也可安装厂商定制版本，如55mlnx37|
|Python|3.9-3.11|openYuanrong datasystem的编译依赖Python环境|
|GCC|7.5.0+|用于编译openYuanrong datasystem的C编译器|
|G++|7.5.0+|用于编译openYuanrong datasystem的C++编译器|
|libtool|-|编译构建openYuanrong datasystem的工具|
|git|-|openYuanrong datasystem使用的源代码管理工具|
|Make|-|openYuanrong datasystem使用的源代码管理工具|
|CMake|3.18.3+|编译构建openYuanrong datasystem的工具|
|patch|2.5+|openYuanrong datasystem使用的源代码补丁工具|

### 下载源码

```bash
git clone https://gitcode.com/openeuler/yuanrong-datasystem.git
```

### 编译

默认配置下数据系统会启用异构能力的编译，需要编译环境中具备 CANN 依赖，如无需异构能力支持，可禁用异构能力。

::::{tab-set}

:::{tab-item}  启用 RDMA 支持

```bash
bash build.sh -A on
```

:::

:::{tab-item} 启用 RDMA，禁用异构能力

```bash
bash build.sh -A on -X off
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
1. 节点准备：须至少在 2 个 已配备 RDMA 硬件并安装相应软件（`rdma-core`）的节点上部署服务端组件。
2. 集群依赖：openYuanrong datasystem 的集群管理功能依赖于 ETCD，因此需预先搭建并确保一个稳定可用的 ETCD 集群。

ETCD部署命令：

```bash
etcd --listen-client-urls http://0.0.0.0:2379 \
     --advertise-client-urls http://0.0.0.0:2379 &
```

::::{tab-set}

:::{tab-item}  进程部署

openYuanrong datasystem 进程部署主要通过 dscli 工具，在使用前请确保在两个节点中已安装 openYuanrong datasystem wheel 包。

分别在两个节点执行如下命令：

```bash
# 指定UCX使用的RDMA传输模式
export UCX_TLS=rc_x
# （可选）配置UCX日志
export UCX_LOG_FILE=/tmp/ucx.log
export UCX_LOG_LEVEL=ERROR
# 启动datasystem worker
dscli start \
    -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --enable_rdma true \
    --arena_per_tenant 1
```
参数说明：
- `UCX_TLS`：用于选择UCX的RDMA传输模式，默认选用`"rc_x"` 加速型可靠连接传输模式，以获得最佳性能，若网卡不支持该模式，可选择`"rc"`、`"ud"`与`"dc"`等，详情参考[UCX环境配置](https://github.com/openucx/ucx/wiki/UCX-environment-parameters)
- `UCX_LOG_FILE`：指定UCX日志的输出文件路径（例如 `/tmp/ucx.log`）。仅当设置了`UCX_LOG_LEVEL`时生效。日志文件需确保运行用户有写入权限。
- `UCX_LOG_LEVEL`：设置UCX日志级别，可选值包括`FATAL`、`ERROR`、`WARN`、`INFO`、`DEBUG`、`TRACE`。建议生产环境使用 `ERROR` 或 `WARN`，调试时使用 `DEBUG`或`TRACE`
- `node_address`：当前节点的通信地址与端口。格式为 `IP:Port`，例如：`192.168.0.1:31501`。
- `etcd_address`：ETCD集群的访问地址列表。格式为多个 `IP:Port` 的逗号分隔字符串，例如：`192.168.1.100:2379,192.168.1.101:2379,192.168.1.102:2379`。
- `arena_per_tenant`：不应超过系统内存资源限制，避免初始化失败，默认值为`1`，在保证功能的前提下提供最快的启动速度

:::

:::{tab-item} K8S部署

#### 容器内 memlock 资源限制配置

如在容器环境下运行，需确保 memlock（内存锁定）资源限制为 unlimited，否则 RDMA 初始化可能失败。注意：Kubernetes 集群中每个节点都需要完成以下配置。具体配置如下：

 - **容器内非 Root 权限用户**：
   - 修改 Docker Daemon 全局配置 `/etc/docker/daemon.json`，添加：
     ```json
     {
       "default-ulimits": {
         "memlock": {
           "Name": "memlock",
           "Hard": -1,
           "Soft": -1
         }
       }
     }
     ```
   - 重启 Docker 服务：
     ```bash
     sudo systemctl daemon-reload
     sudo systemctl restart docker
     ```
   - 验证：
     ```bash
     kubectl exec -it <Pod-name> -- sh -c "ulimit -l"
     ```
     若输出为 `unlimited`，说明 memlock 资源限制已正确放开。

 - **容器内 Root 权限用户**：
   - 在 entrypoint 脚本（ `yuanrong-datasystem/k8s/docker/entrypoint/worker_entry.sh`）中添加如下内容，务必放在 `ilog "start worker" `之前：
     ```bash
     ulimit -l unlimited
     ```
   - 修改 entrypoint 脚本后，必须重新构建 Docker 镜像，这样可在容器启动并启用 RDMA 时自动解除内存锁定限制。

```bash
# 通过dscli获取helm chart包
dscli generate_helm_chart -o /tmp

# 通过源码获取helm chart包
git clone -b ${version} https://gitcode.com/openeuler/yuanrong-datasystem.git
cp -r yuanrong-datasystem/k8s/helm_chart/datasystem /tmp
```

命令运行成功后会在"/tmp"目录下生成helm chart目录。

编辑 `/tmp/datasystem/values.yaml` 对集群启动项进行配置：

```yaml
global:
  # 其他配置项...
  # 请注意分配的arenaPerTenant及sharedMemory大小与允许的资源匹配

  imageRegistry: ""
  images:
    # 镜像名称
    datasystem: "openyuanrong-datasystem:0.6.0"
  
  # （可选）UCX RDMA日志配置
  # 日志将保存至${logDir}/worker/ucx.log
  log:
    # 是否开启UCX RDMA日志
    enableUcxLog: false
    # 日志级别，建议生产环境使用ERROR/WARN，调试时可设为DEBUG/TRACE
    ucxLogLevel: "ERROR"

  etcd:
    # ETCD集群地址
    etcdAddress: "192.168.1.100:2379,192.168.1.101:2379,192.168.1.102:2379"
  
  performance:
    # arena数量不应超过系统内存资源限制，避免初始化失败
    # 默认值为1，在保证功能的前提下提供最快的启动速度
    arenaPerTenant: 1
    # 开启RDMA能力
    enableRdma: true
    # 指定UCX使用的RDMA传输模式
    # 默认选用"rc_x"加速型可靠连接传输模式，以获得最佳性能。
    # 若当前网卡或驱动不支持 "rc_x"，可尝试以下替代选项：
    #   - "rc"   : 可靠连接模式，兼容性更好；
    #   - "ud"   : 不可靠数据报模式，适用于低延迟、小消息场景；
    #   - "dc"   : 动态连接模式，适用于大规模节点通信（需Mellanox网卡支持）。
    # 更多传输模式及其详细说明，请参阅：
    # https://github.com/openucx/ucx/wiki/UCX-environment-parameters
    ucxTransportLayerSelection: "rc_x"
  
  # 挂载IB设备相关目录，默认已挂载/dev/infiniband与/sys/class/infiniband，无需重复挂载
  # 例外情况请参考以下格式自行挂载
  mount:
    - hostPath: ""
      mountPath: ""
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
# ds-worker-5cw42        1/1     Running      1 (2s ago)    13s   127.0.0.1   node1
# ds-worker-4wv63        1/1     Running      1 (10s ago)   23s   127.0.0.2   node2
```

:::

::::

## 快速验证

通过跨节点拉取数据的样例可快速验证RDMA的能力。

在节点1执行以下Python脚本：

```python
from yr.datasystem import KVClient

client = KVClient("192.168.0.1", 31501)
client.init()
key = "key"
expected_val = b"value"
client.set(key, expected_val)
print("[OK] Set value")
```



在节点2执行以下Python脚本：

```python
from yr.datasystem import KVClient

client = KVClient("192.168.0.2", 31501)
client.init()
key = "key"
expected_val = b"value"
val = client.get([key])
assert val[0] == expected_val
print("[OK] Get value")
```

> 注意：
> 
> 脚本中初始化KVClient的入参需要替换为节点1/节点2服务端组件的IP和端口号。

当脚本执行完均打印OK时说明验证成功。

## 推荐配置

为确保RDMA组件在生产环境中达到最佳性能与稳定性，请参考以下配置建议。


### 开启大页内存

开启大页内存可有效提升内存的分配与拷贝性能。部署前，请务必在运行服务的物理节点上手动预留大页内存，否则应用将无法启用该功能。详细操作步骤请参考附录：[大页内存配置指南](../appendix/hugepage_guide.md)。

关键配置约束（请在`values.yaml`中检查）：

1. 如需开启的大页内存大于默认值，请同时修改 `global.resources.datasystemWorker.limits.hugepages2Mi` 以及 `global.resources.datasystemWorker.requests.hugepages2Mi`，以确保资源申请和限制一致。
2. 物理节点预留的大页总量必须大于等于`global.resources.datasystemWorker.limits.hugepages2Mi`的配置值
3. `global.resources.sharedMemory`的设置值必须小于等于`hugepages2Mi`。
4. 严禁将`global.resources.datasystemWorker.limits.memory`设为`0`或极小值。请务必预留至少`512Mi`，以满足线程栈、动态库加载及运行时元数据的刚性需求，防止应用瞬间OOM Kill。

若使用Kubernetes，还需重启kubelet以使节点上报大页资源：

```bash
sudo systemctl restart kubelet

# 验证节点是否识别大页：
kubectl describe node <node-name> | grep -i huge
```

运行环境开启大页内存之后，启动数据系统服务端组件时需要启用大页内存配置项：

::::{tab-set}

:::{tab-item}  进程部署

```bash
export UCX_TLS=rc_x
dscli start -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --enable_rdma true \
    --arena_per_tenant 1 \
    --enable_huge_tlb true
```

:::

:::{tab-item}  K8s部署

编辑 `/tmp/datasystem/values.yaml` 开启大页内存：

```yaml
global:
  # 其他配置项...

  imageRegistry: ""
  images:
    # 镜像名称
    datasystem: "openyuanrong-datasystem:0.6.0"
  
  etcd:
    # ETCD集群地址
    etcdAddress: "192.168.1.100:2379,192.168.1.101:2379,192.168.1.102:2379"
  
  performance:
    # arena数量不应超过系统内存资源限制，避免初始化失败
    # 默认值为1，在保证功能的前提下提供最快的启动速度
    arenaPerTenant: 1
    # 开启RDMA能力
    enableRdma: true
    # 选择RDMA高性能传输模式
    ucxTransportLayerSelection: "rc_x"
    # 开启大页内存
    enableHugeTlb: true
    # 指定分配的 2MiB 大页内存总量（例如 "16Gi" 表示 16 GiB，即 8192 个 2MiB 页面）
    hugepages2Mi: "16Gi"
```

:::

::::

### 绑定NUMA节点

绑定NUMA节点可减少远程内存访问，提升缓存访问性能，进程部署时绑定NUMA节点命令如下：

```bash
# 指定UCX使用的RDMA传输模式
export UCX_TLS=rc_x
# 启动datasystem worker
dscli start \
    --cpunodebind 0 \
    --localalloc \
    -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --enable_rdma true \
    --arena_per_tenant 1 \
    --enable_huge_tlb true
    
```

表示绑定到 NUMA 节点 0 的 CPU，并在节点 NUMA 0 分配内存。
更多 dscli绑定numa节点 部署详细信息请参考：[dscli命令参数说明](../deployment/dscli.md#命令行参数说明)。

## 常见问题

1. **如何验证节点是否具备 RDMA 网卡？**

   部署 RDMA 功能前，需确认节点已配备 RDMA 网卡硬件及相应驱动。可通过以下命令进行验证：

   ```bash
   # 检查 RDMA 设备列表
   ibv_devices

   # 使用 lspci 检查 InfiniBand/RDMA 网卡硬件
   lspci | grep -i "infiniband\|rdma\|mellanox\|connectx"

   # 检查 RDMA 相关内核模块是否加载
   lsmod | grep -E "ib_core|mlx4_core|mlx5_core|rdma"
   ```

   若 `ibv_devices` 输出显示设备列表（如 `mlx5_0`），说明 RDMA 网卡已正确识别。若无输出或报错，请检查：
   - RDMA 网卡是否正确安装
   - `rdma-core` 软件包是否已安装
   - RDMA 内核模块是否已加载

2. **容器环境运行 RDMA 有哪些特殊要求？**

   在容器环境（如 Kubernetes）中运行 RDMA，需满足以下条件：

   - **memlock 资源限制**：必须设置为 `unlimited`，否则 RDMA 内存注册将失败。配置方法详见[部署指南](#部署指南)章节中的容器内 memlock 资源限制配置。

   - **设备挂载**：容器需挂载 RDMA 相关设备目录：
     - `/dev/infiniband`：RDMA 设备文件
     - `/sys/class/infiniband`：RDMA 设备属性信息

3. **如何测试 RDMA 网络连通性？**

   可使用 `ib_write_bw` 工具测试两节点之间的 RDMA 写带宽，验证 RDMA 网络连通性。首先需安装 `perftest` 工具包：

   ```bash
   # 安装 perftest 工具（以 openEuler 为例）
   yum install perftest
   ```

   在服务端节点执行：

   ```bash
   # 启动 ib_write_bw 服务端，-d 指定设备名，-R 表示使用 RDMA 写
   ib_write_bw -d mlx5_0 -R
   ```

   在客户端节点执行：

   ```bash
   # 连接服务端进行带宽测试
   ib_write_bw -d mlx5_0 -R <server_ip>
   ```

   参数说明：
   - `-d mlx5_0`：RDMA 设备名称（可通过 `ibv_devices` 查看）
   - `-R`：使用 RDMA 写操作
   - `<server_ip>`：服务端节点的 IP 地址

   正常输出示例：

   ```bash
    #bytes     #iterations    BW peak[MB/sec]    BW average[MB/sec]   MsgRate[Mpps]
    65536      5000            9993.41            5032.27              0.080516
   ```

   若测试失败，请检查 RDMA 网卡配置、网络链路连通性及防火墙设置。

4. **UCX_TLS=rc_x 环境变量的含义是什么？**

   `UCX_TLS=rc_x` 是 UCX 传输层选择的环境变量配置，指定使用 **RDMA RC (Reliable Connection) 传输模式**。

   **关键特性**：

   - **纯 RDMA 模式**：设置 `UCX_TLS=rc_x` 后，UCX 将**仅使用 RDMA RC 传输，不会回退到 TCP**。
   - **严格验证**：若 RDMA 不可用或通信失败，系统将直接报错而非降级使用 TCP。
   - **成功即 RDMA**：若应用在此模式下成功运行，则必然使用 RDMA 传输，可**确认 RDMA 功能正常工作**。

   这意味着：**开启 `UCX_TLS=rc_x` 后能跑通，必定是使用 RDMA；若不能跑通，说明 RDMA 环境配置有问题**。

   **替代传输模式**：

   若当前网卡或驱动不支持 `rc_x`，可尝试以下替代选项（详见[部署指南](#部署指南)）：

   | 模式 | 说明 |
   | --- | --- |
   | `rc` | 可靠连接模式，兼容性较好 |
   | `ud` | 不可靠数据报模式，适用于低延迟、小消息场景 |
   | `dc` | 动态连接模式，适用于大规模节点通信（需 Mellanox 网卡支持） |

   **验证 RDMA 是否生效**：

   可通过 UCX 日志确认 RDMA 传输是否生效：


   ```bash
   export UCX_LOG_FILE=/tmp/ucx.log
   export UCX_LOG_LEVEL=INFO
   # 启动 worker 后检查日志
   grep -i "rc\|rdma" /tmp/ucx.log
   ```

   若日志中出现 RC 相关传输信息，说明 RDMA 已正确启用。