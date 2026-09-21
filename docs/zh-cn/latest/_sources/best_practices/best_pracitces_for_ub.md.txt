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

编译数据系统时，请确保编译环境中的 URMA 版本与运行环境中的 URMA 版本一致，否则可能导致 UB 能力异常。可通过以下命令查看当前环境中的 URMA 版本：

```bash
rpm -qa | grep urma
```

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

### 通过下载包编译（可选）

默认情况下，编译时 URMA 的头文件和共享库来自编译环境的系统路径（`/usr/include`、`/usr/lib64`），要求编译机预先安装 URMA SDK。若编译环境不便于安装 URMA SDK，可改用下载包方式：编译时从指定 URL 下载 URMA 头文件与共享库压缩包，自动定位并使用，编译产物（whl 包与 SDK 的 `lib/` 目录）会内置 URMA 相关的 6 个共享库（`libtpsa.so`、`libummu.so`、`liburma.so`、`liburma_common.so`、`liburma_ubagg.so`、`liburma-udma.so`），运行环境无需再单独安装 URMA 库。

下载包的内部布局应为：

```text
<任意顶层目录>/
├── include/
│   ├── urma_api.h
│   ├── urma_perf.h
│   └── urma_ubagg.h
└── lib/
    ├── libtpsa.so / libtpsa.so.0
    ├── libummu.so / libummu.so.0
    ├── liburma.so / liburma.so.0
    ├── liburma_common.so / liburma_common.so.0
    ├── liburma_ubagg.so / liburma_ubagg.so.0
    └── liburma-udma.so / liburma-udma.so.0
```

> 说明：压缩包内部的中间层目录名（如示例中的 `umdk_lib`）不做硬性要求，编译脚本会递归搜索 `urma_api.h` 与 `liburma.so` 自动定位 `include` 与 `lib` 根，头文件会被重组到 `ub/umdk/urma/` 路径下以匹配源码的 `#include <ub/umdk/urma/*.h>`。只需保证压缩包内同时存在上述头文件与 6 个共享库即可。

::::{tab-set}

:::{tab-item} CMake 编译

编辑 `cmake/external_libs/urma.cmake`，将 `URMA_PKG_URL` 与 `URMA_PKG_SHA256` 设置为压缩包的下载地址与 SHA256 校验值（文件顶部已有默认的空值声明，取消注释或直接修改即可）：

```cmake
set(URMA_PKG_URL "https://example.com/urma_package.zip" CACHE STRING "URMA package download URL (empty = use system URMA SDK)")
set(URMA_PKG_SHA256 "<sha256-of-the-zip>" CACHE STRING "URMA package SHA256 checksum")
```

留空则回退到系统路径方式（默认行为）。设置后执行：

```bash
bash build.sh -M on -X off
```

:::

:::{tab-item} Bazel 编译

编辑 `.bazelrc`，在 `build:urma` 段下取消注释（或新增）两行 `--repo_env`，填入压缩包的下载地址与 SHA256 校验值：

```text
build:urma --repo_env=URMA_PKG_URL=https://example.com/urma_package.zip
build:urma --repo_env=URMA_PKG_SHA256=<sha256-of-the-zip>
```

注释掉这两行则回退到系统路径方式（默认行为）。设置后执行：

```bash
bazel build //bazel:datasystem_wheel --config=urma --config=release
```

:::

::::

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
cp -r yuanrong-datasystem/k8s/helm_chart/datasystem /tmp
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
# ds-worker-5cw42        1/1     Running      1 (2s ago)    13s   127.0.0.1   node1
# ds-worker-4wv63        1/1     Running      1 (10s ago)   23s   127.0.0.2   node2
```

:::

::::


## 快速验证

通过跨节点拉取数据的样例可快速验证UB的能力。

在节点1执行以下Python脚本：

```python
from yr.datasystem import KVClient

client = KVClient("192.168.0.1:31501", 31501)
client.init()
key = "key"
expected_val = b"value"
client.set(key, expected_val)
print("[OK] Set value")
```



在节点2执行以下Python脚本：

```python
from yr.datasystem import KVClient

client = KVClient("192.168.0.2:31501", 31501)
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

为确保灵衢（UB）组件在生产环境中达到最佳性能与稳定性，请参考以下配置建议。

### 远端端口健康验证与隔离

远端验证的隔离以 `QueryUbPortHealth` 返回的有效端口事实为准：全部端口 BAD 才确认隔离，任一端口 GOOD 即恢复可用性并停止待重试查询。
CQE 错误和被动健康摘要只是验证线索，不能替代查询结果。Client 和 Worker 复用公共
`RemoteUbPortHealthVerifier`、既有查询线程池及总计 4 个并发槽，不新增轮询线程或查询协议。

**隔离的作用范围**：远端 Worker 的 UB 隔离只影响**写路由**（Set/MSet 不再选择该 Worker），不拦截读请求。
Get 是否成功由 Worker 的权威判决决定：Worker 本机允许 UB 回写则正常返回，本机确认全部端口 BAD 则不提交 UB 写入，
并按既有策略走 TCP fallback 或返回 UB 读源不可用（Client 可继续尝试其他副本）。
只有 **Client 本机全部 UB 端口 BAD** 才会在 Client 入口直接拒绝 Get/Set（不发送 RPC）。
隔离由探测结果驱动、异步生效：业务响应携带的全 BAD 摘要只触发端口查询，查询确认（并通过对端、incarnation 与
health epoch 校验）后才建立/解除写隔离，生效延迟约为一次查询 RPC 往返。

| 状态或事件 | 调度行为 |
| --- | --- |
| 新 peer、新 incarnation，或 GOOD 后空闲状态的新验证轮次 | 首次查询立即到期；实际执行仍受并发槽限制 |
| RPC 失败、UNKNOWN、pending、过期或冲突响应 | 验证需求仍存在时，下一次重试在 `[1000, 30000]` ms 内均匀随机 |
| 有效全 BAD 查询结果 | 保持隔离，并按相同随机规则继续查询恢复状态 |
| 有效 GOOD 查询结果 | 立即清除隔离；若无新的 trigger 或摘要 hint 则停止后续查询，否则按随机规则排入下一轮 |
| 不支持查询 RPC | 当前固定等待 30s，不缩短为普通随机重试 |
| 被动摘要 hint | 不缩短或延后已有 deadline；空闲 peer 的新全 BAD 线索立即开启验证；隔离且无 deadline 时只补排一次随机重试；查询在途时仅置 `summaryHintPending`，待本次查询完成后按随机规则排程 |

RPC timeout 与本地 `urma_user_ctl` 监控仍为 1s；30s 只约束下一次恢复查询的排程间隔，不是端到端恢复承诺。
随机重试区间为 `[1000, 30000]` ms：单次恢复查询的额外等待最长为 30 秒、平均约 15.5 秒。
一个 peer 的结果不会压缩其他 peer 的 deadline。拓扑移除或 incarnation 变化会淘汰旧状态，owner 生命周期内单调递增的 generation 会拒绝迟到 ticket。
每个 verifier 只初始化一次随机 seed，再由 seed、HostPort、incarnation 和 generation 派生延迟；测试可使用私有固定 seed/区间入口，生产接口不暴露随机策略。


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
