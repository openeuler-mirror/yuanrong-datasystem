# RH2D最佳实践

远端主机到设备数据传输（Remote Host to Device，RH2D）是一种基于昇腾（Ascend）NPU（Neural Processing Unit）的，支持从远端节点主机侧共享内存到设备侧 HBM 内存的跨节点数据传输机制。通过 NPU 驱动及 CANN 工具包的支持，RH2D 提供了一个高效的数据传输通道，支持异构计算单元间的高效协同，支持大规模的并行计算、AI 训练等高性能计算任务。

openYuanrong datasystem 现已集成对 RH2D over RoCE (RDMA over Converged Ethernet) 的支持，实现了分布式缓存在物理节点之间的硬件级加速。基于 RH2D 构建的全局缓存抽象，透明化了下层拓扑，使得应用能够以极简的编程方式，在跨节点通信场景中实现数据直通，充分释放底层硬件性能。

## 源码编译安装

源码编译安装前请确保编译环境中在正常编译依赖的基础上具备如下软件依赖：

|软件名称|版本|作用|
|-|-|-|
|Ascend-hdk-npu-driver|25.5.0+|Ascend驱动依赖|
|Ascend-cann-toolkit|8.2.RC1+|Ascend工具包依赖|

### 下载源码

```bash
git clone https://gitcode.com/openeuler/yuanrong-datasystem.git
```

### 编译

RH2D需要异构能力的编译。默认配置下数据系统会启用异构能力的编译，需要编译环境中具备 CANN 依赖。

```bash
bash build.sh
```


编译成功后，会在output目录下产生如下编译产物：

```text
output/
├── openyuanrong_datasystem-x.x.x-cp311-cp311-manylinux_2_34_x86_64.whl
└── yr-datasystem-vx.x.x.tar.gz
```

### 安装

```bash
pip install output/openyuanrong_datasystem-*.whl --force-reinstall
```

## 部署指南

部署前，请确保满足以下必要条件：
1. 节点准备：须至少在 2 个 已配备 NPU 硬件并安装相应驱动、固件和工具包的节点上部署服务端组件。
2. 集群依赖：openYuanrong datasystem 的集群管理功能依赖于 ETCD，因此需预先搭建并确保一个稳定可用的 ETCD 集群。

ETCD部署命令样例如下：

```bash
# etcd启动指令
etcd --name etcd-single --data-dir /root/.datasystem/etcd-data \
     --listen-client-urls http://0.0.0.0:2345 --advertise-client-urls http://0.0.0.0:2345 \
     --listen-peer-urls http://0.0.0.0:2346 --initial-advertise-peer-urls http://0.0.0.0:2346 \
     --initial-cluster etcd-single=http://0.0.0.0:2346 > /dev/null 2>&1 &
# etcd状态检查
etcdctl --endpoints "http://0.0.0.0:2345" endpoint health
```

::::{tab-set}

:::{tab-item}  进程部署

openYuanrong datasystem 进程部署主要通过 dscli 工具，在使用前请确保在两个节点中已安装 openYuanrong datasystem wheel 包。

> **容器内进程部署注意事项**
>
> 🔔 **重要提示**：在拉起容器时，请挂载如下宿主机目录，确保容器内可以使用驱动依赖及NPU相关二进制：
> | 宿主机路径 | 容器挂载路径 |
> |--|--|
> | /usr/local/Ascend/driver | /usr/local/Ascend/driver |
> | /usr/local/bin/npu-smi | /usr/local/bin/npu-smi |
> | /usr/local/Ascend/driver/tools/hccn_tool | /usr/local/bin/hccn_tool |
> | /etc/hccn.conf | /etc/hccn.conf |
> | /etc/ascend_install.info | /etc/ascend_install.info |
>
>  并配置设备（需要根据实际情况配置，例如如果设备上没有NPU 7，那么/dev/davinci7不存在），确保容器内可以使用NPU设备：
> | 设备样例 |
> |--|
> | /dev/davinci0 |
> | /dev/davinci1 |
> | /dev/davinci2 |
> | /dev/davinci3 |
> | /dev/davinci4 |
> | /dev/davinci5 |
> | /dev/davinci6 |
> | /dev/davinci7 |
> | /dev/davinci_manager |
> | /dev/devmm_svm |
> | /dev/hisi_hdc |
>
> 以及注意针对大页配置和锁定内存需要**特权容器**配置。
>
> **容器启动样例命令如下**：
>
> ```bash
> docker run -itd -u root --ipc host --net host --privileged=true \
>   --device=/dev/davinci0 --device=/dev/davinci1 --device=/dev/davinci2 \
>   --device=/dev/davinci3 --device=/dev/davinci4 --device=/dev/davinci5 \
>   --device=/dev/davinci6 --device=/dev/davinci7 --device=/dev/davinci_manager \
>   --device=/dev/devmm_svm --device=/dev/hisi_hdc \
>   -v /usr/local/Ascend/driver:/usr/local/Ascend/driver \
>   -v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi \
>   -v /usr/local/Ascend/driver/tools/hccn_tool:/usr/local/bin/hccn_tool \
>   -v /etc/hccn.conf:/etc/hccn.conf \
>   -v /etc/ascend_install.info:/etc/ascend_install.info \
>   --name "<docker name>" <image_name> /bin/bash
> ```

分别在两个节点执行如下命令：

```bash
# 根据共享内存大小提升最大锁定内存大小
ulimit -l unlimited
# 启动datasystem worker
dscli start \
    -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --arena_per_tenant 1 \
    --remote_h2d_device_ids "0"
```
参数说明：
- `node_address`：当前节点的通信地址与端口。格式为 `IP:Port`，例如：`192.168.0.1:31501`。
- `etcd_address`：ETCD集群的访问地址列表。格式为多个 `IP:Port` 的逗号分隔字符串，例如：`192.168.1.100:2379,192.168.1.101:2379,192.168.1.102:2379`。
- `arena_per_tenant`：不应超过系统内存资源限制，避免初始化失败，初始建议值为`1`，在保证功能的前提下提供最快的启动速度。
- `remote_h2d_device_ids`：在worker端提供NPU设备ID并启用RH2D功能，默认值为空表示不启用。worker使用多个NPU需用逗号分隔，例如：`0,1,2,3,4,5,6,7`。


:::

::::

## 快速验证

通过跨节点拉取数据的样例可快速验证RH2D的能力。

在节点1执行以下Python脚本：

```python
import acl
from yr.datasystem import (HeteroClient, Blob, DeviceBlobList)

acl.init()
device_id = 0
acl.rt.set_device(device_id)
client = HeteroClient("192.168.0.1", 31501, enable_remote_h2d=True)
client.init()
expected_val = "value"
blob_size = len(expected_val)
dev_ptr, _ = acl.rt.malloc(blob_size, 0)
acl.rt.memcpy(dev_ptr, blob_size, acl.util.bytes_to_ptr(expected_val.encode()), blob_size, 1)
in_data_blob_list = [DeviceBlobList(device_id, [Blob(dev_ptr, blob_size)])]
client.mset_d2h(["key"], in_data_blob_list)
print("[OK] Set value")
```


在节点2执行以下Python脚本：

```python
import acl
from yr.datasystem import (HeteroClient, Blob, DeviceBlobList)

acl.init()
device_id = 2
acl.rt.set_device(device_id)
client = HeteroClient("192.168.0.2", 31501, enable_remote_h2d=True)
client.init()
key = "key"
expected_val = "value"
blob_size = len(expected_val)
dev_ptr, _ = acl.rt.malloc(blob_size, 0)
out_data_blob_list = [DeviceBlobList(device_id, [Blob(dev_ptr, blob_size)])]
client.mget_h2d([key], out_data_blob_list, 60000)
output_byte = bytes("0", "utf-8").zfill(blob_size)
output_ptr = acl.util.bytes_to_ptr(output_byte)
acl.rt.memcpy(output_ptr, blob_size, dev_ptr, blob_size, 2)
assert output_byte == expected_val.encode()
print("[OK] Get value")
```

> 注意：
> 
> 脚本中初始化HeteroClient的入参需要替换为节点1/节点2服务端组件的IP和端口号。

当脚本执行完均打印OK时说明验证成功。

## 推荐配置

为确保RH2D组件在生产环境中达到最佳性能与稳定性，请参考以下配置建议。


### 开启共享内存大页内存

开启大页内存可有效提升内存的分配与拷贝性能。在910B机器的RH2D场景，驱动版本25.5.0+支持将大于21G的共享内存注册到NPU，在此时需要启用大页内存。开启大页内存可参考附录文档：[大页内存配置指南](../appendix/hugepage_guide.md)。

运行环境开启大页内存之后，启动数据系统服务端组件时需要启用大页内存配置项：

::::{tab-set}

:::{tab-item}  进程部署

```bash
dscli start -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --arena_per_tenant 1 \
    --remote_h2d_device_ids "0" \
    --enable_huge_tlb true
```

:::

::::

### Worker启用多个NPU

在多client进程场景建议worker配置使用多个NPU，轮询使用这些NPU建立传输连接，这可以有效提高总传输带宽。

部署时启用多个NPU的样例命令如下：

::::{tab-set}

:::{tab-item}  进程部署

```bash
dscli start -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --arena_per_tenant 1 \
    --remote_h2d_device_ids "0,1,2,3,4,5,6,7" \
    --enable_huge_tlb true
```

表示worker在部署时使用0-7的全部NPU。

:::

::::


### 绑定NUMA节点

绑定NUMA节点可减少远程内存访问，提升缓存访问性能。并且在RH2D场景，NUMA和NPU的亲和性也会提高传输性能，作为参考在910B设备上经测试单client进程场景会有约2GB/s的差异。

> **注意**：当启用共享内存大页时，即使设置了 cpunodebind 参数，内存分配仍将根据大页分配到 NUMA 节点。

部署时绑定NUMA节点的样例命令如下：

::::{tab-set}

:::{tab-item}  进程部署

```bash
dscli start \
    --cpunodebind 0 \
    --localalloc \
    -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --arena_per_tenant 1 \
    --remote_h2d_device_ids "0"
```


表示绑定到 NUMA 节点 0 的 CPU，并在节点 NUMA 0 分配内存。
更多 dscli 绑定 NUMA 节点部署详细信息请参考：[dscli命令参数说明](../deployment/dscli.md#命令行参数说明)。

查看NPU和NUMA亲和性并通过CPU核心作对应的命令如下：
```bash
# 显示NPU和CPU核心的亲和性对应情况
npu-smi info -t topo
# 显示每个NUMA节点的CPU核心分配情况
lscpu | grep -i numa
```

:::

::::

### worker到worker的对象数据批量获取

在RH2D场景支持批量化获取，在这个基础上通过启用worker到worker的对象数据批量获取可以进一步减少请求耗时。

部署时开启worker到worker的对象数据批量获取的命令如下：

::::{tab-set}

:::{tab-item}  进程部署

```bash
dscli start -w \
    --worker_address "${node_address}" \
    --etcd_address "${etcd_address}" \
    --arena_per_tenant 1 \
    --remote_h2d_device_ids "0" \
    --enable_worker_worker_batch_get true
```

:::

::::

## 性能测试

在启动etcd并根据需求（共享内存大小，NPU IDs，NUMA亲和，共享内存大页，worker到worker对象数据批量获取，等）部署完worker之后，通过tests/benchmark/hetero_h2d_d2h_benchmark.py的性能测试脚本可以进行基础的性能验证。运行可以选择单client进程或多client进程场景，以及各种数据大小规格。

在节点1执行以下命令：

```bash
python hetero_h2d_d2h_benchmark.py -i 192.168.0.1 -p 31501 --set-only --no-warmup -n 1process_1thread_32key_1024KB
```

将生成的keys.json通过scp拷贝到节点2的benchmark路径，并在节点2执行以下命令：

```bash
python hetero_h2d_d2h_benchmark.py -i 192.168.0.2 -p 31501 --get-only --no-warmup -n 1process_1thread_32key_1024KB
```

> 注意事项：
> 
> 1. 参数包括
>
> | 参数名称 | 描述 | 默认值 |
> |--|--|--|
> | --set-only | 仅运行MSetD2H操作 | False |
> | --get-only | 仅运行MGetH2D操作 | False |
> | -i, --ip | Worker组件的IP地址 | 127.0.0.1 |
> | -p, --port | Worker组件的端口号 | 31699|
> | -k, --keys | 保存对象key的路径。set-only场景生成对象key，再由get-only场景读取生成的对象key | ./keys.json |
> | --no-warmup | 跳过预热。在RH2D场景预热是无效的 | False|
> | -n, --name | 测试名称。如果未设置则运行所有测试，而这在仅运行set或get的场景里是无法跑通的 | None |
> | -d, --deviceid | 初始Device ID。在多进程场景每个进程使用的Device ID会增加 | 3 |
> | --get-multiplier | 倍数重复相同的get请求 | 1 |
> | --show-all-request-times | 展示每个请求的时延信息 | False |
>
> 入参IP和端口号需要替换为节点1/节点2 Worker组件的IP和端口号。
>
> 2. 根据需求设置-n参数，来选择测试场景
>
> | 测试名称 | 描述 |
> |--|--|
> | 1process_1thread_1key_72KB         | 1 client 进程 1 key * 61 blobs * 72KB                          |
> | 1process_1thread_1key_144KB        | 1 client 进程 1 key * 61 blobs * 144KB                         |
> | 1process_1thread_1key_1024KB       | 1 client 进程 1 key * 28 blobs * 1024KB                        |
> | 1process_1thread_32key_72KB        | 1 client 进程 32 keys * 61 blobs * 72KB                        |
> | 1process_1thread_32key_144KB       | 1 client 进程 32 keys * 61 blobs * 144KB                       |
> | 1process_1thread_32key_1024KB      | 1 client 进程 32 keys * 28 blobs * 1024KB                      |
> | 1process_8thread_32key_72KB        | 1 client 进程 32 keys * 61 blobs * 72KB (8 threads)            |
> | 1process_8thread_32key_144KB       | 1 client 进程 32 keys * 61 blobs * 144KB (8 threads)           |
> | 1process_8thread_32key_1024KB      | 1 client 进程 32 keys * 28 blobs * 1024KB (8 threads)          |
> | 8process_1thread_1key_72KB         | 8 client 进程 1 key * 61 blobs * 72KB                          |
> | 8process_1thread_1key_144KB        | 8 client 进程 1 key * 61 blobs * 144KB                         |
> | 8process_1thread_1key_1024KB       | 8 client 进程 1 key * 28 blobs * 1024KB                        |
> | 8process_1thread_32key_72KB        | 8 client 进程 32 keys * 61 blobs * 72KB                        |
> | 8process_1thread_32key_144KB       | 8 client 进程 32 keys * 61 blobs * 144KB                       |
> | 8process_1thread_32key_1024KB      | 8 client 进程 32 keys * 28 blobs * 1024KB                      |
> | 1process_1key_16blob_8MB           | 1 client 进程 1 key * 16 blobs * 8MB                           |
> | 1process_1key_32blob_4MB           | 1 client 进程 1 key * 32 blobs * 4MB                           |
> | 1process_1key_64blob_2MB           | 1 client 进程 1 key * 64 blobs * 2MB                           |
> | 1process_1key_128blob_1MB          | 1 client 进程 1 key * 128 blobs * 1MB                          |
> | 1process_1key_256blob_512KB        | 1 client 进程 1 key * 256 blobs * 512KB                        |
> | 1process_1key_512blob_256KB        | 1 client 进程 1 key * 512 blobs * 256KB                        |
>
> 3. 运行完成get的部分之后，数据会被清除，再运行需要重新完成set操作
> 4. 针对较大blob num * blob size的场景，可能会需要配置DS_DEVICE_ACL_SIZE环境变量，其默认值为100MB。样例命令如下：
> ```bash
> export DS_DEVICE_ACL_SIZE=419430400
> ```

## vllm-ascend端到端运行

vllm-ascend环境配置和部署请参考[vllm-ascend + yuanrong connector 调测指南](https://gitcode.com/openeuler/yuanrong-datasystem/blob/master/tests/kvconnector/README.md)以及[[PATCH] Implement yuanrong backend](https://gitcode.com/openeuler/yuanrong-datasystem/blob/master/tests/kvconnector/patch/v0.13.0rc1/0001-Implement-yuanrong-backend.patch)。worker组件的启动和启用RH2D的参数参考本文档中的部署指南。

vllm-ascend样例命令（调整自`[PATCH] Implement yuanrong backend`内容）：
```bash
export PYTHONHASHSEED=0
export DS_WORKER_ADDR="${WORKER_IP}:31501"
export DS_ENABLE_EXCLUSIVE_CONNECTION=0
# 启用client侧RH2D功能，默认为0表示不启用
export DS_ENABLE_REMOTE_H2D=1

python3 -m vllm.entrypoints.openai.api_server \
    --model /xxxxx/Qwen2.5-7B-Instruct \
    --port 8100 \
    --trust-remote-code \
    --enforce-eager \
    --no_enable_prefix_caching \
    --tensor-parallel-size 1 \
    --data-parallel-size 1 \
    --max-model-len 10000 \
    --block-size 128 \
    --max-num-batched-tokens 4096 \
    --kv-transfer-config \
    '{
    "kv_connector": "AscendStoreConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
        "lookup_rpc_port": "1",
        "backend": "yuanrong"
    }
}'
```

## 常见问题FAQ

::::{tab-set}

:::{tab-item}  错误码500000

acl错误码500000对应未知内部错误，但在容器场景有可能是驱动未在容器中挂载所致，请参考部署指南中的容器内进程部署注意事项。

:::

:::{tab-item}  错误码328004

libRA错误码328004对应网口down，在机器上确定有NPU设备时有可能是光模块故障或链路不通等环境问题，所以RH2D over RoCE无法启用。

确认本机RoCE端口是否正常工作，以及RoCE网络是否互通的命令如下：

```bash
# 查看NPU拓扑结构，正常表现为NPU对之间显示为HCCS或HCCS_SW，无NA、SYS、PHB等降级连接
npu-smi info -t topo
# 检查NPU的RoCE光模块状态，present: present代表光模块已成功检测到，反之则未被检测到
hccn_tool -i <本地NPU卡号> -optical -g
# 检查NPU的RoCE链路全局状态，link status: UP代表NPU的RoCE物理链路已正常启用且连接有效
for i in {0..7}; do hccn_tool -i $i -link -g; done
# 获取IP地址和子网掩码
hccn_tool -i <本地NPU卡号> -ip -g
# 获取指定设备到目的地的地址的ping结果
hccn_tool -i <本地NPU卡号> -ping -g address <目标NPU的IP>
```

:::

:::{tab-item}  错误码107001

acl错误码107001对应无效的Device ID，请检查Device ID是否合法。可能的情况是提供的NPU卡号超出可用设备范围，例如
1. 在只有0-7的8张卡可用的情况下配置使用了卡号=8超过了范围
2. 受到环境变量ASCEND_RT_VISIBLE_DEVICES的影响导致设备映射关系变化，在ASCEND_RT_VISIBLE_DEVICES = 4, 5, 6, 7的情况下4-7卡映射到了ID 0-3，这时使用了卡号=4就超过了范围

:::

:::{tab-item} RegisterHostMemory Failed

"RegisterHostMemory Failed"错误日志表示将共享内存注册到NPU的操作失败了，可能的情况是
1. 910B环境25.5.0以下驱动版本不支持设备侧2M大页映射的功能，所以有21G共享内存的限制
2. 910B环境25.5.0及以上的驱动版本需要开启大页支持大于21G的共享内存

:::

::::

其他acl错误码可以参考[aclError定义](https://www.hiascend.com/document/detail/zh/CANNCommunityEdition/850/API/appdevgapi/aclcppdevg_03_1345.html)