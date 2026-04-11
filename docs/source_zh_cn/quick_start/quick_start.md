(overview-getting-started)=

# 入门

## 安装 openYuanrong datasystem

### pip 方式安装

openYuanrong datasystem 已发布至 [PyPI](https://pypi.org/project/openyuanrong-datasystem/)，您可以通过 pip 直接安装。

**前置要求**

通过pip安装 openYuanrong datasystem 之前，请确保满足以下要求：

- **Python 版本**：Python 3.9、3.10 或 3.11
- **操作系统**：Linux（推荐 glibc 2.34+）
- **架构**：x86-64

您可以通过以下命令检查：

```bash
# python版本
python --version
# 操作系统
uname -s
# 架构
uname -m
# glibc版本
ldd --version
```

**安装完整发行版**（包含 Python SDK、C++ SDK 以及命令行工具）：

```bash
pip install openyuanrong-datasystem
```


**验证安装**：

安装完成后，您可以通过以下命令验证安装是否成功：

```bash
python -c "import yr.datasystem; print('openYuanrong datasystem installed successfully')"

dscli --version
```


### 源码编译方式安装

使用源码编译方式安装 openYuanrong datasystem 可以参考文档：[源码编译安装 openYuanrong datasystem](../installation/build_guide/cmake_build.md#源码编译安装)

## 部署 openYuanrong datasystem

### 进程部署

- 准备ETCD
  
  openYuanrong datasystem 的集群管理依赖 ETCD，请先在后台启动单节点 ETCD（示例端口 2379）：
  ```bash
  etcd --listen-client-urls http://0.0.0.0:2379 \
       --advertise-client-urls http://localhost:2379 &
  ```
- 一键部署

  安装 openYuanrong datasystem 完整发行版后，即可通过随包自带的 dscli 命令行工具一键完成集群部署。在当前启动一个监听端口号为 31501 的服务端进程：
  ```bash
  dscli start -w --worker_address "127.0.0.1:31501" --etcd_address "127.0.0.1:2379"
  ```

- 一键卸载
  ```bash
  dscli stop --worker_address "127.0.0.1:31501"
  ```

更多进程部署参数与部署方式请参考文档：[openYuanrong datasystem 进程部署](../deployment/deploy.md#裸进程部署worker)

### Kubernetes 部署

openYuanrong datasystem 还提供了基于 Kubernetes 容器化部署方式，部署前请确保部署环境集群已就绪 Kubernetes、Helm 及可访问的 ETCD 集群。

- 获取 openYuanrong datasystem helm chart 包

  安装 openYuanrong datasystem 完整发行版后，即可通过随包自带的 dscli 命令行工具在当前路径下快速获取 helm chart 包：
  ```bash
  dscli generate_helm_chart -o ./
  ```

- 编辑集群部署配置

  openYuanrong datasystem 通过 ./datasystem/values.yaml 文件进行集群相关配置，其中必配项如下：

  ```yaml
  global:
    # 其他配置项...

    # 镜像仓地址
    imageRegistry: ""
    # 镜像名字和镜像tag
    images:
      datasystem: "openyuanrong-datasystem:0.5.0"
    
    etcd:
      # ETCD集群地址
      etcdAddress: "127.0.0.1:2379"
  ```

- 集群部署

  Helm 会提交 DaemonSet，按节点依次拉起 openYuanrong datasystem 实例：

  ```bash
  helm install openyuanrong_datasystem ./datasystem
  ```

- 集群卸载

  ```bash
  helm uninstall openyuanrong_datasystem
  ```

更多 openYuanrong datasystem Kubernetes 高级参数配置请参考文档：[openYuanrong datasystem Kubernetes 部署](../deployment/deploy.md#kubernetes部署worker)

## 开发指南

### 异构对象

异构对象实现对 HBM 内存的抽象管理，能够高效实现 D2D/H2D/D2H 的数据传输，加速 AI 训推场景数据读写。

主要应用场景

- **LLM 长序列推理 KVCache**：基于异构对象提供分布式多级缓存 (HBM/DRAM/SSD) 和高吞吐 D2D/H2D/D2H 访问能力，构建分布式 KV Cache，实现 Prefill 阶段的 KVCache 缓存以及 Prefill/Decode 实例间 KV Cache 快速传递，提升推理吞吐。
- **模型推理实例 M->N 快速弹性**：利用异构对象的卡间直通及 P2P 数据分发能力实现模型参数快速复制。
- **训练场景 CheckPoint 快速加载到 HBM**：各节点将待恢复的 Checkpoint 分片加载到异构对象中，利用异构对象的卡间直通传输及 P2P 数据分发能力，快速将 Checkpoint 传递到各节点 HBM。

通过异构对象接口，将任意二进制数据以键值对形式写入 HBM：

::::{tab-set}

:::{tab-item} Python

```python
import acl
import os
from yr.datasystem import Blob, DsClient, DeviceBlobList

# hetero_dev_mset and hetero_dev_mget must be executed in different processes
# because they need to be bound to different NPUs.
def hetero_dev_mset():
    client = DsClient("127.0.0.1", 31501)
    client.init()

    acl.init()
    device_idx = 1
    acl.rt.set_device(device_idx)

    key_list = [ 'key1', 'key2', 'key3' ]
    data_size = 1024 * 1024
    test_value = "value"

    in_data_blob_list = []
    for _ in key_list:
        tmp_batch_list = []
        for _ in range(4):
            dev_ptr, _ = acl.rt.malloc(data_size, 0)
            acl.rt.memcpy(dev_ptr, data_size, acl.util.bytes_to_ptr(test_value.encode()), data_size, 1)
            blob = Blob(dev_ptr, data_size)
            tmp_batch_list.append(blob)
        blob_list = DeviceBlobList(device_idx, tmp_batch_list)
        in_data_blob_list.append(blob_list)
    client.hetero().dev_mset(key_list, in_data_blob_list)

def hetero_dev_mget():
    client = DsClient("127.0.0.1", 31501)
    client.init()

    acl.init()
    device_idx = 2
    acl.rt.set_device(device_idx)

    key_list = [ 'key1', 'key2', 'key3' ]
    data_size = 1024 * 1024
    out_data_blob_list = []
    for _ in key_list:
        tmp_batch_list = []
        for _ in range(4):
            dev_ptr, _ = acl.rt.malloc(data_size, 0)
            blob = Blob(dev_ptr, data_size)
            tmp_batch_list.append(blob)
        blob_list = DeviceBlobList(device_idx, tmp_batch_list)
        out_data_blob_list.append(blob_list)
    client.hetero().dev_mget(key_list, out_data_blob_list, 60000)
    client.hetero().dev_delete(key_list)

pid = os.fork()
if pid == 0:
    hetero_dev_mset()
    os._exit(0)
else:
    hetero_dev_mget()
    os.wait()
```

:::

:::{tab-item} C++

```cpp
#include <iostream>
#include <vector>
#include <string>
#include <unistd.h>
#include <sys/wait.h>
#include <acl/acl.h>
#include "datasystem/datasystem.h"

using namespace datasystem;

void HeteroDevMSet()
{
    ConnectOptions connectOpts;
    connectOpts.host = "127.0.0.1";
    connectOpts.port = 31501; 
    auto client = std::make_shared<HeteroClient>(connectOpts);
    client->Init();

    int deviceIdx = 1;
    aclInit(nullptr);
    aclrtSetDevice(deviceIdx);

    std::vector<std::string> keyList = { "key1", "key2", "key3" };
    size_t dataSize = 1024 * 1024;
    std::string testValue = "value";

    std::vector<DeviceBlobList> inDataBlobList;
    for (size_t k = 0; k < keyList.size(); ++k) {
        std::vector<Blob> tmpBatchList;
        for (int i = 0; i < 4; ++i) {
            void* devPtr = nullptr;
            aclrtMalloc(&devPtr, dataSize, aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
            aclrtMemcpy(devPtr, dataSize, testValue.c_str(), testValue.length(), ACL_MEMCPY_HOST_TO_DEVICE);
            Blob blob;
            blob.pointer = devPtr;
            blob.size = dataSize;
            tmpBatchList.push_back(blob);
        }
        DeviceBlobList blobList;
        blobList.deviceIdx = deviceIdx;
        blobList.blobs = tmpBatchList;
        inDataBlobList.push_back(blobList);
    }
    
    std::vector<std::string> failedIdList;
    client->DevMSet(keyList, inDataBlobList, failedIdList);
    client->ShutDown();
}

void HeteroDevMGet()
{
    ConnectOptions connectOpts;
    connectOpts.host = "127.0.0.1";
    connectOpts.port = 31501;
    auto client = std::make_shared<HeteroClient>(connectOpts);
    client->Init();

    int deviceIdx = 2;
    aclInit(nullptr);
    aclrtSetDevice(deviceIdx);

    std::vector<std::string> keyList = { "key1", "key2", "key3" };
    size_t dataSize = 1024 * 1024;

    std::vector<DeviceBlobList> outDataBlobList;
    for (size_t k = 0; k < keyList.size(); ++k) {
        std::vector<Blob> tmpBatchList;
        for (int i = 0; i < 4; ++i) {
            void* devPtr = nullptr;
            aclrtMalloc(&devPtr, dataSize, aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
            Blob blob;
            blob.pointer = devPtr;
            blob.size = dataSize;
            tmpBatchList.push_back(blob);
        }
        DeviceBlobList blobList;
        blobList.deviceIdx = deviceIdx;
        blobList.blobs = tmpBatchList;
        outDataBlobList.push_back(blobList);
    }

    std::vector<std::string> failedIdList;
    client->DevMGet(keyList, outDataBlobList, failedIdList, 60000);
    client->DevDelete(keyList, failedIdList);
    client->ShutDown();
}

int main()
{
    HeteroDevMSet();
    HeteroDevMGet();
    return 0;
}
```

:::

::::

更多异构对象使用方式请参考：[异构对象开发指南](../api_reference/example/hetero.md)

### KV

基于共享内存实现免拷贝的 KV 数据读写，支持通过对接外部组件提供数据可靠性语义，支持数据在 DRAM / SSD / 二级缓存之间置换，实现大容量高性能缓存。

主要应用场景

- **训练场景 Checkpoint 快速保存及加载**：基于 KV 接口快速读写 Checkpoint，并支持将数据持久化到二级缓存保证数据可靠性。

通过 KV 接口，将任意二进制数据以键值对形式写入 DDR：

::::{tab-set}

:::{tab-item} Python

```python
from yr.datasystem.ds_client import DsClient

client = DsClient("127.0.0.1", 31501)
client.init()

key = "key"
expected_val = b"value"
client.kv().set(key, expected_val)

val = client.kv().get([key])
assert val[0] == expected_val

client.kv().delete([key])
```

:::

:::{tab-item} C++

```cpp
#include "datasystem/datasystem.h"

using namespace datasystem;

#define ASSERT_TRUE(condition) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "Assertion failed: %s, file %s, line %d\n", \
                    #condition, __FILE__, __LINE__); \
            exit(1); \
        } \
    } while(0)

int main()
{
    ConnectOptions connectOptions = { .host = "127.0.0.1", .port = 31501 };
    auto client = std::make_shared<DsClient>(connectOptions);
    ASSERT_TRUE(client->Init().IsOk());

    std::string key = "testKey";
    std::string value = "Hello kv client";
    std::string value2 = "Hello modify";
    Status status = client->KV()->Set(key, value);
    ASSERT_TRUE(status.IsOk());

    std::string getValue;
    status = client->KV()->Get(key, getValue);
    ASSERT_TRUE(status.IsOk());
    ASSERT_TRUE(getValue == value);

    status = client->KV()->Set(key, value2);
    ASSERT_TRUE(status.IsOk());

    status = client->KV()->Get(key, getValue);
    ASSERT_TRUE(status.IsOk());
    ASSERT_TRUE(getValue == value2);

    status = client->KV()->Del(key);
    ASSERT_TRUE(status.IsOk());

    status = client->KV()->Get(key, getValue);
    ASSERT_TRUE(status.IsError());
    return 0;
}
```

:::

::::

更多KV使用方式请参考：[KV开发指南](../api_reference/example/kv.md)

### Object

基于共享内存实现 Object 语义读写，提供基于引用计数管理生命周期，将共享内存抽象为 buffer，直接映射共享内存指针，提供更底层灵活的编程接口。

::::{tab-set}

:::{tab-item} Python

```python
import random
from yr.datasystem.ds_client import DsClient

def random_str(slen=10):
    seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#%^*()_+=-"
    sa = []
    for _ in range(slen):
        sa.append(random.choice(seed))
    return ''.join(sa)

def object_test():
    client = DsClient("127.0.0.1", 31501)
    client.init()
    object_key = "test_key"
    value = bytes(random_str(100), encoding='utf8')
    buffer = client.object().create(object_key, len(value))
    client.object().g_increase_ref([object_key])
    assert client.object().query_global_ref_num(object_key) == 1
    buffer.wlatch()
    buffer.memory_copy(value)
    buffer.seal()
    buffer.unwlatch()
    buffer_list = client.object().get([object_key], 0)
    assert buffer_list[0].immutable_data().tobytes() == value
    #self.assertEqual(buffer_list[0].immutable_data().tobytes(), value)
    client.object().g_decrease_ref([object_key])
    assert client.object().query_global_ref_num(object_key) == 0
    #self.assertEqual(client.object().query_global_ref_num(object_key), 0)
    try:
        client.object().get([object_key], 0)
    except RuntimeError as e:
        print("get error:", e)

object_test()
```

:::

:::{tab-item} C++

```cpp
#include "datasystem/datasystem.h"

using namespace datasystem;

#define ASSERT_TRUE(condition) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "Assertion failed: %s, file %s, line %d\n", \
                    #condition, __FILE__, __LINE__); \
            exit(1); \
        } \
    } while(0)

int main()
{
    ConnectOptions connectOptions = { .host = "127.0.0.1", .port = 31501 };
    auto client = std::make_shared<DsClient>(connectOptions);
    ASSERT_TRUE(client->Init().IsOk());

    std::string objectKey = "testKey";
    std::string data = "Hello object client";
    int size = data.size();
    std::shared_ptr<Buffer> buffer;
    Status status = client->Object()->Create(objectKey, size, CreateParam{}, buffer);
    ASSERT_TRUE(status.IsOk());
    ASSERT_EQ(size, buffer->GetSize());
    std::vector<std::string> failedobjectKeys;
    ASSERT_TRUE(client->Object()->GIncreaseRef({ objectKey }, failedobjectKeys).IsOk());
    buffer->WLatch();
    buffer->MemoryCopy((void *)data.data(), size);
    buffer->Seal();
    buffer->UnWLatch();

    std::vector<Optional<Buffer>> buffers;
    ASSERT_TRUE(client->Object()->Get({ objectKey }, 0, buffers).IsOk());
    ASSERT_EQ(buffers[0]->GetSize(), size);
    buffers[0]->RLatch();
    ASSERT_EQ(memcmp(data.data(), buffers[0]->MutableData(), size), 0);
    buffers[0]->UnRLatch();
    ASSERT_TRUE(client->Object()->GDecreaseRef({ objectKey }, failedobjectKeys).IsOk());
    ASSERT_TRUE(client->Object()->Get({ objectKey }, 0, buffers).IsError());
    return 0;
}
```

:::

::::

更多对象缓存使用方式请参考：[Object开发指南](../api_reference/example/object.md)