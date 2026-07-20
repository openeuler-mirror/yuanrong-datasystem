# HeteroClient编程示例

## 基本概念

openYuanrong datasystem (下文中称为数据系统)的 Hetero 语义中，基于 Device 侧的 HBM 内存抽象异构对象接口，实现昇腾 NPU 卡间数据高速直通传输。同时提供 H2D/D2H 高速迁移接口，实现数据快速在 DRAM/HBM 之间传输。  

> **注意**：  
> 数据系统并不直接负责 HBM 内存的申请及释放，用户调用 hetero 接口将 HBM 的指针注册关联到数据系统中并指定 key，数据系统将用户指定的 key 及 HBM 指针抽象为数据对象，控制在不同卡之间的流转。  
> 因此将 HBM 的指针注册给数据系统后，上层业务需要保证指针的有效性，避免被释放或修改。  

## D2D(Device to Device) 数据传输

异构对象针对 D2D 数据传输，提供了两种语义：

**DevPublish / DevSubscribe**：数据传输语义，数据生成端执行 DevPublish 将 HBM 上的数据发布为异构对象，数据接收端申请 HBM 内存后，执行 DevSubscribe 订阅接收数据，数据系统使用卡间直通传输数据并写入用户提供的 HBM 内存中。当数据被接收后，数据系统会自动删除对象，不再关联发布的 HBM 内存。  
DevPublish / DevSubscribe 为异步接口，提供了返回 Future 供用户获取执行结果，每个key返回一个 Future。当 Future::Get 获取到结果为 OK 时，表示数据已经被对端接收成功。  

> **注意**：  
> DevPublish / DevSubscribe 传入的 Device 内存地址不能归属于同一张 NPU 卡。  
> 在执行 DevSubscribe 过程中，执行了 DevPublish 的进程不能退出，否则 DevSubscribe 会失败。  
> 在key，devBlobList内存地址映射关系均一致的情况下，DevPublish在同进程支持重试。
> DevSubscribe单Key的订阅超时时间为20s，多key为60s。

::::{tab-set}

:::{tab-item} Python

```python
import acl
import random
from yr.datasystem import DsClient, DeviceBlobList, Blob

def random_str(slen=10):
    seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#%^*()_+=-"
    sa = []
    for _ in range(slen):
        sa.append(random.choice(seed))
    return ''.join(sa)

# hetero_dev_publish and hetero_dev_subscribe must be executed in different processes
# because they need to be bound to different NPUs.
def hetero_dev_publish():
    client = DsClient("127.0.0.1", 31501)
    client.init()

    acl.init()
    device_idx = 1
    acl.rt.set_device(device_idx)

    key_list = [ 'key1', 'key2', 'key3' ]
    data_size = 1024 * 1024
    test_value = random_str(data_size)

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
    pub_futures = client.hetero().dev_publish(key_list, in_data_blob_list)
    for future in pub_futures:
        future.get()

def hetero_dev_subscribe():
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
    sub_futures = client.hetero().dev_subscribe(key_list, out_data_blob_list)
    for future in sub_futures:
        future.get()
```

:::

:::{tab-item} C++

```cpp
#include <cstdio>
#include "datasystem/datasystem.h"
#include <acl/acl.h>

using namespace datasystem;

// HeteroDevPublish and HeteroDevSubscribe must be executed in different processes
// because they need to be bound to different NPUs.
int HeteroDevPublish()
{
    ConnectOptions connectOptions = { .host = "127.0.0.1", .port = 31501 };
    auto client = std::make_shared<DsClient>(connectOptions);
    if (!client->Init().IsOk()) {
        fprintf(stderr, "Init failed\n");
        return -1;
    }

    // Initialize the ACL interface.
    int deviceId = 1;
    if (aclInit(nullptr) != ACL_SUCCESS) {
        fprintf(stderr, "aclInit failed\n");
        return -1;
    }
    if (aclrtSetDevice(deviceId) != ACL_SUCCESS) { // Bind the NPU card.
        fprintf(stderr, "aclrtSetDevice failed\n");
        return -1;
    }

    // The sender constructs data and publishes it to the data system.
    std::vector<std::string> keys = { "test-key1" };
    std::vector<uint64_t> blobSize = { 10, 20 };
    int blobNum = static_cast<int>(blobSize.size());
    std::vector<DeviceBlobList> inBlobList;
    inBlobList.resize(keys.size());
    // Enter the Device address information on the device to the devblob.
    for (size_t i = 0; i < inBlobList.size(); i++) {
        inBlobList[i].deviceIdx = deviceId;
        for (int j = 0; j < blobNum; j++) {
            void *devPtr = nullptr;
            int code = aclrtMalloc(&devPtr, blobSize[j], aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
            // Copying Data to the Device Memory.
            // aclrtMemcpy(devPtr, blobSize[j], value.data(), size, aclrtMemcpyKind::ACL_MEMCPY_HOST_TO_DEVICE)
            if (code != ACL_SUCCESS) {
                fprintf(stderr, "aclrtMalloc failed\n");
                return -1;
            }
            Blob blob = { .pointer = devPtr, .size = blobSize[j] };
            inBlobList[i].blobs.emplace_back(std::move(blob));
        }
    }

    std::vector<Future> inFutureVecEnque;
    Status status = client->Hetero()->DevPublish(keys, inBlobList, inFutureVecEnque);
    if (!status.IsOk()) {
        fprintf(stderr, "DevPublish failed\n");
        return -1;
    }

    // The sender checks the future.
    // If OK is returned, it indicates that the receiver has received the HBM successfully
    // and the HBM memory is disconnected from the data system.
    for (size_t i = 0; i < inFutureVecEnque.size(); i++) {
        if (!inFutureVecEnque[i].Get().IsOk()) {
            fprintf(stderr, "DevPublish future failed\n");
            return -1;
        }
    }
    client->ShutDown();
    return 0;
}

int HeteroDevSubscribe()
{
    ConnectOptions connectOptions = { .host = "127.0.0.1", .port = 31501 };
    auto client = std::make_shared<DsClient>(connectOptions);
    if (!client->Init().IsOk()) {
        fprintf(stderr, "Init failed\n");
        return -1;
    }

    // Initialize the ACL interface.
    int deviceId = 2;
    if (aclInit(nullptr) != ACL_SUCCESS) {
        fprintf(stderr, "aclInit failed\n");
        return -1;
    }
    if (aclrtSetDevice(deviceId) != ACL_SUCCESS) { // Bind the NPU card.
        fprintf(stderr, "aclrtSetDevice failed\n");
        return -1;
    }

    std::vector<std::string> keys = { "test-key1" };
    std::vector<uint64_t> blobSize = { 10, 20 };
    int blobNum = static_cast<int>(blobSize.size());
    std::vector<DeviceBlobList> outBlobList;
    outBlobList.resize(keys.size());
    // The receiver allocates HBM memory and subscribes to receive data from the data system.
    for (size_t i = 0; i < outBlobList.size(); i++) {
        outBlobList[i].deviceIdx = deviceId;
        for (int j = 0; j < blobNum; j++) {
            void *devPtr = nullptr;
            int code = aclrtMalloc(&devPtr, blobSize[j], aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
            if (code != ACL_SUCCESS) {
                fprintf(stderr, "aclrtMalloc failed\n");
                return -1;
            }
            Blob blob = { .pointer = devPtr, .size = blobSize[j] };
            outBlobList[i].blobs.emplace_back(std::move(blob));
        }
    }

    std::vector<Future> outFutureVecDeque;
    Status status = client->Hetero()->DevSubscribe(keys, outBlobList, outFutureVecDeque);
    if (!status.IsOk()) {
        fprintf(stderr, "DevSubscribe failed\n");
        return -1;
    }

    // The receiver checks the future.
    // If ok is returned, the data is received successfully.
    for (size_t i = 0; i < outFutureVecDeque.size(); i++) {
        if (!outFutureVecDeque[i].Get().IsOk()) {
            fprintf(stderr, "DevSubscribe future failed\n");
            return -1;
        }
    }
    client->ShutDown();
    return 0;
}

int main(int argc, char *argv[])
{
    // 通过命令行参数选择角色，publish 与 subscribe 需分别在不同进程（绑定不同 NPU 卡）执行：
    //   ./demo publish     # 发送端，绑定 deviceId=1
    //   ./demo subscribe   # 接收端，绑定 deviceId=2
    std::string role = (argc > 1) ? argv[1] : "publish";
    if (role == "subscribe") {
        return HeteroDevSubscribe();
    }
    return HeteroDevPublish();
}
```

:::

::::

**DevMSet / DevMGet**：数据缓存语义，数据生成端执行 DevMSet 将 HBM 数据发布到数据系统，数据接收端申请 HBM 内存后，执行 DevMGet 接口读取数据。当数据被读取后，数据系统不会删除对象，该数据可被反复读取。数据使用完成后需要调用 DevLocalDelete/DevDelete 删除对象。  

> **注意**：  
> · `DevMSet / DevMGet` 传入的 Device 内存地址不能归属于同一张 NPU 卡。  
> · 在执行`DevMGet`过程中，执行了 DevMSet 的进程不能退出，否则 DevMGet 会失败。  
> · 在key，devBlobList内存地址映射关系均一致的情况下，DevMGet在同进程支持重试。  
> · `DevMSet / DevMGet`接口注册的是 Device 内存地址，不会隐式等待上游框架异步算子完成。若调用前同一地址上仍有异步写操作（如 `torch` 的 `to/clone/zeros` 等），可能出现数据不一致，可在调用前增加`torch.npu.synchronize()`显示同步。



::::{tab-set}

:::{tab-item} Python

```python
import acl
import random
from yr.datasystem import DsClient, DeviceBlobList, Blob

def random_str(slen=10):
    seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#%^*()_+=-"
    sa = []
    for _ in range(slen):
        sa.append(random.choice(seed))
    return ''.join(sa)

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
    test_value = random_str(data_size)

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
```

:::

:::{tab-item} C++

```cpp
#include <cstdio>
#include "datasystem/datasystem.h"
#include <acl/acl.h>

using namespace datasystem;

// HeteroDevMSet and HeteroDevMGet must be executed in different processes
// because they need to be bound to different NPUs.
int HeteroDevMSet()
{
    ConnectOptions connectOptions = { .host = "127.0.0.1", .port = 31501 };
    auto client = std::make_shared<DsClient>(connectOptions);
    if (!client->Init().IsOk()) {
        fprintf(stderr, "Init failed\n");
        return -1;
    }

    // Initialize the ACL interface.
    int deviceId = 1;
    if (aclInit(nullptr) != ACL_SUCCESS) {
        fprintf(stderr, "aclInit failed\n");
        return -1;
    }
    if (aclrtSetDevice(deviceId) != ACL_SUCCESS) {  // Bind the NPU card.
        fprintf(stderr, "aclrtSetDevice failed\n");
        return -1;
    }

    // The data generator constructs data and writes it to the data system.
    std::vector<std::string> keys = { "test-key1" };
    std::vector<uint64_t> blobSize = { 10, 20 };
    int blobNum = static_cast<int>(blobSize.size());
    std::vector<DeviceBlobList> inBlobList;
    inBlobList.resize(keys.size());
    for (size_t i = 0; i < inBlobList.size(); i++) {
        inBlobList[i].deviceIdx = deviceId;
        for (int j = 0; j < blobNum; j++) {
            void *devPtr = nullptr;
            int code = aclrtMalloc(&devPtr, blobSize[j], aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
            // Copying Data to the Device Memory.
            // aclrtMemcpy(devPtr, blobSize[j], value.data(), size, aclrtMemcpyKind::ACL_MEMCPY_HOST_TO_DEVICE)
            if (code != ACL_SUCCESS) {
                fprintf(stderr, "aclrtMalloc failed\n");
                return -1;
            }
            Blob blob = { .pointer = devPtr, .size = blobSize[j] };
            inBlobList[i].blobs.emplace_back(std::move(blob));
        }
    }

    std::vector<std::string> setFailedKeys;
    Status status = client->Hetero()->DevMSet(keys, inBlobList, setFailedKeys);
    if (!status.IsOk() || !setFailedKeys.empty()) {
        fprintf(stderr, "DevMSet failed\n");
        return -1;
    }
    client->ShutDown();
    return 0;
}

int HeteroDevMGet()
{
    ConnectOptions connectOptions = { .host = "127.0.0.1", .port = 31501 };
    auto client = std::make_shared<DsClient>(connectOptions);
    if (!client->Init().IsOk()) {
        fprintf(stderr, "Init failed\n");
        return -1;
    }

    // Initialize the ACL interface.
    int deviceId = 2;
    if (aclInit(nullptr) != ACL_SUCCESS) {
        fprintf(stderr, "aclInit failed\n");
        return -1;
    }
    if (aclrtSetDevice(deviceId) != ACL_SUCCESS) { // Bind the NPU card.
        fprintf(stderr, "aclrtSetDevice failed\n");
        return -1;
    }

    // The data obtainer allocates the HBM memory and invokes the DevMGet interface to receive data.
    std::vector<std::string> keys = { "test-key1" };
    std::vector<uint64_t> blobSize = { 10, 20 };
    int blobNum = static_cast<int>(blobSize.size());
    std::vector<DeviceBlobList> outBlobList;
    outBlobList.resize(keys.size());
    // Allocate the HBM memory and fill it in the outBlobList.
    for (size_t i = 0; i < outBlobList.size(); i++) {
        outBlobList[i].deviceIdx = deviceId;
        for (int j = 0; j < blobNum; j++) {
            void *devPtr = nullptr;
            int code = aclrtMalloc(&devPtr, blobSize[j], aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
            if (code != ACL_SUCCESS) {
                fprintf(stderr, "aclrtMalloc failed\n");
                return -1;
            }
            Blob blob = { .pointer = devPtr, .size = blobSize[j] };
            outBlobList[i].blobs.emplace_back(std::move(blob));
        }
    }

    std::vector<std::string> getFailedKeys;
    Status status = client->Hetero()->DevMGet(keys, outBlobList, getFailedKeys);
    if (!status.IsOk() || !getFailedKeys.empty()) {
        fprintf(stderr, "DevMGet failed\n");
        return -1;
    }

    std::vector<std::string> delFailedKeys;
    status = client->Hetero()->DevDelete(keys, delFailedKeys);
    if (!status.IsOk() || !delFailedKeys.empty()) {
        fprintf(stderr, "DevDelete failed\n");
        return -1;
    }
    client->ShutDown();
    return 0;
}

int main(int argc, char *argv[])
{
    // 通过命令行参数选择角色，mset 与 mget 需分别在不同进程（绑定不同 NPU 卡）执行：
    //   ./demo mset   # 写入端，绑定 deviceId=1
    //   ./demo mget   # 读取端，绑定 deviceId=2
    std::string role = (argc > 1) ? argv[1] : "mset";
    if (role == "mget") {
        return HeteroDevMGet();
    }
    return HeteroDevMSet();
}
```

:::

::::

> **注意**：  
> 这里提到的删除对象，是指解除数据系统与 HBM 内存的绑定，并不会直接影响 HBM 中的数据，HBM 的内存由上层应用自己释放。  

## H2D(Host to Device)/D2H(Device to Host) 数据传输

异构对象接口提供了 MGetH2D 和 MSetD2H 接口，实现数据在 HBM 和 DRAM 之间快速 swap。  
MSetD2H 接口：将 HBM 上的内存，写入到host中的指定 key 中。当数据系统在 HBM 中是零散的多个小块内存时，在 MSetD2H 接口中，会自动将这些小块数据拼接后写入到 host 内存中。  
MGetH2D 接口：从 host 的指定 key 中读取数据，并写入到 Device 的 HBM 内存中。MGetH2D 接口需要用户提前申请 HBM 内存将地址写入到 devBlobList 参数中。  
MGetH2D / MSetD2H 需配套使用。当 MSetD2H 对 HBM 的小数据块合并后，在 MGetH2D 中会重新还原为小数据块。  
若 host 的 key 不再使用，可调用 Delete 接口删除。  

::::{tab-set}

:::{tab-item} Python

```python
import acl
import random
from yr.datasystem import DsClient, DeviceBlobList, Blob

def random_str(slen=10):
    seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#%^*()_+=-"
    sa = []
    for _ in range(slen):
        sa.append(random.choice(seed))
    return ''.join(sa)

def hetero_mset_d2h_mget_h2d():
    client = DsClient("127.0.0.1", 31501)
    client.init()

    acl.init()
    device_idx = 0
    acl.rt.set_device(device_idx)

    key_list = [ 'key1', 'key2', 'key3' ]
    data_size = 1024 * 1024
    test_value = random_str(data_size)

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

    out_data_blob_list = []
    for _ in key_list:
        tmp_batch_list = []
        for _ in range(4):
            dev_ptr, _ = acl.rt.malloc(data_size, 0)
            blob = Blob(dev_ptr, data_size)
            tmp_batch_list.append(blob)
        blob_list = DeviceBlobList(device_idx, tmp_batch_list)
        out_data_blob_list.append(blob_list)

    client.hetero().mset_d2h(key_list, in_data_blob_list)

    client.hetero().mget_h2d(key_list, out_data_blob_list, 60000)
```

:::

:::{tab-item} C++

```cpp

#include <cstdio>
#include "datasystem/datasystem.h"
#include <acl/acl.h>

using namespace datasystem;

int main()
{
    ConnectOptions connectOptions = { .host = "127.0.0.1", .port = 31501 };
    auto client = std::make_shared<DsClient>(connectOptions);
    if (!client->Init().IsOk()) {
        fprintf(stderr, "Init failed\n");
        return -1;
    }

    // Initialize the ACL interface.
    int deviceId = 1;
    aclInit(nullptr);
    aclrtSetDevice(deviceId); // Bind the NPU card.

    std::vector<std::string> keys = { "test-key1" };
    std::vector<uint64_t> blobSize = { 10, 20 };
    int blobNum = blobSize.size();
    std::vector<DeviceBlobList> swapOutBlobList;
    swapOutBlobList.resize(keys.size());
    // Allocate the HBM memory and fill it in the swapOutBlobList.
    for (size_t i = 0; i < swapOutBlobList.size(); i++) {
        swapOutBlobList[i].deviceIdx = deviceId;
        for (int j = 0; j < blobNum; j++) {
            void *devPtr = nullptr;
            int code = aclrtMalloc(&devPtr, blobSize[j], aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
            // Copying Data to the Device Memory.
            // aclrtMemcpy(devPtr, blobSize[j], value.data(), size, aclrtMemcpyKind::ACL_MEMCPY_HOST_TO_DEVICE)
            if (code != 0) {
                fprintf(stderr, "aclrtMalloc failed\n");
                return -1;
            }
            Blob blob = { .pointer = devPtr, .size = blobSize[j] };
            swapOutBlobList[i].blobs.emplace_back(std::move(blob));
        }
    }

    Status status = client->Hetero()->MSetD2H(keys, swapOutBlobList);
    if (!status.IsOk()) {
        fprintf(stderr, "MSetD2H failed\n");
        return -1;
    }

    std::vector<DeviceBlobList> swapInBlobList;
    swapInBlobList.resize(keys.size());
    // Allocate the HBM memory and fill it in the swapInBlobList.
    for (size_t i = 0; i < swapInBlobList.size(); i++) {
        swapInBlobList[i].deviceIdx = deviceId;
        for (int j = 0; j < blobNum; j++) {
            void *devPtr = nullptr;
            int code = aclrtMalloc(&devPtr, blobSize[j], aclrtMemMallocPolicy::ACL_MEM_MALLOC_HUGE_FIRST);
            if (code != 0) {
                fprintf(stderr, "aclrtMalloc failed\n");
                return -1;
            }
            Blob blob = { .pointer = devPtr, .size = blobSize[j] };
            swapInBlobList[i].blobs.emplace_back(std::move(blob));
        }
    }
    std::vector<std::string> failedList;
    status = client->Hetero()->MGetH2D(keys, swapInBlobList, failedList, 30000);
    if (!status.IsOk()) {
        fprintf(stderr, "MGetH2D failed\n");
        return -1;
    }
    return 0;
}
```

:::

::::

## 使用限制

- key 仅支持大写字母、小写字母、数字以及如下特定字符：`-_!@#%^*()+=:;`。
- key 的最大长度为 1024 字节。
- DevPublish 和 DevSubscribe 需配套使用，不能和 DevMSet 及 DevMGet 混合使用。
- DevMSet 及 DevMGet 配套使用，写入的 key 使用 DevDelete 及 DevLocalDelete 删除。
- MSetD2H 和 MGetH2D 需要配套使用，MGetH2D 接口中只能传入 MSetD2H 接口写入的key。写入的 key 使用 Delete 接口删除。
