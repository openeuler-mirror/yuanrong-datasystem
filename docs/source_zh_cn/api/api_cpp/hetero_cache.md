# HeteroClient

[![查看源文件](https://mindspore-website.obs.cn-north-4.myhuaweicloud.com/website-images/r2.5.0/resource/_static/logo_source.svg)](../../../../include/datasystem/hetero_cache/hetero_client.h)

## 接口汇总
| 类名 | 说明 |
| --- | --- |
| [Blob](#blob) | 描述一段 device 上的内存。|
| [DeviceBlobList](#devicebloblist) | 描述 device 上的一组内存。|
| [HeteroClient](#heteroclient) | 异构对象客户端类。|
| [Future](#future) | 用于接收异步操作结果。|

## Blob

\#include &lt;[datasystem/hetero_cache/hetero_client.h](../../../../include/datasystem/hetero_cache/hetero_client.h)&gt;

Blob 用于描述一段 device 上的内存，当前定义如下：

```cpp
struct Blob {
    void *pointer = 0;
    uint64_t size = 0;
};
```

pointer 表示 device 上的指针地址， size 表示指针指向的内存大小。

## DeviceBlobList

\#include &lt;[datasystem/hetero_cache/hetero_client.h](../../../../include/datasystem/hetero_cache/hetero_client.h)&gt;

DeviceBlobList 用于描述 device 上的一组内存，当前定义如下：

```cpp
struct DeviceBlobList {
    std::vector<Blob> blobs;
    int32_t deviceIdx = -1;
};
```
blobs 用于存放多个 [Blob](#blob)， deviceIdx 指示 device 内存归属的 NPU 卡的 id。

## AsyncResult

\#include &lt;[datasystem/hetero_cache/hetero_client.h](../../../../include/datasystem/hetero_cache/hetero_client.h)&gt;

AsyncResult 用于描述异步请求的调用结果，当前定义如下：

```cpp
struct AsyncResult {
    Status status;
    std::vector<std::string> failedList;
};
```
status 用于表示异步执行的结果， failedList 用于表示失败的对象列表。


## MetaInfo
\#include &lt;[datasystem/hetero_cache/hetero_client.h](../../../../include/datasystem/hetero_cache/hetero_client.h)&gt;

用于描述存入key的元信息， 当前的定义如下:
```cpp
struct MetaInfo {
    std::vector<uint64_t> blobSizeList;
};
```
其中 blobSizeList 表示key对应原始存入数据的device内存块大小， 单位(字节)


## HeteroClient

\#include &lt;[datasystem/hetero_cache/hetero_client.h](../../../../include/datasystem/hetero_cache/hetero_client.h)&gt;

HeteroClient 为异构对象客户端类。

### 构造函数和析构函数

```cpp
explicit HeteroClient(const ConnectOptions &connectOptions = {})
~HeteroClient()
```

### 公有成员函数
| 函数             | 说明 |
|--------------------------------------------------------------------------------------------|--------|
| [`Status ShutDown()`](#shutdown)            |   关闭异构对象客户端。    |
| [`Status Init()`](#init)                    |    初始化异构对象客户端。    |
| [`Status MGetH2D(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys, int32_t subTimeoutMs)`](#mgeth2d)      |    从 host 中获取数据写入 device。    |
| [`Status MSetD2H(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam = {});`](#msetd2h)  |    将 device 的数据写入到 host。    |
| [`Status Delete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)`](#delete)   |    删除 host中的 key。    |
| [`Status GenerateKey(const std::string &prefix, std::string &key)`](#generatekey)     |    生成带数据系统 Worker UUID 的 key。    |
| [`Status DevPublish(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<Future> &futureVec)`](#devpublish)     |    将 device 上的内存发布为数据系统的异构对象。    |
| [`Status DevSubscribe(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<Future> &futureVec)`](#devsubscribe)                       |    订阅并接收 device 上的数据对象。    |
| [`Status DevMGet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys, int32_t subTimeoutMs = 0)`](#devmget) |    获取 device 上的对象。    |
| [`Status DevMSet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys)`](#devmset) |    将 device 对象写入数据系统。    |
| [`std::shared_future<AsyncResult> AsyncMSetD2H(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam = {});`](#AsyncMSetD2H) |    异步接口，将 device 对象写入数据系统。    |
| [`std::shared_future<AsyncResult> AsyncMGetH2D(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, uint64_t subTimeoutMs)`](#AsyncMGetH2D) |    异步接口，获取 device 上的对象。    |
| [`Status DevLocalDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)`](#devlocaldelete)      |    删除本地 device 对象。    |
| [`std::shared_future<AsyncResult> AsyncDevDelete(const std::vector<std::string> &keys)`](#asyncdevdelete)      |    异步删除 device 对象。    |
| [`Status DevDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)`](#devdelete)      |    删除 device 对象。    |
| [`Status GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey, std::vector<MetaInfo> &metaInfos, std::vector<std::string> &failKeys)`](#getmetainfo)      |    获取key对应的元数据信息    |
#### ShutDown

```cpp
Status ShutDown()
```

关闭异构对象客户端，断开与数据系统 Worker 之间的连接。

- 返回值

  返回值状态码为 `K_OK` 时表示关闭成功，否则返回其他错误码。

#### Init

```cpp
Status Init()
```

初始化异构对象客户端，建立与数据系统 Worker 之间的连接。

- 返回值

  返回值状态码为 `K_OK` 时表示初始化成功，否则返回其他错误码。

#### MGetH2D

```cpp
Status MGetH2D(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys, int32_t subTimeoutMs);
```

从 host 中获取数据并写入 device 中。  
MGetH2D 和 MSetD2H 需配套使用。  
若 MSetD2H 时将多个内存地址拼接写入了 host，则在 MGetH2D 中自动将 host 的数据拆分成多个内存地址写入 device。  
若 host 的 key 不再使用，可调用 Delete 接口删除。  

- 参数

    - `keys`: host 的 key 列表。约束：传入的 key 的数量不能超过 1 万。
    - `devBlobList`: 用于描述 device 上的 HBM 内存地址，用于接收从 host 中读取的数据。详见 [DeviceBlobList](#devicebloblist) 章节。
    - `failedKeys`: 失败的 key 列表。
    - `subTimeoutMs`: 超时时间，当在指定时间内无法获取完成，则返回异常。

- 返回值

  - 返回值状态码为 `K_OK` 时表示执行成功。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### MSetD2H

```cpp
Status MSetD2H(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam = {});
```

将 device 的数据写入到 host 中。若 device 的 blob 中存在多个内存地址时，会自动将数据拼接后写入 host。  
若 host 的 key 不再使用，可调用 Delete 接口删除。  
MSetD2H 成功的数据是immutable的，不要对已存在的key set新值，否则新值不会生效。 

- 参数

    - `keys`: host 的 key 列表。约束：传入的key的数量不能超过1万。
    - `devBlobList`: 用于描述 device 上的 HBM 内存地址。详见 [DeviceBlobList](#devicebloblist) 章节。
    - `setParam`: key的配置参数, 默认值为:
    ```
    struct SetParam {
        WriteMode writeMode = WriteMode::NONE_L2_CACHE;  
        uint32_t ttlSecond = 0;
        ExistenceOpt existence = ExistenceOpt::NONE;
        CacheType cacheType = CacheType::MEMORY;
    };
- 返回值

  - 返回值状态码为 `K_OK` 时表示执行成功。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### Delete

```cpp
Status Delete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys);
```

删除 host 中的 key。  
delete 接口与 MGetH2D / MSetD2H 配套使用。  

- 参数

    - `keys`: 待删除的 key 列表。约束：传入的 key 的数量不能超过 1 万。
    - `failedKeys`: 删除失败的 key 列表。

- 返回值

  - 返回值状态码为 `K_OK` 时表示删除成功。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### GenerateKey

```cpp
Status GenerateKey(const std::string &prefix, std::string &key);
```

生成带数据系统 Worker UUID 的 key。

- 参数

    - `prefix`: key 前缀。
    - `key`: 传出参数，生成成功时会将 key 写入到该参数中。

- 返回值

  - 返回值状态码为 `K_OK` 时表示 key 生成成功。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### DevPublish

```cpp
Status DevPublish(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<Future> &futureVec);
```

将 device 上的内存发布为数据系统的异构对象。发布后的异构对象可通过 DevSubscribe 获取。  
DevPublish 和 DevSubscribe 需配套使用。 DevPublish 和 DevSubscribe 传入的 Device 内存地址不能归属于同一张 NPU 卡。  
通过 DevSubscribe 获取数据成功后，数据系统会自动删除此异构对象，不再管理此对象对应的 device 内存。
在key，devBlobList内存地址映射关系均一致的情况下，DevPublish在同进程支持重试。

- 参数

    - `keys`: device 的异构对象的 key。约束：传入的key的数量不能超过1万。
    - `devBlobList`: 用于描述 device 上内存结构列表。详见 [DeviceBlobList](#devicebloblist) 章节。
    - `futureVec`: 用于异步获取发布结果。若 future 返回 OK，则表示数据已被对端接收。详见 [Future](#future) 章节。

- 返回值

  - 返回值状态码为 `K_OK` 时表示数据发布成功。
  - 返回值状态码为 `K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### DevSubscribe

```cpp
Status DevSubscribe(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<Future> &futureVec);
```

订阅发布到数据系统异构对象，并接收数据写入 devBlobList 。  
数据通过 device to device 通道直接传输。  
DevPublish 和 DevSubscribe 需配套使用。 DevPublish 和 DevSubscribe 传入的 Device 内存地址不能归属于同一张 NPU 卡。  
通过 DevSubscribe 获取数据成功后，数据系统会自动删除此异构对象，不再管理此对象对应的 device 内存。  
在执行 DevSubscribe 过程中，执行了 DevPublish 的进程不能退出，否则 DevSubscribe 会失败。
DevSubscribe单Key的订阅超时时间为20s，多key为60s。

- 参数

    - `keys`: device 的异构对象的 key。
    - `devBlobList`: 用于描述 device 上内存结构列表，用于接收数据。详见 [DeviceBlobList](#devicebloblist) 章节。
    - `futureVec`: 用于异步获取订阅结果。若 future 返回 OK，则表示数据已接收成功。详见 [Future](#future) 章节。

- 返回值

  - 返回值状态码为 `K_OK` 时表示对象订阅成功。约束：传入的key的数量不能超过1万。
  - 返回值状态码为 `K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### DevMGet

```cpp
Status DevMGet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys, int32_t subTimeoutMs = 0);
```

获取 device 中的数据，并写入到 devBlobList 中。数据通过 device to device 通道直接传输。  
DevMSet 和 DevMGet 需配套使用。 DevMSet 和 DevMGet 传入的 Device 内存地址不能归属于同一张 NPU 卡。  
DevMGet 后不会自动删除异构对象，如对象不再使用，可调用 DevLocalDelete 或 DevDelete 删除。  
在执行 DevMGet 过程中，执行了 DevMSet 的进程不能退出，否则 DevMGet 会失败。  

- 参数

    - `keys`: device 的异构对象的 key。约束：传入的key的数量不能超过1万。
    - `devBlobList`: 用于描述 device 上内存结构列表。详见 [DeviceBlobList](#devicebloblist) 章节。
    - `failedKeys`: 输出参数，用于描述失败的 key 的列表。
    - `subTimeoutMs`: 超时时间，当在指定时间内无法获取完成，则抛出异常。

- 返回值

  - 返回值状态码为 `K_OK` 时表示至少有一个对象获取成功。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。
  - 返回值状态码为 `K_NOT_FOUND` 时表示所有对象都无法获取到。
  - 返回值状态码为 `K_RUNTIME_ERROR` 时表示获取对象遇到了错误。

#### DevMSet

```cpp
Status DevMSet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys);
```

通过数据系统缓存 Device 上的数据，将 devBlobList 对应的 key 的元数据写入数据系统，可供其他 client 访问。  
DevMSet 和 DevMGet 需配套使用。 DevMSet 和 DevMGet 传入的 Device 内存地址不能归属于同一张 NPU 卡。  
DevMGet 后不会自动删除异构对象，如对象不再使用，可调用 DevLocalDelete 或 DevDelete 删除。  
在key，devBlobList内存地址映射关系均一致的情况下，DevMSet在同进程支持重试。 

- 参数

    - `keys`: device 的异构对象的 key。约束：传入的key的数量不能超过1万。
    - `devBlobList`: 用于描述 device 上内存结构列表。详见 [DeviceBlobList](#devicebloblist) 章节。
    - `failedKeys`: 输出参数，用于描述失败的key的列表。

- 返回值

  - 返回值状态码为 `K_OK` 时表示至少有一个对象DevMSet成功。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### DevLocalDelete

```cpp
Status DevLocalDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys);
```

从数据系统删除本节点上此 key 的元数据，不再管理此 key 对应的 device 内存。  
DevLocalDelete 与 DevMSet / DevMGet 接口配套使用。  

- 参数

    - `keys`: device 的异构对象的 key。约束：传入的key的数量不能超过1万。
    - `failedKeys`: 输出参数，用于描述失败的 key 的列表。

- 返回值

  - 返回值状态码为 `K_OK` 时表示至少有一个对象DevLocalDelete成功，否则返回其他错误码。

#### DevDelete

```cpp
Status DevDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys);
```

从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。  
DevDelete 与 DevMSet / DevMGet 接口配套使用。  

- 参数

    - `keys`: device 的异构对象的 key。约束：传入的 key 的数量不能超过 1 万。
    - `failedKeys`: 输出参数，用于描述失败的 key 的列表。

- 返回值

  - 返回值状态码为 `K_OK` 时表示至少有一个对象DevDelete成功，否则返回其他错误码。

#### AsyncDevDelete

```cpp
std::shared_future<AsyncResult> AsyncDevDelete(const std::vector<std::string> &keys);
```

异步接口，从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。  
AsyncDevDelete 与 DevMSet / DevMGet 接口配套使用。  

- 参数

    - `keys`: device 的异构对象的 key。约束：传入的 key 的数量不能超过 1 万。

- 返回值

  通过返回的future查询异步删除请求的调用结果

#### GetMetaInfo
```cpp
Status GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey, std::vector<MetaInfo> &metaInfos, std::vector<std::string> &failKeys)
```
获取key对应的元数据信息

- 参数

    - `keys`: 输入参数，device 的异构对象的 key。约束：传入的 key 的数量不能超过 1 万。
    - `isDevKey`: 输入参数，key的属性，true 表示D2D类型，false表示D2H类型。
    - `metaInfos`: 输出参数，用于描述key的元信息。
    - `failKeys`: 输出参数，用于描述失败列表。
- 返回值
  - 返回值状态码为 `K_OK` 时表示至少有一个对象GetMetaInfo成功，否则返回其他错误码。


## Future

\#include &lt;[datasystem/hetero_cache/future.h](../../../../include/datasystem/hetero_cache/future.h)&gt;

Future 用于接收异步操作的结果。

### 公有成员函数

| 函数                                                                            | 说明 |
|-------------------------------------------------------------------------------|--------|
| [`Status Get(uint64_t subTimeoutMs = 60000)`](#get)     |    获取异步任务的执行结果。    |

#### Get

```cpp
Status Get(uint64_t subTimeoutMs = 60000)
```

获取异步任务的执行结果。

- 参数

    - `subTimeoutMs`: 描述等待时长。如果 subTimeoutMs 大于0， 则阻塞直到超时或结果变为可用；如果 subTimeoutMs 等于0，则立即返回结果状态。

- 返回值

  - 返回值状态码为 `K_OK` 时表示异步操作执行成功，否则返回其他错误码。