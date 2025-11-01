# ObjectClient

[![查看源文件](https://mindspore-website.obs.cn-north-4.myhuaweicloud.com/website-images/r2.5.0/resource/_static/logo_source.svg)](../../../../include/datasystem/object_cache.h)

## 接口汇总

| 类名 | 说明 |
| --- | --- |
| [ObjectClient](#objectclient) | 对象缓存客户端类。|
| [Buffer](#buffer) | 对象缓存数据类。|
| [CreateParam](#createparam) | 创建对象时的配置参数类。|
| [WriteMode](#writemode) | 配置对象可靠性的枚举类。|
| [ConsistencyType](#consistencytype) | 配置对象一致性的枚举类。|
| [CacheType](#cachetype) | 配置对象缓存类型。|

## ObjectClient

\#include &lt;[datasystem/object_cache/object_client.h](../../../../include/datasystem/object_cache/object_client.h)&gt;

Buffer 类用于表示对象缓存数据类。

### 构造函数和析构函数

```cpp
explicit ObjectClient(const ConnectOptions &connectOptions = {})
~ObjectClient()
```

### 公有成员函数

| 函数                                                                                                                                                                           | 说明 |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| [`Status Init()`](#init)                                                                                                                                                       |    初始化客户端。    |
| [`Status ShutDown()`](#shutdown)                                                                                                                                               |    关闭客户端。    |
| [`Status GIncreaseRef(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)`](#gincreaseref)                                               |    增加对象的全局引用计数。    |
| [`Status GDecreaseRef(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)`](#gdecreaseref)                                               |    减少对象的全局引用计数。    |
| [`int QueryGlobalRefNum(const std::string &objectKey)`](#queryglobalrefnum)                                                                                                     |    查询对象的全局引用计数。    |
| [`Status Create(const std::string &objectKey, uint64_t size, const CreateParam &param, std::shared_ptr<Buffer> &buffer)`](#create)                                            |    创建对象 Buffer。   |
| [`Status Put(const std::string &objectKey, const uint8_t *data, uint64_t size, const CreateParam &param, const std::unordered_set<std::string> &nestedObjectKeys = {})`](#put) |    创建或更新对象并发布。    |
| [`Status Get(const std::vector<std::string> &objectKeys, int32_t subTimeoutMs, std::vector<Optional<Buffer>> &buffers)`](#get)                                            |    获取对象。    |
| [`Status GenerateObjectKey(const std::string &prefix, std::string &objectKey)`](#generateobjectkey)                                                                               |    生成带数据系统 Worker UUID 的对象 key。    |
| [`Status HealthCheck()`](#healthcheck)                                                                                                                                         |    检查当前正在连接的数据系统 Worker 健康检查状况。    |

#### ShutDown

```cpp
Status ShutDown()
```

断开对象缓存客户端与当前数据系统 Worker 之间的连接。

- 返回值

  返回值状态码为 `K_OK` 时表示断链成功，否则返回其他错误码。

#### Init

```cpp
Status Init()
```

建立对象缓存客户端与当前数据系统 Worker 之间的连接并完成初始化。

- 返回值

  返回值状态码为 `K_OK` 时表示初始化成功，否则返回其他错误码。

#### GIncreaseRef

```cpp
Status GIncreaseRef(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)
```

增加对象的全局引用计数。

- 参数

    - `objectKeys`: 需要增加全局引用计数的对象 key 数组。约束：传入的object key的数量不能超过1万。
    - `failedObjectKeys`: 传出参数，增加全局引用计数失败的对象 key 会被保存在该字符串数组中。

- 返回值

  - 返回值状态码为 `K_OK` 时表示至少有一个对象增加全局引用计数成功。
  - 返回值状态码为 `K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### GDecreaseRef

```cpp
Status GDecreaseRef(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)
```

减少对象的全局引用计数。

- 参数

    - `objectKeys`: 需要减少全局引用计数的对象 key 数组。约束：传入的object key的数量不能超过1万。
    - `failedObjectKeys`: 传出参数，减少全局引用计数失败的对象 key 会被保存在该字符串数组中。

- 返回值

  - 返回值状态码为 `K_OK` 时表示至少有一个对象减少全局引用计数成功。
  - 返回值状态码为 `K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### QueryGlobalRefNum

```cpp
int QueryGlobalRefNum(const std::string &objectKey)
```

查询对象的全局引用计数。

- 参数

    - `objectKey`: 对象 key。

- 返回值

  返回值为大于等于 0 的数字时表示查询成功，返回值即为对象 key 的全局引用计数；返回 -1 时表示查询失败。

#### Create

```cpp
Status Create(const std::string &objectKey, uint64_t size, const CreateParam &param,
              std::shared_ptr<Buffer> &buffer)
```

创建对象 Buffer。

- 参数

    - `objectKey`: 对象 key。
    - `size`: 对象大小（以字节为单位）。
    - `param`: 详见 [CreateParam](#createparam) 章节。
    - `buffer`: 传出参数，[Buffer](#buffer)。

- 返回值

  - 返回值状态码为 `K_OK` 时表示创建对象成功。
  - 返回值状态码为 `K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### Put

```cpp
Status Put(const std::string &objectKey, const uint8_t *data, uint64_t size, const CreateParam &param,
           const std::unordered_set<std::string> &nestedObjectKeys = {})
```

创建或更新对象并发布。

- 参数

    - `objectKey`: 对象 key。
    - `data`: 数据内存地址指针。
    - `size`: 数据大小（以字节为单位）。
    - `param`: 详见 [CreateParam](#createparam) 章节。
    - `nestedObjectKeys`: 对该对象 key 有依赖关系的对象 key 数组。

- 返回值

  - 返回值状态码为 `K_OK` 时表示对象创建或更新并发布成功。
  - 返回值状态码为 `K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### Get

```cpp
Status Get(const std::vector<std::string> &objectKeys, int32_t subTimeoutMs, std::vector<Optional<Buffer>> &buffers)
```

获取对象。

- 参数

    - `objectKeys`: 需要获取的对象 key 数组。约束：传入的object key的数量不能超过1万。
    - `subTimeoutMs`: 对象订阅时间（以毫秒为单位）；对象未就绪时，产生订阅等待对象就绪返回，0 表示不订阅，可配置范围为[0, INT32_MAX]。
    - `buffers`: 对象 [Buffer](#buffer)。

- 返回值

  - 返回值状态码为 `K_OK` 时表示至少有一个对象获取成功。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。
  - 返回值状态码为 `K_NOT_FOUND` 时表示所有对象都无法获取到。
  - 返回值状态码为 `K_RUNTIME_ERROR` 时表示获取对象遇到了错误。

#### GenerateObjectKey

```cpp
Status GenerateObjectKey(const std::string &prefix, std::string &objectKey)
```

生成带数据系统 Worker UUID 的对象 key。

- 参数

    - `prefix`: 对象 key 前缀。
    - `objectKey`: 传出参数，生成成功时会将对象 key 写入到该参数中。

- 返回值

  - 返回值状态码为 `K_OK` 时表示对象 key 生成成功。
  - 返回值状态码为 `K_INVALID` 时表示传入参数未通过参数合法校验。

#### HealthCheck

```cpp
Status HealthCheck()
```

检查当前正在连接的数据系统 Worker 健康检查状况。

- 返回值

  - 返回值状态码为 `K_OK` 时表示 Worker 状态健康，否则返回其他错误码。

## Buffer

\#include &lt;[datasystem/object_cache/buffer.h](../../../../include/datasystem/object_cache/buffer.h)&gt;

Buffer 类用于表示对象缓存数据类。

### 构造函数和析构函数

```cpp
Buffer()
Buffer(Buffer &&other) noexcept
~Buffer() = default;
```

### 公有成员函数

| 函数                                                                            | 说明 |
|-------------------------------------------------------------------------------|--------|
| [`Status MemoryCopy(const void *data, uint64_t length)`](#memorycopy)     |    将数据拷贝到 `Buffer` 的缓存。   |
| [`int64_t GetSize() const`](#getsize)     |    获取对象 `Buffer` 的大小。    |
| [`Status Publish(const std::unordered_set<std::string> &nestedKeys = {})`](#publish)     |    将 `Buffer` 的数据发布到数据系统。    |
| [`Status Seal(const std::unordered_set<std::string> &nestedKeys = {})`](#seal)     |    将 `Buffer` 的数据发布到数据系统，并不再允许修改。    |
| [`Status WLatch(uint64_t timeoutSec = 60)`](#wlatch)     |    对 `Buffer` 添加写锁。    |
| [`Status RLatch(uint64_t timeoutSec = 60)`](#rlatch)     |    对 `Buffer` 添加读锁。    |
| [`Status UnRLatch()`](#unrlatch)     |    对 `Buffer` 解除读锁。    |
| [`Status UnWLatch()`](#unwlatch)     |    对 `Buffer` 解除写锁。    |
| [`void *MutableData()`](#mutabledata)     |    获取 `Buffer` 可读写的缓存数据指针。    |
| [`const void *ImmutableData()`](#immutabledata)     |    获取 `Buffer` 只读缓存数据指针。    |
| [`Status InvalidateBuffer()`](#invalidatebuffer)     |    使当前主机上的 `Buffer` 数据无效化。    |

#### MemoryCopy

```cpp
Status MemoryCopy(const void *data, uint64_t length)
```

将数据拷贝到 `Buffer` 的缓存。

- 参数

    - `data`: 需要拷贝的数据内存地址。
    - `length`: 需要拷贝的数据长度。

- 返回值

  返回值状态码为 `K_OK` 时表示数据拷贝成功，否则返回其他错误码。

#### GetSize

```cpp
int64_t GetSize() const
```

获取对象 `Buffer` 的大小。

- 返回值

  `Buffer` 大小（以字节为单位）。

#### Publish

```cpp
Status Publish(const std::unordered_set<std::string> &nestedKeys = {})
```

将 `Buffer` 的数据发布到数据系统。

- 参数

    - `nestedKeys`: 依赖的嵌套对象 key 数组。

- 返回值

  返回值状态码为 `K_OK` 时表示发布成功，否则返回其他错误码。

#### Seal

```cpp
Status Seal(const std::unordered_set<std::string> &nestedKeys = {})
```

将 `Buffer` 的数据发布到数据系统，并不再允许修改。

- 参数

    - `nestedKeys`: 依赖的嵌套对象 key 数组。

- 返回值

  返回值状态码为 `K_OK` 时表示发布成功，否则返回其他错误码。

#### WLatch

```cpp
Status WLatch(uint64_t timeoutSec = 60)
```

对 `Buffer` 添加写锁。

- 参数

    - `timeoutSec`: 添加写锁的超时时间，默认为 60 秒。

- 返回值

  返回值状态码为 `K_OK` 时表示加写锁成功，否则返回其他错误码。

#### RLatch

```cpp
Status RLatch(uint64_t timeoutSec = 60)
```

对 `Buffer` 添加读锁。

- 参数

    - `timeoutSec`: 添加读锁的超时时间，默认为 60 秒。

- 返回值

  返回值状态码为 `K_OK` 时表示加读锁成功，否则返回其他错误码。

#### UnRLatch

```cpp
Status UnRLatch()
```

对 `Buffer` 解除读锁。


- 返回值

  返回值状态码为 `K_OK` 时表示解读锁成功，否则返回其他错误码。

#### UnWLatch

```cpp
Status UnWLatch()
```

对 `Buffer` 解除写锁。


- 返回值

  返回值状态码为 `K_OK` 时表示解写锁成功，否则返回其他错误码。

#### MutableData

```cpp
void *MutableData()
```

获取 `Buffer` 可读写的缓存数据指针。

- 返回值

  返回值为可读写的缓存数据指针。

#### ImmutableData

```cpp
const void *ImmutableData()
```

获取 `Buffer` 只读缓存数据指针。

- 返回值

  返回值为只读缓存数据指针。

#### InvalidateBuffer

```cpp
Status InvalidateBuffer()
```

使当前主机上的 Buffer 数据无效化。

- 返回值

  返回值状态码为 `K_OK` 时表示无效化成功，否则返回其他错误码。

## CreateParam

\#include &lt;[datasystem/object_cache/object_client.h](../../../../include/datasystem/object_cache/object_client.h)&gt;

CreateParam 类用于配置对象的相关属性，当前定义如下：

```cpp
struct CreateParam {
    WriteMode writeMode = WriteMode::NONE_L2_CACHE;
    ConsistencyType consistencyType = ConsistencyType::PRAM;
};
```

详细参数解释见 [WriteMode](#writemode) 和 [ConsistencyType](#consistencytype) 章节。

## WriteMode

\#include &lt;[datasystem/object_cache/object_enum.h](../../../../include/datasystem/object_cache/object_enum.h)&gt;

WriteMode 类用于配置对象可靠性，当前定义如下：

```cpp
enum class WriteMode : int {
    NONE_L2_CACHE = 0,
    WRITE_THROUGH_L2_CACHE = 1,
    WRITE_BACK_L2_CACHE = 2,
    NONE_L2_CACHE_EVICT =  3,
};
```

目前，支持以下 `WriteMode`：
|定义|                                 说明 |
|----|--------------------------------------|
|`WriteMode.NONE_L2_CACHE`           | 对象仅写入到缓存中。默认配置|
|`WriteMode.WRITE_THROUGH_L2_CACHE`  | 对象同步写入缓存和二级缓存中。|
|`WriteMode.WRITE_BACK_L2_CACHE`     | 对象同步写入缓存，异步写入二级缓存中。|
|`WriteMode.NONE_L2_CACHE_EVICT`     | 对象是易失性的，如果缓存资源缺乏，对象可能会提前退出生命周期。|


## ConsistencyType

\#include &lt;[datasystem/object_cache/object_enum.h](../../../../include/datasystem/object_cache/object_enum.h)&gt;

WriteMode 类用于配置对象一致性，当前定义如下：

```cpp
enum class ConsistencyType : int {
    PRAM = 0,
    CAUSAL = 1,
};

```

目前，支持以下 `ConsistencyType`：
|定义|                                 说明 |
|----|--------------------------------------|
|`ConsistencyType.PRAM`          |     PRAM 一致性。|
|`ConsistencyType.CAUSAL`        |     因果一致性。|

## enum CacheType
\#include &lt;[datasystem/object_cache/object_enum.h](../../../../include/datasystem/object_cache/object_enum.h)&gt;

CacheType 类用于配置对象保存位置，当前定义如下：

```cpp
enum class CacheType : int {
    MEMORY = 0,
    DISK = 1,
};

```

目前，支持以下 `CacheType`：
|定义|                                 说明 |
|----|--------------------------------------|
|`CacheType.MEMORY`          |     保存到内存|
|`CacheType.DISK`        |     保存到磁盘|
