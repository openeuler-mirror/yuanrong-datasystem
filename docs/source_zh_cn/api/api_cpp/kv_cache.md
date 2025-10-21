# KVClient

[![查看源文件](https://mindspore-website.obs.cn-north-4.myhuaweicloud.com/website-images/r2.5.0/resource/_static/logo_source.svg)](../../../../include/datasystem/kv_cache.h)

## 接口汇总

| 类名                        | 说明                                |
|--------------------------------|---------------------------------------------|
|[`enum ExistenceOpt`](#enum-existenceopt)            |该选项用于配置对象存在时是否允许继续操作  |
|[`class KVClient`](#class-kvclient) | KV缓存客户端类|
|[`class ReadOnlyBuffer`](#class-readonlybuffer) | 只读缓冲区结构类 |
|[`struct MSetParam`](#struct-msetparam) | 同时创建多个Key时设置的属性 |
|[`struct ReadParam`](#struct-readparam) | 偏移读取Key时设置的属性 |
|[`struct SetParam`](#struct-setparam) | 创建Key时设置的属性 |


## enum ExistenceOpt

\#include &lt;[datasystem/kv_cache/kv_client.h](../../../../include/datasystem/kv_cache/kv_client.h)&gt;

用于配置对象存在时是否允许继续操作的枚举类。

 定义                         | 说明
--------------------------------|---------------------------------------------
NONE            | 允许Key存在时执行Set
NX            | 不允许Key存在时执行Set

## class KVClient

\#include &lt;[datasystem/kv_cache/kv_client.h](../../../../include/datasystem/kv_cache/kv_client.h)&gt;

KV缓存对象类

### 构造函数和析构函数
#### explicit KVClient (const ConnectOptions &connectOptions)

构造[KVClient](#class-kvclient).

- 参数
  - `connectOptions` 配置与 Worker 的连接选项，包括IP地址和端口，详见[ConnectOptions](common.md#connectoptions)章节


#### ~KVClient ()

析构[KVClient](#class-kvclient).

### 公有成员函数

 函数                        | 说明
--------------------------------|---------------------------------------------
[`Status Init ()`](#status-init-) | 建立[KVClient](#class-kvclient)与当前数据系统 Worker 之间的连接并完成初始化。
[`Status ShutDown ()`](#status-shutdown-) | 断开[KVClient](#class-kvclient)与当前数据系统 Worker 之间的连接。
[`Status Set (const std::string &key, const StringView &val, const SetParam &param)`](#set-by-key) | 设置键值对数据。
[`std::string Set (const StringView &val, const SetParam &param)`](#set-by-value) | 设置键值对数据，用户传入值，由接口自动生成键并返回。
[`Status MSetTx (const std::vector<std::string> &keys, const std::vector<StringView> &vals, const MSetParam &param)`](#status-msettx) | 事务性创建多键值对接口。
[`Status MSet (const std::vector<std::string> &keys, const std::vector<StringView> &vals, std::vector<std::string> &outFailedKeys, const MSetParam &param)`](#status-mset) | 键值对批量设置接口。
[`Status Get (const std::string &key, std::string &val, int32_t subTimeoutMs)`](#status-get) | 获取单个数据。
[`Status Get (const std::string &key, Optional<ReadOnlyBuffer> &readOnlyBuffer, int32_t subTimeoutMs)`](#get-readonlybuffer) | 获取单个数据的指针，获取后不可修改。
[`Status Get (const std::vector<std::string> &keys, std::vector<std::string> &vals, int32_t subTimeoutMs)`](#batchget) | 批量获取数据。
[`Status Get (const std::vector<std::string> &keys, std::vector<Optional<ReadOnlyBuffer> &readOnlyBuffers, int32_t subTimeoutMs)`](#batchget-readonlybuffer) | 批量获取多个数据的指针，获取后不可修改。
[`Status Read (const std::vector<ReadParam> &readParams, std::vector<Optional<ReadOnlyBuffer>> &readOnlyBuffers)`](#status-read) | 偏移读接口，支持获取指定键值的其中一部分。
[`Status Del (const std::string &key)`](#del) | 删除指定键值对。
[`Status Del (const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)`](#batchdel) | 批量删除指定键值对。
[`Status GenerateKey (const std::string &prefixKey, std::string &key)`](#generatekey) | 生成具有指定前缀的带workerId的key，以供Set接口使用。
[`Status HealthCheck ()`](#status-healthcheck-) | 检查连接的 Worker 是否健康。
[`Status Expire (const std::vector<std::string> &keys, uint32_t ttlSeconds, std::vector<std::string> &failedKeys)`](#status-expire) | 为指定数据更新过期生命周期时间。

#### Status Init ()

建立[KVClient](#class-kvclient)与当前数据系统 Worker 之间的连接并完成初始化。

- 返回值
  返回值状态码为 `K_OK` 时表示初始化成功，否则返回其他错误码。

#### Status ShutDown ()

断开[KVClient](#class-kvclient)与当前数据系统 Worker 之间的连接。

-  返回值
  返回值状态码为 `K_OK` 时表示断链成功，否则返回其他错误码。

<a id="set-by-key"></a>
#### Status Set (const std::string &key, const StringView &val, const [SetParam](#struct-setparam) &param)

设置键值对数据。

- 参数
  - `key` 键
  - `val` 值
  - `param` 设置参数，详见[SetParam](#struct-setparam)章节

- 返回值
   - 返回值状态码为 `K_OK` 时表示键值对设置成功。
   - 返回值状态码为 `K_INVALID`时表示参数校验不通过

<a id="set-by-value"></a>
#### std::string Set (const StringView &val, const [SetParam](#struct-setparam) &param)

设置键值对数据，用户传入值，由接口自动生成键并返回。

- 参数
  - `val` 值
  - `param` 设置参数，详见[SetParam](#struct-setparam)章节

- 返回值
  - 成功时返回非空字符串
  - 失败时返回空字符串

<a id="status-msettx"></a>
#### Status MSetTx (const std::vector\<std::string\> &keys, const std::vector\<StringView\> &vals, const [MSetParam](#struct-msetparam) &param)

事务性创建多键值对接口。支持事务性地创建一组键值对，键值对数量限制为1个到8个。如果有一个键已存在，则所有键值对都不会创建。


- 参数
  - `keys` 需要设置的一组key. 约束：传入的key的数量不能超过8。
  - `vals` 需要设置的一组key对应的value.
  - `param` 设置参数，详见[MSetParam](#struct-msetparam)章节.

- 返回值
  返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

<a id="status-mset"></a>
#### Status MSet (const std::vector\<std::string\> &keys, const std::vector\<StringView\> &vals, std::vector\<std::string\> &outFailedKeys, const [MSetParam](#struct-msetparam) &param)

键值对批量设置接口。可批量设置键值对并返回失败的键。批量设置个数需小于2000，设置的每个值需小于500KB。

- 参数
  - `keys` 需要设置的一组key. 约束：传入的key的数量需要小于2千。
  - `vals` 需要设置的一组key对应的value.
  - `outFailedKeys` 传出参数，代表设置失败的key.
  - `param` 设置参数，详见[MSetParam](#struct-msetparam)章节.

- 返回值
  返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

<a id="status-get"></a>
#### Status Get (const std::string &key, std::string &val, int32_t subTimeoutMs)

获取单个数据。

- 参数
  - `key` 键.
  - `val` 传出参数，返回键对应的数据.
  - `subTimeoutMs` 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待.

- 返回值
  - 返回`K_OK`表示获取成功
  - 返回`K_INVALID`表示`key`校验不通过
  - 返回`K_NOT_FOUND`表示`key`不存在
  - 返回`K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回`K_RUNTIME_ERROR`表示 worker 侧存在错误

<a id="get-readonlybuffer"></a>
#### Status Get (const std::string &key, Optional\<[ReadOnlyBuffer](#class-readonlybuffer)\> &readOnlyBuffer, int32_t subTimeoutMs)

获取单个数据的指针，获取后不可修改。该接口相比[`Status Get (const std::string &key, std::string &val, int32_t timeoutMs`)](#get)少一次拷贝，性能更优。

- 参数
  - `key` 键.
  - `subTimeoutMs` 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待.

  - `readOnlyBuffer` 传出参数，返回的使用[Optional](common.md#optional)封装的只读数据缓冲区.

- 返回值
  - 返回`K_OK`表示获取成功
  - 返回`K_INVALID`表示`key`校验不通过
  - 返回`K_NOT_FOUND`表示`key`不存在
  - 返回`K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回`K_RUNTIME_ERROR`表示 worker 侧存在错误

<a id="batchget"></a>
#### Status Get (const std::vector\<std::string\> &keys, std::vector\<std::string\> &vals, int32_t subTimeoutMs)

批量获取数据。

- 参数
  - `keys` 需要获取的一组key. 约束：传入的key的数量不能超过1万。
  - `subTimeoutMs` 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待.
  - `vals` 传出参数，返回一组获取的数据。若有部分数据获取不成功，则对应位置的vector的对象为空。

- 返回值
  - 返回`K_OK`表示至少有一个数据获取成功
  - 返回`K_INVALID`表示存在key校验不通过
  - 返回`K_NOT_FOUND`表示所有`keys`不存在
  - 返回`K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回`K_RUNTIME_ERROR`表示 worker 侧存在错误

<a id="batchget-readonlybuffer"></a>
#### Status Get (const std::vector\<std::string\> &keys, std::vector\<Optional\<[ReadOnlyBuffer](#class-readonlybuffer)\>\> &readOnlyBuffers, int32_t subTimeoutMs)

批量获取多个数据的指针，获取后不可修改。该接口相比[`Status Get (const std::vector<std::string> &keys, std::vector<std::string> &vals, int32_t timeoutMs)`](#batchget)少一次拷贝，性能更优。

- 参数
  - `keys` 需要获取的一组key. 约束：传入的key的数量不能超过1万。
  - `subTimeoutMs` 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待.
  - `readOnlyBuffers` 传出参数，返回的一组使用[Optional](common.md#optional)封装的只读数据缓冲区。若有部分数据获取不成功，则对应位置的vector的对象为空。

- 返回值
  - 返回`K_OK`表示至少有一个数据获取成功
  - 返回`K_INVALID`表示存在key校验不通过
  - 返回`K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回`K_NOT_FOUND`表示所有`keys`不存在
  - 返回`K_RUNTIME_ERROR`表示 worker 侧存在错误

<a id="status-read"></a>
#### Status Read (const std::vector\<[ReadParam](#struct-readparam)> &readParams, std::vector\<Optional\<[ReadOnlyBuffer](#class-readonlybuffer)\>\> &readOnlyBuffers)

偏移读接口，支持获取指定键值的其中一部分。在某些场景下，可避免读放大带来的性能损失。

- 参数
  - `readParams` 指定要查询的一个或多个键，详见[ReadParam](#struct-readparam)章节. 约束：传入的数量不能超过1万。
  - `readOnlyBuffers` 传出参数，返回的一组使用[Optional](common.md#optional)封装的只读数据缓冲区。若有部分数据获取不成功，则对应位置的vector的对象为空。

- 返回值
  - 返回`K_OK`表示至少有一个数据获取成功
  - 返回`K_INVALID`表示存在key校验不通过
  - 返回`K_NOT_FOUND`表示所有`keys`不存在
  - 返回`K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回`K_RUNTIME_ERROR`表示 worker 侧存在错误


<a id="del"></a>
#### Status Del (const std::string &key)

删除指定键值对。key不存在时视为删除成功。

- 参数
  - `key` 键.

- 返回值
  返回值状态码为 `K_OK` 时表示初始化成功，否则返回其他错误码。

<a id="batchdel"></a>
#### Status Del (const std::vector\<std::string\> &keys, std::vector\<std::string\> &failedKeys)

批量删除指定键值对。key不存在时视为删除成功。

- 参数
  - `keys` 需要删除的一组key. 约束：传入的key的数量不能超过1万。
  - `failedKeys` 传出参数，返回删除失败的key.

- 返回值
  - 返回`K_OK`表示至少有一个数据删除成功
  - 返回`K_INVALID`表示存在key校验不通过
  - 返回值状态码为 `K_RPC_UNAVAILABLE` 时表示请求遇到了网络错误。
  - 返回`K_RUNTIME_ERROR`表示 worker 侧存在错误

<a id="generatekey"></a>
#### Status GenerateKey (const std::string &prefixKey, std::string &key)

生成具有指定前缀的带workerId的key，以供Set接口使用。具有workerId的key具有元数据本节点亲和性，执行Set时性能更优。

- 参数
  - `prefixKey` 用户指定key的前缀.
  - `key` 传出参数，返回生成的key.

- 返回值
  返回值状态码为 `K_OK` 时表示生成成功，否则返回其他错误码。

#### Status HealthCheck ()

检查连接的 Worker 是否健康。

- 返回值
  返回值状态码为 `K_OK` 时表示 Worker 健康，否则返回其他错误码。

<a id="exist"></a>
#### Status KVClient::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists)

批量查询一组键（keys）是否存在，并返回每个键的存在性状态。支持最多10000个键的查询。

- 参数
  - `keys` 待查询的键列表，最大支持10000个键
  - `exists` 传出参数，返回每个键的存在性状态

- 返回值
  - 返回`K_OK`表示查询成功
  - 返回`K_INVALID`表示提供的键中包含非法字符或为空
  - 返回`K_RPC_UNAVAILABLE` 表示请求遇到了网络错误
  - 返回`K_NOT_READY` 表示服务当前无法处理请求
  - 返回`K_RUNTIME_ERROR`表示 worker 侧存在错误

<a id="status-expire"></a>

#### Status Expire (const std::vector<std::string> &keys, uint32_t ttlSeconds, std::vector<std::string> &failedKeys)

批量为一组键（keys）更新过期生命周期（ttlSeconds），并返回更新失败的键（failedKeys）。最多支持 10000 个键的查询。

- 参数
  - `keys` 待更新生命周期的键列表
  - `ttlSeconds` 为键设置的新的生命周期，单位为秒
  - `failedKeys` 传出参数，返回操作失败的键

- 返回值
  - 返回`K_OK`表示至少有一个键设置生命周期成功
  - 返回`K_INVALID`表示提供的键中包含非法字符或为空
  - 返回`K_NOT_FOUND`表示所有`keys`不存在
  - 返回`K_RPC_UNAVAILABLE` 表示请求遇到了网络错误
  - 返回`K_NOT_READY` 表示服务当前无法处理请求
  - 返回`K_RUNTIME_ERROR`表示 worker 侧存在错误

## class ReadOnlyBuffer

\#include &lt;[datasystem/kv_cache/read_only_buffer.h](../../../../include/datasystem/kv_cache/read_only_buffer.h)&gt;

### 构造函数和析构函数

#### ReadOnlyBuffer () = default

构造只读缓冲区结构类

#### ~ReadOnlyBuffer () = default

析构只读缓冲区结构类

### 公用成员函数

 函数                        | 说明
--------------------------------|----------
[`int64_t GetSize () const`](#int64_t-getsize--const) | 获取数据缓冲区的大小。
[`const void* ImmutableData ()`](#const-void-immutabledata-) | 获取数据缓冲区的只读指针。
[`Status RLatch (uint64_t timeout)`](#status-rlatch-uint64_t-timeout) | 对数据缓冲区加读锁。
[`Status UnRLatch ()`](#status-unrlatch-) | 释放数据缓冲区的读锁。


#### int64_t GetSize () const

获取数据缓冲区的大小。

- 返回值
数据缓冲区的大小，单位是Byte.

#### const void* ImmutableData ()

获取数据缓冲区的只读指针。

- 返回值
数据缓冲区的只读指针

#### Status RLatch (uint64_t timeout)

对数据缓冲区加读锁，保护对应的内存不被并发写（允许并发读）。
- 参数
  - `timeout` 加锁超时时间，单位是：秒，默认超时时间为60秒

- 返回值
  返回值状态码为 `K_OK` 时表示加锁成功，否则返回其他错误码。

#### Status UnRLatch ()

释放数据缓冲区的读锁。

- 返回值
  返回值状态码为 `K_OK` 时表示加锁成功，否则返回其他错误码。

## struct MSetParam

\#include &lt;[datasystem/kv_cache/kv_client.h](../../../../include/datasystem/kv_cache/kv_client.h)&gt;

同时创建多个Key时设置的属性。

 函数                        | 说明
--------------------------------|---------------------------------------------
WriteMode writeMode | 设置数据可靠性级别，默认不写入二级缓存。详见[WriteMode](object_cache.md#writemode)章节
uint32_t ttlSecond | 设置Key的存活时间，单位：秒。Key达到存活时间后系统自动将其删除。默认为0，表示不设置存活时间，Key会一直存在直到显式调用Del接口。注意：系统重启后存活时间计时将重新开始
ExistenceOpt existence | 配置Key存在时是否允许继续操作，默认允许。详见[ExistenceOpt](#enum-existenceopt)章节

## struct ReadParam

\#include &lt;[datasystem/kv_cache/kv_client.h](../../../../include/datasystem/kv_cache/kv_client.h)&gt;

偏移读取Key时设置的属性。

 函数                        | 说明
--------------------------------|---------------------------------------------
std::string key | 要读取的Key
uint64_t offset | 指定从Key对应值的特定偏移量开始读取
uint64_t size | 从偏移量开始，要读取的大小

## struct SetParam

\#include &lt;[datasystem/kv_cache/kv_client.h](../../../../include/datasystem/kv_cache/kv_client.h)&gt;

创建Key时设置的属性。

 函数                        | 说明
--------------------------------|---------------------------------------------
WriteMode writeMode | 设置数据可靠性级别，默认不写入二级缓存。详见[WriteMode](object_cache.md#writemode)章节
uint32_t ttlSecond | 设置Key的存活时间，单位：秒。Key达到存活时间后系统自动将其删除。默认为0，表示不设置存活时间，Key会一直存在直到显式调用Del接口。注意：系统重启后存活时间计时将重新开始
ExistenceOpt existence | 配置Key存在时是否允许继续操作，默认允许。详见[ExistenceOpt](#enum-existenceopt)章节
CacheType | key 数据保存位置 详见[CacheType](object_cache.md#CacheType)
