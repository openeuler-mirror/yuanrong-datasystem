# Common

[![查看源文件](https://mindspore-website.obs.cn-north-4.myhuaweicloud.com/website-images/r2.5.0/resource/_static/logo_source.svg)](../../../../include/datasystem/utils)

## 接口汇总

| 类名 | 说明 |
| --- | --- |
| [Context](#context) | 保存执行中的上下文配置。|
| [ConnectOptions](#connectoptions) | 配置对象客户端的初始化参数类。|
| [Optional](#optional) | 可将任意类型视为 null 的类。|
| [Status](#status) | 返回状态类。|
| [StringView](#stringview) | 免拷贝字符串类。|

## Context

\#include &lt;[datasystem/context/context.h](../../../../include/datasystem/context/context.h)&gt;

Context 类用于保存执行中的上下文配置。

### 构造函数和析构函数

```cpp
Context()
~Context() = default;
```

### 公有成员函数

| 函数                                                                            | 说明 |
|-------------------------------------------------------------------------------|--------|
| [`static Status SetTraceId(const std::string &traceId)`](#settraceid)     |    设置 traceID，可用于请求链路跟踪。    |
| [`static void SetTenantId(const std::string &tenantId)`](#settenantid)     |    设置运行时的租户 ID。    |

#### SetTraceId

```cpp
static Status SetTraceId(const std::string &traceId)
```

设置 traceID，可用于请求链路跟踪。

- 参数

    - `traceId`: 用户描述跟踪链路 ID。

- 返回值

  返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

#### SetTenantId

```cpp
static void SetTenantId(const std::string &tenantId)
```

设置运行时的租户 ID。

- 参数

    - `tenantId`: 运行时的租户 ID。

- 返回值

  返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

## ConnectOptions

\#include &lt;[datasystem/utils/connection.h](../../../../include/datasystem/utils/connection.h)&gt;

ConnectOptions 类用于配置对象客户端的初始化参数。

```cpp
struct ConnectOptions {
    std::string host;
    int32_t port;
    int32_t connectTimeoutMs = 60 * 1000;  // 60s
    std::string clientPublicKey = "";
    SensitiveValue clientPrivateKey = "";
    std::string serverPublicKey = "";
    std::string accessKey = "";
    SensitiveValue secretKey = "";
    std::string tenantId = "";
    bool enableCrossNodeConnection = false;
}
```

参数具体说明如下：
|参数|                                 说明 |
|----|--------------------------------------|
|host          | 数据系统 Worker 的主机 IP 地址。   |
|port        |  数据系统 Worker 的主机 IP 端口号。   |
|connectTimeoutMs        |   客户端连接和请求超时时间，单位为毫秒。默认值：60'000  |
|clientPublicKey          |  用于 curve 认证的客户端公钥。默认值：""  |
|clientPrivateKey        |  用于 curve 认证的客户端私钥。默认值：""   |
|serverPublicKey        |   用于 curve 认证的服务端公钥。默认值：""  |
|accessKey          |  AK/SK 授权使用的访问密钥。默认值：""  |
|secretKey        |  AK/SK 授权的密钥。默认值：""   |
|tenantId        |   租户 ID。默认值：""  |
|enableCrossNodeConnection        |  如果为 `true`，允许客户端在与当前数据系统Worker 连接异常时自动切换到备用节点。默认值：`false`   |

## Optional

\#include &lt;[datasystem/object_cache/utils/optional.h](../../../../include/datasystem/utils/optional.h)&gt;

用于表示一个值可能存在，当值不存在时表示为 null。在 [Get](./object_cache.md#get) 接口的传出参数通过 `Optional` 表示对象是否获取成功。

### 构造函数和析构函数

```cpp
constexpr Optional()
Optional(const Optional &)
Optional &operator=(const Optional &)
Optional(Optional &&other)  noexcept
Optional &operator=(Optional &&)  noexcept
template <typename... Args> explicit Optional(Args &&... args);
~Optional()
```

## Status

\#include &lt;[datasystem/object_cache/utils/status.h](../../../../include/datasystem/utils/status.h)&gt;

Status 类用于表示请求/方法的执行结果。

### 构造函数和析构函数

```cpp
Status() noexcept
Status(const Status &other)
Status(Status &&other) noexcept
Status(StatusCode code, std::string msg)
Status(StatusCode code, int lineOfCode, const std::string &fileName, const std::string &extra = "")
~Status() noexcept
```

### 公有成员函数

| 函数                                                                            | 说明 |
|-------------------------------------------------------------------------------|--------|
| [`static Status OK()`](#ok)     |    返回状态码为 `K_OK` 的 Status。    |
| [`std::string ToString() const`](#tostring)     |    返回 Status 的状态码和报错信息。    |
| [`StatusCode GetCode() const`](#getcode)     |    返回 Status 的状态码。    |
| [`std::string GetMsg() const`](#getmsg)     |    返回 Status 的报错信息。    |
| [`void AppendMsg(const std::string &appendMsg)`](#appendmsg)     |    拼接 Status 的报错信息。    |
| [`explicit operator bool() const`](#operator_bool)     |    将 Status 对象自动转换为`bool`值    |
| [`bool operator==(const Status &other) const`](#operator_equal)     |    重载 Status `==` 运算符   |
| [`bool operator!=(const Status &other) const`](#operator_not_equal)     |    重载 Status `!=` 运算符   |
| [`bool IsOk() const`](#isok)     |    判断 Status 的状态码是否为 `K_OK`。    |
| [`bool IsError() const`](#iserror)     |    判断 Status 的状态码是否为非 `K_OK` 错误码。    |
| [`static std::string StatusCodeName(StatusCode code)`](#statuscodename)     |    获取状态码的字符串表示，多用于打印需求。    |

#### OK

```cpp
static Status OK()
```

返回状态码为 `K_OK` 的 Status。

- 返回值

  状态码为 `K_OK` 的 Status。

#### ToString

```cpp
std::string ToString() const
```

返回 Status 的状态码和报错信息。

- 返回值

  Status 的状态码和报错信息。

#### GetCode

```cpp
StatusCode GetCode() const
```

返回 Status 的状态码。

- 返回值

  Status 的状态码。

#### GetMsg

```cpp
std::string GetMsg() const
```

返回 Status 的报错信息。

- 返回值

  Status 的报错信息。

#### AppendMsg

```cpp
void AppendMsg(const std::string &appendMsg)
```

拼接 Status 的报错信息。

- 参数

  -  `appendMsg`: 需要拼接的报错信息。

<a id="operator_bool"></a>
#### operator bool

```cpp
explicit operator bool() const
```

将 Status 对象自动转换为`bool`值，在 if、while、for 或逻辑运算（&&、||、!）中，对象可以像 `bool` 一样被检查。

#### operator==

<span id="operator_equal"></span>
```cpp
bool operator==(const Status &other) const
```

重载 Status `==` 运算符，用于比较两个 Status 对象是否相等。

- 参数

  -  `other`: 待比较的 Status 对象。

- 返回值

  `true`表示两个 Status 对象相等。

#### operator!=

<span id="operator_not_equal"></span>
```cpp
bool operator!=(const Status &other) const
```

重载 Status `!=` 运算符，用于比较两个 Status 对象是否不相等。

- 参数

  -  `other`: 待比较的 Status 对象。

- 返回值

  `true`表示两个 Status 对象不相等。

#### IsOK

```cpp
bool IsOk() const
```

判断 Status 的状态码是否为 `K_OK`。

- 返回值

  `true`表示 Status 的状态码为 `K_OK`。

#### IsError

```cpp
bool IsError() const
```

判断 Status 的状态码是否为非 `K_OK` 错误码。

- 返回值

  `true`表示 Status 的状态码为非 `K_OK 错误码`。

#### StatusCodeName

```cpp
static std::string StatusCodeName(StatusCode code)
```

获取状态码的字符串表示，多用于打印需求。

- 参数

  -  `code`: 状态码。

- 返回值

  状态码的字符串表示。


## StringView

\#include &lt;[datasystem/object_cache/utils/string_view.h](../../../../include/datasystem/utils/string_view.h)&gt;

字符串视图类，主要用于高效地传递和访问字符串数据，而无需复制或分配内存。

### 构造函数和析构函数

```cpp
constexpr StringView() noexcept
constexpr StringView(const StringView &) noexcept
constexpr StringView(const char *str)
constexpr StringView(const char *str, size_t len)
StringView(const std::string &str)
constexpr StringView &operator=(const StringView &) noexcept
~StringView()
```

### 公有成员函数
| 函数                                                                            | 说明 |
|-------------------------------------------------------------------------------|--------|
| [`constexpr const char *data() const noexcept`](#data)     |    获取 StringView 的数据指针。    |
| [`constexpr size_t size() const noexcept`](#size)     |    获取 StringView 的数据大小。    |
| [`constexpr bool empty() const noexcept`](#empty)     |    判断 StringView 是否为空。    |

#### data

```cpp
constexpr const char *data() const noexcept
```

获取 StringView 的数据指针。

- 返回值

  StringView 的数据指针。

#### size

```cpp
constexpr size_t size() const noexcept
```

获取 StringView 的数据大小。

- 返回值

  StringView 的数据大小。

#### empty

```cpp
constexpr size_t empty() const noexcept
```

判断 StringView 是否为空。

- 返回值

  `true` 表示 StringView 为空。