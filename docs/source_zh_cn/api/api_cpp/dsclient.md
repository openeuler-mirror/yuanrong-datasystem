# DsClient

[![查看源文件](https://mindspore-website.obs.cn-north-4.myhuaweicloud.com/website-images/r2.5.0/resource/_static/logo_source.svg)](../../../../include/datasystem/datasystem.h)

## DsClient

\#include &lt;[datasystem/datasystem.h](../../../../include/datasystem/datasystem.h)&gt;

DsClient 为yuanrong-datasystem的客户端总入口，其中包含了异构对象、KV和object多类客户端。

### 构造函数和析构函数

```cpp
explicit DsClient(const ConnectOptions &connectOptions = {})
~DsClient()
```

### 公有成员函数
| 函数  | 说明 |
|------------------------------------|--------|
| [`Status Init()`](#init)   |    初始化客户端。    |
| [`Status ShutDown()`](#shutdown)      |   关闭客户端。    |
| [`std::shared_ptr<KVClient> KV()`](#kv)      |    获取 KV 客户端。    |
| [`std::shared_ptr<HeteroClient> Hetero()`](#hetero)  |    获取异构对象客户端。    |
| [`std::shared_ptr<ObjectClient> Object()`](#object)  |    获取 object 客户端。    |

#### Init

```cpp
Status Init();
```

初始化客户端，建立与数据系统 Worker 之间的连接。

- 返回值

  返回值状态码为 `K_OK` 时表示初始化成功，否则返回其他错误码。

#### ShutDown

```cpp
Status ShutDown();
```

关闭客户端，断开与数据系统 Worker 之间的连接。

- 返回值

  返回值状态码为 `K_OK` 时表示关闭成功，否则返回其他错误码。

#### KV

```cpp
std::shared_ptr<KVClient> KV();
```

获取 KV 客户端。

- 返回值

  - std::shared_ptr\<KVClient\>，返回 KVClient 客户端指针。

#### Hetero

```cpp
std::shared_ptr<HeteroClient> Hetero();
```

获取异构对象客户端。

- 返回值

  - std::shared_ptr\<HeteroClient\>，返回异构对象客户端指针。

#### Object

```cpp
std::shared_ptr<ObjectClient> Object();
```

获取 object 客户端。

- 返回值

  - std::shared_ptr\<ObjectClient\>，返回 object 客户端指针。
