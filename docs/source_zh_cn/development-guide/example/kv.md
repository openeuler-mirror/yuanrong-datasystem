# KV

## 基本概念

openYuanrong datasystem (下文中称为数据系统)提供了近计算 KV 缓存能力，基于共享内存实现免拷贝的 KV 数据读写，实现高性能数据缓存。同时 KV 接口通过对接外部组件提供数据可靠性语义。

## 样例代码

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
```

:::

::::

## 使用限制

- key 仅支持大写字母、小写字母、数字以及如下特定字符：`-_!@#%^*()+=:;`。
- key 的最大长度为 255 字节。
- value 的最大长度没有限制，但是不能超出配置的共享内存大小。
- 未写入二级缓存的数据，不保证数据可靠性，当发生故障时数据可能会丢失。

## 关于 KV 更多信息

### 数据一致性

KV 接口支持 Causal 级别数据读写一致性。
一致性模型定义参见 [Consistency Models](https://jepsen.io/consistency/models)。

### 数据溢出到磁盘

KV 数据存储在数据系统的共享内存中，当内存不足时，支持自动将数据溢出到磁盘并从内存中删除数据。当数据需要读取时，自动从磁盘中加载到共享内存。
若磁盘空间也不足时，如果数据已写入到二级缓存，则自动将数据从本地磁盘和内存中删除。当数据需要读取时，自动从二级缓存加载到共享内存。
使用 KV 溢出功能，需要在部署时指定相关参数，默认为关闭。

```yaml
# The path of the spilling, empty means local_dick spill disabled.
# It will create a new subdirectory("datasystem_spill_data") under the SPILL_DIRECTORY to store the spill file.
# Example: If SPILL_DIRECTORY is "/home/spill", spill files will exist in the "/home/spill/datasystem_spill_data".
spillDirectory: ""
```

数据溢出有以下参数，可用于设置磁盘空间上限、溢出的并发线程、文件大小等参数，用于性能调优。

```yaml
# Maximum amount of spilled data that can be stored in the spill directory. If spill is enable and spillSizeLimit is 0, spillSizeLimit will be set to 95% of the spill directory.
# Unit for spillSizeLimit is Bytes.
spillSizeLimit: "0"
# It represents the maximum parallelism of writing files, more threads will consume more CPU and I/O resources.
spillThreadNum: 8
# The size limit of single spill file, spilling objects which lager than that value with one object per file.
# If there are some big objects, you can increase this value to avoid run out of inodes quickly.
# The valid range is 200-10240.
spillFileMaxSizeMb: 200
# The maximum number of open file descriptors about spill. If opened file exceed this value,
# some files will be temporarily closed to prevent exceeding the maximum system limit. You need reduce this value if your system resources are limited.
# The valid range is greater than or equal to 8.
spillFileOpenLimit: 512
# Disable readahead can mitigate the read amplification problem for offset read, default is true
spillEnableReadahead: true
# Thread number of eviction for object cache.
evictionThreadNum: 1
```

### 数据可靠性

数据系统 KV 接口提供可靠性语义，在数据写入时，通过 **writeMode** 参数配置数据可靠性级别。仅当 **writeMode** 配置为 ***WRITE_THROUGH_L2_CACHE*** 或 ***WRITE_BACK_L2_CACHE*** 时才保证数据可靠性，否则当出现故障或空间不足时，数据可能丢失。  
数据系统通过对接外部存储组件作为二级缓存实现数据可靠性。当前支持的二级缓存组件有：OBS/SFS。  
在集群部署时，需要在数据系统的部署参数中配置二级缓存相关参数，若未配置，则在 KV 写入时 **writeMode** 参数指定为 ***WRITE_THROUGH_L2_CACHE*** 或 ***WRITE_BACK_L2_CACHE*** 时会写入失败。

集群部署时通过以下参数指定二级缓存类型。

```yaml
# 指定二级缓存的类型。可选值为：'obs', 'sfs'.
# 默认值为'none'，表示不支持二级缓存。
l2CacheType: "none"
```

对接各类外部组件的配置参数如下：
::::{tab-set}

:::{tab-item} obs

```yaml
obs:
  # The access key for obs AK/SK authentication. If the value of encryptKit is not plaintext, encryption is required.
  obsAccessKey: ""
  # The secret key for obs AK/SK authentication. If the value of encryptKit is not plaintext, encryption is required.
  obsSecretKey: ""
  # OBS endpoint. Example: "xxx.hwcloudtest.cn"
  obsEndpoint: ""
  # OBS bucket name.
  obsBucket: ""
  # Whether to enable the https in obs. false: use HTTP (default), true: use HTTPS
  obsHttpsEnabled: false
  # Use cloud service token rotation to connect obs.
  cloudServiceTokenRotation:
    # Whether to use ccms credential rotation mode to access OBS, default is false. If is enabled, need to specify
    # iamHostName, identityProvider, projectId, regionId at least.
    # In addition, obsEndpoint and obsBucket need to be specified.
    enable: false
    # Domain name of the IAM token to be obtained. Example:  iam.example.com.
    iamHostName: ""
    # Provider that provides permissions for the ds-worker. Example: csms-datasystem.
    identityProvider: ""
    # Project id of the OBS to be accessed. Example: fb6a00ff7ae54a5fbb8ff855d0841d00.
    projectId: ""
    # Region id of the OBS to be accessed. Example: cn-beijing-4.
    regionId: ""
    # Whether to access OBS of other accounts by agency, default is false. If is true, need to specify tokenAgencyName
    # and tokenAgencyDomain.
    enableTokenByAgency: false
    # Agency name for proxy access to other accounts. Example: obs_access.
    tokenAgencyName: ""
    # Agency domain for proxy access to other accounts. Example: op_svc_cff.
    tokenAgencyDomain: ""
```

:::

:::{tab-item} sfs

```yaml
# The path to the mounted SFS.
sfsPath: ""
```

:::

::::
