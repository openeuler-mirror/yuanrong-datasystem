# ObjectClient编程示例

## 基本概念

openYuanrong datasystem (下文中称为数据系统)的Object接口，基于共享内存实现 host上的 Object 语义读写，提供基于引用计数的生命周期管理，将共享内存抽象为 buffer，直接映射共享内存指针，提供更底层灵活的编程接口。

## 样例代码

Object 接口中提供了 buffer 类，其中封装了共享内存，可直接将指针映射，并提供了锁保证数据一致性。
Object 当前支持基于引用计数的生命周期管理，当对象的引用计数大于 0 时，保持对象的生命周期，当对象的引用计数变为 0 时，则删除对象。

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

if __name__ == "__main__":
    object_test()
```

:::

:::{tab-item} C++

```cpp
#include "datasystem/datasystem.h"

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
```

:::

::::

## 数据一致性

Object 接口支持可变的读写一致性，当前支持 PRAM 和 Causal 两种一致性级别。在创建对象时，可通过 **CreateParam.consistencyType** 参数指定一致性级别。
一致性模型定义参见 [Consistency Models](https://jepsen.io/consistency/models)。

## 数据溢出到磁盘

Object 接口支持将数据溢出到磁盘，相关配置与 KV 相同，详情参见 [KV](kv.md#数据溢出到磁盘) 中的描述。

## 使用限制

- key 仅支持大写字母、小写字母、数字以及如下特定字符：`-_!@#%^*()+=:;`。
- key 的最大长度为 255 字节。
- value 的最大长度没有限制，但是不能超出配置的共享内存大小。
- 未写入二级缓存的数据，不保证数据可靠性，当发生故障时数据可能会丢失。