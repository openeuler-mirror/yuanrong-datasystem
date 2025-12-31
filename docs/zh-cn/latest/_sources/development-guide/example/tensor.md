# Tensor

## 基本概念

openYuanrong datasystem (下文中称为数据系统)的 Tensor 接口，基于 Device 侧的 HBM 内存抽象 Tensor 对象接口，支持 PyTorch 和 MindSpore 框架的 Tensor 操作。Tensor 接口提供了 D2D(Device to Device)、D2H(Device to Host)、H2D(Host to Device) 等多种数据传输能力，实现昇腾 NPU 卡间数据高速直通传输以及 DRAM/HBM 之间的快速迁移。

> **注意**：  
> Tensor 接口要求传入的 Tensor 地址空间必须连续。  
> 数据系统并不直接负责 Tensor 内存的申请及释放，用户需要保证传入的 Tensor 内存的有效性。

## D2D(Device to Device) 数据传输

Tensor 接口针对 D2D 数据传输，提供了两种语义：

**dev_send / dev_recv**：数据传输语义，数据生成端执行 dev_send 将 Device 上的 Tensor 发布为异构对象，数据接收端申请 Tensor 内存后，执行 dev_recv 订阅接收数据，数据系统使用卡间直通传输数据并写入用户提供的 Tensor 中。当数据被接收后，数据系统会自动删除对象，不再关联发布的 Tensor 内存。  
dev_send / dev_recv 为异步接口，提供了返回 Future 供用户获取执行结果，每个key返回一个 Future。当 Future::Get 获取到结果为 OK 时，表示数据已经被对端接收成功。

> **注意**：  
> dev_send / dev_recv 传入的 Device 内存地址不能归属于同一张 NPU 卡。  
> 在执行 dev_recv 过程中，执行了 dev_send 的进程不能退出，否则 dev_recv 会失败。  
> dev_recv单Key的订阅超时时间为20s，多key为60s。

**dev_mset / dev_mget**：数据缓存语义，通过数据系统缓存 Device 上的 Tensor 数据，将 tensors 对应的 key 的元数据写入数据系统，可供其他 client 访问。dev_mget 后不会自动删除异构对象，如对象不再使用，可调用 dev_local_delete 或 dev_delete 删除。

## D2H/H2D(Device to Host / Host to Device) 数据传输

Tensor 接口提供了 D2H/H2D 数据传输能力，实现数据在 DRAM/HBM 之间的快速迁移：

**mset_d2h / mget_h2d**：将 Device 上的 Tensor 数据写入到 Host 中，或从 Host 中获取数据并写入 Device 中的 Tensor。支持同步和异步两种模式（async_mset_d2h / async_mget_h2d）。

## 样例代码

::::{tab-set}

:::{tab-item} Python (PyTorch)

```python
import acl
import torch
import torch_npu
from yr.datasystem import DsTensorClient

# D2D 数据传输示例
def tensor_dev_send_recv_example():
    # 发送端
    acl.init()
    device_idx = 1
    acl.rt.set_device(device_idx)
    torch_npu.npu.set_device(f'npu:{device_idx}')
    
    client = DsTensorClient("127.0.0.1", 31501, device_idx)
    client.init()
    
    key = "tensor_key"
    tensor = torch.rand((2, 3), dtype=torch.float16, device=f'npu:{device_idx}')
    futures = client.dev_send([key], [tensor])
    futures[0].get()
    
    # 接收端（需要在另一个进程中运行，绑定不同的NPU）
    # acl.init()
    # device_idx = 2
    # acl.rt.set_device(device_idx)
    # torch_npu.npu.set_device(f'npu:{device_idx}')
    # 
    # client = DsTensorClient("127.0.0.1", 31501, device_idx)
    # client.init()
    # 
    # key = "tensor_key"
    # tensor = torch.zeros((2, 3), dtype=torch.float16, device=f'npu:{device_idx}')
    # futures = client.dev_recv([key], [tensor])
    # futures[0].get()

# Device 数据缓存示例
def tensor_dev_mset_mget_example():
    acl.init()
    device_idx = 0
    acl.rt.set_device(device_idx)
    torch_npu.npu.set_device(f'npu:{device_idx}')
    
    client = DsTensorClient("127.0.0.1", 31501, device_idx)
    client.init()
    
    key = "tensor_key"
    in_tensor = torch.rand((2, 3), dtype=torch.float16, device=f'npu:{device_idx}')
    failed_keys = client.dev_mset([key], [in_tensor])
    if failed_keys:
        raise RuntimeError(f"dev_mset failed, failed keys: {failed_keys}")
    
    out_tensor = torch.zeros((2, 3), dtype=torch.float16, device=f'npu:{device_idx}')
    sub_timeout_ms = 30_000
    failed_keys = client.dev_mget([key], [out_tensor], sub_timeout_ms)
    if failed_keys:
        raise RuntimeError(f"dev_mget failed, failed keys: {failed_keys}")

# D2H/H2D 数据传输示例
def tensor_d2h_h2d_example():
    acl.init()
    device_idx = 0
    acl.rt.set_device(device_idx)
    torch_npu.npu.set_device(f'npu:{device_idx}')
    
    client = DsTensorClient("127.0.0.1", 31501, device_idx)
    client.init()
    
    key = "tensor_key"
    tensor = torch.rand((2, 3), dtype=torch.float16, device=f'npu:{device_idx}')
    from yr.datasystem.kv_client import SetParam, WriteMode
    set_param = SetParam(write_mode=WriteMode.NONE_L2_CACHE)
    client.mset_d2h([key], [tensor], set_param)
    
    out_tensor = torch.zeros((2, 3), dtype=torch.float16, device=f'npu:{device_idx}')
    client.mget_h2d([key], [out_tensor])
```

:::

:::{tab-item} Python (MindSpore)

```python
import acl
import numpy
import mindspore
from yr.datasystem import DsTensorClient

# Device 数据缓存示例
def tensor_dev_mset_mget_example():
    acl.init()
    device_idx = 0
    acl.rt.set_device(device_idx)
    mindspore.set_device(device_target="Ascend", device_id=device_idx)
    
    client = DsTensorClient("127.0.0.1", 31501, device_idx)
    client.init()
    
    key = "tensor_key"
    data = numpy.random.rand(2, 3)
    in_tensor = mindspore.Tensor(data, dtype=mindspore.float32) + 0
    failed_keys = client.dev_mset([key], [in_tensor])
    if failed_keys:
        raise RuntimeError(f"dev_mset failed, failed keys: {failed_keys}")
    
    out_tensor = mindspore.Tensor(numpy.ones(shape=[2, 3]), dtype=mindspore.float32) + 0
    sub_timeout_ms = 30_000
    failed_keys = client.dev_mget([key], [out_tensor], sub_timeout_ms)
    if failed_keys:
        raise RuntimeError(f"dev_mget failed, failed keys: {failed_keys}")

# D2H/H2D 数据传输示例
def tensor_d2h_h2d_example():
    acl.init()
    device_idx = 0
    acl.rt.set_device(device_idx)
    mindspore.set_device(device_target="Ascend", device_id=device_idx)
    
    client = DsTensorClient("127.0.0.1", 31501, device_idx)
    client.init()
    
    key = "tensor_key"
    data = numpy.random.rand(2, 3)
    tensor = mindspore.Tensor(data, dtype=mindspore.float32) + 0
    from yr.datasystem.kv_client import SetParam, WriteMode
    set_param = SetParam(write_mode=WriteMode.NONE_L2_CACHE)
    client.mset_d2h([key], [tensor], set_param)
    
    out_tensor = mindspore.Tensor(numpy.zeros(shape=[2, 3]), dtype=mindspore.float32) + 0
    client.mget_h2d([key], [out_tensor])
```

:::

::::

## 使用限制

- key 仅支持大写字母、小写字母、数字以及如下特定字符：`-_!@#%^*()+=:;`。
- key 的最大长度为 255 字节。
- Tensor 的地址空间必须连续。
- dev_send / dev_recv 传入的 Device 内存地址不能归属于同一张 NPU 卡。
- dev_recv单Key的订阅超时时间为20s，多key为60s。

## 关于 Tensor 更多信息

### PagedAttention 支持

Tensor 接口提供了针对 PagedAttention 场景的专用接口，支持层级和块级别的 Tensor 操作：

- **put_page_attn_layerwise_d2d / get_page_attn_layerwise_d2d**：将 PagedAttention 的层级 Tensor 发布为数据系统的异构对象，支持 D2D 传输。
- **mset_page_attn_blockwise_d2h / mget_page_attn_blockwise_h2d**：将 PagedAttention 的块级别 Tensor 异步写入 Host 或从 Host 中获取。

### dev_mget_into_tensor

Tensor 接口提供了 `dev_mget_into_tensor` 接口，支持从 device 中获取多个 key 的数据，并根据用户定义的复制范围将每个数据段复制到单个目标 Tensor 的指定位置。该接口适用于需要将多个数据源合并到单个 Tensor 的场景。

