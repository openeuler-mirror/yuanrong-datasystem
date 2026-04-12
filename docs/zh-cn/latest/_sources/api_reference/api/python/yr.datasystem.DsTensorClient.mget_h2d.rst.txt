yr.datasystem.DsTensorClient.mget_h2d
==============================================

.. py:method:: yr.datasystem.DsTensorClient.mget_h2d(keys, tensors, sub_timeout_ms)

    从 host 中获取数据并写入 device 中。

    mget_h2d 和 mset_d2h 需配套使用。

    若 mset_d2h 时将 Tensor 的 NPU 内存数据写入了 host 的内存，则在 mget_h2d 中自动将 host 内存中的数据写入 Device上的Tensor地址。

    若 host 的 key 不再使用，可调用 delete 接口删除。

    参数：
        - **keys** (list) - host 的 key 列表。约束：传入的object key的数量 `<=10000`。
        - **tensors** (list) - : mindspore或pytorch的Tensor列表。约束：Tensor的地址空间必须连续。
        - **sub_timeout_ms** (int) - 超时时间，以毫秒为单位，当在指定时间内无法获取完成，则抛出异常。

    返回：
        - **failed_keys** (list) - 用于描述 mget_h2d 失败的 key。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未获取成功。