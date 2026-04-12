yr.datasystem.DsTensorClient.dev_mget_into_tensor
======================================================

.. py:method:: yr.datasystem.DsTensorClient.dev_mget_into_tensor(keys, tensor, copy_ranges, sub_timeout_ms)

    从 device 中获取多个 key 的数据，并根据用户定义的复制范围将每个数据段复制到单个目标 Tensor 的指定位置。
    数据通过 device to device 通道直接传输。

    dev_mset 和 dev_mget_into_tensor 需配套使用。dev_mget_into_tensor 执行后不会自动删除异构对象，
    如对象不再使用，可调用 dev_local_delete、async_dev_delete 或 dev_delete 删除。

    参数：
        - **keys** (list) - device 的异构对象的 key 列表。约束：传入的key的数量 `<=10000`。
        - **tensor** (Tensor) - 目标 Tensor，用于接收复制的数据。Tensor 必须有足够的大小以容纳所有指定的复制范围。
        - **copy_ranges** (list) - CopyRange 对象列表，每个 CopyRange 由 [src_offset (int), dst_offset (int), length (int)] 组成，分别表示源数据中的起始偏移量、目标 Tensor 中的起始偏移量和要复制的数据长度（均以字节为单位）。约束：keys 和 copy_ranges 的长度必须相同。
        - **sub_timeout_ms** (int) - 获取操作的超时时间（以毫秒为单位）。默认为``60s``。

    返回：
        - **failed_keys** (list) - 用于描述 dev_mget_into_tensor 操作失败的 key 列表。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 获取所有指定 key 的值失败时抛出运行时错误。

