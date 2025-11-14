datasystem.DsTensorClient.dev_mget
==============================================

.. py:method:: datasystem.DsTensorClient.dev_mget(keys, tensors, sub_timeout_ms)

    获取 device 中的数据，并写入到 tensors 的 Tensor 中。数据通过 device to device 通道直接传输。dev_mset 和 dev_mget 需配套使用。

    dev_mget 后不会自动删除异构对象，如对象不再使用，可调用 dev_local_delete 或 dev_delete 删除。

    在执行 dev_mget 过程中，执行了 dev_mset 的进程不能退出，否则 dev_mget 会失败。

    参数：
        - **keys** (list) - device 的异构对象的 key。约束：传入的key的数量不能超过1万。
        - **tensors** (list) - : mindspore或pytorch的Tensor列表。约束：Tensor的地址空间必须连续。
        - **sub_timeout_ms** (int) - 超时时间，当在指定时间内无法获取完成，则抛出异常。

    返回：
        - **failed_keys** (list) - 用于描述get失败的key。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未获取成功。