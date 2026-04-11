yr.datasystem.DsTensorClient.mget_page_attn_blockwise_h2d
=========================================================

.. py:method:: yr.datasystem.DsTensorClient.mget_page_attn_blockwise_h2d(keys, layer_tensors, block_ids, sub_timeout_ms)

    从 host 中获取 PagedAttention 的层级 Tensor 并写入 device 中。

    mget_page_attn_blockwise_h2d 和 mset_page_attn_blockwise_d2h 需配套使用。

    若 mset_page_attn_blockwise_d2h 时将多个内存地址拼接写入了 Host 的内存，则在 mget_page_attn_blockwise_h2d 中自动将 Host 内存中的数据拆分成多个内存地址写入 Device。

    若 host 的 key 不再使用，可调用 delete 接口删除。

    参数：
        - **keys** (list) - Host 的键列表。约束：最多允许10,000个键。
        - **layer_tensors** (list) - PyTorch 的 Tensor 列表。约束：Tensor 的地址空间必须连续。
        - **block_ids** (list) - 要发布的块ID列表，例如 [0, 3, 5] 表示只传输第 0、3、5 个块。注意：该列表作用于每个 Tensor，即每个 Tensor 都会按相同的 block_ids 提取子块。
        - **sub_timeout_ms** (int) - 超时时间，以毫秒为单位，当在指定时间内无法获取完成，则抛出异常。

    返回：
        - **Future** (Future) - 可通过该Future对象查询异步请求执行结果。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未获取成功。