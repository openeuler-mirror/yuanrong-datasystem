yr.datasystem.DsTensorClient.mset_page_attn_blockwise_d2h
=========================================================

.. py:method:: yr.datasystem.DsTensorClient.mset_page_attn_blockwise_d2h(keys, layer_tensors, block_ids)

    将 PagedAttention 的层级 Tensor 异步写入 host 中。

    若 Device 的 Blob 中存在多个内存地址时，会自动将数据拼接后写入 Host。

    若 host 的 key 不再使用，可调用 delete 接口删除。

    参数：
        - **keys** (list) - Host 的键列表。约束：最多允许10,000个键。
        - **layer_tensors** (list) - PyTorch 的 Tensor 列表。约束：Tensor 的地址空间必须连续。
        - **block_ids** (list) - 要发布的块ID列表，例如 [0, 3, 5] 表示只传输第 0、3、5 个块。注意：该列表作用于每个 Tensor，即每个 Tensor 都会按相同的 block_ids 提取子块。

    返回：
        - **Future** (Future) - 可通过该Future对象查询异步请求执行结果。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未执行成功。