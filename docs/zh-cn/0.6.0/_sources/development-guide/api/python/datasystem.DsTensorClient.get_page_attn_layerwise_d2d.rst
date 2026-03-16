datasystem.DsTensorClient.get_page_attn_layerwise_d2d
=====================================================

.. py:method:: datasystem.DsTensorClient.get_page_attn_layerwise_d2d(keys, layer_tensors, block_ids)

    将 PagedAttention 的层级 Tensor 作为数据系统的异构对象放在设备上。

    put_page_attn_layerwise_d2d 和 get_page_attn_layerwise_d2d 需配套使用。

    put_page_attn_layerwise_d2d 和 get_page_attn_layerwise_d2d 传入的 Device 内存地址不能归属于同一张 NPU 卡。

    通过 get_page_attn_layerwise_d2d 获取数据成功后，数据系统会自动删除此异构对象，不再管理此对象对应的 device 内存。

    在执行 get_page_attn_layerwise_d2d 过程中，执行了 put_page_attn_layerwise_d2d 的进程不能退出，否则 get_page_attn_layerwise_d2d 会失败。

    参数：
        - **keys** (list) - Device 异构对象的键列表。约束：最多允许10,000个键。
        - **layer_tensors** (list) - PyTorch 的 Tensor 列表。约束：Tensor 的地址空间必须连续。
        - **block_ids** (list) - 要发布的块ID列表，例如 [0, 3, 5] 表示只传输第 0、3、5 个块。注意：该列表作用于每个 Tensor，即每个 Tensor 都会按相同的 block_ids 提取子块。

    返回：
        - **futures** (list) - :class:`datasystem.hetero_client.Future` 用于接收异步执行结果，当 :class:`datasystem.hetero_client.Future.get` 正常返回时，表示获取数据成功。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未获取成功。