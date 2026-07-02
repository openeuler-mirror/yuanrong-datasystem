yr.datasystem.DsTensorClient.dev_send
=================================================

.. py:method:: yr.datasystem.DsTensorClient.dev_send(keys, tensors)

    将 device 上的内存发布为数据系统的异构对象。发布后的异构对象可通过 dev_recv 获取。

    dev_send 和 dev_recv 需配套使用。dev_send 和 dev_recv 传入的 Device 内存地址不能归属于同一张 NPU 卡。

    通过 dev_recv 获取数据成功后，数据系统会自动删除此异构对象，不再管理此对象对应的 device 内存。

    参数：
        - **keys** (list) - device 的异构对象的 key
        - **tensors** (list) - : mindspore或pytorch的Tensor列表。约束：Tensor的地址空间必须连续。

    返回：
        - **futures** (list) - :class:`yr.datasystem.hetero_client.Future` 用于接收异步执行结果，当 :class:`yr.datasystem.hetero_client.Future.get` 正常返回时，表示对端已获取数据成功。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未执行成功。