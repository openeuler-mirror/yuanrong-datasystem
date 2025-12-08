datasystem.DsTensorClient.dev_recv
===================================================

.. py:method:: datasystem.DsTensorClient.dev_recv(keys, tensors)

    订阅发布到数据系统的异构对象，并接收数据写入 tensors。数据通过 device to device 通道直接传输。

    dev_send 和 dev_recv 需配套使用。dev_send 和 dev_recv 传入的 Device 内存地址不能归属于同一张 NPU 卡。

    通过 dev_recv 获取数据成功后，数据系统会自动删除此异构对象，不再管理此对象对应的 device 内存。

    在执行 dev_recv 过程中，执行了 dev_send 的进程不能退出，否则 dev_recv 会失败。
    dev_recv单Key的订阅超时时间为20s，多key为60s。

    参数：
        - **keys** (list) - device 的异构对象的 key
        - **tensors** (list) - : mindspore或pytorch的Tensor列表。约束：Tensor的地址空间必须连续。

    返回：
        - **futures** (list) - :class:`datasystem.hetero_client.Future` 用于接收异步执行结果，当 :class:`datasystem.hetero_client.Future.get` 正常返回时，表示获取数据成功。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未获取成功。