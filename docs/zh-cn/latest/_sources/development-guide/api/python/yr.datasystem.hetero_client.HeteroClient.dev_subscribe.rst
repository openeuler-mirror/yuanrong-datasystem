yr.datasystem.hetero_client.HeteroClient.dev_subscribe
======================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.dev_subscribe(keys, data_blob_list)

    订阅发布到数据系统的异构对象，并接收数据写入 data_blob_list。数据通过 device to device 通道直接传输。
    
    通过 dev_subscribe 获取数据成功后，数据系统会自动删除此异构对象，不再管理此对象对应的 device 内存。在执行 dev_subscribe 过程中，执行了 dev_publish 的进程不能退出，否则 dev_subscribe 会失败。

    dev_subscribe单Key的订阅超时时间为20s，多key为60s。

    .. note::
        dev_publish 和 dev_subscribe 需配套使用。

    参数：
        - **keys** (list) - device 的异构对象的 key
        - **data_blob_list** (list) - :class:`yr.datasystem.hetero_client.DeviceBlobList` 列表。

    返回：
        - **futures** (list) - :class:`yr.datasystem.hetero_client.Future` 用于接收异步执行结果，当 :class:`yr.datasystem.hetero_client.Future.get` 正常返回时，表示获取数据成功。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未获取成功。