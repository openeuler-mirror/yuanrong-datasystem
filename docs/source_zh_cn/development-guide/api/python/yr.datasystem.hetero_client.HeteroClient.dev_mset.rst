yr.datasystem.hetero_client.HeteroClient.dev_mset
=================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.dev_mset(keys, data_blob_list)

    通过数据系统缓存 Device 上的数据，将 data_blob_list 对应的 key 的元数据写入数据系统，可供其他 client 访问。

    dev_mset 和 dev_mget 需配套使用。dev_mset 和 dev_mget 传入的 Device 内存地址不能归属于同一张 NPU 卡。

    dev_mget 后不会自动删除异构对象，如对象不再使用，可调用 dev_local_delete 或 dev_delete 删除。

    参数：
        - **keys** (list) - device 的异构对象的 key。约束：传入的key的数量不能超过1万。
        - **data_blob_list** (list) - :class:`yr.datasystem.hetero_client.DeviceBlobList` 列表。

    返回值：
        - **failed_keys** (list) - 用于描述 set 失败的 key。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未执行成功。