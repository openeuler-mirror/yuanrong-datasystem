yr.datasystem.hetero_client.HeteroClient.mset_d2h
=================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.mset_d2h(keys, data_blob_list, set_param)

    将 device 的数据写入到 host 中。若 device 的 blob 中存在多个内存地址时，会自动将数据拼接后写入 host。

    若 host 的 key 不再使用，可调用 delete 接口删除。
    mset_d2h 成功的数据是immutable的，不要对已存在的key set新值，否则新值不会生效。   

    参数：
        - **keys** (list) - host 的 key 列表。约束：传入的key的数量不能超过1万。
        - **data_blob_list** (list) - :class:`yr.datasystem.hetero_client.DeviceBlobList` 列表。
        - **set_param** (SetParam) - :class:`yr.datasystem.kv_client.SetParam`, key的设置参数, 默认为SetParam(),各属性如下:

        .. code-block:: text 
            
            write_mode = WriteMode.NONE_L2_CACHE
            ttl_second = 0
            existence = ExistenceOpt.NONE
            cache_type = CacheType.MEMEORY
    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未执行成功。