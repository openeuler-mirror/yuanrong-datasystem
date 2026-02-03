yr.datasystem.DsTensorClient.mset_d2h
==============================================

.. py:method:: yr.datasystem.DsTensorClient.mset_d2h(keys, tensors, set_param)

    将 device 的 Tensors 数据写入到 host 中。

    若 host 的 key 不再使用，可调用 delete 接口删除。

    参数：
        - **keys** (list) - host 的 key 列表。约束：传入的key的数量 `<=10000`。
        - **tensors** (list) - : mindspore或pytorch的Tensor列表。约束：Tensor的地址空间必须连续。
        - **set_param** (SetParam) - :class:`yr.datasystem.kv_client.SetParam`, key的设置参数, 默认为SetParam(),各属性如下:

        .. code-block:: text 
            
            write_mode = WriteMode.NONE_L2_CACHE
            ttl_second = 0
            existence = ExistenceOpt.NONE
            cache_type = CacheType.MEMORY
    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未执行成功。