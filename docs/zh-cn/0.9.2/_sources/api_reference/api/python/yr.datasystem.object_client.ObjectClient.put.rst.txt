yr.datasystem.object_client.ObjectClient.put
============================================

.. py:method:: yr.datasystem.object_client.ObjectClient.put(object_key, value, write_mode=WriteMode.NONE_L2_CACHE, consistency_type=ConsistencyType.PRAM, nested_object_keys=None)

    将对象缓存到数据系统中。

    参数：
        - **object_key** (str) - 对象 key。
        - **value** (Union[memoryview, bytes, bytearray]) - 对象值 。
        - **write_mode** (:class:`yr.datasystem.object_client.WriteMode`), 对象可靠性配置，有4种等级：`WriteMode.NONE_L2_CACHE`，`WriteMode.WRITE_THROUGH_L2_CACHE`，`WriteMode.WRITE_BACK_L2_CACHE` 以及 `WriteMode.NONE_L2_CACHE_EVICT`。
        - **consistency_type** (:class:`yr.datasystem.object_client.ConsistencyType`)，对象一致性配置，有2种：`ConsistencyType.PRAM` 以及 `ConsistencyType.CAUSAL`。
        - **nested_object_keys** (list) - 嵌套对象 key 列表。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 对象写入数据系统失败。