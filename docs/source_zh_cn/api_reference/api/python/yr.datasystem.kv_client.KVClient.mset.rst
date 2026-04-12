yr.datasystem.kv_client.KVClient.mset
=====================================

.. py:method:: yr.datasystem.kv_client.KVClient.mset(keys, vals, write_mode=WriteMode.NONE_L2_CACHE, ttl_second=0, existence_opt=ExistenceOpt.NONE)

    批量设置键值对。

    参数：
        - **keys** (list) - 键列表。约束：传入的key的数量需 `<= 10000`。
        - **vals** (list) - 值列表。
        - **write_mode** (:class:`yr.datasystem.object_client.WriteMode`) - 控制数据是否写入二级缓存以增强数据可靠性。默认值：``WriteMode.NONE_L2_CACHE``。
        - **ttl_second** (int) - 控制数据的过期时间，超时会自动删除，单位为秒。0表示不会自动过期，需要通过调用 :func:`yr.datasystem.kv_client.KVClient.delete` 接口删除 ``keys`` 才能退出生命周期。默认值：``0``。
        - **existence_opt** (:class:`yr.datasystem.object_client.ExistenceOpt`) - 控制key存在时能否设置。

    异常：
        - **RuntimeError** - 所有的键都设置失败。
        - **TypeError** - 输入参数校验失败，存在非法值。