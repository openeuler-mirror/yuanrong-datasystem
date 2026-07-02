yr.datasystem.kv_client.KVClient.mcreate
========================================

.. py:method:: yr.datasystem.kv_client.KVClient.mcreate(keys, vals, write_mode=WriteMode.NONE_L2_CACHE, ttl_second=0)

    创建数据系统共享内存 Buffer ，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。

    参数：
        - **keys** (list) - 键列表。约束：传入的key的数量需要小于2千。
        - **vals** (list) - 值列表。
        - **write_mode** (:class:`yr.datasystem.object_client.WriteMode`) - 控制数据是否写入二级缓存以增强数据可靠性。默认值：``WriteMode.NONE_L2_CACHE``。
        - **ttl_second** (int) - 控制数据的过期时间，超时会自动删除，单位为秒。0表示不会自动过期，需要通过调用 :func:`yr.datasystem.kv_client.KVClient.delete` 接口删除 ``keys`` 才能退出生命周期。默认值：``0``。
    
    返回：
        `StateValueBuffer` 共享内存对象列表。
    
    异常：
        - **RuntimeError** - 所有的键都设置失败。
        - **TypeError** - 输入参数校验失败，存在非法值。