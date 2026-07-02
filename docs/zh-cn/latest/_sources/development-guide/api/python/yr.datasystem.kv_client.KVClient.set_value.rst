yr.datasystem.kv_client.KVClient.set_value
==========================================

.. py:method:: yr.datasystem.kv_client.KVClient.set_value(val, write_mode=WriteMode.NONE_L2_CACHE, ttl_second=0)

    设置键的值，键由系统生成并返回。

    参数：
        - **val** (Union[memoryview, bytes, bytearray, str]) - 要设置的数据。
        - **write_mode** (WriteMode) - 控制数据是否写入二级缓存以增强数据可靠性。
        - **ttl_second** (int) - 控制数据的过期时间，超时会自动删除，单位为秒。0表示不会自动过期，需要通过调用 :func:`yr.datasystem.kv_client.KVClient.delete` 接口删除 ``keys`` 才能退出生命周期。默认值：``0``。

    返回：
        数据的键。如果设置失败，将返回空字符串。

    异常：
        - **TypeError** - 如果输入参数无效，将抛出类型错误。