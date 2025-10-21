datasystem.kv_client.KVClient.set
=================================

.. py:method:: datasystem.kv_client.KVClient.set(key, val, write_mode=WriteMode.NONE_L2_CACHE, ttl_second=0)

    设置键的值。

    参数：
        - **key** (str) - 字符串类型的键。
        - **val** (Union[memoryview, bytes, bytearray, str]) - 要设置的数据。
        - **write_mode** (WriteMode) - 控制数据是否写入L2缓存以增强数据可靠性。
        - **ttl_second** (int) - 控制数据的过期时间。

    异常：
        - **RuntimeError** - 如果设置键值失败，将抛出运行时错误。
        - **TypeError** - 如果输入参数无效，将抛出类型错误。
