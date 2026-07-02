yr.datasystem.kv_client.KVClient.mset_buffer
============================================

.. py:method:: yr.datasystem.kv_client.KVClient.mset_buffer(buffers)

    批量将共享内存 Buffer 缓存到数据系统中。  

    参数：
        - **buffers** (list) - 共享内存列表。需要缓存到数据系统的共享内存 `Buffer` 数组。

    异常：
        - **RuntimeError** - 所有的键都设置失败。
        - **TypeError** - 输入参数校验失败，存在非法值。