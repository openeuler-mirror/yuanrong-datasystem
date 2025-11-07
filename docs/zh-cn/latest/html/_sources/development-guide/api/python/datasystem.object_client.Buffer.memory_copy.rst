datasystem.object_client.Buffer.memory_copy
===========================================

.. py:method:: datasystem.object_client.Buffer.memory_copy(value)

    将 `value` 中的数据拷贝到Buffer中

    参数：
        - **value** (Union[memoryview, bytes, bytearray]) - 需要拷贝到Buffer的数据。

    异常：
        - **TypeError** - `value` 为非法参数。
        - **RuntimeError** - 数据拷贝失败。
