yr.datasystem.kv_client.ReadParam
=================================

.. py:class:: yr.datasystem.kv_client.ReadParam

    :func:`yr.datasystem.kv_client.KVClient.read` 方法的入参，用于指定随机读的键名称、偏移量以及大小。

    :ivar str key: 键的名称。
    :ivar int offset: 读取的偏移。
    :ivar int size: 读取的大小（以字节为单位）。

