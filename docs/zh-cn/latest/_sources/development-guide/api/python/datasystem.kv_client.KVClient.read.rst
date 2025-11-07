datasystem.kv_client.KVClient.read
======================================================

.. py:method:: datasystem.kv_client.KVClient.read(read_params)

    读取指定偏移量的数据。

    参数：
        - **read_params** (list( :class:`datasystem.kv_client.ReadParam` )) - 读取参数列表。 `ReadParam` 详情请查看 :class:`datasystem.kv_client.ReadParam` 。约束：传入的数量不能超过1万。

    返回：
        :class:`datasystem.kv_client.ReadOnlyBuffer` 对象列表。