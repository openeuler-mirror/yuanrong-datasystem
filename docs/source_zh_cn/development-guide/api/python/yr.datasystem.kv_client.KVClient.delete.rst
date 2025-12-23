yr.datasystem.kv_client.KVClient.delete
=======================================

.. py:method:: yr.datasystem.kv_client.KVClient.delete(keys=None)

    删除所有给定键的值。

    参数：
        - **keys** (list) - 字符串类型的键列表。约束：传入的key的数量不能超过1万。

    返回：
        失败的键列表。

    异常：
        - **RuntimeError** - 如果删除所有键的值失败，将抛出运行时错误。
        - **TypeError** - 如果输入参数无效，将抛出类型错误。
