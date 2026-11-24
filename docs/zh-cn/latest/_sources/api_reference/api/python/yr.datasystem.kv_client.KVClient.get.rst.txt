yr.datasystem.kv_client.KVClient.get
====================================

.. py:method:: yr.datasystem.kv_client.KVClient.get(keys=None, convert_to_str=False)

    获取所有给定键的值。

    参数：
        - **keys** (list) - 字符串类型的键列表。约束：传入的key的数量 `<=10000`。
        - **convert_to_str** (bool) - 是否将返回值转换为字符串。默认值： ``False`` 。

    返回：
        值列表。如果所有给定键未成功获取，将抛出 ``RuntimeError``，对于未成功的键，其返回值列表中的对应索引为 ``None``。

    异常：
        - **RuntimeError** - 如果获取所有键的值失败，将抛出运行时错误。
        - **TypeError** - 如果输入参数无效，将抛出类型错误。
