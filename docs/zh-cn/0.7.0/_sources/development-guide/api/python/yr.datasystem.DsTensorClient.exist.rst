yr.datasystem.DsTensorClient.exist
==============================================

.. py:method:: yr.datasystem.DsTensorClient.exist(keys)

    检查给定 key 在数据系统中是否存在。

    参数：
        - **keys** (list) - 待查询的 key 列表。约束：传入的key的数量 `<=10000`。

    返回：
        - **exists** (list) - 对应 key 的存在性，返回布尔值列表。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 查询所有 key 是否存在失败时抛出运行时错误。

