yr.datasystem.object_client.Buffer.seal
=======================================

.. py:method:: yr.datasystem.object_client.Buffer.seal(nested_object_keys=None)

    将Buffer中的数据发布到数据系统中，语义等同 publish（封存语义已移除）。

    参数：
        - **nested_object_keys** (list) - 依赖的嵌套对象 key 列表。默认值： ``None`` 。

    异常：
        - **TypeError** - 输入参数为非法参数。
        - **RuntimeError** - 数据拷贝失败。
