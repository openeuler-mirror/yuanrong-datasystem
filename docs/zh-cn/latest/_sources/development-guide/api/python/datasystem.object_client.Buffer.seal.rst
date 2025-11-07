datasystem.object_client.Buffer.seal
====================================

.. py:method:: datasystem.object_client.Buffer.seal(nested_object_keys=None)

    将Buffer中的数据发布到数据系统中，发布成功之后Buffer所对应的对象的值将无法再被修改。

    参数：
        - **nested_object_keys** (list) - 依赖的嵌套对象 key 列表。默认值： ``None`` 。

    异常：
        - **TypeError** - 输入参数为非法参数。
        - **RuntimeError** - 数据拷贝失败。
