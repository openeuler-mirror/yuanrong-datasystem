yr.datasystem.object_client.ObjectClient.generate_object_id
============================================================

.. py:method:: yr.datasystem.object_client.ObjectClient.generate_object_id(prefix='')

    生成唯一的对象键。

    参数：
        - **prefix** (str) - 键前缀。为空时返回随机 UUID 键；非空时原样返回 prefix。默认值： ``""`` 。

    返回：
        唯一键。如果键生成失败，将返回空字符串。
