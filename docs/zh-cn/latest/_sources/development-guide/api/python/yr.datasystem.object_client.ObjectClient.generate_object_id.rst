yr.datasystem.object_client.ObjectClient.generate_object_id
============================================================

.. py:method:: yr.datasystem.object_client.ObjectClient.generate_object_id(prefix='')

    生成一个带数据系统Worker UUID的对象 key。

    参数：
        - **prefix** (str) - 前缀。默认值： ``""`` 。

    返回：
        唯一键。如果键生成失败，将返回空字符串。