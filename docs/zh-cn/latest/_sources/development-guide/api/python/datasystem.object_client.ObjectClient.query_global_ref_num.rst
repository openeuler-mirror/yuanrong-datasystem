datasystem.object_client.ObjectClient.query_global_ref_num
==========================================================

.. py:method:: datasystem.object_client.ObjectClient.query_global_ref_num(object_key)

    查询对象全局引用计数。

    参数：
        - **object_key** (str) - 对象 key。
    
    返回：
        ref_num，对象全局引用计数

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 查询对象全局引用计数失败