yr.datasystem.object_client.ObjectClient.g_increase_ref
=======================================================

.. py:method:: yr.datasystem.object_client.ObjectClient.g_increase_ref(object_keys)

    增加给定列表对象 key 的全局引用计数。

    参数：
        - **object_keys** (int) - 对象 key 列表。约束：传入的object key的数量不能超过1万。

    返回：
        failed_object_keys，增加全局引用计数失败的对象 key 列表。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 增全局引用计数都未成功。