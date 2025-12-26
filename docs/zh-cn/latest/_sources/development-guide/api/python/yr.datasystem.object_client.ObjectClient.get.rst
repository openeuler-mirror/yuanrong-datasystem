yr.datasystem.object_client.ObjectClient.get
============================================

.. py:method:: yr.datasystem.object_client.ObjectClient.get(object_keys, timeout_ms)

    获取给定列表对象 key 的Buffer。

    参数：
        - **object_keys** (list) - 对象 key 列表。约束：传入的object key的数量不能超过1万。
        - **timeout_ms** (int) - 订阅超时时间，当该值大于 ``0`` 时，如果列表中有未存在的对象 key，则会在超时时间内等待对象创建并返回，如果超时时间到达之后对象仍未被创建，则返回空。默认值： ``0`` 。

    返回：
        buffers(list): 携带对象 key 列表对应值的 :class:`yr.datasystem.object_client.Buffer` 列表。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未获取成功。