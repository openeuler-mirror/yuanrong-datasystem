datasystem.kv_client.KVClient.expire
=================================

.. py:method:: datasystem.kv_client.KVClient.expire(self, keys, ttl_second)

    为一组键设置过期生命周期，返回函数操作状态及设置失败的键列表。

    参数：
        - **keys** (str) - 待更新生命周期的键列表，最大支持10000个键。
        - **ttl_second** (uint32)

    返回：
        失败的键列表。如果所有给定键未成功获取，将抛出 ``RuntimeError``.

    异常：
        - **RuntimeError** - 如果更新所有键生命周期失败，将抛出运行时错误。
        - **TypeError** - 如果输入参数无效，将抛出类型错误。
