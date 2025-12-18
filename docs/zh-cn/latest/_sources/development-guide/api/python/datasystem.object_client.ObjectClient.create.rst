datasystem.object_client.ObjectClient.create
============================================

.. py:method:: datasystem.object_client.ObjectClient.create(object_key, size, param=None)

    创建对象buffer。

    参数：
        - **object_key** (int) - 对象 key。
        - **size** (int) - 对象大小，单位为byte。
        - **write_mode** (:class:`datasystem.object_client.WriteMode`), 对象可靠性配置，详细说明可参考： :class:`datasystem.object_client.WriteMode` 。默认值： ``WriteMode.NONE_L2_CACHE`` 。
        - **param** (dict)，- 默认值 为None时，param = {consistency_type: ConsistencyType.PRAM}, 对象一致性配置，详细说明可参考： :class:`datasystem.object_client.ConsistencyType` 。

    返回：
        buffer，对象(:class:`datasystem.object_client.Buffer`)。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - `create` 请求失败时。