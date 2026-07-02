yr.datasystem.kv_client.KVClient.msettx
=======================================

.. py:method:: yr.datasystem.kv_client.KVClient.msettx(keys, vals, write_mode=WriteMode.NONE_L2_CACHE,
    ttl_second=0, existence_opt=ExistenceOpt.NONE)

    已废弃 API。该接口仅为保持 SDK API 兼容而保留，调用时固定返回包含
    ``deprecated API`` 字样的 ``RuntimeError``。

    参数：
        - **keys** (list) - 键列表。
        - **vals** (list) - 值列表。
        - **write_mode** (:class:`yr.datasystem.object_client.WriteMode`) - 控制数据是否写入二级缓存以增强数据可靠性。
          默认值：``WriteMode.NONE_L2_CACHE``。
        - **ttl_second** (int) - 控制数据的过期时间，单位为秒。默认值：``0``。
        - **existence_opt** (:class:`yr.datasystem.object_client.ExistenceOpt`) - 控制 key 存在时能否设置。

    异常：
        - **RuntimeError** - 固定抛出，表示该接口为废弃 API。
        - **TypeError** - 输入参数校验失败，存在非法值。
