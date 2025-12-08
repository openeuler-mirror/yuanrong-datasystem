datasystem.kv_client.SetParam
===============================

.. py:class:: datasystem.kv_client.SetParam

    :func:`datasystem.hetero_client.HeteroClient.mset_d2h` 以及 :func:`datasystem.hetero_client.HeteroClient.async_mset_d2h` 方法的入参，用于指定随机读的键名称、偏移量以及大小。

    :ivar WriteMode write_mode: key的可靠性配置。
    :ivar int ttl_seconds: key的存活时间, (以秒为单位)。
    :ivar ExistenceOpt existence: key存在时能否设置。
    :ivar CacheType cache_type: key的缓存位置
