datasystem.kv_client.SetParam
===============================

.. py:class:: datasystem.kv_client.SetParam

    :ivar WriteMode write_mode: key的可靠性配置。
    :ivar int ttl_seconds: key的存活时间, (以秒为单位)。
    :ivar ExistenceOpt existence: key存在时能否设置。
    :ivar CacheType cache_type: key的缓存位置
