CreateParam
==========================

.. cpp:class:: CreateParam

    :header-file: #include <datasystem/object_client.h>
    :namespace: datasystem

    用于配置对象客户端的初始化参数的结构体。

    **公共成员**

    .. cpp:member:: ConsistencyType consistencyType = ConsistencyType::PRAM

        设置对象的一致性配置, 默认PRAM一致性。

    .. cpp:member:: CacheType cacheType = CacheType::MEMORY

        配置缓存介质，默认为内存缓存介质。