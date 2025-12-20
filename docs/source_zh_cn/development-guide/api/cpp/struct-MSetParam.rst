MSetParam
==========================

.. cpp:class:: MSetParam

    :header-file: #include <datasystem/kv_client.h>
    :namespace: datasystem

    用于配置对象客户端的初始化参数的结构体。

    **公共成员**

    .. cpp:member:: WriteMode writeMode = WriteMode::NONE_L2_CACHE

        设置数据可靠性级别，默认不写入二级缓存。

    .. cpp:member:: uint32_t ttlSecond = 0

        设置Key的存活时间，单位：秒。Key达到存活时间后系统自动将其删除。默认为0，表示不设置存活时间，Key会一直存在直到显式调用Del接口。注意：系统重启后存活时间计时将重新开始。

    .. cpp:member:: CacheType cacheType = CacheType::MEMORY

        配置缓存介质，默认为内存缓存介质。
