SubscriptionConfig
==========================

.. cpp:class:: SubscriptionConfig

    :header-file: #include <datasystem/stream/stream_config.h>
    :namespace: datasystem

    消费者的配置信息。

    **公共函数**

    .. cpp:function:: SubscriptionConfig(std::string subName, const SubscriptionType subType)
 
       构造用于创建消费者的配置信息。

       参数：
            - **subName** - 订阅名称。
            - **SubscriptionType** - 订阅类型，目前仅支持SubscriptionType::STREAM。表示生产者广播，订阅同一条流的消费者可接收相同的数据。
 
       返回：
           返回用于创建消费者的配置信息。

    .. cpp:function:: SubscriptionConfig(std::string subName, const SubscriptionType subType, uint32_t cacheMax, uint16_t cachePrefetchPercent)
 
       构造用于创建消费者的配置信息。

       参数：
            - **subName** - 订阅名称。
            - **SubscriptionType** - 订阅类型，目前仅支持SubscriptionType::STREAM。表示生产者广播，订阅同一条流的消费者可接收相同的数据。
            - **cacheMax** - 消费者本地缓存数据容量，单位是字节。
            - **cachePrefetchPercent** - 消费者预取时可占用的本地缓存容量百分比，取值范围[0, 100]

       返回：
           返回用于创建消费者的配置信息。

    .. cpp:function:: SubscriptionConfig() = default
 
       构造用于创建消费者的配置信息。
 
       返回：
           返回用于创建消费者的配置信息。

    .. cpp:function:: SubscriptionConfig(const SubscriptionConfig &other) = default
 
       拷贝构造函数。

       参数：
            - **other** - 一个指向 `SubscriptionConfig` 对象的引用。
 
       返回：
           返回用于创建消费者的配置信息。

    .. cpp:function:: SubscriptionConfig(SubscriptionConfig &&other) noexcept

        移动构造函数。

        参数：
            - **other** - 一个指向 `SubscriptionConfig` 对象的右值（临时对象或被移动对象）引用。
 
        返回：
            返回用于创建消费者的配置信息。

    .. cpp:function:: SubscriptionConfig &operator=(const SubscriptionConfig &other) = default
 
       重载赋值运算符，返回配置信息。

        参数：
            - **other** - `SubscriptionConfig` 对象的引用。
 
        返回：
            返回用于创建消费者的配置信息。

    .. cpp:function:: bool operator==(const SubscriptionConfig &config) const

        重载 `==` 运算符。

        参数：
            - **other** - `SubscriptionConfig` 对象的引用。

        返回：
            当每一个成员都相等时返回true, 否则返回false。

    .. cpp:function:: bool operator!=(const SubscriptionConfig &config) const
 
        重载 `！=` 运算符。

        参数：
            - **other** - `SubscriptionConfig` 对象的引用。

        返回：
            当每一个成员函数都相等时返回false, 否则返回true。

    **公共成员**
    .. cpp:member:: static constexpr uint32_t SC_CACHE_CAPACITY = 32768

        静态常量 `SC_CACHE_CAPACITY` 的值为 32768

    .. cpp:member:: static constexpr uint16_t SC_CACHE_LWM = 0

        静态常量 `SC_CACHE_LWM` 的值为 0

    .. cpp:member:: std::string subscriptionName

        订阅名称。

    .. cpp:member:: SubscriptionType subscriptionType = SubscriptionType::STREAM

        订阅类型，目前仅支持SubscriptionType::STREAM。表示生产者广播，订阅同一条流的消费者可接收相同的数据。

    .. cpp:member:: uint32_t cacheCapacity = SC_CACHE_CAPACITY

        消费者本地缓存数据容量，单位是字节，取值范围[64, 1048576], 默认值：32768

    .. cpp:member:: uint16_t cachePrefetchLWM = SC_CACHE_LWM

        消费者预取时可占用的本地缓存容量百分比，取值范围[0, 100], 默认值：0