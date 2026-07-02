SubscriptionType
========================================

.. cpp:class:: SubscriptionType

    :header-file: #include <datasystem/stream/stream_config.h>
    :namespace: datasystem

    `SubscriptionType` 流缓存消费者订阅模式。

    目前，目前仅支持 `SubscriptionType::STREAM` 

    =====================================  ==================================================================
    定义                                   说明
    =====================================  ==================================================================
    ``SubscriptionType::STREAM``           流模式，支持
    ``SubscriptionType::ROUND_ROBIN``      轮询模式， 不支持
    ``SubscriptionType::KEY_PARTITIONS``   按键分区模式， 不支持
    ``SubscriptionType::UNKNOWN``          未知模式， 不支持
    =====================================  ==================================================================
