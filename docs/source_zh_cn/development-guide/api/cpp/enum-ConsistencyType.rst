ConsistencyType
========================================

.. cpp:class:: ConsistencyType

    :header-file: #include <datasystem/object/object_enum.h>
    :namespace: datasystem

    `ConsistencyType` 类定义对象的一致性配置。

    目前，支持以下 `ConsistencyType`：

    ===================================  ==================================================================
    定义                                  说明
    ===================================  ==================================================================
    ``ConsistencyType::PRAM``             Pipeline RAM一致性。默认配置
    ``ConsistencyType::CAUSAL``           因果一致性。
    ===================================  ==================================================================
