ServiceAffinityPolicy
========================

.. cpp:class:: ServiceAffinityPolicy

    :header-file: #include <datasystem/utils/service_discovery.h>
    :namespace: datasystem

    定义 Worker 选择亲和性策略的枚举类。

    =============================================  =========  =============================================================
    定义                                            值         说明
    =============================================  =========  =============================================================
    ``ServiceAffinityPolicy::PREFERRED_SAME_NODE``  0          优先选择同节点 Worker。若同节点无可用 Worker，则从所有 Worker 中随机选择。默认配置。
    ``ServiceAffinityPolicy::REQUIRED_SAME_NODE``   1          强制选择同节点 Worker。若无同节点可用 Worker，返回错误。
    ``ServiceAffinityPolicy::RANDOM``               2          不考虑节点亲和性，从所有可用 Worker 中随机选择。
    =============================================  =========  =============================================================
