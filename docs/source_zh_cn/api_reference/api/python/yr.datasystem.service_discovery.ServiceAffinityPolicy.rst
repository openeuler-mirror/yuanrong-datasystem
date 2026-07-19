yr.datasystem.service_discovery.ServiceAffinityPolicy
=====================================================

.. py:class:: yr.datasystem.service_discovery.ServiceAffinityPolicy

    服务发现选择 Worker 时使用的节点亲和性策略。

    枚举值：
        - **PREFERRED_SAME_NODE** - 优先选择与客户端同节点的 Worker；如果没有同节点 Worker，则随机选择其他可用 Worker。
        - **REQUIRED_SAME_NODE** - 必须选择与客户端同节点的 Worker；如果没有同节点 Worker，则返回错误。
        - **RANDOM** - 在所有可用 Worker 中随机选择。
