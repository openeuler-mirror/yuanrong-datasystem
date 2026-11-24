yr.datasystem.service_discovery.CoordinatorServiceDiscoveryOptions
==================================================================

.. py:class:: yr.datasystem.service_discovery.CoordinatorServiceDiscoveryOptions()

    通过 Coordinator 发现 Worker 时使用的配置。当前仅支持配置一个 Coordinator 地址。

    属性：
        - **service_address** (str) - Coordinator 服务地址。
        - **cluster_name** (str) - 集群名称。默认值： ``""`` 。
        - **host_id_env_name** (str) - 用于读取 host id 的环境变量名称。默认值： ``""`` 。
        - **affinity_policy** (:doc:`ServiceAffinityPolicy <yr.datasystem.service_discovery.ServiceAffinityPolicy>`) - Worker 选择策略。默认值： ``ServiceAffinityPolicy.PREFERRED_SAME_NODE`` 。
