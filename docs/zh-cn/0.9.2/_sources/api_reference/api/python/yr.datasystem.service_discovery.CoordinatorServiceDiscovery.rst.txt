yr.datasystem.service_discovery.CoordinatorServiceDiscovery
===========================================================

.. py:class:: yr.datasystem.service_discovery.CoordinatorServiceDiscovery(options)

    通过 Coordinator 获取可用 Worker。当前仅支持配置一个 Coordinator 地址。

    参数：
        - **options** (:doc:`CoordinatorServiceDiscoveryOptions <yr.datasystem.service_discovery.CoordinatorServiceDiscoveryOptions>`) - Coordinator 服务发现配置。

    输出：
        CoordinatorServiceDiscovery

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - ``init()``
         - 初始化服务发现实例。
       * - ``select_worker()``
         - 按配置的亲和性策略选择一个 Worker，返回 ``(status, worker_ip, worker_port, is_same_node)`` 。
       * - ``select_same_node_worker()``
         - 选择一个同节点 Worker，返回 ``(status, worker_ip, worker_port)`` 。
       * - ``get_all_workers()``
         - 获取所有可用 Worker，返回 ``(status, same_host_addrs, other_addrs)`` 。
       * - ``get_affinity_policy()``
         - 获取当前 Worker 选择策略。
       * - ``has_host_affinity()``
         - 检查当前配置是否具备可生效的节点亲和性。

    示例：

    .. code-block:: python

        from yr.datasystem import (
            CoordinatorServiceDiscovery,
            CoordinatorServiceDiscoveryOptions,
            KVClient,
            ServiceAffinityPolicy,
        )

        options = CoordinatorServiceDiscoveryOptions()
        options.service_address = "127.0.0.1:31511"
        options.cluster_name = "cluster-a"
        options.host_id_env_name = "HOST_ID"
        options.affinity_policy = ServiceAffinityPolicy.PREFERRED_SAME_NODE

        service_discovery = CoordinatorServiceDiscovery(options)
        service_discovery.init()

        client = KVClient(service_discovery=service_discovery, enable_cross_node_connection=True)
        client.init()
