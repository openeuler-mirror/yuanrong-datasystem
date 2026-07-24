CoordinatorServiceDiscoveryOptions
==================================

.. cpp:class:: CoordinatorServiceDiscoveryOptions

    :header-file: #include <datasystem/utils/service_discovery.h>
    :namespace: datasystem

    Coordinator 服务发现配置参数结构体。

    调用方可以通过 ``serviceAddress`` 配置一个固定 Coordinator 地址，也可以通过 ``coordinatorDiscovery``
    注入自定义的 Coordinator 地址发现实现。同时配置时使用 ``coordinatorDiscovery``；两者均未配置时，
    :cpp:func:`CoordinatorServiceDiscovery::Init` 返回参数错误。

    **公共成员**

    .. cpp:member:: std::string serviceAddress

        固定 Coordinator 地址，格式为 ``host:port``，例如 ``"127.0.0.1:31511"``。

    .. cpp:member:: std::string clusterName = ""

        集群名称。多集群场景下用于隔离不同集群的数据；非空时需要与 Worker 的 ``cluster_name``
        启动参数保持一致，否则无法发现该集群内的 Worker。为空时使用默认集群。默认值：``""``。

    .. cpp:member:: std::string hostIdEnvName = ""

        本机 hostId 对应的环境变量名称。Worker 与 SDK 通过该环境变量值判断是否位于同一节点。
        为空或无法取得对应环境变量时，服务发现不能执行同节点选择。默认值：``""``。

    .. cpp:member:: ServiceAffinityPolicy affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE

        Worker 选择亲和性策略。默认值：``ServiceAffinityPolicy::PREFERRED_SAME_NODE``。

    .. cpp:member:: std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery = nullptr

        用户提供的 Coordinator 地址发现实现。当前实现仅在初始化时调用一次
        :cpp:func:`ICoordinatorDiscovery::GetCoordinators`，拒绝空候选列表，并缓存非空列表的首个
        Coordinator 地址；后续 Worker 查询不再刷新该 Discovery。
