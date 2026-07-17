IServiceDiscovery
=================

.. cpp:class:: IServiceDiscovery

    :header-file: #include <datasystem/utils/service_discovery.h>
    :namespace: datasystem

    Worker 服务发现的统一接口。客户端可以通过 :cpp:member:`ConnectOptions::serviceDiscovery` 注入该接口的实现。
    调用方应在创建客户端前显式调用 :cpp:func:`IServiceDiscovery::Init` 并检查返回状态；客户端初始化流程
    不保证代替调用方调用该函数。

    openYuanrong datasystem 提供基于 ETCD 的 :cpp:class:`ServiceDiscovery` 和基于 Coordinator 的
    :cpp:class:`CoordinatorServiceDiscovery`。

    **公共函数**

    .. cpp:function:: virtual Status Init() = 0

        初始化服务发现。

        返回：
            初始化结果状态码。

    .. cpp:function:: virtual Status SelectWorker(std::string &workerIp, int &workerPort, bool *isSameNode = nullptr, bool *isNoAvailableWorker = nullptr) = 0

        按服务发现实现配置的亲和性策略选择一个可用 Worker。

        参数：
            - **workerIp** - 传出参数。选中的 Worker IP 地址。
            - **workerPort** - 传出参数。选中的 Worker 端口号。
            - **isSameNode** - 传出参数。可选；非空时表示选中的 Worker 是否与客户端位于同一节点。
            - **isNoAvailableWorker** - 传出参数。可选。内置实现使用 ``RANDOM``，或使用 ``PREFERRED_SAME_NODE`` 且没有同节点 Worker 时，如果全部 Worker 均不可用，则设置为 ``true``。``REQUIRED_SAME_NODE`` 下没有同节点 Worker 时返回 ``K_TRY_AGAIN``，该参数保持 ``false``。

        返回：
            操作结果状态码。

    .. cpp:function:: virtual Status SelectSameNodeWorker(std::string &workerIp, int &workerPort) = 0

        只选择与客户端位于同一节点的 Worker。

        参数：
            - **workerIp** - 传出参数。选中的 Worker IP 地址。
            - **workerPort** - 传出参数。选中的 Worker 端口号。

        返回：
            操作结果状态码。

    .. cpp:function:: virtual Status GetAllWorkers(std::vector<std::string> &sameHostAddrs, std::vector<std::string> &otherAddrs) = 0

        获取服务发现实现可见的所有就绪 Worker。内置实现根据亲和性策略组织输出：

        - ``PREFERRED_SAME_NODE``：``sameHostAddrs`` 包含同节点 Worker，``otherAddrs`` 包含其他 Worker。
        - ``REQUIRED_SAME_NODE``：只返回同节点 Worker，``otherAddrs`` 为空。
        - ``RANDOM``：全部 Worker 放入 ``otherAddrs``，``sameHostAddrs`` 为空。

        参数：
            - **sameHostAddrs** - 传出参数。同节点分组中的 Worker 地址列表，地址格式为 ``host:port``。
            - **otherAddrs** - 传出参数。其他分组中的 Worker 地址列表，地址格式为 ``host:port``。

        返回：
            操作结果状态码。

    .. cpp:function:: virtual ServiceAffinityPolicy GetAffinityPolicy() const = 0

        获取当前 Worker 选择亲和性策略。

        返回：
            当前亲和性策略，参见 :cpp:class:`ServiceAffinityPolicy`。

    .. cpp:function:: virtual bool HasHostAffinity() const = 0

        检查当前服务发现实现是否能够按节点亲和性选择 Worker。

        返回：
            ``true`` 表示可以按同节点条件选择 Worker，否则返回 ``false``。
