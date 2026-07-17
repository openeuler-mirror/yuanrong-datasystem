CoordinatorServiceDiscovery
===========================

.. cpp:class:: CoordinatorServiceDiscovery

    :header-file: #include <datasystem/utils/service_discovery.h>
    :namespace: datasystem

    基于 Coordinator 的 Worker 服务发现实现。该类型从配置的 Coordinator 获取就绪 Worker，
    并根据 :cpp:class:`ServiceAffinityPolicy` 选择客户端连接的 Worker。

    当前实现只支持访问一个 Coordinator 地址。使用自定义 :cpp:class:`ICoordinatorDiscovery` 时，
    其查询结果必须包含且仅包含一个地址。

    **公共函数**

    .. cpp:function:: explicit CoordinatorServiceDiscovery(const CoordinatorServiceDiscoveryOptions &opts)

        构造 Coordinator 服务发现实例。

        参数：
            - **opts** - 服务发现配置参数，参见 :cpp:class:`CoordinatorServiceDiscoveryOptions`。

    .. cpp:function:: Status Init()

        初始化 Coordinator 服务发现。配置中必须提供 ``serviceAddress`` 或 ``coordinatorDiscovery``。
        将该对象设置到 ``ConnectOptions.serviceDiscovery`` 前，应显式调用本函数并检查返回状态。

        返回：
            初始化结果状态码。

    .. cpp:function:: Status SelectWorker(std::string &workerIp, int &workerPort, bool *isSameNode = nullptr, bool *isNoAvailableWorker = nullptr)

        从 Coordinator 返回的就绪 Worker 中，按配置的亲和性策略选择一个 Worker。

        参数：
            - **workerIp** - 传出参数。选中的 Worker IP 地址。
            - **workerPort** - 传出参数。选中的 Worker 端口号。
            - **isSameNode** - 传出参数。可选；非空时表示选中的 Worker 是否与客户端位于同一节点。
            - **isNoAvailableWorker** - 传出参数。可选。使用 ``RANDOM``，或使用 ``PREFERRED_SAME_NODE`` 且没有同节点 Worker 时，如果全部 Worker 均不可用，则设置为 ``true``。``REQUIRED_SAME_NODE`` 下没有同节点 Worker 时返回 ``K_TRY_AGAIN``，该参数保持 ``false``。

        返回：
            操作结果状态码。

    .. cpp:function:: Status SelectSameNodeWorker(std::string &workerIp, int &workerPort)

        只从与客户端位于同一节点的就绪 Worker 中选择一个地址。调用前需确保
        :cpp:func:`CoordinatorServiceDiscovery::HasHostAffinity` 返回 ``true``。

        参数：
            - **workerIp** - 传出参数。选中的 Worker IP 地址。
            - **workerPort** - 传出参数。选中的 Worker 端口号。

        返回：
            操作结果状态码。

    .. cpp:function:: Status GetAllWorkers(std::vector<std::string> &sameHostAddrs, std::vector<std::string> &otherAddrs)

        获取 Coordinator 中记录的全部就绪 Worker，并按当前亲和性策略组织输出：

        - ``PREFERRED_SAME_NODE``：``sameHostAddrs`` 包含同节点 Worker，``otherAddrs`` 包含其他 Worker。
        - ``REQUIRED_SAME_NODE``：只返回同节点 Worker，``otherAddrs`` 为空。
        - ``RANDOM``：全部 Worker 放入 ``otherAddrs``，``sameHostAddrs`` 为空。

        参数：
            - **sameHostAddrs** - 传出参数。与客户端位于同一节点的 Worker 地址。
            - **otherAddrs** - 传出参数。其他 Worker 地址。

        返回：
            操作结果状态码。

    .. cpp:function:: ServiceAffinityPolicy GetAffinityPolicy() const

        获取当前 Worker 选择亲和性策略。

    .. cpp:function:: bool HasHostAffinity() const

        检查当前配置是否能够按节点亲和性选择 Worker。当策略不是
        ``ServiceAffinityPolicy::RANDOM`` 且已获取本机 hostId 时返回 ``true``。

    **使用固定 Coordinator 地址**

    .. code-block:: cpp

        datasystem::CoordinatorServiceDiscoveryOptions options;
        options.serviceAddress = "127.0.0.1:31511";

        auto discovery = std::make_shared<datasystem::CoordinatorServiceDiscovery>(options);
        auto status = discovery->Init();
        if (status.IsError()) {
            return status;
        }
        datasystem::ConnectOptions connectOptions;
        connectOptions.serviceDiscovery = discovery;

    **使用自定义 Coordinator 地址发现**

    .. code-block:: cpp

        class CustomCoordinatorDiscovery : public datasystem::ICoordinatorDiscovery {
        public:
            datasystem::Status GetCoordinators(std::vector<std::string> &serviceList) override
            {
                serviceList = { "127.0.0.1:31511" };
                return datasystem::Status::OK();
            }
        };

        datasystem::CoordinatorServiceDiscoveryOptions options;
        options.coordinatorDiscovery = std::make_shared<CustomCoordinatorDiscovery>();

        auto discovery = std::make_shared<datasystem::CoordinatorServiceDiscovery>(options);
        auto status = discovery->Init();
        if (status.IsError()) {
            return status;
        }
        datasystem::ConnectOptions connectOptions;
        connectOptions.serviceDiscovery = discovery;
