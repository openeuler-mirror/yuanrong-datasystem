ServiceDiscovery
====================

.. cpp:class:: ServiceDiscovery

    :header-file: #include <datasystem/utils/service_discovery.h>
    :namespace: datasystem

    当客户端不确定具体连接哪个 Worker 时，ServiceDiscovery 通过 ETCD 自动发现集群中可用的 Worker 地址，并根据亲和性策略选择合适的 Worker。

    **公共函数**

    .. cpp:function:: explicit ServiceDiscovery(const ServiceDiscoveryOptions &opts)

        构造 ServiceDiscovery 实例。如果连接的 ETCD 启用了证书认证，需要通过 ``opts`` 指定 ``etcdCa``、``etcdCert``、``etcdKey`` 和 ``etcdDNSName``。

        参数：
            - **opts** - 服务发现配置参数，参见 :cpp:class:`ServiceDiscoveryOptions`。

    .. cpp:function:: Status Init()

        连接 ETCD 并完成服务发现初始化。包括：
        验证 ETCD 地址合法性；建立 ETCD 连接并进行身份认证；读取宿主环境变量获取本机 hostId；创建集群表。

        返回：
            初始化结果状态码。

    .. cpp:function:: Status SelectWorker(std::string &workerIp, int &workerPort, bool *isSameNode = nullptr)

        根据亲和性策略选择一个 Worker 地址。

        - ``ServiceAffinityPolicy::REQUIRED_SAME_NODE``：从同节点 Worker 中随机选择，若无同节点 Worker 则返回错误。
        - ``ServiceAffinityPolicy::PREFERRED_SAME_NODE``：优先选择同节点 Worker，无同节点 Worker 时从全部 Worker 中随机选择。
        - ``ServiceAffinityPolicy::RANDOM``：从所有可用 Worker 中随机选择。

        参数：
            - **workerIp** - 传出参数。选择的 Worker IP 地址。
            - **workerPort** - 传出参数。选择的 Worker 端口号。
            - **isSameNode** - 传出参数。可选，当非空时设置为 ``true`` 表示选择的 Worker 位于同节点。

        返回：
            操作结果状态码。

    .. cpp:function:: Status SelectSameNodeWorker(std::string &workerIp, int &workerPort)

        只从同节点 Worker 中选择一个可用地址。调用前需确保 ``HasHostAffinity()`` 返回 ``true``。

        参数：
            - **workerIp** - 传出参数。选择的同节点 Worker IP 地址。
            - **workerPort** - 传出参数。选择的同节点 Worker 端口号。

        返回：
            操作结果状态码。

    .. cpp:function:: Status GetAllWorkers(std::vector<std::string> &sameHostAddrs, std::vector<std::string> &otherAddrs)

        获取所有就绪 Worker 地址，按 hostId 是否匹配本机进行分组。

        - ``ServiceAffinityPolicy::REQUIRED_SAME_NODE``：``sameHostAddrs`` 包含同节点 Worker，``otherAddrs`` 为空。
        - ``ServiceAffinityPolicy::RANDOM``：所有 Worker 归入 ``otherAddrs``，``sameHostAddrs`` 为空。
        - ``ServiceAffinityPolicy::PREFERRED_SAME_NODE``：保持原始分组，调用方可优先处理同节点地址。

        参数：
            - **sameHostAddrs** - 传出参数。hostId 与本机匹配的 Worker 地址列表。
            - **otherAddrs** - 传出参数。其余 Worker 地址列表。

        返回：
            操作结果状态码。

    .. cpp:function:: ServiceAffinityPolicy GetAffinityPolicy() const

        获取当前配置的亲和性策略。

        返回：
            当前亲和性策略，参见 :cpp:class:`ServiceAffinityPolicy`。

    .. cpp:function:: bool HasHostAffinity() const

        检查是否实际启用了节点亲和性。当策略为 ``ServiceAffinityPolicy::PREFERRED_SAME_NODE`` 或 ``ServiceAffinityPolicy::REQUIRED_SAME_NODE`` 且 hostId 已成功解析时返回 ``true``。

        返回：
            ``true`` 表示客户端可以正常使用同节点 Worker 选择能力。
