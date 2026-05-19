ServiceDiscoveryOptions
==========================

.. cpp:class:: ServiceDiscoveryOptions

    :header-file: #include <datasystem/utils/service_discovery.h>
    :namespace: datasystem

    服务发现配置参数结构体。

    **公共成员**

    .. cpp:member:: std::string etcdAddress

        ETCD 地址。支持单地址（如 ``"127.0.0.1:2379"``）或多地址逗号分隔（如 ``"127.0.0.1:2379,127.0.0.2:2379"``）。

    .. cpp:member:: std::string clusterName

        集群名称。多集群场景下用于隔离不同集群的数据，为空时表示使用默认集群。

    .. cpp:member:: SensitiveValue etcdCa

        ETCD 根证书，用于 TLS 连接。默认值：""。

    .. cpp:member:: SensitiveValue etcdCert

        ETCD 证书链，用于 TLS 连接。默认值：""。

    .. cpp:member:: SensitiveValue etcdKey

        ETCD 私钥，用于 TLS 连接。默认值：""。

    .. cpp:member:: std::string etcdDNSName

        ETCD TLS DNS 名称覆盖。默认值：""。

    .. cpp:member:: std::string username

        ETCD 用户名，用于用户名/密码认证。默认值：""。

    .. cpp:member:: SensitiveValue password

        ETCD 密码，用于用户名/密码认证。默认值：""。

    .. cpp:member:: uint32_t tokenRefreshIntervalSec = 30

        ETCD Token 刷新间隔，单位为秒。默认值：30。

    .. cpp:member:: std::string hostIdEnvName

        本机 hostId 对应的环境变量名称。Worker 与 SDK 通过此环境变量值匹配来判断是否为同一节点。例如 ``"HOST_ID"`` 或 ``"K8S_NODE_NAME"``。为空时亲和性策略降级为 ``ServiceAffinityPolicy::RANDOM`` 行为。默认值：""。

    .. cpp:member:: ServiceAffinityPolicy affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE

        Worker 选择亲和性策略。默认值：``ServiceAffinityPolicy::PREFERRED_SAME_NODE``。
