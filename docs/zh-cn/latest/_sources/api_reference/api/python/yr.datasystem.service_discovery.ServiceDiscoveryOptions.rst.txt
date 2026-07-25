yr.datasystem.service_discovery.ServiceDiscoveryOptions
=======================================================

.. py:class:: yr.datasystem.service_discovery.ServiceDiscoveryOptions()

    通过 ETCD 发现 Worker 时使用的配置。

    属性：
        - **etcd_address** (str) - ETCD 服务地址。
        - **cluster_name** (str) - 集群名称。默认值： ``""`` 。
        - **etcd_ca** (str) - ETCD TLS CA 证书路径。默认值： ``""`` 。
        - **etcd_cert** (str) - ETCD TLS 客户端证书路径。默认值： ``""`` 。
        - **etcd_key** (str) - ETCD TLS 客户端私钥路径。默认值： ``""`` 。
        - **etcd_dns_name** (str) - ETCD TLS DNS 名称。默认值： ``""`` 。
        - **username** (str) - ETCD 鉴权用户名。默认值： ``""`` 。
        - **password** (str) - ETCD 鉴权密码。默认值： ``""`` 。
        - **token_refresh_interval_sec** (int) - ETCD 鉴权 Token 刷新间隔，单位为秒。默认值： ``30`` 。
        - **host_id_env_name** (str) - 用于读取 host id 的环境变量名称。默认值： ``""`` 。
        - **affinity_policy** (:doc:`ServiceAffinityPolicy <yr.datasystem.service_discovery.ServiceAffinityPolicy>`) - Worker 选择策略。默认值： ``ServiceAffinityPolicy.PREFERRED_SAME_NODE`` 。
