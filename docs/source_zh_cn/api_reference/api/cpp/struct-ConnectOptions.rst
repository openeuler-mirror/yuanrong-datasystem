ConnectOptions
==========================

.. cpp:class:: ConnectOptions

    :header-file: #include <datasystem/utils/connection.h>
    :namespace: datasystem

    用于配置对象客户端的初始化参数的结构体。

    **公共成员**

    .. cpp:member:: std::string host

        数据系统 Worker 的主机 IP 地址。

    .. cpp:member:: int32_t port

        数据系统 Worker 的主机 IP 端口号。

    .. cpp:member:: int32_t connectTimeoutMs = 9 * 1000;

        客户端连接超时时间，单位为毫秒。默认值：9'000, 数值约束>=500。

    .. cpp:member:: int32_t requestTimeoutMs;

        客户端请求超时时间，单位为毫秒。默认值0，表示与connectTimeoutMs一致，数值约束>=0。

    .. cpp:member:: std::string clientPublicKey = "";

        用于 curve 认证的客户端公钥。默认值：""。

    .. cpp:member:: SensitiveValue clientPrivateKey = "";

        用于 curve 认证的客户端私钥。默认值：""

    .. cpp:member:: std::string serverPublicKey = "";

        用于 curve 认证的服务端公钥。默认值：""

    .. cpp:member:: std::string accessKey = "";

        AK/SK 授权使用的访问密钥。默认值：""

    .. cpp:member:: SensitiveValue secretKey = "";

        AK/SK 授权的密钥。默认值：""

    .. cpp:member:: std::string tenantId = "";

        租户 ID。默认值：""

    .. cpp:member:: bool enableCrossNodeConnection = false;

        如果为 true，允许客户端在与当前数据系统Worker 连接异常时自动切换到备用节点。默认值：false

    .. cpp:member:: bool enableRemoteH2D = false;
        
        如果为 true，开启 RH2D 功能，该功能需要服务端同步开启 ``enable_remote_h2d`` 能力。默认值：false

    .. cpp:member:: std::shared_ptr<IServiceDiscovery> serviceDiscovery = nullptr;

        Worker 服务发现实现。可以配置基于 ETCD 的 :cpp:class:`ServiceDiscovery`、基于 Coordinator 的
        :cpp:class:`CoordinatorServiceDiscovery`，或用户自定义的 :cpp:class:`IServiceDiscovery` 实现。
        配置该成员前，调用方应显式调用服务发现对象的 ``Init()`` 并检查返回状态；客户端初始化流程不保证
        代替调用方调用 ``Init()``。未配置时，客户端使用 ``host`` 和 ``port`` 指定的 Worker 地址。
        默认值：``nullptr``。

    .. cpp:member:: uint64_t fastTransportMemSize = 256 * 1024 * 1024;

        client 进程级 fast transport（URMA）传输内存池大小，单位为字节。默认值：256MB，取值范围为 ``(0, 2GB]``。同一进程内各 client 需保持一致，由首个启用 fast transport 的 client 生效。

    **公共函数**
 
    .. cpp:function:: void SetAkSkAuth(const std::string &accessKey, const SensitiveValue &secretKey, const std::string &tenantId)
 
       设置 AK/SK 用于后续请求访问。

       参数：
            - **accessKey** - 设置授权使用的访问密钥。
            - **accessKey** - 设置AK/SK 授权的密钥。
            - **tenantId** - 租户ID。
