ConnectOptions
==========================

.. cpp:enum-class:: datasystem::DataPlacementPolicy : uint8_t

    写入数据放置策略。``PREFERRED_SAME_NODE`` 优先同节点 Worker，
    ``REQUIRED_SAME_NODE`` 只允许同节点 Worker，``PREFERRED_META_OWNER``
    优先 metadata owner。该枚举不改变 Get/MGet 的 metadata owner 查询协议。

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
        初始化阶段的 ``GetSocketPath``、``RegisterClient`` 等控制 RPC 从当前连接尝试预算计算超时时间，
        而不受 ``requestTimeoutMs`` 限制。

    .. cpp:member:: int32_t requestTimeoutMs;

        客户端请求超时时间，单位为毫秒。默认值0，表示与connectTimeoutMs一致，数值约束>=0。

        .. note::

            此值为 client 运行期业务 RPC 的全局超时上限，不限制客户端初始化阶段的控制 RPC。
            Get 类接口的 ``subTimeoutMs`` 参数实际生效值不超过此值，
            实际生效值为 ``min(requestTimeoutMs, subTimeoutMs)``。


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

    .. cpp:member:: DataPlacementPolicy dataPlacementPolicy = DataPlacementPolicy::PREFERRED_SAME_NODE;

        ``enableLocalCache=false`` 时 Set/MSet 使用的数据放置策略。该配置按客户端实例生效，
        默认值为 ``PREFERRED_SAME_NODE``，不受同一进程中其他 ``KVClient`` 的初始化顺序影响。
        该配置不改变 Get/MGet 的现有路由和传输行为。

    .. cpp:member:: bool enableRemoteH2D = false;
        
        如果为 true，开启 RH2D 功能，该功能需要服务端同步开启 ``enable_remote_h2d`` 能力。默认值：false

    .. cpp:member:: bool enableClientDirectPipelineH2D = false;

        是否开启 Client 无本地 Worker 场景的 Pipeline H2D 能力。默认值：``false``。开启后，Client 可复用
        fast transport 内存池直接接收远端 Worker 数据并执行 H2D；如果本地 Worker 可用，仍优先使用原有
        Client-Local-Worker RH2D 路径。

    .. cpp:member:: int32_t clientDirectPipelineH2DThreadNum = 64;

        Client-direct Pipeline H2D 使用的 MLCacheDirect 线程数。默认值：``64``。该参数仅在
        ``enableClientDirectPipelineH2D`` 为 ``true`` 时生效，有效范围为 ``[8, 128]``；配置超出该范围时使用
        默认值 ``64``。

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
