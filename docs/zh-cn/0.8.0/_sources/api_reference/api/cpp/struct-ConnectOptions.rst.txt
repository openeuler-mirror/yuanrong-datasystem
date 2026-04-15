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

    .. cpp:member:: int32_t connectTimeoutMs = 60 * 1000;

        客户端连接超时时间，单位为毫秒。默认值：60'000, 数值约束>=500。

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

    .. cpp:member:: bool enableExclusiveConnection = false;

        如果为 true，开启IPC性能调优功能，减小客户端与同节点服务端的访问时延。默认值：false

        .. note::
            **连接数限制**：单个 ``worker`` 最多支持 128 个启用 ``enableExclusiveConnection`` 的客户端连接。若并发连接数超过此阈值，系统将抛出请求异常。

    .. cpp:member:: bool enableRemoteH2D = false;
        
        如果为 true，开启 RH2D 功能，该功能需要服务端同步开启 ``enable_remote_h2d`` 能力。默认值：false

    .. cpp:member:: uint64_t fastTransportMemSize = 128 * 1024 * 1024;

        client侧 fast transport（urma）内存池大小，单位为字节。默认值：128MB，取值范围为 ``(0, 2GB]`` 。仅在 fast transport 场景下生效。

    **公共函数**
 
    .. cpp:function:: void SetAkSkAuth(const std::string &accessKey, const SensitiveValue &secretKey, const std::string &tenantId)
 
       设置 AK/SK 用于后续请求访问。

       参数：
            - **accessKey** - 设置授权使用的访问密钥。
            - **accessKey** - 设置AK/SK 授权的密钥。
            - **tenantId** - 租户ID。
