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

        客户端连接和请求超时时间，单位为毫秒。默认值：60'000。

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

    **公共函数**
 
    .. cpp:function:: void SetAkSkAuth(const std::string &accessKey, const SensitiveValue &secretKey, const std::string &tenantId)
 
       设置 AK/SK 用于后续请求访问。

       参数：
            - **accessKey** - 设置授权使用的访问密钥。
            - **accessKey** - 设置AK/SK 授权的密钥。
            - **tenantId** - 租户ID。
