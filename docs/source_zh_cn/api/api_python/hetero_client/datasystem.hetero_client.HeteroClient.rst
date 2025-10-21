datasystem.hetero_client.HeteroClient
=====================================

.. py:class:: datasystem.hetero_client.HeteroClient(host, port, connect_timeout_ms=60000, client_public_key="", client_private_key="", server_public_key="", access_key="", secret_key="", tenant_id="", enable_cross_node_connection=False)

    异构对象客户端。

    参数：
        - **host** (str) - 数据系统 Worker 的主机 IP 地址。
        - **port** (int) - 数据系统 Worker 的端口号。
        - **connect_timeout_ms** (int) - 客户端连接和请求超时时间，单位为毫秒。默认值： ``60000`` 。
        - **client_public_key** (str) - 用于 curve 认证的客户端公钥。默认值： ``""`` 。
        - **client_private_key** (str) - 用于 curve 认证的客户端私钥。默认值： ``""`` 。
        - **server_public_key** (str) - 用于 curve 认证的服务端公钥。默认值： ``""`` 。
        - **access_key** (str) - AK/SK 授权使用的访问密钥。默认值： ``""`` 。
        - **secret_key** (str) - AK/SK 授权的密钥。默认值： ``""`` 。
        - **tenant_id** (str) - 租户 ID。默认值： ``""`` 。
        - **enable_cross_node_connection** (bool) - 如果为 ``True`` ，允许客户端在与当前数据系统 Worker 连接异常时自动切换到备用节点。默认值： ``False`` 。

    输出：
        HeteroClient

.. dscnautosummary::
    :toctree: HeteroClient
    :nosignatures:

    datasystem.hetero_client.HeteroClient.init
    datasystem.hetero_client.HeteroClient.mget_h2d
    datasystem.hetero_client.HeteroClient.mset_d2h
    datasystem.hetero_client.HeteroClient.async_mget_h2d
    datasystem.hetero_client.HeteroClient.async_mset_d2h
    datasystem.hetero_client.HeteroClient.delete
    datasystem.hetero_client.HeteroClient.dev_publish
    datasystem.hetero_client.HeteroClient.dev_subscribe
    datasystem.hetero_client.HeteroClient.dev_mset
    datasystem.hetero_client.HeteroClient.dev_mget
    datasystem.hetero_client.HeteroClient.dev_local_delete
    datasystem.hetero_client.HeteroClient.dev_delete
    datasystem.hetero_client.HeteroClient.generate_key
    datasystem.hetero_client.HeteroClient.get_meta_info
