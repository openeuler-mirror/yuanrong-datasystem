datasystem.DsTensorClient
=====================================

.. py:class:: datasystem.DsTensorClient(host, port, device_id, connect_timeout_ms=60000, client_public_key="", client_private_key="", server_public_key="")

    异构对象客户端。

    参数：
        - **host** (str) - 数据系统 Worker 的主机 IP 地址。
        - **port** (int) - 数据系统 Worker 的端口号。
        - **device_id** (int) - 客户端所在进程绑定的device id。
        - **connect_timeout_ms** (int) - 客户端连接和请求超时时间，单位为毫秒。默认值： ``60000`` 。
        - **client_public_key** (str) - 用于 curve 认证的客户端公钥。默认值： ``""`` 。
        - **client_private_key** (str) - 用于 curve 认证的客户端私钥。默认值： ``""`` 。
        - **server_public_key** (str) - 用于 curve 认证的服务端公钥。默认值： ``""`` 。

    输出：
        DsTensorClient

.. dscnautosummary::
    :toctree: DsTensorClient
    :nosignatures:

    datasystem.DsTensorClient.init
    datasystem.DsTensorClient.mset_d2h
    datasystem.DsTensorClient.mget_h2d
    datasystem.DsTensorClient.async_mset_d2h
    datasystem.DsTensorClient.async_mget_h2d
    datasystem.DsTensorClient.delete
    datasystem.DsTensorClient.dev_send
    datasystem.DsTensorClient.dev_recv
    datasystem.DsTensorClient.dev_mset
    datasystem.DsTensorClient.dev_mget
    datasystem.DsTensorClient.dev_local_delete
    datasystem.DsTensorClient.dev_delete
    datasystem.DsTensorClient.put_page_attn_layerwise_d2d
    datasystem.DsTensorClient.get_page_attn_layerwise_d2d
    datasystem.DsTensorClient.mset_page_attn_blockwise_d2h
    datasystem.DsTensorClient.mget_page_attn_blockwise_h2d
