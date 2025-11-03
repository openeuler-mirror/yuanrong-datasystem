datasystem.object_client.ObjectClient
=====================================

.. py:class:: datasystem.object_client.ObjectClient(host, port, connect_timeout_ms=60000, client_public_key="", client_private_key="", server_public_key="", access_key="", secret_key="", tenant_id="")

    对象缓存客户端。

    参数：
        - **host** (str) - 数据系统Worker的主机IP地址。
        - **port** (int) - 数据系统Worker的主机IP端口号。
        - **connect_timeout_ms** (int) - 客户端连接和请求超时时间，单位为毫秒。默认值： ``60000`` 。
        - **client_public_key** (str) - 用于curve认证的客户端公钥。默认值： ``""`` 。
        - **client_private_key** (str) - 用于curve认证的客户端私钥。默认值： ``""`` 。
        - **server_public_key** (str) - 用于curve认证的服务端公钥。默认值： ``""`` 。
        - **access_key** (str) - AK/SK授权使用的访问密钥。默认值： ``""`` 。
        - **secret_key** (str) - AK/SK授权的密钥。默认值： ``""`` 。
        - **tenant_id** (str) - 租户ID。默认值： ``""`` 。

    输出：
        ObjectClient

.. dscnautosummary::
    :toctree: ObjectClient
    :nosignatures:

    datasystem.object_client.ObjectClient.init
    datasystem.object_client.ObjectClient.create
    datasystem.object_client.ObjectClient.put
    datasystem.object_client.ObjectClient.get
    datasystem.object_client.ObjectClient.g_increase_ref
    datasystem.object_client.ObjectClient.g_decrease_ref
    datasystem.object_client.ObjectClient.query_global_ref_num
    datasystem.object_client.ObjectClient.generate_object_key