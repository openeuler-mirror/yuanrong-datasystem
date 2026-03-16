datasystem.DsClient
===================

.. py:class:: datasystem.DsClient(host, port, connect_timeout_ms=60000, client_public_key="", client_private_key="", server_public_key="", access_key="", secret_key="", tenant_id="", enable_cross_node_connection=False, enable_exclusive_connection=False)

    数据系统客户端

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
        - **enable_exclusive_connection** (bool) - 实验性质特性，开启可提升client与本地datasystem_worker之间的IPC传输性能。默认值： ``False`` 。

    输出：
        DsClient

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`init <datasystem.DsClient.init>`
         - 初始化数据系统客户端
       * - :doc:`hetero <datasystem.DsClient.hetero>` 
         - 获取数据系统异构缓存客户端。
       * - :doc:`kv <datasystem.DsClient.kv>`
         - 获取数据系统KV缓存客户端。
       * - :doc:`object <datasystem.DsClient.object>`
         - 获取数据系统对象缓存客户端。

.. toctree::
    :maxdepth: 1
    :hidden:

    datasystem.DsClient.init
    datasystem.DsClient.hetero
    datasystem.DsClient.kv
    datasystem.DsClient.object
