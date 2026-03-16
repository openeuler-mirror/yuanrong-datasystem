datasystem.stream_client.StreamClient
==================================================================

.. py:class:: datasystem.stream_client.StreamClient(host, port, connect_timeout_ms=60000, client_public_key="", client_private_key="", server_public_key="", access_key="", secret_key="", tenant_id="", enable_exclusive_connection=False)

    流缓存客户端。

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
        - **enable_exclusive_connection** (bool) - 实验性质特性，开启可提升client与本地datasystem_worker之间的IPC传输性能。默认值： ``False`` 。

    输出：
        StreamClient

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`init <datasystem.stream_client.StreamClient.init>`
         - 初始化流缓存客户端。
       * - :doc:`create_producer <datasystem.stream_client.StreamClient.create_producer>`
         - 创建用于生产数据的生产者。
       * - :doc:`subscribe <datasystem.stream_client.StreamClient.subscribe>`
         - 创建用于消费数据的消费者。
       * - :doc:`delete_stream <datasystem.stream_client.StreamClient.delete_stream>`
         - 删除数据流。
       * - :doc:`query_global_producer_num <datasystem.stream_client.StreamClient.query_global_producer_num>`
         - 指定流的名称，查询该条流有多少个生产者。
       * - :doc:`query_global_consumer_num <datasystem.stream_client.StreamClient.query_global_consumer_num>`
         - 指定流的名称，查询该条流有多少个消费者。

.. toctree::
    :maxdepth: 1
    :hidden:

    datasystem.stream_client.StreamClient.init
    datasystem.stream_client.StreamClient.create_producer
    datasystem.stream_client.StreamClient.subscribe
    datasystem.stream_client.StreamClient.delete_stream
    datasystem.stream_client.StreamClient.query_global_producer_num
    datasystem.stream_client.StreamClient.query_global_consumer_num