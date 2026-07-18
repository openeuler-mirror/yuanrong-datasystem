yr.datasystem.kv_client.KVClient
================================

.. py:class:: yr.datasystem.kv_client.KVClient(host="", port=0, connect_timeout_ms=9000, token="", client_public_key="", client_private_key="", server_public_key="", access_key="", secret_key="", tenant_id="", enable_cross_node_connection=False, req_timeout_ms=0, fast_transport_mem_size=268435456, service_discovery=None)

    KV缓存客户端。

    参数：
        - **host** (str) - 数据系统Worker的主机IP地址。使用 ``service_discovery`` 时可不填写。
        - **port** (int) - 数据系统Worker的服务端口号。使用 ``service_discovery`` 时可不填写。
        - **connect_timeout_ms** (int) - 客户端连接和请求超时时间，单位为毫秒。默认值： ``9000`` 。
        - **token** (str) - 认证使用的 Token。默认值： ``""`` 。
        - **client_public_key** (str) - 用于curve认证的客户端公钥。默认值： ``""`` 。
        - **client_private_key** (str) - 用于curve认证的客户端私钥。默认值： ``""`` 。
        - **server_public_key** (str) - 用于curve认证的服务端公钥。默认值： ``""`` 。
        - **access_key** (str) - AK/SK授权使用的访问密钥。默认值： ``""`` 。
        - **secret_key** (str) - AK/SK授权的密钥。默认值： ``""`` 。
        - **tenant_id** (str) - 租户ID。默认值： ``""`` 。
        - **enable_cross_node_connection** (bool) - 如果为 ``True`` ，允许客户端在与当前数据系统Worker连接异常时自动切换到备用节点。与 ``service_discovery`` 配合使用时，已有 KVClient 实例要在当前 Worker 故障后切换到其他已发现 Worker，需要开启该参数。默认值： ``False`` 。
        - **req_timeout_ms** (int) - 请求超时时间，单位为毫秒。当 req_timeout_ms<=0 时，req_timeout_ms 与 connect_timeout_ms 相同。默认值： ``0`` 。
        - **fast_transport_mem_size** (int) - 客户端 fast transport 内存池大小，单位为字节。默认值： ``268435456`` 。
        - **service_discovery** (:doc:`yr.datasystem.service_discovery.ServiceDiscovery <yr.datasystem.service_discovery.ServiceDiscovery>` | :doc:`yr.datasystem.service_discovery.CoordinatorServiceDiscovery <yr.datasystem.service_discovery.CoordinatorServiceDiscovery>`) - 服务发现实例。提供后，原生 KVClient 通过该实例发现可用 Worker，传入的 ``host`` 和 ``port`` 会被忽略。创建 KVClient 前需先调用 ``service_discovery.init()``。默认值： ``None`` 。

    输出：
        KVClient

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`init <yr.datasystem.kv_client.KVClient.init>`
         - 初始化KV缓存客户端以连接到 Worker 。
       * - :doc:`set <yr.datasystem.kv_client.KVClient.set>`
         - 设置键的值。
       * - :doc:`set_value <yr.datasystem.kv_client.KVClient.set_value>`
         - 设置键的值，键由系统生成并返回。
       * - :doc:`mset <yr.datasystem.kv_client.KVClient.mset>`
         - 批量设置键值对。
       * - :doc:`mcreate <yr.datasystem.kv_client.KVClient.mcreate>`
         - 创建数据系统共享内存 Buffer ，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。
       * - :doc:`mset_buffer <yr.datasystem.kv_client.KVClient.mset_buffer>`
         - 批量将共享内存 Buffer 缓存到数据系统中。     
       * - :doc:`mget_buffer <yr.datasystem.kv_client.KVClient.mget_buffer>`
         - 获取键对应的只读共享内存 Buffer 。
       * - :doc:`msettx <yr.datasystem.kv_client.KVClient.msettx>`
         - 已废弃 API，仅为兼容保留，调用固定抛出 ``RuntimeError``。
       * - :doc:`get_read_only_buffers <yr.datasystem.kv_client.KVClient.get_read_only_buffers>`
         - 以只读缓冲区形式获取给定键的值。
       * - :doc:`get <yr.datasystem.kv_client.KVClient.get>`
         - 获取所有给定键的值。
       * - :doc:`read <yr.datasystem.kv_client.KVClient.read>`
         - 读取指定偏移量的数据。
       * - :doc:`delete <yr.datasystem.kv_client.KVClient.delete>`
         - 删除指定的键值对。
       * - :doc:`generate_key <yr.datasystem.kv_client.KVClient.generate_key>`
         - 生成唯一的键。
       * - :doc:`exist <yr.datasystem.kv_client.KVClient.exist>`
         - 查看 key 在数据系统中是否存在。
       * - :doc:`expire <yr.datasystem.kv_client.KVClient.expire>`
         - 为一组键设置过期生命周期，返回函数操作状态及设置失败的键列表。
       * - :doc:`health_check <yr.datasystem.kv_client.KVClient.health_check>`
         - 查看连接的 Worker 的健康状态。
       
.. toctree::
    :maxdepth: 1
    :hidden:

    yr.datasystem.kv_client.KVClient.init
    yr.datasystem.kv_client.KVClient.set
    yr.datasystem.kv_client.KVClient.set_value
    yr.datasystem.kv_client.KVClient.mset
    yr.datasystem.kv_client.KVClient.mcreate
    yr.datasystem.kv_client.KVClient.mset_buffer
    yr.datasystem.kv_client.KVClient.mget_buffer
    yr.datasystem.kv_client.KVClient.msettx
    yr.datasystem.kv_client.KVClient.get_read_only_buffers
    yr.datasystem.kv_client.KVClient.get
    yr.datasystem.kv_client.KVClient.read
    yr.datasystem.kv_client.KVClient.delete
    yr.datasystem.kv_client.KVClient.generate_key
    yr.datasystem.kv_client.KVClient.exist
    yr.datasystem.kv_client.KVClient.expire
    yr.datasystem.kv_client.KVClient.health_check
