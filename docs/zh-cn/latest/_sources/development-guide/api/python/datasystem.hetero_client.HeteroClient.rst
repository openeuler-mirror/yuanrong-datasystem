datasystem.hetero_client.HeteroClient
=====================================

.. py:class:: datasystem.hetero_client.HeteroClient(host, port, connect_timeout_ms=60000, client_public_key="", client_private_key="", server_public_key="", access_key="", secret_key="", tenant_id="", enable_cross_node_connection=False, enable_exclusive_connection=False)

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
        - **enable_exclusive_connection** (bool) - 实验性质特性，开启可提升client与本地datasystem_worker之间的IPC传输性能。默认值： ``False`` 。

    输出：
        HeteroClient

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`init <datasystem.hetero_client.HeteroClient.init>`
         - 初始化异构对象客户端。
       * - :doc:`mget_h2d <datasystem.hetero_client.HeteroClient.mget_h2d>`
         - 从 host 中获取数据并写入 device 中。
       * - :doc:`mset_d2h <datasystem.hetero_client.HeteroClient.mset_d2h>`
         - 将 device 的数据写入到 host 中。
       * - :doc:`async_mget_h2d <datasystem.hetero_client.HeteroClient.async_mget_h2d>`
         - 从 host 中获取数据并写入 device 中的异步接口。
       * - :doc:`async_mset_d2h <datasystem.hetero_client.HeteroClient.async_mset_d2h>`
         - 将 device 的数据写入到 host 中的异步接口。
       * - :doc:`delete <datasystem.hetero_client.HeteroClient.delete>`
         - 删除 host 中的 key，与 mget_h2d / mset_d2h 配套使用。
       * - :doc:`dev_publish <datasystem.hetero_client.HeteroClient.dev_publish>`
         - 将 device 上的内存发布为数据系统的异构对象。发布后的异构对象可通过 dev_subscribe 获取。
       * - :doc:`dev_subscribe <datasystem.hetero_client.HeteroClient.dev_subscribe>`
         - 订阅发布到数据系统的异构对象，并接收数据写入 data_blob_list。数据通过 device to device 通道直接传输。
       * - :doc:`dev_mset <datasystem.hetero_client.HeteroClient.dev_mset>`
         - 通过数据系统缓存 Device 上的数据。
       * - :doc:`dev_mget <datasystem.hetero_client.HeteroClient.dev_mget>`
         - 获取 device 中的数据。
       * - :doc:`dev_local_delete <datasystem.hetero_client.HeteroClient.dev_local_delete>`
         - 从数据系统删除本节点上此 key 的元数据，不再管理此 key 对应的 device 内存。
       * - :doc:`dev_delete <datasystem.hetero_client.HeteroClient.dev_delete>`
         - 从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。
       * - :doc:`async_dev_delete <datasystem.hetero_client.HeteroClient.async_dev_delete>`
         - 从数据系统删除此 key 的元数据的异步接口，删除成功后不再管理此 key 对应的 device 内存。
       * - :doc:`generate_key <datasystem.hetero_client.HeteroClient.generate_key>`
         - 生成一个带数据系统 Worker UUID 的 key。
       * - :doc:`get_meta_info <datasystem.hetero_client.HeteroClient.get_meta_info>`
         - 获取keys 对应的元数据信息。
       * - :doc:`exist <datasystem.hetero_client.HeteroClient.exist>`
         - 检查给定的键在数据系统中是否存在。

.. toctree::
    :maxdepth: 1
    :hidden:

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
    datasystem.hetero_client.HeteroClient.async_dev_delete
    datasystem.hetero_client.HeteroClient.generate_key
    datasystem.hetero_client.HeteroClient.get_meta_info
    datasystem.hetero_client.HeteroClient.exist
