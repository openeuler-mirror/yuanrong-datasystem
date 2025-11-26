yr.datasystem.DsTensorClient
=====================================

.. py:class:: yr.datasystem.DsTensorClient(host, port, device_id, connect_timeout_ms=60000, client_public_key="", client_private_key="", server_public_key="")

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

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`init <yr.datasystem.DsTensorClient.init>`
         - 初始化数据系统客户端。
       * - :doc:`mset_d2h <yr.datasystem.DsTensorClient.mset_d2h>` 
         - 将 device 的数据写入到 host 中。
       * - :doc:`mget_h2d <yr.datasystem.DsTensorClient.mget_h2d>`
         - 从 host 中获取数据并写入 device 中。
       * - :doc:`async_mset_d2h <yr.datasystem.DsTensorClient.async_mset_d2h>`
         - 将 device 的数据写入到 host 中的异步接口。
       * - :doc:`async_mget_h2d <yr.datasystem.DsTensorClient.async_mget_h2d>`
         - 从 host 中获取数据并写入 device 中的异步接口。
       * - :doc:`delete <yr.datasystem.DsTensorClient.delete>`
         - 删除 host 中的 key。
       * - :doc:`dev_send <yr.datasystem.DsTensorClient.dev_send>`
         - 订阅发布到数据系统的异构对象，并接收数据写入 tensors。
       * - :doc:`dev_recv <yr.datasystem.DsTensorClient.dev_recv>`
         - 将 device 上的内存发布为数据系统的异构对象，发布后的异构对象可通过 dev_recv 获取。
       * - :doc:`dev_mset <yr.datasystem.DsTensorClient.dev_mset>`
         - 通过数据系统缓存 Device 上的数据。
       * - :doc:`dev_mget <yr.datasystem.DsTensorClient.dev_mget>`
         - 获取 device 中的数据，并写入到 Tensor 中。
       * - :doc:`dev_local_delete <yr.datasystem.DsTensorClient.dev_local_delete>`
         - 从数据系统删除本节点上此 key 的元数据，不再管理此 key 对应的 device 内存。
       * - :doc:`dev_delete <yr.datasystem.DsTensorClient.dev_delete>`
         -  从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。
       * - :doc:`async_dev_delete <yr.datasystem.DsTensorClient.async_dev_delete>`
         -  从数据系统删除此 key 的元数据的异步接口，删除成功后不再管理此 key 对应的 device 内存。
       * - :doc:`put_page_attn_layerwise_d2d <yr.datasystem.DsTensorClient.put_page_attn_layerwise_d2d>`
         - 将 PagedAttention 的层级 Tensor 发布为数据系统的异构对象。发布后的异构对象可通过 get_page_attn_layerwise_d2d 获取。
       * - :doc:`get_page_attn_layerwise_d2d <yr.datasystem.DsTensorClient.get_page_attn_layerwise_d2d>`
         - 根据 key 获取缓存在数据系统的 PagedAttention 的层级 Tensor。
       * - :doc:`mset_page_attn_blockwise_d2h <yr.datasystem.DsTensorClient.mset_page_attn_blockwise_d2h>`
         - 将 PagedAttention 的层级 Tensor 异步写入 Host 中。
       * - :doc:`mget_page_attn_blockwise_h2d <yr.datasystem.DsTensorClient.mget_page_attn_blockwise_h2d>`
         - 从 Host 中获取 PagedAttention 的层级 Tensor 并写入 Device 中。

.. toctree::
    :maxdepth: 1
    :hidden:

    yr.datasystem.DsTensorClient.init
    yr.datasystem.DsTensorClient.mset_d2h
    yr.datasystem.DsTensorClient.mget_h2d
    yr.datasystem.DsTensorClient.async_mset_d2h
    yr.datasystem.DsTensorClient.async_mget_h2d
    yr.datasystem.DsTensorClient.delete
    yr.datasystem.DsTensorClient.dev_send
    yr.datasystem.DsTensorClient.dev_recv
    yr.datasystem.DsTensorClient.dev_mset
    yr.datasystem.DsTensorClient.dev_mget
    yr.datasystem.DsTensorClient.dev_local_delete
    yr.datasystem.DsTensorClient.dev_delete
    yr.datasystem.DsTensorClient.async_dev_delete
    yr.datasystem.DsTensorClient.put_page_attn_layerwise_d2d
    yr.datasystem.DsTensorClient.get_page_attn_layerwise_d2d
    yr.datasystem.DsTensorClient.mset_page_attn_blockwise_d2h
    yr.datasystem.DsTensorClient.mget_page_attn_blockwise_h2d
