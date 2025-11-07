Python
==============================


.. toctree::
   :glob:
   :hidden:
   :maxdepth: 1

   datasystem.DsClient
   datasystem.DsTensorClient
   datasystem.hetero_client.HeteroClient
   datasystem.hetero_client.Blob
   datasystem.hetero_client.DeviceBlobList
   datasystem.hetero_client.Future
   datasystem.hetero_client.MetaInfo
   datasystem.kv_client.KVClient
   datasystem.kv_client.ReadOnlyBuffer
   datasystem.kv_client.SetParam
   datasystem.kv_client.ReadParam
   datasystem.object_client.ObjectClient
   datasystem.object_client.Buffer
   datasystem.object_client.ConsistencyType
   datasystem.object_client.WriteMode
   datasystem.object_client.CacheType

DsClient：聚合各语义的客户端
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :doc:`datasystem.DsClient.init <datasystem.DsClient.init>`
      - 初始化数据系统客户端
    * - :doc:`datasystem.DsClient.hetero <datasystem.DsClient.hetero>` 
      - 获取数据系统异构缓存客户端。
    * - :doc:`datasystem.DsClient.kv <datasystem.DsClient.kv>`
      - 获取数据系统KV缓存客户端。
    * - :doc:`datasystem.DsClient.object <datasystem.DsClient.object>`
      - 获取数据系统对象缓存客户端。


Tensor接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`datasystem.DsTensorClient.init <datasystem.DsTensorClient.init>`
     - 初始化数据系统客户端。
   * - :doc:`datasystem.DsTensorClient.mset_d2h <datasystem.DsTensorClient.mset_d2h>` 
     - 将 device 的数据写入到 host 中。
   * - :doc:`datasystem.DsTensorClient.mget_h2d <datasystem.DsTensorClient.mget_h2d>`
     - 从 host 中获取数据并写入 device 中。
   * - :doc:`datasystem.DsTensorClient.async_mset_d2h <datasystem.DsTensorClient.async_mset_d2h>`
     - 将 device 的数据写入到 host 中的异步接口。
   * - :doc:`datasystem.DsTensorClient.async_mget_h2d <datasystem.DsTensorClient.async_mget_h2d>`
     - 从 host 中获取数据并写入 device 中的异步接口。
   * - :doc:`datasystem.DsTensorClient.delete <datasystem.DsTensorClient.delete>`
     - 删除 host 中的 key。
   * - :doc:`datasystem.DsTensorClient.dev_send <datasystem.DsTensorClient.dev_send>`
     - 订阅发布到数据系统的异构对象，并接收数据写入 tensors。
   * - :doc:`datasystem.DsTensorClient.dev_recv <datasystem.DsTensorClient.dev_recv>`
     - 将 device 上的内存发布为数据系统的异构对象，发布后的异构对象可通过 dev_recv 获取。
   * - :doc:`datasystem.DsTensorClient.dev_mset <datasystem.DsTensorClient.dev_mset>`
     - 通过数据系统缓存 Device 上的数据。
   * - :doc:`datasystem.DsTensorClient.dev_mget <datasystem.DsTensorClient.dev_mget>`
     - 获取 device 中的数据，并写入到 Tensor 中。
   * - :doc:`datasystem.DsTensorClient.dev_local_delete <datasystem.DsTensorClient.dev_local_delete>`
     - 从数据系统删除本节点上此 key 的元数据，不再管理此 key 对应的 device 内存。
   * - :doc:`datasystem.DsTensorClient.dev_delete <datasystem.DsTensorClient.dev_delete>`
     -  从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。
   * - :doc:`datasystem.DsTensorClient.async_dev_delete <datasystem.DsTensorClient.async_dev_delete>`
     -  从数据系统删除此 key 的元数据的异步接口，删除成功后不再管理此 key 对应的 device 内存。
   * - :doc:`datasystem.DsTensorClient.put_page_attn_layerwise_d2d <datasystem.DsTensorClient.put_page_attn_layerwise_d2d>`
     - 将 PagedAttention 的层级 Tensor 发布为数据系统的异构对象。发布后的异构对象可通过 get_page_attn_layerwise_d2d 获取。
   * - :doc:`datasystem.DsTensorClient.get_page_attn_layerwise_d2d <datasystem.DsTensorClient.get_page_attn_layerwise_d2d>`
     - 根据 key 获取缓存在数据系统的 PagedAttention 的层级 Tensor。
   * - :doc:`datasystem.DsTensorClient.mset_page_attn_blockwise_d2h <datasystem.DsTensorClient.mset_page_attn_blockwise_d2h>`
     - 将 PagedAttention 的层级 Tensor 异步写入 Host 中。
   * - :doc:`datasystem.DsTensorClient.mget_page_attn_blockwise_h2d <datasystem.DsTensorClient.mget_page_attn_blockwise_h2d>`
     - 从 Host 中获取 PagedAttention 的层级 Tensor 并写入 Device 中。


异构对象接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`datasystem.hetero_client.HeteroClient.init <datasystem.hetero_client.HeteroClient.init>`
     - 初始化异构对象客户端。
   * - :doc:`datasystem.hetero_client.HeteroClient.mget_h2d <datasystem.hetero_client.HeteroClient.mget_h2d>`
     - 从 host 中获取数据并写入 device 中。
   * - :doc:`datasystem.hetero_client.HeteroClient.mset_d2h <datasystem.hetero_client.HeteroClient.mset_d2h>`
     - 将 device 的数据写入到 host 中。
   * - :doc:`datasystem.hetero_client.HeteroClient.async_mget_h2d <datasystem.hetero_client.HeteroClient.async_mget_h2d>`
     - 从 host 中获取数据并写入 device 中的异步接口。
   * - :doc:`datasystem.hetero_client.HeteroClient.async_mset_d2h <datasystem.hetero_client.HeteroClient.async_mset_d2h>`
     - 将 device 的数据写入到 host 中的异步接口。
   * - :doc:`datasystem.hetero_client.HeteroClient.delete <datasystem.hetero_client.HeteroClient.delete>`
     - 删除 host 中的 key，与 mget_h2d / mset_d2h 配套使用。
   * - :doc:`datasystem.hetero_client.HeteroClient.dev_publish <datasystem.hetero_client.HeteroClient.dev_publish>`
     - 将 device 上的内存发布为数据系统的异构对象。发布后的异构对象可通过 dev_subscribe 获取。
   * - :doc:`datasystem.hetero_client.HeteroClient.dev_subscribe <datasystem.hetero_client.HeteroClient.dev_subscribe>`
     - 订阅发布到数据系统的异构对象，并接收数据写入 data_blob_list。数据通过 device to device 通道直接传输。
   * - :doc:`datasystem.hetero_client.HeteroClient.dev_mset <datasystem.hetero_client.HeteroClient.dev_mset>`
     - 通过数据系统缓存 Device 上的数据。
   * - :doc:`datasystem.hetero_client.HeteroClient.dev_mget <datasystem.hetero_client.HeteroClient.dev_mget>`
     - 获取 device 中的数据。
   * - :doc:`datasystem.hetero_client.HeteroClient.dev_local_delete <datasystem.hetero_client.HeteroClient.dev_local_delete>`
     - 从数据系统删除本节点上此 key 的元数据，不再管理此 key 对应的 device 内存。
   * - :doc:`datasystem.hetero_client.HeteroClient.dev_delete <datasystem.hetero_client.HeteroClient.dev_delete>`
     - 从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。
   * - :doc:`datasystem.hetero_client.HeteroClient.async_dev_delete <datasystem.hetero_client.HeteroClient.async_dev_delete>`
     - 从数据系统删除此 key 的元数据的异步接口，删除成功后不再管理此 key 对应的 device 内存。
   * - :doc:`datasystem.hetero_client.HeteroClient.generate_key <datasystem.hetero_client.HeteroClient.generate_key>`
     - 生成一个带数据系统 Worker UUID 的 key。
   * - :doc:`datasystem.hetero_client.HeteroClient.get_meta_info <datasystem.hetero_client.HeteroClient.get_meta_info>`
     - 获取keys 对应的元数据信息。


KV接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`datasystem.kv_client.KVClient.init <datasystem.kv_client.KVClient.init>`
     - 初始化KV缓存客户端以连接到 Worker 。
   * - :doc:`datasystem.kv_client.KVClient.set <datasystem.kv_client.KVClient.set>`
     - 设置键的值。
   * - :doc:`datasystem.kv_client.KVClient.set_value <datasystem.kv_client.KVClient.set_value>`
     - 设置键的值，键由系统生成并返回。
   * - :doc:`datasystem.kv_client.KVClient.mset <datasystem.kv_client.KVClient.mset>`
     - 批量设置键值对。
   * - :doc:`datasystem.kv_client.KVClient.msettx <datasystem.kv_client.KVClient.msettx>`
     - 批量设置键值对（事务操作），它保证所有的键要么都成功设置，要么都失败。
   * - :doc:`datasystem.kv_client.KVClient.get_read_only_buffers <datasystem.kv_client.KVClient.get_read_only_buffers>`
     - 获取所有给定键的值。
   * - :doc:`datasystem.kv_client.KVClient.get <datasystem.kv_client.KVClient.get>`
     - 获取所有给定键的值。
   * - :doc:`datasystem.kv_client.KVClient.read <datasystem.kv_client.KVClient.read>`
     - 读取指定偏移量的数据。
   * - :doc:`datasystem.kv_client.KVClient.delete <datasystem.kv_client.KVClient.delete>`
     - 初始化KV缓存客户端以连接到 Worker 。
   * - :doc:`datasystem.kv_client.KVClient.generate_key <datasystem.kv_client.KVClient.generate_key>`
     - 生成一个带数据系统 Worker UUID 的 key。
   * - :doc:`datasystem.kv_client.KVClient.exist <datasystem.kv_client.KVClient.exist>`
     - 查看 key 在数据系统中是否存在。
   * - :doc:`datasystem.kv_client.KVClient.expire <datasystem.kv_client.KVClient.expire>`
     - 为一组键设置过期生命周期，返回函数操作状态及设置失败的键列表。


对象缓存接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`datasystem.object_client.ObjectClient.init <datasystem.object_client.ObjectClient.init>`
     - 初始化对象缓存客户端。
   * - :doc:`datasystem.object_client.ObjectClient.create <datasystem.object_client.ObjectClient.create>`
     - 创建对象buffer。
   * - :doc:`datasystem.object_client.ObjectClient.put <datasystem.object_client.ObjectClient.put>`
     - 将对象缓存到数据系统中。
   * - :doc:`datasystem.object_client.ObjectClient.get <datasystem.object_client.ObjectClient.get>`
     - 获取给定列表对象 key 的Buffer。
   * - :doc:`datasystem.object_client.ObjectClient.g_increase_ref <datasystem.object_client.ObjectClient.g_increase_ref>`
     - 增加给定列表对象 key 的全局引用计数。
   * - :doc:`datasystem.object_client.ObjectClient.g_decrease_ref <datasystem.object_client.ObjectClient.g_decrease_ref>`
     - 减少给定列表对象 key 的全局引用计数。
   * - :doc:`datasystem.object_client.ObjectClient.query_global_ref_num <datasystem.object_client.ObjectClient.query_global_ref_num>`
     - 查询对象全局引用计数。
   * - :doc:`datasystem.object_client.ObjectClient.generate_object_key <datasystem.object_client.ObjectClient.generate_object_key>`
     - 生成一个带数据系统Worker UUID的对象 key。