Python
==============================


.. toctree::
   :glob:
   :hidden:
   :maxdepth: 1

   yr.datasystem.DsClient
   yr.datasystem.DsTensorClient
   yr.datasystem.hetero_client.HeteroClient
   yr.datasystem.hetero_client.Blob
   yr.datasystem.hetero_client.DeviceBlobList
   yr.datasystem.hetero_client.Future
   yr.datasystem.hetero_client.MetaInfo
   yr.datasystem.kv_client.KVClient
   yr.datasystem.kv_client.ReadOnlyBuffer
   yr.datasystem.kv_client.SetParam
   yr.datasystem.kv_client.ReadParam
   yr.datasystem.object_client.ObjectClient
   yr.datasystem.object_client.Buffer
   yr.datasystem.object_client.ConsistencyType
   yr.datasystem.object_client.WriteMode
   yr.datasystem.object_client.CacheType
   yr.datasystem.stream_client.StreamClient
   yr.datasystem.stream_client.Producer
   yr.datasystem.stream_client.Consumer
   yr.datasystem.stream_client.SubconfigType

DsClient：聚合各语义的客户端
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :doc:`yr.datasystem.DsClient.init <yr.datasystem.DsClient.init>`
      - 初始化数据系统客户端
    * - :doc:`yr.datasystem.DsClient.hetero <yr.datasystem.DsClient.hetero>` 
      - 获取数据系统异构缓存客户端。
    * - :doc:`yr.datasystem.DsClient.kv <yr.datasystem.DsClient.kv>`
      - 获取数据系统KV缓存客户端。
    * - :doc:`yr.datasystem.DsClient.object <yr.datasystem.DsClient.object>`
      - 获取数据系统对象缓存客户端。


Tensor接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`yr.datasystem.DsTensorClient.init <yr.datasystem.DsTensorClient.init>`
     - 初始化数据系统客户端。
   * - :doc:`yr.datasystem.DsTensorClient.mset_d2h <yr.datasystem.DsTensorClient.mset_d2h>` 
     - 将 device 的数据写入到 host 中。
   * - :doc:`yr.datasystem.DsTensorClient.mget_h2d <yr.datasystem.DsTensorClient.mget_h2d>`
     - 从 host 中获取数据并写入 device 中。
   * - :doc:`yr.datasystem.DsTensorClient.async_mset_d2h <yr.datasystem.DsTensorClient.async_mset_d2h>`
     - 将 device 的数据写入到 host 中的异步接口。
   * - :doc:`yr.datasystem.DsTensorClient.async_mget_h2d <yr.datasystem.DsTensorClient.async_mget_h2d>`
     - 从 host 中获取数据并写入 device 中的异步接口。
   * - :doc:`yr.datasystem.DsTensorClient.delete <yr.datasystem.DsTensorClient.delete>`
     - 删除 host 中的 key。
   * - :doc:`yr.datasystem.DsTensorClient.dev_send <yr.datasystem.DsTensorClient.dev_send>`
     - 将 device 上的 Tensor 缓存作为数据系统的异构对象发送。
   * - :doc:`yr.datasystem.DsTensorClient.dev_recv <yr.datasystem.DsTensorClient.dev_recv>`
     - 接收数据系统的异构对象，并将数据写入 tensors。
   * - :doc:`yr.datasystem.DsTensorClient.exist <yr.datasystem.DsTensorClient.exist>`
     - 检查给定 key 在数据系统中是否存在。
   * - :doc:`yr.datasystem.DsTensorClient.dev_mset <yr.datasystem.DsTensorClient.dev_mset>`
     - 通过数据系统缓存 device 上的数据。
   * - :doc:`yr.datasystem.DsTensorClient.dev_mget <yr.datasystem.DsTensorClient.dev_mget>`
     - 获取 device 中的数据，并写入到 Tensor 中。
   * - :doc:`yr.datasystem.DsTensorClient.dev_mget_into_tensor <yr.datasystem.DsTensorClient.dev_mget_into_tensor>`
     - 从 device 中获取多个 key 的数据，并根据复制范围写入到单个目标 Tensor 的指定位置。
   * - :doc:`yr.datasystem.DsTensorClient.dev_local_delete <yr.datasystem.DsTensorClient.dev_local_delete>`
     - 从数据系统删除本节点上此 key 的元数据，不再管理此 key 对应的 device 内存。
   * - :doc:`yr.datasystem.DsTensorClient.dev_delete <yr.datasystem.DsTensorClient.dev_delete>`
     -  从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。
   * - :doc:`yr.datasystem.DsTensorClient.async_dev_delete <yr.datasystem.DsTensorClient.async_dev_delete>`
     -  从数据系统删除此 key 的元数据的异步接口，删除成功后不再管理此 key 对应的 device 内存。


异构对象接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.init <yr.datasystem.hetero_client.HeteroClient.init>`
     - 初始化异构对象客户端。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.mget_h2d <yr.datasystem.hetero_client.HeteroClient.mget_h2d>`
     - 从 host 中获取数据并写入 device 中。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.mget_h2d_from_multi_buffers <yr.datasystem.hetero_client.HeteroClient.mget_h2d_from_multi_buffers>`
     - 通过二维地址和大小列表将 host 对象直接写入多个 device buffer。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.pre_register_device_memory <yr.datasystem.hetero_client.HeteroClient.pre_register_device_memory>`
     - 为 RH2D over HIXL HCCS 预注册后续 MGetH2D 使用的 device 目标内存。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.mset_d2h <yr.datasystem.hetero_client.HeteroClient.mset_d2h>`
     - 将 device 的数据写入到 host 中。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.mset_d2h_from_multi_buffers <yr.datasystem.hetero_client.HeteroClient.mset_d2h_from_multi_buffers>`
     - 通过二维地址和大小列表将多个 device buffer 写入 host 对象。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.async_mget_h2d <yr.datasystem.hetero_client.HeteroClient.async_mget_h2d>`
     - 从 host 中获取数据并写入 device 中的异步接口。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.async_mset_d2h <yr.datasystem.hetero_client.HeteroClient.async_mset_d2h>`
     - 将 device 的数据写入到 host 中的异步接口。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.delete <yr.datasystem.hetero_client.HeteroClient.delete>`
     - 删除 host 中的 key，与 mget_h2d / mset_d2h 配套使用。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.dev_publish <yr.datasystem.hetero_client.HeteroClient.dev_publish>`
     - 将 device 上的内存发布为数据系统的异构对象。发布后的异构对象可通过 dev_subscribe 获取。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.dev_subscribe <yr.datasystem.hetero_client.HeteroClient.dev_subscribe>`
     - 订阅发布到数据系统的异构对象，并接收数据写入 data_blob_list。数据通过 device to device 通道直接传输。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.dev_mset <yr.datasystem.hetero_client.HeteroClient.dev_mset>`
     - 通过数据系统缓存 device 上的数据。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.dev_mget <yr.datasystem.hetero_client.HeteroClient.dev_mget>`
     - 获取 device 中的数据。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.dev_local_delete <yr.datasystem.hetero_client.HeteroClient.dev_local_delete>`
     - 从数据系统删除本节点上此 key 的元数据，不再管理此 key 对应的 device 内存。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.dev_delete <yr.datasystem.hetero_client.HeteroClient.dev_delete>`
     - 从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.async_dev_delete <yr.datasystem.hetero_client.HeteroClient.async_dev_delete>`
     - 从数据系统删除此 key 的元数据的异步接口，删除成功后不再管理此 key 对应的 device 内存。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.generate_key <yr.datasystem.hetero_client.HeteroClient.generate_key>`
     - 生成唯一的键。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.get_meta_info <yr.datasystem.hetero_client.HeteroClient.get_meta_info>`
     - 获取keys 对应的元数据信息。
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.batch_is_exist <yr.datasystem.hetero_client.HeteroClient.batch_is_exist>`
     - Batch-check key existence and return integer indicators.
   * - :doc:`yr.datasystem.hetero_client.HeteroClient.exist <yr.datasystem.hetero_client.HeteroClient.exist>`
     - 检查给定的键在数据系统中是否存在。


KV接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`yr.datasystem.kv_client.KVClient.init <yr.datasystem.kv_client.KVClient.init>`
     - 初始化KV缓存客户端以连接到 Worker 。
   * - :doc:`yr.datasystem.kv_client.KVClient.set <yr.datasystem.kv_client.KVClient.set>`
     - 设置键的值。
   * - :doc:`yr.datasystem.kv_client.KVClient.set_value <yr.datasystem.kv_client.KVClient.set_value>`
     - 设置键的值，键由系统生成并返回。
   * - :doc:`yr.datasystem.kv_client.KVClient.mset <yr.datasystem.kv_client.KVClient.mset>`
     - 批量设置键值对。
   * - :doc:`yr.datasystem.kv_client.KVClient.mcreate <yr.datasystem.kv_client.KVClient.mcreate>`
     - 创建数据系统共享内存 Buffer ，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。
   * - :doc:`yr.datasystem.kv_client.KVClient.mset_buffer <yr.datasystem.kv_client.KVClient.mset_buffer>`
     - 批量将共享内存 Buffer 缓存到数据系统中。     
   * - :doc:`yr.datasystem.kv_client.KVClient.mget_buffer <yr.datasystem.kv_client.KVClient.mget_buffer>`
     - 获取键对应的只读共享内存 Buffer 。
   * - :doc:`yr.datasystem.kv_client.KVClient.msettx <yr.datasystem.kv_client.KVClient.msettx>`
     - 已废弃 API，仅为兼容保留，调用固定抛出 ``RuntimeError``。
   * - :doc:`yr.datasystem.kv_client.KVClient.get_read_only_buffers <yr.datasystem.kv_client.KVClient.get_read_only_buffers>`
     - 获取所有给定键的值。
   * - :doc:`yr.datasystem.kv_client.KVClient.get <yr.datasystem.kv_client.KVClient.get>`
     - 获取所有给定键的值。
   * - :doc:`yr.datasystem.kv_client.KVClient.read <yr.datasystem.kv_client.KVClient.read>`
     - 读取指定偏移量的数据。
   * - :doc:`yr.datasystem.kv_client.KVClient.delete <yr.datasystem.kv_client.KVClient.delete>`
     - 初始化KV缓存客户端以连接到 Worker 。
   * - :doc:`yr.datasystem.kv_client.KVClient.generate_key <yr.datasystem.kv_client.KVClient.generate_key>`
     - 生成唯一的键。
   * - :doc:`yr.datasystem.kv_client.KVClient.exist <yr.datasystem.kv_client.KVClient.exist>`
     - 查看 key 在数据系统中是否存在。
   * - :doc:`yr.datasystem.kv_client.KVClient.expire <yr.datasystem.kv_client.KVClient.expire>`
     - 为一组键设置过期生命周期，返回函数操作状态及设置失败的键列表。
   * - :doc:`yr.datasystem.kv_client.KVClient.health_check <yr.datasystem.kv_client.KVClient.health_check>`
     - 查看连接的 Worker 的健康状态。


对象缓存接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`yr.datasystem.object_client.ObjectClient.init <yr.datasystem.object_client.ObjectClient.init>`
     - 初始化对象缓存客户端。
   * - :doc:`yr.datasystem.object_client.ObjectClient.create <yr.datasystem.object_client.ObjectClient.create>`
     - 创建对象buffer。
   * - :doc:`yr.datasystem.object_client.ObjectClient.put <yr.datasystem.object_client.ObjectClient.put>`
     - 将对象缓存到数据系统中。
   * - :doc:`yr.datasystem.object_client.ObjectClient.get <yr.datasystem.object_client.ObjectClient.get>`
     - 获取给定列表对象 key 的Buffer。
   * - :doc:`yr.datasystem.object_client.ObjectClient.g_increase_ref <yr.datasystem.object_client.ObjectClient.g_increase_ref>`
     - 增加给定列表对象 key 的全局引用计数。
   * - :doc:`yr.datasystem.object_client.ObjectClient.g_decrease_ref <yr.datasystem.object_client.ObjectClient.g_decrease_ref>`
     - 减少给定列表对象 key 的全局引用计数。
   * - :doc:`yr.datasystem.object_client.ObjectClient.query_global_ref_num <yr.datasystem.object_client.ObjectClient.query_global_ref_num>`
     - 查询对象全局引用计数。
   * - :doc:`yr.datasystem.object_client.ObjectClient.generate_object_id <yr.datasystem.object_client.ObjectClient.generate_object_id>`
     - 生成唯一的对象键。

  
流缓存接口
-----------------

.. list-table::
   :header-rows: 0
   :widths: 30 70
  
   * - :doc:`yr.datasystem.stream_client.StreamClient.init <yr.datasystem.stream_client.StreamClient.init>`
     - 初始化流缓存客户端。
   * - :doc:`yr.datasystem.stream_client.StreamClient.create_producer <yr.datasystem.stream_client.StreamClient.create_producer>`
     - 创建生产者, 创建生产者时会创建流。
   * - :doc:`yr.datasystem.stream_client.StreamClient.subscribe <yr.datasystem.stream_client.StreamClient.subscribe>`
     - 创建消费者，创建消费者时会创建流。
   * - :doc:`yr.datasystem.stream_client.StreamClient.delete_stream <yr.datasystem.stream_client.StreamClient.delete_stream>`
     - 删除数据流。
   * - :doc:`yr.datasystem.stream_client.StreamClient.query_global_producer_num <yr.datasystem.stream_client.StreamClient.query_global_producer_num>`
     - 指定流的名称，查询流的生产者数量。
   * - :doc:`yr.datasystem.stream_client.StreamClient.query_global_consumer_num <yr.datasystem.stream_client.StreamClient.query_global_consumer_num>`
     - 指定流的名称，查询流的消费者数量。
   * - :doc:`yr.datasystem.stream_client.Producer.send <yr.datasystem.stream_client.Producer.send>`
     - 生产者发送数据。
   * - :doc:`yr.datasystem.stream_client.Producer.close <yr.datasystem.stream_client.Producer.close>`
     - 关闭生产者。一旦关闭后，生产者不可再用。
   * - :doc:`yr.datasystem.stream_client.Producer.receive <yr.datasystem.stream_client.Consumer.receive>`
     - 消费者接收数据带有订阅功能，接收数据会等待接收expectNum个elements的时候返回成功，或者当超时时间timeoutMs到达返回成功。
   * - :doc:`yr.datasystem.stream_client.Producer.receive_any <yr.datasystem.stream_client.Consumer.receive_any>`
     - 消费者获取到element后立刻返回。如果没有element，将等待直到超时时间到达。
   * - :doc:`yr.datasystem.stream_client.Producer.ack <yr.datasystem.stream_client.Consumer.ack>`
     - 消费者接收完某elementId标识的element后，需要确认已消费完，使得各个worker上可以获取到是否所有消费者都已经消费完的信息，若所有消费者都消费完某个Page， 可以触发内部的内存回收机制。若不Ack，则在消费者退出时候才会自动Ack。
   * - :doc:`yr.datasystem.stream_client.Producer.close <yr.datasystem.stream_client.Consumer.close>`
     - 关闭消费者，关闭消费者后，它将不再允许调用receive和ack。对已关闭的消费者调用 Close() 方法将返回 ``StatusCode::K_OK``。
