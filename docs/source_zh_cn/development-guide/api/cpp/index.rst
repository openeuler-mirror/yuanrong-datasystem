C++
==============================

.. toctree::
   :glob:
   :hidden:
   :maxdepth: 1

   KVClient
   HeteroClient
   ObjectClient
   Buffer
   struct-ConnectOptions
   struct-SetParam
   struct-Blob
   struct-DeviceBlobList
   struct-MetaInfo
   struct-AsyncResult
   struct-CreateParam
   struct-ObjMetaInfo
   enum-WriteMode
   enum-ConsistencyType
   enum-CacheType
   SensitiveValue
   StringView
   Optional
   Status
   Future
   enum-StatusCode

KV接口
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :cpp:func:`KVClient::KVClient`
      - 构造KV缓存客户端实例。
    * - :cpp:func:`KVClient::~KVClient`
      - 析构KV缓存客户端实例，析构过程中会自动断开与 Worker 的连接，释放客户端持有的资源。
    * - :cpp:func:`KVClient::Init`
      - 建立与数据系统 Worker 之间的连接并完成初始化。
    * - :cpp:func:`KVClient::ShutDown`
      - 断开与数据系统 Worker 之间的连接。
    * - :cpp:func:`KVClient::Create`
      - 创建数据系统共享内存Buffer，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。
    * - :cpp:func:`KVClient::Set`
      - 将共享内存数据缓存到数据系统。
    * - :cpp:func:`KVClient::MCreate`
      - 创建数据系统共享内存Buffer，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。
    * - :cpp:func:`KVClient::MSet`
      - 键值对批量设置接口。
    * - :cpp:func:`KVClient::Get`
      - 获取键对应的数据。
    * - :cpp:func:`KVClient::Del`
      - 删除指定键值对。
    * - :cpp:func:`KVClient::DelAll`
      - 异步删除集群中所有的键值对。
    * - :cpp:func:`KVClient::HealthCheck`
      - 检查连接的 Worker 是否健康。
    * - :cpp:func:`KVClient::Exist`
      - 批量查询一组键（keys）是否存在。
    * - :cpp:func:`KVClient::Expire`
      - 批量为一组键（keys）更新过期生命周期。

Hetero接口
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :cpp:func:`HeteroClient::HeteroClient`
      - 构造Hetero缓存客户端实例。
    * - :cpp:func:`HeteroClient::~HeteroClient`
      - 析构Hetero缓存客户端实例，析构过程中会自动断开与 Worker 的连接，释放客户端持有的资源。
    * - :cpp:func:`HeteroClient::Init`
      - 建立与数据系统 Worker 之间的连接并完成初始化。
    * - :cpp:func:`HeteroClient::ShutDown`
      - 断开与数据系统 Worker 之间的连接。
    * - :cpp:func:`HeteroClient::MSetD2H`
      - 批量将数据从异构设备(Device)缓存到数据系统主机(Host)侧。
    * - :cpp:func:`HeteroClient::MGetH2D`
      - 批量从数据系统主机(Host)侧获取数据并直接写入到异构设备(Device)内存中。该接口与 :cpp:func:`MSetD2H` 配合使用。
    * - :cpp:func:`HeteroClient::AsyncMSetD2H`
      - 批量将数据从异构设备(Device)异步缓存到数据系统主机(Host)侧，立即返回 std::shared_future< :cpp:class:`AsyncResult` > 对象。
    * - :cpp:func:`HeteroClient::AsyncMGetH2D`
      - 批量将数据从系统主机(Host)异步获取到异构设备主机(Host)侧，立即返回 std::shared_future< :cpp:class:`AsyncResult` > 对象。与 :cpp:func:`AsyncMSetD2H` 配合使用。
    * - :cpp:func:`HeteroClient::DevPublish`
      - 将异构设备上的内存作为数据系统的异构对象发布。异构对象可通过 DevSubscribe 获取。DevPublish 和 DevSubscribe 必须同时使用。
    * - :cpp:func:`HeteroClient::DevSubscribe`
      - 批量获取异构设备内存上的数据。DevPublish 和 DevSubscribe 必须同时使用。通过 DevSubscribe 获取数据后，数据系统会自动删除该异构对象，并且不再管理与该对象对应的异构设备内存。
    * - :cpp:func:`HeteroClient::DevMSet`
      - 批量缓存异构设备（Device）数据到数据系统中，缓存成功后其他客户端可通过 key 访问对应的异构设备（Device）数据。
    * - :cpp:func:`HeteroClient::DevMGet`
      - 批量从异构设备获取数据并将其写入devBlobList。数据直接通过异构设备间通道传输。
    * - :cpp:func:`HeteroClient::Delete`
      - 批量删除指定key(通过MSetD2H/AsyncMSetD2H缓存进来的key)。key不存在时视为删除成功。
    * - :cpp:func:`HeteroClient::AsyncDevDelete`
      - 异步删除数据在异构设备的key(通过 :cpp:func:`DevMSet` 缓存的key)。执行此命令后，数据系统将不再管理与该key对应的设备内存。
    * - :cpp:func:`HeteroClient::DevDelete`
      - 批量删除数据在异构设备的key（由DevMset缓存的key）。执行此命令后，数据系统将不再管理与该key对应的设备内存。
    * - :cpp:func:`HeteroClient::DevLocalDelete`
      - 删除当前客户端连接存储在数据系统中的数据副本。
    * - :cpp:func:`HeteroClient::HealthCheck`
      - 检查连接的 Worker 是否健康。
    * - :cpp:func:`HeteroClient::Exist`
      - 批量查询一组key是否存在，并返回每个key的存在性状态。支持最多10000个key的查询。
    * - :cpp:func:`HeteroClient::GetMetaInfo`
      - 批量获取指定租户的key的数据大小和位置。
    * - :cpp:func:`HeteroClient::GenerateKey`
      - 生成一个唯一的key，提供给其他接口设置数据使用。

Object接口
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :cpp:func:`ObjectClient::ObjectClient`
      - 构造Object缓存客户端实例。
    * - :cpp:func:`ObjectClient::~ObjectClient`
      - 析构Object缓存客户端实例，析构过程中会自动断开与 Worker 的连接，释放客户端持有的资源。
    * - :cpp:func:`ObjectClient::Init`
      - 建立与数据系统 Worker 之间的连接并完成初始化。
    * - :cpp:func:`ObjectClient::ShutDown`
      - 断开与数据系统 Worker 之间的连接。
    * - :cpp:func:`ObjectClient::GIncreaseRef`
      - 为指定的对象的全局引用计数加1。推荐在create对象之前为对象增加引用计数，因为在跨节点访问场景中，未进行引用计数的对象可能刚创造就会被驱逐。
    * - :cpp:func:`ObjectClient::GDecreaseRef`
      - 为指定的对象的全局引用计数减1。全局引用计数为0时，删除对象。
    * - :cpp:func:`ObjectClient::QueryGlobalRefNum`
      - 获取指定object key的全局引用计数数量。
    * - :cpp:func:`ObjectClient::Create`
      - 创建数据系统共享内存Buffer，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。该接口应用于避免创建临时内存，减少内存拷贝的场景。
    * - :cpp:func:`ObjectClient::Put`
      - 设置键值对数据缓存到数据系统。
    * - :cpp:func:`ObjectClient::GetObjMetaInfo`
      - 获取指定租户的object keys的大小和位置。
    * - :cpp:func:`ObjectClient::GenerateKey`
      - 生成一个唯一的key，提供给Put接口使用。。
    * - :cpp:func:`ObjectClient::GetPrefix`
      - 获取指定key的前缀。
    * - :cpp:func:`KVClient::HealthCheck`
      - 检查连接的 Worker 是否健康。