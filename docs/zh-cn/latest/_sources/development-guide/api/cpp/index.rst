C++
==============================

.. toctree::
   :glob:
   :hidden:
   :maxdepth: 1

   KVClient
   Buffer
   struct-ConnectOptions
   struct-SetParam
   enum-WriteMode
   enum-ConsistencyType
   enum-CacheType
   SensitiveValue
   StringView
   Optional
   Status
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