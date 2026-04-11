HeteroClient
====================

.. cpp:class:: HeteroClient

    :header-file: #include <datasystem/hetero_client.h>
    :namespace: datasystem

    Hetero缓存客户端。

    **公共函数**
 
    .. cpp:function:: HeteroClient(const ConnectOptions &connectOptions)
 
       构造Hetero缓存客户端实例。

       参数：
            - **connectOptions** - 配置连接选项，包括IP地址和端口，详见 :cpp:class:`ConnectOptions` 章节。
 
       返回：
           Hetero缓存客户端实例。

    .. cpp:function:: ~HeteroClient()
 
       析构Hetero缓存客户端实例，析构过程中会自动断开与 Worker 的连接，释放客户端持有的资源。
 
    .. cpp:function:: Status Init()
 
       建立与数据系统 Worker 之间的连接并完成初始化。
 
       返回：
           返回值状态码为 StatusCode::K_OK 时表示初始化成功，否则返回其他错误码。

    .. cpp:function:: Status ShutDown()

        断开与数据系统 Worker 之间的连接。

        返回:
            返回值状态码为 StatusCode::K_OK 时表示断链成功，否则返回其他错误码。

    .. cpp:function:: Status MSetD2H(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam = {})

        批量将数据从异构设备(Device)缓存到数据系统主机(Host)侧。

        参数：
            - **keys** - 需要设置的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。推荐单次设置的key个数 `<=64`。出于性能考虑，只校验第一个key的有效性。
            - **devBlobList** - 传入参数，描述用于发送的异构设备内存结构的列表，数据从 `devBlobList` 中的指针获取。详见 :cpp:class:`DeviceBlobList` 章节。
            - **setParam** - 设置参数，详见 :cpp:class:`SetParam` 章节。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status MGetH2D(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failKeys, int32_t subTimeoutMs)

        批量从数据系统主机(Host)侧获取数据并直接写入到异构设备(Device)内存中。该接口与 :cpp:func:`MSetD2H` 配合使用。

        参数：
            - **keys** - 需要获取的一组key， 最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **devBlobList** - 传出参数，描述用于接收的异构设备内存结构的列表，数据从主机获取并写入 `devBlobList` 中的指针。详见 :cpp:class:`DeviceBlobList` 章节。
            - **failKeys** - 传出参数，返回获取失败的key。
            - **subTimeoutMs** - 传入参数， 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示获取成功，否则返回其他错误码。

    .. cpp:function:: std::shared_future<AsyncResult> AsyncMSetD2H(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, const SetParam &setParam = {})

        批量将数据从异构设备(Device)异步缓存到数据系统主机(Host)侧，立即返回 std::shared_future< :cpp:class:`AsyncResult` > 对象。

        参数：
            - **keys** - 需要设置的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。推荐单次设置的key个数 `<=64`。出于性能考虑，只校验第一个key的有效性。
            - **devBlobList** - 传入参数，描述用于发送的异构设备内存结构的列表，如果 DeviceBlobList 包含多个 HBM 指针，则数据将被合并并写入主机对应的共享内存。详见 :cpp:class:`DeviceBlobList` 章节。
            - **setParam** - 设置参数，详见 :cpp:class:`SetParam` 章节。

        返回：
            异步操作结果通过 std::shared_future< :cpp:class:`AsyncResult` > 返回。

    .. cpp:function:: std::shared_future<AsyncResult> AsyncMGetH2D(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, uint64_t subTimeoutMs)

        批量将数据从系统主机(Host)异步获取到异构设备主机(Host)侧，立即返回 std::shared_future< :cpp:class:`AsyncResult` > 对象， 与 :cpp:func:`AsyncMSetD2H` 配合使用。

        参数：
            - **keys** - 需要获取的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **devBlobList** - 传出参数。描述用于接收的异构设备内存结构的列表， 数据从主机获取并写入 `devBlobList` 中的指针。详见 :cpp:class:`DeviceBlobList` 章节。
            - **subTimeoutMs** - 传入参数， 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。

        返回：
            异步操作结果通过 std::shared_future< :cpp:class:`AsyncResult` > 返回。

    .. cpp:function:: Status DevPublish(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<Future> &futureVec)


        将异构设备上的内存作为数据系统的异构对象发布。异构对象可通过 DevSubscribe 获取。DevPublish 和 DevSubscribe 必须同时使用。

        参数：
            - **keys** - 需要设置的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。推荐单次设置的key个数 `<=64`。
            - **devBlobList** - 传入参数，描述用于发送的异构设备内存结构的列表。详见 :cpp:class:`DeviceBlobList` 章节。
            - **futureVec** - 传出参数，异步返回的 :cpp:class:`Future` 列表，用于获取发送异步操作结果。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示设置成功，否则返回其他错误码。


    .. cpp:function:: Status DevSubscribe(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<Future> &futureVec)

        批量获取异构设备内存上的数据。DevPublish 和 DevSubscribe 必须同时使用。通过 DevSubscribe 获取数据后，数据系统会自动删除该异构对象，并且不再管理与该对象对应的异构设备内存。

        参数：
            - **keys** - 需要获取的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **devBlobList** - 传出参数，描述用于接收的异构设备内存结构的列表。详见 :cpp:class:`DeviceBlobList` 章节。
            - **futureVec** - 传出参数，异步返回的 :cpp:class:`Future` 列表，用于获取发送异步操作结果。

        返回：
           返回值状态码为 ``StatusCode::K_OK`` 时表示获取成功，否则返回其他错误码。

    .. cpp:function:: Status DevMSet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys)

        批量缓存异构设备（Device）数据到数据系统中，缓存成功后其他客户端可通过 key 访问对应的异构设备（Device）数据。
        **note**: 数据系统不负责管理异构设备（Device）数据的生命周期，只管理了其对应的元数据，使用该接口时需要用户自行保证数据的生命周期。

        参数：
            - **keys** - 需要设置的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。推荐单次设置的key个数 `<=64`。出于性能考虑，只校验第一个key的有效性。
            - **devBlobList** - 传入参数，用于获取异构设备内存上的数据。详见 :cpp:class:`DeviceBlobList` 章节。
            - **failedKeys** - 传出参数，返回设置失败的key。
        
        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status DevMGet(const std::vector<std::string> &keys, std::vector<DeviceBlobList> &devBlobList, std::vector<std::string> &failedKeys, int32_t subTimeoutMs = 0)
    
        批量从异构设备获取数据并将其写入devBlobList。数据直接通过异构设备间通道传输。
        
        参数：
            - **keys** - 要获取的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **devBlobList** - 传出参数。用于获取异构设备内存上的数据。详见 :cpp:class:`DeviceBlobList` 章节。
            - **failedKeys** - 传出参数，返回获取数据失败的key。
            - **subTimeoutMs** - 传入参数， 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示获取成功，否则返回其他错误码。

    .. cpp:function:: Status Delete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)

        批量删除指定key( :cpp:func:`MSetD2H` / :cpp:func:`AsyncMSetD2H` 缓存进来的key)。key不存在时视为删除成功。

        参数：
            - **keys** - 需要删除的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个key最大长度为255字节。推荐单次获取key个数 `<=64`。
            - **failedKeys** - 传出参数，返回删除失败的key。
        
        返回：
            - 返回 ``StatusCode::K_OK`` 表示至少有一个数据删除成功。
            - 返回 ``StatusCode::K_INVALID`` 表示存在key校验不通过。
            - 返回值状态码为 ``StatusCode::K_RPC_UNAVAILABLE`` 时表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。

    .. cpp:function:: std::shared_future<AsyncResult> AsyncDevDelete(const std::vector<std::string> &keys)

        异步删除数据在异构设备的key(通过 :cpp:func:`DevMSet` 缓存的key)。执行此命令后，数据系统将不再管理与该key对应的设备内存。
        
        参数：
            - **keys** - 需要删除的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。推荐单次设置的key个数 `<=64`。
        
        返回：
            异步删除操作结果通过 std::shared_future< :cpp:class:`AsyncResult`> 返回。

    .. cpp:function:: Status DevDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)

        批量删除数据在异构设备的key（由DevMset缓存的key）。执行此命令后，数据系统将不再管理与该key对应的设备内存。
        
        参数：
            - **keys** - 需要删除的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。推荐单次设置的key个数 `<=64`。
            - **failedKeys** - 传出参数，返回删除失败的key。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示删除成功，否则返回其他错误码。

    .. cpp:function:: Status DevLocalDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)

        删除当前客户端连接存储在数据系统中的数据副本。

        参数：
            - **keys** - 需要删除的一组key，最多不超过10000个。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。推荐单次设置的key个数 `<=64`。
            - **failedKeys** - 传出参数，返回删除失败的key。
        
        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示删除成功，否则返回其他错误码。

    .. cpp:function:: Status HealthCheck(ServerState &state)

        检查连接的 Worker 是否健康。
        
        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示 Worker 健康，否则返回其他错误码。

    .. cpp:function:: Status Exist(const std::vector<std::string> &keys, std::vector<bool> &exists)

        批量查询一组key是否存在，并返回每个key的存在性状态。支持最多10000个key的查询。

        参数：
            - **keys** - 待查询的key列表，最大支持10000个key。
            - **exists** - 传出参数，返回每个key的存在性状态。

        返回：
            - 返回 ``StatusCode::K_OK`` 表示查询成功。
            - 返回 ``StatusCode::K_INVALID`` 表示提供的key中包含非法字符或为空。
            - 返回 ``StatusCode::K_RPC_UNAVAILABLE`` 表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_NOT_READY`` 表示服务当前无法处理请求。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。

    .. cpp:function:: Status GetMetaInfo(const std::vector<std::string> &keys, bool isDevKey, std::vector<MetaInfo> &metaInfos, std::vector<std::string> &failKeys)

        批量获取指定租户的key的数据大小和位置。

        参数：
            - **keys** - 待查询的key列表，最大支持10000个key， key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个key最大长度为255字节。
            - **isDevKey** - 这一批key是否数据是设置在异构设备上的。
            - **metaInfos** - 传出参数，返回每个key的数据大小和存储的Worker节点信息 :cpp:class:`MetaInfo`。
            - **failKeys** - 传出参数， 返回查询失败的key。

        返回：
           返回值状态码为 ``StatusCode::K_OK`` 时表示获取成功，否则返回其他错误码。

    .. cpp:function:: Status GenerateKey(const std::string &prefix, std::string &key)

        生成一个唯一的key，提供给其他接口设置数据使用。
        
        参数：
            - **prefix** - 传入参数， 指定返回生成的key的前缀。
            - **key** - 传出参数，支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示生成成功，否则返回其他错误码。