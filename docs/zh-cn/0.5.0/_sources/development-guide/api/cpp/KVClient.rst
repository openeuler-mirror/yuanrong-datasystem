KVClient
====================

.. cpp:class:: KVClient

    :header-file: #include <datasystem/kv_client.h>
    :namespace: datasystem

    KV缓存客户端。

    **公共函数**
 
    .. cpp:function:: KVClient(const ConnectOptions &connectOptions)
 
       构造KV缓存客户端实例。

       参数：
            - **connectOptions** - 配置连接选项，包括IP地址和端口，详见 cpp:class:`ConnectOptions` 章节
 
       返回：
           KV缓存客户端实例。

    .. cpp:function:: ~KVClient()
 
       析构KV缓存客户端实例，析构过程中会自动断开与 Worker 的连接，释放客户端持有的资源。
 
    .. cpp:function:: Status Init()
 
       建立与数据系统 Worker 之间的连接并完成初始化。
 
       返回：
           返回值状态码为 StatusCode::K_OK 时表示初始化成功，否则返回其他错误码。

    .. cpp:function:: Status ShutDown()

        断开与数据系统 Worker 之间的连接。

        返回:
            返回值状态码为 StatusCode::K_OK 时表示断链成功，否则返回其他错误码。


    .. cpp:function:: Status Create(const std::string &key, uint64_t size, const SetParam &param, std::shared_ptr<Buffer> &buffer)

        创建数据系统共享内存Buffer，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。该接口应用于避免创建临时内存，减少内存拷贝的场景。

        参数：
            - **key** - 需要设置的key。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **size** - 需要创建的共享内存Buffer的大小，以字节为单位。
            - **param** - 设置参数，详见 :cpp:class:`SetParam` 章节。
            - **buffer** - 传出参数，表示创建好的共享内存 :cpp:class:`Buffer` 。

        返回：
            返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status Set(const std::shared_ptr<Buffer> &buffer)

        将共享内存数据缓存到数据系统。

        参数：
            - **buffer** - 共享内存 :cpp:class:`Buffer` 。

        返回值
            返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status Set(const std::string &key, const StringView &val, const SetParam &param)

        设置键值对数据缓存到数据系统。

        参数：
            - **key** - 键，key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节.
            - **val** - 需要缓存的值.
            - **param** - 设置参数，详见 :cpp:class:`SetParam` 章节。

        返回：
            返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status MCreate(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes, const SetParam &param, std::vector<std::shared_ptr<Buffer>> &buffers)

        创建数据系统共享内存Buffer，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。该接口应用于避免创建临时内存，减少内存拷贝的场景。

        参数： 
            - **keys** - 需要设置的一组key. key的合法字符为：英文字母（a-zA-Z）、数字以及 ``·-_!@#%^*()+=:;``，单个key最大长度为255字节. key的最大个数为10,000，推荐单次设置的key个数小于等于64个。
            - **sizes** - 设置共享内存Buffer的大小，以字节为单位. 该数组长度需要与 ``keys`` 的长度相等。
            - **param** - 设置参数，详见 :cpp:class:`SetParam` 章节。
            - **buffers** - 传出参数，表示创建好的共享内存 :cpp:class:`Buffer` 数组，该数组的长度与 ``keys`` 相等，索引位置一一对应，即每个 ``buffers[i]`` 的值与 ``keys[i]`` 相对应。

        返回：
            返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status MSet(const std::vector<std::shared_ptr<Buffer>> &buffers)

        键值对批量设置接口。与 :cpp:func:`Status MCreate(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes, const SetParam &param, std::vector<std::shared_ptr<Buffer>> &buffers)` 接口相互配合使用，用于批量将共享内存 :cpp:class:`Buffer` 缓存到数据系统中。

        参数：
            - **buffers** - 需要缓存到数据系统的共享内存 :cpp:class:`Buffer` 数组。

        返回：
            返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status MSet(const std::vector<std::string> &keys, const std::vector<StringView> &vals, std::vector<std::string> &outFailedKeys, const SetParam &param)

        键值对批量设置接口。可批量设置键值对并返回失败的键。

        参数：
            - **keys** - 需要设置的一组key. key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节. key的最大个数为10,000，推荐单次设置的key个数小于等于64个。
            - **vals** - 需要设置的一组key对应的value. 该数组长度需要与 ``keys`` 的长度相等。
            - **outFailedKeys** - 传出参数，代表设置失败的key。
            - **param** - 设置参数，详见 :cpp:class:`SetParam` 章节。

        返回：
            返回值状态码为 `K_OK` 时表示设置成功，否则返回其他错误码。
    
    .. cpp:function:: Status Get(const std::string &key, std::string &val, int32_t subTimeoutMs)

        获取键对应的数据。

        参数：
            - **key** - 键. key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。
            - **val** - 传出参数，返回缓存数据。
            - **subTimeoutMs** - 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。

        返回：
            - 返回 ``StatusCode::K_OK`` 表示获取成功。
            - 返回 ``StatusCode::K_INVALID`` 表示 ``key`` 校验不通过。
            - 返回 ``StatusCode::K_NOT_FOUND`` 表示 ``key`` 不存在。
            - 返回 ``StatusCode::K_RPC_UNAVAILABLE`` 时表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。

    .. cpp:function:: Status Get(const std::string &key, Optional<Buffer> &buffer, int32_t subTimeoutMs)

        获取键对应的共享内存 :cpp:class:`Buffer` 。该接口相比 :cpp:func:`Status Get(const std::string &key, std::string &val, int32_t subTimeoutMs)` 可减少一次从共享内存到临时内存的拷贝，直接读取缓存在共享内存上的数据，性能更优。

        参数：
            - **key** - 键. key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个key最大长度为255字节。
            - **subTimeoutMs** - 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。
            - **buffer** - 传出参数，返回的使用 :cpp:class:`Optional` 封装的共享内存 :cpp:class:`Buffer` ，当 Get 返回失败时，``buffer`` 的值为` ``nullptr``。

        返回：
            - 返回 ``StatusCode::K_OK`` 表示获取成功。
            - 返回 ``StatusCode::K_INVALID`` 表示 ``key`` 校验不通过。
            - 返回 ``StatusCode::K_NOT_FOUND`` 表示 ``key`` 不存在。
            - 返回 ``StatusCode::K_RPC_UNAVAILABLE`` 时表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。


    .. cpp:function:: Status Get(const std::vector<std::string> &keys, std::vector<std::string> &vals, int32_t subTimeoutMs)

        批量获取数据。

        参数：
            - **keys** - 需要获取的一组key. key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个key最大长度为255字节。传入的key的个数不能超过1万，推荐单次获取key个数小于等于64个。
            - **subTimeoutMs** - 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。
            - **vals** - 传出参数，返回一组获取的数据。若有部分数据获取不成功，则对应位置的vector的对象为空。

        返回：
            - 返回 ``StatusCode::K_OK`` 表示至少有一个数据获取成功。
            - 返回 ``StatusCode::K_INVALID`` 表示存在key校验不通过。
            - 返回 ``StatusCode::K_RPC_UNAVAILABLE`` 时表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_NOT_FOUND`` 表示所有 ``keys`` 不存在。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。


    .. cpp:function:: Status Get(const std::vector<std::string> &keys, std::vector<Optional<Buffer>> &buffers, int32_t subTimeoutMs)
    
        批量获取多个键对应的共享内存 :cpp:class:`Buffer` 。该接口相比 :cpp:func:`Status Get(const std::vector<std::string> &keys, std::vector<std::string> &vals, int32_t subTimeoutMs)` 少一次从共享内存到临时内存的拷贝，性能更优。
        
        参数：
            - **keys** - 需要获取的一组key. key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个key最大长度为255字节。传入的key的个数不能超过1万，推荐单次获取key个数小于等于64个。
            - **subTimeoutMs** - 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。
            - **buffers** - 传出参数，返回的一组使用 :cpp:class:`Optional` 封装的共享内存 :cpp:class:`Buffer` 。若有部分数据获取不成功，则对应位置的vector的对象为空。

        返回：
            - 返回 ``StatusCode::K_OK`` 表示至少有一个数据获取成功。
            - 返回 ``StatusCode::K_INVALID`` 表示存在key校验不通过。
            - 返回 ``StatusCode::K_RPC_UNAVAILABLE`` 时表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_NOT_FOUND`` 表示所有 ``keys`` 不存在。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。

    .. cpp:function:: Status Del(const std::string &key)

        删除指定键值对。key不存在时视为删除成功。

        参数：
            - **key** - 键. key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个key最大长度为255字节。

        返回：
            返回值状态码为 `K_OK` 时表示初始化成功，否则返回其他错误码。

    .. cpp:function:: Status Del(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)

        批量删除指定键值对。key不存在时视为删除成功。

        参数：
            - **keys** - 需要获取的一组key. key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个key最大长度为255字节。传入的key的个数不能超过1万，推荐单次获取key个数小于等于64个。
            - **failedKeys** - 传出参数，返回删除失败的key。
        
        返回：
            - 返回 ``StatusCode::K_OK`` 表示至少有一个数据删除成功。
            - 返回 ``StatusCode::K_INVALID`` 表示存在key校验不通过。
            - 返回值状态码为 ``StatusCode::K_RPC_UNAVAILABLE`` 时表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。

    .. cpp:function:: std::future<Status> DelAll()

        异步删除集群中所有的键值对。

        返回：
            ``future`` 代表 ``DelAll`` 操作的未来结果，可以通过 ``std::future::get()`` 函数获取异步结果。
    
    .. cpp:function:: Status HealthCheck()

        检查连接的 Worker 是否健康。
        
        返回：
            返回值状态码为 `K_OK` 时表示 Worker 健康，否则返回其他错误码。

    .. cpp:function:: Status KVClient::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists)

        批量查询一组键（keys）是否存在，并返回每个键的存在性状态。支持最多10000个键的查询。

        参数：
            - **keys** - 待查询的键列表，最大支持10000个键。
            - **exists** - 传出参数，返回每个键的存在性状态。

        返回：
            - 返回 ``StatusCode::K_OK`` 表示查询成功。
            - 返回 ``StatusCode::K_INVALID`` 表示提供的键中包含非法字符或为空。
            - 返回 ``StatusCode::K_RPC_UNAVAILABLE`` 表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_NOT_READY`` 表示服务当前无法处理请求。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。

    .. cpp:function:: Status Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds, std::vector<std::string> &failedKeys)

        批量为一组键（keys）更新过期生命周期（ttlSeconds），并返回更新失败的键（failedKeys）。最多支持 10000 个键的查询。

        参数：
            - **keys** - 待更新生命周期的键列表。
            - **ttlSeconds** - 为键设置的新的生命周期，单位为秒。
            - **failedKeys** - 传出参数，返回操作失败的键。

        返回：
            - 返回 ``StatusCode::K_OK`` 表示至少有一个键设置生命周期成功。
            - 返回 ``StatusCode::K_INVALID`` 表示提供的键中包含非法字符或为空。
            - 返回 ``StatusCode::K_NOT_FOUND`` 表示所有 ``keys`` 不存在。
            - 返回 ``StatusCode::K_RPC_UNAVAILABLE`` 表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_NOT_READY`` 表示服务当前无法处理请求。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。