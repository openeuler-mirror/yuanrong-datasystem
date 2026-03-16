ObjectClient
====================

.. cpp:class:: ObjectClient

    :header-file: #include <datasystem/object_client.h>
    :namespace: datasystem

    Object缓存客户端。

    **公共函数**
 
    .. cpp:function:: ObjectClient(const ConnectOptions &connectOptions)
 
       构造Object缓存客户端实例。

       参数：
            - **connectOptions** - 配置连接选项，包括IP地址和端口，详见 :cpp:class:`ConnectOptions` 章节
 
       返回：
           Object缓存客户端实例。

    .. cpp:function:: ~ObjectClient()
 
       析构Object缓存客户端实例，析构过程中会自动断开与 Worker 的连接，释放客户端持有的资源。
 
    .. cpp:function:: Status Init()
 
       建立与数据系统 Worker 之间的连接并完成初始化。
 
       返回：
           返回值状态码为 ``StatusCode::K_OK`` 时表示初始化成功，否则返回其他错误码。

    .. cpp:function:: Status ShutDown()

        断开与数据系统 Worker 之间的连接。

        返回:
            返回值状态码为 ``StatusCode::K_OK`` 时表示断链成功，否则返回其他错误码。

    .. cpp:function:: Status GIncreaseRef(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)

        为指定的对象的全局引用计数加1。推荐在create对象之前为对象增加引用计数，因为在跨节点访问场景中，未进行引用计数的对象可能刚创造就会被驱逐。

        参数：
            - **objectKeys** - 需要增加引用计数的一组对象名称，最多不超过10000个。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **failedObjectKeys** - 传出参数，如果引用计数增加失败了，会返回失败的对象名称。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示加引用计数成功，否则返回其他错误码。

    .. cpp:function:: Status GDecreaseRef(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)

        为指定的对象的全局引用计数减1。全局引用计数为0时，删除对象。

        参数：
            - **objectKeys** - 需要减少引用计数的一组对象名称，最多不超过10000个。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **failedObjectKeys** - 传出参数，如果引用计数减少失败了，会返回失败的对象名称。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示减引用计数成功，否则返回其他错误码。

    .. cpp:function:: int QueryGlobalRefNum(const std::string &objectKey)

        获取指定对象的全局引用计数数量。

        参数：
            - **objectKey** - 需要获取全局引用计数的对象名称名称。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。

        返回：
            返回全局引用计数的值。

    .. cpp:function:: Status Create(const std::string &objectKey, uint64_t size, const CreateParam &param, std::shared_ptr<Buffer> &buffer)

        创建数据系统共享内存Buffer，可以将数据拷贝到Buffer中，再调用Set接口缓存到数据系统中。该接口应用于避免创建临时内存，减少内存拷贝的场景。

        参数：
            - **objectKey** - 需要设置的对象名称。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **size** - 需要创建的共享内存Buffer的大小，以字节为单位。
            - **param** - 设置参数，详见 :cpp:class:`CreateParam` 章节。
            - **buffer** - 传出参数，表示创建好的共享内存 :cpp:class:`Buffer` 。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示创建成功，否则返回其他错误码。

    .. cpp:function:: Status Put(const std::string &objectKey, const uint8_t *data, uint64_t size, const CreateParam &param, const std::unordered_set<std::string> &nestedObjectKeys = {})

        设置对象数据缓存到数据系统。

        参数：
            - **objectKey** - 需要设置对象名称. 对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个对象名称最大长度为255字节。
            - **data** - 传入参数 缓存到数据系统的数据的指针。
            - **size** - 传入参数 缓存到数据系统的数据大小，以字节为单位。
            - **param** - 设置参数，详见 :cpp:class:`CreateParam` 章节。
            - **nestedObjectKeys** - 传入参数，嵌套对象id。用于表示nestedIds中的对象会被objectId所关联的对象所使用，如果objectId所关联的对象未被删除时，nestedIds中所有已创建的对象也会一直被系统持有，直到objectId所关联的对象被删除，才会开始对nestedIds的对象的生命周期状态进行改变。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status Get(const std::vector<std::string> &objectKeys, int32_t subTimeoutMs, std::vector<Optional<Buffer>> &buffers)


        获取一个或多个Buffer内存块，从而通过Buffer操作指针读写数据。

        参数：
            - **objectKeys** - 对象名称. 需要获取buffer的一组对象名称，最多不超过10000个。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个对象名称最大长度为255字节。
            - **subTimeoutMs** - 支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。
            - **buffer** - 出参数，返回的一组使用 :cpp:class:`Optional` 封装的共享内存 :cpp:class:`Buffer` 。若有部分数据获取不成功，则对应位置的vector的对象为空。

        返回：
            - 返回 ``StatusCode::K_OK`` 表示获取成功。
            - 返回 ``StatusCode::K_INVALID`` 表示对象名称校验不通过。
            - 返回 ``StatusCode::K_NOT_FOUND`` 表示对象名称不存在。
            - 返回 ``StatusCode::K_RPC_UNAVAILABLE`` 时表示请求遇到了网络错误。
            - 返回 ``StatusCode::K_RUNTIME_ERROR`` 表示 worker 侧存在错误。


    .. cpp:function:: Status GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys, std::vector<ObjMetaInfo> &objMetas)

        获取指定租户的一组对象的大小和位置。

        参数：
            - **tenantId** - 传入参数，指定的租户id.
            - **objectKeys** - 待查询的对象列表，最大支持10000个键， 对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;`` ，单个对象名称最大长度为255字节。
            - **objMetas** - 传出参数，返回每个对象的数据大小和存储的Worker节点信息 :cpp:class:`objMetaInfo`。

        返回：
           返回值状态码为 ``StatusCode::K_OK`` 时表示获取成功，否则返回其他错误码。

    .. cpp:function:: Status GenerateKey(const std::string &prefix, std::string &key)
    
        生成一个唯一的对象名称，提供给Put接口使用。
        
        参数：
            - **prefix** - 传入参数， 指定返回生成的对象名称的前缀。
            - **key** - 传出参数，支持订阅不存在的数据，subTimeoutMs表示订阅等待的时长，单位ms。不允许为负数，默认值为0表示不等待。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示生成成功，否则返回其他错误码。

    .. cpp:function:: Status GetPrefix(const std::string &key, std::string &prefix)

        获取指定对象名称的前缀。

        参数：
            - **key** - 传入参数，需要获取前缀的对象名称。
            - **prefix** - 传出参数， 该对象名称的前缀。
        
        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示获取成功，否则返回其他错误码。
    
    .. cpp:function:: Status HealthCheck()

        检查连接的 Worker 是否健康。
        
        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示 Worker 健康，否则返回其他错误码。