StreamClient
====================

.. cpp:class:: StreamClient

    :header-file: #include <datasystem/stream_client.h>
    :namespace: datasystem

    Stream缓存客户端。

    **公共函数**
 
    .. cpp:function:: StreamClient(const ConnectOptions connectOptions)
 
       构造Stream缓存客户端实例。

       参数：
            - **connectOptions** - 配置连接选项，包括IP地址和端口，详见 :cpp:class:`ConnectOptions` 章节
 
       返回：
           Stream缓存客户端实例。

    .. cpp:function:: ~StreamClient()
 
       析构Stream缓存客户端实例，析构过程中会自动断开与 Worker 的连接，释放客户端持有的资源。
 
    .. cpp:function:: Status Init(bool reportWorkerLost = false)
 
       建立与数据系统 Worker 之间的连接并完成初始化。

       参数：
            - **reportWorkerLost** - 当client与worker断连时是否向用户返回 `K_SC_WORKER_WAS_LOST` 错误码。默认为false。
 
       返回：
           返回值状态码为 ``StatusCode::K_OK`` 时表示初始化成功，否则返回其他错误码。

    .. cpp:function:: Status ShutDown()

        断开与数据系统 Worker 之间的连接。

        返回:
            返回值状态码为 ``StatusCode::K_OK`` 时表示断链成功，否则返回其他错误码。


    .. cpp:function:: Status CreateProducer(const std::string &streamName, std::shared_ptr<Producer> &outProducer, ProducerConf producerConf = {})

        创建生产者, 创建生产者时会创建流。

        参数：
            - **streamName** - 指定流的名称， 用于创建生产者。 `streamName` 的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **outProducer** - 传出参数，返回 std::shared_ptr< :cpp:class:`Producer` > 用于生产数据。
            - **producerConf** - 设置参数，详见 :cpp:class:`ProducerConf` 章节。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示创建成功，否则返回其他错误码。

    .. cpp:function:: Status Subscribe(const std::string &streamName, const struct SubscriptionConfig &config, std::shared_ptr<Consumer> &outConsumer, bool autoAck = false)

        创建消费者，创建消费者时会创建流。

        参数：
            - **streamName** - 指定流的名称， 用于创建消费者。 `streamName` 的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **config** - 设置参数，详见 :cpp:class:`SubscriptionConfig` 。
            - **outConsumer** - 传出参数，返回 std::shared_ptr< :cpp:class:`Consumer` > 用于消费数据。
            - **autoAck** - 设置参数，是否支持自动 :cpp:func:`Consumer::Ack` 功能。

        返回值
            返回值状态码为 ``StatusCode::K_OK`` 时表示创建成功，否则返回其他错误码。

    .. cpp:function:: Status DeleteStream(const std::string &streamName)

        删除数据流，用于删除数据流。。

        参数：
            - **streamName** - 键，key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节.

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示删除成功，否则返回其他错误码。

    .. cpp:function:: Status QueryGlobalProducersNum(const std::string &streamName, uint64_t &gProducerNum)

        指定流的名称，查询流的生产者数量。

        参数： 
            - **streamName** - 指定流的名称，查询该条流有多少个生产者。 `streamName` 的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **gProducerNum** - 传出参数， 指定 `streamName` 对应的生产者的数量。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示查询成功，否则返回其他错误码。

    .. cpp:function:: Status QueryGlobalConsumersNum(const std::string &streamName, uint64_t &gConsumerNum)

        指定流的名称，查询流的消费者数量。

        参数：
            - **streamName** - 指定流的名称，查询该条流有多少消费者。 `streamName` 的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
            - **gConsumerNum** - 传出参数， 指定 `streamName` 对应的消费者的数量。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示查询成功，否则返回其他错误码。