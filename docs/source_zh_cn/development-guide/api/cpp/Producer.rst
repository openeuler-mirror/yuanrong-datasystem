Producer
====================

.. cpp:class:: Producer

    :header-file: #include <datasystem/stream/producer.h>
    :namespace: datasystem

    流缓存生产者。

    **公共函数**
 
    .. cpp:function:: Producer()
 
       构造流缓存生产者实例。
 
       返回：
           流缓存生产者实例。

    .. cpp:function:: ~Producer()
 
       析构流缓存生产者实例，析构过程中会自动断开与 Worker 的连接，释放流缓存生产者持有的资源。
 
    .. cpp:function:: Status Send(const Element &element)
 
       Producer发送数据，数据会首先放入缓冲区中，根据创建生产者时配置的自动Flush策略 (发送间隔一段时间或者缓冲区写满) 去刷新缓冲区让消费者可以访问到。

       参数：
            - **element** - 需要发送的Element数据。详见 cpp:class:`Element`。
 
       返回：
           返回值状态码为 StatusCode::K_OK 时表示发送数据成功，否则返回其他错误码。

    .. cpp:function:: Status Send(const Element &element, int64_t timeoutMs)

        Producer发送数据，数据会首先放入缓冲中，根据创建生产者时配置的自动Flush策略 (发送间隔一段时间或者缓冲写满) 去刷新缓冲区让消费者可以访问到。

        参数：
            - **Element** - 需要发送的Element数据。详见 cpp:class:`Element`。
            - **timeoutMs** - 接口超时时间， 单位ms。值为0时如果发送失败，将立刻返回错误原因。大于0时将会阻塞，等待完成。如果等待时间超过该值，则停止等待，并返回错误原因。

        返回:
            返回值状态码为 StatusCode::K_OK 时表示发送成功，否则返回其他错误码。


    .. cpp:function:: Status Close()

        关闭生产者。一旦关闭后，生产者不可再用。

        返回：
            返回值状态码为 `K_OK` 时表示关闭成功，否则返回其他错误码。