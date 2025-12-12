Consumer
====================

.. cpp:class:: Consumer

    :header-file: #include <datasystem/stream/consumer.h>
    :namespace: datasystem

    流缓存消费者。

    **公共函数**
 
    .. cpp:function:: Consumer()
 
       构造流缓存消费者实例。注：consumer对象并非线程安全，所以当有多个线程尝试调用同一个consumer做操作时会返回 ``K_SC_STREAM_IN_USE`` 错误码。
 
       返回：
           流缓存消费者实例。

    .. cpp:function:: ~Consumer()
 
       析构流缓存消费者实例，析构过程中会自动断开与 Worker 的连接，释放消费者持有的资源。

    .. cpp:function:: Status Receive(uint32_t expectNum, uint32_t timeoutMs, std::vector<Element> &outElements)

        消费者接收数据带有订阅功能，接收数据会等待接收expectNum个elements的时候返回成功，或者当超时时间timeoutMs到达返回成功。

        参数：
            - **expectNum** - 期望接收elements的个数。
            - **timeoutMs** - 超时时间， 单位ms, 在超时时间内未收到期望个数的element时, 接口返回 ``StatusCode::K_OK``
            - **outElements** - 实际接收到的elements，详见 :cpp:class:`Element` 章节。

        返回:
            返回值状态码为 ``StatusCode::K_OK`` 时表示接收成功，否则返回其他错误码。


    .. cpp:function:: Status Receive(uint32_t timeoutMs, std::vector<Element> &outElements)

        消费者获取到element后立刻返回。如果没有element，将等待直到超时时间到达。

        参数：
            - **timeoutMs** - 超时时间， 单位ms, 在超时时间内未收到期望个数的element时, 接口返回K_OK
            - **outElements** - 实际接收到的elements，详见 :cpp:class:`Element` 章节。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示接收成功，否则返回其他错误码。

    .. cpp:function:: Status Ack(uint64_t elementId)

        消费者接收完某elementId标识的element后，需要确认已消费完，使得各个worker上可以获取到是否所有消费者都已经消费完的信息，若所有消费者都消费完某个Page， 可以触发内部的内存回收机制。若不Ack，则在消费者退出时候才会自动Ack。

        参数：
            - **elementId** - 待确认已消费完成的element的id 。

        返回值
            返回值状态码为 ``StatusCode::K_OK`` 时表示确认成功，否则返回其他错误码。

    .. cpp:function:: Status Close()

        关闭消费者，关闭消费者后，它将不再允许调用receive和ack。对已关闭的消费者调用 Close() 方法将返回 ``StatusCode::K_OK``。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示设置成功，否则返回其他错误码。

    .. cpp:function:: Status GetStatisticsMessage(uint64_t &totalElements, uint64_t &notProcessedElements)

        获取自此消费者构造以来已接收的element的数量，以及未处理的element的数量。

        参数： 
            - **totalElements** - 消费者构造以来接收的element的数量。
            - **notProcessedElements** - 消费者未处理的element的数量。

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示获取成功，否则返回其他错误码。