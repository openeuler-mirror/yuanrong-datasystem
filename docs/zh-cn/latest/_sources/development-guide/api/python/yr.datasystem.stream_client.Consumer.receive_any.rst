yr.datasystem.stream_client.Consumer.receive_any
================================================

.. py:method:: stream_client.Consumer.receive_any(timeout_ms)

    消费者获取到element后立刻返回。如果没有element，将等待直到超时时间到达。

    参数：
        - **timeout_ms** - 超时时间， 单位ms, 在超时时间内未收到期望个数的element时, 接口返回K_OK送失败，将立刻返回错误原因。大于0时将会阻塞，等待完成。如果等待时间超过该值，则停止等待，并返回错误原因。

    返回：
        接收到的数据

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 关闭生产者失败。