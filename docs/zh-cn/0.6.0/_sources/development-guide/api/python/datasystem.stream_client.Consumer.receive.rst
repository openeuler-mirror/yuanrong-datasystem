datasystem.stream_client.Consumer.receive
=========================================

.. py:method:: stream_client.Consumer.receive(expect_num, timeout_ms)

    消费者接收数据带有订阅功能，接收数据会等待接收expectNum个elements的时候返回成功，或者当超时时间timeoutMs到达返回成功。

    参数：
        - **expect_num** - 期望接收elements的个数。
        - **timeout_ms** - 超时时间， 单位ms, 在超时时间内未收到期望个数的element时, 接口返回K_OK。

    返回：
        接收到的数据

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 关闭生产者失败。