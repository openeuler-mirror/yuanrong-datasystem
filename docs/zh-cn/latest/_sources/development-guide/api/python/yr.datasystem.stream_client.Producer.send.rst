yr.datasystem.stream_client.Producer.send
==================================================================

.. py:method:: yr.datasystem.stream_client.Producer.send(element_bytes, timeout_ms)

    生产者发送数据。

    参数：
        - **element_bytes** (bytes) - 要发送的数据。
        - **timeout_ms** (int) - 接口超时时间， 单位ms。值为0时如果发送失败，将立刻返回错误原因。大于0时将会阻塞，等待完成。如果等待时间超过该值，则停止等待，并返回错误原因。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 发送失败。