datasystem.stream_client.Consumer.close
=========================================

.. py:method:: stream_client.Consumer.close()

    关闭消费者。一旦关闭后，消费者不可再用。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 关闭消费者失败。