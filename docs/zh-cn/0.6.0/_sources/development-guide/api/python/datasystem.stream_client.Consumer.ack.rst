datasystem.stream_client.Consumer.close
=========================================

.. py:method:: stream_client.Consumer.ack(element_id)

    消费者接收完某elementId标识的element后，需要确认已消费完，使得各个worker上可以获取到是否所有消费者都已经消费完的信息，若所有消费者都消费完某个Page， 可以触发内部的内存回收机制。若不Ack，则在消费者退出时候才会自动Ack。

    参数：
        - **element_id** (str) - 待确认已消费完成的element的id 。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 关闭生产者失败。