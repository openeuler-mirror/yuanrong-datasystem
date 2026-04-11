yr.datasystem.stream_client.Consumer
=====================================

.. py:class:: yr.datasystem.stream_client.Consumer(consumer)

    流缓存消费者

    参数：
        - **consumer**： - 持有consumer指针的对象。

    输出：
        StreamClient

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`receive <yr.datasystem.stream_client.Consumer.receive>`
         - 消费者接收数据带有订阅功能，接收数据会等待接收expectNum个elements的时候返回成功，或者当超时时间timeoutMs到达返回成功。
       * - :doc:`receive_any <yr.datasystem.stream_client.Consumer.receive_any>`
         - 消费者获取到element后立刻返回。如果没有element，将等待直到超时时间到达。
       * - :doc:`ack <yr.datasystem.stream_client.Consumer.ack>`
         - 消费者接收完某elementId标识的element后，需要确认已消费完，使得各个worker上可以获取到是否所有消费者都已经消费完的信息，若所有消费者都消费完某个Page， 可以触发内部的内存回收机制。若不Ack，则在消费者退出时候才会自动Ack。
       * - :doc:`close <yr.datasystem.stream_client.Consumer.close>`
         - 关闭消费者，关闭消费者后，它将不再允许调用receive和ack。对已关闭的消费者调用 Close() 方法将返回 ``StatusCode::K_OK``。

.. toctree::
    :maxdepth: 1
    :hidden:

    yr.datasystem.stream_client.Consumer.receive
    yr.datasystem.stream_client.Consumer.receive_any
    yr.datasystem.stream_client.Consumer.ack
    yr.datasystem.stream_client.Consumer.close
