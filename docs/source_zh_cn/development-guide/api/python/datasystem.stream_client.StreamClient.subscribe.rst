datasystem.stream_client.StreamClient.subscribe
==================================================================

.. py:method:: datasystem.stream_client.StreamClient.subscribe(stream_name, sub_name, subscription_type)

    创建消费者，创建消费者时会创建流。

    参数：
        - **stream_name** (str) - 指定流的名称， 用于创建消费者。
        - **sub_name** (str) - 订阅名称。。
        - **subscription_type** (int), 流缓存消费者订阅模式。
    
    返回：
        outConsumer(:class:`datasystem.stream_client.Consumer`):消费者实例。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 创建生产者失败。