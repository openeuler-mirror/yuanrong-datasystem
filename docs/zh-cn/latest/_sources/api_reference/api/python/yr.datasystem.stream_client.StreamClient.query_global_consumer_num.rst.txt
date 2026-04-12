yr.datasystem.stream_client.StreamClient.query_global_consumer_num
==================================================================

.. py:method:: yr.datasystem.stream_client.StreamClient.query_global_consumer_num(stream_name)
    指定流的名称，查询流的消费者数量。

    参数：
        - **stream_name** (str) - 指定流的名称。

    返回：
        global_consumer_num(int): 消费者的数量
        
    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 查询消费者数量失败。