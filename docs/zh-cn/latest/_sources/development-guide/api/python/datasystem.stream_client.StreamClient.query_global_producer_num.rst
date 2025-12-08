datasystem.stream_client.StreamClient.query_global_producer_num
==================================================================

.. py:method:: datasystem.stream_client.StreamClient.query_global_producer_num(stream_name)
    指定流的名称，查询流的生产者数量。

    参数：
        - **stream_name** (str) - 指定流的名称。

    返回：
        global_producer_num(int): 生产者的数量
        
    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 查询生产者数量失败。