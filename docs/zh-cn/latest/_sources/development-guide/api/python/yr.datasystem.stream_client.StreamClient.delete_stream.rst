yr.datasystem.stream_client.StreamClient.delete_stream
==================================================================

.. py:method:: yr.datasystem.stream_client.StreamClient.delete_stream(stream_name)
    删除数据流。

    参数：
        - **stream_name** (str) - 指定流的名称，用于删除数据流。
        
    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 删除数据流失败。