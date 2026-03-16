datasystem.stream_client.Producer.close
==================================================================

.. py:method:: datasystem.stream_client.Producer.close()

    关闭生产者。一旦关闭后，生产者不可再用。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 关闭生产者失败。