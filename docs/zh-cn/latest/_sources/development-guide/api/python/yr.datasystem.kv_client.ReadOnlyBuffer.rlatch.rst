yr.datasystem.kv_client.ReadOnlyBuffer.rlatch
=============================================

.. py:method:: yr.datasystem.kv_client.ReadOnlyBuffer.rlatch(timeout_sec=60)

    获取读锁以保护缓冲区免受并发写操作。

    参数：
        - **timeout_sec** (int) - 尝试获取锁的超时时间，单位为秒，默认值为60秒。

    异常：
        - **TypeError** - 如果输入参数无效，将抛出类型错误。
        - **RuntimeError** - 如果获取读锁失败，将抛出运行时错误。