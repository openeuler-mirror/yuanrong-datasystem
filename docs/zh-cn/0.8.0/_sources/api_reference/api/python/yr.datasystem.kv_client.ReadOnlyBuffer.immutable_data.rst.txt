yr.datasystem.kv_client.ReadOnlyBuffer.immutable_data
=====================================================

.. py:method:: yr.datasystem.kv_client.ReadOnlyBuffer.immutable_data(with_latch=False, timeout_sec=60)

    获取一个不可变的数据内存视图。

    参数：
        - **with_latch** (bool) - 是否在获取缓冲区之前获取锁。
        - **timeout_sec** (int) - 尝试获取锁的超时时间，单位为秒，默认值为60秒。

    返回：
        缓冲区的不可变内存视图。

    异常：
        - **TypeError** - 如果输入参数无效，将抛出类型错误。
        - **RuntimeError** - 如果获取读锁失败，将抛出运行时错误。
