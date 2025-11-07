datasystem.object_client.Buffer.wlatch
======================================

.. py:method:: datasystem.object_client.Buffer.wlatch(timeout_sec=60)

    对共享内存Buffer加写锁。

    参数：
        - **timeout_sec** (int) - 尝试加锁的最大等待时间（单位为秒）。默认值： ``60`` 。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 加写锁失败。