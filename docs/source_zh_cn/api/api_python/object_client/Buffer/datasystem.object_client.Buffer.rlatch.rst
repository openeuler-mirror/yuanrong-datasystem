datasystem.object_client.Buffer.rlatch
======================================

.. py:method:: datasystem.object_client.Buffer.rlatch(timeout_sec=60)

    对对象Buffer加读锁。

    参数：
        - **timeout_sec** (int) - 尝试加锁的最大等待时间（单位为秒）。默认值： ``60`` 。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 加读锁失败。