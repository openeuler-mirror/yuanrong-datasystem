yr.datasystem.hetero_client.Future.get
======================================

.. py:method:: yr.datasystem.hetero_client.Future.get(sub_timeout_ms=60000)

    获取异步任务的执行结果。

    参数：
        - **sub_timeout_ms** (int) - 描述等待时长，默认值为 60000 ms。如果 sub_timeout_ms 大于 0， 则阻塞直到超时或结果变为可用；如果 sub_timeout_ms 等于 0，则立即返回结果状态。

    异常：
        - **RuntimeError** - 异步任务执行失败。
