yr.datasystem.kv_client.KVClient.health_check
======================================

.. py:method:: yr.datasystem.kv_client.KVClient.health_check(self)

    检测连接的 Worker 的健康状态。

    返回：
        **Status**。返回 Status ，内含状态码及详细信息，可通过 **.is_ok()** 来确认是否健康。