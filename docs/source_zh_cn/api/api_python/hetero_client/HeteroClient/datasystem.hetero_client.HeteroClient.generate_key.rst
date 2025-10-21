datasystem.hetero_client.HeteroClient.generate_key
==================================================

.. py:method:: datasystem.hetero_client.HeteroClient.generate_key(prefix)

    生成一个带数据系统 Worker UUID 的 key。

    参数：
        - **prefix** (str) - key 的前缀。

    返回：
        - **key** (string) - 生成的 key。

    异常：
        - **RuntimeError** - 给定列表的对象 key 都未获取成功。