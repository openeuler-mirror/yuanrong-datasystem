datasystem.kv_client.KVClient.exist
===================================

.. py:method:: datasystem.kv_client.KVClient.exist(self, keys)

    批量查询一组键是否存在，并返回每个键的存在性状态。

    参数：
        - **keys** (str) - 待查询的键列表，最大支持10000个键。

    异常：
        - **RuntimeError** - 如果查询键是否存在失败，将抛出运行时错误。
        - **TypeError** - 如果输入参数无效，将抛出类型错误。
