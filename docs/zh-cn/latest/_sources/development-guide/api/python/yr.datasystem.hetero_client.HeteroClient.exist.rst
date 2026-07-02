yr.datasystem.hetero_client.HeteroClient.exist
==============================================
 
.. py:method:: yr.datasystem.hetero_client.HeteroClient.exist(self, keys)
 
    检查给定的键在数据系统中是否存在。
 
    参数：
        - **keys** (list) - 待查询的键列表。约束：传入的key的数量 `<=10000`。
 
    返回：
        - **exists** (list) - 对应key的存在性。
 
    异常：
        - **TypeError** - 如果输入参数无效，将抛出类型错误。
        - **RuntimeError** - 如果查询键是否存在失败，将抛出运行时错误。