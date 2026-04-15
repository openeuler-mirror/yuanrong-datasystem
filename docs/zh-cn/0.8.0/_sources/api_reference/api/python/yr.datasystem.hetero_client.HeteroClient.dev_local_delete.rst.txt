yr.datasystem.hetero_client.HeteroClient.dev_local_delete
=========================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.dev_local_delete(keys)

    从数据系统删除本节点上此 key 的元数据，不再管理此 key 对应的 device 内存。

    dev_local_delete 与 dev_mset / dev_mget 接口配套使用。

    参数：
        - **keys** (list) - device 的异构对象的 key。约束：传入的key的数量 `<=10000`。

    返回：
        - **failed_keys** (list) - 用于描述 delete 失败的 key。

    异常：
        - **TypeError** - 输入参数存在非法值。
