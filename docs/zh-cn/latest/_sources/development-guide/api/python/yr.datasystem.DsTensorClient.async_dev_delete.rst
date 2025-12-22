yr.datasystem.DsTensorClient.async_dev_delete
==============================================

.. py:method:: yr.datasystem.DsTensorClient.async_dev_delete(keys)

异步接口，从数据系统删除此 key 的元数据，不再管理此 key 对应的 device 内存。可通过返回的Future查询删除结果。

    async_dev_delete 与 dev_mset / dev_mget 接口配套使用。

    参数：
        - **keys** (list) - host 的 key 列表。约束：传入的key的数量不能超过1万。

    返回：
        - **Future** (Future) - 可通过该Future对象查询异步请求执行结果。当调用Future的get方法时，如果删除key存在部分失败，则返回失败的列表；如果全部失败，则抛出RuntimeError异常。

    异常：
        - **TypeError** - 输入参数存在非法值。