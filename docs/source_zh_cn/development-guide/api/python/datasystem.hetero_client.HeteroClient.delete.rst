datasystem.hetero_client.HeteroClient.delete
============================================

.. py:method:: datasystem.hetero_client.HeteroClient.delete(keys)

    删除 host 中的 key。delete 接口与 mget_h2d / mset_d2h 配套使用。

    .. note::
        删除不存在的 key 也认为删除成功。

    参数：
        - **keys** (list) - host 的 key 列表。约束：传入的key的数量不能超过1万。

    返回：
        - **failed_keys** (list) - 用于描述 delete 失败的 key。

    异常：
        - **TypeError** - 输入参数存在非法值。
