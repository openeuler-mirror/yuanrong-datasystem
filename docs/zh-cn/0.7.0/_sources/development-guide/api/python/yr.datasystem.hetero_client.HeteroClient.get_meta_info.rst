yr.datasystem.hetero_client.HeteroClient.get_meta_info
======================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.get_meta_info(self, keys, is_dev_key)

    获取keys 对应的元数据信息。

    参数：
        - **keys** (list) - host 的 key 列表。约束：传入的key的数量 `<=10000`。
        - **is_dev_key** (bool) - key的属性，true 表示D2D类型，false表示D2H类型。
    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 给定列表的对象 key 都未执行成功。

    返回：
        - **failed_keys** (list) - 失败的key列表。
        - **meta_infos** (list) - 获取到的元信息(MetaInfo)列表。