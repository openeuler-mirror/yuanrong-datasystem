yr.datasystem.hetero_client.HeteroClient.mset_d2h_from_multi_buffers
====================================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.mset_d2h_from_multi_buffers(keys, addrs, sizes, set_param=SetParam())

    从同一 device 的多个源内存地址读取数据，将每个 key 对应的多个 buffer 拼接后写入 host 对象。

    该接口与 :func:`yr.datasystem.hetero_client.HeteroClient.mset_d2h` 的写入和错误语义一致，但直接接收二维地址和大小列表，不需要调用方构造 ``Blob`` 和 ``DeviceBlobList`` 对象。

    源地址所属的 device 由调用线程当前绑定的 device context（例如 vLLM rank worker 事先绑定的 ACL context）现查得到，无需调用方传入；但单次调用内的所有地址仍须位于同一 device 上。

    参数：
        - **keys** (list) - 待写入 host 的 key 列表，数量不能超过 30000。
        - **addrs** (list) - 二维源 device 地址列表，``addrs[i]`` 对应 ``keys[i]``。
        - **sizes** (list) - 与 ``addrs`` 一一对应的二维内存大小列表，单位为字节。
        - **set_param** (:class:`yr.datasystem.kv_client.SetParam`) - 写入模式、存在性条件、TTL 和缓存类型，默认使用 ``SetParam()``。

    异常：
        - **TypeError** - 输入参数类型不正确。
        - **ValueError** - ``keys``、``addrs``、``sizes`` 的维度不匹配。
        - **RuntimeError** - 无法现查当前 device（线程未绑定 device context），或原生 MSetD2H 请求失败。

