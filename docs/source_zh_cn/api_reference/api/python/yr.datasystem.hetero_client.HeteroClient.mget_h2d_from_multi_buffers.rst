yr.datasystem.hetero_client.HeteroClient.mget_h2d_from_multi_buffers
====================================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.mget_h2d_from_multi_buffers(keys, addrs, sizes, sub_timeout_ms=0)

    从 host 获取多个对象，并将每个对象直接拆分写入同一 device 上的多个目标内存地址。

    该接口与 :func:`yr.datasystem.hetero_client.HeteroClient.mget_h2d` 的传输和错误语义一致，但直接接收二维地址和大小列表，不需要调用方构造 ``Blob`` 和 ``DeviceBlobList`` 对象。

    目标地址所属的 device 由调用线程当前绑定的 device context（例如 vLLM rank worker 事先绑定的 ACL context）现查得到，无需调用方传入；但单次调用内的所有地址仍须位于同一 device 上。

    参数：
        - **keys** (list) - host 中的 key 列表，数量不能超过 30000。
        - **addrs** (list) - 二维目标 device 地址列表，``addrs[i]`` 对应 ``keys[i]``。
        - **sizes** (list) - 与 ``addrs`` 一一对应的二维内存大小列表，单位为字节。
        - **sub_timeout_ms** (int) - 子请求超时时间，单位为毫秒，默认值为 ``0``。

    返回：
        - **failed_keys** (list) - 获取失败的 key 列表。

    异常：
        - **TypeError** - 输入参数类型不正确。
        - **ValueError** - ``keys``、``addrs``、``sizes`` 的维度不匹配。
        - **RuntimeError** - 无法现查当前 device（线程未绑定 device context），或原生 MGetH2D 请求失败。

