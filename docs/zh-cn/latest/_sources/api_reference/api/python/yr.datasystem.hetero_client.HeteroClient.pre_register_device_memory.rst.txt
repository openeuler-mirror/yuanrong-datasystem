yr.datasystem.hetero_client.HeteroClient.pre_register_device_memory
===================================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.pre_register_device_memory(dev_ptrs, sizes)

    为 RH2D over HIXL HCCS 预注册后续 MGetH2D 使用的 device 目标内存范围。

    该接口仅支持启用 RH2D 且链路类型为 HCCS 的场景。调用前需完成 ACL 初始化并设置当前 device，且当前 device 应与后续 MGetH2D 的 :class:`yr.datasystem.hetero_client.DeviceBlobList` 中的 device id 一致。

    预注册的内存句柄由 HeteroClient 管理，并在 client shutdown 或析构时释放。推荐预注册一块或少量大连续 HBM 目标池，再将后续 MGetH2D 的目标 Blob 分配到这些范围内，避免把大量离散小块逐个预注册。

    参数：
        - **dev_ptrs** (list) - device 内存范围的起始地址列表。
        - **sizes** (list) - device 内存范围大小列表，单位为字节，长度需与 ``dev_ptrs`` 一致。

    返回：
        None

    异常：
        - **TypeError** - 输入参数类型非法。
        - **RuntimeError** - 输入为空、长度不匹配，或当前场景不支持预注册。

    示例：

        >>> import acl
        >>> from yr.datasystem.hetero_client import HeteroClient
        >>> acl.init()
        >>> device_id = 0
        >>> acl.rt.set_device(device_id)
        >>> client = HeteroClient("127.0.0.1", 31501, enable_remote_h2d=True)
        >>> client.init()
        >>> dev_ptr, _ = acl.rt.malloc(1024 * 1024, 0)
        >>> client.pre_register_device_memory([dev_ptr], [1024 * 1024])
