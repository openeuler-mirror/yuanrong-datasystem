yr.datasystem.kv_client.KVClient.get_buffers
=====================================

.. py:method:: yr.datasystem.kv_client.KVClient.get_buffers(keys=None, timeout_ms=0)

    获取所有给定键的值。

    和 `get_read_only_buffers` 方法不同的是，该方法返回的是 `ReadOnlyBuffer` 对象列表，该对象的生命周期内始终持有读锁，数据系统不可修改。
    而 `get_read_only_buffers` 方法返回的是 `StateValueBuffer` 对象列表，用户可以通过调用方法 rlatch() 和 unrlatch() 自定义读锁的范围。

    参数：
        - **keys** (list) - 字符串类型的键列表。约束：传入的key的数量不能超过1万。
        - **timeout_ms** (int) - 等待结果返回的超时时间，单位为毫秒。

    返回：
        `ReadOnlyBuffer` 对象列表。该buffer生命周期内始终持有读锁，数据系统不可修改。

    异常：
        - **RuntimeError** - 如果获取所有键的值失败，将抛出运行时错误。
        - **TypeError** - 如果输入参数无效，将抛出类型错误。