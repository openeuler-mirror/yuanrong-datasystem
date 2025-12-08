datasystem.kv_client.ReadOnlyBuffer
===================================

.. py:class:: datasystem.kv_client.ReadOnlyBuffer

    只读缓冲区结构类。

    输出：
        ReadOnlyBuffer。

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`immutable_data <datasystem.kv_client.ReadOnlyBuffer.immutable_data>`
         - 获取一个不可变的数据内存视图。
       * - :doc:`rlatch <datasystem.kv_client.ReadOnlyBuffer.rlatch>`
         - 获取读锁以保护缓冲区免受并发写操作。
       * - :doc:`unrlatch <datasystem.kv_client.ReadOnlyBuffer.unrlatch>`
         - 释放读锁。
       
.. toctree::
    :maxdepth: 1
    :hidden:

    datasystem.kv_client.ReadOnlyBuffer.immutable_data
    datasystem.kv_client.ReadOnlyBuffer.rlatch
    datasystem.kv_client.ReadOnlyBuffer.unrlatch