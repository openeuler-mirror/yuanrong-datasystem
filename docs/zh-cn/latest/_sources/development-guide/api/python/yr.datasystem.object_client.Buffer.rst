yr.datasystem.object_client.Buffer
==================================

.. py:class:: yr.datasystem.object_client.Buffer

    对象共享内存缓存。

    输出：
        Buffer。

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`wlatch <yr.datasystem.object_client.Buffer.wlatch>`
         - 对共享内存Buffer加写锁。
       * - :doc:`unwlatch <yr.datasystem.object_client.Buffer.unwlatch>`
         - 对共享内存Buffer解写锁。
       * - :doc:`rlatch <yr.datasystem.object_client.Buffer.rlatch>`
         - 对共享内存Buffer加读锁。
       * - :doc:`unrlatch <yr.datasystem.object_client.Buffer.unrlatch>`
         - 对共享内存Buffer解读锁。
       * - :doc:`mutable_data <yr.datasystem.object_client.Buffer.mutable_data>`
         - 获取Buffer的可读写 `memoryview`。
       * - :doc:`immutable_data <yr.datasystem.object_client.Buffer.immutable_data>`
         - 获取Buffer的只读 `memoryview`。
       * - :doc:`memory_copy <yr.datasystem.object_client.Buffer.memory_copy>`
         - 将 `value` 中的数据拷贝到Buffer中
       * - :doc:`publish <yr.datasystem.object_client.Buffer.publish>`
         - 将Buffer中的数据发布到数据系统中。
       * - :doc:`seal <yr.datasystem.object_client.Buffer.seal>`
         - 将Buffer中的数据发布到数据系统中，发布成功之后Buffer所对应的对象的值将无法再被修改。
       * - :doc:`invalidate_buffer <yr.datasystem.object_client.Buffer.invalidate_buffer>`
         - 使当前主机上的Buffer数据无效化。
       * - :doc:`get_size <yr.datasystem.object_client.Buffer.get_size>`
         - 获取对象buffer的大小。
       
.. toctree::
    :maxdepth: 1
    :hidden:

    yr.datasystem.object_client.Buffer.wlatch
    yr.datasystem.object_client.Buffer.unwlatch
    yr.datasystem.object_client.Buffer.rlatch
    yr.datasystem.object_client.Buffer.unrlatch
    yr.datasystem.object_client.Buffer.mutable_data
    yr.datasystem.object_client.Buffer.immutable_data
    yr.datasystem.object_client.Buffer.memory_copy
    yr.datasystem.object_client.Buffer.publish
    yr.datasystem.object_client.Buffer.seal
    yr.datasystem.object_client.Buffer.invalidate_buffer
    yr.datasystem.object_client.Buffer.get_size