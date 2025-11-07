datasystem.object_client.Buffer
===============================

.. py:class:: datasystem.object_client.Buffer

    对象共享内存缓存。

    输出：
        Buffer。

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`wlatch <datasystem.object_client.Buffer.wlatch>`
         - 对共享内存Buffer加写锁。
       * - :doc:`unwlatch <datasystem.object_client.Buffer.unwlatch>`
         - 对共享内存Buffer解写锁。
       * - :doc:`rlatch <datasystem.object_client.Buffer.rlatch>`
         - 对共享内存Buffer加读锁。
       * - :doc:`unrlatch <datasystem.object_client.Buffer.unrlatch>`
         - 对共享内存Buffer解读锁。
       * - :doc:`mutable_data <datasystem.object_client.Buffer.mutable_data>`
         - 获取Buffer的可读写 `memoryview`。
       * - :doc:`immutable_data <datasystem.object_client.Buffer.immutable_data>`
         - 获取Buffer的只读 `memoryview`。
       * - :doc:`memory_copy <datasystem.object_client.Buffer.memory_copy>`
         - 将 `value` 中的数据拷贝到Buffer中
       * - :doc:`publish <datasystem.object_client.Buffer.publish>`
         - 将Buffer中的数据发布到数据系统中。
       * - :doc:`seal <datasystem.object_client.Buffer.seal>`
         - 将Buffer中的数据发布到数据系统中，发布成功之后Buffer所对应的对象的值将无法再被修改。
       * - :doc:`invalidate_buffer <datasystem.object_client.Buffer.invalidate_buffer>`
         - 使当前主机上的Buffer数据无效化。
       * - :doc:`get_size <datasystem.object_client.Buffer.get_size>`
         - 获取对象buffer的大小。
       
.. toctree::
    :maxdepth: 1
    :hidden:

    datasystem.object_client.Buffer.wlatch
    datasystem.object_client.Buffer.unwlatch
    datasystem.object_client.Buffer.rlatch
    datasystem.object_client.Buffer.unrlatch
    datasystem.object_client.Buffer.mutable_data
    datasystem.object_client.Buffer.immutable_data
    datasystem.object_client.Buffer.memory_copy
    datasystem.object_client.Buffer.publish
    datasystem.object_client.Buffer.seal
    datasystem.object_client.Buffer.invalidate_buffer
    datasystem.object_client.Buffer.get_size