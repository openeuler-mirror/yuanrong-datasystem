datasystem.hetero_client.Blob
=============================

.. py:class:: datasystem.hetero_client.Blob(dev_ptr, size)

    用于描述 Device NPU 卡上的一段内存。

    参数：
        - **dev_ptr** (int) - 用于描述 Device 上的内存指针。
        - **size** (list) - 用于描述内存的大小。

    输出：
        Blob

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`set_dev_ptr <datasystem.hetero_client.Blob.set_dev_ptr>`
         - 指定 Device 内存指针地址。
       * - :doc:`set_size <datasystem.hetero_client.Blob.set_size>`
         - 指定 Device 内存地址空间大小。

.. toctree::
    :maxdepth: 1
    :hidden:

    datasystem.hetero_client.Blob.set_dev_ptr
    datasystem.hetero_client.Blob.set_size
