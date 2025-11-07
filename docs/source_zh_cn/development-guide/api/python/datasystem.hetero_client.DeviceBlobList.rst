datasystem.hetero_client.DeviceBlobList
=======================================

.. py:class:: datasystem.hetero_client.DeviceBlobList(dev_idx, blob_list)

    描述 device 上的一组内存，每段内存的信息存放在 :class:`datasystem.hetero_client.Blob` 中。

    参数：
        - **dev_idx** (int) - 表示 device 内存归属的 NPU 卡的 id。
        - **blob_list** (list) - 用于存放多个 :class:`datasystem.hetero_client.Blob`。

    输出：
        DeviceBlobList

    **方法**：

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`append_blob <datasystem.hetero_client.DeviceBlobList.append_blob>`
         - 添加一个  :class:`datasystem.hetero_client.Blob`， 即一段 Device 内存信息。
       * - :doc:`set_dev_idx <datasystem.hetero_client.DeviceBlobList.set_dev_idx>`
         - 指定 DeviceBlobList 中内存归属的 NPU 卡的 ID。
       * - :doc:`get_blob_list <datasystem.hetero_client.DeviceBlobList.get_blob_list>`
         - 获取 :class:`datasystem.hetero_client.Blob` 列表。

.. toctree::
    :maxdepth: 1
    :hidden:

    datasystem.hetero_client.DeviceBlobList.append_blob
    datasystem.hetero_client.DeviceBlobList.set_dev_idx
    datasystem.hetero_client.DeviceBlobList.get_blob_list
