datasystem.hetero_client.DeviceBlobList
=======================================

.. py:class:: datasystem.hetero_client.DeviceBlobList(dev_idx, blob_list)

    描述 device 上的一组内存，每段内存的信息存放在 :class:`datasystem.hetero_client.Blob` 中。

    参数：
        - **dev_idx** (int) - 表示 device 内存归属的 NPU 卡的 id。
        - **blob_list** (list) - 用于存放多个 :class:`datasystem.hetero_client.Blob`。

    输出：
        DeviceBlobList

.. dscnautosummary::
    :toctree: DeviceBlobList
    :nosignatures:

    datasystem.hetero_client.DeviceBlobList.append_blob
    datasystem.hetero_client.DeviceBlobList.set_dev_idx
    datasystem.hetero_client.DeviceBlobList.get_blob_list
