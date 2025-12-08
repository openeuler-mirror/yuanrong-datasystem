DeviceBlobList
==========================

.. cpp:class:: DeviceBlobList

    :header-file: #include <datasystem/hetero/device_common.h>
    :namespace: datasystem

    描述用于接收或发送的异构设备内存结构的列表

    **公共成员**

    .. cpp:member:: std::vector<Blob> blobs

        描述接收或发送的异构设备内存结构的列表。详见 `:cpp:class:Blob` 章节

    .. cpp:member:: int32_t deviceIdx = -1

        异构设备的索引， 默认值-1

    .. cpp:member:: int32_t srcOffset = 0

        用于传输或获取指定范围数据的初始偏移值，默认值0.单位字节。