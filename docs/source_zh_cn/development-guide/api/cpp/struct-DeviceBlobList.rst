DeviceBlobList
==========================

.. cpp:class:: DeviceBlobList

    :header-file: #include <datasystem/hetero/device_common.h>
    :namespace: datasystem

    描述用于接收或发送的异构设备内存结构的列表

    **公共成员**

    .. cpp:member:: std::vector<Blob> blobs

        描述接收或发送的异构设备内存结构的列表。详见 :cpp:class:`Blob` 章节

    .. cpp:member:: int32_t deviceIdx = -1

        异构设备的索引， 默认值-1

    .. cpp:member:: int32_t srcOffset = 0

        发送端数据源地址的偏移量，用于接收端获取发送端指定范围的数据，默认值0，单位字节。该参数只用于D2D（Device-to-Device）场景下，使接收端可以灵活获取发送端数据的指定部分。
        例如，发送端通过 DevMSet 设置了一个 key，该 key 对应一组数据块（blob），每个 blob 大小为 100 字节。如果接收端仅需获取每个 blob 的后 50 字节，则可在执行 DevMGet 时，将 srcOffset 设置为 50，从而指定从发送端数据的第 50 字节处开始获取。
        约束说明：srcOffset 仅适用于设备间直传（D2D）模式，并且仅在接收端配置时生效。
