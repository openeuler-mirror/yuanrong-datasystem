ObjMetaInfo
==========================

.. cpp:class:: ObjMetaInfo

    :header-file: #include <datasystem/object_client.h>
    :namespace: datasystem

    用于查看对象数据大小和存储节点信息的结构体。

    **公共成员**

    .. cpp:member:: uint64_t objSize

        对象数据大小， 单位为字节。

    .. cpp:member:: std::vector<std::string> locations

        对象数据存储的节点信息， 一组存储对象数据的节点， 节点格式为<ip>:<port>，例子：{127.0.0.1:31501}。