Element
====================

.. cpp:class:: Element

    :header-file: #include <datasystem/stream/element.h>
    :namespace: datasystem

    用于生产和消费数据的结构体。

    **公共函数**
 
    .. cpp:function:: Element(uint8_t *ptr = nullptr, uint64_t size = 0, uint64_t id = ULONG_MAX)
 
       构造用于生产和消费的数据结构体

       参数：
            - **ptr** - 指向数据的指针, 默认为 ``nullptr`` 。
            - **size** - 数据大小，单位字节。
            - **id** - 用于确认消费完成的element id。
 
       返回：
           返回用于生产和消费的数据结构体。

    .. cpp:function:: ~Element()
 
       析构Element实例。

    **公共成员**

    .. cpp:member:: uint8_t *ptr

        执向数据的指针。

    .. cpp:member:: uint64_t size

        数据大小，单位字节。

    .. cpp:member:: uint64_t id

        用于确认消费完成的element id。