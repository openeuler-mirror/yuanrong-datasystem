Buffer
====================

.. cpp:class:: Buffer

    :header-file: #include <datasystem/object/buffer.h>
    :namespace: datasystem

    用于表示共享内存数据的类。

    **公共函数**

    .. cpp:function:: Buffer()

        默认构造类，创建一个空的 Buffer。

        返回：
            默认 ``Buffer`` 实例。

    .. cpp:function:: Buffer(Buffer &&other) noexcept

        移动构造函数。

        返回：
            移动构造后的 ``Buffer`` 实例。

    .. cpp:function:: ~Buffer() = default

        默认析构函数。

    .. cpp:function:: void *GetData()

        获取 ``Buffer`` 可读写的缓存数据指针。

        返回：
            可读写的缓存数据指针。

    .. cpp:function:: int64_t GetSize() const

        获取键值对 ``Buffer`` 的大小。

        返回： 
            ``Buffer`` 大小（以字节为单位）。

    .. cpp:function:: Status WLatch(uint64_t timeoutSec = 60)

        对 ``Buffer`` 添加写锁。

        .. note::
            仅在涉及单节点多实例同时访问的场景才需要加锁进行数据保护，否则无需在访问共享内存的数据前对其加锁。

        参数：
            - **timeoutSec** - 添加写锁的超时时间，默认为 60 秒。

        返回：
            返回值状态码为 `K_OK` 时表示加写锁成功，否则返回其他错误码。

    .. cpp:function:: Status UnWLatch()

        对 ``Buffer`` 解除写锁。

        返回：
            返回值状态码为 `K_OK` 时表示加写锁成功，否则返回其他错误码。

    .. cpp:function:: Status RLatch(uint64_t timeoutSec = 60)

        对 ``Buffer`` 添加读锁。

        .. note::
            仅在涉及单节点多实例同时访问的场景才需要加锁进行数据保护，否则无需在访问共享内存的数据前对其加锁。

        参数：
            - **timeoutSec** - 添加写锁的超时时间，默认为 60 秒。

        返回：
            返回值状态码为 `K_OK` 时表示加写锁成功，否则返回其他错误码。

    .. cpp:function:: Status UnRLatch()

        对 ``Buffer`` 解除读锁。

        返回：
            返回值状态码为 `K_OK` 时表示加写锁成功，否则返回其他错误码。

    .. cpp:function:: Status MemoryCopy(const void *data, uint64_t length)

        将数据拷贝到 `Buffer` 的缓存。该函数具备多线程并行拷贝的能力，同时能拷贝超过 2GB 的连续内存地址。

        参数：
            - **data** - 需要拷贝的数据内存地址。
            - **length** - 需要拷贝的数据长度。

        返回：
            返回值状态码为 `K_OK` 时表示数据拷贝成功，否则返回其他错误码。

        
 