Optional
==========================

.. cpp:class:: Optional

    :header-file: #include <datasystem/utils/optional.h>
    :namespace: datasystem

    用于表示一个值可能存在，当值不存在时表示为 null。

    **公共函数**

    .. cpp:function:: constexpr Optional()

        默认构造函数。

        返回：
            ``Optional`` 实例。

    .. cpp:function:: Optional(const Optional &)

        拷贝构造函数。

        返回：
            ``Optional`` 实例。

    .. cpp:function:: Optional &operator=(const Optional &)

        拷贝构造函数。

        返回：
            ``Optional`` 实例。

    .. cpp:function:: Optional(Optional &&other)  noexcept

        移动构造函数。

        返回：
            ``Optional`` 实例。

    .. cpp:function:: Optional &operator=(Optional &&)  noexcept

        移动构造函数。

        返回：
            ``Optional`` 实例。

    .. cpp:function:: template <typename... Args> explicit Optional(Args &&... args);

        构造函数。

        参数： 
            - **args** - 构造入参。

        返回：
            ``Optional`` 实例。

    .. cpp:function:: ~Optional()

        析构函数