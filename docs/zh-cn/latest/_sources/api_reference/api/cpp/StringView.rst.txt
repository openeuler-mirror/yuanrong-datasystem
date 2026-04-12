StringView
====================

.. cpp:class:: StringView

    :header-file: #include <datasystem/utils/string_view.h>
    :namespace: datasystem

    字符串视图类，主要用于高效地传递和访问字符串数据，而无需复制或分配内存。

    **公共函数**

    .. cpp:function:: constexpr StringView() noexcept

        默认构造函数，构造一个空字符串视图实例。

        返回：
            ``StringView`` 实例。

    .. cpp:function:: constexpr StringView(const StringView &) noexcept

        拷贝构造函数，使用默认实现。

        参数：
            - **StringView** - 要拷贝的 ``StringView`` 实例。

        返回：
            ``StringView`` 实例。

    .. cpp:function:: constexpr StringView(const char *str)

        保存字符串指针上的并构造实例，这个过程不会发生拷贝。

        参数：
            - **str** - 需要保存的字符串指针。

        返回：
            ``StringView`` 实例。

    .. cpp:function:: constexpr StringView(const char *str, size_t len)

        保存字符串指针上的并构造实例，这个过程不会发生拷贝。

        参数：
            - **str** - 需要保存的字符串指针。
            - **len** - 字符串长度。

        返回：
            ``StringView`` 实例。

    .. cpp:function:: StringView(const std::string &str)

        保存字符串的指针上与字符串大小并构造实例，这个过程不会发生拷贝。

        参数：
            - **str** - 字符串。

        返回：
            ``StringView`` 实例。

    .. cpp:function:: constexpr StringView &operator=(const StringView &) noexcept

        拷贝赋值运算符，使用默认实现。

        参数：
            - **StringView** - 要拷贝的 ``StringView`` 实例。

        返回：
            ``StringView &`` 引用。

    .. cpp:function:: ~StringView()

        默认析构函数。

    .. cpp:function:: constexpr const char *data() const noexcept

        获取 ``StringView`` 的数据指针。

        返回：
            ``StringView`` 的数据指针。

    .. cpp:function:: constexpr size_t size() const noexcept

        获取 StringView 的数据大小。

        返回：
            ``StringView`` 的数据大小。

    .. cpp:function:: constexpr bool empty() const noexcept

        判断 StringView 是否为空。

        返回：
            ``true`` 表示 ``StringView`` 为空。