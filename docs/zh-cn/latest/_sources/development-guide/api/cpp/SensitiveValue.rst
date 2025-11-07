SensitiveValue
==========================

.. cpp:class:: SensitiveValue

    :header-file: #include <datasystem/utils/sensitive_value.h>
    :namespace: datasystem

    用于保存敏感信息的类。

    **公共函数**
 
    .. cpp:function:: SensitiveValue() = default

        默认构造函数。

    .. cpp:function:: SensitiveValue(const char *str)

        拷贝传入字符串指针上的敏感数据并构造实例。

        参数：
            - **str** - 需要读取的字符串指针。

    .. cpp:function:: SensitiveValue(const std::string &str)

        拷贝传入字符串的敏感数据并构造实例。

        参数：
            - **str** - 需要读取的字符串。

    .. cpp:function:: SensitiveValue(const char *str, size_t size)

        拷贝传入字符串指针上的敏感数据并构造实例。

        参数：
            - **str** - 需要读取的字符串指针。
            - **size** - 需要读取的大小。

    .. cpp:function:: SensitiveValue(std::unique_ptr<char[]> data, size_t size);

        拷贝传入数据指针上的敏感数据并构造实例。

        参数：
            - **data** - 需要读取的数据指针。
            - **size** - 需要读取的大小。
    
    .. cpp:function:: ~SensitiveValue()

        析构函数，析构时会释放保存的敏感数据。

    .. cpp:function:: bool Empty() const

        判断是否为空实例，即不包含任何敏感数据。

        返回：
            如果返回为 ``true``，表明该实例内不包含敏感数据。

    .. cpp:function:: const char *GetData() const

        获取敏感数据的内存地址。

        返回：
            敏感数据内存地址。

    .. cpp:function:: size_t GetSize() const

        获取敏感数据的大小。

        返回：
            敏感数据大小。

    .. cpp:function:: bool MoveTo(std::unique_ptr<char[]> &outData, size_t &outSize)

        将实例内的敏感数据移动到传入的 ``outData`` 中，该过程不涉及敏感数据的拷贝。

        参数：
            - **outData** - 用于接收敏感数据的智能指针。
            - **outSize** - 敏感数据的大小。

        返回：
            ``true`` 表示移动成功。

    .. cpp:function:: void Clear()

        清理实例内的敏感数据，