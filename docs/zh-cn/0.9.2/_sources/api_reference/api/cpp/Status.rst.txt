Status
====================

.. cpp:class:: Status

    :header-file: #include <datasystem/utils/status.h>
    :namespace: datasystem

    Status 类用于表示请求/方法的执行结果。

    **公共函数**

    .. cpp:function:: Status() noexcept

        默认构造类，创建一个空的 Status 实例，表示 OK。

        返回：
            默认 ``Status`` 实例。

    .. cpp:function:: Status(const Status &other)

        构造函数。

        返回：
            构造后的 ``Status`` 实例。

    .. cpp:function:: Status(Status &&other) noexcept

        移动构造函数。

        返回：
            移动构造后的 ``Status`` 实例。

    .. cpp:function:: Status &operator=(const Status &other) noexcept

        拷贝赋值运算符。

        参数：
            - **other** - 其他 ``Status`` 实例。

        返回：
            ``Status`` 实例引用。

    .. cpp:function:: Status &operator=(Status &&other) noexcept

        移动赋值运算符。

        参数：
            - **other** - 其他 ``Status`` 实例。

        返回：
            ``Status`` 实例引用。

    .. cpp:function:: Status(StatusCode code, std::string msg)

        构造函数，根据 StatusCode 和 ``msg`` 构造 ``Status`` 实例。

        参数：
            - **code** - 错误码。
            - **msg** - 错误信息。

        返回：
            ``Status`` 实例。

    .. cpp:function:: Status(StatusCode code, int lineOfCode, const std::string &fileName, const std::string &extra = "")

        构造函数，根据 ``StatusCode``、``lineOfCode``、``filenName`` 以及 ``extra`` 构造 ``Status`` 实例。

        参数：
            - **code** - 错误码。
            - **lineOfCode** - 文件行号。
            - **filenName** - 文件名。
            - **extra** - 额外的报错信息。

        返回： 
            ``Status`` 实例。

    .. cpp:function:: ~Status() noexcept

        默认析构函数。

    .. cpp:function:: static Status OK()

        返回状态码为 ``K_OK`` 的 ``Status`` 实例。

        返回：
            状态码为 `K_OK` 的 ``Status`` 实例。

    .. cpp:function:: std::string ToString() const

        返回 Status 的状态码和报错信息。

        返回：
            Status 的状态码和报错信息。

    .. cpp:function:: StatusCode GetCode() const

        返回 Status 的状态码。

        返回： 
            Status 的状态码。

    .. cpp:function:: std::string GetMsg()

        返回 Status 的报错信息。

        返回：
            Status 的报错信息。

    .. cpp:function:: void AppendMsg(const std::string &appendMsg)

        拼接 Status 的报错信息。

        参数： 
            - **appendMsg** - 需要拼接的报错信息。

    .. cpp:function:: friend std::ostream &operator<<(std::ostream &os, const Status &s)

        重载输出流运算符。

        参数：
            - **os** - 输出流。
            - **s** - ``Status`` 实例。

        返回：
            输出流引用。

    .. cpp:function:: explicit operator bool() const

        显式布尔转换运算符，判断 Status 的状态码是否为 ``K_OK``。

        返回：
            ``true`` 表示 Status 的状态码为 ``K_OK``。

    .. cpp:function:: bool operator==(const Status &other) const

        比较两个 Status 实例的状态码是否相等。

        参数：
            - **other** - 其他 ``Status`` 实例。

        返回：
            ``true`` 表示状态码相等。

    .. cpp:function:: bool operator!=(const Status &other) const

        比较两个 Status 实例的状态码是否不相等。

        参数：
            - **other** - 其他 ``Status`` 实例。

        返回：
            ``true`` 表示状态码不相等。

    .. cpp:function:: bool IsOk() const

        判断 Status 的状态码是否为 ``K_OK``。

        返回：
            ``true`` 表示 Status 的状态码为 ``K_OK``。

    .. cpp:function:: bool IsError() const

        判断 Status 的状态码是否为非 ``K_OK`` 错误码。

        返回： 
            ``true`` 表示 Status 的状态码为非 ``K_OK`` 错误码。

    .. cpp:function:: static std::string StatusCodeName(StatusCode code)

        获取状态码的字符串表示，多用于打印需求。

        参数：
            - **code** - 状态码。

        返回：
            状态码的字符串表示。

    
        
 