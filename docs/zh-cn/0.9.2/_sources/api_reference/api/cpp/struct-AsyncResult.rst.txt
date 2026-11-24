AsyncResult
==========================

.. cpp:class:: AsyncResult

    :header-file: #include <datasystem/hetero_client.h>
    :namespace: datasystem

    用于获取异步调用返回结果的结构体。

    **公共成员**

    .. cpp:member:: Status status

        返回值状态码为 `K_OK` 时表示接口调用成功，否则返回其他错误码。

    .. cpp:member:: std::vector<std::string> failedList

        返回失败的key。