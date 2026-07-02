Future
====================

.. cpp:class:: Future

    :header-file: #include <datasystem/hetero/future.h>
    :namespace: datasystem

    Future 类用于获取异步请求的执行结果。

    **公共函数**

    .. cpp:function:: Status Get(uint64_t subTimeoutMs = 60000)

        等待异步请求执行完成并且获取异步请求的执行结果

        参数：
            **subTimeoutMs** - 传入参数，如果 **subTimeoutMs** > 0, 阻塞直到异步请求结果返回， 或者阻塞时间达到 **subTimeoutMs**， 单位ms。如果subTimeoutMs = 0, 立即返回。默认值60000ms

        返回：
            返回值状态码为 ``StatusCode::K_OK`` 时表示获取异步请求结果成功，否则返回其他错误码。