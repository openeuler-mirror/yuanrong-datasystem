StatusCode
========================================

.. cpp:class:: StatusCode

    :header-file: #include <datasystem/utils/status.h>
    :namespace: datasystem

    `StatusCode` 类定义Status的状态码。

    目前，支持以下 `StatusCode`：

    ===================================  ==================================================================
    定义                                 说明
    ===================================  ==================================================================
    ``StatusCode::K_OK``                 表示成功
    ``StatusCode::K_INVALID``            表示入参检验失败
    ``StatusCode::K_NOT_FOUND``          表示对象不存在
    ``StatusCode::K_RUNTIME_ERROR``      表示运行时错误，多发生于内部错误
    ``StatusCode::K_OUT_OF_MEMORY``      表示内存不足
    ``StatusCode::K_NOT_AUTHORIZED``     表示认证鉴权失败
    ``StatusCode::K_RPC_CANCELLED``      表示请求的连接通道被意外关闭
    ``StatusCode::K_RPC_UNAVAILABLE``    表示请求发生超时
    ``StatusCode::K_ACL_ERROR``          表示发生了ACL相关的错误
    ``StatusCode::K_HCCL_ERROR``         表示发生了HCCL相关的错误
    ``StatusCode::K_URMA_ERROR``         表示发生了URMA相关的错误
    ===================================  ==================================================================
