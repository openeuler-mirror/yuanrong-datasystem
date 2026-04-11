StatusCode
========================================

.. cpp:class:: StatusCode

    :header-file: #include <datasystem/utils/status.h>
    :namespace: datasystem

    `StatusCode` 类定义Status的状态码。

    目前，支持以下 `StatusCode`：

通用错误码 (范围: [0, 1000))
-----------------------------------

==================================================  =========  ==================================================================
定义                                                状态码     说明
==================================================  =========  ==================================================================
``StatusCode::K_OK``                                0          表示成功
``StatusCode::K_DUPLICATED``                        1          表示重复
``StatusCode::K_INVALID``                           2          表示入参检验失败
``StatusCode::K_NOT_FOUND``                         3          表示对象不存在
``StatusCode::K_KVSTORE_ERROR``                     4          表示KV存储错误
``StatusCode::K_RUNTIME_ERROR``                     5          表示运行时错误，多发生于内部错误
``StatusCode::K_OUT_OF_MEMORY``                     6          表示内存不足
``StatusCode::K_IO_ERROR``                          7          表示IO错误
``StatusCode::K_NOT_READY``                         8          表示未就绪
``StatusCode::K_NOT_AUTHORIZED``                    9          表示认证鉴权失败
``StatusCode::K_UNKNOWN_ERROR``                     10         表示未知错误
``StatusCode::K_INTERRUPTED``                        11         表示操作被中断
``StatusCode::K_OUT_OF_RANGE``                       12         表示超出范围
``StatusCode::K_NO_SPACE``                           13         表示空间不足
``StatusCode::K_NOT_LEADER_MASTER``                   14         表示不是主节点
``StatusCode::K_RECOVERY_ERROR``                     15         表示恢复错误
``StatusCode::K_RECOVERY_IN_PROGRESS``               16         表示恢复进行中
``StatusCode::K_FILE_NAME_TOO_LONG``                 17         表示文件名过长
``StatusCode::K_FILE_LIMIT_REACHED``                 18         表示文件数量达到限制
``StatusCode::K_TRY_AGAIN``                          19         表示需要重试
``StatusCode::K_DATA_INCONSISTENCY``                 20         表示数据不一致
``StatusCode::K_SHUTTING_DOWN``                      21         表示正在关闭
``StatusCode::K_WORKER_ABNORMAL``                    22         表示Worker异常
``StatusCode::K_CLIENT_WORKER_DISCONNECT``            23         表示客户端Worker断开连接
``StatusCode::K_WORKER_TIMEOUT``                     24         表示Worker超时
``StatusCode::K_MASTER_TIMEOUT``                     25         表示Master超时
``StatusCode::K_NOT_FOUND_IN_L2CACHE``               26         表示在L2缓存中未找到
``StatusCode::K_REPLICA_NOT_READY``                  27         表示副本未就绪
``StatusCode::K_CLIENT_WORKER_VERSION_MISMATCH``      28         表示客户端Worker版本不匹配
``StatusCode::K_SERVER_FD_CLOSED``                   29         表示服务器文件描述符已关闭
``StatusCode::K_RETRY_IF_LEAVING``                   30         表示如果正在离开则重试
``StatusCode::K_SCALE_DOWN``                         31         表示缩容
``StatusCode::K_SCALING``                            32         表示正在扩缩容
``StatusCode::K_CLIENT_DEADLOCK``                    33         表示客户端请求冲突，处理超时
``StatusCode::K_LRU_HARD_LIMIT``                     34         表示LRU硬限制
``StatusCode::K_LRU_SOFT_LIMIT``                     35         表示LRU软限制
==================================================  =========  ==================================================================

RPC错误码 (范围: [1000, 2000))
-----------------------------------

==================================================  =========  ==================================================================
定义                                                状态码     说明
==================================================  =========  ==================================================================
``StatusCode::K_RPC_CANCELLED``                     1000       表示请求的连接通道被意外关闭
``StatusCode::K_RPC_DEADLINE_EXCEEDED``             1001       表示RPC请求超时
``StatusCode::K_RPC_UNAVAILABLE``                   1002       表示请求发生超时
``StatusCode::K_RPC_STREAM_END``                    1003       表示RPC流结束
``StatusCode::K_URMA_ERROR``                         1004       表示发生了URMA相关的错误
``StatusCode::K_RDMA_ERROR``                         1005       表示发生了RDMA相关的错误
==================================================  =========  ==================================================================

对象错误码 (范围: [2000, 3000))
-----------------------------------

==================================================  =========  ==================================================================
定义                                                状态码     说明
==================================================  =========  ==================================================================
``StatusCode::K_OC_ALREADY_SEALED``                 2000       表示对象已密封
``StatusCode::K_OC_OBJECT_NOT_IN_USED``             2001       表示对象未在使用中
``StatusCode::K_OC_REMOTE_GET_NOT_ENOUGH``           2002       表示远程获取不足
``StatusCode::K_WRITE_BACK_QUEUE_FULL``              2003       表示写回队列已满
``StatusCode::K_OC_KEY_ALREADY_EXIST``               2004       表示对象键已存在
``StatusCode::K_WORKER_PULL_OBJECT_NOT_FOUND``       2005       表示Worker拉取对象未找到
==================================================  =========  ==================================================================

流错误码 (范围: [3000, 4000))
-----------------------------------

==================================================  =========  ==================================================================
定义                                                状态码     说明
==================================================  =========  ==================================================================
``StatusCode::K_SC_STREAM_NOT_FOUND``               3000       表示流未找到
``StatusCode::K_SC_PRODUCER_NOT_FOUND``             3001       表示生产者未找到
``StatusCode::K_SC_CONSUMER_NOT_FOUND``             3002       表示消费者未找到
``StatusCode::K_SC_END_OF_PAGE``                    3003       表示页面结束
``StatusCode::K_SC_STREAM_IN_RESET_STATE``          3004       表示流处于重置状态
``StatusCode::K_SC_WORKER_WAS_LOST``                 3005       表示Worker已丢失
``StatusCode::K_SC_STREAM_IN_USE``                  3006       表示流正在使用中
``StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS``       3007       表示流删除进行中
``StatusCode::K_SC_STREAM_RESOURCE_ERROR``           3008       表示流资源错误
``StatusCode::K_SC_ALREADY_CLOSED``                  3009       表示流已关闭
``StatusCode::K_SC_STREAM_NOTIFICATION_PENDING``     3010       表示流通知待处理
==================================================  =========  ==================================================================

异构错误码 (范围: [5000, 6000))
-----------------------------------

==================================================  =========  ==================================================================
定义                                                状态码     说明
==================================================  =========  ==================================================================
``StatusCode::K_ACL_ERROR``                         5000       表示发生了ACL相关的错误
``StatusCode::K_HCCL_ERROR``                         5001       表示发生了HCCL相关的错误
``StatusCode::K_FUTURE_TIMEOUT``                     5002       表示Future超时
==================================================  =========  ==================================================================
