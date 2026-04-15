StreamClient
============

:包路径: org.yuanrong.datasystem.stream

流客户端。

构造方法
--------

public StreamClient(ConnectOptions connectOptions)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

连接到 Worker 并创建一个流缓存客户端实例。

参数：
    - **connectOptions** - 用于建立连接的参数，详见 :doc:`org.yuanrong.datasystem.ConnectOptions` 章节。

返回：
    流缓存客户端实例。

StreamClient(ConnectOptions connectOptions, boolean shouldReportWorkerLost)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

连接到 Worker 并创建一个流缓存客户端实例。

参数：
    - **connectOptions** - 用于建立连接的参数，详见 :doc:`org.yuanrong.datasystem.ConnectOptions` 章节。
    - **shouldReportWorkerLost** - 当 Worker 崩溃或 Worker 丢失客户端时是否向调用者报告。

返回：
    流缓存客户端实例。

公共方法
--------

public Producer createProducer(String streamName)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个 :doc:`org.yuanrong.datasystem.stream.Producer` 来发送元素。

参数：
    - **streamName** - 流的名称。

返回：
    用户可以用来发送元素的 :doc:`org.yuanrong.datasystem.stream.Producer` 接口。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Producer createProducer(String streamName, long delayFlushTimeMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个 :doc:`org.yuanrong.datasystem.stream.Producer` 来发送元素。

参数：
    - **streamName** - 流的名称。
    - **delayFlushTimeMs** - 发送后自动刷新的时间（毫秒），默认值为 5ms。

返回：
    用户可以用来发送元素的 :doc:`org.yuanrong.datasystem.stream.Producer` 接口。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个 :doc:`org.yuanrong.datasystem.stream.Producer` 来发送元素。

参数：
    - **streamName** - 流的名称。
    - **delayFlushTimeMs** - 发送后自动刷新的时间（毫秒）。
    - **pageSizeByte** - 分配页面的大小，默认大小为 1MB，必须是 4KB 的倍数。

返回：
    用户可以用来发送元素的 :doc:`org.yuanrong.datasystem.stream.Producer` 接口。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte, long maxStreamSize)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个 :doc:`org.yuanrong.datasystem.stream.Producer` 来发送元素。

参数：
    - **streamName** - 流的名称。
    - **delayFlushTimeMs** - 发送后自动刷新的时间（毫秒）。
    - **pageSizeByte** - 分配页面的大小，默认大小为 1MB，必须是 4KB 的倍数。
    - **maxStreamSize** - Worker 中的最大流大小，默认大小为 1GB，必须大于 64KB 且小于共享内存大小。

返回：
    用户可以用来发送元素的 :doc:`org.yuanrong.datasystem.stream.Producer` 接口。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte, long maxStreamSize, boolean autoCleanup)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个 :doc:`org.yuanrong.datasystem.stream.Producer` 来发送元素。

参数：
    - **streamName** - 流的名称。
    - **delayFlushTimeMs** - 发送后自动刷新的时间（毫秒）。
    - **pageSizeByte** - 分配页面的大小，默认大小为 1MB，必须是 4KB 的倍数。
    - **maxStreamSize** - Worker 中的最大流大小，默认大小为 1GB，必须大于 64KB 且小于共享内存大小。
    - **autoCleanup** - 当最后一个消费者/生产者退出时是否自动删除，默认为 false。

返回：
    用户可以用来发送元素的 :doc:`org.yuanrong.datasystem.stream.Producer` 接口。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte, long maxStreamSize, boolean autoCleanup, long retainForNumConsumers)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个 :doc:`org.yuanrong.datasystem.stream.Producer` 来发送元素。

参数：
    - **streamName** - 流的名称。
    - **delayFlushTimeMs** - 发送后自动刷新的时间（毫秒）。
    - **pageSizeByte** - 分配页面的大小，默认大小为 1MB，必须是 4KB 的倍数。
    - **maxStreamSize** - Worker 中的最大流大小，默认大小为 1GB，必须大于 64KB 且小于共享内存大小。
    - **autoCleanup** - 当最后一个消费者/生产者退出时是否自动删除，默认为 false。
    - **retainForNumConsumers** - 为多少个消费者保留数据，默认为 0。

返回：
    用户可以用来发送元素的 :doc:`org.yuanrong.datasystem.stream.Producer` 接口。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte, long maxStreamSize, boolean autoCleanup, long retainForNumConsumers, boolean encryptStream)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个 :doc:`org.yuanrong.datasystem.stream.Producer` 来发送元素。

参数：
    - **streamName** - 流的名称。
    - **delayFlushTimeMs** - 发送后自动刷新的时间（毫秒）。
    - **pageSizeByte** - 分配页面的大小，默认大小为 1MB，必须是 4KB 的倍数。
    - **maxStreamSize** - Worker 中的最大流大小，默认大小为 1GB，必须大于 64KB 且小于共享内存大小。
    - **autoCleanup** - 当最后一个消费者/生产者退出时是否自动删除，默认为 false。
    - **retainForNumConsumers** - 为多少个消费者保留数据，默认为 0。
    - **encryptStream** - 是否启用 Worker 之间的流数据加密，默认为 false。

返回：
    用户可以用来发送元素的 :doc:`org.yuanrong.datasystem.stream.Producer` 接口。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Producer createProducer(String streamName, long delayFlushTimeMs, long pageSizeByte, long maxStreamSize, boolean autoCleanup, long retainForNumConsumers, boolean encryptStream, long reserveSize)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个 :doc:`org.yuanrong.datasystem.stream.Producer` 来发送元素。

参数：
    - **streamName** - 流的名称。
    - **delayFlushTimeMs** - 发送后自动刷新的时间（毫秒）。
    - **pageSizeByte** - 分配页面的大小，默认大小为 1MB，必须是 4KB 的倍数。
    - **maxStreamSize** - Worker 中的最大流大小，默认大小为 1GB，必须大于 64KB 且小于共享内存大小。
    - **autoCleanup** - 当最后一个消费者/生产者退出时是否自动删除，默认为 false。
    - **retainForNumConsumers** - 为多少个消费者保留数据，默认为 0。
    - **encryptStream** - 是否启用 Worker 之间的流数据加密，默认为 false。
    - **reserveSize** - 默认保留大小为页面大小，必须是页面大小的倍数。

返回：
    用户可以用来发送元素的 :doc:`org.yuanrong.datasystem.stream.Producer` 接口。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Consumer subscribe(String streamName, String subName, SubscriptionType subscriptionType)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

订阅一个新的消费者到主请求。

参数：
    - **streamName** - 流的名称。
    - **subName** - 订阅的名称。
    - **subscriptionType** - 订阅类型，详见 :doc:`org.yuanrong.datasystem.stream.SubscriptionType` 章节。

返回：
    :doc:`org.yuanrong.datasystem.stream.Consumer` 实例。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public Consumer subscribe(String streamName, String subName, SubscriptionType subscriptionType, boolean shouldAutoAck)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

订阅一个新的消费者到主请求。

参数：
    - **streamName** - 流的名称。
    - **subName** - 订阅的名称。
    - **subscriptionType** - 订阅类型，详见 :doc:`org.yuanrong.datasystem.stream.SubscriptionType` 章节。
    - **shouldAutoAck** - 是否为此订阅者启用自动确认。

返回：
    :doc:`org.yuanrong.datasystem.stream.Consumer` 实例。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public long queryGlobalProducerNum(String streamName)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

查询全局 Worker 节点中的生产者数量。

参数：
    - **streamName** - 目标流的名称。

返回：
    查询结果。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public long queryGlobalConsumerNum(String streamName)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

查询全局 Worker 节点中的消费者数量。

参数：
    - **streamName** - 目标流的名称。

返回：
    查询结果。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public void deleteStream(String streamName)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

删除流。

参数：
    - **streamName** - 目标流的名称。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public void close()
~~~~~~~~~~~~~~~~~~~

删除流客户端，断开与数据系统 Worker 之间的连接，释放客户端持有的资源。

public void finalize()
~~~~~~~~~~~~~~~~~~~~~~

finalize() 方法用于释放 JNI 端的流客户端指针，由于java垃圾回收的不确定性，该操作不能保证会被执行。

强烈建议使用 close() 方法显式释放资源，而不是依赖 finalize()。