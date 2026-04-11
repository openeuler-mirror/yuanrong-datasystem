Consumer
========

:包路径: org.yuanrong.datasystem.stream

流客户端的消费者接口。

内部类
------

StatisticsMessage
~~~~~~~~~~~~~~~~~

统计消息的容器类。提供 totalElements 和 notProcessedElements 的 getter 和 setter。

**公共方法**

BigInteger getTotalElements()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

获取总元素数量。

返回：
    总元素数量。

BigInteger getNotProcessedElements()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

获取未处理的元素数量。

返回：
    未处理的元素数量。

void setTotalElements(BigInteger uint64)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

设置总元素数量。

参数：
    - **uint64** - 总元素数量。

void setNotProcessedElements(BigInteger uint64)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

设置未处理的元素数量。

参数：
    - **uint64** - 未处理的元素数量。

公共方法
--------

public List<Element> receive(long expectNum, int timeoutMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

接收元素元数据，由 Worker 处理大小元素的查找和解析。

参数：
    - **expectNum** - 要读取的元素数量。
    - **timeoutMs** - 接收元素的超时时间（毫秒）。

返回：
    已接收的 :doc:`org.yuanrong.datasystem.stream.Element` 列表。

public List<Element> receive(int timeoutMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

接收元素元数据，由 Worker 处理大小元素的查找和解析。

参数：
    - **timeoutMs** - 接收元素的超时时间（毫秒）。

返回：
    已接收的 :doc:`org.yuanrong.datasystem.stream.Element` 列表。

public void ack(long elementId)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

确认此消费者已读取的元素。

参数：
    - **elementId** - 要确认的元素 ID。

public void close()
~~~~~~~~~~~~~~~~~~~

关闭消费者，关闭后将不允许接收和确认元素。

public void getStatisticsMessage(StatisticsMessage statistics)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

获取统计消息。

参数：
    - **statistics** - 一个空的 StatisticsMessage 实例，用于保存返回的指标。
