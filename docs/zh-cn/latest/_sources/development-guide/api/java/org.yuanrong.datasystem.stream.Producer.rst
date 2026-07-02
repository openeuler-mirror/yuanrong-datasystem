Producer
========

:包路径: org.yuanrong.datasystem.stream

流客户端的生产者接口。

公共方法
--------

public void send(ByteBuffer buffer)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

发送缓冲区数据。

参数：
    - **buffer** - 要发送的缓冲区。

public void send(ByteBuffer buffer, int timeoutMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

发送缓冲区数据（阻塞版本）。

参数：
    - **buffer** - 要发送的缓冲区。
    - **timeoutMs** - 发送元素的超时时间（毫秒）。

public void close()
~~~~~~~~~~~~~~~~~~~

关闭生产者，注销流的发布者。
