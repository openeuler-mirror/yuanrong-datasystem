Buffer
======

:包路径: org.yuanrong.datasystem.object

用于读写数据并将数据发布到服务器的 Buffer 接口。

公共方法
--------

public void publish()
~~~~~~~~~~~~~~~~~~~~~

将可变数据发布到服务器。

public void publish(List<String> nestedKeys)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

将可变数据发布到服务器。

参数：
    - **nestedKeys** - 嵌套对象的对象键。

public void seal()
~~~~~~~~~~~~~~~~~~

将不可变数据发布到服务器。

public void seal(List<String> nestedKeys)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

将不可变数据发布到服务器。

参数：
    - **nestedKeys** - 嵌套对象的对象键。

public void wLatch()
~~~~~~~~~~~~~~~~~~~~

对内存执行写锁，以保护内存免受并发读写。

public void wLatch(int timeoutSec)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

对内存执行写锁，以保护内存免受并发读写。

参数：
    - **timeoutSec** - 尝试锁定超时时间（秒）。

public void rLatch()
~~~~~~~~~~~~~~~~~~~~

对内存执行读锁，以保护内存免受并发写入（允许并发读取）。

public void rLatch(int timeoutSec)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

对内存执行读锁，以保护内存免受并发写入（允许并发读取）。

参数：
    - **timeoutSec** - 尝试锁定超时时间，默认值为 60 秒。

public void unWLatch()
~~~~~~~~~~~~~~~~~~~~~~

解除内存上的写锁。

public void unRLatch()
~~~~~~~~~~~~~~~~~~~~~~

解除内存上的读锁。

public void memoryCopy(ByteBuffer buffer)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

将数据写入缓冲区。

参数：
    - **buffer** - 对象数据。

public ByteBuffer mutableData()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

获取可变的 ByteBuffer 对象。

返回：
    ByteBuffer 对象。

public ByteBuffer immutableData()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

获取不可变的 ByteBuffer 对象。

返回：
    ByteBuffer 对象。

public void invalidateBuffer()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

使当前主机上的数据失效。

public long getSize()
~~~~~~~~~~~~~~~~~~~~~

获取缓冲区的数据大小。

返回：
    缓冲区的数据大小。

public void close()
~~~~~~~~~~~~~~~~~~~

删除缓冲区，释放资源。
