ObjectClient
============

:包路径: org.yuanrong.datasystem.object

对象缓存客户端。

构造方法
--------

public ObjectClient()
~~~~~~~~~~~~~~~~~~~~~

连接到 Worker 并创建一个对象缓存客户端实例。

返回：
    对象缓存客户端实例。

ObjectClient(ConnectOptions connectOptions)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

连接到 Worker 并创建一个对象缓存客户端实例。

参数：
    - **connectOptions** - 用于建立连接的参数，详见 :doc:`org.yuanrong.datasystem.ConnectOptions` 章节。

返回：
    对象缓存客户端实例。

公共方法
--------

public Buffer create(String objectKey, int size, CreateParam param)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端创建一个对象。

参数：
    - **objectKey** - 要创建的对象 ID。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
    - **size** - 对象的大小（字节）。
    - **param** - 创建参数，详见 :doc:`org.yuanrong.datasystem.object.CreateParam` 章节。

返回：
    对象的 :doc:`org.yuanrong.datasystem.object.Buffer`。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public void put(String objectKey, ByteBuffer buffer, CreateParam param, List<String> nestedObjectKeys)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端放置一个对象（发布语义）。

参数：
    - **objectKey** - 对象 ID。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
    - **buffer** - 用户的 ByteBuffer 对象。
    - **param** - 创建参数，详见 :doc:`org.yuanrong.datasystem.object.CreateParam` 章节。
    - **nestedObjectKeys** - 依赖于 objectKey 的对象列表。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public void put(String objectKey, ByteBuffer buffer, CreateParam param)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端放置一个对象（发布语义）。

参数：
    - **objectKey** - 对象 ID。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
    - **buffer** - 用户的 ByteBuffer 对象。
    - **param** - 创建参数，详见 :doc:`org.yuanrong.datasystem.object.CreateParam` 章节。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public List<Buffer> get(List<String> objectKeys, int timeoutMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端获取所有给定对象键的缓冲区。

参数：
    - **objectKeys** - 对象键的列表，最多不超过10000个。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个对象名称最大长度为255字节。
    - **timeoutMs** - 获取操作的超时时间（毫秒）。

返回：
    对象 :doc:`org.yuanrong.datasystem.object.Buffer` 列表。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public List<String> gIncreaseRef(List<String> objectKeys)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

增加数据系统中对象的全局引用计数。

参数：
    - **objectKeys** - 要增加引用的对象键列表，不能为空，最多不超过10000个。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个对象名称最大长度为255字节。

返回：
    增加引用失败的对象键列表。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public List<String> gDecreaseRef(List<String> objectKeys)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

减少数据系统中对象的全局引用计数。全局引用计数为0时，删除对象。

参数：
    - **objectKeys** - 要减少引用的对象键列表，不能为空，最多不超过10000个。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个对象名称最大长度为255字节。

返回：
    减少引用失败的对象键列表。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public int queryGlobalRefNum(String objectKey)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

获取指定对象的全局引用计数数量。

参数：
    - **objectKey** - 要查询的对象键。对象名称的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。

返回：
    引用计数数量。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭，将抛出异常。

public void close()
~~~~~~~~~~~~~~~~~~~

删除对象客户端，断开与数据系统 Worker 之间的连接，释放客户端持有的资源。

public void finalize()
~~~~~~~~~~~~~~~~~~~~~~

finalize() 方法用于释放 JNI 端的对象客户端指针，由于java垃圾回收的不确定性，该操作不能保证会被执行。

强烈建议使用 close() 方法显式释放资源，而不是依赖 finalize()。 
