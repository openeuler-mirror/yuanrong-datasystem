KVClient
========

:包路径: org.yuanrong.datasystem.kv

KV缓存客户端。

构造方法
--------

public KVClient()
~~~~~~~~~~~~~~~~~

连接到 Worker 并创建一个 KV 客户端实例。

返回：
    KV缓存客户端实例。

public KVClient(ConnectOptions connectOptions)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个KV客户端实例并连接服务端。

参数：
    - **connectOptions** - 用于建立连接的参数，详见 :doc:`org.yuanrong.datasystem.ConnectOptions` 章节。

返回：
    KV缓存客户端实例。

公共方法
--------

public void set(String key, ByteBuffer value)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端设置键的值。

参数：
    - **key** - 键。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
    - **value** - 键对应的值。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public void set(String key, ByteBuffer value, SetParam param)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端设置键的值。

参数：
    - **key** - 键。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
    - **value** - 键对应的值。
    - **param** - 设置参数，详见 :doc:`org.yuanrong.datasystem.kv.SetParam` 章节。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public ByteBuffer get(String key)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端获取键的值。

参数：
    - **key** - 键。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。

返回：
    键对应的值。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public ByteBuffer get(String key, int timeoutMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端获取键的值。

参数：
    - **key** - 键。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。
    - **timeoutMs** - 如果对象未就绪时等待结果返回的超时时间（毫秒）。需要为正整数，0表示不等待。

返回：
    键对应的值。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public List<ByteBuffer> get(List<String> keys)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端获取所有给定键的值。

参数：
    - **keys** - 键的向量。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。传入的key的个数 `<=10000`，推荐单次获取key个数 `<=64`。

返回：
    值的列表。若有部分数据获取不成功，则对应位置的值为null。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public List<ByteBuffer> get(List<String> keys, int timeoutMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端获取所有给定键的值。

参数：
    - **keys** - 键的向量。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。传入的key的个数 `<=10000`，推荐单次获取key个数 `<=64`。
    - **timeoutMs** - 如果对象未就绪时等待结果返回的超时时间（毫秒）。需要为正整数，0表示不等待。

返回：
    值的列表。若有部分数据获取不成功，则对应位置的值为null。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public void del(String key)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端删除一个键。

参数：
    - **key** - 键。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，最大长度为255字节。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public List<String> del(List<String> keys)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

调用 Worker 客户端删除所有给定的键。

参数：
    - **keys** - 键的向量。key的合法字符为：英文字母（a-zA-Z）、数字以及 ``-_!@#%^*()+=:;``，单个key最大长度为255字节。传入的key的个数 `<=10000`，推荐单次获取key个数 `<=64`。

返回：
    删除失败的键列表。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public String generateKey()
~~~~~~~~~~~~~~~~~~~~~~~~~~~

为 SET 操作生成一个唯一的键。

返回：
    唯一的键，如果键生成失败，则返回空字符串。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果客户端已关闭（kvClientPtr == 0），将抛出异常，消息为 "Client closed"。

public void close()
~~~~~~~~~~~~~~~~~~~

删除 KV 客户端，断开与数据系统 Worker 之间的连接，释放客户端持有的资源。

调用此方法后，客户端将不可再用。多次调用 close() 是安全的。

public void finalize()
~~~~~~~~~~~~~~~~~~~~~~

finalize() 方法用于释放 JNI 端的对象客户端指针，由于java垃圾回收的不确定性，该操作不能保证会被执行。

强烈建议使用 close() 方法显式释放资源，而不是依赖 finalize()。 