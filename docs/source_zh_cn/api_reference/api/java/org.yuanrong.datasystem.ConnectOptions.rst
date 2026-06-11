ConnectOptions
==============

:包路径: org.yuanrong.datasystem

连接选项配置类。

公共成员
--------

.. list-table::
   :widths: 20 20 60
   :header-rows: 1

   * - 字段名
     - 类型
     - 说明
   * - host
     - String
     - Worker 的 IPv4或IPv6 地址。
   * - port
     - int
     - Worker 服务的端口。
   * - connectTimeoutMs
     - int
     - 与 Worker 建立连接的超时时间，默认值为 60000 毫秒（60秒）。
   * - requestTimeoutMs
     - int
     - 请求超时时间，默认值为 0。
   * - clientPublicKey
     - String
     - 客户端公钥。默认值：""。
   * - clientPrivateKey
     - byte[]
     - 客户端私钥。
   * - serverPublicKey
     - String
     - 服务端公钥。默认值：""。
   * - accessKey
     - String
     - AK/SK 认证的访问密钥。默认值：""。
   * - secretKey
     - byte[]
     - AK/SK 认证的密钥。
   * - tenantID
     - String
     - 租户 ID。默认值：""。
   * - enableCrossNodeConnection
     - boolean
     - 是否允许客户端连接到备用节点，默认为 false。

构造方法
--------

public ConnectOptions()
~~~~~~~~~~~~~~~~~~~~~~~

创建一个空的连接选项实例。

public ConnectOptions(String host, int port)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个连接选项实例。

参数：
    - **host** - Worker 的 IPv4 或 IPv6 地址。
    - **port** - Worker 服务的端口。

公共方法
--------

public void setConnectTimeout(int connectTimeoutMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置连接超时时间。

参数：
    - **connectTimeoutMs** - 连接超时时间（毫秒）。

public void setRequestTimeout(int requestTimeoutMs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置请求超时时间。

参数：
    - **requestTimeoutMs** - 请求超时时间（毫秒）。

public void setEnableCrossNodeConnection(boolean enableCrossNodeConnection)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置是否允许跨节点连接。

参数：
    - **enableCrossNodeConnection** - 是否允许连接到备用节点。

public void setClientPublicKey(String clientPublicKey)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置客户端公钥。

参数：
    - **clientPublicKey** - 客户端公钥。

public void setClientPrivateKey(byte[] clientPrivateKey)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置客户端私钥。

参数：
    - **clientPrivateKey** - 客户端私钥。

public void setServerPublicKey(String serverPublicKey)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置服务端公钥。

参数：
    - **serverPublicKey** - 服务端公钥。

public void setAccessKey(String accessKey)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置 AK/SK 认证的访问密钥。

参数：
    - **accessKey** - 访问密钥。

public void setSecretKey(byte[] secretKey)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置 AK/SK 认证的密钥。

参数：
    - **secretKey** - 密钥。

public void setAkSkAuth(String accessKey, byte[] secretKey, String tenantID)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置 AK/SK 认证信息。

参数：
    - **accessKey** - 访问密钥。
    - **secretKey** - 密钥。
    - **tenantID** - 租户 ID。
