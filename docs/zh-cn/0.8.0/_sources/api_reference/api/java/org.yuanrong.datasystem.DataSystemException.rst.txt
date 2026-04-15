DataSystemException
===================

:包路径: org.yuanrong.datasystem

数据系统异常类。

公共成员
--------

.. list-table::
   :widths: 20 20 60
   :header-rows: 1

   * - 字段名
     - 类型
     - 说明
   * - errorCode
     - int
     - 异常的错误码。

构造方法
--------

public DataSystemException(int errorCode, String message)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

使用给定参数创建一个 DataSystemException。

参数：
    - **errorCode** - 异常的错误码。
    - **message** - 异常详细消息。

public DataSystemException(String message)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

使用给定参数创建一个 DataSystemException。

参数：
    - **message** - 异常详细消息。

DataSystemException(int errorCode, String message, Throwable cause)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

使用给定参数创建一个 DataSystemException。

参数：
    - **errorCode** - 异常的错误码。
    - **message** - 异常详细消息。
    - **cause** - 异常原因。

DataSystemException(String message, Throwable cause)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

使用给定参数创建一个 DataSystemException。

参数：
    - **message** - 异常详细消息。
    - **cause** - 异常原因。

公共方法
--------

public int getErrorCode()
~~~~~~~~~~~~~~~~~~~~~~~~~

获取错误码。

返回：
    异常的错误码。
