SetParam
========

:包路径: org.yuanrong.datasystem.kv

KV 设置参数类。

公共成员
--------

.. list-table::
   :widths: 20 20 60
   :header-rows: 1

   * - 字段名
     - 类型
     - 说明
   * - writeMode
     - WriteMode
     - 指定写入模式，默认值为 ``NONE_L2_CACHE``。详见 :doc:`org.yuanrong.datasystem.WriteMode` 章节。
   * - ttlSecond
     - long
     - 指定过期时间（秒）。如果值大于 0，数据将在过期后自动删除；如果设置为 0，数据需要手动删除。默认值为 0。

公共方法
--------

public void setWriteMode(WriteMode writeMode)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置写入模式。

参数：
    - **writeMode** - 写入模式，详见 :doc:`org.yuanrong.datasystem.WriteMode` 章节。

public void setTtlSecond(long ttlSecond)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置过期时间。

参数：
    - **ttlSecond** - 过期时间（秒），范围为 [0, 4294967295]。

异常：
    :doc:`org.yuanrong.datasystem.DataSystemException` - 如果 ttlSecond 不在有效范围内，将抛出异常。
