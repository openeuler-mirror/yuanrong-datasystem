CreateParam
===========

:包路径: org.yuanrong.datasystem.object

对象创建参数类。

公共成员
--------

.. list-table::
   :widths: 20 20 60
   :header-rows: 1

   * - 字段名
     - 类型
     - 说明
   * - consistencyType
     - ConsistencyType
     - 指定一致性类型模式，默认值为 ``PRAM``。详见 :doc:`org.yuanrong.datasystem.ConsistencyType` 章节。

公共方法
--------

public void setConsistencyType(ConsistencyType consistencyType)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

设置一致性类型。

参数：
    - **consistencyType** - 一致性类型，详见 :doc:`org.yuanrong.datasystem.ConsistencyType` 章节。
