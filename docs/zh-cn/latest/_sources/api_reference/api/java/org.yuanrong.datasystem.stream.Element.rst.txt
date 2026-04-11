Element
=======

:包路径: org.yuanrong.datasystem.stream

封装元素 ID 和缓冲区的元素类。

公共成员
--------

.. list-table::
   :widths: 20 20 60
   :header-rows: 1

   * - 字段名
     - 类型
     - 说明
   * - id
     - long
     - 元素 ID。
   * - buffer
     - ByteBuffer
     - 此元素的缓冲区。

构造方法
--------

public Element(long id, ByteBuffer buffer)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

创建一个元素实例。

参数：
    - **id** - 元素 ID。
    - **buffer** - 此元素的缓冲区。

公共方法
--------

public long getId()
~~~~~~~~~~~~~~~~~~~

获取元素 ID。

返回：
    元素 ID。

public ByteBuffer getBuffer()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

获取元素的缓冲区。

返回：
    元素的缓冲区。
