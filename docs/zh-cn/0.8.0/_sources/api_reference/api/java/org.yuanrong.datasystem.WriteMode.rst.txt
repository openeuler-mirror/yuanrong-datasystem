WriteMode
=========

:包路径: org.yuanrong.datasystem

配置数据可靠性级别的枚举类。

枚举值
------

.. list-table::
   :widths: 40 60
   :header-rows: 1

   * - 定义
     - 说明
   * - ``WriteMode.NONE_L2_CACHE``
     - 对象仅写入到缓存中。默认配置。
   * - ``WriteMode.WRITE_THROUGH_L2_CACHE``
     - 对象同步写入缓存和二级缓存中。
   * - ``WriteMode.WRITE_BACK_L2_CACHE``
     - 对象同步写入缓存，异步写入二级缓存中。
   * - ``WriteMode.NONE_L2_CACHE_EVICT``
     - 对象是易失性的，如果缓存资源缺乏，对象可能会提前退出生命周期。
