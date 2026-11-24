Java
==============================


.. toctree::
   :glob:
   :hidden:
   :maxdepth: 1

   org.yuanrong.datasystem.kv.KVClient
   org.yuanrong.datasystem.object.ObjectClient
   org.yuanrong.datasystem.stream.StreamClient
   org.yuanrong.datasystem.stream.Producer
   org.yuanrong.datasystem.stream.Consumer
   org.yuanrong.datasystem.object.Buffer
   org.yuanrong.datasystem.stream.Element
   org.yuanrong.datasystem.ConnectOptions
   org.yuanrong.datasystem.kv.SetParam
   org.yuanrong.datasystem.object.CreateParam
   org.yuanrong.datasystem.Context
   org.yuanrong.datasystem.DataSystemException
   org.yuanrong.datasystem.WriteMode
   org.yuanrong.datasystem.ConsistencyType
   org.yuanrong.datasystem.stream.SubscriptionType

KV接口
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :doc:`org.yuanrong.datasystem.kv.KVClient <org.yuanrong.datasystem.kv.KVClient>`
      - KV缓存客户端。
    * - :doc:`org.yuanrong.datasystem.kv.SetParam <org.yuanrong.datasystem.kv.SetParam>`
      - KV 设置参数类。

Object接口
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :doc:`org.yuanrong.datasystem.object.ObjectClient <org.yuanrong.datasystem.object.ObjectClient>`
      - 对象缓存客户端。
    * - :doc:`org.yuanrong.datasystem.object.Buffer <org.yuanrong.datasystem.object.Buffer>`
      - 用于读写数据并将数据发布到服务器的 Buffer 接口。
    * - :doc:`org.yuanrong.datasystem.object.CreateParam <org.yuanrong.datasystem.object.CreateParam>`
      - 对象创建参数类。

Stream接口
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :doc:`org.yuanrong.datasystem.stream.StreamClient <org.yuanrong.datasystem.stream.StreamClient>`
      - 流客户端。
    * - :doc:`org.yuanrong.datasystem.stream.Producer <org.yuanrong.datasystem.stream.Producer>`
      - 流客户端的生产者接口。
    * - :doc:`org.yuanrong.datasystem.stream.Consumer <org.yuanrong.datasystem.stream.Consumer>`
      - 流客户端的消费者接口。
    * - :doc:`org.yuanrong.datasystem.stream.Element <org.yuanrong.datasystem.stream.Element>`
      - 封装元素 ID 和缓冲区的元素类。
    * - :doc:`org.yuanrong.datasystem.stream.SubscriptionType <org.yuanrong.datasystem.stream.SubscriptionType>`
      - 配置订阅模式的枚举类。

公共类
-----------------------------------

.. list-table::
    :widths: 30 70
    :header-rows: 0

    * - :doc:`org.yuanrong.datasystem.ConnectOptions <org.yuanrong.datasystem.ConnectOptions>`
      - 连接选项配置类。
    * - :doc:`org.yuanrong.datasystem.Context <org.yuanrong.datasystem.Context>`
      - 用于管理全局信息的上下文类。
    * - :doc:`org.yuanrong.datasystem.DataSystemException <org.yuanrong.datasystem.DataSystemException>`
      - 数据系统异常类。
    * - :doc:`org.yuanrong.datasystem.WriteMode <org.yuanrong.datasystem.WriteMode>`
      - 配置数据可靠性级别的枚举类。
    * - :doc:`org.yuanrong.datasystem.ConsistencyType <org.yuanrong.datasystem.ConsistencyType>`
      - 配置一致性类型模式的枚举类。
