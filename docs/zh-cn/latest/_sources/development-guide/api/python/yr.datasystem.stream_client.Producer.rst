yr.datasystem.stream_client.Producer
==================================================================

.. py:class:: yr.datasystem.stream_client.Producer(producer)

    流缓存生产者。

    参数：
        - **producer**： - 持有producer指针的对象。

    输出：
        Producer

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`send <yr.datasystem.stream_client.Producer.send>`
         - 生产者发送数据。
       * - :doc:`close <yr.datasystem.stream_client.Producer.close>`
         - 关闭生产者。一旦关闭后，生产者不可再用。
       
.. toctree::
    :maxdepth: 1
    :hidden:

    yr.datasystem.stream_client.Producer.send
    yr.datasystem.stream_client.Producer.close