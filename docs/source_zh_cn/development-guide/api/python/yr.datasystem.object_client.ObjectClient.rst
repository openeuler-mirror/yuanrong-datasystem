yr.datasystem.object_client.ObjectClient
========================================

.. py:class:: yr.datasystem.object_client.ObjectClient(host, port, connect_timeout_ms=60000, client_public_key="", client_private_key="", server_public_key="", access_key="", secret_key="", tenant_id="")

    对象缓存客户端。

    参数：
        - **host** (str) - 数据系统Worker的主机IP地址。
        - **port** (int) - 数据系统Worker的主机IP端口号。
        - **connect_timeout_ms** (int) - 客户端连接和请求超时时间，单位为毫秒。默认值： ``60000`` 。
        - **client_public_key** (str) - 用于curve认证的客户端公钥。默认值： ``""`` 。
        - **client_private_key** (str) - 用于curve认证的客户端私钥。默认值： ``""`` 。
        - **server_public_key** (str) - 用于curve认证的服务端公钥。默认值： ``""`` 。
        - **access_key** (str) - AK/SK授权使用的访问密钥。默认值： ``""`` 。
        - **secret_key** (str) - AK/SK授权的密钥。默认值： ``""`` 。
        - **tenant_id** (str) - 租户ID。默认值： ``""`` 。

    输出：
        ObjectClient

    .. list-table::
       :widths: 40 60
       :header-rows: 0

       * - :doc:`init <yr.datasystem.object_client.ObjectClient.init>`
         - 初始化对象缓存客户端。
       * - :doc:`create <yr.datasystem.object_client.ObjectClient.create>`
         - 创建对象buffer。
       * - :doc:`put <yr.datasystem.object_client.ObjectClient.put>`
         - 将对象缓存到数据系统中。
       * - :doc:`get <yr.datasystem.object_client.ObjectClient.get>`
         - 获取给定列表对象 key 的Buffer。
       * - :doc:`g_increase_ref <yr.datasystem.object_client.ObjectClient.g_increase_ref>`
         - 增加给定列表对象 key 的全局引用计数。
       * - :doc:`g_decrease_ref <yr.datasystem.object_client.ObjectClient.g_decrease_ref>`
         - 减少给定列表对象 key 的全局引用计数。
       * - :doc:`query_global_ref_num <yr.datasystem.object_client.ObjectClient.query_global_ref_num>`
         - 查询对象全局引用计数。
       * - :doc:`generate_object_key <yr.datasystem.object_client.ObjectClient.generate_object_key>`
         - 生成一个带数据系统Worker UUID的对象 key。
       
.. toctree::
    :maxdepth: 1
    :hidden:

    yr.datasystem.object_client.ObjectClient.init
    yr.datasystem.object_client.ObjectClient.create
    yr.datasystem.object_client.ObjectClient.put
    yr.datasystem.object_client.ObjectClient.get
    yr.datasystem.object_client.ObjectClient.g_increase_ref
    yr.datasystem.object_client.ObjectClient.g_decrease_ref
    yr.datasystem.object_client.ObjectClient.query_global_ref_num
    yr.datasystem.object_client.ObjectClient.generate_object_key