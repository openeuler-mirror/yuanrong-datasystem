yr.datasystem.hetero_client.HeteroClient.generate_key
=====================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.generate_key(prefix='')

    生成唯一的键。

    参数：
        - **prefix** (str) - 键前缀。为空时返回随机 UUID 键；非空时原样返回 prefix。

    返回：
        - **key** (string) - 生成的键。

    示例：
        >>> from yr.datasystem.hetero_client import HeteroClient
        >>> client = HeteroClient('127.0.0.1', 18482)
        >>> client.init()
        >>> client.generate_key()
        '0a595240-5506-4c7c-b1f7-7abfb1eb4add'
        >>> client.generate_key('my_key')
        'my_key'

    异常：
        - **RuntimeError** - 生成键失败。
