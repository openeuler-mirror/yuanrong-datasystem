yr.datasystem.kv_client.KVClient.generate_key
=============================================

.. py:method:: yr.datasystem.kv_client.KVClient.generate_key(prefix='')

    生成唯一的键。

    参数：
        - **prefix** (str) - 键前缀。为空时返回随机 UUID 键；非空时原样返回 prefix。

    返回：
        唯一键。如果键生成失败，将返回空字符串。

    示例：
        >>> from yr.datasystem.kv_client import KVClient
        >>> client = KVClient('127.0.0.1', 18482)
        >>> client.init()
        >>> client.generate_key()
        '0a595240-5506-4c7c-b1f7-7abfb1eb4add'
        >>> client.generate_key('my_key')
        'my_key'
