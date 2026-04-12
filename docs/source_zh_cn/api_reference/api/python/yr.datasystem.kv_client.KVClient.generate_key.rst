yr.datasystem.kv_client.KVClient.generate_key
=============================================

.. py:method:: yr.datasystem.kv_client.KVClient.generate_key(prefix='')

    生成唯一的键。

    参数：
        - **prefix** (str) - 前缀，当为空时自动生成前缀。

    返回：
        唯一键。如果键生成失败，将返回空字符串。

    示例：
        >>> from yr.datasystem.kv_client import KVClient
        >>> client = KVClient('127.0.0.1', 18482)
        >>> client.init()
        >>> client.generate_key()
        '0a595240-5506-4c7c-b1f7-7abfb1eb4add;b053480f-75bf-41dd-8ce5-6f9ef58e9de4'