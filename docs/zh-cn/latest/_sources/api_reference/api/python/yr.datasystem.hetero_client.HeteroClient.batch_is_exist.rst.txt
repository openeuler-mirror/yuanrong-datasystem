yr.datasystem.hetero_client.HeteroClient.batch_is_exist
======================================================

.. py:method:: yr.datasystem.hetero_client.HeteroClient.batch_is_exist(keys)

    Batch-check whether keys exist and return integer indicators without an additional Python bool-to-int conversion.

    Parameters:
        - **keys** (list) - Object keys to check. The number of keys cannot exceed 30,000.

    Returns:
        - **exists** (list) - ``1`` if the corresponding key exists, otherwise ``0``.

    Raises:
        - **TypeError** - An input argument has an invalid type.
        - **RuntimeError** - The native existence query fails.
