datasystem.stream_client.StreamClient.create_producer
==================================================================

.. py:method:: datasystem.stream_client.StreamClient.create_producer(stream_name, delay_flush_time_ms=5, page_size_byte=1024 * 1024, max_stream_size_byte=1024 * 1024 * 1024, auto_cleanup=False, retain_for_num_consumers=0, encrypt_stream=False, reserve_size=0)

    创建生产者, 创建生产者时会创建流。

    参数：
        - **stream_name** (str) - 指定流的名称， 用于创建生产者。
        - **delay_flush_time_ms** (int) - Send数据后最多延时相应时长后触发刷新缓冲区, 单位ms, 默认5ms。
        - **page_size_byte** (int), 代表生产者对应的缓冲Page大小， 单位字节。page写满后会触发刷新缓冲区。默认1MB, 必须大于0而且是4K的倍数。最大不能超过16MB。
        - **max_stream_size_byte** (int)，指定流在worker上最大能使用的共享内存大小， 单位字节。默认100MB, 范围[64KB, worker共享内存的大小]，必须是page size的两倍或以上。
        - **auto_cleanup** (bool) -  当一条流没有producer和consumer时是否自动清理。默认为false，不会自动清理。
        - **retain_for_num_consumers** (int), 为N个消费者保留数据，已经关闭的消费者也会被统计。最大值16.
        - **encrypt_stream** (bool)，配置流在worker推送数据时对数据进行加密。默认值为false，不启用加密。
        - **reserve_size** (int) - 配置流预留共享内存空间， 单位字节。默认值为0，这时预留的共享内存空间默认为pageSize。reserveSize配置值需要是pageSize的整数倍，最大值为maxStreamSize。
    
    返回：
        outProducer(:class:`datasystem.stream_client.Producer`): 生产者实例。

    异常：
        - **TypeError** - 输入参数存在非法值。
        - **RuntimeError** - 创建生产者失败。