ProducerConf
==========================

.. cpp:class:: ProducerConf

    :header-file: #include <datasystem/stream/stream_config.h>
    :namespace: datasystem

    生产者的配置信息。

    **公共成员**

    .. cpp:member:: int64_t delayFlushTime = 5

        Send后最多延时相应时长后触发刷新缓冲区。
        - delayFlushTime = 0：代表立即刷新
        - delayFlushTime > 0，表示延迟刷新的时长，单位为毫秒

    .. cpp:member:: int64_t pageSize = 1024 * 1024ul

        代表生产者对应的缓冲Page大小， 单位字节。page写满后会触发刷新缓冲区。默认1MB, 必须大于0而且是4K的倍数。最大不能超过16MB。

    .. cpp:member:: uint64_t maxStreamSize = 100 * 1024 * 1024ul

        指定流在worker上最大能使用的共享内存大小， 单位字节。默认100MB, 范围[64KB, worker共享内存的大小]，必须是page size的两倍或以上。

    .. cpp:member:: bool autoCleanup = false

        当一条流没有producer和consumer时是否自动清理。默认为false，不会自动清理。

    .. cpp:member:: uint64_t retainForNumConsumers = 0

        为N个消费者保留数据，已经关闭的消费者也会被统计。最大值16.
        - 默认值0，如果消费者不存在时，生产者发送的数据不会保留。
        - N大于0，在第N个消费者消费数据前，生产者发送的数据会保留。

        - 注意：1、如果流启用了autoCleanup，生产者发送数据并关闭，然后再创建消费者的场景，不保留数据。2、worker如果发生主动缩容，即使设置了retainForNumConsumers，在没有创建远端consumer的情况下，也会导致数据丢失。

    .. cpp:member:: bool encryptStream = false

        配置流在worker推送数据时对数据进行加密。默认值为false，不启用加密。

    .. cpp:member:: uint64_t reserveSize = 0

        配置流预留共享内存空间。
        - 默认值为0，这时预留的共享内存空间默认为pageSize。
        - reserveSize配置值需要是pageSize的整数倍，最大值为maxStreamSize。

    .. cpp:member:: StreamMode streamMode = StreamMode::MPMC

        配置流的模式，默认值（MPMC）, 详见 :cpp:class:`StreamMode` 。