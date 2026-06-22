KVClientConfig
==========================

.. cpp:class:: KVClientConfig

    :header-file: #include <datasystem/utils/kv_client_config.h>
    :namespace: datasystem

    KV 客户端初始化配置。通过嵌套类 :cpp:class:`KVClientConfig::Builder` 构建，
    并在 ``KVClient::Init(const KVClientConfig &clientConfig)`` 中传入。

    本配置仅覆盖客户端日志、监控与 ZMQ 相关参数，不包含 :cpp:class:`ConnectOptions` 中的连接选项。

    **配置生效规则**

    进程内 **首次** 调用任意 ``KVClient::Init``（含无参 ``Init()``）时，会将当前传入的
    :cpp:class:`KVClientConfig` 快照固化为进程级配置；此后同一进程内其他 ``KVClient`` 实例再调用
    ``Init`` 时会复用已固化的进程级配置，不会用后续传入配置覆盖已生效设置。

    - 首次 Init 时，单个配置项的取值优先级（仅适用于本类所管理的参数）：

      1. :cpp:class:`KVClientConfig` 中通过 Builder 显式设置的字段（最高）；
      2. 进程启动时命令行 flag（``argv``）中已设置的同名字段；
      3. 对应的环境变量；
      4. 代码内置默认值（最低）。

      若首次 ``Init`` 传入的配置未显式设置某字段，该字段按上述 2→3→4 顺序取值。

    - 后续 Init 的行为：仅检查后续传入配置中通过 Builder 显式设置的字段；未设置字段
      视为未指定，不会要求与已固化快照逐字段一致，也不会用于清空已生效配置。若显式传入
      字段与已固化快照存在差异（含新增字段、修改字段值），实现会记录错误日志，但 ``Init``
      仍返回 ``StatusCode::K_OK``，且 **不会** 用新配置覆盖已生效的 flag 或日志设置。

    - ``Init()`` 与 ``Init(const KVClientConfig &)`` 的差异：

      - ``Init()`` 等价于传入未调用任何 Builder setter 的空 :cpp:class:`KVClientConfig`。
      - 若进程内首次调用为 ``Init()``，显式配置快照为空；此后再调用 ``Init(config)`` 并传入
        显式配置，该配置 **不会** 生效。若已先通过 ``Init(config)`` 固化了配置，后续 ``Init()``
        或空配置不表示清空或覆盖原配置。
      - 若需通过 Builder 指定参数，应确保进程内第一次 ``Init`` 即传入目标配置。

    **公共函数**

    .. cpp:function:: ~KVClientConfig()

       析构 KV 客户端配置对象。

.. cpp:class:: KVClientConfig::Builder

    :header-file: #include <datasystem/utils/kv_client_config.h>
    :namespace: datasystem

    KV 客户端配置构建器，支持链式设置日志、监控和 ZMQ 相关参数。

    **公共函数**

    .. cpp:function:: Builder()

       构造配置构建器。

    .. cpp:function:: ~Builder()

       析构配置构建器。

    .. cpp:function:: Builder &LogDir(const std::string &path)

       设置日志目录，对应 ``log_dir`` 配置项。

    .. cpp:function:: Builder &LogName(const std::string &name)

       设置客户端日志文件名，对应 ``log_filename`` 配置项。

    .. cpp:function:: Builder &LogWithoutPid(bool enable)

       设置客户端日志文件名是否不带 pid。默认为 false，即日志文件名默认带 pid。

    .. cpp:function:: Builder &AccessLogName(const std::string &name)

       设置客户端访问日志文件名。

    .. cpp:function:: Builder &MinLogLevel(int level)

       设置写入日志文件的最小级别，对应 ``minloglevel`` 配置项。

    .. cpp:function:: Builder &VLogLevel(int level)

       设置 verbose 日志级别，对应 ``v`` 配置项。

    .. cpp:function:: Builder &StderrThreshold(int level)

       设置写入 stderr 的最小级别，对应 ``stderrthreshold`` 配置项。

    .. cpp:function:: Builder &MaxLogSize(int size)

       设置日志轮转前的最大大小（MiB），对应 ``max_log_size`` 配置项。

    .. cpp:function:: Builder &MaxLogFileNum(uint32_t num)

       设置保留的日志文件数量上限，对应 ``max_log_file_num`` 配置项。

    .. cpp:function:: Builder &LogCompress(bool enable)

       设置是否启用日志压缩，对应 ``log_compress`` 配置项。

    .. cpp:function:: Builder &LogRetentionDay(uint32_t days)

       设置日志保留天数，对应 ``log_retention_day`` 配置项。

    .. cpp:function:: Builder &LogToStderr(bool enable)

       设置是否仅输出到 stderr，对应 ``logtostderr`` 配置项。

    .. cpp:function:: Builder &AlsoLogToStderr(bool enable)

       设置是否同时输出到 stderr，对应 ``alsologtostderr`` 配置项。

    .. cpp:function:: Builder &LogOnlyWriteInfoFile(bool enable)

       设置是否仅写 info 日志文件，对应 ``log_only_write_info_file`` 配置项。

    .. cpp:function:: Builder &LogAsyncEnable(bool enable)

       设置是否启用异步日志，对应 ``log_async`` 配置项。

    .. cpp:function:: Builder &LogAsyncQueueSize(uint32_t size)

       设置异步日志队列大小，对应 ``log_async_queue_size`` 配置项。

    .. cpp:function:: Builder &LogMonitorEnable(bool enable)

       设置是否启用日志监控，对应 ``log_monitor`` 配置项。

    .. cpp:function:: Builder &MonitorConfigPath(const std::string &path)

       设置日志监控配置文件路径，对应 ``monitor_config_file`` 配置项。

    .. cpp:function:: Builder &ZmqClientIoContext(int32_t threads)

       设置 ZMQ 客户端 IO context 数量，对应 ``zmq_client_io_context`` 配置项。

    .. cpp:function:: Builder &ZmqClientIoThread(int32_t threads)

       设置 ZMQ 客户端 IO 线程数量，对应 ``zmq_client_io_thread`` 配置项。

    .. cpp:function:: Status Build(KVClientConfig &config) const

       校验并构建 :cpp:class:`KVClientConfig`。

       参数：
           - **config** - 传出参数，返回构建后的配置对象。

       返回：
           返回值状态码为 ``StatusCode::K_OK`` 时表示构建成功，否则返回参数校验错误。
