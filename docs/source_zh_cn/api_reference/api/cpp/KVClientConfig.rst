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
      2. 对应的环境变量；
      3. 代码内置默认值（最低）。

      若首次 ``Init`` 传入的配置未显式设置某字段，该字段按上述 2→3 顺序取值。

    - 后续 Init 的行为：仅检查后续传入配置中通过 Builder 显式设置的字段；未设置字段
      视为未指定，不会要求与已固化快照逐字段一致，也不会用于清空已生效配置。若显式传入
      字段与已固化快照存在差异（含新增字段、修改字段值），实现会记录错误日志，但 ``Init``
      仍返回 ``StatusCode::K_OK``，且 **不会** 用新配置覆盖已生效的 flag 或日志设置。

      具体分三种情况：

      1. 首次 ``Init`` 显式设置了某字段，后续 ``Init`` 再设置该字段（值不同）：新值不生效，
         ``Init`` 返回 ``StatusCode::K_OK``，并打印日志：
         ``The KVClient config [name=value] is different from the process-level config and will not take effect.``。
      2. 首次 ``Init`` 未设置某字段，后续 ``Init`` 显式设置该字段：该显式设置同样不生效，
         ``Init`` 返回 ``StatusCode::K_OK``，并打印上述日志。
      3. ``Init()`` 未传入 :cpp:class:`KVClientConfig` （等同于传入全是默认值的
         :cpp:class:`KVClientConfig`）：设置的配置不生效，``Init`` 返回 ``StatusCode::K_OK``。

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

       设置日志目录，对应环境变量 ``DATASYSTEM_CLIENT_LOG_DIR``。默认值为 ``~/.datasystem/logs``。

    .. cpp:function:: Builder &LogName(const std::string &name)

       设置客户端日志文件名（不含目录和扩展名）。对应内部配置项 ``log_filename``。
       不允许为空；允许字符为英文字母、数字及路径字符集中的符号。默认值为 ``ds_client``。

    .. cpp:function:: Builder &LogWithoutPid(bool enable)

       设置客户端日志文件名是否不带 pid。对应环境变量 ``DATASYSTEM_CLIENT_LOG_WITHOUT_PID``。
       为 ``true`` 时输出为 ``ds_client.log``、``ds_client_access.log``；为 ``false`` 时恢复带
       ``<pid>`` 后缀的命名，适合多 client 进程同时运行且不希望覆盖日志的场景。默认值为 ``true``。

    .. cpp:function:: Builder &AccessLogName(const std::string &name)

       设置客户端访问日志文件名（不含目录和扩展名）。非空时仅允许英文字母、数字和下划线。
       默认值为 ``ds_client_access``。

    .. cpp:function:: Builder &MinLogLevel(int level)

       数据系统 SDK 的最小日志级别。数值越大，输出的日志越少：``0`` 输出 INFO 及以上，
       ``1`` 输出 WARNING 及以上，``2`` 输出 ERROR 及以上，``3`` 只输出 FATAL。
       取值范围 ``[0, 3]``，默认值为 ``0``。对应环境变量 ``DATASYSTEM_MIN_LOG_LEVEL``。

    .. cpp:function:: Builder &VLogLevel(int level)

       设置 SDK 的 verbose（VLOG）日志级别。数值越大，输出的调试日志越详细，一般用于排查
       内部细节。取值范围 ``[0, 10]``，默认值为 ``0``。对应环境变量 ``DATASYSTEM_LOG_V``。

    .. cpp:function:: Builder &StderrThreshold(int level)

       控制哪些级别的日志同时复制到 stderr。取值含义同 ``MinLogLevel`` （``0``=INFO、
       ``1``=WARNING、``2``=ERROR、``3``=FATAL）；设为 ``3`` 表示只有 FATAL 输出到 stderr。
       取值范围 ``[0, 3]``，默认值为 ``3``（FATAL）。对应环境变量 ``DATASYSTEM_STD_THRESHOLD``。

    .. cpp:function:: Builder &MaxLogSize(int size)

       设置日志轮转前的最大大小（MiB）。取值范围 ``[1, 4095]``，默认值为 ``100``。
       对应环境变量 ``DATASYSTEM_CLIENT_MAX_LOG_SIZE``。

    .. cpp:function:: Builder &MaxLogFileNum(uint32_t num)

       设置保留的日志文件数量上限（按级别分别保留）。取值范围 ``[0, 200000]``，
       ``0`` 表示不限制数量。默认值为 ``5``。对应环境变量 ``DATASYSTEM_MAX_LOG_FILE_NUM``。

    .. cpp:function:: Builder &LogCompress(bool enable)

       设置是否对轮转后的旧日志文件进行 ``.gz`` 压缩。默认值为 ``false``。
       对应环境变量 ``DATASYSTEM_LOG_COMPRESS``。

    .. cpp:function:: Builder &LogRetentionDay(uint32_t days)

       设置日志保留天数。``0`` 表示不按时间清理日志。默认值为 ``0``。
       对应环境变量 ``DATASYSTEM_LOG_RETENTION_DAY``。

    .. cpp:function:: Builder &LogToStderr(bool enable)

       设置是否将日志仅输出到 stderr 而不写文件。为 ``true`` 时所有日志输出到 stderr 并关闭
       文件输出；为 ``false`` 时日志写入文件。默认值为 ``false``。对应环境变量
       ``DATASYSTEM_LOG_TO_STDERR``。该参数与 ``AlsoLogToStderr`` （在文件输出基础上额外输出
       到 stderr）、``LogOnlyWriteInfoFile`` （控制是否按级别拆分文件）相互独立。

    .. cpp:function:: Builder &AlsoLogToStderr(bool enable)

       设置是否在写文件的同时也输出到 stderr。为 ``true`` 时日志同时写文件和 stderr；
       为 ``false`` 时只写文件。默认值为 ``false``。对应环境变量
       ``DATASYSTEM_ALSO_LOG_TO_STDERR``。当 ``LogToStderr`` 为 ``true`` 时本参数无效
       （此时已全部重定向到 stderr）。

    .. cpp:function:: Builder &LogOnlyWriteInfoFile(bool enable)

       设置是否将所有级别的日志都写入单个 INFO 日志文件。为 ``true`` 时所有级别日志只写入
       ``.INFO`` 文件；为 ``false`` 时额外生成 ``.WARNING``、``.ERROR`` 文件，高级别日志按
       等级写入各自文件。默认值为 ``true``。对应环境变量
       ``DATASYSTEM_LOG_ONLY_WRITE_INFO_FILE``。

    .. cpp:function:: Builder &LogAsyncEnable(bool enable)

       设置是否启用异步日志写入。为 ``true`` 时异步写日志以降低前台时延；为 ``false`` 时同步
       写入。默认值为 ``true``。

    .. cpp:function:: Builder &LogAsyncQueueSize(uint32_t size)

       设置异步日志队列大小。取值范围 ``[256, 1048576]``，默认值为 ``1024``。
       对应环境变量 ``DATASYSTEM_LOG_ASYNC_BUFFER_MB``。

    .. cpp:function:: Builder &LogMonitorEnable(bool enable)

       设置是否开启接口日志统计。开启后会统计每个请求的时延。默认值为 ``true`` （开启）。
       对应环境变量 ``DATASYSTEM_LOG_MONITOR_ENABLE``。

    .. cpp:function:: Builder &MonitorConfigPath(const std::string &path)

       设置运行时配置文件监控路径。worker 启动后会监控该文件的变化并热加载 flag 变更。
       对应环境变量 ``DATASYSTEM_CLIENT_CONFIG_PATH``，默认值为
       ``~/datasystem/config/datasystem.config``。设为空字符串时禁用文件监控，
       此时可通过 :cpp:func:`KVClient::UpdateConfig` 动态更新配置；两者互斥，不能同时使用。

    .. cpp:function:: Builder &ZmqClientIoContext(int32_t threads)

       设置 ZMQ 客户端 IO context 数量。取值范围 ``[1, 128]``，默认值为 ``1``。

    .. cpp:function:: Builder &ZmqClientIoThread(int32_t threads)

       设置 ZMQ 客户端 IO 线程数量，数值与系统吞吐量正相关。取值范围 ``[1, 32]``，
       默认值为 ``1``。对应环境变量 ``DATASYSTEM_ZMQ_CLIENT_IO_THREAD``。

    .. cpp:function:: Status Build(KVClientConfig &config) const

       校验并构建 :cpp:class:`KVClientConfig`。

       参数：
           - **config** - 传出参数，返回构建后的配置对象。

       返回：
           返回值状态码为 ``StatusCode::K_OK`` 时表示构建成功，否则返回参数校验错误。
