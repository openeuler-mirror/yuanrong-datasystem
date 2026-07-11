# Client环境变量

数据系统Client SDK支持通过环境变量进行配置。以下是所有支持的环境变量参数说明。

## 连接配置

| 序号 | 名称 | 默认值 | 含义/用途 |
|------|------|--------|-----------|
| 2 | `DATASYSTEM_HOST` | `""` | 设置数据系统Client SDK连接的worker IP地址。如果SDK中没有配置,则读取该环境变量获取host值。 |
| 3 | `DATASYSTEM_PORT` | `""` | 设置数据系统Client SDK连接的worker端口。如果SDK中没有配置,则读取该环境变量获取port值。 |
| 4 | `DATASYSTEM_CONNECT_TIME_MS` | `9000` | 设置与worker建立连接超时的时间(毫秒)。如果超出该时间还未成功连接,则返回超时异常。如果SDK中没有配置,则读取该环境变量获取值,环境变量读取不到则赋值为9000。 |
| 27 | `DATASYSTEM_ZMQ_CLIENT_IO_THREAD` | `1` | ZMQ客户端IO线程数，其数值与系统吞吐量正相关，取值范围：[1, 32]。通过该环境变量可调整客户端ZMQ上下文的IO线程数。 |

## 安全认证配置

| 序号 | 名称 | 默认值 | 含义/用途 |
|------|------|--------|-----------|
| 5 | `DATASYSTEM_TOKEN` | `""` | 开启租户鉴权场景时,必须指定一个租户的token以进行租户鉴权。如果SDK中没有配置,则读取该环境变量获取值,环境变量读取不到则赋值为空。 |
| 6 | `DATASYSTEM_CLIENT_PUBLIC_KEY` | `""` | 设置client与worker连接通信的公钥。如果SDK中没有配置,则读取该环境变量获取值,环境变量读取不到则赋值为空。 |
| 7 | `DATASYSTEM_CLIENT_PRIVATE_KEY` | `""` | 设置client与worker连接通信的私钥。如果SDK中没有配置,则读取该环境变量获取值,环境变量读取不到则赋值为空。 |
| 8 | `DATASYSTEM_SERVER_PUBLIC_KEY` | `""` | 设置client与worker连接通信的worker的公钥。如果SDK中没有配置,则读取该环境变量获取值,环境变量读取不到则赋值为空。 |
| 9 | `DATASYSTEM_ACCESS_KEY` | `""` | 设置AK(Access Key Id)。用于标识用户,数据系统使用该标志用来区分该请求是来自系统组件还是租户组件。如果SDK中没有配置,则读取该环境变量获取值,环境变量读取不到则赋值为空。 |
| 10 | `DATASYSTEM_SECRET_KEY` | `""` | 设置SK(Secret Access Key)。用户用于加密认证字符串和用来验证认证字符串的密钥,其中SK必须保密。数据系统使用该密钥对请求进行加密,防止请求被篡改。如果SDK中没有配置,则读取该环境变量获取值,环境变量读取不到则赋值为空。 |
| 20 | `DATASYSTEM_HTTP_VERIFY_CERT` | `True` | 访问IAM鉴权服务器时是否校验证书。 |

## 日志配置

| 序号 | 名称 | 默认值 | 含义/用途 |
|------|------|--------|-----------|
| 1 | `DATASYSTEM_CLIENT_LOG_DIR` | `~/.datasystem/logs` | 数据系统SDK日志的输出文件夹。 |
| 11 | `DATASYSTEM_LOG_V` | `0` | 数据系统SDK的 `VLOG` 级别。数值越大，输出的调试日志越详细；一般用于排查内部细节，默认值为0。 |
| 12 | `DATASYSTEM_CLIENT_MAX_LOG_SIZE` | `100MB` | 日志文件最大大小,默认100MB。 |
| 13 | `DATASYSTEM_LOG_TO_STDERR` | `false` | 为true时,日志信息输出到stderr而不是log_file。 |
| 14 | `DATASYSTEM_ALSO_LOG_TO_STDERR` | `false` | 为true时,日志信息输出到stderr,同时输出到文件。 |
| 15 | `DATASYSTEM_STD_THRESHOLD` | `3` | 控制哪些级别的日志同时复制到stderr。取值范围`[0, 3]`，数值含义同`DATASYSTEM_MIN_LOG_LEVEL`（`0`=INFO、`1`=WARNING、`2`=ERROR、`3`=FATAL）；默认`3`表示只有FATAL输出到stderr。 |
| 16 | `DATASYSTEM_LOG_RETENTION_DAY` | `0` | 日志保留时间(天),默认0表示不清除日志。 |
| 17 | `DATASYSTEM_LOG_ASYNC_BUFFER_MB` | `2` | 异步日志缓冲区大小,单位为MB,默认2MB。 |
| 18 | `DATASYSTEM_MAX_LOG_FILE_NUM` | `5` | 日志文件最大数量,默认5。 |
| 19 | `DATASYSTEM_LOG_COMPRESS` | `true` | 日志文件是否压缩,默认压缩。 |
| 23 | `DATASYSTEM_MIN_LOG_LEVEL` | `0` | 数据系统SDK的最小日志级别。数值越大，输出的日志越少；默认值为0，通常表示INFO级别及以上日志都可输出。 |
| 26 | `DATASYSTEM_LOG_ONLY_WRITE_INFO_FILE` | `true` | INFO日志文件始终写入所有级别日志。该值为`true`时不额外生成WARNING/ERROR日志文件；为`false`时会额外生成WARNING/ERROR日志文件，高级别日志会按等级写入多个日志文件。 |
| 28 | `DATASYSTEM_CLIENT_LOG_WITHOUT_PID` | `true` | 是否让客户端日志文件名不带进程号。默认`true`时输出为`ds_client.log`、`ds_client_access.log`；设置为`false`时恢复为带`<pid>`后缀的命名，适合多client进程同时运行且不希望覆盖日志的场景。 |

## 运行时环境配置

| 序号 | 名称 | 默认值 | 含义/用途 |
|------|------|--------|-----------|
| 21 | `POD_NAME` | `""` | K8S部署时的容器名,用于记录日志。 |
| 22 | `HOSTNAME` | `""` | 主机名,用于记录日志。 |

## 监控配置

| 序号 | 名称 | 默认值 | 含义/用途 |
|------|------|--------|-----------|
| 24 | `DATASYSTEM_LOG_MONITOR_ENABLE` | `true` | 是否开启接口日志统计,默认开启。 |

## 客户端运行参数

以下 gflag 参数可通过 `DATASYSTEM_CLIENT_CONFIG_PATH` 配置文件设置，支持热更新。

| 序号 | 名称 | 默认值 | 取值范围 | 含义/用途 |
|------|------|--------|----------|-----------|
| 29 | `urma_failover_success_rate_ratio` | `0.5` | `[0.0, 1.0]` | Client 侧 URMA 数据面成功率阈值。一个完整统计窗口的成功率严格低于该 ratio 时，client 尝试切换 worker；设置为 `0.0` 表示关闭 URMA 故障切换功能，同时清空当前窗口状态。 |
| 30 | `urma_failover_min_sample_count` | `5` | `> 0` | 每个 `client_dead_timeout_s` 统计窗口内进行故障切换判断前所需的最小 URMA 数据面样本数。样本不足时即使成功率很低也不会触发切换，避免少量请求误触发。 |
| 31 | `client_slow_log_process_slower_than` | `2000` | `>= 0` | Client 侧处理阶段慢日志门限（微秒）。默认2000μs(2ms)；设为0禁用。可通过 `DATASYSTEM_CLIENT_CONFIG_PATH` 配置文件热更新。 |
| 32 | `client_slow_log_rpc_slower_than` | `5000` | `>= 0` | Client 侧 RPC 阶段慢日志门限（微秒）。默认5000μs(5ms)；设为0禁用。可通过 `DATASYSTEM_CLIENT_CONFIG_PATH` 配置文件热更新。 |

> **废弃说明**：旧版本通过环境变量 `DATASYSTEM_CLIENT_SLOW_LOG_PROCESS_SLOWER_THAN` 和 `DATASYSTEM_CLIENT_SLOW_LOG_RPC_SLOWER_THAN` 配置客户端慢日志阈值。自本版本起上述环境变量不再生效，请改用 gflag `--client_slow_log_process_slower_than` / `--client_slow_log_rpc_slower_than`，通过 `DATASYSTEM_CLIENT_CONFIG_PATH` 配置文件或 `UpdateConfig` API 设置。

## 使用说明

1.环境变量的优先级低于SDK内部配置。如果SDK中已经配置了某个参数,则不会读取该环境变量;只有当SDK中没有配置该参数时,才会从环境变量中读取对应的值。
2.日志配置相关环境变量只有配置了某个参数后，才会从环境变量中读取对应的值。
3.`DATASYSTEM_LOG_V` 控制 VLOG 详细程度，`DATASYSTEM_MIN_LOG_LEVEL` 控制常规日志最低输出级别，两者分别影响不同日志通道。
4.客户端运行参数通过 `DATASYSTEM_CLIENT_CONFIG_PATH` 配置文件设置。

设置环境变量的示例:

```bash
export DATASYSTEM_HOST=127.0.0.1
export DATASYSTEM_PORT=8080
export DATASYSTEM_LOG_V=3
```
