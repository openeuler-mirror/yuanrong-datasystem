# Client环境变量

数据系统Client SDK支持通过环境变量进行配置。以下是所有支持的环境变量参数说明。

## 连接配置

| 序号 | 名称 | 默认值 | 含义/用途 |
|------|------|--------|-----------|
| 2 | `DATASYSTEM_HOST` | `""` | 设置数据系统Client SDK连接的worker IP地址。如果SDK中没有配置,则读取该环境变量获取host值。 |
| 3 | `DATASYSTEM_PORT` | `""` | 设置数据系统Client SDK连接的worker端口。如果SDK中没有配置,则读取该环境变量获取port值。 |
| 4 | `DATASYSTEM_CONNECT_TIME_MS` | `60000` | 设置与worker建立连接超时的时间(毫秒)。如果超出该时间还未成功连接,则返回超时异常。如果SDK中没有配置,则读取该环境变量获取值,环境变量读取不到则赋值为60000。 |

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
| 11 | `DATASYSTEM_LOG_V` | `0` | 数据系统SDK的日志级别。 |
| 12 | `DATASYSTEM_CLIENT_MAX_LOG_SIZE` | `100MB` | 日志文件最大大小,默认100MB。 |
| 13 | `DATASYSTEM_LOG_TO_STDERR` | `false` | 为true时,日志信息输出到stderr而不是log_file。 |
| 14 | `DATASYSTEM_ALSO_LOG_TO_STDERR` | `false` | 为true时,日志信息输出到stderr,同时输出到文件。 |
| 15 | `DATASYSTEM_STD_THRESHOLD` | `10` | 除了日志文件之外,还将等于或高于此级别的日志消息复制到stderr。 |
| 16 | `DATASYSTEM_LOG_RETENTION_DAY` | `0` | 日志保留时间(天),默认0表示不清除日志。 |
| 17 | `DATASYSTEM_LOG_ASYNC_BUFFER_MB` | `2` | 异步日志缓冲区大小,单位为MB,默认2MB。 |
| 18 | `DATASYSTEM_MAX_LOG_FILE_NUM` | `5` | 日志文件最大数量,默认5。 |
| 19 | `DATASYSTEM_LOG_COMPRESS` | `true` | 日志文件是否压缩,默认压缩。 |
| 23 | `DATASYSTEM_MIN_LOG_LEVEL` | `0` | 用于设置日志记录的最低级别,默认是0。 |
| 25 | `DATASYSTEM_LOG_RATE_LIMIT` | `0` | 每秒采样请求数上限（0表示不限速）。仅对带 traceId 的请求日志生效：被采样到的请求会完整打印链路日志，未采样请求的链路日志会被整体丢弃（包含 WARNING/ERROR/FATAL）。 |

## 运行时环境配置

| 序号 | 名称 | 默认值 | 含义/用途 |
|------|------|--------|-----------|
| 21 | `POD_NAME` | `""` | K8S部署时的容器名,用于记录日志。 |
| 22 | `HOSTNAME` | `""` | 主机名,用于记录日志。 |

## 监控配置

| 序号 | 名称 | 默认值 | 含义/用途 |
|------|------|--------|-----------|
| 24 | `DATASYSTEM_LOG_MONITOR_ENABLE` | `true` | 是否开启接口日志统计,默认开启。 |

## 使用说明

1.环境变量的优先级低于SDK内部配置。如果SDK中已经配置了某个参数,则不会读取该环境变量;只有当SDK中没有配置该参数时,才会从环境变量中读取对应的值。
2.日志配置相关环境变量只有配置了某个参数后，才会从环境变量中读取对应的值。

设置环境变量的示例:

```bash
export DATASYSTEM_HOST=127.0.0.1
export DATASYSTEM_PORT=8080
export DATASYSTEM_LOG_V=3
```
