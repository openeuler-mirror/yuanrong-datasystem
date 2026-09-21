# 现网 Client / Worker 日志收集指南

本文用于在 512 节点 Kubernetes 集群中收集 `datasystem_worker` 和 kvtest Client 日志。命令默认在 `tests/kvtest` 目录执行，并按默认 namespace 展示。

执行前确认脚本版本支持相关选项：

```bash
python3 deploy_worker.py collect --help
python3 deploy_client.py collect --help
```

## 1. 现网使用建议

- 512 节点建议从 `--max-workers 64` 开始。`16` 较保守，适合业务高峰或单节点日志很大；`128` 适合日志量较小且已确认 API Server、网络和本机资源有余量的场景。
- 首次使用先加 `--count 1` 验证文件名、日志目录和关键字，再扩大范围。
- 大范围收集建议按 128 个目标分批，每批并发 64。
- Kubernetes 场景建议增加 `--pod-info`，在目录名中记录 Pod、Pod IP 和 Host IP。
- 每次使用新的 `-o` 目录，避免与历史结果混合。
- 优先在远端用文件名和关键字筛选，减少传输量。
- 日志收集期间可能发生轮转，各节点结果不是同一时刻的快照。

上述并发值是现网操作建议，尚未在目标 512 节点集群完成基准测试。若出现 API Server 限流、超时或本机负载过高，应降低并发。

## 2. 功能总览

| 功能 | Worker | Client |
| --- | --- | --- |
| 全量收集 | 支持 | 支持 |
| 按文件名筛选 | `--file-pattern` | `--file-pattern` / `--log-pattern` |
| 按日志内容筛选 | `--keyword` | `--keyword` |
| 关键字匹配工具 | `--keyword-engine {grep,rg}` | `--keyword-engine {grep,rg}` |
| 排除压缩源文件 | `--uncompressed-only` | `--uncompressed-only` |
| 只收 Client SDK 日志 | 不适用 | `--sdk-only` |
| Pod 前缀筛选 | `-p/--prefix` | `-p/--prefix` |
| 精确目标筛选 | `--pod-names` | `--instance-ids` |
| Host IP 筛选 | `--host-filter` | `--host-filter` |
| 记录 Pod/IP/HostIP | `--pod-info` | `--pod-info` |
| 分批收集 | `--count`、`--offset` | `--count`、`--offset` |
| 限制并发 | `--max-workers` | `--max-workers` |
| 传输方式 | kubectl | kubectl、SSH、localhost |

## 3. 筛选规则

### 3.1 文件名

`--file-pattern` 和 `--log-pattern` 使用大小写敏感的 glob，不是正则表达式。通配符应加单引号。

```text
*.log             以 .log 结尾的文件
*INFO.log         以 INFO.log 结尾的文件
*access.log       以 access.log 结尾的文件
*access.log.gz    以 access.log.gz 结尾的文件
logs/*/INFO.log   模式含 / 时，按日志根目录下的相对路径匹配
```

同一选项重复传入时为 OR：

```bash
--file-pattern '*access.log' --file-pattern '*access.log.gz'
```

### 3.2 关键字

`--keyword` 是大小写敏感的 UTF-8 字面子串匹配，目前不支持正则表达式。多个关键字为 OR；文件名与关键字之间为 AND。默认使用远端 `grep -F`；传 `--keyword-engine rg` 可改用 `rg -F`。

```bash
--file-pattern '*INFO.log' --keyword 'ERROR' --keyword 'TIMEOUT'
```

以上表示只扫描 `*INFO.log`，保留包含 `ERROR` 或 `TIMEOUT` 的行。输出文件追加 `.matched`，不包含上下文行。

关键字筛选支持未压缩文本及 `.gz`、`.bz2`、`.xz`。对 `.zip`、`.zst`、`.lz4`、`.tgz`、`.tar`、`.7z`、`.rar` 等格式会报错，不会退回全量下载。

目标 Pod 或主机必须安装所选匹配工具。未压缩文件由工具直接读取；压缩文件由 Python 流式解压后送入工具。工具缺失或执行失败时，该目标收集失败，不会回退到 Python 匹配。

使用 `--keyword` 且没有命中时：

- Worker 不创建该 Pod 的本地目录，也不保存该 Pod 的 `worker_config.json`；
- Client 不创建该实例的本地目录；
- Client 未使用 `--sdk-only` 时，顶层 deploy/config 归档仍可能存在。

因此在 512 节点中只有少量 Pod 命中时，输出目录只保留实际命中的节点。

### 3.3 未压缩与未轮转

`--uncompressed-only` 只排除压缩源文件，仍会收集 `INFO.log.1` 等未压缩轮转文件。

- 只排除压缩文件：`--uncompressed-only`
- 只收当前 `.log`：`--file-pattern '*.log'`
- Client SDK 只收当前 `.log`：`--sdk-only --log-pattern '*.log'`

筛选模式会固定排除 `env`、`procmon.py`、符号链接和其他非普通文件。

## 4. Worker 日志收集

Worker 至少需要一个 Pod 前缀或精确 Pod 名称：

```bash
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  [筛选选项] \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>
```

主要参数：

| 参数 | 说明 |
| --- | --- |
| `-p/--prefix` | Pod 名称前缀，可重复；多个前缀为 OR |
| `--pod-names` | 精确 Pod 名，可重复或用空格分隔；与前缀取并集 |
| `-n/--namespace` | Kubernetes namespace，默认 `default` |
| `--host-filter` | 按 Host IP 包含或排除目标 |
| `--pod-info` | 目录名追加 Pod IP 和 Host IP |
| `--file-pattern` | 文件名 glob，可重复 |
| `--keyword` | 字面关键字，可重复 |
| `--keyword-engine` | `grep`（默认）或 `rg` |
| `--uncompressed-only` | 排除压缩源文件 |
| `--count` / `--offset` | 对按 Pod 名排序的目标分批 |
| `--max-workers` | 最大并发任务数 |
| `--log-dir` | 覆盖 Worker 日志根目录 |
| `--remote-config` | Pod 内 Worker 配置，默认 `/tmp/worker.config` |
| `--remote-dir` | standalone 目录，默认 `/tmp/ds_worker` |
| `--timeout` | 单次操作超时，默认 300 秒 |
| `-o/--output` | 本地输出目录 |

Worker 普通收集会获取日志目录顶层的 `.log`、`.log.gz`、`.txt`，以及存在的 `resource_monitor.csv`、standalone `stdout.log` 和 Worker 配置。筛选收集会递归扫描日志目录；默认候选为 `*.log`、`*.log.*`、`*.txt` 和 `resource_monitor.csv`。未指定 `--log-dir` 时从远端 Worker 配置读取日志目录。

使用 `--pod-info` 后目录格式为：

```text
<pod>__podip-<pod-ip>__hostip-<host-ip>
```

常用命令：

```bash
# 从 INFO.log 中收集命中关键字的行
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  --file-pattern '*INFO.log' \
  --keyword '<关键字>' \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>

# 只收当前未轮转的 .log
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  --file-pattern '*.log' \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>

# 收集所有未压缩文件，包括 .log.1 等未压缩轮转文件
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  --uncompressed-only \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>
```

## 5. Client 日志收集

```bash
python3 deploy_client.py collect <deploy.json> <config.json> \
  [筛选选项] \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>
```

主要参数：

| 参数 | 说明 |
| --- | --- |
| `-p/--prefix` | 按 Client Kubernetes Pod 前缀筛选，可重复 |
| `--instance-ids` | 按 `deploy.json` 中的实例 ID 精确筛选 |
| `--host-filter` | 按 Host IP 包含或排除目标 |
| `--pod-info` | Kubernetes 目录名追加 Pod IP、Host IP 和实例 ID |
| `--file-pattern` | 对所有 Client 日志来源使用文件名 glob |
| `--log-pattern` | Client SDK 日志 glob，常与 `--sdk-only` 配合 |
| `--keyword` | 字面关键字，可重复 |
| `--keyword-engine` | `grep`（默认）或 `rg` |
| `--uncompressed-only` | 排除压缩源文件 |
| `--sdk-only` | 只扫描 SDK 日志，跳过 summary、case 配置和 kvtest 输出 |
| `--sdk-log-dir` | SDK 日志目录，默认 `/root/.datasystem/logs` |
| `--count` / `--offset` | 对按 host:instance 排序的目标分批 |
| `--max-workers` | 最大并发任务数 |
| `--summary-timeout` | summary 接口超时 |
| `-o/--output` | 本地输出目录 |

无筛选时，Client 会收集测试输出、指标目录、`run.log`、资源监控文件、SDK 顶层 `.log`/`.log.gz`/`.txt`，并请求每个节点的 `/summary`，同时保存 deploy/config 配置。使用任意过滤选项或 `--sdk-only` 时会跳过 `/summary`；`--sdk-only` 还会跳过 case 配置和 kvtest 输出。

Client 的连接方式由 `deploy.json` 决定：Kubernetes 使用 kubectl，远端主机使用 SSH，本机实例使用 localhost。过滤模式要求目标环境有 Python 3。

Kubernetes 使用 `--pod-info` 后目录格式为：

```text
<pod>__podip-<pod-ip>__hostip-<host-ip>__client-<instance-id>
```

SSH 和 localhost 仍使用 `<host>_<instance-id>`。

常用命令：

```bash
# 只收 Client 接口日志，包括当前文件和 gzip 轮转文件
python3 deploy_client.py collect <deploy.json> <config.json> \
  --sdk-only \
  --log-pattern '*access.log' \
  --log-pattern '*access.log.gz' \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>

# 从 Client INFO.log 中收集命中关键字的行
python3 deploy_client.py collect <deploy.json> <config.json> \
  --sdk-only \
  --log-pattern '*INFO.log' \
  --keyword '<关键字>' \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>

# 只收 Client 当前未轮转的 SDK .log
python3 deploy_client.py collect <deploy.json> <config.json> \
  --sdk-only \
  --log-pattern '*.log' \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>

# 收集 Client 全部来源中的未压缩文件
python3 deploy_client.py collect <deploy.json> <config.json> \
  --uncompressed-only \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>

# 只收 Client SDK 中的未压缩文件，包括 .log.1
python3 deploy_client.py collect <deploy.json> <config.json> \
  --sdk-only \
  --uncompressed-only \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>
```

## 6. 按 Host IP 筛选

`--host-filter` 接受 JSON 文件，`include` 和 `exclude` 使用完整 IPv4/IPv6 地址，`exclude` 优先。

```json
{
  "include": ["10.0.0.11", "10.0.0.12"],
  "exclude": ["10.0.0.12"]
}
```

```bash
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  --host-filter host_filter.json \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>

python3 deploy_client.py collect <deploy.json> <config.json> \
  --host-filter host_filter.json \
  --pod-info \
  --max-workers 64 \
  -o <全新输出目录>
```

筛选顺序如下：

- Worker：Host IP → Pod 前缀/精确 Pod → `offset/count`；
- Client：Host IP → Pod 前缀 → instance ID → `offset/count`。

Worker 的 Pod 前缀与精确 Pod 名取并集；Client 同时指定 Pod 前缀和 instance ID 时取交集。

## 7. 512 节点分批模板

建议每批 128 个目标、批内并发 64。目标在筛选后排序，因此四批分别使用以下参数：

```text
--count 128 --offset 0
--count 128 --offset 128
--count 128 --offset 256
--count 128 --offset 384
```

Worker 示例：

```bash
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  --count 128 \
  --offset <0|128|256|384> \
  --max-workers 64 \
  --pod-info \
  -o <每批独立输出目录>
```

Client 示例：

```bash
python3 deploy_client.py collect <deploy.json> <config.json> \
  --count 128 \
  --offset <0|128|256|384> \
  --max-workers 64 \
  --pod-info \
  -o <每批独立输出目录>
```

若 64 并发稳定且收集内容较小，可试运行 `--max-workers 128`；若出现超时、限流、网络拥塞或磁盘压力，降为 32 或 16。

## 8. 结果与失败判断

- 每个目标独立执行，单个目标失败不会中止其他目标。
- 筛选后无文件或关键字无命中属于正常空结果。
- 关键字零命中时不会创建空的 Pod/Client 实例目录。
- 任一目标筛选失败时命令返回非零，应检查终端汇总和失败目标。
- 普通远端收集依赖 `tar` 等基础工具；筛选收集要求远端 Python 3，关键字筛选还要求 `grep`，选择 `rg` 时要求安装 ripgrep。
- 使用 `--pod-info` 才会把 Kubernetes Pod IP 和 Host IP 写入目录名。

## 9. 现网推荐命令模板

按实际环境替换尖括号内容，每次使用新的输出目录。

```bash
# Worker：从 INFO.log 中按关键字收集
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  --file-pattern '*INFO.log' \
  --keyword '<关键字>' \
  --pod-info \
  --max-workers 64 \
  -o <输出目录>

# Client：只收接口 access.log 和 access.log.gz
python3 deploy_client.py collect <deploy.json> <config.json> \
  --sdk-only \
  --log-pattern '*access.log' \
  --log-pattern '*access.log.gz' \
  --pod-info \
  --max-workers 64 \
  -o <输出目录>

# Worker：只收当前未轮转 .log
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  --file-pattern '*.log' \
  --pod-info \
  --max-workers 64 \
  -o <输出目录>

# Client：只收当前未轮转的 SDK .log
python3 deploy_client.py collect <deploy.json> <config.json> \
  --sdk-only \
  --log-pattern '*.log' \
  --pod-info \
  --max-workers 64 \
  -o <输出目录>

# Worker：收集全部未压缩文件，包括未压缩轮转文件
python3 deploy_worker.py collect \
  -p <worker-pod-prefix> \
  --uncompressed-only \
  --pod-info \
  --max-workers 64 \
  -o <输出目录>

# Client：收集全部来源中的未压缩文件
python3 deploy_client.py collect <deploy.json> <config.json> \
  --uncompressed-only \
  --pod-info \
  --max-workers 64 \
  -o <输出目录>
```
