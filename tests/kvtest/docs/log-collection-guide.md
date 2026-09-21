# kvtest 日志定向收集命令

本文给出 `deploy_worker.py` 和 `deploy_client.py` 的常用日志定向收集命令，覆盖以下场景：

- 从 Worker `INFO.log` 中按关键字收集日志行；
- 只收集 Client 的 `*access.log` 和 `*access.log.gz`；
- 只收集 Worker 未轮转的 `.log`；
- 只收集 Client 未轮转的 `.log`。

所有示例均使用 `--pod-info`。启用后，本地节点目录同时注明 Pod 名、Pod IP 和 Host IP：

```text
<pod-name>__podip-<pod-ip>__hostip-<host-ip>
```

## 参数约定

以下示例假设：

| 参数 | 示例值 | 说明 |
| --- | --- | --- |
| Worker Pod 前缀 | `ds-worker` | 按实际 Pod 名修改 |
| Kubernetes namespace | `datasystem` | 按实际 namespace 修改 |
| Client 部署配置 | `deploy.json` | `deploy_client.py gen-config` 生成的部署文件 |
| Client SDK 日志目录 | `/root/.datasystem/logs` | 可通过 `--sdk-log-dir` 修改 |

文件通配符按文件名区分大小写匹配。重复指定 `--file-pattern`、`--log-pattern` 或 `--keyword` 时，多个值之间是 OR 关系。

## 从 Worker INFO.log 中按关键字收集

```bash
python3 deploy_worker.py collect \
  -p ds-worker \
  -n datasystem \
  --file-pattern '*INFO.log' \
  --keyword '关键字A' \
  --keyword '关键字B' \
  --pod-info \
  -o collected_worker_info
```

该命令只扫描文件名匹配 `*INFO.log` 的日志，并保留包含任一关键字的行。过滤在 Worker Pod 内执行，本地收到的过滤结果文件以 `.matched` 结尾。

当前 `--keyword` 执行大小写敏感的字面子串匹配，不解析正则表达式。例如，要匹配包含 `ERROR` 或 `TIMEOUT` 的行，应重复传入两个参数：

```bash
--keyword 'ERROR' --keyword 'TIMEOUT'
```

`ERROR|TIMEOUT` 和 `request_id=[0-9]+` 会被当作普通文本，不具有正则含义。

## 只收集 Client access 日志及 gzip 文件

```bash
python3 deploy_client.py collect deploy.json \
  --sdk-only \
  --log-pattern '*access.log' \
  --log-pattern '*access.log.gz' \
  --pod-info \
  -o collected_client_access
```

`--sdk-only` 将收集源限制为 Client SDK 日志目录，并跳过 summary 生成、case 配置复制和 kvtest 输出。匹配结果保存在每个节点目录的 `sdk/` 子目录。

上述模式只匹配以 `access.log` 或 `access.log.gz` 结尾的文件。如果还需要收集 `access.log.1.gz` 等带编号的 gzip 轮转文件，可改为：

```bash
--log-pattern '*access*.gz'
```

若日志不在默认目录，增加：

```bash
--sdk-log-dir /实际日志目录
```

## 只收集 Worker 未轮转日志

```bash
python3 deploy_worker.py collect \
  -p ds-worker \
  -n datasystem \
  --file-pattern '*.log' \
  --pod-info \
  -o collected_worker_current
```

`*.log` 匹配当前日志，例如 `INFO.log`、`access.log` 和 `stdout.log`，不会匹配 `INFO.log.1`、`INFO.log.gz` 或 `access.log.1.gz`。

Worker 收集命令还会在节点目录保存 `worker_config.json`，用于记录本次日志对应的 Worker 配置。

## 只收集 Client 未轮转日志

```bash
python3 deploy_client.py collect deploy.json \
  --sdk-only \
  --log-pattern '*.log' \
  --pod-info \
  -o collected_client_current
```

该命令只扫描 Client SDK 日志目录，并收集以 `.log` 结尾的当前日志，不收集 `.log.gz`、`.log.1` 或 `.log.1.gz`。

## 常用范围选项

Worker 可通过以下选项缩小 Pod 范围：

```bash
-p <Pod前缀>
--pod-names '<完整Pod名1> <完整Pod名2>'
-n <namespace>
```

Client 可通过以下选项缩小实例范围：

```bash
-p <Pod前缀>
--instance-ids <实例ID1> <实例ID2>
```

大规模集群可使用 `--max-workers` 限制并发收集数，并使用 `--count`、`--offset` 分批处理。
