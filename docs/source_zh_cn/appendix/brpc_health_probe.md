# brpc 健康探针与诊断接口

适用启用 brpc 模式（`FLAGS_use_brpc=true`）的 worker。brpc server 内置一组 HTTP 服务，可用于线上诊断与可选的 K8s 探针。**内置服务默认关闭**（由 `FLAGS_brpc_enable_builtin_services` 控制，见下文注意事项）；开启后无需额外组件即可用于诊断与探针。

## 端口说明

brpc server 监听 worker 的对外端口（与业务 RPC 同端口，brpc 内置 HTTP 服务复用该端口）。下文示例用 `31501`，实际填 `worker_address` 中的端口；单机默认常为 `127.0.0.1:31501`，跨机部署用各节点 `worker_address` 的真实 IP:Port。

## K8s 探针

> 本仓默认 K8s 部署使用 `exec` 文件检查探针（不依赖 brpc 内置服务，见注意事项）。下文 `httpGet` 方式为**可选**方案，仅当设 `FLAGS_brpc_enable_builtin_services=true` 开启内置服务时可用。

### livenessProbe

判断 worker 进程是否存活。brpc `/health` 在 server 启动后即返回 200，进程崩溃则探针失败、触发 Pod 重启。

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 31501
  initialDelaySeconds: 10   # 给 worker + etcd 初始化留时间
  periodSeconds: 10
  failureThreshold: 3
```

### readinessProbe

判断 worker 是否就绪可接流量。`/status` 返回 server 运行状态，比 `/health` 更能反映"能否处理请求"。readiness 失败时 K8s 把 Pod 从 Service endpoints 摘除，不再转发流量，但**不重启** Pod。

```yaml
readinessProbe:
  httpGet:
    path: /status
    port: 31501
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 2
```

> liveness 与 readiness 用不同路径是有意为之：liveness 只关心"进程还在"（避免误重启），readiness 关心"能干活"（可摘流量）。

## 诊断接口速查

| 路径 | 用途 | 典型场景 |
|---|---|---|
| `/health` | K8s liveness 探针 | Pod 是否存活 |
| `/status` | server 运行状态概览 | readiness / 快速体检 |
| `/vars` | bvar 文本格式（人读，非 Prometheus） | 快速浏览 QPS / P99 / 错误率 / 连接数 |
| `/prometheus_metrics` | Prometheus 格式导出 | 接入 Prometheus 抓取（非 `/vars`） |
| `/rpcz` | RPC trace 耗时分解 | 哪个 RPC 变慢、卡在哪一步 |
| `/bthreads` | 所有 bthread 栈 | 死锁 / 卡死诊断 |
| `/pprof` | CPU / heap profile | 内存泄漏 / CPU 热点 |
| `/connections` | 连接状态 | 连接池健康、长连接排查 |
| `/flags` | 运行时 gflag 值 | 确认配置是否生效 |

## 常用诊断命令

```bash
# 快速体检（worker 本机）
curl -s http://127.0.0.1:31501/health
curl -s http://127.0.0.1:31501/status | head

# 浏览 bvar 指标（QPS/P99/错误率/连接数，人读文本格式）
curl -s http://127.0.0.1:31501/vars | less

# Prometheus 抓取（注意是 /prometheus_metrics，不是 /vars）
curl -s http://127.0.0.1:31501/prometheus_metrics

# 抓 30s CPU profile（需安装 go tool pprof 或用 pprof-cpu.pb.gz 离线分析）
curl -s 'http://127.0.0.1:31501/pprof/profile?seconds=30' -o cpu.pb.gz

# 查看所有 bthread 栈（卡死时最有用）
curl -s http://127.0.0.1:31501/bthreads | head -50
```

## 注意事项

- **brpc 内置服务默认关闭**，对齐 ZMQ 模式的安全基线（无 HTTP 管理端点）。`/flags` 可 POST 翻转任意 gflag，`/pprof/heap` 可 dump 内存，默认关闭可防止经业务 RPC 端口越权操作。
- 由 gflag `FLAGS_brpc_enable_builtin_services` 控制（默认 `false`）。仅在受信网络调试时设为 `true` 开启 `/flags`、`/pprof`、`/vars` 等诊断接口，无需改代码重新编译。
- K8s 健康探针**不依赖** brpc 内置服务。本仓 K8s 部署（`worker_daemonset.yaml`）的 startup/readiness/liveness probe 均为 `exec` 文件检查（`file_check.sh` / `liveness_check.sh -f $(LIVENESS_CHECK_PATH)`，检查 `/home/yuanrong/datasystem/health/{healthy,ready,liveness}` 文件），与 `has_builtin_services` 是否开启无关。
- 仅当希望用 brpc HTTP `/health`、`/status` 作为探针时，才需设 `FLAGS_brpc_enable_builtin_services=true` 并参考上文 `httpGet` 示例；此时 `/health` 与 `/status` 是只读 GET，不鉴权、不产生业务负载。
- 探针端口若与业务端口相同，注意 NetworkPolicy / 端口策略放行 K8s node→Pod 的 31501。
