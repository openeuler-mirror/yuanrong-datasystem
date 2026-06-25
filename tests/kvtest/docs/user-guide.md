# kvtest 用户操作指南

`kvtest` 是一个独立的 C++ 分布式 KV 性能测试工具，用于对 yuanrong-datasystem 集群进行端到端的读写性能测试和缓存命中率验证。

## 三种使用模式

| 模式 | 说明 | 详细指南 |
|------|------|---------|
| **Pipeline 模式** | Writer/Reader 角色分离，持续运行，QPS 控制，HTTP 通知机制 | [pipeline-guide.md](pipeline-guide.md) |
| **Cache 模式** | cacheGetOrCreate 流水线，Key Pool 管理，动态命中率控制 | [cache-guide.md](cache-guide.md) |
| **Benchmark 模式** | 16 种 Set/Get 测试模式（含 mixed 混合 + MSet/MGet 批量），round-based 执行 | [benchmark-guide.md](benchmark-guide.md) |

**共同特性：** 多节点部署、内置指标采集（CSV + HTML 报告）、CPU 亲和性、远程部署。

---

## 1. 环境准备

### 1.1 依赖

| 依赖 | 版本要求 | 说明 |
|------|---------|------|
| datasystem SDK | 与目标集群同版本 | headers + libdatasystem.so |
| GCC | >= 10 | 支持 C++17 |
| CMake | >= 3.14 | 编译工具 |
| etcd | >= 3.5 | 集群注册发现 |
| datasystem Worker | 运行中 | 至少 1 个 Worker 节点 |

### 1.2 获取 SDK

在 datasystem 源码目录执行 Bazel 编译：

```bash
bazel build //bazel:datasystem_sdk --config=release
mkdir -p output/cpp
tar xf bazel-bin/bazel/datasystem_sdk.tar.gz -C output/cpp/ --strip-components=1
cp bazel-bin/libdatasystem.so output/cpp/lib/
```

### 1.3 编译

```bash
cd tests/kvtest
./build.sh                  # 使用默认 SDK 路径
./build.sh -s /path/to/sdk  # 指定 SDK 路径
./build.sh -c -j$(nproc)    # 清理重建
```

编译产物位于 `output/`：`kvtest` 可执行文件（静态链接 libstdc++）、`lib/libdatasystem.so`、`deploy_client.py` 等。

---

## 2. 配置文件详解

### 2.1 公共配置模板

> 以下为**所有模式共用**的参数（代码默认值）。Pipeline/Cache/Benchmark 各自的特有参数见对应章节。

```json
{
  "mode": "pipeline",
  "instance_id": 0,
  "listen_port": 9000,
  "etcd_address": "127.0.0.1:2379",
  "cluster_name": "",
  "host_id_env_name": "JD_HOST_IP",
  "connect_options": {
    "connect_timeout_ms": 1000,
    "request_timeout_ms": 20,
    "enable_cross_node_connection": true,
    "fast_transport_mem_size": "512MB"
  },
  "data_sizes": ["1MB"],
  "set_param": {"ttl_second": 0},
  "num_threads": 16,
  "metrics_interval_ms": 3000,
  "cpu_affinity": "",
  "numa_node": -1,
  "nodes": [],
  "peers": []
}
```

### 2.2 参数说明

#### 公共参数（所有模式）

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `mode` | string | 自动推断 | 运行模式：`"pipeline"` / `"cache"` / `"benchmark"`，不设置时根据其他参数自动推断 |
| `instance_id` | int | 0 | 实例唯一标识，同一测试中不可重复 |
| `listen_port` | int | 9000 | HTTP 服务端口（接收通知） |
| `etcd_address` | string | **必填** | etcd 地址，格式 `ip:port` |
| `cluster_name` | string | "" | 集群名，多集群环境区分 |
| `host_id_env_name` | string | "JD_HOST_IP" | Worker IP 环境变量名 |
| `connect_options.connect_timeout_ms` | int | 1000 | KVClient 连接超时（毫秒） |
| `connect_options.request_timeout_ms` | int | 20 | KVClient 请求超时（毫秒） |
| `connect_options.enable_cross_node_connection` | bool | true | 允许跨节点连接 standby Worker |
| `connect_options.fast_transport_mem_size` | string | "512MB" | 快速传输内存大小，支持 KB/MB/GB 后缀 |
| `data_sizes` | string[] | ["1MB"] | 数据大小列表，支持 KB/MB/GB 后缀，随机选取 |
| `set_param.ttl_second` | uint | 0 | Set 操作 TTL（秒），0 = 永不过期。所有模式通用 |
| `num_threads` | int | 16 | 工作线程数（Pipeline 写入线程 / Benchmark 并发线程 / Cache 读取线程） |
| `metrics_interval_ms` | int | 3000 | 指标刷新间隔（毫秒） |
| `metrics_file` | string | 自动生成 | CSV 输出文件名，默认 `outputDir/latency_timeseries.csv` |
| `cpu_affinity` | string | "" | CPU 亲和性，如 "0-7" 或 "0,2,4,6"，空 = 自动检测。所有模式（Pipeline/Cache/Benchmark）均生效 |
| `numa_node` | int | -1 | NUMA 节点绑定，-1=禁用，非负=绑定到指定 node。需链接 libnuma，未编译时自动回退到 `cpu_affinity` |
| `nodes` | NodeInfo[] | [] | 集群节点列表（自动生成 peers） |
| `peers` | string[] | [] | Peer 地址列表，如 `["http://host:port"]` |

#### Pipeline 模式参数

> 详细用法和示例见 [pipeline-guide.md](pipeline-guide.md)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `role` | string | "writer" | 角色：`"writer"` 或 `"reader"` |
| `pipeline` | string[] | ["setStringView"] | 写入流水线操作列表 |
| `notify_pipeline` | string[] | ["getBuffer"] | 收到通知后执行的操作列表 |
| `target_qps` | int | 100 | 目标 QPS，0 = 不限速；数组 = 多阶段压测 |
| `notify_count` | int | 10 | 每次操作后通知的 peer 数量 |
| `notify_interval_us` | int | 0 | 通知间隔（微秒），0 = 并行发送 |
| `enable_jitter` | bool | true | 是否启用随机抖动（错开请求时间） |
| `batch_keys_count` | int | 1 | 每次操作的 key 数量，用于 batch 操作 |
| `stage_duration_seconds` | int | 0 | 多阶段 QPS 时每阶段时长（秒） |

**可用操作类型：** `setStringView` / `getBuffer` / `exist` / `createBuffer` / `memoryCopy` / `setBuffer` / `mCreate` / `mSet` / `mGet` / `cacheGetOrCreate`

#### Cache 模式参数

> 详细用法和示例见 [cache-guide.md](cache-guide.md)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `key_pool_size` | int | 0 | Key 池初始大小，**0 = 禁用 Cache 模式** |
| `target_hit_rate` | double | 0.0 | 目标命中率，0 = 固定池大小，0.01~1.0 = 自动调整 |
| `max_key_pool_size` | int | 0 | Key 池上限，0 = 自动（key_pool_size × 20） |
| `inference_delay_ms` | int | 0 | 模拟推理延迟（毫秒），Cache miss 后的等待 |
| `warmup_retry_count` | int | 3 | 预热阶段每个 key 的最大重试次数 |
| `warmup_retry_delay_ms` | int | 1000 | 预热重试间隔（毫秒） |
| `warmup_timeout_seconds` | int | 300 | Reader 等待所有 Writer 预热完成的超时 |

#### Benchmark 模式参数

> 详细用法和示例见 [benchmark-guide.md](benchmark-guide.md)

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `test_mode` | string | — | 测试模式（16 种），详见 [benchmark-guide.md](benchmark-guide.md) |
| `worker_memory_mb` | int | 0 | Worker 共享内存上限（MB），用于计算每轮 key 数 |
| `duration_seconds` | int | 0 | 总运行时长（秒），0 = 不限时 |
| `total_rounds` | int | 0 | 总轮数，0 = 不限轮 |
| `set_api` | string | "string_view" | Set API 路径：`"string_view"` 或 `"create_buffer"` |
| `cleanup_method` | string | "del" | 清理方式：`"del"`（每轮删除）或 `"ttl"`（TTL 过期） |
| `remote_worker.host` | string | "" | 远端 Worker 地址（部分模式必填） |
| `remote_worker.port` | int | 31501 | 远端 Worker 端口 |

---

## 3. 远程多节点部署

### 3.1 deploy_client.py 用法

```bash
python3 deploy_client.py deploy deploy.json config.json  # 部署 + 启动
python3 deploy_client.py stop deploy.json                 # 停止
python3 deploy_client.py collect deploy.json              # 收集结果
python3 deploy_client.py clean deploy.json                # 清理
```

### 3.2 部署配置文件

**`deploy.json` 示例（SSH 部署）：**
```json
{
  "remote_work_dir": "/tmp/kvclient_test",
  "transport": "ssh",
  "ssh_user": "root",
  "ssh_options": "-o StrictHostKeyChecking=no",
  "enable_procmon": true,
  "remote_sdk_dir": "/root/tmp/yr/datasystem/lib",
  "duration": "30s",
  "nodes": [
    {"host": "192.168.0.100", "ssh_port": 22, "instance_id": 0, "role": "writer", "port": 9000},
    {"host": "192.168.0.101", "ssh_port": 22, "instance_id": 1, "role": "reader", "port": 9001}
  ]
}
```

### 3.3 使用 gen-config 自动生成部署配置

通过 `--mode` 指定运行模式，生成对应的配置模板：

```bash
# Pipeline 模式：1 writer + N readers
python3 deploy_client.py gen-config -p ds-worker -n datasystem -w 1 \
  --mode pipeline --pipeline setStringView --target-qps 100

# Cache 模式：cacheGetOrCreate + 动态命中率
python3 deploy_client.py gen-config -p ds-worker -n datasystem -w 1 \
  --mode cache --pipeline cacheGetOrCreate --key-pool-size 100 \
  --target-hit-rate 0.8 --inference-delay 10

# Benchmark 模式：单节点 Set 性能测试（不需要 Pod 发现）
python3 deploy_client.py gen-config -p ds-worker -n datasystem \
  --mode benchmark --test-mode set_local --worker-memory-mb 4096 \
  --num-threads 16 --duration 30
```

**模式专属参数：**

| 参数 | 模式 | 说明 |
|------|------|------|
| `--mode` | 全部 | `pipeline` / `cache` / `benchmark`（必填） |
| `--pipeline` | Pipeline/Cache | 写入流水线操作，逗号分隔 |
| `--notify-pipeline` | Pipeline/Cache | 通知后操作，逗号分隔 |
| `--target-qps` | Pipeline/Cache | 目标 QPS（默认 100） |
| `--key-pool-size` | Cache（必填） | Key 池大小 |
| `--target-hit-rate` | Cache | 目标命中率 0.01~1.0 |
| `--inference-delay` | Cache | 模拟推理延迟（毫秒） |
| `--warmup-timeout` | Cache | 预热超时（秒） |
| `--test-mode` | Benchmark（必填） | `set_local`/`set_remote`/`get_local`/`get_cross_node`/`get_remote_direct`/`get_remote_cross` |
| `--worker-memory-mb` | Benchmark（必填） | Worker 共享内存（MB） |
| `--set-api` | Benchmark | `string_view` / `create_buffer` |
| `--duration` | Benchmark | 运行时长（秒） |
| `--total-rounds` | Benchmark | 总轮数 |
| `--ttl` | 全部 | TTL 秒数（默认 0，永不过期） |
| `--data-sizes` | 全部 | 数据大小，逗号分隔（默认 `1MB`） |
| `--num-threads` | 全部 | 工作线程数（默认 16） |

### 3.4 收集结果

`collect` 从每个节点收集到 `collected/` 目录：`metrics_{id}.csv`、`summary_{id}_{ts}.txt`、`stdout_{id}.log`。

---

## 4. 指标采集与分析

### 4.1 指标输出格式

**metrics CSV 格式：**
```csv
timestamp,op,count,success,fail,qps,avg_ms,p50_ms,p90_ms,p99_ms,p99_9_ms,min_ms,max_ms,bytes
```

**Summary 文件格式（退出时生成）：**
```
=== KVTest Summary ===
Instance ID: 0
Uptime: 33 seconds

--- cacheGetOrCreate ---
Total: 302, Success: 302, Fail: 0
Avg: 1.234ms, P90: 1.315ms, P99: 2.078ms, P99.9: 2.153ms
```

### 4.2 使用日志分析工具生成 HTML 报告

```bash
# Worker access log 报告
python3 scripts/generate_access_log_report.py /path/to/worker-logs \
  --since "2026-05-13T10:00:00" --interval 60000 -o report.html

# Worker resource log 报告
python3 scripts/generate_resource_report.py /path/to/worker-logs \
  --since "2026-05-13T10:00:00" --interval 60000 -o resource.html
```

---

## 5. 故障排查

### 5.1 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| `KVClient init failed: connection refused` | Worker 未启动或端口不对 | 检查 `dscli start -w` 和 `etcd_address` |
| `Warmup Set failed permanently` | Worker 共享内存不足 | 增大 `--shared_memory_size_mb` 或减少 `key_pool_size` |
| `warmupTimeoutSeconds exceeded` | Reader 等待 Writer 预热超时 | 检查网络连通性，增大 `warmup_timeout_seconds` |
| `libdatasystem.so: not found` | LD_LIBRARY_PATH 未设置 | `export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH` |
| `GLIBCXX_3.4.30 not found` | 目标系统 GCC 版本低 | 已静态链接 libstdc++，检查其他动态库冲突 |
| 命中率始终 100% 不收敛 | TTL=0 导致 miss 后立即填充 | 设置 `set_param.ttl_second > 0` 或接受满池行为 |
| Pool 大小不调整 | `target_hit_rate` 未设置或为 0 | 设置 `target_hit_rate` 为 0.01~1.0 |
| `RocksDB LOCK: Resource temporarily unavailable` | 多 Worker 共享默认 RocksDB 目录 | 每个 Worker 使用独立 `rocksdb_store_dir` |

**多 Worker RocksDB 冲突解决：**
```bash
# Worker 1
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379

# Worker 2（独立路径）
dscli generate_config -o ./
# 编辑 rocksdb_store_dir 和 worker_address
dscli start -f worker_config.json
```

### 5.2 优雅退出

`Ctrl+C` 或 `SIGTERM` 触发：停止写入线程 → 停止 HTTP 服务器 → 刷新指标 → 生成 Summary → 退出。Summary 写入 `summary_{instance_id}_{timestamp}.txt`。
