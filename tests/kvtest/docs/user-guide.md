# kvtest 用户操作指南

## 1. 概述

`kvtest` 是一个独立的 C++ 分布式 KV 性能测试工具，用于对 yuanrong-datasystem 集群进行端到端的读写性能测试和缓存命中率验证。

**核心能力：**
- 单/多实例部署，支持 Writer + Reader 角色分离
- 10 种操作类型（Set/Get/Create/MSet/MGet/cacheGetOrCreate 等）
- Cache 模式：`cacheGetOrCreate` 流水线操作，模拟"先查后写"缓存复用场景
- 动态命中率控制：`target_hit_rate` 自动调整 Key Pool 大小，趋近目标命中率
- 多节点通知：Writer 完成写入后通过 HTTP 通知 Reader 进行跨节点读验证
- 内置指标采集：CSV 格式输出 QPS、延迟百分位（avg/p90/p99/p99.9）、命中率
- 一键打包部署：`deploy_client.py` 支持 SSH/kubectl 两种传输方式

**适用场景：**
- KVCache 缓存命中率回归测试
- 多 Worker 集群读延迟压测
- cacheGetOrCreate 流水线功能验证
- Worker 端性能基线建立

---

## 2. 环境准备

### 2.1 依赖

| 依赖 | 版本要求 | 说明 |
|------|---------|------|
| datasystem SDK | 与目标集群同版本 | headers + libdatasystem.so |
| GCC | >= 10 | 支持 C++17 |
| CMake | >= 3.14 | 编译工具 |
| etcd | >= 3.5 | 集群注册发现 |
| datasystem Worker | 运行中 | 至少 1 个 Worker 节点 |

### 2.2 获取 SDK

在 datasystem 源码目录执行 Bazel 编译：

```bash
# 编译 SDK（生成 tar 包）
bazel build //bazel:datasystem_sdk --config=release

# 解压到 output/cpp/ 目录
mkdir -p output/cpp
tar xf bazel-bin/bazel/datasystem_sdk.tar.gz -C output/cpp/ --strip-components=1

# 复制 libdatasystem.so（SDK tar 中未包含）
cp bazel-bin/libdatasystem.so output/cpp/lib/
```

SDK 目录结构：
```
output/cpp/
├── include/
│   └── datasystem/          # C++ SDK 头文件
└── lib/
    └── libdatasystem.so     # 动态库
```

### 2.3 编译

```bash
cd tests/kvtest

# 使用默认 SDK 路径 (../../output/cpp)
./build.sh

# 或指定 SDK 路径
./build.sh -s /path/to/sdk

# 清理重建
./build.sh -c -j$(nproc)
```

编译成功后产物位于 `output/` 目录：
```
output/
├── kvtest              # 可执行文件（静态链接 libstdc++）
├── lib/                # SDK 动态库
│   └── libdatasystem.so
├── config/             # 示例配置
├── deploy_client.py    # 远程部署工具（含 gen-config 子命令）
├── deploy_worker.py    # K8s Worker 管理工具
├── procmon.py          # 进程监控
└── VERSION             # 版本号
```

---

## 3. 配置文件详解

### 3.1 完整配置模板

> **注意：** 以下模板中的值为推荐配置，非代码默认值。各参数默认值见 3.2 节表格。`connect_timeout_ms` 代码默认值为 1000，`request_timeout_ms` 默认值为 20，此处使用 5000 是测试环境推荐值。

```json
{
  "instance_id": 0,
  "listen_port": 9000,
  "etcd_address": "127.0.0.1:2379",
  "cluster_name": "",
  "host_id_env_name": "JD_HOST_IP",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "fast_transport_mem_size": "512MB",
  "data_sizes": ["1MB"],
  "ttl_seconds": 0,
  "target_qps": 10,
  "num_set_threads": 4,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_{instance_id}.csv",
  "cpu_affinity": "",
  "role": "writer",
  "pipeline": ["cacheGetOrCreate"],
  "notify_pipeline": ["getBuffer"],
  "notify_count": 10,
  "notify_interval_us": 0,
  "enable_jitter": true,
  "enable_cross_node_connection": true,
  "batch_keys_count": 1,
  "peers": [],
  "nodes": [],
  "key_pool_size": 0,
  "inference_delay_ms": 0,
  "max_key_pool_size": 0,
  "target_hit_rate": 0.0,
  "warmup_batch_size": 100,
  "warmup_retry_count": 3,
  "warmup_retry_delay_ms": 1000,
  "warmup_timeout_seconds": 300
}
```

### 3.2 参数说明

#### 基础连接参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `instance_id` | int | 0 | 实例唯一标识，同一测试中不可重复 |
| `listen_port` | int | 9000 | HTTP 服务端口（接收通知） |
| `etcd_address` | string | **必填** | etcd 地址，格式 `ip:port` |
| `cluster_name` | string | "" | 集群名，多集群环境区分 |
| `host_id_env_name` | string | "JD_HOST_IP" | Worker IP 环境变量名 |
| `connect_timeout_ms` | int | 1000 | KVClient 连接超时（毫秒） |
| `request_timeout_ms` | int | 20 | KVClient 请求超时（毫秒） |
| `fast_transport_mem_size` | string | "512MB" | 快速传输内存大小，支持 KB/MB/GB 后缀 |

#### 数据和负载参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `data_sizes` | string[] | ["8MB"] | 数据大小列表，支持 KB/MB/GB 后缀，随机选取 |
| `ttl_seconds` | int | 5 | 数据 TTL（秒），0 = 永不过期 |
| `target_qps` | int | 100 | 目标 QPS，0 = 不限速（全速写入） |
| `num_set_threads` | int | 16 | 写入线程数 |
| `batch_keys_count` | int | 1 | 每次操作的 key 数量，用于 batch 操作 |
| `cpu_affinity` | string | "" | CPU 亲和性，如 "0-7" 或 "0,2,4,6"，空 = 自动检测 |
| `enable_jitter` | bool | true | 是否启用随机抖动（错开请求时间） |

#### Pipeline 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `role` | string | "writer" | 角色：`"writer"` = 写入+通知，`"reader"` = 等待通知 |
| `pipeline` | string[] | ["setStringView"] | 写入流水线操作列表 |
| `notify_pipeline` | string[] | ["getBuffer"] | 收到通知后执行的操作列表 |
| `notify_count` | int | 10 | 每次操作后通知的 peer 数量 |
| `notify_interval_us` | int | 0 | 通知间隔（微秒），0 = 并行发送 |

**可用操作类型：**

| 操作名 | 说明 |
|--------|------|
| `setStringView` | Set(key, StringView(data)) |
| `getBuffer` | Get(key, Buffer)，读取并校验大小 |
| `exist` | Exist({key})，检查 key 是否存在 |
| `createBuffer` | Create(key, size, param, buffer) |
| `memoryCopy` | buffer->MemoryCopy(data, size) |
| `setBuffer` | Set(buffer) |
| `mCreate` | MCreate(keys, sizes, buffers) |
| `mSet` | MSet(buffers) |
| `mGet` | Get(keys, buffers) |
| `cacheGetOrCreate` | **缓存核心操作**：先 Get，命中则返回；未命中则 Create+MemoryCopy+Set |

#### Cache 模式参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `key_pool_size` | int | 0 | Key 池初始大小，**0 = 禁用 Cache 模式** |
| `target_hit_rate` | double | 0.0 | 目标命中率，0 = 固定池大小，0.01~1.0 = 自动调整 |
| `max_key_pool_size` | int | 0 | Key 池上限，0 = 自动（key_pool_size × 10） |
| `inference_delay_ms` | int | 0 | 模拟推理延迟（毫秒），Cache miss 后的等待 |
| `warmup_retry_count` | int | 3 | 预热阶段每个 key 的最大重试次数 |
| `warmup_retry_delay_ms` | int | 1000 | 预热重试间隔（毫秒） |
| `warmup_timeout_seconds` | int | 300 | Reader 等待所有 Writer 预热完成的超时 |

#### 指标采集参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `metrics_interval_ms` | int | 3000 | 指标刷新间隔（毫秒） |
| `metrics_file` | string | "metrics_{instance_id}.csv" | CSV 输出文件名，`{instance_id}` 自动替换 |

#### 多节点参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `peers` | string[] | [] | Peer 地址列表，如 `["http://host:port"]` |
| `nodes` | NodeInfo[] | [] | 集群节点列表（自动生成 peers） |
| `enable_cross_node_connection` | bool | true | 允许跨节点连接 standby Worker |

**NodeInfo 结构：**
```json
{
  "host": "127.0.0.1",
  "port": 9000,
  "instance_id": 0,
  "role": "writer"
}
```

---

## 4. 快速开始

### 4.1 场景一：单实例基准测试

最简单的用法——单个 Writer 对单个 Worker 写入。

**前置条件：** etcd 和 Worker 已运行。

**步骤 1：创建配置文件 `config/test1.json`**
```json
{
  "instance_id": 0,
  "listen_port": 9000,
  "etcd_address": "127.0.0.1:2379",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1MB"],
  "ttl_seconds": 0,
  "target_qps": 10,
  "num_set_threads": 1,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_0.csv",
  "role": "writer",
  "pipeline": ["setStringView"]
}
```

**步骤 2：运行**
```bash
cd output/
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH \
  ./kvtest config/test1.json
```

**步骤 3：观察输出**
```
[setStringView=10/s] [out_q=0, in_q=0]
[setStringView=10/s] [out_q=0, in_q=0]
```
每 3 秒打印一次 QPS 和队列深度。

**步骤 4：停止**
- `Ctrl+C` 发送 SIGTERM，程序优雅退出并生成 Summary 文件
- 或从另一终端：`./kvtest --stop config/test1.json`

### 4.2 场景二：Cache 模式 + 固定命中率测试

测试 `cacheGetOrCreate` 流水线，验证缓存命中行为。

**配置文件 `config/test_cache.json`：**
```json
{
  "instance_id": 0,
  "role": "writer",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "data_sizes": ["1MB"],
  "key_pool_size": 100,
  "target_qps": 10,
  "num_set_threads": 2,
  "ttl_seconds": 0,
  "pipeline": ["cacheGetOrCreate"],
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"}
  ]
}
```

**运行：**
```bash
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH \
  ./kvtest config/test_cache.json
```

**观察日志：**
```
Starting warmup: 100 keys
Warmup done: 100 ok, 0 fail
Starting 2 pipeline threads
[cacheGetOrCreate=10/s, getBuffer=10/s] [pool=100, hit_rate=0.0000]
[cacheGetOrCreate=10/s, getBuffer=10/s] [pool=100, hit_rate=0.5500]
[cacheGetOrCreate=10/s, getBuffer=10/s] [pool=100, hit_rate=0.9000]
```

**预期行为：**
1. 预热阶段：写入 100 个 key（`cache_pool_0_0` ~ `cache_pool_0_99`）
2. 稳态阶段：`cacheGetOrCreate` 从池中随机选取 key 执行 Get
   - 命中 → 直接返回（记录 cacheGetOrCreate + getBuffer）
   - 未命中 → Create + MemoryCopy + Set（记录全部子操作）
3. 由于 TTL=0，数据不会过期，命中率会逐渐上升并稳定

### 4.3 场景三：动态命中率调整

启用 `target_hit_rate` 让系统自动调整 Key Pool 大小。

**配置文件 `config/test_dynamic.json`：**
```json
{
  "instance_id": 0,
  "role": "writer",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "data_sizes": ["1MB"],
  "key_pool_size": 100,
  "target_hit_rate": 0.8,
  "target_qps": 10,
  "num_set_threads": 4,
  "ttl_seconds": 0,
  "pipeline": ["cacheGetOrCreate"],
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"}
  ]
}
```

**运行并观察：**
```
Pool adjust: hit_rate=1.000 > target=0.8 → pool 100→105
Pool adjust: hit_rate=0.952 > target=0.8 → pool 105→110
Pool adjust: hit_rate=0.889 > target=0.8 → pool 110→116
...
Pool adjust: hit_rate=0.812 > target=0.8 → pool 158→166
```

**调整算法：**
- 每 3 秒评估一次当前命中率
- 命中率 > target + 0.02 → 扩大池（增加 key 范围），命中率自然下降
- 命中率 < target - 0.02 → 缩小池（减少 key 范围），命中率自然上升
- 调整步长：当前池大小的 1/20，最小 1
- 池大小范围：`key_pool_size / 10` ~ `key_pool_size × 20`

### 4.4 场景四：Writer + Reader 多实例部署

Writer 负责预热和持续写入，Reader 负责接收通知后读取验证。

**Writer 配置 `config/writer.json`：**
```json
{
  "instance_id": 0,
  "role": "writer",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "data_sizes": ["1MB"],
  "key_pool_size": 100,
  "target_qps": 20,
  "num_set_threads": 2,
  "ttl_seconds": 0,
  "pipeline": ["cacheGetOrCreate"],
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"},
    {"host": "127.0.0.1", "port": 9001, "instance_id": 1, "role": "reader"}
  ]
}
```

**Reader 配置 `config/reader.json`：**
```json
{
  "instance_id": 1,
  "role": "reader",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9001,
  "data_sizes": ["1MB"],
  "key_pool_size": 100,
  "inference_delay_ms": 10,
  "warmup_timeout_seconds": 60,
  "target_qps": 20,
  "num_set_threads": 2,
  "ttl_seconds": 0,
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"},
    {"host": "127.0.0.1", "port": 9001, "instance_id": 1, "role": "reader"}
  ]
}
```

**启动（两个终端）：**
```bash
# 终端 1：先启动 Reader（等待 Writer 预热通知）
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH \
  ./kvtest config/reader.json

# 终端 2：再启动 Writer
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH \
  ./kvtest config/writer.json
```

**交互流程：**
```
1. Reader 启动 → 等待 Writer 预热完成通知
2. Writer 启动 → 预热 100 keys → HTTP POST /notify 通知 Reader
3. Reader 收到通知 → key 池填充 → 开始读取
4. Writer 持续 cacheGetOrCreate → 写入新 key → 通知 Reader
5. Reader 收到通知 → 扩展 key 池 → 读取新 key
```

### 4.5 场景五：多 Worker + 多 Client 部署

在同一台机器上部署 2 个 Worker，每个 Worker 对应 1 个 Writer + 1 个 Reader，验证跨节点缓存命中和通知机制。

**前置条件：** 每个 Worker 必须使用独立的 `rocksdb_store_dir`（见 8.1 节故障排查）。

**Worker 启动：**
```bash
# Worker 1
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379

# Worker 2（需要独立 RocksDB 目录）
dscli generate_config -o /tmp/worker2_config.json
# 编辑 worker_address 和 rocksdb_store_dir
dscli start -f /tmp/worker2_config.json
```

**Writer 0 配置 `config/writer0.json`：**
```json
{
  "instance_id": 0,
  "role": "writer",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "data_sizes": ["1MB"],
  "key_pool_size": 50,
  "target_hit_rate": 0.7,
  "target_qps": 10,
  "num_set_threads": 2,
  "ttl_seconds": 0,
  "pipeline": ["cacheGetOrCreate"],
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"},
    {"host": "127.0.0.1", "port": 9001, "instance_id": 1, "role": "reader"}
  ]
}
```

**Reader 0 配置 `config/reader0.json`：**
```json
{
  "instance_id": 1,
  "role": "reader",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9001,
  "data_sizes": ["1MB"],
  "key_pool_size": 50,
  "warmup_timeout_seconds": 60,
  "target_qps": 10,
  "ttl_seconds": 0,
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"},
    {"host": "127.0.0.1", "port": 9001, "instance_id": 1, "role": "reader"}
  ]
}
```

Writer 1 和 Reader 1 配置类似，修改 `instance_id`（2/3）、`listen_port`（9002/9003），`nodes` 指向 Writer 1 + Reader 1。

**启动顺序：** 先启动 Reader，再启动 Writer（Writer 预热完成后会 HTTP 通知 Reader）。

**验证要点：**
- Writer 命中率趋向 `target_hit_rate`（Pool 自动扩缩）
- Reader `cacheGetOrFill_hit` > 0（跨节点命中）
- Summary 中所有操作 Fail = 0

---

## 5. 远程多节点部署

### 5.1 deploy_client.py 用法

`deploy_client.py` 支持通过 SSH 或 kubectl 将二进制和配置文件部署到多个节点。

**基本用法：**
```bash
# 部署 + 启动
python3 deploy_client.py deploy deploy.json config.json

# 停止所有实例
python3 deploy_client.py stop deploy.json

# 收集结果文件
python3 deploy_client.py collect deploy.json

# 清理远程目录
python3 deploy_client.py clean deploy.json
```

### 5.2 部署配置文件

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
    {
      "host": "192.168.0.100",
      "ssh_port": 22,
      "instance_id": 0,
      "role": "writer",
      "pipeline": ["cacheGetOrCreate"],
      "port": 9000
    },
    {
      "host": "192.168.0.101",
      "ssh_port": 22,
      "instance_id": 1,
      "role": "reader",
      "port": 9001
    }
  ]
}
```

**`deploy.json` 示例（SSH 隧道 + 端口映射）：**
```json
{
  "remote_work_dir": "/tmp/kvclient_test",
  "transport": "ssh",
  "ssh_user": "root",
  "ssh_options": "-o StrictHostKeyChecking=no",
  "enable_procmon": true,
  "nodes": [
    {
      "host": "1.95.199.126",
      "ssh_port": 22223,
      "comm_host": "192.168.0.223",
      "instance_id": 0,
      "role": "writer",
      "pipeline": ["cacheGetOrCreate"],
      "port": 9000
    },
    {
      "host": "1.95.199.126",
      "ssh_port": 22223,
      "comm_host": "192.168.0.223",
      "instance_id": 1,
      "role": "reader",
      "port": 9001
    }
  ]
}
```

**部署配置字段说明：**

| 字段 | 说明 |
|------|------|
| `remote_work_dir` | 远端工作目录 |
| `transport` | 传输方式：`ssh` 或 `kubectl` |
| `ssh_user` | SSH 用户名 |
| `ssh_options` | SSH 选项 |
| `enable_procmon` | 是否启动进程监控 |
| `remote_sdk_dir` | 容器内 SDK 路径（跳过 SDK 上传） |
| `duration` | 自动运行时长，如 `"30s"`、`"5m"`、`"2h"` |
| `nodes[].host` | SSH 目标 IP |
| `nodes[].ssh_port` | SSH 端口 |
| `nodes[].comm_host` | 通信 IP（kvclient 实例间互访） |
| `nodes[].instance_id` | 实例 ID |
| `nodes[].role` | 角色：writer / reader |
| `nodes[].pipeline` | 该节点的 pipeline 覆盖 |
| `nodes[].port` | HTTP 服务端口 |

### 5.3 完整部署流程

```bash
# 1. 编译打包
./build.sh -s /path/to/sdk

# 2. 部署并运行 30 秒后自动停止+收集
cd output/
python3 deploy_client.py deploy deploy.json config.json

# 3. 或分步操作
python3 deploy_client.py deploy deploy.json config.json   # 仅部署
python3 deploy_client.py stop deploy.json                  # 停止
python3 deploy_client.py collect deploy.json               # 收集结果
python3 deploy_client.py clean deploy.json                 # 清理
```

### 5.4 使用 gen-config 自动生成部署配置

在 Kubernetes 环境下，`deploy_client.py gen-config` 子命令自动发现 Pod 并生成 `deploy.json` + `config.json`，无需手动编写节点配置。

**基本读写（非 Cache 模式）：**
```bash
# 1 writer + 10 readers，setStringView 写入
python3 deploy_client.py gen-config -p ds-worker -n datasystem -w 1
# 生成: config/deploy.json + config/config.json
python3 deploy_client.py deploy config/deploy.json config/config.json
```

**cacheGetOrCreate 缓存命中测试：**
```bash
# 1 writer + 10 readers，启用 Cache 模式
python3 deploy_client.py gen-config -p ds-worker -n datasystem -w 1 \
  --pipeline cacheGetOrCreate \
  --key-pool-size 100
# config.json 自动包含: key_pool_size=100, pipeline=["cacheGetOrCreate"]
```

**动态命中率调整 + 跨节点缓存验证：**
```bash
# 2 writers + 8 readers，cacheGetOrCreate 写入，目标命中率 0.7
python3 deploy_client.py gen-config -p ds-worker -n datasystem -w 2 \
  --pipeline cacheGetOrCreate \
  --key-pool-size 50 \
  --target-hit-rate 0.7 \
  --warmup-timeout 120 \
  -e "10.0.0.5:2379"
# config.json 自动包含:
#   key_pool_size=50, target_hit_rate=0.7,
#   warmup_timeout_seconds=120, etcd_address="10.0.0.5:2379"
# deploy.json: 前 2 个 Pod 为 writer(role=writer, pipeline=["cacheGetOrCreate"])
#              后 8 个 Pod 为 reader(role=reader, pipeline=[])
```

**带推理延迟模拟（模拟 LLM 推理场景）：**
```bash
python3 deploy_client.py gen-config -p ds-worker -n datasystem -w 1 \
  --pipeline cacheGetOrCreate \
  --key-pool-size 100 \
  --target-hit-rate 0.8 \
  --inference-delay 10
# Reader 每次读取后等待 10ms，模拟推理计算间隔
```

**gen-config 参数表：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-p` / `--prefix` | (必填) | Pod 名称前缀 |
| `-n` / `--namespace` | `default` | k8s 命名空间 |
| `-w` / `--writer-count` | `1` | Writer 数量，前 N 个 Pod 为 writer |
| `--pipeline` | `setStringView` | Writer pipeline ops，逗号分隔。支持：setStringView, getBuffer, exist, cacheGetOrCreate, mCreate, mSet 等 |
| `--notify-pipeline` | `getBuffer` | Notify pipeline ops，逗号分隔 |
| `--batch-keys-count` | `1` | Batch ops 的 key 数量，>1 时写入 deploy.json |
| `--key-pool-size` | `0` | Cache 模式 Key Pool 大小，`0` = 不启用 |
| `--target-hit-rate` | `0` | 目标缓存命中率（0.01~1.0），`0` = 不调整 |
| `--warmup-timeout` | `60` | Reader 等待 Writer 预热超时（秒） |
| `--inference-delay` | `0` | Reader 推理延迟模拟（毫秒） |
| `-e` / `--etcd-address` | | 覆盖 config.json 中的 etcd_address |
| `-c` / `--cluster-name` | | 设置 config.json 中的 cluster_name |
| `--remote-sdk-dir` | | 容器内 SDK 路径（跳过 SDK 上传） |
| `-o` / `--output-dir` | `config` | 输出目录 |
| `-r` / `--remote-work-dir` | `/home/user/kvclient_test` | 远端工作目录 |

### 5.5 收集结果

`deploy_client.py collect` 会从每个节点收集以下文件到 `collected/` 目录：

```
collected/
├── 192.168.0.100_0/
│   ├── metrics_0.csv          # 指标数据
│   ├── summary_0_xxx.txt      # 汇总报告
│   └── stdout_0.log           # 标准输出日志
└── 192.168.0.101_1/
    ├── metrics_1.csv
    ├── summary_1_xxx.txt
    └── stdout_1.log
```

---

## 6. 指标采集与分析

### 6.1 指标输出格式

**metrics CSV 格式：**
```csv
timestamp,op,count,success,fail,qps,avg_ms,p50_ms,p90_ms,p99_ms,p99_9_ms,min_ms,max_ms,bytes
2026-05-13T04:09:30,cacheGetOrCreate,30,30,0,10.0,1.234,1.100,1.315,2.078,2.153,0.501,2.153,31457280
2026-05-13T04:09:30,getBuffer,30,27,3,9.0,1.003,0.950,1.315,2.078,2.153,0.501,2.153,28311552
2026-05-13T04:09:30,cache_summary,hits=27,misses=3,hit_rate=0.9000
```

**Summary 文件格式（退出时生成）：**
```
=== KVTest Summary ===
Instance ID: 0
Uptime: 33 seconds

--- cacheGetOrCreate ---
Total: 302, Success: 302, Fail: 0
Avg: 1.234ms, P90: 1.315ms, P99: 2.078ms, P99.9: 2.153ms

=== Cache Statistics ===
Hits: 272
Misses: 30
Hit Rate: 0.9007
```

### 6.2 关键指标解读

| 指标 | 含义 | 正常范围 |
|------|------|---------|
| `hit_rate` | 缓存命中率（hit / (hit + miss)） | 取决于 target_hit_rate |
| `cacheGetOrCreate QPS` | 端到端操作吞吐 | 接近 target_qps |
| `getBuffer avg_ms` | Get 操作平均延迟 | 1MB 对象 < 2ms |
| `p99_ms` | 99 百分位延迟 | 取决于对象大小和 Worker 负载 |
| `pool` | 当前 Key Pool 大小 | 在 `key_pool_size/10` ~ `key_pool_size×20` 范围 |

### 6.3 使用日志分析工具生成 HTML 报告

项目提供了 KVCache 日志分析脚本，可将 Worker access.log 和 client metrics CSV 转为交互式 HTML 报告。

**Worker 端日志分析：**
```bash
# Access log 报告（QPS、延迟分布、错误分析）
# <log_dir> 是包含 access.log 文件的目录根路径
# --since 是必填参数，格式 YYYY-MM-DDTHH:MM:SS
python3 scripts/generate_access_log_report.py /path/to/worker-logs \
  --since "2026-05-13T10:00:00" \
  --interval 60000 \
  -o worker_access_report.html

# Resource log 报告（内存、线程池、缓存命中率）
python3 scripts/generate_resource_report.py /path/to/worker-logs \
  --since "2026-05-13T10:00:00" \
  --interval 60000 \
  -o worker_resource_report.html
```

**Client 端指标分析（自定义脚本）：**
```bash
# 使用 generate_client_report.py 生成 ECharts 交互式报告
cd remote_build_results/kvclient-cache-test/
python3 generate_client_report.py
# 输出: client_metrics_report.html
```

HTML 报告包含：
- 命中率收敛曲线（目标线标注）
- 延迟百分位趋势图（avg/p90/p99/p99.9）
- Sub-operation 延迟分解（getBuffer/createBuffer/memoryCopy/setBuffer）
- 多实例 Pool Size 和 QPS 对比
- Worker 端操作延迟 CDF 和时间线
- 测试结论表格（PASS/WARN/FAIL）

---

## 7. 高级用法

### 7.1 多 Writer 并发测试

在同一主机启动多个 Writer 实例，共享一个 Worker：

```bash
# 终端 1：Writer 0
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH \
  ./kvtest config/writer0.json

# 终端 2：Writer 1
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH \
  ./kvtest config/writer1.json

# 终端 3：Writer 2
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH \
  ./kvtest config/writer2.json
```

每个 Writer 使用不同的 `instance_id`、`listen_port`，key 命名自动按 instance_id 隔离（`cache_pool_{instance_id}_{index}`）。

### 7.2 不同对象大小混合测试

`data_sizes` 支持配置多个大小，运行时随机选取：

```json
{
  "data_sizes": ["4KB", "64KB", "1MB", "4MB"],
  "target_qps": 50
}
```

### 7.3 Batch 操作测试

使用 `mCreate` + `mSet` + `mGet` 进行批量操作：

```json
{
  "pipeline": ["mCreate", "mSet"],
  "batch_keys_count": 10
}
```

### 7.4 带推理延迟的 Cache 测试

模拟 LLM 推理场景中 Cache miss 的处理延迟：

```json
{
  "key_pool_size": 100,
  "inference_delay_ms": 50,
  "pipeline": ["cacheGetOrCreate"]
}
```

### 7.5 CPU 亲和性绑定

将进程绑定到指定 CPU 核心，减少调度抖动：

```json
{
  "cpu_affinity": "0-7"
}
```

### 7.6 通知节流

当 peer 数量多时，避免瞬时大量 HTTP 请求：

```json
{
  "notify_interval_us": 1000,
  "notify_count": 5
}
```

---

## 8. 故障排查

### 8.1 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| `KVClient init failed: connection refused` | Worker 未启动或端口不对 | 检查 `dscli start -w` 和 `etcd_address` |
| `Warmup Set failed permanently` | Worker 共享内存不足 | 增大 `--shared_memory_size_mb` 或减少 `key_pool_size` |
| `warmupTimeoutSeconds exceeded` | Reader 等待 Writer 预热超时 | 检查网络连通性，增大 `warmup_timeout_seconds` |
| `libdatasystem.so: not found` | LD_LIBRARY_PATH 未设置 | `export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH` |
| `GLIBCXX_3.4.30 not found` | 目标系统 GCC 版本低 | 已静态链接 libstdc++，检查是否有其他动态库冲突 |
| `cache_summary` CSV 解析错误 | cache_summary 行多一列 | 在解析器中单独处理 cache_summary 行 |
| 命中率始终 100% 不收敛 | TTL=0 导致 miss 后立即填充 | 设置 `ttl_seconds > 0` 或接受满池行为 |
| Pool 大小不调整 | `target_hit_rate` 未设置或为 0 | 设置 `target_hit_rate` 为 0.01~1.0 |
| `RocksDB LOCK: Resource temporarily unavailable` | 多 Worker 共享默认 RocksDB 目录 | 每个 Worker 使用独立 `rocksdb_store_dir`（见下方） |

**多 Worker RocksDB 冲突解决：**

在同一台机器上运行多个 Worker 时，必须为每个 Worker 指定独立的 RocksDB 存储目录：

```bash
# Worker 1（默认路径）
dscli start -w --worker_address 127.0.0.1:31501 --etcd_address 127.0.0.1:2379

# Worker 2（需要独立路径）
# 先生成配置模板，修改 rocksdb_store_dir
dscli generate_config -o ./
# 编辑 worker_config.json，修改:
#   worker_address: "127.0.0.1:31502"
#   rocksdb_store_dir: "/tmp/rocksdb_worker2"
dscli start -f worker_config.json
```

### 8.2 日志级别

程序使用 stderr 输出日志：
- 正常输出（版本信息、线程启动等）→ stderr
- 运行时指标（QPS、命中率）→ stderr（`SLOG_INFO`）
- 警告（重试、pool 调整）→ stderr（`SLOG_WARN`）

### 8.3 优雅退出

按 `Ctrl+C` 或发送 `SIGTERM` 触发优雅退出：
1. 停止写入线程
2. 停止 HTTP 服务器
3. 刷新指标到 CSV
4. 生成 Summary 文件
5. 退出

Summary 文件在退出时写入 `summary_{instance_id}_{timestamp}.txt`。

---

## 9. 测试验证清单

使用以下清单验证功能正确性：

### 9.1 基础写入测试
- [ ] `setStringView` pipeline 正常写入，QPS 接近 target_qps
- [ ] `getBuffer` 读取成功，数据大小校验通过
- [ ] Summary 文件生成，数据与 CSV 一致

### 9.2 cacheGetOrCreate 测试
- [ ] 预热阶段完成，所有 key 写入成功
- [ ] 稳态阶段命中率 > 0
- [ ] 子操作指标（getBuffer/createBuffer/memoryCopy/setBuffer）正确记录
- [ ] cache_summary 行的 hit_rate 与实际一致

### 9.3 target_hit_rate 动态调整测试
- [ ] 设置 `target_hit_rate=0.8` 后，Pool Size 自动调整
- [ ] 命中率在 target ± 0.05 范围内波动
- [ ] Pool Size 不低于 `key_pool_size / 10`，不超过 `key_pool_size × 20`

### 9.4 多实例部署测试
- [ ] Writer + Reader 正常启动和通信
- [ ] Reader 收到预热通知后开始读取
- [ ] 跨节点 getBuffer 指标反映远端读取
- [ ] 无操作失败（Fail = 0）

### 9.5 Worker 端验证
- [ ] Worker access.log 记录所有操作
- [ ] GET 延迟 p99 < 预期阈值（1MB 对象通常 < 1ms）
- [ ] PUBLISH 延迟 p99 < 预期阈值
- [ ] Worker 资源使用正常（共享内存、线程池）
