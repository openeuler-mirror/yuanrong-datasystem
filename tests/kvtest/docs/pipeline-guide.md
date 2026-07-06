# kvtest Pipeline 模式指南

> **相关文档：** [编译部署与通用配置](user-guide.md) | [Cache 模式](cache-guide.md) | [Benchmark 模式](benchmark-guide.md)

Pipeline 模式是 kvtest 的核心运行模式。支持 Writer/Reader 角色分离、10 种 KV 操作类型、QPS 精确控制、HTTP 通知机制，适用于持续读写压测和跨实例验证。

**核心特性：**
- Writer/Reader 角色分离，Writer 持续写入并通过 HTTP 通知 Reader
- 10 种操作类型（setStringView、getBuffer、exist、createBuffer、mCreate 等）
- QPS 控制：相位偏移 + Jitter 打散，避免请求同步突发
- 多阶段 QPS 压测（target_qps 数组）
- 进程级 CPU 绑核

---

## 1. 快速开始

### 1.1 单实例基准测试

最简单的用法——单个 Writer 对单个 Worker 写入。

**前置条件：** etcd 和 Worker 已运行。

**配置文件 `config/test1.json`：**
```json
{
  "mode": "pipeline",
  "instance_id": 0,
  "listen_port": 9000,
  "etcd_address": "127.0.0.1:2379",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1MB"],
  "set_param": {"ttl_second": 0},
  "target_qps": 10,
  "num_threads": 1,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_0.csv",
  "role": "writer",
  "pipeline": ["setStringView"],
  "notify_pipeline": []
}
```

> **getBuffer 测试示例：** 将 pipeline 改为 `["setStringView", "getBuffer"]` 可验证写入后立即读取的写后读场景。

**运行：**
```bash
cd output/
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/test1.json
```

**观察输出：**
```
[setStringView=10/s] [out_q=0, in_q=0]
[setStringView=10/s] [out_q=0, in_q=0]
```

**停止：** `Ctrl+C` 优雅退出并生成 Summary 文件，或 `./kvtest --stop config/test1.json`。

### 1.2 Writer + Reader 多实例部署

Writer 负责预热和持续写入，Reader 负责接收通知后读取验证。

**Writer 配置 `config/writer.json`：**
```json
{
  "mode": "pipeline",
  "instance_id": 0,
  "role": "writer",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "data_sizes": ["1MB"],
  "key_pool_size": 100,
  "target_qps": 20,
  "num_threads": 2,
  "set_param": {"ttl_second": 0},
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
  "mode": "pipeline",
  "instance_id": 1,
  "role": "reader",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9001,
  "data_sizes": ["1MB"],
  "key_pool_size": 100,
  "inference_delay_ms": 10,
  "warmup_timeout_seconds": 60,
  "target_qps": 20,
  "num_threads": 2,
  "set_param": {"ttl_second": 0},
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"},
    {"host": "127.0.0.1", "port": 9001, "instance_id": 1, "role": "reader"}
  ]
}
```

**启动（两个终端）：**
```bash
# 终端 1：先启动 Reader
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/reader.json

# 终端 2：再启动 Writer
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/writer.json
```

**交互流程：**
1. Reader 启动 → 等待 Writer 预热完成通知
2. Writer 启动 → 预热 100 keys → HTTP POST /notify 通知 Reader
3. Reader 收到通知 → key 池填充 → 开始读取
4. Writer 持续写入 → 通知 Reader → Reader 读取新 key

### 1.3 多 Worker + 多 Client 部署

在同一台机器上部署 2 个 Worker，验证跨节点缓存命中和通知机制。

**前置条件：** 每个 Worker 必须使用独立的 `rocksdb_store_dir`。

**Writer 0 配置 `config/writer0.json`：**
```json
{
  "mode": "pipeline",
  "instance_id": 0,
  "role": "writer",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "data_sizes": ["1MB"],
  "key_pool_size": 50,
  "target_hit_rate": 0.7,
  "target_qps": 10,
  "num_threads": 2,
  "set_param": {"ttl_second": 0},
  "pipeline": ["cacheGetOrCreate"],
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"},
    {"host": "127.0.0.1", "port": 9001, "instance_id": 1, "role": "reader"}
  ]
}
```

Writer 1 / Reader 1 类似，修改 `instance_id`（2/3）、`listen_port`（9002/9003）。

**验证要点：** Writer 命中率趋向 `target_hit_rate`，Reader 跨节点命中 > 0，无 Fail。

---

## 2. Pipeline 操作类型

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
| `mGet` | MGet(keys, buffers) |
| `cacheGetOrCreate` | 先 Get，命中则返回；未命中则 Create+MemoryCopy+Set |

**Pipeline 组合示例：**

| 场景 | pipeline | notify_pipeline |
|------|----------|-----------------|
| 纯写入 | `["setStringView"]` | `[]` |
| 写后读 | `["setStringView", "getBuffer"]` | `[]` |
| Buffer 流水线 | `["createBuffer", "memoryCopy", "setBuffer"]` | `[]` |
| 跨实例验证 | `["setStringView"]` | `["getBuffer"]` |
| 批量写入 | `["mCreate", "mSet"]` | `["mGet"]` |

---

## 3. 高级用法

### 3.1 多 Writer 并发测试

在同一主机启动多个 Writer 实例，共享一个 Worker：

```bash
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/writer0.json
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/writer1.json
```

每个 Writer 使用不同的 `instance_id`、`listen_port`，key 命名按 instance_id 隔离。

### 3.2 不同对象大小混合测试

`data_sizes` 支持配置多个大小，运行时随机选取：

```json
{"data_sizes": ["4KB", "64KB", "1MB", "4MB"], "target_qps": 50}
```

### 3.3 Batch 操作测试

使用 `mCreate` + `mSet` + `mGet` 进行批量操作：

```json
{"pipeline": ["mCreate", "mSet"], "batch_keys_count": 10}
```

### 3.4 不限速压测

`target_qps` 设为 `0` 关闭 QPS 控制，以最大速度发送请求，用于测量 Worker 端吞吐上限：

```json
{"target_qps": 0, "num_threads": 8}
```

不限速模式下日志显示实际 QPS（而非目标 QPS），可直接观察 Worker 承载能力。

### 3.5 多阶段 QPS 压测

`target_qps` 配置为数组时启用多阶段 QPS：

```json
{"target_qps": [100, 200, 500], "stage_duration_seconds": 60}
```

每个阶段运行 60 秒，自动切换 QPS。

### 3.6 CPU 亲和性绑定

进程级 CPU 绑核，在创建任何线程之前执行：

```json
{"cpu_affinity": "0-7"}
```

NUMA 节点绑定（同时绑定 CPU + 本地内存，需 `libnuma`）：

```json
{"numa_node": 0}
```

`numa_node` 优先级高于 `cpu_affinity`。NUMA 不可用时自动回退到 CPU 绑核。

### 3.7 通知节流

当 peer 数量多时，避免瞬时大量 HTTP 请求：

```json
{"notify_interval_us": 1000, "notify_count": 5}
```

---

## 4. Get 数据校验

`getBuffer` / `mGet` / `cacheGetOrCreate` 命中分支在 Get 返回后对数据做校验。校验级别通过 `verify` 配置块控制，**默认 `size` 级别保持与历史行为一致**（仅比对大小，不使操作失败）。

### 4.1 校验级别

| `verify.level` | 校验内容 | CPU 开销 | 适用场景 |
|----------------|---------|---------|---------|
| `off` | 不校验 | 0 | 仅测吞吐，不在意正确性 |
| `size`（默认） | `buf.GetSize() == expected` | ~1 次比较 | 默认基线，与历史行为一致 |
| `sample` | size + 首段/尾段/中间均匀采样段内容比对 | 数十 µs/MB（取决于 `sample_bytes`/`sample_step`） | 大对象生产级正确性巡检 |
| `full` | size + 全量逐字节比对 | ~1 ms/MB（memcpy 量级） | 端到端数据完整性验收 |

### 4.2 采样参数

仅在 `level=sample` 时生效：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `verify.sample_bytes` | `"4KB"` | 每个采样段长度 |
| `verify.sample_step` | `"1MB"` | 采样段起始间隔（1MB 对象约采样 3 段：首、中、尾） |

采样策略：始终覆盖首段 `[0, sample_bytes)` 和尾段 `[size-sample_bytes, size)`，中间每隔 `sample_step` 取一段。8MB 对象 + 默认参数约扫描 12KB（< 0.2%）。

### 4.3 失败处理

```json
{"verify": {"level": "sample", "fail_op": true}}
```

- `fail_op=false`（默认）：校验失败时 `verify_fail` 计数 +1 并打印 `SLOG_WARN`，但**操作仍计为成功**（与历史一致，不污染成功率基线）。
- `fail_op=true`：校验失败时操作**计为失败**（Fail +1），同时 `verify_fail` 也 +1，便于在指标层区分"RPC 失败"与"数据损坏"。

> **注意（行为变更）：** `cacheGetOrCreate` 命中分支此前不做任何校验；启用默认 `size` 级别后会开始检查大小并在不匹配时打印告警。命中率统计不受影响（key 存在即计为 hit），仅多出 `verify_fail` 计数与日志。

### 4.4 配置示例

端到端内容验收（采样）：
```json
{
  "verify": {"level": "sample", "sample_bytes": "4KB", "sample_step": "1MB", "fail_op": true}
}
```

完全校验（小对象 / 验收场景）：
```json
{
  "data_sizes": ["4KB", "64KB"],
  "verify": {"level": "full", "fail_op": true}
}
```

关闭校验（纯吞吐压测）：
```json
{"verify": {"level": "off"}}
```

---

## 5. 测试验证清单

### 5.1 基础写入测试
- [ ] `setStringView` pipeline 正常写入，QPS 接近 target_qps
- [ ] `getBuffer` 读取成功，数据大小校验通过
- [ ] Summary 文件生成，数据与 CSV 一致

### 5.2 多实例部署测试
- [ ] Writer + Reader 正常启动和通信
- [ ] Reader 收到预热通知后开始读取
- [ ] 跨节点 getBuffer 指标反映远端读取
- [ ] 无操作失败（Fail = 0）

### 5.3 Worker 端验证
- [ ] Worker access.log 记录所有操作
- [ ] GET 延迟 p99 < 预期阈值（1MB 对象通常 < 1ms）
- [ ] PUBLISH 延迟 p99 < 预期阈值
- [ ] Worker 资源使用正常（共享内存、线程池）
