# kvtest Cache 模式指南

> **相关文档：** [编译部署与通用配置](user-guide.md) | [Pipeline 模式](pipeline-guide.md) | [Benchmark 模式](benchmark-guide.md)

Cache 模式使用 `cacheGetOrCreate` 流水线操作，模拟 LLM 推理中的 KVCache prefix 缓存场景。Writer 持续从 Key Pool 中随机选取 key 执行 Get（命中则返回，未命中则 Create + MemoryCopy + Set 回填），支持动态命中率控制。

**核心特性：**
- `cacheGetOrCreate` 流水线：先查后写，模拟缓存复用
- Key Pool 机制：预热阶段写入初始 key，稳态从池中随机选取
- 动态命中率控制：`target_hit_rate` 自动调整 Pool 大小（只扩不缩，靠 evict 压命中率）
- 概率化批量 Get：`mget_size_distribution` 支持 writer/reader 一次 MGet 多个 key
- 动态 QPS：writer/reader 均支持 `target_qps_stages` 分阶段与 `target_qps_min/max` 随机
- NX 写入（`set_nx`）与 TTL 过期（`set_param.ttl_second`，默认 1 小时）
- Writer + Reader 跨实例缓存验证
- 推理延迟模拟：Cache miss 后的等待时间

---

## 1. 固定命中率测试

测试 `cacheGetOrCreate` 流水线，验证缓存命中行为。

**配置文件 `config/test_cache.json`：**
```json
{
  "mode": "cache",
  "instance_id": 0,
  "role": "writer",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "data_sizes": ["1MB"],
  "key_pool_size": 100,
  "target_qps": 10,
  "num_threads": 2,
  "set_param": {"ttl_second": 0},
  "pipeline": ["cacheGetOrCreate"],
  "nodes": [
    {"host": "127.0.0.1", "port": 9000, "instance_id": 0, "role": "writer"}
  ]
}
```

**运行：**
```bash
LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH ./kvtest config/test_cache.json
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

---

## 2. 动态命中率调整

启用 `target_hit_rate` 让系统自动调整 Key Pool 大小。

**配置文件 `config/test_dynamic.json`：**
```json
{
  "mode": "cache",
  "instance_id": 0,
  "role": "writer",
  "etcd_address": "127.0.0.1:2379",
  "listen_port": 9000,
  "data_sizes": ["1MB"],
  "key_pool_size": 100,
  "target_hit_rate": 0.8,
  "target_qps": 10,
  "num_threads": 4,
  "set_param": {"ttl_second": 0},
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

---

## 3. Cache 模式参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `key_pool_size` | int | 0 | Key 池初始大小，**0 = 禁用 Cache 模式** |
| `target_hit_rate` | double | 0.8 | 目标命中率，0 = 固定池大小（不自动调整），0.01~1.0 = 自动调整 |
| `cache_update_prob` | double | 0.1 | 命中后以该概率重新 Set 一个带版本号的新值（覆盖写路径），0 = 关闭 |
| `set_nx` | bool | true | 命中写入用 `existence=NX`（仅当 key 不存在才写）；true 时跳过版本检查与更新广播 |
| `hot_key_ratio` | double | 0.0 | 热集占池比例，(0.0, 1.0]，0 = 均匀随机 |
| `hot_access_prob` | double | 0.0 | 每次从热集取 key 的概率，[0.0, 1.0]，0 = 均匀随机（与 hot_key_ratio 必须同时为 0 或同时 >0） |
| `max_key_pool_size` | int | 0 | Key 池上限，0 = 自动（`kMaxPoolSize` = 5M） |
| `set_param.ttl_second` | int | 3600 | key 过期时间（秒）；0 = 永不过期。cache 模式默认 1 小时 |
| `mget_size_distribution` | object | 关闭 | 概率化批量 Get（见 §8）；`single_prob < 1.0` 启用 |
| `inference_delay_ms` | int | 0 | 模拟推理延迟（毫秒），Cache miss 后的等待 |
| `warmup_retry_count` | int | 3 | 预热阶段每个 key 的最大重试次数 |
| `warmup_retry_delay_ms` | int | 1000 | 预热重试间隔（毫秒） |
| `warmup_timeout_seconds` | int | 300 | Reader 等待所有 Writer 预热完成的超时 |

> **data_sizes 多档位**：Cache 模式下每个 key 的大小由其 idx 确定（`size = data_sizes[idx % 档位数]`）。同一 key 永远同一大小，warmup / 稳态 / 回填 / 更新全链路一致，多档位可正常使用且 verify 不会误报 size mismatch。warmup 阶段各档位近似均分（idx 连续取模）。

---

## 4. 命中率调整算法

- 每 `metrics_interval_ms`（默认 3s）评估一次当前窗口命中率
- 命中率 > target + 0.02 → 扩大池（新增一批未见过 key，广播给所有 client）
- 命中率 < target - 0.02 → 保持池大小，miss 100% 回填暖场
- 命中率 ≈ target → 保持
- 调整步长：当前池大小的 1/20（即 5%），最小 1；**池只增不减**（索引不复用）
- 池上限由 `max_key_pool_size` 控制（默认 `kMaxPoolSize` = 5M）
- 命中率下降依赖「池不断扩张 → 数据量超过缓存容量 → LRU 淘汰」自然产生 miss（新增 key 采样到后 miss → 回填 Set，不会保持为空）

---

## 5. 推理延迟模拟

模拟 LLM 推理场景中 Cache miss 的处理延迟：

```json
{
  "key_pool_size": 100,
  "inference_delay_ms": 50,
  "num_threads": 2,
  "set_param": {"ttl_second": 0},
  "pipeline": ["cacheGetOrCreate"]
}
```

Reader 每次读取后等待指定毫秒数，模拟推理计算间隔。

---

## 6. Get 数据校验

`cacheGetOrCreate` 命中分支会在 Get 返回后校验缓存内容。校验行为由 `verify` 配置块控制，**默认 `size` 级别**（仅比对大小，不使操作失败，与历史一致）。

> **行为变更提示：** 此前命中分支不做任何校验；启用默认 `size` 后会开始检查大小并在不匹配时打印 `SLOG_WARN` + `verify_fail` 计数。命中率统计不变（key 存在即计为 hit）。

| `verify.level` | 校验内容 | 适用场景 |
|----------------|---------|---------|
| `off` | 不校验 | 纯命中率/吞吐压测 |
| `size`（默认） | `buf.GetSize() == expected` | 默认基线 |
| `sample` | size + 首尾/中间采样内容比对 | 大对象生产级正确性巡检（1MB 对象默认扫描 ~3 段共 12KB） |
| `full` | size + 全量逐字节比对 | 小对象端到端验收 |

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `verify.sample_bytes` | `"4KB"` | `level=sample` 时每段长度 |
| `verify.sample_step` | `"1MB"` | `level=sample` 时采样段间隔 |
| `verify.fail_op` | false | true=校验失败让操作计入 Fail；false=仅 `verify_fail` +1 + 日志 |

**采样级正确性巡检示例：**
```json
{
  "key_pool_size": 100,
  "data_sizes": ["1MB"],
  "verify": {"level": "sample", "sample_bytes": "4KB", "sample_step": "1MB", "fail_op": true}
}
```

校验失败时日志形如：
```
2026-08-19 03:11:23.451 [WARN] cacheGetOrFill content mismatch on hit: key=cache_pool_0_42 level=2 senderId=0
```
对应 `stats` JSON 的 `verify_fail` 字段会递增。SLOG 行格式为 `<YYYY-MM-DD HH:MM:SS.mmm> [INFO|WARN|ERROR] <msg>`，时间戳为本地时间。完整参数说明见 [user-guide.md 公共参数表](user-guide.md)。

### 6.1 命中后更新值（`cache_update_prob`）

`cache_update_prob > 0` 时，命中分支在校验通过后以该概率重新 `Set` 一个**带版本号的新值**（前 8 字节写递增版本号，其余保持确定性 pattern），用于压测覆盖写/写穿路径。`cache_update_prob=0`（默认）关闭，命中仍只读。

- 版本号使每次更新的值内容真实变化，但总大小不变 → 默认 `verify.level=size` 不受影响。
- **注意**：因值前 8 字节是版本号，`verify.level=sample/full` 会对更新后的 key 报 `content mismatch`。启用 `cache_update_prob` 时建议保持默认 `size` 校验。
- 更新操作单独记录为指标 `cacheSetUpdate`。

### 6.2 Get 错误与未命中区分

Get 返回码分三类：`K_OK`（命中）、`K_NOT_FOUND`（正常 miss → 回填）、其他错误码（超时/连接错误等）。

- 其他错误码**不**回填、不计 hit/miss，单独计入 `cache_get_error`（stats JSON 与 `run_summary.txt` 均输出），并按每 1000 次节流打印 `SLOG_WARN`。

### 6.3 Reader 池的滚动更新

Writer 稳态每次成功操作都会把触碰的 key 以空 action 通知 Reader，Reader 收到后追加进自己的 key 池（超过 `max_key_pool_size` 时裁剪**非 warmup 段**）。因此 Reader 池 = 全部 writer 的 warmup key + 滚动收到的活跃 key，不会漏掉扩池产生的新 key。warmup key 永不裁剪；若 `writer_count × key_pool_size > max_key_pool_size`，会告警并保留全部 warmup key（evict 窗口失效）。

---

## 7. 测试验证清单

### 7.1 cacheGetOrCreate 测试
- [ ] 预热阶段完成，所有 key 写入成功
- [ ] 稳态阶段命中率 > 0
- [ ] 子操作指标（getBuffer/createBuffer/memoryCopy/setBuffer）正确记录
- [ ] cache_summary 行的 hit_rate 与实际一致

### 7.2 target_hit_rate 动态调整测试
- [ ] 设置 `target_hit_rate=0.8` 后，Pool Size 自动调整
- [ ] 命中率在 target ± 0.05 范围内波动
- [ ] Pool Size 不低于 `max(10, key_pool_size / 10)`，不超过 `key_pool_size × 20`

### 7.3 跨实例缓存测试
- [ ] Writer 预热完成后通知 Reader
- [ ] Reader 收到通知后开始读取
- [ ] 跨节点 getBuffer 指标反映远端读取
- [ ] 无操作失败（Fail = 0）

### 7.4 数据校验测试
- [ ] `verify.level=full` + `fail_op=true` 下，命中率正常且 `verify_fail=0`
- [ ] 人为篡改 key 内容后 `verify_fail` 递增、日志出现 `content mismatch`
- [ ] `verify.level=off` 下 `verify_fail` 恒为 0

### 7.5 命中更新值（cache_update_prob）
- [ ] `cache_update_prob=0`（默认）时 `cacheSetUpdate=0`，命中只读
- [ ] `cache_update_prob=0.5` 下 `cacheSetUpdate > 0`，且命中率统计不受影响
- [ ] 更新后的 key 再次 Get 大小仍 = 配置 size（`verify.level=size` 通过）

### 7.6 热点分布（hot_key_ratio / hot_access_prob）
- [ ] `hot_key_ratio=0.05` + `hot_access_prob=0.95` 下，命中率显著高于均匀基线
- [ ] 只设其中一个字段（另一个为 0）时启动直接报错退出

### 7.7 Get 错误分类
- [ ] 注入一次 Get 超时/连接错误后 `cache_get_error > 0`，且不计入 hit/miss
- [ ] 多 writer（>20）下 reader 池覆盖 = N×P 全量（warmup key 不被裁）

### 7.8 多档位 data_sizes
- [ ] `data_sizes=["8MB","16MB"]` 下，key `cache_pool_0_N` 的大小 = N%2 对应档位，verify 无 size mismatch
- [ ] 同一 key 命中 / 回填 / 更新（cache_update_prob）后大小始终不变
- [ ] 单档配置行为与旧版一致（退化为固定大小）

---

## 8. 概率化批量 Get（mget）

`mget_size_distribution` 让 cache 模式的 writer/reader 在 Get 时按概率采样批大小，一次 `MGet` 多个 key：

```json
{
  "mget_size_distribution": {
    "single_prob": 0.95,
    "min_batch": 2,
    "max_batch": 5,
    "pending_queue_max": 65536
  }
}
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `single_prob` | 1.0 | 单 key Get 的概率；< 1.0 启用批量 |
| `min_batch` | 2 | 批量分支的最小批大小 |
| `max_batch` | 5 | 批量分支的最大批大小 |
| `pending_queue_max` | 65536 | 仅 pipeline 模式 notify 驱动 mGet 的队列上限；cache 模式不使用 |

语义：

- 每次 Get 以 `single_prob` 概率取 1 个 key，否则均匀取 `[min_batch, max_batch]` 个 key
- 抽样 key 去重；一次 `MGet` 后逐 key 处理 hit（校验 + 可选 re-Set）/ miss（回填）
- `single_prob >= 1.0`（默认）关闭，走单 key 路径
- 日志里批量 Get 记为 `mGet`（替代单 key 的 `getBuffer`）

> **注意**：`pending_queue_max` 只对 pipeline 模式 notify 驱动的 mGet 有效；cache 模式主动采样直接 MGet，无 pending 队列。
