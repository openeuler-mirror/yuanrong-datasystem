# kvtest Cache 模式指南

> **相关文档：** [编译部署与通用配置](user-guide.md) | [Pipeline 模式](pipeline-guide.md) | [Benchmark 模式](benchmark-guide.md)

Cache 模式使用 `cacheGetOrCreate` 流水线操作，模拟 LLM 推理中的 KVCache prefix 缓存场景。Writer 持续从 Key Pool 中随机选取 key 执行 Get（命中则返回，未命中则 Create + MemoryCopy + Set 回填），支持动态命中率控制。

**核心特性：**
- `cacheGetOrCreate` 流水线：先查后写，模拟缓存复用
- Key Pool 机制：预热阶段写入初始 key，稳态从池中随机选取
- 动态命中率控制：`target_hit_rate` 自动调整 Pool 大小
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
| `target_hit_rate` | double | 0.0 | 目标命中率，0 = 固定池大小，0.01~1.0 = 自动调整 |
| `max_key_pool_size` | int | 0 | Key 池上限，0 = 自动（key_pool_size × 20） |
| `inference_delay_ms` | int | 0 | 模拟推理延迟（毫秒），Cache miss 后的等待 |
| `warmup_retry_count` | int | 3 | 预热阶段每个 key 的最大重试次数 |
| `warmup_retry_delay_ms` | int | 1000 | 预热重试间隔（毫秒） |
| `warmup_timeout_seconds` | int | 300 | Reader 等待所有 Writer 预热完成的超时 |

---

## 4. 命中率调整算法

- 每 `metrics_interval_ms`（默认 3s）评估一次当前命中率
- 命中率 > target + 0.02 → 扩大池（增加 key 范围），命中率自然下降
- 命中率 < target - 0.02 → 缩小池（减少 key 范围），命中率自然上升
- 调整步长：当前池大小的 1/20（即 5%），最小 1
- 池大小范围：`max(10, key_pool_size / 10)` ~ `key_pool_size × 20`
- 池大小上限由 `max_key_pool_size` 配置项控制（默认 `key_pool_size × 20`）

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
[WARN] cacheGetOrFill content mismatch on hit: key=cache_pool_0_42 level=2 senderId=0
```
对应 `stats` JSON 的 `verify_fail` 字段会递增。完整参数说明见 [user-guide.md 公共参数表](user-guide.md)。

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
