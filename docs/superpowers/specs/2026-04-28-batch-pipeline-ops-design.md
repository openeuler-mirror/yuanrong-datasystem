# Batch Pipeline Ops Design

**Date:** 2026-04-28
**Status:** Draft

## Context

kvclient_standalone_test 现有 6 个单 key pipeline op（setStringView, getBuffer, exist, createBuffer, memoryCopy, setBuffer）。需要新增 3 个 batch op（mCreate, mSet, mGet）支持多 key 批量操作，通过 `batch_keys_count` 配置每次操作的 key 数量。

## SDK API 签名

```cpp
// kv_client.h:160-161
Status MCreate(const std::vector<std::string> &keys,
               const std::vector<uint64_t> &sizes,
               const SetParam &param,
               std::vector<std::shared_ptr<Buffer>> &buffers);

// kv_client.h:173-174
Status MSet(const std::vector<std::shared_ptr<Buffer>> &buffers);

// kv_client.h:275-276
Status Get(const std::vector<std::string> &keys,
           std::vector<Optional<Buffer>> &buffers,
           int32_t subTimeoutMs = 0);
```

## 设计

### 1. Config 扩展

`config.h` 新增字段：

```cpp
int batchKeysCount = 1;  // batch 操作的 key 数量，1 = 单 key 兼容
```

`config.json` 对应字段：

```json
{
  "batch_keys_count": 10,
  "pipeline": ["mCreate", "mSet"],
  "notify_pipeline": ["mGet"]
}
```

`config.cpp` 解析逻辑：`if (j.contains("batch_keys_count")) cfg.batchKeysCount = j["batch_keys_count"]`

校验规则：`batchKeysCount` 必须 >= 1，否则报错退出。

### 2. PipelineContext 扩展

`pipeline.h` 新增字段：

```cpp
struct PipelineContext {
    // 现有字段...
    std::string key;                          // 单 key（兼容现有 op）
    std::string data;                         // 单 key 数据
    uint64_t size = 0;

    // 新增 batch 字段
    std::vector<std::string> batchKeys;       // batchKeysCount 个 key
    std::vector<std::shared_ptr<datasystem::Buffer>> batchBuffers;  // MCreate 输出 / MSet 输入
    std::vector<datasystem::Optional<datasystem::Buffer>> batchResults;  // MGet 输出
};
```

### 3. 三个 Batch Op 实现

`pipeline.cpp` 新增 3 个 static 函数 + 注册到 kOpRegistry + 注册到 kAllOpNames。

#### mCreate

```cpp
static bool OpMCreate(PipelineContext &ctx, double &latencyMs) {
    // 构建 sizes 向量：batchKeysCount 个 ctx.size
    std::vector<uint64_t> sizes(ctx.batchKeys.size(), ctx.size);
    return Measure([&]() {
        return ctx.client->MCreate(ctx.batchKeys, sizes, ctx.param, ctx.batchBuffers);
    }, latencyMs);
}
```

#### mSet

```cpp
static bool OpMSet(PipelineContext &ctx, double &latencyMs) {
    if (ctx.batchBuffers.empty()) {
        SLOG_WARN("mSet: no buffers (mCreate not called?)");
        latencyMs = 0;
        return false;
    }
    return Measure([&]() {
        return ctx.client->MSet(ctx.batchBuffers);
    }, latencyMs);
}
```

#### mGet

```cpp
static bool OpMGet(PipelineContext &ctx, double &latencyMs) {
    bool ok = Measure([&]() {
        return ctx.client->Get(ctx.batchKeys, ctx.batchResults);
    }, latencyMs);
    if (!ok) return false;
    // 校验每个 key 的 buffer size
    for (size_t i = 0; i < ctx.batchResults.size(); i++) {
        if (ctx.batchResults[i]) {
            int64_t bufSize = ctx.batchResults[i]->GetSize();
            if (static_cast<uint64_t>(bufSize) != ctx.size) {
                SLOG_WARN("mGet size mismatch: key=" << ctx.batchKeys[i]
                          << " expected=" << ctx.size << " got=" << bufSize);
                if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
            }
        }
    }
    return true;
}
```

### 4. Op 名称常量

```cpp
inline constexpr const char *kOpMCreate = "mCreate";
inline constexpr const char *kOpMSet = "mSet";
inline constexpr const char *kOpMGet = "mGet";
```

### 5. PipelineLoop 改动

`kv_worker.cpp` PipelineLoop 中，每轮循环生成 key 时：

- 若 pipeline 中包含 mCreate/mSet/mGet 任一 op，则同时填充 `ctx.batchKeys`（`batchKeysCount` 个 key）
- `ctx.key` 仍然保留第一个 key，兼容现有单 key op
- `ctx.data` 使用预生成的数据（与现有逻辑一致）

```cpp
// 生成 batchKeys
ctx.batchKeys.clear();
for (int i = 0; i < cfg_.batchKeysCount; i++) {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    ctx.batchKeys.push_back("kv_test_" + std::to_string(cfg_.instanceId)
                           + "_" + std::to_string(threadId)
                           + "_" + std::to_string(now) + "_" + std::to_string(i));
}
ctx.key = ctx.batchKeys[0];  // 兼容单 key op
```

### 6. HttpServer HandleNotify 改动

notify handler 收到请求后，需要构造 `batchKeys`。有两种方案：

**方案 A**：notify body 传递所有 batch keys。Writer NotifyPeers 时发送完整 key 列表。
**方案 B**：notify body 只发送一个 key 前缀 + count，reader 端重建 keys。

选择 **方案 A**：改动最小，JSON body 自然扩展。

NotifyPeers 修改：
```cpp
// body 改为数组格式
// 单 key: {"keys":["kv_test_0_0_1234"],"sender":0,"size":8388608}
// 新增 keys 数组字段，保持 key 字段兼容
```

HandleNotify 修改：
```cpp
// 解析 keys 数组填充 batchKeys
if (j.contains("keys")) {
    for (auto &k : j["keys"]) ctx.batchKeys.push_back(k);
}
```

## 修改文件清单

| 文件 | 改动 |
|------|------|
| `src/config.h` | 新增 `batchKeysCount` 字段 |
| `src/config.cpp` | 解析 `batch_keys_count` + 校验 >= 1 |
| `src/pipeline.h` | PipelineContext 新增 batchKeys/batchBuffers/batchResults + 3 个 op 常量 |
| `src/pipeline.cpp` | 实现 OpMCreate/OpMSet/OpMGet + 注册到 registry + 注册到 kAllOpNames |
| `src/kv_worker.cpp` | PipelineLoop 生成 batchKeys + NotifyPeers body 改为 keys 数组 |
| `src/http_server.cpp` | HandleNotify 解析 keys 数组填充 batchKeys |
| `config/config.json.example` | 新增 batch_keys_count 示例 |

## 测试验证方案

### 1. 单 key 兼容测试
**配置**: `batch_keys_count=1`, `pipeline=["mCreate","mSet"]`, `notify_pipeline=["mGet"]`
**验证**: 行为与 `createBuffer`+`setBuffer` 一致，metrics 中 mCreate/mSet/mGet 均有成功计数。

### 2. 多 key batch 写入测试
**配置**: `batch_keys_count=10`, `pipeline=["mCreate","mSet"]`, `target_qps=100`
**验证**:
- mCreate 成功率 100%
- mSet 成功率 100%
- summary 中 mCreate count ≈ 运行时间 × target_qps / num_set_threads

### 3. 多 key batch 读取测试
**配置**: writer `pipeline=["mCreate","mSet"]`, reader `notify_pipeline=["mGet"]`, `batch_keys_count=10`
**验证**:
- writer notify 发送 10 个 key
- reader mGet 成功返回 10 个 buffer
- verifyFailCount = 0（size 校验全部通过）

### 4. 异常场景测试
- `batch_keys_count=0`: 启动报错，提示必须 >= 1
- `batch_keys_count=1` + 单 key pipeline `["setStringView"]`: 正常运行，batchKeys 只有一个元素但不影响单 key op

### 5. 性能对比测试
**配置**: 相同 target_qps 和 data_sizes，分别用单 key 和 batch 模式运行
**验证**:
- batch 模式 QPS（按 key 数计算）应高于单 key 模式
- batch 模式平均延迟可能高于单 key（批量开销），但吞吐更高
