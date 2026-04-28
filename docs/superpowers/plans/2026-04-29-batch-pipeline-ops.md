# Batch Pipeline Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增 mCreate/mSet/mGet 三个 batch pipeline op，通过 batch_keys_count 配置每次操作的 key 数量。

**Architecture:** PipelineContext 增加 batchKeys/batchBuffers/batchResults 字段，pipeline.cpp 新增 3 个 op 函数并注册。PipelineLoop 按 batchKeysCount 生成多 key，NotifyPeers 发送 keys 数组，HandleNotify 解析填充。

**Tech Stack:** C++17, datasystem SDK (MCreate/MSet/batch Get)

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/config.h:28` | 新增 `batchKeysCount` 字段 |
| Modify | `src/config.cpp:53,139` | 解析 `batch_keys_count` + 校验 |
| Modify | `src/pipeline.h:20` | PipelineContext 新增 batch 字段 + 3 个 op 常量 |
| Modify | `src/pipeline.cpp:95-111` | 实现 OpMCreate/OpMSet/OpMGet + 注册 |
| Modify | `src/kv_worker.cpp:68-96,113-167` | PipelineLoop 生成 batchKeys + NotifyPeers 改 keys 数组 |
| Modify | `src/http_server.cpp:71-108` | HandleNotify 解析 keys 数组 |
| Modify | `config/config.json.example:15-18` | 新增 batch_keys_count 示例 |

---

### Task 1: Config 扩展

**Files:**
- Modify: `tests/kvclient_standalone/src/config.h:28`
- Modify: `tests/kvclient_standalone/src/config.cpp:53`
- Modify: `tests/kvclient_standalone/src/config.cpp:139`

- [ ] **Step 1: config.h 新增字段**

在 `config.h` 的 `enableCrossNodeConnection` 之后添加：

```cpp
    bool enableCrossNodeConnection = false; // allow failover to standby workers on other nodes
    int batchKeysCount = 1;                 // batch 操作的 key 数量，1 = 单 key 兼容
```

- [ ] **Step 2: config.cpp 解析字段**

在 `config.cpp:54`（`enable_cross_node_connection` 解析行）之后添加：

```cpp
        if (j.contains("batch_keys_count")) cfg.batchKeysCount = j["batch_keys_count"];
```

- [ ] **Step 3: config.cpp 校验**

在 `config.cpp` 校验块中（`ttl_seconds == 0` 的 warn 之前）添加：

```cpp
    if (cfg.batchKeysCount < 1) {
        SLOG_ERROR("batch_keys_count must be >= 1, got " << cfg.batchKeysCount);
        return false;
    }
```

- [ ] **Step 4: 更新日志输出**

在 `config.cpp` 的 SLOG_INFO 日志中追加 batch_keys_count：

```cpp
              << ", threads=" << cfg.numSetThreads
              << ", batch_keys_count=" << cfg.batchKeysCount);
```

- [ ] **Step 5: 更新 config.json.example**

在 Pipeline配置 段落中 `num_set_threads` 之后添加：

```json
  "num_set_threads": 16,
  "batch_keys_count": 1,
```

- [ ] **Step 6: Commit**

```bash
git add tests/kvclient_standalone/src/config.h tests/kvclient_standalone/src/config.cpp tests/kvclient_standalone/config/config.json.example
git commit -m "feat: add batch_keys_count config field for batch pipeline ops"
```

---

### Task 2: PipelineContext 扩展 + Batch Op 实现

**Files:**
- Modify: `tests/kvclient_standalone/src/pipeline.h:20`
- Modify: `tests/kvclient_standalone/src/pipeline.cpp:95-111`

- [ ] **Step 1: pipeline.h 新增 batch 字段**

在 `PipelineContext` 结构体中 `buffer` 字段之后添加：

```cpp
    std::shared_ptr<datasystem::Buffer> buffer;
    std::vector<std::string> batchKeys;
    std::vector<std::shared_ptr<datasystem::Buffer>> batchBuffers;
    std::vector<datasystem::Optional<datasystem::Buffer>> batchResults;
```

- [ ] **Step 2: pipeline.h 新增 op 常量**

在现有 op 常量之后（`kOpSetBuffer` 行之后）添加：

```cpp
inline constexpr const char *kOpMCreate = "mCreate";
inline constexpr const char *kOpMSet = "mSet";
inline constexpr const char *kOpMGet = "mGet";
```

- [ ] **Step 3: pipeline.cpp 实现 OpMCreate**

在 `OpSetBuffer` 函数之后、`kOpRegistry` 之前添加：

```cpp
// mCreate: client->MCreate(keys, sizes, param, buffers)
static bool OpMCreate(PipelineContext &ctx, double &latencyMs) {
    std::vector<uint64_t> sizes(ctx.batchKeys.size(), ctx.size);
    return Measure([&]() {
        return ctx.client->MCreate(ctx.batchKeys, sizes, ctx.param, ctx.batchBuffers);
    }, latencyMs);
}
```

- [ ] **Step 4: pipeline.cpp 实现 OpMSet**

在 OpMCreate 之后添加：

```cpp
// mSet: client->MSet(buffers)
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

- [ ] **Step 5: pipeline.cpp 实现 OpMGet**

在 OpMSet 之后添加：

```cpp
// mGet: client->Get(keys, buffers)
static bool OpMGet(PipelineContext &ctx, double &latencyMs) {
    bool ok = Measure([&]() {
        return ctx.client->Get(ctx.batchKeys, ctx.batchResults);
    }, latencyMs);
    if (!ok) return false;
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

- [ ] **Step 6: 注册到 kOpRegistry**

在 `kOpRegistry` 数组中追加 3 个条目：

```cpp
static const std::vector<std::pair<std::string, OpFunc>> kOpRegistry = {
    {kOpSetStringView, OpSetStringView},
    {kOpGetBuffer, OpGetBuffer},
    {kOpExist, OpExist},
    {kOpCreateBuffer, OpCreateBuffer},
    {kOpMemoryCopy, OpMemoryCopy},
    {kOpSetBuffer, OpSetBuffer},
    {kOpMCreate, OpMCreate},
    {kOpMSet, OpMSet},
    {kOpMGet, OpMGet},
};
```

- [ ] **Step 7: 注册到 kAllOpNames**

在 `kAllOpNames` 数组中追加 3 个名称：

```cpp
static const std::vector<const char *> kAllOpNames = {
    kOpSetStringView, kOpGetBuffer, kOpExist,
    kOpCreateBuffer, kOpMemoryCopy, kOpSetBuffer,
    kOpMCreate, kOpMSet, kOpMGet,
};
```

- [ ] **Step 8: Commit**

```bash
git add tests/kvclient_standalone/src/pipeline.h tests/kvclient_standalone/src/pipeline.cpp
git commit -m "feat: add mCreate/mSet/mGet batch pipeline ops"
```

---

### Task 3: PipelineLoop 生成 batchKeys

**Files:**
- Modify: `tests/kvclient_standalone/src/kv_worker.cpp:68-96`

- [ ] **Step 1: 替换 key 生成逻辑**

将 `kv_worker.cpp` PipelineLoop 中第 68-83 行（从 `uint64_t size = ...` 到 `ctx.verifyFailCount = ...`）替换为：

```cpp
        uint64_t size = cfg_.dataSizes[sizeDist(rng)];
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();

        PipelineContext ctx;
        ctx.batchKeys.resize(cfg_.batchKeysCount);
        for (int i = 0; i < cfg_.batchKeysCount; i++) {
            ctx.batchKeys[i] = "kv_test_" + std::to_string(cfg_.instanceId)
                             + "_" + std::to_string(threadId)
                             + "_" + std::to_string(now) + "_" + std::to_string(i);
        }
        ctx.key = ctx.batchKeys[0];
        ctx.size = size;
        ctx.senderId = cfg_.instanceId;
        ctx.data = pregenData_[size];
        ctx.client = client_;
        ctx.param.writeMode = WriteMode::NONE_L2_CACHE;
        ctx.param.ttlSecond = cfg_.ttlSeconds;
        ctx.verifyFailCount = &metrics_.VerifyFailCounter();
```

- [ ] **Step 2: NotifyPeers 传递 batchKeys**

将 NotifyPeers 调用（第 96 行）从：

```cpp
            NotifyPeers(key, size);
```

改为：

```cpp
            NotifyPeers(ctx.batchKeys, size);
```

- [ ] **Step 3: 修改 NotifyPeers 签名和 body 构建**

将 `kv_worker.cpp` 中整个 `NotifyPeers` 函数替换为：

```cpp
void KVWorker::NotifyPeers(const std::vector<std::string> &keys, uint64_t size) {
    if (cfg_.peers.empty() || cfg_.notifyCount <= 0) return;

    int total = static_cast<int>(cfg_.peers.size());
    int count = std::min(cfg_.notifyCount, total);

    // Fisher-Yates partial shuffle
    std::vector<int> indices(total);
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937 rng(std::random_device{}());
    for (int i = 0; i < count; i++) {
        std::uniform_int_distribution<int> dist(i, total - 1);
        std::swap(indices[i], indices[dist(rng)]);
    }

    // 构建 JSON body：{"keys":["...","..."],"sender":0,"size":8388608}
    std::string body = "{\"keys\":[";
    for (size_t i = 0; i < keys.size(); i++) {
        if (i > 0) body += ",";
        body += "\"" + keys[i] + "\"";
    }
    body += "],\"sender\":" + std::to_string(cfg_.instanceId)
          + ",\"size\":" + std::to_string(size) + "}";

    for (int i = 0; i < count; i++) {
        std::string hostPort = cfg_.peers[indices[i]];
        if (hostPort.size() > 7 && hostPort.compare(0, 7, "http://") == 0) {
            hostPort = hostPort.substr(7);
        }

        auto colonPos = hostPort.find(':');
        if (colonPos == std::string::npos) continue;

        std::string host;
        int port;
        try {
            host = hostPort.substr(0, colonPos);
            port = std::stoi(hostPort.substr(colonPos + 1));
        } catch (...) {
            continue;
        }

        notifyPool_.Submit([host, port, body]() {
            try {
                thread_local std::unordered_map<std::string,
                    std::unique_ptr<httplib::Client>> clientCache;
                std::string ckey = host + ":" + std::to_string(port);
                auto &ref = clientCache[ckey];
                if (!ref) {
                    ref = std::make_unique<httplib::Client>(host, port);
                    ref->set_connection_timeout(2);
                    ref->set_read_timeout(2);
                }
                ref->Post("/notify", body, "application/json");
            } catch (...) {}
        });
    }
}
```

- [ ] **Step 4: 修改 kv_worker.h 声明**

将 `kv_worker.h` 中 NotifyPeers 声明从：

```cpp
    void NotifyPeers(const std::string &key, uint64_t size);
```

改为：

```cpp
    void NotifyPeers(const std::vector<std::string> &keys, uint64_t size);
```

- [ ] **Step 5: Commit**

```bash
git add tests/kvclient_standalone/src/kv_worker.h tests/kvclient_standalone/src/kv_worker.cpp
git commit -m "feat: generate batchKeys in PipelineLoop, send keys array in NotifyPeers"
```

---

### Task 4: HandleNotify 解析 keys 数组

**Files:**
- Modify: `tests/kvclient_standalone/src/http_server.cpp:71-108`

- [ ] **Step 1: 替换 HandleNotify 实现**

将 `http_server.cpp` 中整个 `HandleNotify` 函数替换为：

```cpp
void HttpServer::HandleNotify(const std::string &body) {
    try {
        json j = json::parse(body);
        int sender = j["sender"];
        uint64_t expectedSize = j["size"];

        // 兼容两种格式：keys 数组（batch）和 key 字符串（单 key）
        std::vector<std::string> keys;
        if (j.contains("keys") && j["keys"].is_array()) {
            for (auto &k : j["keys"]) keys.push_back(k.get<std::string>());
        } else if (j.contains("key")) {
            keys.push_back(j["key"].get<std::string>());
        }

        if (keys.empty()) return;

        notifyPool_.Submit([this, keys = std::move(keys), sender, expectedSize]() {
            PipelineContext ctx;
            ctx.key = keys[0];
            ctx.batchKeys = keys;
            ctx.size = expectedSize;
            ctx.senderId = sender;
            if (notifyNeedsData_) {
                auto cacheKey = std::to_string(expectedSize) + "_" + std::to_string(sender);
                {
                    std::lock_guard<std::mutex> lock(pregenMutex_);
                    auto it = pregenData_.find(cacheKey);
                    if (it != pregenData_.end()) {
                        ctx.data = it->second;
                    } else {
                        ctx.data = GeneratePatternData(expectedSize, sender);
                        pregenData_[cacheKey] = ctx.data;
                    }
                }
            }
            ctx.client = client_;
            ctx.param.writeMode = WriteMode::NONE_L2_CACHE;
            ctx.param.ttlSecond = cfg_.ttlSeconds;
            ctx.verifyFailCount = &metrics_.VerifyFailCounter();

            ExecutePipeline(notifyOps_, ctx, metrics_,
                            metrics_.VerifyFailCounter());
        });

    } catch (const std::exception &e) {
        SLOG_WARN("Parse notify body failed: " << e.what());
    }
}
```

关键变化：
- `key` 字段解析改为 `keys` 数组解析（向下兼容单 key 的 `key` 字段）
- `ctx.batchKeys` 填充解析出的 keys
- lambda 捕获 `keys` 用 `std::move` 避免拷贝

- [ ] **Step 2: Commit**

```bash
git add tests/kvclient_standalone/src/http_server.cpp
git commit -m "feat: HandleNotify parses keys array for batch pipeline support"
```

---

### Task 5: 编译验证

**Files:** 无新改动

- [ ] **Step 1: 编译**

```bash
cd tests/kvclient_standalone/build
cmake -DDATASYSTEM_SDK_DIR=$PWD/../../output/datasystem/sdk/cpp .. && make -j$(nproc)
```

Expected: 编译成功，无 error。

- [ ] **Step 2: 验证 usage 输出**

```bash
./kvclient_standalone_test
```

Expected: 打印 usage，退出码 1。

- [ ] **Step 3: 验证 batch_keys_count=0 校验**

创建临时 config 测试校验：

```bash
cat > /tmp/test_batch_config.json << 'EOF'
{
  "instance_id": 0,
  "etcd_address": "http://127.0.0.1:2379",
  "listen_port": 9000,
  "batch_keys_count": 0
}
EOF
./kvclient_standalone_test /tmp/test_batch_config.json
```

Expected: 输出 `[ERROR] batch_keys_count must be >= 1, got 0`，退出码 1。

---

### Task 6: 更新 config.json.example 注释

**Files:**
- Modify: `tests/kvclient_standalone/config/config.json.example`

- [ ] **Step 1: 添加 batch_keys_count 到 Pipeline配置 段**

在 `num_set_threads` 之后添加：

```json
  "num_set_threads": 16,
  "batch_keys_count": 1,
```

- [ ] **Step 2: Commit**

```bash
git add tests/kvclient_standalone/config/config.json.example
git commit -m "docs: add batch_keys_count to config example"
```

---

## Self-Review

**Spec coverage check:**
- [x] Config batchKeysCount 字段 + 解析 + 校验 → Task 1
- [x] PipelineContext batchKeys/batchBuffers/batchResults → Task 2 Step 1
- [x] OpMCreate 实现 → Task 2 Step 3
- [x] OpMSet 实现 → Task 2 Step 4
- [x] OpMGet 实现（含 size 校验）→ Task 2 Step 5
- [x] Op 常量 + registry 注册 → Task 2 Step 2/6/7
- [x] PipelineLoop 生成 batchKeys → Task 3 Step 1
- [x] NotifyPeers 改为 keys 数组 → Task 3 Step 3
- [x] HandleNotify 解析 keys 数组 → Task 4 Step 1
- [x] config.json.example 更新 → Task 6

**Placeholder scan:** 无 TBD/TODO。

**Type consistency:** NotifyPeers 签名 `vector<string>` 在 kv_worker.h/cpp 和调用处一致。PipelineContext.batchKeys 类型为 `vector<string>` 在 pipeline.h/kv_worker.cpp/http_server.cpp 一致。
