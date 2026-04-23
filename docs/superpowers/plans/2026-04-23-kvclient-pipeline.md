# Pipeline & Role Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend kvclient_standalone with configurable operation pipelines, writer/reader roles, and 4 new SDK API tests.

**Architecture:** Introduce a `Pipeline` module that maps string operation names to function objects. The worker loop and /notify handler execute the pipeline against each key. Config gains `role`, `pipeline`, `notify_pipeline` fields. Metrics pre-creates all 6 operation buckets.

**Tech Stack:** C++17, datasystem SDK (KVClient, Buffer, Optional, StringView), nlohmann_json, httplib

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `tests/kvclient_standalone/pipeline.h` | PipelineContext struct, op name constants, OpFunc type, ExecutePipeline function, registration |
| Create | `tests/kvclient_standalone/pipeline.cpp` | 6 atomic op implementations + ExecutePipeline |
| Modify | `tests/kvclient_standalone/config.h` | Add role, pipeline, notify_pipeline fields |
| Modify | `tests/kvclient_standalone/config.cpp` | Parse new config fields |
| Modify | `tests/kvclient_standalone/metrics.cpp` | Pre-create 6 op buckets, change from "set"/"get" to op names |
| Modify | `tests/kvclient_standalone/kv_worker.h` | Include pipeline.h, change SetLoop to PipelineLoop |
| Modify | `tests/kvclient_standalone/kv_worker.cpp` | Rewrite SetLoop to use pipeline execution |
| Modify | `tests/kvclient_standalone/http_server.h` | Remove GenerateExpectedData, add pipeline dependency |
| Modify | `tests/kvclient_standalone/http_server.cpp` | Rewrite HandleNotify to use notify_pipeline |
| Modify | `tests/kvclient_standalone/main.cpp` | Skip KVWorker for reader role |
| Modify | `tests/kvclient_standalone/CMakeLists.txt` | Add pipeline.cpp to sources |
| Modify | `tests/kvclient_standalone/config.json.example` | Add new fields |
| Modify | `tests/kvclient_standalone/deploy.sh` | Support node-level role/pipeline override |
| Create | `tests/kvclient_standalone/test_deploy.sh` | Self-contained multi-node deployment test (local + openclaw via SSH tunnels) |

---

### Task 1: Create pipeline.h — PipelineContext and op declarations

**Files:**
- Create: `tests/kvclient_standalone/pipeline.h`

- [ ] **Step 1: Write pipeline.h**

```cpp
#pragma once

#include <datasystem/kv_client.h>
#include <datasystem/object/buffer.h>
#include <datasystem/utils/optional.h>
#include <datasystem/utils/string_view.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct PipelineContext {
    std::string key;
    std::string data;
    uint64_t size = 0;
    int senderId = 0;
    datasystem::SetParam param;
    std::shared_ptr<datasystem::KVClient> client;
    std::shared_ptr<datasystem::Buffer> buffer;
};

// Op function: returns true on success, fills latencyMs.
using OpFunc = std::function<bool(PipelineContext &ctx, double &latencyMs)>;

// Op name constants
inline constexpr const char *kOpSetStringView = "setStringView";
inline constexpr const char *kOpGetBuffer = "getBuffer";
inline constexpr const char *kOpExist = "exist";
inline constexpr const char *kOpCreateBuffer = "createBuffer";
inline constexpr const char *kOpMemoryCopy = "memoryCopy";
inline constexpr const char *kOpSetBuffer = "setBuffer";

// All known op names (for metrics pre-creation).
const std::vector<const char *> &GetAllOpNames();

// Look up op function by name. Returns nullptr for unknown ops.
OpFunc GetOpFunc(const std::string &name);

// Execute a pipeline: run each op in order, record metrics via its name.
// Returns true if all ops succeeded.
bool ExecutePipeline(
    const std::vector<std::pair<std::string, OpFunc>> &ops,
    PipelineContext &ctx,
    class MetricsCollector &metrics,
    std::atomic<uint64_t> &verifyFailCount);
```

- [ ] **Step 2: Commit**

```bash
git add tests/kvclient_standalone/pipeline.h
git commit -m "feat: add pipeline.h with PipelineContext and op declarations"
```

---

### Task 2: Create pipeline.cpp — 6 atomic op implementations

**Files:**
- Create: `tests/kvclient_standalone/pipeline.cpp`

- [ ] **Step 1: Write pipeline.cpp**

```cpp
#include "pipeline.h"
#include "metrics.h"
#include "data_pattern.h"
#include "simple_log.h"
#include <chrono>

using namespace datasystem;

static bool Measure(std::function<Status()> fn, double &latencyMs) {
    auto start = std::chrono::steady_clock::now();
    Status rc = fn();
    auto end = std::chrono::steady_clock::now();
    latencyMs = std::chrono::duration<double, std::milli>(end - start).count();
    return rc.IsOk();
}

// setStringView: client->Set(key, StringView(data), param)
static bool OpSetStringView(PipelineContext &ctx, double &latencyMs) {
    return Measure([&]() {
        return ctx.client->Set(ctx.key, StringView(ctx.data), ctx.param);
    }, latencyMs);
}

// getBuffer: client->Get(key, Optional<Buffer>&)
// On success, verify data size and content.
static bool OpGetBuffer(PipelineContext &ctx, double &latencyMs) {
    Optional<Buffer> optBuf;
    bool ok = Measure([&]() {
        return ctx.client->Get(ctx.key, optBuf);
    }, latencyMs);
    if (!ok || !optBuf) return false;

    // Verify size
    int64_t bufSize = optBuf->GetSize();
    if (static_cast<uint64_t>(bufSize) != ctx.size) {
        SLOG_WARN("getBuffer size mismatch: key=" << ctx.key
                  << " expected=" << ctx.size << " got=" << bufSize);
        return true; // op succeeded, but caller checks verify
    }

    // Verify content
    std::string expected = GeneratePatternData(ctx.size, ctx.senderId);
    const char *bufData = static_cast<const char *>(optBuf->ImmutableData());
    std::string actual(bufData, bufSize);
    if (actual != expected) {
        SLOG_WARN("getBuffer content mismatch: key=" << ctx.key);
        // Don't return false — the Get itself succeeded, data mismatch is a verify fail
    }
    return true;
}

// exist: client->Exist({key}, exists)
static bool OpExist(PipelineContext &ctx, double &latencyMs) {
    std::vector<bool> exists;
    bool ok = Measure([&]() {
        return ctx.client->Exist({ctx.key}, exists);
    }, latencyMs);
    if (!ok) return false;
    // Verify key exists
    if (exists.empty() || !exists[0]) {
        SLOG_WARN("exist: key not found: " << ctx.key);
        // Op succeeded but verify failed — caller handles this
    }
    return true;
}

// createBuffer: client->Create(key, size, param, buffer)
static bool OpCreateBuffer(PipelineContext &ctx, double &latencyMs) {
    std::shared_ptr<Buffer> buf;
    bool ok = Measure([&]() {
        return ctx.client->Create(ctx.key, ctx.size, ctx.param, buf);
    }, latencyMs);
    if (ok && buf) {
        ctx.buffer = buf;
    }
    return ok;
}

// memoryCopy: buffer->MemoryCopy(data, size)
static bool OpMemoryCopy(PipelineContext &ctx, double &latencyMs) {
    if (!ctx.buffer) {
        SLOG_WARN("memoryCopy: no buffer (createBuffer not called?)");
        latencyMs = 0;
        return false;
    }
    return Measure([&]() {
        return ctx.buffer->MemoryCopy(ctx.data.data(), ctx.size);
    }, latencyMs);
}

// setBuffer: client->Set(buffer)
static bool OpSetBuffer(PipelineContext &ctx, double &latencyMs) {
    if (!ctx.buffer) {
        SLOG_WARN("setBuffer: no buffer (createBuffer not called?)");
        latencyMs = 0;
        return false;
    }
    return Measure([&]() {
        return ctx.client->Set(ctx.buffer);
    }, latencyMs);
}

// ---- Registry ----

static const std::vector<std::pair<std::string, OpFunc>> kOpRegistry = {
    {kOpSetStringView, OpSetStringView},
    {kOpGetBuffer, OpGetBuffer},
    {kOpExist, OpExist},
    {kOpCreateBuffer, OpCreateBuffer},
    {kOpMemoryCopy, OpMemoryCopy},
    {kOpSetBuffer, OpSetBuffer},
};

static const std::vector<const char *> kAllOpNames = {
    kOpSetStringView, kOpGetBuffer, kOpExist,
    kOpCreateBuffer, kOpMemoryCopy, kOpSetBuffer,
};

const std::vector<const char *> &GetAllOpNames() { return kAllOpNames; }

OpFunc GetOpFunc(const std::string &name) {
    for (auto &[n, fn] : kOpRegistry) {
        if (n == name) return fn;
    }
    return nullptr;
}

bool ExecutePipeline(
    const std::vector<std::pair<std::string, OpFunc>> &ops,
    PipelineContext &ctx,
    MetricsCollector &metrics,
    std::atomic<uint64_t> &verifyFailCount) {
    bool allOk = true;
    for (auto &[name, fn] : ops) {
        double latencyMs = 0;
        bool ok = fn(ctx, latencyMs);
        metrics.Record(name, latencyMs, ok);
        if (!ok) {
            allOk = false;
            break;
        }
    }
    return allOk;
}

- [ ] **Step 2: Commit**

```bash
git add tests/kvclient_standalone/pipeline.cpp
git commit -m "feat: add pipeline.cpp with 6 atomic ops and ExecutePipeline"
```

---

### Task 3: Update config.h and config.cpp — Add role, pipeline, notify_pipeline fields

**Files:**
- Modify: `tests/kvclient_standalone/config.h`
- Modify: `tests/kvclient_standalone/config.cpp`

- [ ] **Step 1: Update config.h**

Add after the existing fields:

```cpp
#pragma once

#include <string>
#include <vector>
#include <cstdint>

struct Config {
    int instanceId = 0;
    int listenPort = 9000;
    std::string etcdAddress;
    std::string clusterName = "default";
    int32_t connectTimeoutMs = 1000;
    int32_t requestTimeoutMs = 20;
    std::vector<uint64_t> dataSizes;
    uint32_t ttlSeconds = 5;
    int targetQps = 1600;
    int numSetThreads = 16;
    int notifyCount = 10;
    int metricsIntervalMs = 3000;
    std::string metricsFile = "metrics_{instance_id}.csv";
    std::vector<std::string> peers;
    // New fields
    std::string role = "writer";                    // "writer" or "reader"
    std::vector<std::string> pipeline = {"setStringView"};
    std::vector<std::string> notifyPipeline = {"getBuffer"};
};

uint64_t ParseSize(const std::string &str);
bool LoadConfig(const std::string &path, Config &cfg);
```

- [ ] **Step 2: Update config.cpp — add parsing for new fields**

In `LoadConfig()`, add after the existing field parsing (after the `peers` block, before the `{instance_id}` replacement):

```cpp
        if (j.contains("role")) cfg.role = j["role"].get<std::string>();
        if (j.contains("pipeline")) {
            cfg.pipeline.clear();
            for (auto &s : j["pipeline"]) {
                cfg.pipeline.push_back(s.get<std::string>());
            }
        }
        if (j.contains("notify_pipeline")) {
            cfg.notifyPipeline.clear();
            for (auto &s : j["notify_pipeline"]) {
                cfg.notifyPipeline.push_back(s.get<std::string>());
            }
        }
```

Also update the log line at the end to include role and pipeline:

```cpp
    SLOG_INFO("Config loaded: instance_id=" << cfg.instanceId
              << ", port=" << cfg.listenPort
              << ", etcd=" << cfg.etcdAddress
              << ", role=" << cfg.role
              << ", pipeline=[" << /* join pipeline names */ "]"
              << ", notify_pipeline=[" << /* join notify_pipeline names */ "]"
              << ", data_sizes_count=" << cfg.dataSizes.size()
              << ", target_qps=" << cfg.targetQps
              << ", threads=" << cfg.numSetThreads);
```

For the join, use a helper or inline loop:

```cpp
    auto joinStr = [](const std::vector<std::string> &v) -> std::string {
        std::string r;
        for (size_t i = 0; i < v.size(); i++) {
            if (i > 0) r += ",";
            r += v[i];
        }
        return r;
    };
```

- [ ] **Step 3: Commit**

```bash
git add tests/kvclient_standalone/config.h tests/kvclient_standalone/config.cpp
git commit -m "feat: add role, pipeline, notify_pipeline config fields"
```

---

### Task 4: Update metrics.cpp — Pre-create all 6 op buckets

**Files:**
- Modify: `tests/kvclient_standalone/metrics.cpp`

- [ ] **Step 1: Replace Start() pre-creation**

In `MetricsCollector::Start()`, replace the hardcoded "set"/"get" pre-creation:

```cpp
    // Old:
    // GetOrCreateOp("set");
    // GetOrCreateOp("get");

    // New: pre-create all 6 pipeline op buckets
    for (auto *name : GetAllOpNames()) {
        GetOrCreateOp(name);
    }
```

This requires adding `#include "pipeline.h"` at the top of metrics.cpp.

Also add a public method to metrics.h to expose the verify fail counter for ExecutePipeline:

In `metrics.h`, change `RecordVerifyFail()` stays as-is. The `verifyFailCount_` is already `std::atomic<uint64_t>` and `RecordVerifyFail()` just increments it. So ExecutePipeline can call `metrics.RecordVerifyFail()` — no change needed to the metrics interface.

But ExecutePipeline takes `std::atomic<uint64_t> &verifyFailCount`. We need to expose the internal atomic. Add to `MetricsCollector` public section in metrics.h:

```cpp
    std::atomic<uint64_t> &VerifyFailCounter() { return verifyFailCount_; }
```

- [ ] **Step 2: Commit**

```bash
git add tests/kvclient_standalone/metrics.h tests/kvclient_standalone/metrics.cpp
git commit -m "feat: metrics pre-create all 6 op buckets, expose verify counter"
```

---

### Task 5: Rewrite kv_worker.cpp — Use pipeline execution

**Files:**
- Modify: `tests/kvclient_standalone/kv_worker.h`
- Modify: `tests/kvclient_standalone/kv_worker.cpp`

- [ ] **Step 1: Update kv_worker.h**

```cpp
#pragma once

#include "config.h"
#include "metrics.h"
#include "pipeline.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <memory>
#include <vector>
#include <thread>

class KVWorker {
public:
    KVWorker(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
             MetricsCollector &metrics);
    ~KVWorker();

    void Start();
    void Stop();

private:
    void PipelineLoop(int threadId);
    void NotifyPeers(const std::string &key, uint64_t size);

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    std::atomic<bool> running_{false};
    std::vector<std::thread> threads_;
    std::vector<std::pair<std::string, OpFunc>> pipelineOps_;
};
```

- [ ] **Step 2: Rewrite kv_worker.cpp**

```cpp
#include "kv_worker.h"
#include "data_pattern.h"
#include "httplib.h"
#include <datasystem/utils/string_view.h>
#include "simple_log.h"
#include <chrono>
#include <random>
#include <algorithm>

using namespace datasystem;

KVWorker::KVWorker(const Config &cfg, std::shared_ptr<KVClient> client,
                   MetricsCollector &metrics)
    : cfg_(cfg), client_(client), metrics_(metrics) {
    // Build pipeline ops from config
    for (auto &name : cfg_.pipeline) {
        auto fn = GetOpFunc(name);
        if (!fn) {
            SLOG_WARN("Unknown pipeline op: " << name << ", skipping");
            continue;
        }
        pipelineOps_.emplace_back(name, fn);
    }
}

KVWorker::~KVWorker() { Stop(); }

void KVWorker::Start() {
    running_ = true;
    int qpsPerThread = cfg_.targetQps / cfg_.numSetThreads;
    if (qpsPerThread <= 0) qpsPerThread = 1;

    SLOG_INFO("Starting " << cfg_.numSetThreads << " pipeline threads, "
              << qpsPerThread << " QPS each (total target: " << cfg_.targetQps << ")");

    for (int i = 0; i < cfg_.numSetThreads; i++) {
        threads_.emplace_back(&KVWorker::PipelineLoop, this, i);
    }
}

void KVWorker::Stop() {
    running_ = false;
    for (auto &t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
}

void KVWorker::PipelineLoop(int threadId) {
    int qpsPerThread = cfg_.targetQps / cfg_.numSetThreads;
    if (qpsPerThread <= 0) qpsPerThread = 1;
    double intervalMs = 1000.0 / qpsPerThread;

    std::mt19937 rng(threadId + cfg_.instanceId * 1000);
    auto sizeDist = std::uniform_int_distribution<size_t>(0, cfg_.dataSizes.size() - 1);

    SLOG_INFO("Thread " << threadId << " started: " << qpsPerThread << " QPS");

    while (running_) {
        uint64_t size = cfg_.dataSizes[sizeDist(rng)];
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        std::string key = "kv_test_" + std::to_string(cfg_.instanceId)
                        + "_" + std::to_string(threadId)
                        + "_" + std::to_string(now);

        PipelineContext ctx;
        ctx.key = key;
        ctx.size = size;
        ctx.senderId = cfg_.instanceId;
        ctx.data = GeneratePatternData(size, cfg_.instanceId);
        ctx.client = client_;
        ctx.param.writeMode = WriteMode::NONE_L2_CACHE;
        ctx.param.ttlSecond = cfg_.ttlSeconds;

        auto start = std::chrono::steady_clock::now();
        bool allOk = ExecutePipeline(pipelineOps_, ctx, metrics_,
                                     metrics_.VerifyFailCounter());
        auto end = std::chrono::steady_clock::now();
        double elapsedMs = std::chrono::duration<double, std::milli>(end - start).count();

        if (allOk) {
            NotifyPeers(key, size);
        }

        double sleepMs = intervalMs - elapsedMs;
        if (sleepMs > 0) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(static_cast<int64_t>(sleepMs * 1000)));
        }
    }

    SLOG_INFO("Thread " << threadId << " stopped");
}

void KVWorker::NotifyPeers(const std::string &key, uint64_t size) {
    if (cfg_.peers.empty() || cfg_.notifyCount <= 0) return;

    std::vector<std::string> candidates = cfg_.peers;
    std::mt19937 rng(std::random_device{}());
    std::shuffle(candidates.begin(), candidates.end(), rng);

    int count = std::min(cfg_.notifyCount, static_cast<int>(candidates.size()));

    std::string body = "{\"key\":\"" + key + "\",\"sender\":"
                     + std::to_string(cfg_.instanceId) + ",\"size\":"
                     + std::to_string(size) + "}";

    for (int i = 0; i < count; i++) {
        std::string hostPort = candidates[i];
        if (hostPort.substr(0, 7) == "http://") hostPort = hostPort.substr(7);

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

        try {
            httplib::Client cli(host, port);
            cli.set_connection_timeout(2);
            cli.set_read_timeout(2);
            cli.Post("/notify", body, "application/json");
        } catch (...) {}
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add tests/kvclient_standalone/kv_worker.h tests/kvclient_standalone/kv_worker.cpp
git commit -m "feat: rewrite KVWorker to use configurable pipeline"
```

---

### Task 6: Rewrite http_server.cpp — Use notify_pipeline

**Files:**
- Modify: `tests/kvclient_standalone/http_server.h`
- Modify: `tests/kvclient_standalone/http_server.cpp`

- [ ] **Step 1: Update http_server.h**

```cpp
#pragma once

#include "config.h"
#include "metrics.h"
#include "pipeline.h"
#include "httplib.h"
#include <datasystem/kv_client.h>
#include <memory>
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>

class HttpServer {
public:
    HttpServer(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
               MetricsCollector &metrics, std::atomic<bool> &running);
    ~HttpServer();

    void Start();
    void Stop();

private:
    void HandleNotify(const std::string &body);

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    std::atomic<bool> &running_;
    std::unique_ptr<httplib::Server> server_;
    std::thread serverThread_;
    std::vector<std::thread> notifyThreads_;
    std::mutex notifyMutex_;
    std::vector<std::pair<std::string, OpFunc>> notifyOps_;
};
```

- [ ] **Step 2: Rewrite http_server.cpp**

```cpp
#include "http_server.h"
#include "data_pattern.h"
#include "httplib.h"
#include "simple_log.h"
#include <nlohmann/json.hpp>
#include <chrono>
#include <thread>

using json = nlohmann::json;
using namespace datasystem;

HttpServer::HttpServer(const Config &cfg, std::shared_ptr<KVClient> client,
                       MetricsCollector &metrics, std::atomic<bool> &running)
    : cfg_(cfg), client_(client), metrics_(metrics), running_(running),
      server_(std::make_unique<httplib::Server>()) {
    // Build notify pipeline ops from config
    for (auto &name : cfg_.notifyPipeline) {
        auto fn = GetOpFunc(name);
        if (!fn) {
            SLOG_WARN("Unknown notify_pipeline op: " << name << ", skipping");
            continue;
        }
        notifyOps_.emplace_back(name, fn);
    }
}

HttpServer::~HttpServer() { Stop(); }

void HttpServer::Start() {
    server_->Post("/notify", [this](const httplib::Request &req, httplib::Response &res) {
        HandleNotify(req.body);
        res.status = 200;
        res.set_content("ok", "text/plain");
    });

    server_->Get("/stats", [this](const httplib::Request &, httplib::Response &res) {
        res.status = 200;
        res.set_content(metrics_.GetStatsJson(), "application/json");
    });

    server_->Post("/stop", [this](const httplib::Request &, httplib::Response &res) {
        SLOG_INFO("Received /stop request");
        res.status = 200;
        res.set_content("stopping", "text/plain");
        running_ = false;
    });

    serverThread_ = std::thread([this]() {
        SLOG_INFO("HTTP server listening on port " << cfg_.listenPort);
        if (!server_->listen("0.0.0.0", cfg_.listenPort)) {
            SLOG_ERROR("Failed to start HTTP server on port " << cfg_.listenPort);
        }
    });
}

void HttpServer::Stop() {
    if (server_) server_->stop();
    if (serverThread_.joinable()) serverThread_.join();
    {
        std::lock_guard<std::mutex> lock(notifyMutex_);
        for (auto &t : notifyThreads_) {
            if (t.joinable()) t.join();
        }
        notifyThreads_.clear();
    }
}

void HttpServer::HandleNotify(const std::string &body) {
    try {
        json j = json::parse(body);
        std::string key = j["key"];
        int sender = j["sender"];
        uint64_t expectedSize = j["size"];

        {
            std::lock_guard<std::mutex> lock(notifyMutex_);
            constexpr size_t kMaxNotifyThreads = 100;
            if (notifyThreads_.size() >= kMaxNotifyThreads) {
                for (auto &t : notifyThreads_) {
                    if (t.joinable()) t.join();
                }
                notifyThreads_.clear();
            }
            notifyThreads_.emplace_back([this, key, sender, expectedSize]() {
                PipelineContext ctx;
                ctx.key = key;
                ctx.size = expectedSize;
                ctx.senderId = sender;
                ctx.data = GeneratePatternData(expectedSize, sender);
                ctx.client = client_;
                ctx.param.writeMode = WriteMode::NONE_L2_CACHE;
                ctx.param.ttlSecond = cfg_.ttlSeconds;

                ExecutePipeline(notifyOps_, ctx, metrics_,
                                metrics_.VerifyFailCounter());
            });
        }

    } catch (const std::exception &e) {
        SLOG_WARN("Parse notify body failed: " << e.what());
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add tests/kvclient_standalone/http_server.h tests/kvclient_standalone/http_server.cpp
git commit -m "feat: rewrite HttpServer /notify to use notify_pipeline"
```

---

### Task 7: Update main.cpp — Skip KVWorker for reader role

**Files:**
- Modify: `tests/kvclient_standalone/main.cpp`

- [ ] **Step 1: Add role-based logic**

In `RunMode()`, change the worker section:

```cpp
    HttpServer httpServer(cfg, client, metrics, gRunning);
    httpServer.Start();

    std::unique_ptr<KVWorker> worker;
    if (cfg.role == "writer") {
        worker = std::make_unique<KVWorker>(cfg, client, metrics);
        worker->Start();
    } else {
        std::cerr << "Reader mode: waiting for notifications..." << std::endl;
    }

    while (gRunning) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::cerr << "Shutting down..." << std::endl;
    if (worker) worker->Stop();
    httpServer.Stop();
    metrics.Stop();
```

Also add `#include <memory>` if not already present (it is).

Remove the `#include "kv_worker.h"` and replace with forward include since we use unique_ptr. Actually, we need the full include for make_unique. Keep `#include "kv_worker.h"`.

Add `#include "pipeline.h"` to ensure pipeline symbols are available.

- [ ] **Step 2: Commit**

```bash
git add tests/kvclient_standalone/main.cpp
git commit -m "feat: skip KVWorker for reader role"
```

---

### Task 8: Update CMakeLists.txt and config.json.example

**Files:**
- Modify: `tests/kvclient_standalone/CMakeLists.txt`
- Modify: `tests/kvclient_standalone/config.json.example`

- [ ] **Step 1: Add pipeline.cpp to CMakeLists.txt**

Change the `add_executable` line:

```cmake
add_executable(kvclient_standalone_test
    main.cpp config.cpp kv_worker.cpp http_server.cpp metrics.cpp stop.cpp pipeline.cpp)
```

- [ ] **Step 2: Update config.json.example**

```json
{
  "instance_id": 0,
  "listen_port": 9000,
  "etcd_address": "http://127.0.0.1:2379",
  "cluster_name": "default",
  "connect_timeout_ms": 1000,
  "request_timeout_ms": 20,
  "data_sizes": ["8MB", "512KB"],
  "ttl_seconds": 5,
  "target_qps": 1600,
  "num_set_threads": 16,
  "notify_count": 10,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_{instance_id}.csv",
  "role": "writer",
  "pipeline": ["setStringView"],
  "notify_pipeline": ["getBuffer"],
  "peers": [
    "http://host1:9000",
    "http://host2:9000"
  ]
}
```

- [ ] **Step 3: Commit**

```bash
git add tests/kvclient_standalone/CMakeLists.txt tests/kvclient_standalone/config.json.example
git commit -m "feat: add pipeline.cpp to build, update config example"
```

---

### Task 9: Update deploy.sh — Support node-level role/pipeline override

**Files:**
- Modify: `tests/kvclient_standalone/deploy.sh`

- [ ] **Step 1: Add node-level overrides to config generation**

In the `do_deploy()` function, the config generation python script currently does:

```python
d['instance_id'] = $instance_id
d['peers'] = [$peers]
```

Extend it to also inject node-level overrides. The deploy.json nodes may contain `role`, `pipeline`, `notify_pipeline` fields. Parse them and inject:

```python
# In the node config generation, add after peers:
import json
node_overrides_raw = """
$(python3 -c "
import json
with open('$DEPLOY_JSON') as f:
    d = json.load(f)
for n in d.get('nodes', []):
    if n['instance_id'] == $instance_id:
        print(json.dumps({k: v for k, v in n.items() if k in ('role', 'pipeline', 'notify_pipeline')}))
        break
" 2>/dev/null || echo '{}')
"""
try:
    overrides = json.loads(node_overrides_raw.strip())
    d.update(overrides)
except:
    pass
```

Actually, a simpler approach: pass the override fields directly via shell variables. In the deploy loop, before the python config generation, extract overrides:

```bash
# Extract node-level overrides for this instance
NODE_OVERRIDES=$(python3 -c "
import json
with open('$DEPLOY_JSON') as f:
    d = json.load(f)
for n in d.get('nodes', []):
    if n['instance_id'] == $instance_id:
        o = {k: v for k, v in n.items() if k in ('role', 'pipeline', 'notify_pipeline')}
        if o:
            print(json.dumps(o))
        break
" 2>/dev/null || echo "{}")
```

Then in the config generation python:

```python
d['instance_id'] = $instance_id
d['peers'] = [$peers]
import json
try:
    overrides = json.loads('''$NODE_OVERRIDES''')
    d.update(overrides)
except:
    pass
```

- [ ] **Step 2: Commit**

```bash
git add tests/kvclient_standalone/deploy.sh
git commit -m "feat: deploy.sh support node-level role/pipeline override"
```

---

### Task 10: Build, smoke test, fix issues

**Files:**
- All modified files

- [ ] **Step 1: Build**

```bash
cd tests/kvclient_standalone/build
cmake -DDATASYSTEM_SDK_DIR=$PWD/../../../output/datasystem/sdk/cpp .. && make -j$(nproc)
```

Fix any compilation errors. Common issues:
- Missing includes in pipeline.cpp
- Buffer/Optional header paths
- Link errors

- [ ] **Step 2: Run smoke test with setStringView pipeline**

Use the existing config.local.json (which still has setStringView by default). Start etcd + worker, run test for 15s.

```bash
export LD_LIBRARY_PATH=$PWD/../../../output/datasystem/sdk/cpp/lib:$LD_LIBRARY_PATH
./kvclient_standalone_test ../config.local.json &
TEST_PID=$!
sleep 15
kill $TEST_PID 2>/dev/null; wait $TEST_PID 2>/dev/null
cat summary_0.txt
```

Expected: setStringView stats in summary, same results as before.

- [ ] **Step 3: Test reader mode**

Create a reader config:
```json
{
  "instance_id": 1,
  "listen_port": 9001,
  "etcd_address": "http://127.0.0.1:2379",
  "cluster_name": "default",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1KB"],
  "ttl_seconds": 30,
  "role": "reader",
  "pipeline": [],
  "notify_pipeline": ["getBuffer", "exist"],
  "notify_count": 0,
  "target_qps": 10,
  "num_set_threads": 1,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_reader.csv",
  "peers": ["http://127.0.0.1:9000"]
}
```

Start writer (instance 0) and reader (instance 1) simultaneously. Writer notifies reader. Verify reader has getBuffer + exist metrics.

- [ ] **Step 4: Test createBuffer->memoryCopy->setBuffer pipeline**

Create config with pipeline `["createBuffer", "memoryCopy", "setBuffer"]`. Run and verify the 3 ops show in summary.

- [ ] **Step 5: Commit fixes if any**

```bash
git add tests/kvclient_standalone/
git commit -m "fix: resolve build and test issues from pipeline refactoring"
```

---

### Task 11: Multi-node deployment verification (local + openclaw)

This task verifies the complete multi-node deployment flow using two real machines connected via SSH tunnels. The goal is to prove writer → reader notification works across nodes with the new pipeline feature.

**Environment:**

| Machine | SSH Alias | IP | Arch | Role |
|---------|-----------|-----|------|------|
| local | (current) | 10.255.235.183 | x86_64 Ubuntu 24.04 | writer (instance 0) |
| openclaw | `ssh openclaw` | 10.0.0.9 | x86_64 Ubuntu 24.04 | reader (instance 1) |

**Network constraint:** The two machines are on different subnets with NO direct IP connectivity. Only SSH from local → openclaw works. We use SSH tunnels to bridge etcd and HTTP notification ports.

**Files:**
- Create: `tests/kvclient_standalone/deploy.test.json`
- Create: `tests/kvclient_standalone/config.tunnel.json`
- Create: `tests/kvclient_standalone/test_deploy.sh`
- Modify: `tests/kvclient_standalone/deploy.sh` (if issues found)

- [ ] **Step 1: Create the deployment test script**

Create `tests/kvclient_standalone/test_deploy.sh` — a self-contained script that handles the entire multi-node test flow including SSH tunnel setup, worker start, binary deployment, test execution, and cleanup.

```bash
#!/usr/bin/env bash
set -euo pipefail
# Multi-node deployment test: local (writer) + openclaw (reader)
# Uses SSH tunnels for etcd and HTTP notification.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKTREE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SDK_LIB_DIR="$WORKTREE_ROOT/output/datasystem/sdk/cpp/lib"
SERVICE_DIR="$WORKTREE_ROOT/output/datasystem/service"
BINARY="$SCRIPT_DIR/build/kvclient_standalone_test"
REMOTE_HOST="openclaw"
REMOTE_USER="ubuntu"
REMOTE_WORK_DIR="/home/ubuntu/kvclient_test"
LOCAL_ETCD_PORT=2379
LOCAL_HTTP_PORT=9000
REMOTE_HTTP_PORT=9000
# Tunnel ports on openclaw: local etcd → openclaw:22379, local HTTP → openclaw:19000
TUNNEL_ETCD_PORT=22379
TUNNEL_HTTP_LOCAL_PORT=19000  # local sees openclaw:9000 at localhost:19000

cleanup() {
    echo "=== Cleanup ==="
    # Stop local kvclient instance
    pkill -f "kvclient_standalone_test.*config_tunnel_local" 2>/dev/null || true
    # Stop remote kvclient instance
    ssh "$REMOTE_HOST" "pkill -f kvclient_standalone_test" 2>/dev/null || true
    sleep 1
    # Stop local worker
    export LD_LIBRARY_PATH="$SERVICE_DIR/lib:$SDK_LIB_DIR:$LD_LIBRARY_PATH"
    dscli stop -w "127.0.0.1:31501" 2>/dev/null || true
    # Kill SSH tunnels
    pkill -f "ssh.*-fNR.*22379" 2>/dev/null || true
    pkill -f "ssh.*-fNL.*19000" 2>/dev/null || true
    # Clean remote
    ssh "$REMOTE_HOST" "rm -rf $REMOTE_WORK_DIR" 2>/dev/null || true
    echo "Cleanup done"
}
trap cleanup EXIT

echo "=== Multi-Node Deployment Test ==="
echo "Local (writer): instance_id=0, port=$LOCAL_HTTP_PORT"
echo "Remote ($REMOTE_HOST, reader): instance_id=1, port=$REMOTE_HTTP_PORT"

# --- Prerequisite checks ---
if [ ! -f "$BINARY" ]; then
    echo "ERROR: Binary not found at $BINARY. Run 'make' first."
    exit 1
fi
if ! ssh -o ConnectTimeout=5 "$REMOTE_HOST" "echo OK" >/dev/null 2>&1; then
    echo "ERROR: Cannot SSH to $REMOTE_HOST"
    exit 1
fi
if ! etcdctl endpoint health >/dev/null 2>&1; then
    echo "ERROR: etcd not running locally"
    exit 1
fi

# --- Step 1: Set up SSH tunnels ---
echo ""
echo "--- Setting up SSH tunnels ---"
# Tunnel 1: local etcd (2379) → openclaw localhost:22379 (reverse)
# So openclaw can access etcd at localhost:22379
ssh -fNR ${TUNNEL_ETCD_PORT}:localhost:${LOCAL_ETCD_PORT} "$REMOTE_HOST"
echo "  etcd tunnel: openclaw:localhost:${TUNNEL_ETCD_PORT} → local:${LOCAL_ETCD_PORT}"
sleep 1

# Verify tunnel
if ssh "$REMOTE_HOST" "curl -s http://127.0.0.1:${TUNNEL_ETCD_PORT}/health" 2>/dev/null | grep -q "true"; then
    echo "  etcd tunnel OK"
else
    echo "ERROR: etcd tunnel not working"
    exit 1
fi

# Tunnel 2: openclaw:9000 → local:19000 (forward)
# So local can reach openclaw's HTTP server at localhost:19000
ssh -fNL ${TUNNEL_HTTP_LOCAL_PORT}:localhost:${REMOTE_HTTP_PORT} "$REMOTE_HOST"
echo "  HTTP tunnel: local:localhost:${TUNNEL_HTTP_LOCAL_PORT} → openclaw:${REMOTE_HTTP_PORT}"
sleep 1

# --- Step 2: Start local worker ---
echo ""
echo "--- Starting local worker ---"
export LD_LIBRARY_PATH="$SERVICE_DIR/lib:$SDK_LIB_DIR:$LD_LIBRARY_PATH"
dscli start -w --worker_address "127.0.0.1:31501" \
    --etcd_address "127.0.0.1:${LOCAL_ETCD_PORT}" \
    -d "$WORKTREE_ROOT/output/datasystem"
echo "  Worker started"

# --- Step 3: Generate configs ---
echo ""
echo "--- Generating configs ---"

# Writer config (local, instance 0)
cat > /tmp/config_tunnel_local.json << 'LOCAL_EOF'
{
  "instance_id": 0,
  "listen_port": 9000,
  "etcd_address": "http://127.0.0.1:2379",
  "cluster_name": "default",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1KB", "4KB"],
  "ttl_seconds": 30,
  "target_qps": 10,
  "num_set_threads": 1,
  "notify_count": 1,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_local.csv",
  "role": "writer",
  "pipeline": ["setStringView"],
  "notify_pipeline": ["getBuffer", "exist"],
  "peers": ["http://127.0.0.1:19000"]
}
LOCAL_EOF

# Reader config (openclaw, instance 1)
# Uses etcd at localhost:22379 via SSH tunnel
cat > /tmp/config_tunnel_remote.json << REMOTE_EOF
{
  "instance_id": 1,
  "listen_port": ${REMOTE_HTTP_PORT},
  "etcd_address": "http://127.0.0.1:${TUNNEL_ETCD_PORT}",
  "cluster_name": "default",
  "connect_timeout_ms": 5000,
  "request_timeout_ms": 5000,
  "data_sizes": ["1KB", "4KB"],
  "ttl_seconds": 30,
  "target_qps": 10,
  "num_set_threads": 1,
  "notify_count": 0,
  "metrics_interval_ms": 3000,
  "metrics_file": "metrics_remote.csv",
  "role": "reader",
  "pipeline": [],
  "notify_pipeline": ["getBuffer", "exist"],
  "peers": []
}
REMOTE_EOF

echo "  Local config: /tmp/config_tunnel_local.json"
echo "  Remote config: /tmp/config_tunnel_remote.json"

# --- Step 4: Deploy to openclaw ---
echo ""
echo "--- Deploying to $REMOTE_HOST ---"
ssh "$REMOTE_HOST" "mkdir -p $REMOTE_WORK_DIR"
scp "$BINARY" "${REMOTE_HOST}:${REMOTE_WORK_DIR}/kvclient_standalone_test"
scp -r "$SDK_LIB_DIR" "${REMOTE_HOST}:${REMOTE_WORK_DIR}/sdk_lib"
scp /tmp/config_tunnel_remote.json "${REMOTE_HOST}:${REMOTE_WORK_DIR}/config_1.json"
echo "  Binary + SDK libs + config deployed"

# Start remote reader instance
ssh "$REMOTE_HOST" "cd ${REMOTE_WORK_DIR} && chmod +x kvclient_standalone_test && \
    LD_LIBRARY_PATH=${REMOTE_WORK_DIR}/sdk_lib:\$LD_LIBRARY_PATH \
    nohup ./kvclient_standalone_test config_1.json > stdout_1.log 2>&1 &"
sleep 2
echo "  Remote reader started"

# Verify remote is listening
if ssh "$REMOTE_HOST" "curl -s http://127.0.0.1:${REMOTE_HTTP_PORT}/stats" 2>/dev/null | grep -q "instance_id"; then
    echo "  Remote reader is alive"
else
    echo "WARNING: Remote reader may not be responding yet, continuing..."
fi

# --- Step 5: Start local writer instance ---
echo ""
echo "--- Starting local writer ---"
cd "$SCRIPT_DIR"
export LD_LIBRARY_PATH="$SDK_LIB_DIR:$LD_LIBRARY_PATH"
cp /tmp/config_tunnel_local.json config_tunnel_local.json
./build/kvclient_standalone_test config_tunnel_local.json > stdout_local.log 2>&1 &
LOCAL_PID=$!
echo "  Local writer PID: $LOCAL_PID"

# --- Step 6: Wait and collect results ---
echo ""
echo "--- Running test for 20 seconds ---"
sleep 20

# --- Step 7: Collect stats ---
echo ""
echo "--- Results ---"

echo ""
echo "Local writer stats:"
curl -s http://127.0.0.1:${LOCAL_HTTP_PORT}/stats 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "  (no stats)"

echo ""
echo "Remote reader stats (via tunnel):"
curl -s http://127.0.0.1:${TUNNEL_HTTP_LOCAL_PORT}/stats 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "  (no stats)"

echo ""
echo "Remote stdout (last 10 lines):"
ssh "$REMOTE_HOST" "tail -10 ${REMOTE_WORK_DIR}/stdout_1.log" 2>/dev/null

# --- Step 8: Stop ---
echo ""
echo "--- Stopping ---"
kill $LOCAL_PID 2>/dev/null; wait $LOCAL_PID 2>/dev/null
echo "  Local writer stopped"

# Read local summary
echo ""
echo "Local summary:"
cat "$SCRIPT_DIR/summary_0.txt" 2>/dev/null || echo "  (no summary file)"

# Read remote summary
echo ""
echo "Remote summary:"
ssh "$REMOTE_HOST" "cat ${REMOTE_WORK_DIR}/summary_1.txt" 2>/dev/null || echo "  (no summary file)"

# Check remote metrics CSV
echo ""
echo "Remote metrics CSV (last 5 lines):"
ssh "$REMOTE_HOST" "tail -5 ${REMOTE_WORK_DIR}/metrics_remote.csv" 2>/dev/null || echo "  (no metrics file)"

echo ""
echo "=== Test Complete ==="
```

- [ ] **Step 2: Make test script executable and commit**

```bash
chmod +x tests/kvclient_standalone/test_deploy.sh
git add tests/kvclient_standalone/test_deploy.sh
git commit -m "test: add multi-node deployment test script (local + openclaw)"
```

- [ ] **Step 3: Run the multi-node deployment test**

```bash
cd tests/kvclient_standalone
./test_deploy.sh
```

Expected output:
- etcd tunnel OK
- Worker started
- Remote reader alive (stats endpoint responding)
- Local writer: setStringView count > 0, success > 0
- Remote reader: getBuffer count > 0, exist count > 0, verify_fail = 0
- Both summaries show correct operation stats

If the test fails, debug by checking:
1. Remote stdout: `ssh openclaw "cat /home/ubuntu/kvclient_test/stdout_1.log"`
2. Tunnel health: `ssh openclaw "curl http://127.0.0.1:22379/health"`
3. SDK libs on remote: `ssh openclaw "ls /home/ubuntu/kvclient_test/sdk_lib/"`
4. ldd on remote: `ssh openclaw "LD_LIBRARY_PATH=/home/ubuntu/kvclient_test/sdk_lib ldd /home/ubuntu/kvclient_test/kvclient_standalone_test"`

- [ ] **Step 4: Fix any issues found during deployment test**

Common deployment issues:
- Missing .so on remote: check `ldd` output on openclaw, copy missing libs
- KVClient Init fails on remote: etcd tunnel not working, or worker not registered
- /notify not reaching remote: forward tunnel not working, or wrong port
- Data verification fails: TTL expired, or data race between Set and Get

Fix in source code or test_deploy.sh as needed.

- [ ] **Step 5: Re-run until clean pass**

```bash
./test_deploy.sh
```

- [ ] **Step 6: Commit any fixes**

```bash
git add tests/kvclient_standalone/
git commit -m "fix: resolve multi-node deployment issues"
```

---

### Task 12: Final code review

**Files:**
- All modified/new files under `tests/kvclient_standalone/`

- [ ] **Step 1: Review**

Run code-reviewer subagent to check:
- Pipeline op implementations match SDK API signatures exactly
- Thread safety of PipelineContext (local per-invocation, not shared — good)
- Metrics pre-creation covers all 6 ops
- reader mode skips worker properly
- deploy.sh override injection is correct
- test_deploy.sh tunnel setup and cleanup is robust
- No regressions in existing functionality

- [ ] **Step 2: Fix issues and commit**

```bash
git add tests/kvclient_standalone/
git commit -m "fix: address code review issues from pipeline feature"
```
