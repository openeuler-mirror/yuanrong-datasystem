#include "pipeline.h"
#include "metrics.h"
#include "data_pattern.h"
#include "simple_log.h"
#include <chrono>
#include <cstring>

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

    // Verify size only (skip content check to save CPU)
    int64_t bufSize = optBuf->GetSize();
    if (static_cast<uint64_t>(bufSize) != ctx.size) {
        SLOG_WARN("getBuffer size mismatch: key=" << ctx.key
                  << " expected=" << ctx.size << " got=" << bufSize);
        if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
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
        if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
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

// mCreate: client->MCreate(keys, sizes, param, buffers)
static bool OpMCreate(PipelineContext &ctx, double &latencyMs) {
    std::vector<uint64_t> sizes(ctx.batchKeys.size(), ctx.size);
    return Measure([&]() {
        return ctx.client->MCreate(ctx.batchKeys, sizes, ctx.param, ctx.batchBuffers);
    }, latencyMs);
}

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
        } else {
            SLOG_WARN("mGet missing result: key=" << ctx.batchKeys[i]);
            if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
        }
    }
    return true;
}

// ---- Registry ----

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

static const std::vector<const char *> kAllOpNames = {
    kOpSetStringView, kOpGetBuffer, kOpExist,
    kOpCreateBuffer, kOpMemoryCopy, kOpSetBuffer,
    kOpMCreate, kOpMSet, kOpMGet,
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
        metrics.Record(name, latencyMs, ok, ctx.size);
        if (!ok) {
            SLOG_WARN("Pipeline op failed: " << name
                      << " key=" << ctx.key
                      << " latency=" << latencyMs << "ms");
            allOk = false;
            break;
        }
    }
    return allOk;
}
