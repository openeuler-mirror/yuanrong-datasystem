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
        if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
        return true; // op succeeded, but verify failed
    }

    // Verify content
    std::string expected = GeneratePatternData(ctx.size, ctx.senderId);
    const char *bufData = static_cast<const char *>(optBuf->ImmutableData());
    std::string actual(bufData, bufSize);
    if (actual != expected) {
        SLOG_WARN("getBuffer content mismatch: key=" << ctx.key);
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
            SLOG_WARN("Pipeline op failed: " << name
                      << " key=" << ctx.key
                      << " latency=" << latencyMs << "ms");
            allOk = false;
            break;
        }
    }
    return allOk;
}
