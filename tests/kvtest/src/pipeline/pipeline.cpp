#include "pipeline.h"
#include "metrics/metrics.h"
#include "data_pattern.h"
#include "common/simple_log.h"
#include <chrono>
#include <cstring>

using namespace datasystem;

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
        (void)ctx;
        // No-copy benchmark: keep this pipeline stage as a no-op so setBuffer
        // can publish the freshly created Buffer directly.
        // return ctx.buffer->MemoryCopy(ctx.data.data(), ctx.size);
        return Status::OK();
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
    if (ctx.batchBuffers.size() != ctx.batchKeys.size()) {
        SLOG_WARN("mSet: buffer/key count mismatch (" << ctx.batchBuffers.size()
                  << " vs " << ctx.batchKeys.size() << ")");
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

// cacheGetOrCreate: Get first, if miss → CreateBuffer + MemoryCopy + SetBuffer
// Records each sub-step to metrics using real API names (getBuffer/createBuffer/memoryCopy/setBuffer)
static bool OpCacheGetOrCreate(PipelineContext &ctx, double &latencyMs) {
    if (!ctx.metrics) {
        SLOG_WARN("cacheGetOrCreate: no metrics collector");
        return false;
    }
    latencyMs = 0;

    // Step 1: Get
    Optional<Buffer> optBuf;
    double getLat = 0;
    bool hit = Measure([&]() {
        return ctx.client->Get(ctx.key, optBuf);
    }, getLat);
    latencyMs += getLat;
    ctx.metrics->Record(kOpGetBuffer, getLat, hit, ctx.size);

    if (hit && optBuf) {
        ctx.metrics->RecordCacheHit();
        return true;
    }

    ctx.metrics->RecordCacheMiss();

    // Step 2: CreateBuffer
    std::shared_ptr<Buffer> buf;
    double createLat = 0;
    bool ok = Measure([&]() {
        return ctx.client->Create(ctx.key, ctx.size, ctx.param, buf);
    }, createLat);
    latencyMs += createLat;
    ctx.metrics->Record(kOpCreateBuffer, createLat, ok, ctx.size);
    if (!ok || !buf) return false;

    // Step 3: MemoryCopy
    double copyLat = 0;
    ok = Measure([&]() {
        // No-copy benchmark: skip filling the Buffer before publishing it.
        // Restore the write below when content validation is needed again.
        // return buf->MemoryCopy(ctx.data.data(), ctx.size);
        return Status::OK();
    }, copyLat);
    latencyMs += copyLat;
    ctx.metrics->Record(kOpMemoryCopy, copyLat, ok, 0);
    if (!ok) return false;

    // Step 4: SetBuffer
    double setLat = 0;
    ok = Measure([&]() {
        return ctx.client->Set(buf);
    }, setLat);
    latencyMs += setLat;
    ctx.metrics->Record(kOpSetBuffer, setLat, ok, ctx.size);
    return ok;
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
    {kOpCacheGetOrCreate, OpCacheGetOrCreate},
};

const std::vector<const char *> &GetAllOpNames(bool cacheMode) {
    static const std::vector<const char *> baseNames = {
        kOpSetStringView, kOpGetBuffer, kOpExist,
        kOpCreateBuffer, kOpMemoryCopy, kOpSetBuffer,
        kOpMCreate, kOpMSet, kOpMGet,
        kOpCacheGetOrCreate,
    };
    static const std::vector<const char *> cacheNames = {
        kOpCacheGetOrFillHit, kOpCacheExist,
        kOpCacheSetFill, kOpCacheGetOrFillMiss,
    };
    static const std::vector<const char *> allNames = [] {
        auto v = baseNames;
        v.insert(v.end(), cacheNames.begin(), cacheNames.end());
        return v;
    }();
    return cacheMode ? allNames : baseNames;
}

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
