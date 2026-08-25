#include "pipeline.h"
#include "metrics/metrics.h"
#include "data_pattern.h"
#include "common/simple_log.h"
#include <datasystem/context/context.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <optional>
#include <sstream>
#include <unistd.h>

using namespace datasystem;

std::string GenerateTraceId(const char *prefix, int instanceId) {
    constexpr int kTraceIdIndexWidth = 8;
    static const auto processId = getpid();
    static std::atomic<uint64_t> index{0};

    const uint64_t current = index.fetch_add(1, std::memory_order_relaxed) + 1;
    std::ostringstream traceId;
    traceId << prefix << '-' << instanceId << '-' << processId << '-'
            << std::setfill('0') << std::setw(kTraceIdIndexWidth) << current;
    return traceId.str();
}

// setStringView: client->Set(key, StringView(data), param)
static Status OpSetStringView(PipelineContext &ctx, double &latencyMs) {
    return Measure([&]() {
        return ctx.client->Set(ctx.key, StringView(ctx.data), ctx.param);
    }, latencyMs);
}

// getBuffer: client->Get(key, Optional<Buffer>&)
// On success, verify data per ctx.verifyCfg (off/size/sample/full).
static Status OpGetBuffer(PipelineContext &ctx, double &latencyMs) {
    Optional<Buffer> optBuf;
    Status rc = Measure([&]() {
        return ctx.client->Get(ctx.key, optBuf);
    }, latencyMs);
    if (!rc.IsOk()) return rc;
    if (!optBuf) return Status(K_RUNTIME_ERROR, "getBuffer: Get returned OK but buffer is empty");

    VerifyFailReason reason = VerifyFailReason::NONE;
    std::optional<uint64_t> mismatchPos;
    bool vok = VerifyBuffer(optBuf->ImmutableData(),
                            static_cast<uint64_t>(optBuf->GetSize()),
                            ctx.size, ctx.senderId, ctx.verifyCfg,
                            &reason, &mismatchPos);
    if (!vok) {
        if (reason == VerifyFailReason::SIZE) {
            SLOG_WARN("getBuffer size mismatch: key=" << ctx.key
                      << " expected=" << ctx.size
                      << " got=" << optBuf->GetSize()
                      << " traceId=" << ctx.traceId);
        } else if (mismatchPos) {
            SLOG_WARN("getBuffer content mismatch: key=" << ctx.key
                      << " level=" << static_cast<int>(ctx.verifyCfg.level)
                      << " senderId=" << ctx.senderId
                      << " traceId=" << ctx.traceId
                      << " mismatchPos=" << *mismatchPos);
        } else {
            SLOG_WARN("getBuffer content mismatch: key=" << ctx.key
                      << " level=" << static_cast<int>(ctx.verifyCfg.level)
                      << " senderId=" << ctx.senderId
                      << " traceId=" << ctx.traceId
                      << " mismatchPos=unknown");
        }
        if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
        if (ctx.verifyCfg.failOp) return Status(K_INVALID, "getBuffer: verify failed");
    }
    return Status::OK();
}

// exist: client->Exist({key}, exists)
static Status OpExist(PipelineContext &ctx, double &latencyMs) {
    std::vector<bool> exists;
    Status rc = Measure([&]() {
        return ctx.client->Exist({ctx.key}, exists);
    }, latencyMs);
    if (!rc.IsOk()) return rc;
    // Verify key exists
    if (exists.empty() || !exists[0]) {
        SLOG_WARN("exist: key not found: " << ctx.key);
        if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
    }
    return Status::OK();
}

// createBuffer: client->Create(key, size, param, buffer)
static Status OpCreateBuffer(PipelineContext &ctx, double &latencyMs) {
    std::shared_ptr<Buffer> buf;
    Status rc = Measure([&]() {
        return ctx.client->Create(ctx.key, ctx.size, ctx.param, buf);
    }, latencyMs);
    if (rc.IsOk() && buf) {
        ctx.buffer = buf;
    }
    return rc;
}

// memoryCopy: buffer->MemoryCopy(data, size)
static Status OpMemoryCopy(PipelineContext &ctx, double &latencyMs) {
    if (!ctx.buffer) {
        SLOG_WARN("memoryCopy: no buffer (createBuffer not called?)");
        latencyMs = 0;
        return Status(K_INVALID, "memoryCopy: no buffer");
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
static Status OpSetBuffer(PipelineContext &ctx, double &latencyMs) {
    if (!ctx.buffer) {
        SLOG_WARN("setBuffer: no buffer (createBuffer not called?)");
        latencyMs = 0;
        return Status(K_INVALID, "setBuffer: no buffer");
    }
    return Measure([&]() {
        return ctx.client->Set(ctx.buffer);
    }, latencyMs);
}

// mCreate: client->MCreate(keys, sizes, param, buffers)
static Status OpMCreate(PipelineContext &ctx, double &latencyMs) {
    std::vector<uint64_t> sizes(ctx.batchKeys.size(), ctx.size);
    return Measure([&]() {
        return ctx.client->MCreate(ctx.batchKeys, sizes, ctx.param, ctx.batchBuffers);
    }, latencyMs);
}

// mSet: client->MSet(buffers)
static Status OpMSet(PipelineContext &ctx, double &latencyMs) {
    if (ctx.batchBuffers.empty()) {
        SLOG_WARN("mSet: no buffers (mCreate not called?)");
        latencyMs = 0;
        return Status(K_INVALID, "mSet: no buffers");
    }
    if (ctx.batchBuffers.size() != ctx.batchKeys.size()) {
        SLOG_WARN("mSet: buffer/key count mismatch (" << ctx.batchBuffers.size()
                  << " vs " << ctx.batchKeys.size() << ")");
        latencyMs = 0;
        return Status(K_INVALID, "mSet: buffer/key count mismatch");
    }
    return Measure([&]() {
        return ctx.client->MSet(ctx.batchBuffers);
    }, latencyMs);
}

// mGet: client->Get(keys, buffers)
static Status OpMGet(PipelineContext &ctx, double &latencyMs) {
    Status rc = Measure([&]() {
        return ctx.client->Get(ctx.batchKeys, ctx.batchResults);
    }, latencyMs);
    if (!rc.IsOk()) return rc;
    bool anyFail = false;
    for (size_t i = 0; i < ctx.batchResults.size(); i++) {
        if (!ctx.batchResults[i]) {
            SLOG_WARN("mGet missing result: key=" << ctx.batchKeys[i]);
            if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
            if (ctx.verifyCfg.failOp) anyFail = true;
            continue;
        }
        VerifyFailReason reason = VerifyFailReason::NONE;
        bool vok = VerifyBuffer(ctx.batchResults[i]->ImmutableData(),
                                static_cast<uint64_t>(ctx.batchResults[i]->GetSize()),
                                ctx.size, ctx.senderId, ctx.verifyCfg, &reason);
        if (!vok) {
            if (reason == VerifyFailReason::SIZE) {
                SLOG_WARN("mGet size mismatch: key=" << ctx.batchKeys[i]
                          << " expected=" << ctx.size
                          << " got=" << ctx.batchResults[i]->GetSize());
            } else {
                SLOG_WARN("mGet content mismatch: key=" << ctx.batchKeys[i]
                          << " level=" << static_cast<int>(ctx.verifyCfg.level)
                          << " senderId=" << ctx.senderId);
            }
            if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
            if (ctx.verifyCfg.failOp) anyFail = true;
        }
    }
    return anyFail ? Status(K_INVALID, "mGet: verify failed") : Status::OK();
}

// cacheGetOrCreate: Get first, if miss → CreateBuffer + MemoryCopy + SetBuffer
// Records each sub-step to metrics using real API names (getBuffer/createBuffer/memoryCopy/setBuffer)
static Status OpCacheGetOrCreate(PipelineContext &ctx, double &latencyMs) {
    if (!ctx.metrics) {
        SLOG_WARN("cacheGetOrCreate: no metrics collector");
        return Status(K_INVALID, "cacheGetOrCreate: no metrics collector");
    }
    latencyMs = 0;

    // Step 1: Get
    Optional<Buffer> optBuf;
    double getLat = 0;
    Status getRc = Measure([&]() {
        return ctx.client->Get(ctx.key, optBuf);
    }, getLat);
    latencyMs += getLat;
    ctx.metrics->Record(kOpGetBuffer, getLat, getRc.GetCode(), ctx.size);

    if (getRc.IsOk() && optBuf) {
        // Verify the cached payload. Previously the hit path did no check at
        // all. A corrupted hit still counts as a cache hit (the key was
        // present) but, with failOp=true, fails the op for success-rate stats.
        VerifyFailReason reason = VerifyFailReason::NONE;
        bool vok = VerifyBuffer(optBuf->ImmutableData(),
                                static_cast<uint64_t>(optBuf->GetSize()),
                                ctx.size, ctx.senderId, ctx.verifyCfg, &reason);
        if (!vok) {
            if (reason == VerifyFailReason::SIZE) {
                SLOG_WARN("cacheGetOrCreate size mismatch on hit: key=" << ctx.key
                          << " expected=" << ctx.size
                          << " got=" << optBuf->GetSize());
            } else {
                SLOG_WARN("cacheGetOrCreate content mismatch on hit: key=" << ctx.key
                          << " level=" << static_cast<int>(ctx.verifyCfg.level)
                          << " senderId=" << ctx.senderId);
            }
            if (ctx.verifyFailCount) (*ctx.verifyFailCount)++;
        }
        ctx.metrics->RecordCacheHit();
        if (!vok && ctx.verifyCfg.failOp) return Status(K_INVALID, "cacheGetOrCreate: verify failed on hit");
        return Status::OK();
    }

    ctx.metrics->RecordCacheMiss();

    // Step 2: CreateBuffer
    std::shared_ptr<Buffer> buf;
    double createLat = 0;
    Status createRc = Measure([&]() {
        return ctx.client->Create(ctx.key, ctx.size, ctx.param, buf);
    }, createLat);
    latencyMs += createLat;
    ctx.metrics->Record(kOpCreateBuffer, createLat, createRc.GetCode(), ctx.size);
    if (!createRc.IsOk() || !buf) return createRc.IsOk() ? Status(K_RUNTIME_ERROR, "cacheGetOrCreate: Create returned OK but no buffer") : createRc;

    // Step 3: MemoryCopy
    double copyLat = 0;
    Status copyRc = Measure([&]() {
        // No-copy benchmark: skip filling the Buffer before publishing it.
        // Restore the write below when content validation is needed again.
        // return buf->MemoryCopy(ctx.data.data(), ctx.size);
        return Status::OK();
    }, copyLat);
    latencyMs += copyLat;
    ctx.metrics->Record(kOpMemoryCopy, copyLat, copyRc.GetCode(), 0);
    if (!copyRc.IsOk()) return copyRc;

    // Step 4: SetBuffer
    double setLat = 0;
    Status setRc = Measure([&]() {
        return ctx.client->Set(buf);
    }, setLat);
    latencyMs += setLat;
    ctx.metrics->Record(kOpSetBuffer, setLat, setRc.GetCode(), ctx.size);
    return setRc;
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
    std::atomic<uint64_t> &verifyFailCount,
    int instanceId) {
    bool allOk = true;
    for (auto &[name, fn] : ops) {
        ctx.traceId = GenerateTraceId(name.c_str(), instanceId);
        Status traceRc = Context::SetTraceId(ctx.traceId);
        if (!traceRc.IsOk()) {
            SLOG_WARN("Pipeline set trace id failed: traceId=" << ctx.traceId << " error=" << traceRc.GetMsg());
        }

        double latencyMs = 0;
        Status rc = fn(ctx, latencyMs);
        metrics.Record(name, latencyMs, rc.GetCode(), ctx.size);
        if (!rc.IsOk()) {
            SLOG_WARN("Pipeline op failed: " << name
                      << " key=" << ctx.key
                      << " traceId=" << ctx.traceId
                      << " rc=" << rc.GetCode()
                      << " msg=" << rc.GetMsg()
                      << " latency=" << latencyMs << "ms");
            allOk = false;
            break;
        }
    }
    (void)verifyFailCount;
    return allOk;
}
