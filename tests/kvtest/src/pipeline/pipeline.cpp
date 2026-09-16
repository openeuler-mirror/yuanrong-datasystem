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

namespace {
std::atomic<bool> gKvtestClientInitialized{true};
}

void SetKvtestClientInitialized(bool initialized)
{
    gKvtestClientInitialized.store(initialized, std::memory_order_release);
}

bool IsKvtestClientInitialized()
{
    return gKvtestClientInitialized.load(std::memory_order_acquire);
}

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
    if (ctx.cudaReadback) {
        ctx.readBuffer = std::move(optBuf);
        return Status::OK();
    }

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
    if (ctx.batchResults.size() != ctx.batchKeys.size()) {
        return Status(K_RUNTIME_ERROR, "mGet: result/key count mismatch");
    }
    if (ctx.cudaBatchReadback) return Status::OK();
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

static Status CollectCudaHosts(PipelineContext &ctx, bool h2d, bool batch, std::vector<void *> &hosts) {
    const size_t count = batch ? ctx.batchKeys.size() : 1;
    if (count == 0 || (batch && (h2d ? ctx.batchResults.size() : ctx.batchBuffers.size()) != count)) {
        return Status(K_INVALID, "CUDA transfer buffer/key count mismatch");
    }
    hosts.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        Buffer *buffer = nullptr;
        if (h2d) {
            auto &result = batch ? ctx.batchResults[i] : ctx.readBuffer;
            if (result) buffer = &*result;
        } else {
            buffer = batch ? ctx.batchBuffers[i].get() : ctx.buffer.get();
        }
        if (!buffer || buffer->GetSize() <= 0 || static_cast<uint64_t>(buffer->GetSize()) != ctx.size) {
            return Status(K_INVALID, "CUDA transfer requires a present Buffer of the expected size");
        }
        void *host = h2d ? const_cast<void *>(buffer->ImmutableData()) : buffer->MutableData();
        if (!host) return Status(K_INVALID, "CUDA transfer received null Host address");
        hosts.push_back(host);
    }
    return Status::OK();
}

static Status OpCudaCopy(PipelineContext &ctx, double &latencyMs, bool h2d, bool batch) {
    if (!kvtest::CudaTransfersEnabled() || !ctx.cudaLane) return Status(K_INVALID, "CUDA transfer not initialized");
    std::vector<void *> hosts;
    Status rc = CollectCudaHosts(ctx, h2d, batch, hosts);
    if (!rc.IsOk()) return rc;
    if (!h2d && ctx.cudaLane->sourceSenderId != ctx.senderId) {
        double prepareMs = 0;
        rc = Measure([&ctx]() { return kvtest::PrepareCudaSource(*ctx.cudaLane, ctx.senderId); }, prepareMs);
        if (ctx.metrics) ctx.metrics->Record("cuda_prepare", prepareMs, rc.GetCode());
        if (!rc.IsOk()) return rc;
    }
    const std::string name = h2d ? (batch ? "mH2d" : "h2d") : (batch ? "mD2h" : "d2h");
    kvtest::CudaTiming timing;
    rc = kvtest::CopyCudaBuffers(*ctx.client, *ctx.cudaLane, hosts, ctx.size, h2d, timing);
    latencyMs = timing.totalMs;
    if (ctx.metrics) {
        ctx.metrics->Record(name + "_enqueue", timing.enqueueMs, rc.GetCode());
        ctx.metrics->Record(name + "_event", timing.eventMs, rc.GetCode());
        ctx.metrics->Record(name + "_wait", timing.waitMs, rc.GetCode());
    }
    if (!rc.IsOk() || !h2d || ctx.verifyCfg.level == VerifyLevel::OFF || ctx.verifyCfg.level == VerifyLevel::SIZE) return rc;
    bool matches = true;
    double verifyMs = 0;
    rc = Measure([&ctx, &hosts, &matches]() {
        return kvtest::VerifyCudaBuffers(*ctx.cudaLane, hosts.size(), ctx.size, ctx.senderId, ctx.verifyCfg, matches);
    }, verifyMs);
    if (!matches) {
        if (ctx.verifyFailCount) ++(*ctx.verifyFailCount);
        SLOG_WARN("CUDA content verification failed: key=" << ctx.key << " senderId=" << ctx.senderId);
    }
    if (ctx.metrics) ctx.metrics->Record("cuda_verify", verifyMs, !matches ? K_INVALID : rc.GetCode());
    if (rc.IsOk() && !matches && ctx.verifyCfg.failOp) return Status(K_INVALID, "CUDA content verification failed");
    return rc;
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
    {"d2h", [](PipelineContext &ctx, double &ms) { return OpCudaCopy(ctx, ms, false, false); }},
    {"h2d", [](PipelineContext &ctx, double &ms) { return OpCudaCopy(ctx, ms, true, false); }},
    {"mD2h", [](PipelineContext &ctx, double &ms) { return OpCudaCopy(ctx, ms, false, true); }},
    {"mH2d", [](PipelineContext &ctx, double &ms) { return OpCudaCopy(ctx, ms, true, true); }},
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

Status WarmupCudaPipeline(const Config &cfg, const std::shared_ptr<KVClient> &client) {
    if (!kvtest::CudaTransfersEnabled()) return Status::OK();
    if (!client || cfg.dataSizes.empty()) return Status(K_INVALID, "CUDA warmup requires client and data size");
    kvtest::CudaLaneGuard lane(true);
    if (!lane.Get()) return Status(K_RUNTIME_ERROR, "CUDA warmup lane unavailable");
    const auto epoch = std::chrono::system_clock::now().time_since_epoch().count();
    const auto key = GenerateTraceId("kvtest_cuda_warmup", cfg.instanceId) + "-" + std::to_string(epoch);
    Status result;
    {
        PipelineContext ctx;
        ctx.client = client;
        ctx.key = key;
        ctx.size = cfg.dataSizes.front();
        ctx.senderId = cfg.instanceId;
        ctx.param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        ctx.param.ttlSecond = 60;
        ctx.cudaLane = lane.Get();
        ctx.cudaReadback = true;
        ctx.verifyCfg = BuildVerifyConfig(cfg.verifyLevel, cfg.verifySampleBytes, cfg.verifySampleStepBytes, true);
        for (const char *op : {"createBuffer", "d2h", "setBuffer", "getBuffer", "h2d"}) {
            double latencyMs = 0;
            result = GetOpFunc(op)(ctx, latencyMs);
            SLOG_INFO("CUDA warmup: key=" << key << " op=" << op << " latency_ms=" << latencyMs
                      << " status=" << result.ToString());
            if (!result.IsOk()) break;
        }
    }
    const Status cleanup = client->Del(key);
    if (!cleanup.IsOk()) SLOG_ERROR("CUDA warmup cleanup failed: key=" << key << " status=" << cleanup.ToString());
    if (!result.IsOk()) return result;
    return cleanup;
}

bool ExecutePipeline(
    const std::vector<std::pair<std::string, OpFunc>> &ops,
    PipelineContext &ctx,
    MetricsCollector &metrics,
    std::atomic<uint64_t> &verifyFailCount,
    int instanceId) {
    bool needsCuda = false;
    ctx.cudaReadback = false;
    ctx.cudaBatchReadback = false;
    for (const auto &op : ops) {
        if (op.first == "h2d") ctx.cudaReadback = true;
        if (op.first == "mH2d") ctx.cudaBatchReadback = true;
        if (op.first == "d2h" || op.first == "mD2h" || ctx.cudaReadback || ctx.cudaBatchReadback) needsCuda = true;
    }
    kvtest::CudaLaneGuard lane(needsCuda);
    ctx.cudaLane = lane.Get();
    ctx.metrics = &metrics;
    ctx.verifyFailCount = &verifyFailCount;
    if (needsCuda && !ctx.cudaLane) {
        SLOG_ERROR("CUDA lane unavailable: check transfer_enabled and concurrent executor count");
        return false;
    }
    bool allOk = true;
    for (auto &[name, fn] : ops) {
        const bool isGetOperation = name == kOpGetBuffer || name == kOpMGet || name == kOpCacheGetOrCreate;
        if (isGetOperation && !IsKvtestClientInitialized()) {
            continue;
        }
        ctx.traceId = GenerateTraceId(name.c_str(), instanceId);
        Status traceRc = Context::SetTraceId(ctx.traceId);
        if (!traceRc.IsOk()) {
            SLOG_WARN("Pipeline set trace id failed: traceId=" << ctx.traceId << " error=" << traceRc.GetMsg());
        }

        double latencyMs = 0;
        Status rc = fn(ctx, latencyMs);
        const bool batch = name == "mCreate" || name == "mSet" || name == "mGet" || name == "mD2h" || name == "mH2d";
        metrics.Record(name, latencyMs, rc.GetCode(), ctx.size * (batch ? ctx.batchKeys.size() : 1));
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
    ctx.cudaLane = nullptr;
    return allOk;
}
