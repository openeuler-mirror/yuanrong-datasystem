#pragma once

#include <datasystem/kv_client.h>
#include <datasystem/object/buffer.h>
#include <datasystem/utils/optional.h>
#include <datasystem/utils/string_view.h>
#include "common/config.h"
#include "data_pattern.h"
#include "cuda_workload.h"
#include <chrono>
#include <cmath>
#include <atomic>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <vector>

void SetKvtestClientInitialized(bool initialized);
bool IsKvtestClientInitialized();

// Measure execution time of a function. Returns the Status produced by fn.
inline datasystem::Status Measure(std::function<datasystem::Status()> fn, double &latencyMs) {
    auto start = std::chrono::steady_clock::now();
    datasystem::Status rc = fn();
    auto end = std::chrono::steady_clock::now();
    latencyMs = std::chrono::duration<double, std::milli>(end - start).count();
    return rc;
}

struct PipelineContext {
    std::string key;
    std::string data;
    std::string traceId;
    uint64_t size = 0;
    int senderId = 0;
    datasystem::SetParam param;
    std::shared_ptr<datasystem::KVClient> client;
    std::shared_ptr<datasystem::Buffer> buffer;
    datasystem::Optional<datasystem::Buffer> readBuffer;
    kvtest::CudaLane *cudaLane = nullptr;
    bool cudaReadback = false;
    bool cudaBatchReadback = false;
    std::vector<std::string> batchKeys;
    std::vector<std::shared_ptr<datasystem::Buffer>> batchBuffers;
    std::vector<datasystem::Optional<datasystem::Buffer>> batchResults;
    std::atomic<uint64_t> *verifyFailCount = nullptr;
    VerifyConfig verifyCfg;
    class MetricsCollector *metrics = nullptr;
    // Cache-update-on-hit (writer side): probability to re-Set a timestamped
    // value on hit. 0 = disabled.
    double cacheUpdateProb = 0.0;
    // Set true by the op when it performed a cache update (re-Set on hit);
    // updateVersionValue carries the timestamp that was written.
    bool updated = false;
    uint64_t updateVersionValue = 0;
    // Set true by the op on a cache hit; readVersionValue is the version stamp
    // read back from the value (the writer's pipeline loop does the monotonic
    // stale check against its own per-key max-seen map).
    bool hit = false;
    uint64_t readVersionValue = 0;
    // Miss backfill policy (writer side): probability to backfill a miss.
    // 1.0 = always (legacy), 0.0 = never (leave key absent), 0.5 = half.
    double missBackfillProb = 1.0;
    // Batch cache-get state (mget mode): per-key value sizes, and the results
    // gathered across the batch so the caller (PipelineLoop) can run the
    // per-key stale check and update broadcast after the op returns.
    std::vector<uint64_t> batchSizes;
    std::vector<std::string> batchHitKeys;
    std::vector<uint64_t> batchHitVersions;
    std::vector<std::string> batchUpdatedKeys;
    std::vector<uint64_t> batchUpdatedVersions;
};

// Build a traceId that is unique across worker instances running on the same
// host. The instanceId prefix disambiguates workers that happen to share a
// pid (e.g. after a restart) while the trailing index keeps it sortable.
std::string GenerateTraceId(const char *prefix, int instanceId);

// Op function: returns Status::OK() on success and fills latencyMs.
using OpFunc = std::function<datasystem::Status(PipelineContext &ctx, double &latencyMs)>;

// Op name constants
inline constexpr const char *kOpSetStringView = "setStringView";
inline constexpr const char *kOpGetBuffer = "getBuffer";
inline constexpr const char *kOpExist = "exist";
inline constexpr const char *kOpCreateBuffer = "createBuffer";
inline constexpr const char *kOpMemoryCopy = "memoryCopy";
inline constexpr const char *kOpSetBuffer = "setBuffer";
inline constexpr const char *kOpMCreate = "mCreate";
inline constexpr const char *kOpMSet = "mSet";
inline constexpr const char *kOpMGet = "mGet";
inline constexpr const char *kOpCacheGetOrCreate = "cacheGetOrCreate";

// Sample the next mGet target batch size per the configured probability
// distribution. Returns 1 with probability singleProb; otherwise a uniform
// draw from [minBatch, maxBatch]. Used both by cache-mode Get (writer
// PipelineLoop / reader ReaderLoop, which sample a batch of keys directly) and
// by the pipeline reader-side notify-driven mGet (C2 non-blocking variant)
// where the actual batch is min(target, queue depth). Distribution-disabled
// configs (singleProb >= 1.0) always return 1. The caller-owned rng avoids
// shared-state contention on the hot path.
inline int SampleMgetBatchSize(const Config::MgetSizeDistribution &d, std::mt19937 &rng) {
    if (!d.enabled) return 1;
    std::uniform_real_distribution<double> pd(0.0, 1.0);
    if (pd(rng) < d.singleProb) return 1;
    std::uniform_int_distribution<int> bd(d.minBatch, d.maxBatch);
    return bd(rng);
}

// Sample a key-pool index honoring hot_key_ratio / hot_access_prob. The hot
// set is the first ceil(poolSize * hotKeyRatio) keys; with probability
// hotAccessProb draw uniformly from the hot set, otherwise from the cold
// remainder. When either ratio or prob is 0, falls back to uniform over the
// whole pool. Used by both the writer (its own currentPoolSize) and the
// reader (its own keyPool_); the two hot sets are therefore not guaranteed to
// overlap exactly.
inline uint64_t SamplePoolIndex(std::mt19937 &rng, uint64_t poolSize,
                                double hotKeyRatio, double hotAccessProb) {
    if (poolSize == 0) return 0;
    if (hotKeyRatio <= 0.0 || hotAccessProb <= 0.0) {
        return std::uniform_int_distribution<uint64_t>(0, poolSize - 1)(rng);
    }
    uint64_t hotBound = static_cast<uint64_t>(std::ceil(static_cast<double>(poolSize) * hotKeyRatio));
    if (hotBound < 1) hotBound = 1;
    if (hotBound > poolSize) hotBound = poolSize;
    std::uniform_real_distribution<double> pd(0.0, 1.0);
    if (pd(rng) < hotAccessProb || hotBound >= poolSize) {
        return std::uniform_int_distribution<uint64_t>(0, hotBound - 1)(rng);
    }
    return std::uniform_int_distribution<uint64_t>(hotBound, poolSize - 1)(rng);
}

// Cache mode per-key size: maps a pool index to a fixed data size via modulo
// so a key's value size is deterministic and never changes across warmup /
// steady state / backfill / update. With one entry this degenerates to the
// legacy single-size behavior.
inline uint64_t CacheKeySize(uint64_t idx, const std::vector<uint64_t> &dataSizes)
{
    return dataSizes.empty() ? 0 : dataSizes[static_cast<size_t>(idx % dataSizes.size())];
}

// Parse the pool index out of a "cache_pool_<writerId>_<idx>" key name.
// Returns 0 for keys that do not match the expected shape.
inline uint64_t ParseCacheKeyIdx(const std::string &key)
{
    auto pos = key.find_last_of('_');
    if (pos == std::string::npos || pos + 1 >= key.size()) return 0;
    uint64_t idx = 0;
    for (size_t i = pos + 1; i < key.size(); i++) {
        char c = key[i];
        if (c < '0' || c > '9') return 0;
        idx = idx * 10 + static_cast<uint64_t>(c - '0');
    }
    return idx;
}

// Cache mode sub-step names (not in kOpRegistry, for metrics pre-allocation only)
inline constexpr const char *kOpCacheGetOrFillHit  = "cacheGetOrFill_hit";
inline constexpr const char *kOpCacheExist          = "cacheExist";
inline constexpr const char *kOpCacheSetFill        = "cacheSetFill";
inline constexpr const char *kOpCacheGetOrFillMiss  = "cacheGetOrFill_miss";
inline constexpr const char *kOpCacheSetUpdate      = "cacheSetUpdate";

// All known op names (for metrics pre-creation).
const std::vector<const char *> &GetAllOpNames(bool cacheMode = false);

// Look up op function by name. Returns nullptr for unknown ops.
OpFunc GetOpFunc(const std::string &name);

datasystem::Status WarmupCudaPipeline(const Config &cfg, const std::shared_ptr<datasystem::KVClient> &client);

// Execute a pipeline: run each op in order, record metrics via its name.
// Returns true if all ops succeeded.
bool ExecutePipeline(
    const std::vector<std::pair<std::string, OpFunc>> &ops,
    PipelineContext &ctx,
    class MetricsCollector &metrics,
    std::atomic<uint64_t> &verifyFailCount,
    int instanceId);
