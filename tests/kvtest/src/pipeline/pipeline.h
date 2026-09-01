#pragma once

#include <datasystem/kv_client.h>
#include <datasystem/object/buffer.h>
#include <datasystem/utils/optional.h>
#include <datasystem/utils/string_view.h>
#include "data_pattern.h"
#include <chrono>
#include <atomic>
#include <functional>
#include <memory>
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
    std::vector<std::string> batchKeys;
    std::vector<std::shared_ptr<datasystem::Buffer>> batchBuffers;
    std::vector<datasystem::Optional<datasystem::Buffer>> batchResults;
    std::atomic<uint64_t> *verifyFailCount = nullptr;
    VerifyConfig verifyCfg;
    class MetricsCollector *metrics = nullptr;
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

// Cache mode sub-step names (not in kOpRegistry, for metrics pre-allocation only)
inline constexpr const char *kOpCacheGetOrFillHit  = "cacheGetOrFill_hit";
inline constexpr const char *kOpCacheExist          = "cacheExist";
inline constexpr const char *kOpCacheSetFill        = "cacheSetFill";
inline constexpr const char *kOpCacheGetOrFillMiss  = "cacheGetOrFill_miss";

// All known op names (for metrics pre-creation).
const std::vector<const char *> &GetAllOpNames(bool cacheMode = false);

// Look up op function by name. Returns nullptr for unknown ops.
OpFunc GetOpFunc(const std::string &name);

// Execute a pipeline: run each op in order, record metrics via its name.
// Returns true if all ops succeeded.
bool ExecutePipeline(
    const std::vector<std::pair<std::string, OpFunc>> &ops,
    PipelineContext &ctx,
    class MetricsCollector &metrics,
    std::atomic<uint64_t> &verifyFailCount,
    int instanceId);
