#pragma once

#include <datasystem/kv_client.h>
#include <datasystem/object/buffer.h>
#include <datasystem/utils/optional.h>
#include <datasystem/utils/string_view.h>
#include "data_pattern.h"
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

// Measure execution time of a function. Returns true if fn succeeded.
inline bool Measure(std::function<datasystem::Status()> fn, double &latencyMs) {
    auto start = std::chrono::steady_clock::now();
    datasystem::Status rc = fn();
    auto end = std::chrono::steady_clock::now();
    latencyMs = std::chrono::duration<double, std::milli>(end - start).count();
    return rc.IsOk();
}

struct PipelineContext {
    std::string key;
    std::string data;
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

// Op function: returns true on success, fills latencyMs.
using OpFunc = std::function<bool(PipelineContext &ctx, double &latencyMs)>;

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
    std::atomic<uint64_t> &verifyFailCount);
