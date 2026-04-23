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
    std::atomic<uint64_t> *verifyFailCount = nullptr;
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
