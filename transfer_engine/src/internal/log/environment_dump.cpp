#include "internal/log/environment_dump.h"

#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <array>
#include <atomic>
#include <mutex>
#include <string>

#include "internal/log/logging.h"

namespace datasystem {
namespace internal {
namespace {

bool IsEnvDumpEnabled()
{
    static const bool enabled = []() {
        const char *value = std::getenv("YR_TE_ENABLE_ENV_DUMP");
        if (value == nullptr) {
            return false;
        }
        const std::string flag(value);
        return flag == "1" || flag == "true" || flag == "TRUE" || flag == "on" || flag == "ON" || flag == "yes"
               || flag == "YES";
    }();
    return enabled;
}

constexpr size_t K_MAX_DUMPED_STAGES = 32;
constexpr size_t K_MAX_STAGE_LENGTH = 64;

using StageName = std::array<char, K_MAX_STAGE_LENGTH>;

// The table copies each stage name so it does not constrain the caller's string lifetime.
// Entries below the released count are fully written and never mutated again, so repeat
// callers scan them without the lock; only the first claim of a stage takes the mutex.
// DumpProcessEnvironment is reached from the batch-read path, hence the lock-free scan.
bool ClaimStage(const char *stage)
{
    static std::atomic<size_t> dumpedStageCount{ 0 };
    static std::array<StageName, K_MAX_DUMPED_STAGES> dumpedStages{};
    static std::mutex claimMutex;

    const size_t count = dumpedStageCount.load(std::memory_order_acquire);
    for (size_t i = 0; i < count; ++i) {
        if (std::strncmp(dumpedStages[i].data(), stage, K_MAX_STAGE_LENGTH - 1) == 0) {
            return false;
        }
    }

    std::lock_guard<std::mutex> lock(claimMutex);
    const size_t claimedCount = dumpedStageCount.load(std::memory_order_relaxed);
    for (size_t i = count; i < claimedCount; ++i) {
        if (std::strncmp(dumpedStages[i].data(), stage, K_MAX_STAGE_LENGTH - 1) == 0) {
            return false;
        }
    }
    if (claimedCount >= K_MAX_DUMPED_STAGES) {
        return false;
    }
    StageName &slot = dumpedStages[claimedCount];
    const size_t length = std::min(std::strlen(stage), K_MAX_STAGE_LENGTH - 1);
    std::memcpy(slot.data(), stage, length);
    slot[length] = '\0';
    dumpedStageCount.store(claimedCount + 1, std::memory_order_release);
    return true;
}

void DumpOncePerStage(const char *stage)
{
    const char *safeStage = stage == nullptr ? "unknown" : stage;
    if (!ClaimStage(safeStage)) {
        return;
    }
    TE_LOG_INFO << "process environment dump begin, stage=" << safeStage;
    constexpr std::array<const char *, 14> safeNames = { "YR_TE_HIXL_ROUTE",
                                                         "YR_TE_HIXL_CS_MODE",
                                                         "YR_TE_HIXL_AUTO_CONNECT",
                                                         "YR_TE_HIXL_BASE_PORT",
                                                         "YR_TE_HIXL_CONNECT_TIMEOUT_MS",
                                                         "YR_TE_HIXL_TRANSFER_TIMEOUT_MS",
                                                         "YR_TE_HIXL_READ_LEASE_TTL_MS",
                                                         "YR_TE_RPC_PORT_MIN",
                                                         "YR_TE_RPC_PORT_MAX",
                                                         "YR_TE_LOG_LEVEL",
                                                         "YR_TE_VLOG_LEVEL",
                                                         "YR_TE_LOG_TO_STDERR",
                                                         "YR_TE_ALSO_LOG_TO_STDERR",
                                                         "YR_TE_LOG_TO_STDOUT" };
    for (const char *name : safeNames) {
        const char *value = std::getenv(name);
        if (value != nullptr) {
            TE_LOG_INFO << "env " << name << "=" << value;
        }
    }
    TE_LOG_INFO << "process environment dump end, stage=" << safeStage;
}

}  // namespace

void DumpProcessEnvironment(const char *stage)
{
    if (!IsEnvDumpEnabled()) {
        return;
    }
    DumpOncePerStage(stage);
}

}  // namespace internal
}  // namespace datasystem
