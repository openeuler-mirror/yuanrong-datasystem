#include "internal/log/environment_dump.h"

#include <cstdlib>
#include <array>
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

}  // namespace

void DumpProcessEnvironment(const char *stage)
{
    if (!IsEnvDumpEnabled()) {
        return;
    }
    static std::once_flag dumpOnce;
    std::call_once(dumpOnce, [stage]() {
        const char *safeStage = stage == nullptr ? "unknown" : stage;
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
    });
}

}  // namespace internal
}  // namespace datasystem
