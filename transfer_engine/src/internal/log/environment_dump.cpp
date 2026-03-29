#include "internal/log/environment_dump.h"

#include <cstdlib>
#include <string>

#include <glog/logging.h>

extern char **environ;

namespace datasystem {
namespace internal {
namespace {

bool IsEnvDumpEnabled()
{
    const char *value = std::getenv("TRANSFER_ENGINE_ENABLE_ENV_DUMP");
    if (value == nullptr) {
        return false;
    }
    const std::string flag(value);
    return flag == "1" || flag == "true" || flag == "TRUE" || flag == "on" || flag == "ON" || flag == "yes" ||
           flag == "YES";
}

}  // namespace

void DumpProcessEnvironment(const char *stage)
{
    if (!IsEnvDumpEnabled()) {
        return;
    }
    const char *safeStage = stage == nullptr ? "unknown" : stage;
    LOG(INFO) << "process environment dump begin, stage=" << safeStage;
    if (::environ == nullptr) {
        LOG(WARNING) << "process environment dump skipped: environ is null, stage=" << safeStage;
        return;
    }
    for (int index = 0; ::environ[index] != nullptr; ++index) {
        LOG(INFO) << "env[" << index << "] " << ::environ[index];
    }
    LOG(INFO) << "process environment dump end, stage=" << safeStage;
}

}  // namespace internal
}  // namespace datasystem
