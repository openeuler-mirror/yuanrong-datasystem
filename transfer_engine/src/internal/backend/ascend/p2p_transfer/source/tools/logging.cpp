/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include "tools/logging.h"

#include <cstdlib>
#include <iostream>

#ifdef P2P_TRANSFER_USE_GLOG
#include <glog/logging.h>
#endif

extern char **environ;

namespace p2p {

namespace {

void PrintFallback(const char *level, const std::string &message)
{
    std::cerr << "[P2P][" << level << "] " << message << std::endl;
}

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

void LogInfo(const std::string &message)
{
#ifdef P2P_TRANSFER_USE_GLOG
    VLOG(1) << "[P2P] " << message;
#else
    PrintFallback("INFO", message);
#endif
}

void LogWarning(const std::string &message)
{
#ifdef P2P_TRANSFER_USE_GLOG
    LOG(WARNING) << "[P2P] " << message;
#else
    PrintFallback("WARN", message);
#endif
}

void LogError(const std::string &message)
{
#ifdef P2P_TRANSFER_USE_GLOG
    LOG(ERROR) << "[P2P] " << message;
#else
    PrintFallback("ERROR", message);
#endif
}

void DumpProcessEnvironment(const char *stage)
{
    if (!IsEnvDumpEnabled()) {
        return;
    }
    const char *safeStage = stage == nullptr ? "unknown" : stage;
    LogInfo(std::string("process environment dump begin, stage=") + safeStage);
    if (::environ == nullptr) {
        LogWarning(std::string("process environment dump skipped: environ is null, stage=") + safeStage);
        return;
    }
    for (int index = 0; ::environ[index] != nullptr; ++index) {
        LogInfo(std::string("env[") + std::to_string(index) + "] " + ::environ[index]);
    }
    LogInfo(std::string("process environment dump end, stage=") + safeStage);
}

}  // namespace p2p
