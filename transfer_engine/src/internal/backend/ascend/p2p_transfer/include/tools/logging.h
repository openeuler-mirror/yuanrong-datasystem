/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#ifndef P2P_LOGGING_H
#define P2P_LOGGING_H

#include <string>

#include "p2p.h"

namespace p2p {

void SetLogCallback(P2pLogCallback callback);
void LogInfo(const std::string &message);
void LogWarning(const std::string &message);
void LogError(const std::string &message);
void DumpProcessEnvironment(const char *stage);

}  // namespace p2p

#endif  // P2P_LOGGING_H
