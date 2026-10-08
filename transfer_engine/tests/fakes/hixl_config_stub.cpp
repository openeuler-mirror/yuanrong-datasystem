/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include "internal/backend/ascend/hixl_config.h"

// Connection tests replace vendor configuration separately from the production JSON/config tests.
namespace datasystem {

const char *HixlEngineModeName(HixlEngineMode)
{
    return "legacy";
}

Result ResolveHixlCsConfig(const HixlCsConfigInput &, HixlCsConfig *config)
{
    *config = HixlCsConfig{};
    return Result::OK();
}

Result ResolveHixlAutoConnectConfig(const std::string &mode, bool, HixlAutoConnectConfig *config)
{
    config->enabled = mode == "on";
    config->optionValue = config->enabled ? "1" : "0";
    return Result::OK();
}

Result ParseHixlPeerInfo(const std::string &endpoint, HixlPeerInfo *info)
{
    *info = HixlPeerInfo{ "ascend", endpoint, "roce", HixlEngineMode::kLegacy };
    return Result::OK();
}

std::string EncodeHixlPeerInfo(const HixlPeerInfo &info)
{
    return info.endpoint;
}

}  // namespace datasystem
