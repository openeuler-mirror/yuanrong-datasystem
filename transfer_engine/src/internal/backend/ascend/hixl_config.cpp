/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include "internal/backend/ascend/hixl_config.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr char K_CS_MODE_AUTO[] = "auto";
constexpr char K_CS_MODE_ON[] = "on";
constexpr char K_CS_MODE_OFF[] = "off";
constexpr char K_AUTO_CONNECT_AUTO[] = "auto";
constexpr char K_AUTO_CONNECT_ON[] = "on";
constexpr char K_AUTO_CONNECT_OFF[] = "off";
constexpr char K_LOCAL_COMM_RES_V1_3[] = R"({"version":"1.3"})";
constexpr char K_PROTOCOL_DESC_KEY[] = "comm_resource_config.protocol_desc";
constexpr char K_ROOT_INFO_V1[] = "transfer_engine_hixl_root_info_v1";
constexpr char K_ROOT_INFO_V2[] = "transfer_engine_hixl_root_info_v2";

std::string ToLowerAscii(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

Result ParseGlobalResourceConfig(const std::string &raw, nlohmann::json *config)
{
    TE_CHECK_PTR_OR_RETURN(config);
    if (raw.empty()) {
        *config = nlohmann::json::object();
        return Result::OK();
    }
    try {
        *config = nlohmann::json::parse(raw);
    } catch (const nlohmann::json::exception &e) {
        return TE_MAKE_STATUS(ErrorCode::kInvalid,
                              std::string("invalid YR_TE_HIXL_GLOBAL_RESOURCE_CONFIG: ") + e.what());
    }
    TE_CHECK_OR_RETURN(config->is_object(), ErrorCode::kInvalid,
                       "YR_TE_HIXL_GLOBAL_RESOURCE_CONFIG should be a JSON object");
    return Result::OK();
}

Result ResolveLocalCommRes(const std::string &raw, std::string *localCommRes)
{
    TE_CHECK_PTR_OR_RETURN(localCommRes);
    if (raw.empty()) {
        *localCommRes = K_LOCAL_COMM_RES_V1_3;
        return Result::OK();
    }
    nlohmann::json config;
    try {
        config = nlohmann::json::parse(raw);
    } catch (const nlohmann::json::exception &e) {
        return TE_MAKE_STATUS(ErrorCode::kInvalid,
                              std::string("invalid YR_TE_HIXL_LOCAL_COMM_RES: ") + e.what());
    }
    TE_CHECK_OR_RETURN(config.is_object(), ErrorCode::kInvalid,
                       "YR_TE_HIXL_LOCAL_COMM_RES should be a JSON object");
    const auto version = config.find("version");
    TE_CHECK_OR_RETURN(version != config.end() && version->is_string() && version->get<std::string>() == "1.3",
                       ErrorCode::kInvalid,
                       "YR_TE_HIXL_LOCAL_COMM_RES requires string version 1.3");
    *localCommRes = config.dump();
    return Result::OK();
}

Result ReadProtocolDescriptors(const nlohmann::json &config, std::vector<std::string> *descriptors)
{
    TE_CHECK_PTR_OR_RETURN(descriptors);
    descriptors->clear();
    const auto iter = config.find(K_PROTOCOL_DESC_KEY);
    if (iter == config.end()) {
        return Result::OK();
    }
    if (iter->is_string()) {
        std::string descriptor = iter->get<std::string>();
        TE_CHECK_OR_RETURN(!descriptor.empty(), ErrorCode::kInvalid,
                           "comm_resource_config.protocol_desc should not be empty");
        descriptors->push_back(std::move(descriptor));
        return Result::OK();
    }
    TE_CHECK_OR_RETURN(iter->is_array(), ErrorCode::kInvalid,
                       "comm_resource_config.protocol_desc should be a string or string array");
    for (const auto &descriptor : *iter) {
        TE_CHECK_OR_RETURN(descriptor.is_string(), ErrorCode::kInvalid,
                           "comm_resource_config.protocol_desc array should contain only strings");
        std::string value = descriptor.get<std::string>();
        TE_CHECK_OR_RETURN(!value.empty(), ErrorCode::kInvalid,
                           "comm_resource_config.protocol_desc should not contain empty values");
        descriptors->push_back(std::move(value));
    }
    return Result::OK();
}

Result ApplyExplicitRoute(const std::string &routePolicy, nlohmann::json *globalResourceConfig)
{
    if (routePolicy == "auto") {
        return Result::OK();
    }
    const std::string expectedDescriptor = routePolicy + ":device";
    std::vector<std::string> configuredDescriptors;
    TE_RETURN_IF_ERROR(ReadProtocolDescriptors(*globalResourceConfig, &configuredDescriptors));
    if (!configuredDescriptors.empty()) {
        TE_CHECK_OR_RETURN(configuredDescriptors.size() == 1 && configuredDescriptors.front() == expectedDescriptor,
                           ErrorCode::kInvalid,
                           "YR_TE_HIXL_ROUTE conflicts with comm_resource_config.protocol_desc");
    }
    (*globalResourceConfig)[K_PROTOCOL_DESC_KEY] = expectedDescriptor;
    return Result::OK();
}

Result ParseRootInfoHeader(const std::string &header, HixlEngineMode *engineMode)
{
    TE_CHECK_PTR_OR_RETURN(engineMode);
    if (header == K_ROOT_INFO_V1) {
        *engineMode = HixlEngineMode::kLegacy;
        return Result::OK();
    }
    if (header == K_ROOT_INFO_V2) {
        *engineMode = HixlEngineMode::kClientServer;
        return Result::OK();
    }
    return TE_MAKE_STATUS(ErrorCode::kInvalid, "invalid hixl root info header");
}

}  // namespace

const char *HixlEngineModeName(HixlEngineMode mode)
{
    return mode == HixlEngineMode::kClientServer ? "cs" : "legacy";
}

Result ResolveHixlCsConfig(const HixlCsConfigInput &input, HixlCsConfig *config)
{
    TE_CHECK_PTR_OR_RETURN(config);
    const std::string requestedMode =
        ToLowerAscii(input.requestedMode.empty() ? K_DEFAULT_HIXL_CS_MODE : input.requestedMode);
    TE_CHECK_OR_RETURN(
        requestedMode == K_CS_MODE_AUTO || requestedMode == K_CS_MODE_ON || requestedMode == K_CS_MODE_OFF,
        ErrorCode::kInvalid, "YR_TE_HIXL_CS_MODE should be auto, on or off");
    TE_CHECK_OR_RETURN(input.routePolicy == "auto" || input.routePolicy == "hccs" || input.routePolicy == "roce",
                       ErrorCode::kInvalid, "invalid HIXL route policy");

    nlohmann::json globalResourceConfig;
    TE_RETURN_IF_ERROR(ParseGlobalResourceConfig(input.globalResourceConfig, &globalResourceConfig));
    std::vector<std::string> configuredDescriptors;
    TE_RETURN_IF_ERROR(ReadProtocolDescriptors(globalResourceConfig, &configuredDescriptors));

    const bool enableCs =
        requestedMode == K_CS_MODE_ON || (requestedMode == K_CS_MODE_AUTO && input.capabilityAvailable);
    if (requestedMode == K_CS_MODE_ON) {
        TE_CHECK_OR_RETURN(input.capabilityAvailable, ErrorCode::kNotSupported,
                           "HIXL client-server mode requires CANN/HIXL 9.1.0 or newer");
    }
    if (!enableCs) {
        TE_CHECK_OR_RETURN(input.localCommRes.empty(),
                           requestedMode == K_CS_MODE_OFF ? ErrorCode::kInvalid : ErrorCode::kNotSupported,
                           "YR_TE_HIXL_LOCAL_COMM_RES requires HIXL client-server mode");
        TE_CHECK_OR_RETURN(configuredDescriptors.empty(),
                           requestedMode == K_CS_MODE_OFF ? ErrorCode::kInvalid : ErrorCode::kNotSupported,
                           "comm_resource_config.protocol_desc requires HIXL client-server mode");
        TE_CHECK_OR_RETURN(input.routePolicy != "roce" || input.legacyRoceEnabled, ErrorCode::kNotSupported,
                           "legacy HIXL requires HCCL_INTRA_ROCE_ENABLE=1 to force the RoCE route");
        config->engineMode = HixlEngineMode::kLegacy;
        config->localCommRes.clear();
        config->globalResourceConfig = input.globalResourceConfig;
        return Result::OK();
    }

    TE_RETURN_IF_ERROR(ApplyExplicitRoute(input.routePolicy, &globalResourceConfig));
    config->engineMode = HixlEngineMode::kClientServer;
    TE_RETURN_IF_ERROR(ResolveLocalCommRes(input.localCommRes, &config->localCommRes));
    config->globalResourceConfig = globalResourceConfig.empty() ? std::string() : globalResourceConfig.dump();
    return Result::OK();
}

Result ResolveHixlAutoConnectConfig(const std::string &requestedMode, bool capabilityAvailable,
                                    HixlAutoConnectConfig *config)
{
    TE_CHECK_PTR_OR_RETURN(config);
    const std::string mode = ToLowerAscii(requestedMode.empty() ? K_AUTO_CONNECT_AUTO : requestedMode);
    TE_CHECK_OR_RETURN(mode == K_AUTO_CONNECT_AUTO || mode == K_AUTO_CONNECT_ON || mode == K_AUTO_CONNECT_OFF ||
                           mode == "1" || mode == "0",
                       ErrorCode::kInvalid,
                       "YR_TE_HIXL_AUTO_CONNECT should be auto, on or off");
    const bool explicitlyEnabled = mode == K_AUTO_CONNECT_ON || mode == "1";
    if (explicitlyEnabled) {
        TE_CHECK_OR_RETURN(capabilityAvailable, ErrorCode::kNotSupported,
                           "HIXL AutoConnect is not supported by the current CANN/HIXL runtime");
    }
    config->enabled = explicitlyEnabled || (mode == K_AUTO_CONNECT_AUTO && capabilityAvailable);
    config->optionValue = config->enabled ? "1" : "0";
    return Result::OK();
}

Result ParseHixlPeerInfo(const std::string &rootInfoBytes, HixlPeerInfo *peerInfo)
{
    TE_CHECK_PTR_OR_RETURN(peerInfo);
    *peerInfo = HixlPeerInfo{};
    std::istringstream input(rootInfoBytes);
    std::string line;
    if (!std::getline(input, line)) {
        return TE_MAKE_STATUS(ErrorCode::kInvalid, "invalid hixl root info header");
    }
    TE_RETURN_IF_ERROR(ParseRootInfoHeader(line, &peerInfo->engineMode));
    bool modeSeen = false;
    while (std::getline(input, line)) {
        const size_t separator = line.find('=');
        if (separator == std::string::npos) {
            continue;
        }
        const std::string key = line.substr(0, separator);
        const std::string value = line.substr(separator + 1);
        if (key == "backend") {
            peerInfo->backendKind = value;
        } else if (key == "endpoint") {
            peerInfo->endpoint = value;
        } else if (key == "route") {
            peerInfo->routePolicy = value;
        } else if (key == "mode") {
            modeSeen = true;
            TE_CHECK_OR_RETURN(value == HixlEngineModeName(peerInfo->engineMode), ErrorCode::kInvalid,
                               "hixl root info mode does not match its version");
        }
    }
    TE_CHECK_OR_RETURN(peerInfo->backendKind == "ascend", ErrorCode::kInvalid, "invalid ascend root backend");
    TE_CHECK_OR_RETURN(!peerInfo->endpoint.empty(), ErrorCode::kInvalid, "hixl root endpoint is empty");
    TE_CHECK_OR_RETURN(
        peerInfo->routePolicy == "auto" || peerInfo->routePolicy == "hccs" || peerInfo->routePolicy == "roce",
        ErrorCode::kInvalid, "invalid hixl route policy");
    TE_CHECK_OR_RETURN(peerInfo->engineMode != HixlEngineMode::kClientServer || modeSeen, ErrorCode::kInvalid,
                       "hixl client-server root info is missing mode");
    return Result::OK();
}

std::string EncodeHixlPeerInfo(const HixlPeerInfo &peerInfo)
{
    std::ostringstream output;
    output << (peerInfo.engineMode == HixlEngineMode::kClientServer ? K_ROOT_INFO_V2 : K_ROOT_INFO_V1) << "\n"
           << "backend=" << peerInfo.backendKind << "\n"
           << "endpoint=" << peerInfo.endpoint << "\n"
           << "route=" << peerInfo.routePolicy << "\n";
    if (peerInfo.engineMode == HixlEngineMode::kClientServer) {
        output << "mode=" << HixlEngineModeName(peerInfo.engineMode) << "\n";
    }
    return output.str();
}

}  // namespace datasystem
