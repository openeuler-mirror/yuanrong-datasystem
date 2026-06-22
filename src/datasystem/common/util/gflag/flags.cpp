/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Defines operations related to gflag.
 */
#include "datasystem/common/util/gflag/flags.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>

#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/operation_logger.h"

#include <securec.h>
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/validator.h"

DS_DECLARE_string(monitor_config_file);

namespace datasystem {
GFlagsMap Flags::GetAllFlagsToMap()
{
    std::vector<FlagInfo> defaultFlags;
    GetAllFlags(defaultFlags);
    GFlagsMap gflagsMap;
    std::transform(defaultFlags.begin(), defaultFlags.end(),
                   std::insert_iterator<GFlagsMap>(gflagsMap, gflagsMap.begin()),
                   [](const FlagInfo &flag) -> std::pair<std::string, FlagInfo> {
                       return std::make_pair(flag.name, std::move(flag));
                   });
    return gflagsMap;
}

std::string Flags::GetNonDefaultFlags(const GFlagsMap &defaultFlags)
{
    std::ostringstream args;
    std::vector<FlagInfo> currentFlags;
    GetAllFlags(currentFlags);
    for (const auto &flag : currentFlags) {
        if (!flag.isDefault) {
            // is_default indicates that the flag has been modified, but it cannot be determined whether the flag is the
            // same as the default value. Therefore, we need to compare the flag with the obtained default value.
            const auto &defaultFlag = defaultFlags.find(flag.name);
            // It's very unlikely, but still possible that we don't have the flag in defaults.
            if (defaultFlag == defaultFlags.end() || flag.value != defaultFlag->second.value) {
                std::string flagValue = excludeGflags_.count(flag.name) ? "xxx" : flag.value;
                args << "--" << flag.name << '=' << flagValue << "\n";
            }
        }
    }
    return args.str();
}

std::string Flags::GetExplicitDeclaredFlags()
{
    // The differences between this and GetNonDefaultFlags() are:
    // 1. This function returns all flags user defined and passed in through the commandline,
    // even through the user defined the same value as the default one.
    // 2. This function also returns the flag changed during runtime, while GetNonDefaultFlags() not.
    std::vector<FlagInfo> defaultFlags;
    GetAllFlags(defaultFlags);
    std::ostringstream args;
    for (const auto &flag : defaultFlags) {
        if (flag.wasSpecified || !flag.isDefault) {
            args << "--" << flag.name << '=' << flag.value << '\n';
        }
    }
    return args.str();
}

std::string Flags::GetAllFlagsStr()
{
    std::vector<FlagInfo> defaultFlags;
    GetAllFlags(defaultFlags);
    std::ostringstream args;
    for (const auto &flag : defaultFlags) {
        args << "--" << flag.name << '=' << flag.value << '\n';
    }
    return args.str();
}

Status Flags::EraseInfo(int argc, char **argv)
{
    if (argc == 0 || argv == nullptr) {
        return Status::OK();
    }
    std::unordered_set<std::string> infos;
    for (const auto &name : excludeGflags_) {
        // erase gflag format: -key1 value1,  --key2 value2, -key3=value3, --key4=value4
        infos.insert("-" + name);
        infos.insert("--" + name);
    }
    auto needEraseForOffset = [&infos](const std::string &para) {
        auto offset = para.find('=');
        const std::string offsetPara = para.substr(0, offset);
        return infos.count(offsetPara);
    };
    int ret = 0;
    for (int i = 0; i < argc; ++i) {
        auto target = std::string(argv[i], strlen(argv[i]));
        if (infos.count(target) && i + 1 < argc) {
            ret = memset_s(argv[i + 1], strlen(argv[i + 1]), '\0', strlen(argv[i + 1]));
        } else if (needEraseForOffset(target)) {
            auto offset = target.find('=');
            ret = memset_s(argv[i] + offset, target.length() - offset, '\0', target.length() - offset);
        } else {
            continue;
        }
        if (ret != EOK) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Erase info failed");
        }
    }
    return Status::OK();
}

void Flags::MonitorConfigFile(const std::string &configFilePath)
{
    if (configFilePath.empty()) {
        return;
    }
    std::chrono::time_point<clock> nowTime = clock::now();
    uint64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(nowTime - preConfigCheckTime_).count();
    if (elapsed >= TRIGGER_CONFIG_CHECK_NANO_INTERVAL) {
        StartConfigFileHandle(configFilePath, nowTime);
    }
}

void Flags::StartConfigFileHandle(const std::string &configFilePath,
                                  std::chrono::time_point<std::chrono::steady_clock> nowTime)
{
    // Check whether the configfile exists.
    preConfigCheckTime_ = nowTime;
    bool fileExists = false;
    auto rc = CheckFileExists(&fileExists, configFilePath);
    if (!fileExists) {
        LOG(WARNING) << FormatString("Monitor config file does not exist: %s", configFilePath);
        return;
    }
    // Check whether the configfile has been modified.
    int64_t modifiedTime;
    rc = GetFileModifiedTime(configFilePath, modifiedTime);
    LOG_IF_ERROR(rc, "GetFileModifiedTime failed");
    if (rc.IsError()) {
        return;
    }
    if (lastModifiedTime_ == modifiedTime) {
        return;
    }
    lastModifiedTime_ = modifiedTime;
    std::unordered_map<std::string, std::string> flagMap = ProcessFlagFile(configFilePath);
    // Update the gflag parameter.
    LOG_IF_ERROR(UpdateFlagParameter(flagMap), "UpdateFlagParameter failed");
}

std::unordered_map<std::string, std::string> Flags::ProcessFlagFile(const std::string &configFilePath)
{
    std::unordered_map<std::string, std::string> flagMap;
    if (configFilePath.empty()) {
        return flagMap;
    }
    std::string fileContext;
    auto status = ReadFileToString(configFilePath, fileContext);
    if (status.IsError()) {
        LOG(ERROR) << "Failed to read the datasystem.config file as a string:" << status.ToString();
        return {};
    }
    size_t fileSize = fileContext.size();
    if (fileSize > FILE_SIZE_MAX_LIMIT) {
        LOG(ERROR) << FormatString(
            "The size of the configuration file is %zu Byte, which exceeds the max limit of %zu Byte.", fileSize,
            FILE_SIZE_MAX_LIMIT);
        return flagMap;
    }
    return ProcessOptions(fileContext);
}

std::string::size_type Flags::FindFirstSeparator(const std::string &content)
{
    auto spaceSeparator = content.find_first_of(' ');
    auto horizontalTab = content.find_first_of('\t');
    auto windowOrMacSeparator = content.find_first_of('\r');
    auto linuxSeparator = content.find_first_of('\n');
    auto min = std::numeric_limits<std::string::size_type>::max();
    if (spaceSeparator != std::string::npos && spaceSeparator < min) {
        min = spaceSeparator;
    }
    if (horizontalTab != std::string::npos && horizontalTab < min) {
        min = horizontalTab;
    }
    if (windowOrMacSeparator != std::string::npos && windowOrMacSeparator < min) {
        min = windowOrMacSeparator;
    }
    if (linuxSeparator != std::string::npos && linuxSeparator < min) {
        min = linuxSeparator;
    }
    return min == std::numeric_limits<std::string::size_type>::max() ? std::string::npos : min;
}

std::unordered_map<std::string, std::string> Flags::ProcessOptions(const std::string &flagFileContext)
{
    std::unordered_map<std::string, std::string> nameAndValue;
    const char *fileContext = flagFileContext.c_str();
    // Read the flag commands one by one based on '\n' '\r' or ' ' as the separator.
    while (fileContext != nullptr) {
        // Ignore white space characters at the beginning
        while (*fileContext && isspace(static_cast<unsigned char>(*fileContext))) {
            ++fileContext;
        }
        std::string flagCommand;
        auto separatorIndex = FindFirstSeparator(fileContext);
        if (separatorIndex == std::string::npos) {
            flagCommand = fileContext;
            fileContext = nullptr;
        } else {
            flagCommand = std::string(fileContext, separatorIndex);
            fileContext += separatorIndex;
        }
        // Cut off all spaces before and after a string.
        TrimSpace(flagCommand);

        // Each line can be one of three things:
        // 1) An empty line                        -- we skip it
        // 2) A -flag=value or --flag=value        -- apply if previous filenames match
        // 3) other                                -- report error
        if (flagCommand.empty()) {
            // empty line; just ignore
        } else if (flagCommand[0] == '-') {  // flag
            int num = std::count(flagCommand.begin(), flagCommand.end(), '=');
            // an equal sign(=) is required between the parameter and the value.
            if (num != 1) {
                LOG(ERROR) << "Invalid flag command:" << flagCommand;
                continue;
            }
            std::pair<std::string, std::string> kv;
            if (SplitArgument(flagCommand.c_str(), kv)) {
                nameAndValue.emplace(kv);
            }
        } else {
            LOG(ERROR) << "Invalid flag command:" << flagCommand;
        }
    }
    return nameAndValue;
}

bool Flags::SplitArgument(const char *flagCommand, std::pair<std::string, std::string> &nameAndVal)
{
    // skip the leading -
    const char *kv = flagCommand + 1;
    // skip second - too
    if (*kv == '-') {
        kv++;
    }
    std::string kvString = kv;
    // split flagName and value
    std::string flagName;
    std::string value;
    auto i = kvString.find_first_of('=');
    if (i == std::string::npos) {
        LOG(ERROR) << "Invalid flag command:" << flagCommand;
        return false;
    }
    flagName = kvString.substr(0, i);
    value = kvString.substr(i + 1, kvString.size());

    // Check whether the flag parameter can be dynamically modified.
    if (ValidateFlagName(flagName)) {
        nameAndVal.first = std::move(flagName);
        nameAndVal.second = std::move(value);
        return true;
    }
    return false;
}

bool Flags::ValidateFlagName(const std::string &flagName)
{
    if (!FlagManager::GetInstance()->IsModifiableFlag(flagName)) {
        LOG(ERROR) << FormatString("Invalid flag parameter:%s. Flag is not dynamically modifiable.", flagName);
        OperationLogger::Instance().LogConfigFailed(flagName, "not modifiable");
        return false;
    }
    return true;
}

void Flags::SetIsToHandle(
    std::function<bool(const std::unordered_map<std::string, std::string> &, const std::string &)> handler)
{
    isToHandle_ = handler;
}

bool Flags::IsToHandle(const std::unordered_map<std::string, std::string> &flagMap, const std::string &flagName)
{
    if (isToHandle_) {
        return isToHandle_(flagMap, flagName);
    }
    return true;
}

bool Flags::ValidateAndCommitSamplerFlags(const std::unordered_map<std::string, std::string> &flagMap)
{
    static const std::unordered_set<std::string> samplerFlagNames = {
        "request_sample_rate", "access_sample_rate", "diagnostic_sample_rate"
    };
    LogSampleUserConfig cfg;
    cfg.requestSampleRate = FLAGS_request_sample_rate;
    cfg.accessSampleRate = FLAGS_access_sample_rate;
    cfg.diagnosticSampleRate = FLAGS_diagnostic_sample_rate;
    cfg.requestSampleRateExplicit = (flagMap.count("request_sample_rate") > 0);
    cfg.accessSampleRateExplicit = (flagMap.count("access_sample_rate") > 0);
    cfg.diagnosticSampleRateExplicit = (flagMap.count("diagnostic_sample_rate") > 0);

    std::unordered_map<std::string, std::string> candidates;
    for (const auto &name : samplerFlagNames) {
        auto it = flagMap.find(name);
        if (it == flagMap.end() || !IsToHandle(flagMap, name)) continue;
        std::string newVal = it->second;
        if (ValidateSpecial(name, newVal)) {
            LOG(ERROR) << FormatString("Sampler flag %s=%s validation failed, aborting batch commit", name, newVal);
            return false;
        }
        candidates[name] = newVal;
    }
    if (candidates.empty()) {
        if (cfg.requestSampleRateExplicit || cfg.accessSampleRateExplicit || cfg.diagnosticSampleRateExplicit) {
            LogSampler::Instance().UpdateConfigFromFlags(cfg);
        }
        return true;
    }
    return CommitSamplerFlagsTransaction(candidates, cfg);
}

bool Flags::CommitSamplerFlagsTransaction(
    const std::unordered_map<std::string, std::string> &candidates, const LogSampleUserConfig &cfg)
{
    std::unordered_map<std::string, std::string> prevVals;
    std::string errMsg;
    for (const auto &kv : candidates) {
        std::string prevVal;
        GetCommandLineOption(kv.first.c_str(), prevVal);
        prevVals[kv.first] = prevVal;
        if (!SetCommandLineOption(kv.first.c_str(), kv.second, errMsg)) {
            LOG(ERROR) << errMsg;
            for (const auto &prev : prevVals) {
                std::string revertErrMsg;
                SetCommandLineOption(prev.first.c_str(), prev.second, revertErrMsg);
            }
            return false;
        }
    }
    LogSampleUserConfig updatedCfg;
    updatedCfg.requestSampleRate = FLAGS_request_sample_rate;
    updatedCfg.accessSampleRate = FLAGS_access_sample_rate;
    updatedCfg.diagnosticSampleRate = FLAGS_diagnostic_sample_rate;
    updatedCfg.requestSampleRateExplicit = cfg.requestSampleRateExplicit;
    updatedCfg.accessSampleRateExplicit = cfg.accessSampleRateExplicit;
    updatedCfg.diagnosticSampleRateExplicit = cfg.diagnosticSampleRateExplicit;
    if (!LogSampler::Instance().UpdateConfigFromFlags(updatedCfg)) {
        for (const auto &prev : prevVals) {
            std::string revertErrMsg;
            SetCommandLineOption(prev.first.c_str(), prev.second, revertErrMsg);
        }
        LOG(ERROR) << FormatString("LogSampler batch commit failed, retaining previous sampler config");
        return false;
    }
    return true;
}

Status Flags::UpdateFlagParameter(const std::unordered_map<std::string, std::string> &flagMap)
{
    static const std::unordered_set<std::string> samplerFlagNames = {
        "request_sample_rate", "access_sample_rate", "diagnostic_sample_rate"
    };
    bool hasSamplerFlags = false;
    std::unordered_map<std::string, std::string> prevVals;
    for (const auto &kv : flagMap) {
        if (samplerFlagNames.count(kv.first)) {
            hasSamplerFlags = true;
            continue;
        }
        if (!IsToHandle(flagMap, kv.first)) {
            continue;
        }
        std::string prevVal;
        if (GetCommandLineOption(kv.first.c_str(), prevVal)) {
            prevVals[kv.first] = prevVal;
        }
    }
    for (const auto &kv : flagMap) {
        if (samplerFlagNames.count(kv.first)) {
            continue;
        }
        auto status = UpdateSingleFlag(flagMap, kv.first, kv.second);
        if (status.IsError()) {
            RollbackFlagValues(prevVals);
            return status;
        }
    }
    if (hasSamplerFlags && !ValidateAndCommitSamplerFlags(flagMap)) {
        RollbackFlagValues(prevVals);
        RETURN_STATUS(StatusCode::K_INVALID, "failed to update sampler flags");
    }
    return Status::OK();
}

void Flags::RollbackFlagValues(const std::unordered_map<std::string, std::string> &prevVals)
{
    for (const auto &kv : prevVals) {
        std::string errMsg;
        if (!SetCommandLineOption(kv.first.c_str(), kv.second, errMsg)) {
            LOG(ERROR) << FormatString("failed to rollback flag %s: %s", kv.first, errMsg);
        }
    }
}

Status Flags::UpdateSingleFlag(const std::unordered_map<std::string, std::string> &flagMap,
                               const std::string &flagName, const std::string &newVal)
{
    if (!IsToHandle(flagMap, flagName)) {
        return Status::OK();
    }
    std::string currVal;
    bool getResult = GetCommandLineOption(flagName.c_str(), currVal);
    if (!getResult) {
        return Status::OK();
    }
    if (currVal == newVal) {
        return Status::OK();
    }
    if (ValidateSpecial(flagName, newVal)) {
        std::string handledVal;
        if (GetCommandLineOption(flagName.c_str(), handledVal) && handledVal != currVal) {
            OperationLogger::Instance().LogConfigChanged(flagName, currVal, handledVal);
            LOG(INFO) << FormatString("The flag parameter is successfully changed: %s=%s --> %s", flagName,
                                      currVal, handledVal);
            return Status::OK();
        }
        const std::string errMsg = FormatString("failed to update flag %s: special validation rejected value %s",
                                                flagName, newVal);
        LOG(ERROR) << errMsg;
        OperationLogger::Instance().LogConfigFailed(flagName, errMsg);
        RETURN_STATUS(StatusCode::K_INVALID, errMsg);
    }
    std::string errMsg;
    if (!SetCommandLineOption(flagName.c_str(), newVal, errMsg)) {
        LOG(ERROR) << errMsg;
        OperationLogger::Instance().LogConfigFailed(flagName, errMsg);
        RETURN_STATUS(StatusCode::K_INVALID, errMsg);
    }
#ifdef WITH_TESTS
    if (flagName == "inject_actions") {
        LOG_IF_ERROR(inject::ClearAll(), "clear all inject actions failed");
        LOG_IF_ERROR(inject::SetByString(newVal), "set inject actions failed");
    }
#endif
    OperationLogger::Instance().LogConfigChanged(flagName, currVal, newVal);
    LOG(INFO) << FormatString("The flag parameter is successfully changed: %s=%s --> %s", flagName, currVal,
                              newVal);
    return Status::OK();
}

void Flags::TrimSpace(std::string &value)
{
    if (value.empty()) {
        return;
    }
    value.erase(0, value.find_first_not_of(' '));
    value.erase(value.find_last_not_of(' ') + 1);
}

void Flags::SetValidateSpecial(const std::function<bool(const std::string &, const std::string &)> &validator)
{
    validateSpecial_ = validator;
}

bool Flags::ValidateSpecial(const std::string &flagName, const std::string &newVal)
{
    return validateSpecial_ && validateSpecial_(flagName, newVal);
}

bool Flags::ValidateSpecialConstraint(const std::unordered_map<std::string, std::string> &flagMap,
                                      const std::string &flagName, const std::string &newVal,
                                      std::string &errMsg)
{
    static const std::unordered_set<std::string> samplerFlagNames = {
        "request_sample_rate", "access_sample_rate", "diagnostic_sample_rate"
    };
    if (samplerFlagNames.count(flagName) > 0) {
        return true;
    }
    if (!IsToHandle(flagMap, flagName)) {
        return true;
    }
    std::string currVal;
    if (!GetCommandLineOption(flagName.c_str(), currVal)) {
        return true;
    }
    if (currVal == newVal) {
        return true;
    }
    if (!ValidateSpecial(flagName, newVal)) {
        return true;
    }
    std::string handledVal;
    if (GetCommandLineOption(flagName.c_str(), handledVal) && handledVal != currVal) {
        std::string revertErr;
        if (!SetCommandLineOption(flagName.c_str(), currVal.c_str(), revertErr)) {
            errMsg = FormatString("failed to revert flag %s after special validation dry-run: %s", flagName,
                                  revertErr);
            return false;
        }
        return true;
    }
    errMsg = FormatString("special validation rejected value %s", newVal);
    return false;
}

}  // namespace datasystem
