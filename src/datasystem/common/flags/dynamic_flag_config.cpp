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
 * Description: Runtime dynamic flag configuration operations.
 */
#include "datasystem/common/flags/dynamic_flag_config.h"
#include <algorithm>
#include <cstring>
#include <iterator>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/flags/flags.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/operation_logger.h"

#include <securec.h>
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/validator.h"

DS_DECLARE_string(monitor_config_file);

namespace {

const std::unordered_set<std::string> kSamplerFlagNames = {
    "request_sample_rate", "access_sample_rate", "diagnostic_sample_rate"
};

}  // namespace

namespace datasystem {

void DynamicFlagConfig::SortRecordsByName(std::vector<DynamicFlagConfig::FlagUpdateRecord> &records)
{
    std::sort(records.begin(), records.end(),
              [](const DynamicFlagConfig::FlagUpdateRecord &a, const DynamicFlagConfig::FlagUpdateRecord &b) {
                  return a.name < b.name;
              });
}

std::string DynamicFlagConfig::JoinPassedRecords(const std::vector<DynamicFlagConfig::FlagUpdateRecord> &records)
{
    std::vector<std::string> parts;
    parts.reserve(records.size());
    std::transform(records.begin(), records.end(), std::back_inserter(parts),
                   [](const DynamicFlagConfig::FlagUpdateRecord &record) { return record.name + "=" + record.detail; });
    return Join(parts, ", ");
}

std::string DynamicFlagConfig::JoinRecordsWithReason(const std::vector<DynamicFlagConfig::FlagUpdateRecord> &records)
{
    std::vector<std::string> parts;
    parts.reserve(records.size());
    std::transform(records.begin(), records.end(), std::back_inserter(parts),
                   [](const DynamicFlagConfig::FlagUpdateRecord &record) {
                       return record.name + "(" + record.detail + ")";
                   });
    return Join(parts, ", ");
}

FlagInfoMap DynamicFlagConfig::GetAllFlagsToMap()
{
    std::vector<FlagInfo> defaultFlags;
    GetAllFlags(defaultFlags);
    FlagInfoMap flagInfoMap;
    std::transform(defaultFlags.begin(), defaultFlags.end(),
                   std::insert_iterator<FlagInfoMap>(flagInfoMap, flagInfoMap.begin()),
                   [](const FlagInfo &flag) -> std::pair<std::string, FlagInfo> {
                       return std::make_pair(flag.name, std::move(flag));
                   });
    return flagInfoMap;
}

std::string DynamicFlagConfig::GetNonDefaultFlags(const FlagInfoMap &defaultFlags)
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
                std::string flagValue = MaskFlagValue(flag.name, flag.value);
                args << "--" << flag.name << '=' << flagValue << "\n";
            }
        }
    }
    return args.str();
}

std::string DynamicFlagConfig::GetExplicitDeclaredFlags()
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

std::string DynamicFlagConfig::GetAllFlagsStr()
{
    std::vector<FlagInfo> defaultFlags;
    GetAllFlags(defaultFlags);
    std::ostringstream args;
    for (const auto &flag : defaultFlags) {
        args << "--" << flag.name << '=' << flag.value << '\n';
    }
    return args.str();
}

Status DynamicFlagConfig::EraseInfo(int argc, char **argv)
{
    if (argc == 0 || argv == nullptr) {
        return Status::OK();
    }
    // erase gflag format: -key1 value1, --key2 value2, -key3=value3, --key4=value4
    auto flagNameOf = [](const std::string &para) {
        const size_t nameStart = para.find_first_not_of('-');
        if (nameStart == std::string::npos) {
            return std::string();
        }
        size_t nameEnd = para.find('=');
        if (nameEnd == std::string::npos) {
            nameEnd = para.size();
        }
        return para.substr(nameStart, nameEnd - nameStart);
    };
    int ret = 0;
    for (int i = 0; i < argc; ++i) {
        auto target = std::string(argv[i], strlen(argv[i]));
        if (target.empty() || target[0] != '-' || !IsSensitiveFlagName(flagNameOf(target))) {
            continue;
        }
        const size_t offset = target.find('=');
        if (offset == std::string::npos) {
            if (i + 1 >= argc) {
                continue;
            }
            ret = memset_s(argv[i + 1], strlen(argv[i + 1]), '\0', strlen(argv[i + 1]));
        } else {
            ret = memset_s(argv[i] + offset, target.length() - offset, '\0', target.length() - offset);
        }
        if (ret != EOK) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Erase info failed");
        }
    }
    return Status::OK();
}

void DynamicFlagConfig::MonitorConfigFile(const std::string &configFilePath)
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

void DynamicFlagConfig::StartConfigFileHandle(const std::string &configFilePath,
                                              std::chrono::time_point<std::chrono::steady_clock> nowTime)
{
    if (configFilePath.empty()) {
        return;
    }
    // Check whether the configfile exists.
    preConfigCheckTime_ = nowTime;
    bool fileExists = false;
    auto rc = CheckFileExists(&fileExists, configFilePath);
    if (!fileExists) {
        LOG_FIRST_N(WARNING, 1) << FormatString("Monitor config file does not exist: %s", configFilePath);
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
    // Update the gflag parameter. LOG_IF_ERROR appends the status detail with the failed-flag list.
    LOG_IF_ERROR(UpdateFlagParameter(flagMap), "UpdateFlagParameter failed");
}

std::unordered_map<std::string, std::string> DynamicFlagConfig::ProcessFlagFile(const std::string &configFilePath)
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

std::string::size_type DynamicFlagConfig::FindFirstSeparator(const std::string &content)
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

std::unordered_map<std::string, std::string> DynamicFlagConfig::ProcessOptions(const std::string &flagFileContext)
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

bool DynamicFlagConfig::SplitArgument(const char *flagCommand, std::pair<std::string, std::string> &nameAndVal)
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

bool DynamicFlagConfig::ValidateFlagName(const std::string &flagName)
{
    if (!FlagManager::GetInstance()->IsModifiableFlag(flagName)) {
        LOG(ERROR) << FormatString("Invalid flag parameter:%s. Flag is not dynamically modifiable.", flagName);
        OperationLogger::Instance().LogConfigFailed(flagName, "not modifiable");
        return false;
    }
    return true;
}

void DynamicFlagConfig::SetIsToHandle(
    std::function<bool(const std::unordered_map<std::string, std::string> &, const std::string &)> handler)
{
    isToHandle_ = handler;
}

bool DynamicFlagConfig::IsToHandle(const std::unordered_map<std::string, std::string> &flagMap,
                                   const std::string &flagName)
{
    if (isToHandle_) {
        return isToHandle_(flagMap, flagName);
    }
    return true;
}

bool DynamicFlagConfig::ValidateAndCommitSamplerFlags(const std::unordered_map<std::string, std::string> &flagMap)
{
    bool hasSamplerFlag = std::any_of(kSamplerFlagNames.begin(), kSamplerFlagNames.end(),
                                      [&flagMap](const std::string &name) { return flagMap.count(name) > 0; });

    std::unordered_map<std::string, std::string> candidates;
    for (const auto &name : kSamplerFlagNames) {
        auto it = flagMap.find(name);
        if (it == flagMap.end() || !IsToHandle(flagMap, name)) continue;
        std::string newVal = it->second;
        // Skip unchanged values so the audit log stays consistent with the batch summary (skipped)
        // and no x=x CONFIG_CHANGED record is produced.
        std::string currVal;
        if (GetCommandLineOption(name.c_str(), currVal) && currVal == newVal) {
            continue;
        }
        if (ValidateSpecial(name, newVal)) {
            LOG(ERROR) << FormatString("Sampler flag %s=%s validation failed, aborting batch commit", name, newVal);
            return false;
        }
        candidates[name] = newVal;
    }
    if (candidates.empty()) {
        if (hasSamplerFlag) {
            LogSampleUserConfig cfg;
            cfg.requestSampleRate = FLAGS_request_sample_rate;
            cfg.accessSampleRate = FLAGS_access_sample_rate;
            cfg.diagnosticSampleRate = FLAGS_diagnostic_sample_rate;
            LogSampler::Instance().UpdateConfigFromFlags(cfg);
        }
        return true;
    }
    return CommitSamplerFlagsTransaction(candidates);
}

bool DynamicFlagConfig::CommitSamplerFlagsTransaction(
    const std::unordered_map<std::string, std::string> &candidates)
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
    if (!LogSampler::Instance().UpdateConfigFromFlags(updatedCfg)) {
        for (const auto &prev : prevVals) {
            std::string revertErrMsg;
            SetCommandLineOption(prev.first.c_str(), prev.second, revertErrMsg);
        }
        LOG(ERROR) << FormatString("LogSampler batch commit failed, retaining previous sampler config");
        return false;
    }
    for (const auto &kv : candidates) {
        OperationLogger::Instance().LogConfigChanged(kv.first, prevVals[kv.first], kv.second);
    }
    LOG(INFO) << FormatString("Sampler flags updated: request=%g access=%g diagnostic=%g",
                              updatedCfg.requestSampleRate, updatedCfg.accessSampleRate,
                              updatedCfg.diagnosticSampleRate);
    return true;
}

Status DynamicFlagConfig::ValidateFlagBatch(const std::map<std::string, std::string> &orderedFlagMap,
                                            const std::unordered_map<std::string, std::string> &flagMap,
                                            FlagBatchValidation &validated)
{
    // Phase 1: dry-run validation of every flag in the batch before writing anything, so an
    // invalid value rejects the whole batch (same semantics as the JSON UpdateConfig API).
    for (const auto &kv : orderedFlagMap) {
        ValidateOneFlag(flagMap, kv.first, kv.second, validated);
    }
    SortRecordsByName(validated.passed);
    SortRecordsByName(validated.skipped);
    SortRecordsByName(validated.failed);
    return Status::OK();
}

void DynamicFlagConfig::ValidateOneFlag(const std::unordered_map<std::string, std::string> &flagMap,
                                        const std::string &flagName, const std::string &newVal,
                                        FlagBatchValidation &validated)
{
    if (kSamplerFlagNames.count(flagName) > 0) {
        validated.hasSamplerFlags = true;
    }
    if (!IsToHandle(flagMap, flagName)) {
        validated.skipped.push_back({flagName, "filtered"});
        return;
    }
    std::string currVal;
    if (!GetCommandLineOption(flagName.c_str(), currVal)) {
        validated.skipped.push_back({flagName, "unknown flag"});
        return;
    }
    if (currVal == newVal) {
        validated.skipped.push_back({flagName, "value unchanged"});
        return;
    }
    std::string errMsg;
    if (!FlagManager::GetInstance()->ValidateChange(flagName, newVal, errMsg)) {
        errMsg = Trim(errMsg);
        OperationLogger::Instance().LogConfigFailed(flagName, errMsg);
        validated.failed.push_back({flagName, errMsg});
        return;
    }
    if (kSamplerFlagNames.count(flagName) > 0) {
        if (ValidateSpecial(flagName, newVal)) {
            errMsg = FormatString("special validation rejected value %s", newVal.c_str());
            OperationLogger::Instance().LogConfigFailed(flagName, errMsg);
            validated.failed.push_back({flagName, errMsg});
            return;
        }
    } else if (!ValidateSpecialConstraint(flagMap, flagName, newVal, errMsg)) {
        errMsg = Trim(errMsg);
        OperationLogger::Instance().LogConfigFailed(flagName, errMsg);
        validated.failed.push_back({flagName, errMsg});
        return;
    }
    validated.passed.push_back(
        {flagName, MaskFlagValue(flagName, currVal) + "-->" + MaskFlagValue(flagName, newVal)});
}

Status DynamicFlagConfig::CommitFlagBatch(const std::unordered_map<std::string, std::string> &flagMap,
                                          const std::map<std::string, std::string> &orderedFlagMap,
                                          const FlagBatchValidation &validated)
{
    // Phase 2: commit the validated batch. A residual failure can only come from a concurrent
    // change between validation and commit; roll the batch back to keep all-or-nothing semantics.
    std::unordered_set<std::string> passedNames;
    std::transform(validated.passed.begin(), validated.passed.end(), std::inserter(passedNames, passedNames.end()),
                   [](const FlagUpdateRecord &record) { return record.name; });
    std::unordered_map<std::string, std::string> prevVals;
    for (const auto &kv : orderedFlagMap) {
        if (kSamplerFlagNames.count(kv.first) > 0 || passedNames.count(kv.first) == 0) {
            continue;
        }
        std::string prevVal;
        if (GetCommandLineOption(kv.first.c_str(), prevVal)) {
            prevVals[kv.first] = prevVal;
        }
        auto status = UpdateSingleFlag(flagMap, kv.first, kv.second);
        if (status.IsError()) {
            return AbortCommit("Dynamic flag update aborted during commit phase: " + status.ToString(), prevVals,
                               status);
        }
    }
    if (validated.hasSamplerFlags && batchCommitHandler_ && !batchCommitHandler_(flagMap)) {
        return AbortCommit("failed to update sampler flags", prevVals,
                           Status(StatusCode::K_INVALID, __LINE__, __FILE__, "failed to update sampler flags"));
    }
    return Status::OK();
}

Status DynamicFlagConfig::UpdateFlagParameter(const std::unordered_map<std::string, std::string> &flagMap)
{
    if (flagMap.empty()) {
        return Status::OK();
    }
    std::map<std::string, std::string> orderedFlagMap(flagMap.begin(), flagMap.end());
    const size_t total = orderedFlagMap.size();
    FlagBatchValidation validated;
    auto validateStatus = ValidateFlagBatch(orderedFlagMap, flagMap, validated);
    if (validateStatus.IsError()) {
        return validateStatus;
    }

    if (!validated.failed.empty()) {
        std::string summary = FormatString(
            "Dynamic flag update rejected (all-or-nothing): total=%zu, passed=%zu, skipped=%zu, failed=%zu; none "
            "applied",
            total, validated.passed.size(), validated.skipped.size(), validated.failed.size());
        if (!validated.passed.empty()) {
            summary += "\n  not-applied(passed): " + JoinPassedRecords(validated.passed);
        }
        if (!validated.skipped.empty()) {
            summary += "\n  skipped: " + JoinRecordsWithReason(validated.skipped);
        }
        summary += "\n  failed: " + JoinRecordsWithReason(validated.failed);
        LOG(ERROR) << summary;
        RETURN_STATUS(StatusCode::K_INVALID,
                      FormatString("dynamic flag update rejected: %s",
                                   JoinRecordsWithReason(validated.failed).c_str()));
    }

    auto commitStatus = CommitFlagBatch(flagMap, orderedFlagMap, validated);
    if (commitStatus.IsError()) {
        return commitStatus;
    }

    std::string summary = FormatString("Dynamic flag update summary: total=%zu, updated=%zu, skipped=%zu, failed=0",
                                       total, validated.passed.size(), validated.skipped.size());
    if (!validated.passed.empty()) {
        summary += "\n  updated: " + JoinPassedRecords(validated.passed);
    }
    if (!validated.skipped.empty()) {
        summary += "\n  skipped: " + JoinRecordsWithReason(validated.skipped);
    }
    LOG(INFO) << summary;
    return Status::OK();
}

std::string DynamicFlagConfig::MaskFlagValue(const std::string &flagName, const std::string &value) const
{
    return OperationLogger::MaskSensitiveFlagValue(flagName, value);
}

std::vector<std::string> DynamicFlagConfig::RollbackFlagValues(
    const std::unordered_map<std::string, std::string> &prevVals)
{
    std::vector<std::string> restoreFailed;
    for (const auto &kv : prevVals) {
        std::string errMsg;
        if (!SetCommandLineOption(kv.first.c_str(), kv.second, errMsg)) {
            LOG(ERROR) << FormatString("failed to rollback flag %s: %s", kv.first, errMsg);
            restoreFailed.push_back(kv.first);
        }
    }
    return restoreFailed;
}

Status DynamicFlagConfig::AbortCommit(const std::string &reason,
                                      const std::unordered_map<std::string, std::string> &prevVals,
                                      const Status &failStatus)
{
    std::vector<std::string> restoreFailed = RollbackFlagValues(prevVals);
    if (restoreFailed.empty()) {
        LOG(ERROR) << reason << "; batch rolled back";
        return failStatus;
    }
    const std::string msg = reason + "; batch rollback incomplete, restore failed for: " + Join(restoreFailed, ", ");
    LOG(ERROR) << msg;
    RETURN_STATUS(StatusCode::K_INVALID, msg);
}

Status DynamicFlagConfig::UpdateSingleFlag(const std::unordered_map<std::string, std::string> &flagMap,
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

void DynamicFlagConfig::TrimSpace(std::string &value)
{
    if (value.empty()) {
        return;
    }
    value.erase(0, value.find_first_not_of(' '));
    value.erase(value.find_last_not_of(' ') + 1);
}

void DynamicFlagConfig::SetValidateSpecial(
    const std::function<bool(const std::string &, const std::string &)> &validator)
{
    validateSpecial_ = validator;
}

bool DynamicFlagConfig::ValidateSpecial(const std::string &flagName, const std::string &newVal)
{
    return validateSpecial_ && validateSpecial_(flagName, newVal);
}

void DynamicFlagConfig::SetBatchCommitHandler(
    std::function<bool(const std::unordered_map<std::string, std::string> &)> handler)
{
    // nullptr restores the default built-in log sampler transactional commit handler installed in the constructor.
    if (handler) {
        batchCommitHandler_ = std::move(handler);
    } else {
        batchCommitHandler_ = [this](const std::unordered_map<std::string, std::string> &flagMap) {
            return ValidateAndCommitSamplerFlags(flagMap);
        };
    }
}

bool DynamicFlagConfig::ValidateSpecialConstraint(const std::unordered_map<std::string, std::string> &flagMap,
                                                  const std::string &flagName, const std::string &newVal,
                                                  std::string &errMsg)
{
    if (kSamplerFlagNames.count(flagName) > 0) {
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
