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
#include <numeric>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>

#include <securec.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_string(monitor_config_file, "~/datasystem/config/datasystem.config",
                 "Path of the flag configuration file. The application code dynamically modifies the configuration by "
                 "monitoring whether the flagfile is updated.");
DS_DEFINE_validator(monitor_config_file, &Validator::ValidatePathString);

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
        if (!flag.isDefault) {
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
        return;
    }
    // Check whether the configfile has been modified.
    int64_t modifiedTime;
    rc = GetFileModifiedTime(configFilePath, modifiedTime);
    LOG_IF_ERROR(rc, rc.ToString());
    if (rc.IsError()) {
        return;
    }
    if (lastModifiedTime_ == modifiedTime) {
        return;
    }
    lastModifiedTime_ = modifiedTime;
    std::unordered_map<std::string, std::string> flagMap = ProcessFlagFile(configFilePath);
    // Update the gflag parameter.
    UpdateFlagParameter(flagMap);
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
            "The size of the configuration file is %s Byte, which exceeds the max limit of %s Byte.", fileSize,
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
    auto i = flagNameTrustList_.find(flagName);
    if (i == flagNameTrustList_.end()) {
        std::string s =
            std::accumulate(std::begin(flagNameTrustList_), std::end(flagNameTrustList_), std::string{},
                            [](const std::string &a, const std::string &b) { return a.empty() ? b : a + ',' + b; });
        LOG(ERROR) << FormatString(
            "Invalid flag parameter:%s. Only the flag parameter in the following list can be modified:%s", flagName, s);
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

void Flags::UpdateFlagParameter(const std::unordered_map<std::string, std::string> &flagMap)
{
    for (const auto &kv : flagMap) {
        std::string flagName = kv.first;
        // The glag parameter of the log level is specially processed.
        // Determines whether to take effect on the worker.
        if (!IsToHandle(flagMap, flagName)) {
            continue;
        }
        std::string newVal = kv.second;
        std::string currVal;
        bool getResult = GetCommandLineOption(flagName.c_str(), currVal);
        if (!getResult) {
            return;
        }
        if (currVal != newVal) {
            if (ValidateSpecial(flagName, newVal)) {
                continue;
            }
            std::string errMsg;
            if (!SetCommandLineOption(flagName.c_str(), newVal, errMsg)) {
                LOG(ERROR) << errMsg;
            }
#ifdef WITH_TESTS
            if (flagName == "inject_actions") {
                LOG_IF_ERROR(inject::ClearAll(), "clear all inject actions failed");
                LOG_IF_ERROR(inject::SetByString(newVal), "set inject actions failed");
            }
#endif
            LOG(INFO) << FormatString("The flag parameter is successfully changed: %s=%s --> %s", flagName, currVal,
                                      newVal);
        }
    }
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

}  // namespace datasystem
