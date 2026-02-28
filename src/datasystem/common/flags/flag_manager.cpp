/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

#include "datasystem/common/flags/flag_manager.h"
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdarg.h>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <iostream>

#include "datasystem/common/util/strings_util.h"

DS_DECLARE_bool(help);
DS_DECLARE_bool(version);

#define ugly_exit exit

#define TREAT_VALUE_AS(type, value) (*reinterpret_cast<type *>(value))

#define TREAT_VALIDATOR_AS(type, validator, name, value) \
    (reinterpret_cast<bool (*)(const char *, type)>(validator)(name, TREAT_VALUE_AS(const type, value)))

namespace datasystem {
char one[2] = { '1', '\0' };

void ReportError(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    vfprintf(stdout, format, ap);
    va_end(ap);
    fflush(stdout);
}

bool Flag::Assign(const std::string &value, std::string &errMsg)
{
    bool success;
    switch (type_) {
        case FLAG_BOOL:
            success = ParseAssignFromBoolean(value, errMsg);
            break;
        case FLAG_UINT32:
            success = ParseAssignFromUint32(value, errMsg);
            break;
        case FLAG_INT32:
            success = ParseAssignFromInt32(value, errMsg);
            break;
        case FLAG_UINT64:
            success = ParseAssignFromUint64(value, errMsg);
            break;
        case FLAG_INT64:
            success = ParseAssignFromInt64(value, errMsg);
            break;
        case FLAG_STRING:
            success = ParseAssignFromString(value, errMsg);
            break;
        default:
            success = false;
    }

    return success;
}

bool Flag::IsValidate(const void *value) const
{
    if (validator_ == nullptr) {
        return true;
    }

    value = value != nullptr ? value : currentVal_;
    switch (type_) {
        case FLAG_BOOL:
            return TREAT_VALIDATOR_AS(bool, validator_, name_.c_str(), value);
        case FLAG_UINT32:
            return TREAT_VALIDATOR_AS(uint32_t, validator_, name_.c_str(), value);
        case FLAG_INT32:
            return TREAT_VALIDATOR_AS(int32_t, validator_, name_.c_str(), value);
        case FLAG_UINT64:
            return TREAT_VALIDATOR_AS(uint64_t, validator_, name_.c_str(), value);
        case FLAG_INT64:
            return TREAT_VALIDATOR_AS(int64_t, validator_, name_.c_str(), value);
        case FLAG_STRING:
            return TREAT_VALIDATOR_AS(std::string, validator_, name_.c_str(), value);
        default:
            return false;
    }
}

std::string Flag::TypeName() const
{
    switch (type_) {
        case FLAG_BOOL:
            return "bool";
        case FLAG_UINT32:
            return "uint32";
        case FLAG_INT32:
            return "int32";
        case FLAG_UINT64:
            return "uint64";
        case FLAG_INT64:
            return "int64";
        case FLAG_STRING:
            return "string";
        default:
            return "Unknown";
    }
}

std::string Flag::ValueString() const
{
    switch (type_) {
        case FLAG_BOOL:
            return std::to_string(TREAT_VALUE_AS(bool, currentVal_));
        case FLAG_UINT32:
            return std::to_string(TREAT_VALUE_AS(uint32_t, currentVal_));
        case FLAG_INT32:
            return std::to_string(TREAT_VALUE_AS(int32_t, currentVal_));
        case FLAG_UINT64:
            return std::to_string(TREAT_VALUE_AS(uint64_t, currentVal_));
        case FLAG_INT64:
            return std::to_string(TREAT_VALUE_AS(int64_t, currentVal_));
        case FLAG_STRING:
            return TREAT_VALUE_AS(std::string, currentVal_);
        default:
            return "Unknown";
    }
}

void Flag::UpdateModified()
{
    switch (type_) {
        case FLAG_BOOL:
            modified_ = (TREAT_VALUE_AS(bool, currentVal_) != TREAT_VALUE_AS(bool, defaultVal_));
            break;
        case FLAG_UINT32:
            modified_ = (TREAT_VALUE_AS(uint32_t, currentVal_) != TREAT_VALUE_AS(uint32_t, defaultVal_));
            break;
        case FLAG_INT32:
            modified_ = (TREAT_VALUE_AS(int32_t, currentVal_) != TREAT_VALUE_AS(int32_t, defaultVal_));
            break;
        case FLAG_UINT64:
            modified_ = (TREAT_VALUE_AS(uint64_t, currentVal_) != TREAT_VALUE_AS(uint64_t, defaultVal_));
            break;
        case FLAG_INT64:
            modified_ = (TREAT_VALUE_AS(int64_t, currentVal_) != TREAT_VALUE_AS(int64_t, defaultVal_));
            break;
        case FLAG_STRING:
            modified_ = (TREAT_VALUE_AS(std::string, currentVal_) != TREAT_VALUE_AS(std::string, defaultVal_));
            break;
        default:
            break;
    }
}

namespace {
const char *kTrue[] = { "1", "t", "true", "y", "yes" };
const char *kFalse[] = { "0", "f", "false", "n", "no" };
}  // namespace

bool Flag::ParseAssignFromBoolean(const std::string &value, std::string &errMsg)
{
    static_assert(sizeof(kTrue) == sizeof(kFalse), "True false equal");
    if (value.empty()) {
        errMsg = IllegalValueMessage(value);
        return false;
    }
    bool res = false;
    bool success = false;
    for (size_t i = 0; i < sizeof(kTrue) / sizeof(*kTrue); ++i) {
        if (strcasecmp(value.c_str(), kTrue[i]) == 0) {
            res = true;
            success = true;
            break;
        } else if (strcasecmp(value.c_str(), kFalse[i]) == 0) {
            res = false;
            success = true;
            break;
        }
    }

    if (!success) {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    success = IsValidate(reinterpret_cast<const void *>(&res));
    if (success) {
        TREAT_VALUE_AS(bool, currentVal_) = res;
        UpdateModified();
    } else {
        errMsg = ValidateFailureMessage();
    }
    return success;
}

bool Flag::ParseAssignFromUint32(const std::string &value, std::string &errMsg)
{
    if (value.empty() || value[0] == '-') {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    uint32_t res;
    bool success = false;
    try {
        uint64_t res64 = StrToUnsignedLong(value);
        res = static_cast<uint32_t>(res64);
        success = static_cast<uint64_t>(res) == res64;
    } catch (std::invalid_argument &e) {
        success = false;
    } catch (const std::out_of_range &e) {
        success = false;
    }

    if (!success) {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    success = IsValidate(reinterpret_cast<const void *>(&res));
    if (success) {
        TREAT_VALUE_AS(uint32_t, currentVal_) = res;
        UpdateModified();
    } else {
        errMsg = ValidateFailureMessage();
    }
    return success;
}

bool Flag::ParseAssignFromInt32(const std::string &value, std::string &errMsg)
{
    if (value.empty()) {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    int32_t res;
    bool success = false;
    try {
        res = std::stoi(value);
        success = true;
    } catch (std::invalid_argument &e) {
        success = false;
    } catch (const std::out_of_range &e) {
        success = false;
    }

    if (!success) {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    success = IsValidate(reinterpret_cast<const void *>(&res));
    if (success) {
        TREAT_VALUE_AS(int32_t, currentVal_) = res;
        UpdateModified();
    } else {
        errMsg = ValidateFailureMessage();
    }
    return success;
}

bool Flag::ParseAssignFromUint64(const std::string &value, std::string &errMsg)
{
    if (value.empty() || value[0] == '-') {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    uint64_t res;
    bool success = false;
    try {
        res = StrToUnsignedLongLong(value);
        success = true;
    } catch (std::invalid_argument &e) {
        success = false;
    } catch (const std::out_of_range &e) {
        success = false;
    }

    if (!success) {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    success = IsValidate(reinterpret_cast<const void *>(&res));
    if (success) {
        TREAT_VALUE_AS(uint64_t, currentVal_) = res;
        UpdateModified();
    } else {
        errMsg = ValidateFailureMessage();
    }
    return success;
}

bool Flag::ParseAssignFromInt64(const std::string &value, std::string &errMsg)
{
    if (value.empty()) {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    int64_t res;
    bool success = false;
    try {
        res = std::stoll(value);
        success = true;
    } catch (std::invalid_argument &e) {
        success = false;
    } catch (const std::out_of_range &e) {
        success = false;
    }

    if (!success) {
        errMsg = IllegalValueMessage(value);
        return false;
    }

    success = IsValidate(reinterpret_cast<const void *>(&res));
    if (success) {
        TREAT_VALUE_AS(int64_t, currentVal_) = res;
        UpdateModified();
    } else {
        errMsg = ValidateFailureMessage();
    }
    return success;
}

bool Flag::ParseAssignFromString(const std::string &value, std::string &errMsg)
{
    bool success = IsValidate(static_cast<const void *>(&value));
    if (success) {
        TREAT_VALUE_AS(std::string, currentVal_) = value;
        UpdateModified();
    } else {
        errMsg = ValidateFailureMessage();
    }
    return success;
}

std::string Flag::IllegalValueMessage(const std::string &value) const
{
    return "Error: illegal value '" + value + "' specified for " + TypeName() + " flag '" + name_ + "' \n";
}

std::string Flag::ValidateFailureMessage() const
{
    return "Error: failed validation of new value '" + ValueString() + "' for flag '" + name_ + "'\n";
}

FlagManager *FlagManager::GetInstance()
{
    static FlagManager manager;
    return &manager;
}

bool FlagManager::ParseCommandLineFlags(const EmbeddedConfig &config, std::string &errMsg)
{
    for (const auto &flagKv : config.GetArgs()) {
        if (flagKv.first == "connectTimeoutMs") {
            continue;
        }
        auto it = flagMap_.find(flagKv.first);
        if (it == flagMap_.end()) {
            (void)unknownFlags_.emplace(flagKv.first, "ERROR: unknown command line flag '" + flagKv.first + "'\n");
            continue;
        }
        auto &flag = it->second;
        if (flagKv.second.empty()) {
            errorFlags_[flagKv.first] =
                "Error: flag '" + flagKv.first + "' is missing its argument; flag description: " + flag.meaning_ + "\n";
            continue;
        }
        (void)AssignFlagValue(flag, flagKv.second);
    }

    // 3. Validate the default value.
    ValidateDefaultFlagsLocked();

    // 4. Report error and exit if need.
    if (CheckAndReportErrors(errMsg)) {
        return false;
    }
    return true;
}

void FlagManager::ParseCommandLineFlags(int argc, char **argv)
{
    if (argc == 0 || argv == nullptr || *argv == nullptr) {
        ReportError("Illegal command line arguments");
        ugly_exit(1);
    }

    argv0_ = argv[0];
    std::lock_guard<std::mutex> l(mutex_);
    for (int i = 1; i < argc; ++i) {
        auto *arg = argv[i];

        // We always thought that
        if (arg[0] != '-' || arg[1] == '\0') {
            continue;
        }
        arg++;
        if (arg[0] == '-') {
            arg++;
        }
        if (arg[0] == '\0') {
            break;
        }

        std::string key;
        char *value = nullptr;
        ParseFlagValue(arg, key, &value);

        auto it = flagMap_.find(key);
        if (it == flagMap_.end()) {
            // Unknown flag, report error.
            (void)unknownFlags_.emplace(key, "ERROR: unknown command line flag '" + key + "'\n");
            continue;
        }

        auto &flag = it->second;
        if (value == nullptr) {
            if (i + 1 >= argc) {
                // Value absent, we treat it as an unrecoverable error.
                errorFlags_[key] =
                    "Error: flag '" + key + "' is missing its argument; flag description: " + flag.meaning_ + "\n";
                break;
            } else {
                value = argv[i + 1];
                ++i;
            }
        }

        std::string errMsg;
        (void)AssignFlagValue(flag, value);
    }

    // 2. If help or version flag is set, we need to output the help message and exit ugly.
    if (NeedPrintHelpfulMessage(argv[0])) {
        ugly_exit(1);
    }

    // 3. Validate the default value.
    ValidateDefaultFlagsLocked();

    // 4. Report error and exit if need.
    std::string unUseErrMsg;
    if (CheckAndReportErrors(unUseErrMsg)) {
        ugly_exit(1);
    }
}

std::string FlagManager::ProgramInvocationShortName()
{
    size_t pos = argv0_.rfind('/');
    return (pos == std::string::npos ? argv0_ : std::string(argv0_.c_str() + pos + 1));
}

void FlagManager::GetAllFlags(std::vector<FlagInfo> &output) const
{
    std::lock_guard<std::mutex> l(mutex_);
    for (const auto &entry : flagMap_) {
        const auto &flag = entry.second;
        FlagInfo info{ .type = flag.TypeName(),
                       .name = flag.name_,
                       .meaning = flag.meaning_,
                       .filename = flag.filename_,
                       .value = flag.ValueString(),
                       .isDefault = !flag.IsModified() };
        output.emplace_back(std::move(info));
    }
}

void FlagManager::SetVersionString(const std::string &version)
{
    version_ = version;
}

void FlagManager::SetUsageMessage(const std::string &description)
{
    description_ = description;
}

void FlagManager::RegisterFlag(const std::string &name, FlagType type, const std::string &meaning,
                               const std::string &filename, void *currentVal, void *defaultVal)
{
    Flag flag(type, name, meaning, filename, currentVal, defaultVal);
    std::lock_guard<std::mutex> l(mutex_);
    auto pair = flagMap_.emplace(name, flag);
    if (!pair.second) {
        const auto &existedFlag = pair.first->second;
        if (flag.filename_ != pair.first->second.filename_) {
            ReportError("ERROR: flag '%s' was defined more than once (inf file '%s' and '%s')\n", name.c_str(),
                        flag.filename_.c_str(), pair.first->second.filename_.c_str());
        } else {
            if (existedFlag.currentVal_ == currentVal) {
                return;
            }
            ReportError(
                "ERROR: register flag '%s' in file '%s' meets error. One possibility: file '%s' is being linked both "
                "staticallyand dynamically into this executable.\n",
                name.c_str(), flag.filename_.c_str(), flag.filename_.c_str());
        }
        ugly_exit(1);
    }
    (void)flagPtrMap_.emplace(currentVal, &pair.first->second);
}

bool FlagManager::RegisterValidator(void *flag, void *func)
{
    std::lock_guard<std::mutex> l(mutex_);
    auto it = flagPtrMap_.find(flag);
    if (it == flagPtrMap_.end()) {
        ReportError("WARNING: Ignore register validator for flag: no flag found\n");
        return false;
    } else if (func == it->second->validator_) {
        return true;
    } else if (func != nullptr && it->second->validator_ != nullptr) {
        ReportError("WARNING: Ignore register validator for flag: validator already registered\n");
        return false;
    } else {
        it->second->validator_ = func;
        if (it->second->currentVal_ != nullptr) {
            it->second->IsValidate(it->second->currentVal_);
        }
        return true;
    }
}

bool FlagManager::FindAndAssignFlagValue(const char *name, const std::string &value, std::string &errMsg)
{
    if (name == nullptr) {
        errMsg = "name or value is nullptr";
        return false;
    }

    std::lock_guard<std::mutex> l(mutex_);
    auto it = flagMap_.find(name);
    if (it == flagMap_.end()) {
        errMsg = "flag '" + std::string(name) + "' not found!";
        return false;
    }

    auto &flag = it->second;
    return flag.Assign(value, errMsg);
}

bool FlagManager::FindAndGetFlagValue(const char *name, std::string &output)
{
    if (name == nullptr) {
        return false;
    }

    std::lock_guard<std::mutex> l(mutex_);
    auto it = flagMap_.find(name);
    if (it == flagMap_.end()) {
        return false;
    }

    output = it->second.ValueString();
    return true;
}

bool FlagManager::NeedPrintHelpfulMessage(const char *argv0)
{
    if (!FLAGS_help && !FLAGS_version) {
        return false;
    }

    const char *basename = std::strrchr(argv0, '/');
    basename = basename == nullptr ? argv0 : basename + 1;
    std::stringstream ss;
    ss << std::string(basename) << ": " << description_;

    if (FLAGS_help) {
        ss << "\n";
        for (const auto &entry : flagMap_) {
            const auto &flag = entry.second;
            ss << "  -" << flag.name_ << " (" << flag.meaning_ << ")"
               << "\n   type: " << flag.TypeName() << " default: ";
            if (flag.TypeName() == "string") {
                ss << "\"" << flag.ValueString() << "\"\n";
            } else {
                ss << flag.ValueString() << "\n";
            }
        }
    } else if (FLAGS_version) {
        ss << " version: " << version_ << "\n";
    }
    ReportError("%s", ss.str().c_str());
    return true;
}

void FlagManager::ValidateDefaultFlagsLocked()
{
    for (auto it = flagMap_.begin(); it != flagMap_.end(); ++it) {
        auto &flag = it->second;
        if (flag.IsModified()) {
            continue;
        }
        if (!errorFlags_[flag.name_].empty()) {
            continue;
        }
        if (flag.IsValidate()) {
            continue;
        }
        errorFlags_[flag.name_] = "Error: failed validation of default value for flag '" + flag.name_ + "'\n";
    }
}

void FlagManager::ParseFlagValue(char *arg, std::string &flag, char **value)
{
    auto *res = std::strchr(arg, '=');
    if (res == nullptr) {
        flag = arg;
    } else {
        flag.assign(arg, res - arg);
        *value = res + 1;
    }

    // If we found help flag, it means that we need to print the
    // help message, so let's set it as true to let it work.
    if (flag == "help" || flag == "version") {
        *value = one;
    }
}

void FlagManager::AssignFlagValue(Flag &flag, const std::string &value)
{
    std::string errMsg;
    bool success = flag.Assign(value, errMsg);
    if (!success) {
        errorFlags_[flag.name_] =
            "Error: illegal value '" + value + "' specified for " + flag.TypeName() + " flag '" + flag.name_ + "' \n";
    }
}

bool FlagManager::CheckAndReportErrors(std::string &errMsg) const
{
    bool error = false;
    std::stringstream ss;
    for (const auto &entry : errorFlags_) {
        if (!entry.second.empty()) {
            ss << entry.second;
            error = true;
        }
    }

    for (const auto &entry : unknownFlags_) {
        if (!entry.second.empty()) {
            ss << entry.second;
            error = true;
        }
    }
    errMsg = ss.str();
    if (error) {
        ReportError("%s", ss.str().c_str());
    }
    return error;
}
}  // namespace datasystem