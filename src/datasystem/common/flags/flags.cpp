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

#include "datasystem/common/flags/flags.h"

#include <cstdlib>

#include "datasystem/common/flags/flag_manager.h"

DS_DEFINE_bool(help, false, "show help on all flags");
DS_DEFINE_bool(version, false, "show version and build info and exit");

namespace datasystem {

void ParseCommandLineFlags(int argc, char **argv)
{
    FlagManager::GetInstance()->ParseCommandLineFlags(argc, argv);
}

bool ParseCommandLineFlags(const EmbeddedConfig &config, std::string &errMsg)
{
    return FlagManager::GetInstance()->ParseCommandLineFlags(config, errMsg);
}

std::string ProgramInvocationShortName()
{
    return FlagManager::GetInstance()->ProgramInvocationShortName();
}

void GetAllFlags(std::vector<FlagInfo> &output)
{
    return FlagManager::GetInstance()->GetAllFlags(output);
}

bool SetCommandLineOption(const char *name, const std::string &value, std::string &errMsg)
{
    return FlagManager::GetInstance()->FindAndAssignFlagValue(name, value, errMsg);
}

bool GetCommandLineOption(const char *name, std::string &output)
{
    return FlagManager::GetInstance()->FindAndGetFlagValue(name, output);
}

void SetVersionString(const std::string &version)
{
    FlagManager::GetInstance()->SetVersionString(version);
}

void SetUsageMessage(const std::string &description)
{
    FlagManager::GetInstance()->SetUsageMessage(description);
}

bool RegisterValidator(void *flag, void *func)
{
    return FlagManager::GetInstance()->RegisterValidator(flag, func);
}

FlagRegisterHelper::FlagRegisterHelper(const std::string &name, FlagType type, const std::string &meaning,
                                       const std::string &filename, void *currentVal, void *defaultVal)
{
    FlagManager::GetInstance()->RegisterFlag(name, type, meaning, filename, currentVal, defaultVal);
}

/**
 * @brief Get value from environment variable.
 * @param[in] env Environemnt variable.
 * @param[in] defValue Default value if environemnt variable no exist or invalidate.
 * @param[in] type Flag type.
 * @return Value parsed from environemnt variable, if parsed failed, default value is returned.
 */
template<typename Type>
Type GetFromEnv(const char *env, Type defValue, FlagType type)
{
    if (env == nullptr) {
        return defValue;
    }
    
    const char *value = ::getenv(env);
    if (value == nullptr) {
        return defValue;
    }

    Type result;
    Flag flag(type, "", "", "", reinterpret_cast<void *>(&result), reinterpret_cast<void *>(&defValue));
    std::string errMsg;
    return flag.Assign(std::string(value), errMsg) ? result : defValue;
}

bool GetBoolFromEnv(const char *env, bool defValue)
{
    return GetFromEnv(env, defValue, FLAG_BOOL);
}

uint32_t GetUint32FromEnv(const char *env, uint32_t defValue)
{
    return GetFromEnv(env, defValue, FLAG_UINT32);
}

int32_t GetInt32FromEnv(const char *env, int32_t defValue)
{
    return GetFromEnv(env, defValue, FLAG_INT32);
}

uint64_t GetUint64FromEnv(const char *env, uint64_t defValue)
{
    return GetFromEnv(env, defValue, FLAG_UINT64);
}

int64_t GetInt64FromEnv(const char *env, int64_t defValue)
{
    return GetFromEnv(env, defValue, FLAG_INT64);
}

std::string GetStringFromEnv(const char *env, const std::string &defValue)
{
    if (env == nullptr) {
        return defValue;
    }
    const char *value = ::getenv(env);
    return value != nullptr ? std::string(value) : defValue;
}
}  // namespace datasystem
