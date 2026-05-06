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

/**
 * Description: The flag used by command line.
 */
#ifndef DATASYSTEM_COMMON_FLAGS_FLAGS_H
#define DATASYSTEM_COMMON_FLAGS_FLAGS_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "datasystem/utils/embedded_config.h"

// clang-format off
#define DS_DEFINE_TYPE(name, defvalue, meaning, type, shorttype, enumtype)                                 \
    namespace datasystem {                                                                                 \
    namespace fl##shorttype {                                                                              \
        static const type FLAGS_def##name = defvalue;                                                      \
        static type FLAGS_no##name = FLAGS_def##name;                                                      \
        type FLAGS_##name = FLAGS_def##name;                                                               \
        static FlagRegisterHelper helper_##name(#name, enumtype, meaning, __FILE__, (void *)&FLAGS_##name, \
                                                (void *)&FLAGS_no##name);                                  \
    }                                                                                                      \
    }                                                                                                      \
    using ::datasystem::fl##shorttype::FLAGS_##name

#define DS_DECLARE_TYPE(name, type, shorttype) \
    namespace datasystem {                     \
    namespace fl##shorttype {                  \
        extern type FLAGS_##name;              \
    }                                          \
    }                                          \
    using ::datasystem::fl##shorttype::FLAGS_##name

#define DS_DEFINE_bool(name, defvalue, meaning) \
    DS_DEFINE_TYPE(name, defvalue, meaning, bool, B, FLAG_BOOL)

#define DS_DECLARE_bool(name) \
    DS_DECLARE_TYPE(name, bool, B)

#define DS_DEFINE_int32(name, defvalue, meaning) \
    DS_DEFINE_TYPE(name, defvalue, meaning, int32_t, I32, FLAG_INT32)

#define DS_DECLARE_int32(name) \
    DS_DECLARE_TYPE(name, int32_t, I32)

#define DS_DEFINE_uint32(name, defvalue, meaning) \
    DS_DEFINE_TYPE(name, defvalue, meaning, uint32_t, U32, FLAG_UINT32)

#define DS_DECLARE_uint32(name) \
    DS_DECLARE_TYPE(name, uint32_t, U32)

#define DS_DEFINE_int64(name, defvalue, meaning) \
    DS_DEFINE_TYPE(name, defvalue, meaning, int64_t, I64, FLAG_INT64)

#define DS_DECLARE_int64(name) \
    DS_DECLARE_TYPE(name, int64_t, I64)

#define DS_DEFINE_uint64(name, defvalue, meaning) \
    DS_DEFINE_TYPE(name, defvalue, meaning, uint64_t, U64, FLAG_UINT64)

#define DS_DECLARE_uint64(name) \
    DS_DECLARE_TYPE(name, uint64_t, U64)

#define DS_DEFINE_string(name, defvalue, meaning) \
    DS_DEFINE_TYPE(name, defvalue, meaning, std::string, S, FLAG_STRING)

#define DS_DECLARE_string(name) \
    DS_DECLARE_TYPE(name, std::string, S)

// Convenience macro for the registration of a flag validator
#define DS_DEFINE_validator(name, func) \
    static const bool is_##name = ::datasystem::RegisterValidator(&FLAGS_##name, func)

#define DS_DEFINE_environment(name, env_var)

// clang-format on

namespace datasystem {
struct FlagInfo {
    // Flag type: boolean/uint32/int32/uint64/int64/string.
    std::string type;

    // Flag name.
    std::string name;

    // Flag description.
    std::string meaning;

    // Flag defined filename.
    std::string filename;

    // Flag value.
    std::string value;

    // Indicate flag value is default or not.
    bool isDefault;
};

/**
 * @brief Looks for flags in argv and parses them.
 * @param[in] argc Command line argument counts.
 * @param[in] argv Command line argument values.
 */
void ParseCommandLineFlags(int argc, char **argv);

/**
 * @brief Looks for flags in argv and parses them.
 * @param[in] config worker configs.
 * @param[in] errMsg errMsg if prase failed.
 */
bool ParseCommandLineFlags(const EmbeddedConfig &config, std::string &errMsg);

/**
 * @brief Get program invocation short name.
 * @return Program invocation short name.
 */
std::string ProgramInvocationShortName();

/**
 * @brief Get all flags.
 * @param[out] output Output flag informations.
 */
void GetAllFlags(std::vector<FlagInfo> &output);

/**
 * @brief Set command line options, it is thread safe.
 * @param[in] name Flag name.
 * @param[in] value Flag value.
 * @param[in] errMsg Error message.
 * @return True if set success.
 */
bool SetCommandLineOption(const char *name, const std::string &value, std::string &errMsg);

/**
 * @brief Get comand line options, it is thread safe.
 * @param[in] name Flag name.
 * @param[out] output Flag value.
 * @return True if get success.
 */
bool GetCommandLineOption(const char *name, std::string &output);

/**
 * @brief Set executable version, it would be show in help message.
 * @param[in] version Version message.
 */
void SetVersionString(const std::string &version);

/**
 * @brief Set executable usage message, it would be show in help message.
 * @param[in] description Usage message.
 */
void SetUsageMessage(const std::string &description);

/**
 * @brief Get bool value from environment variable.
 * @param[in] env Environment variable.
 * @param[in] defValue Default value if environment variable is null or error.
 * @return Boolean value.
 */
bool GetBoolFromEnv(const char *env, bool defValue);

/**
 * @brief Get uint32 value from environment variable.
 * @param[in] env Environment variable.
 * @param[in] defValue Default value if environment variable is null or error.
 * @return Uint32 value.
 */
uint32_t GetUint32FromEnv(const char *env, uint32_t defValue);

/**
 * @brief Get int32 value from environment variable.
 * @param[in] env Environment variable.
 * @param[in] defValue Default value if environment variable is null or error.
 * @return Int32 value.
 */
int32_t GetInt32FromEnv(const char *env, int32_t defValue);

/**
 * @brief Get uint64 value from environment variable.
 * @param[in] env Environment variable.
 * @param[in] defValue Default value if environment variable is null or error.
 * @return Uint64 value.
 */
uint64_t GetUint64FromEnv(const char *env, uint64_t defValue);

/**
 * @brief Get int64 value from environment variable.
 * @param[in] env Environment variable.
 * @param[in] defValue Default value if environment variable is null or error.
 * @return Int64 value.
 */
int64_t GetInt64FromEnv(const char *env, int64_t defValue);

/**
 * @brief Get string value from environment variable.
 * @param[in] env Environment variable.
 * @param[in] defValue Default value if environment variable is null or error.
 * @return String value.
 */
std::string GetStringFromEnv(const char *env, const std::string &defValue);

/**
 * @brief Register flag validator function before main() execute.
 * @param[in] flag Flag current value memory address.
 * @param[in] func Validator function.
 */
bool RegisterValidator(void *flag, void *func);

/**
 * @brief Register boolean type flag validator function before main() execute.
 * @param[in] flag Flag current value memory address.
 * @param[in] func Validator function.
 */
inline bool RegisterValidator(bool *flag, bool (*func)(const char *, bool))
{
    return RegisterValidator(flag, reinterpret_cast<void *>(func));
}

/**
 * @brief Register int32 type flag validator function before main() execute.
 * @param[in] flag Flag current value memory address.
 * @param[in] func Validator function.
 */
inline bool RegisterValidator(int32_t *flag, bool (*func)(const char *, int32_t))
{
    return RegisterValidator(flag, reinterpret_cast<void *>(func));
}

/**
 * @brief Register uint32 type flag validator function before main() execute.
 * @param[in] flag Flag current value memory address.
 * @param[in] func Validator function.
 */
inline bool RegisterValidator(uint32_t *flag, bool (*func)(const char *, uint32_t))
{
    return RegisterValidator(flag, reinterpret_cast<void *>(func));
}

/**
 * @brief Register uint64 type flag validator function before main() execute.
 * @param[in] flag Flag current value memory address.
 * @param[in] func Validator function.
 */
inline bool RegisterValidator(uint64_t *flag, bool (*func)(const char *, uint64_t))
{
    return RegisterValidator(flag, reinterpret_cast<void *>(func));
}

/**
 * @brief Register int64 type flag validator function before main() execute.
 * @param[in] flag Flag current value memory address.
 * @param[in] func Validator function.
 */
inline bool RegisterValidator(int64_t *flag, bool (*func)(const char *, int64_t))
{
    return RegisterValidator(flag, reinterpret_cast<void *>(func));
}

/**
 * @brief Register string type flag validator function before main() execute.
 * @param[in] flag Flag current value memory address.
 * @param[in] func Validator function.
 */
inline bool RegisterValidator(std::string *flag, bool (*func)(const char *, const std::string &))
{
    return RegisterValidator(flag, reinterpret_cast<void *>(func));
}

enum FlagType {
    FLAG_BOOL = 0,
    FLAG_UINT32,
    FLAG_INT32,
    FLAG_UINT64,
    FLAG_INT64,
    FLAG_STRING
};

class FlagRegisterHelper {
public:
    FlagRegisterHelper(const std::string &name, FlagType type, const std::string &meaning, const std::string &filename,
                       void *currentVal, void *defaultVal);
};
}  // namespace datasystem

#endif
