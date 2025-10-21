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
#ifndef DATASYSTEM_COMMON_FLAGS_FLAG_MANAGER_H
#define DATASYSTEM_COMMON_FLAGS_FLAG_MANAGER_H

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "datasystem/common/flags/flags.h"

namespace datasystem {
/**
 * @brief Report error.
 * @param[in] format Error message format.
 */
void ReportError(const char *format, ...);

class Flag {
public:
    Flag(FlagType type, const std::string &name, const std::string meaning, const std::string &filename, void *curr,
         void *def)
        : type_(type),
          name_(name),
          meaning_(meaning),
          filename_(filename),
          currentVal_(curr),
          defaultVal_(def),
          modified_(false),
          validator_(nullptr)
    {
    }

    ~Flag() = default;

    /**
     * @brief Assign value to flag.
     * @param[in] value Value to be parsed and assigned.
     * @param[out] errMsg Error message would be fill if error happen.
     * @return True if assign success.
     */
    bool Assign(const std::string &value, std::string &errMsg);

    /**
     * @brief Check if flag value is validate via validator function.
     * @param[in] vlaue Check value.
     * @return True if value is validate.
     */
    bool IsValidate(const void *value = nullptr) const;

    /**
     * @brief Return type name.
     * @return Type name.
     */
    std::string TypeName() const;

    /**
     * @brief Update modified flag.
     */
    void UpdateModified();

    /**
     * @brief Check if flag value is modified.
     * @return True if flag value is modified.
     */
    bool IsModified() const
    {
        return modified_;
    }

    /**
     * @brief Get value.
     * @return Value string.
     */
    std::string ValueString() const;

private:
    friend class FlagManager;

    /**
     * @brief Parse and assign value from boolean value.
     * @param[in] value Value string.
     * @param[out] Error message would be fill if error happen.
     * @return True if assign success.
     */
    bool ParseAssignFromBoolean(const std::string &value, std::string &errMsg);

    /**
     * @brief Parse and assign value from uint32 value.
     * @param[in] value Value string.
     * @param[out] Error message would be fill if error happen.
     * @return True if assign success.
     */
    bool ParseAssignFromUint32(const std::string &value, std::string &errMsg);

    /**
     * @brief Parse and assign value from int32 value.
     * @param[in] value Value string.
     * @param[out] Error message would be fill if error happen.
     * @return True if assign success.
     */
    bool ParseAssignFromInt32(const std::string &value, std::string &errMsg);

    /**
     * @brief Parse and assign value from uint64 value.
     * @param[in] value Value string.
     * @param[out] Error message would be fill if error happen.
     * @return True if assign success.
     */
    bool ParseAssignFromUint64(const std::string &value, std::string &errMsg);

    /**
     * @brief Parse and assign value from int64 value.
     * @param[in] value Value string.
     * @param[out] Error message would be fill if error happen.
     * @return True if assign success.
     */
    bool ParseAssignFromInt64(const std::string &value, std::string &errMsg);

    /**
     * @brief Parse and assign value from string value.
     * @param[in] value Value string.
     * @param[out] Error message would be fill if error happen.
     * @return True if assign success.
     */
    bool ParseAssignFromString(const std::string &value, std::string &errMsg);

    /**
     * @brief Return illegal value message.
     * @param[in] value Value string.
     * @return Illegal value message.
     */
    std::string IllegalValueMessage(const std::string &value) const;

    /**
     * @brief Return invalid value message.
     * @param[in] value Value string.
     * @return Invalid value message.
     */
    std::string ValidateFailureMessage() const;

    // Flag type: boolean/uint32/int32/uint64/int64/string.
    FlagType type_;

    // Flag name like 'help'
    std::string name_;

    // Flag description.
    std::string meaning_;

    // Flag defined filename.
    std::string filename_;

    // Flag current value memory address.
    void *currentVal_;

    // Flag default value memory address.
    void *defaultVal_;

    // Indicate flag value is modified or not.
    bool modified_;

    // Flag validator function.
    void *validator_;
};

class FlagManager {
public:
    ~FlagManager() = default;

    /**
     * @brief Get FlagManager single instance.
     * @return FlagManager instance.
     */
    static FlagManager *GetInstance();

    /**
     * @brief Looks for flags in argv and parses them.
     * @param[in] argc Command line argument counts.
     * @param[in] argv Command line argument values.
     */
    void ParseCommandLineFlags(int argc, char **argv);

    /**
     * @brief Get program invocation short name.
     * @return Program invocation short name.
     */
    std::string ProgramInvocationShortName();

    /**
     * @brief Register flag before main() execute.
     * @param[in] name Flag name.
     * @param[in] type Flag type.
     * @param[in] meaning Flag description.
     * @param[in] filename Flag defined filename.
     * @param[in] currentVal Flag current value memory address.
     * @param[in] defaultVal Flag default value memory address.
     */
    void RegisterFlag(const std::string &name, FlagType type, const std::string &meaning, const std::string &filename,
                      void *currentVal, void *defaultVal);

    /**
     * @brief Register flag validator function before main() execute.
     * @param[in] flag Flag current value memory address.
     * @param[in] func Validator function.
     */
    bool RegisterValidator(void *flag, void *func);

    /**
     * @brief Find flag from flag map and try to assign value.
     * @param[in] name Flag name.
     * @param[in] value Need to be assigned value.
     * @param[out] errMsg Error message if find or assign failed.
     * @return True if find and assign success.
     */
    bool FindAndAssignFlagValue(const char *name, const std::string &value, std::string &errMsg);

    /**
     * @brief Find flag from flag map and get its value.
     * @param[in] name Flag name.
     * @param[out] output Flag value.
     * @return True if flag found.
     */
    bool FindAndGetFlagValue(const char *name, std::string &output);

    /**
     * @brief Get all flags from flag map.
     * @param[out] output Flag info list.
     */
    void GetAllFlags(std::vector<FlagInfo> &output) const;

    /**
     * @brief Set version.
     * @param[in] version Version message.
     */
    void SetVersionString(const std::string &version);

    /**
     * @brief Set usage description.
     * @param[in] description Usage description.
     */
    void SetUsageMessage(const std::string &description);

private:
    FlagManager() = default;

    /**
     * @brief Check if need print helpful message or not.
     * @param[in] argv0 Program name.
     * @return True if need print helpful message.
     */
    bool NeedPrintHelpfulMessage(const char *argv0);

    /**
     * @brief Validate default flags.
     */
    void ValidateDefaultFlagsLocked();

    /**
     * @brief Parse flag value.
     * @param[in] arg Argument get from command line.
     * @param[in] flag Flag name.
     * @param[out] value Parsed value, null if value is not contained in arg.
     */
    void ParseFlagValue(char *arg, std::string &flag, char **value);

    /**
     * @brief Assign flag value.
     * @param[in] flag Flag instance.
     * @param[in] value Value need to be assigned.
     */
    void AssignFlagValue(Flag &flag, const std::string &value);

    /**
     * @brief Check if need report error.
     * @return Ture if need to report error.
     */
    bool CheckAndReportErrors() const;

    std::map<std::string, Flag> flagMap_;

    std::map<void *, Flag *> flagPtrMap_;

    std::map<std::string, std::string> errorFlags_;

    std::map<std::string, std::string> unknownFlags_;

    std::string version_;

    std::string description_;

    std::string argv0_{ "UNKNOWN" };

    // Protect the variables above.
    mutable std::mutex mutex_;
};
}  // namespace datasystem

#endif