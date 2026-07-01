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
 * Description: Runtime dynamic flag configuration.
 */
#ifndef DATASYSTEM_COMMON_FLAGS_DYNAMIC_FLAG_CONFIG_H
#define DATASYSTEM_COMMON_FLAGS_DYNAMIC_FLAG_CONFIG_H

#include <chrono>
#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
using FlagInfoMap = std::unordered_map<std::string, FlagInfo>;
class DynamicFlagConfig {
public:
    DynamicFlagConfig() : preConfigCheckTime_(clock::now())
    {
        // Default grouped-flag commit handler: built-in log sampler transactional commit.
        // Callers may override via SetBatchCommitHandler to decouple sampler (or other grouped-flag) semantics.
        batchCommitHandler_ = [this](const std::unordered_map<std::string, std::string> &flagMap) {
            return ValidateAndCommitSamplerFlags(flagMap);
        };
    }

    ~DynamicFlagConfig() = default;

    /**
     * @brief Obtains all gflag parameters and converts them to maps.
     * @return FlagInfoMap - Map containing all flags.
     */
    FlagInfoMap GetAllFlagsToMap();

    /**
     * @brief Obtains the non-default flag.
     * @param[in] defaultFlags Default values of all flags.
     * @return std::string - Strings containing all non-default values.
     */
    std::string GetNonDefaultFlags(const FlagInfoMap &defaultFlags);

    /**
     * @brief Obtains flags explicitly declared through commandline or flags changed at runtime.
     * @return std::string -Strings containing all explicitly declared flags and non default commandline.
     */
    std::string GetExplicitDeclaredFlags();

    /**
     * @brief Obtains all flags.
     * @return std::string -Strings containing all flags.
     */
    std::string GetAllFlagsStr();

    /**
     * @brief Erase sensitive information.
     * @param[in] argc The count of cmdline arguments.
     * @param[in] argv The value of cmdline arguments.
     * @return @return K_OK on success; the error code otherwise.
     */
    Status EraseInfo(int argc, char **argv);

    /**
     * @brief Monitors the worker configuration file and dynamically updates the flag parameter value.
     * @param[in] configFilePath The path of the worker configuration file.
     */
    void MonitorConfigFile(const std::string &configFilePath);

    /**
     * @brief Start to process the worker configuration file and dynamically updates the flag parameter value.
     * @param[in] configFilePath The path of the worker configuration file.
     * @param[in] nowTime Current system timestamp
     */
    void StartConfigFileHandle(const std::string &configFilePath,
                               std::chrono::time_point<std::chrono::steady_clock> nowTime);

    /**
     * @brief Find the position of the first occurrence of "\n" "\r" "\t" or " " in a string.
     * @param[in] content The string to be found.
     * @return std::string::size_type Returns the character index, or npos if not found.
     */
    std::string::size_type FindFirstSeparator(const std::string &content);

    /**
     * @brief Processes the content of the configfile and extracts the flag parameter that meets the conditions.
     * @note Valid flag parameter:
     *       1、A -flag=value or --flag=value. Commands must be separated by spaces or line breaks.
     *       2、If the same flag parameter name exists, the first flag parameter is used.
     * @param[in] flagFileContext Contents of the configuration file.
     * @return std::unordered_map<std::string, std::string> - Contains valid flag parameters and values parsed from the
     * configuration file.
     */
    std::unordered_map<std::string, std::string> ProcessOptions(const std::string &flagFileContext);

    /**
     * @brief Split the flagName and value from the flag command and check whether the flagName supports dynamic update.
     * @param[in] flagCommand The flag command.
     * @param[out] nameAndVal The flagName and value split from the flag command
     * @return bool - Indicates whether the flagName obtained from the command string can be dynamically modified.
     */
    bool SplitArgument(const char *flagCommand, std::pair<std::string, std::string> &nameAndVal);

    /**
     * @brief Check whether the flag parameter can be dynamically modified.
     * @param[in] flagName The name of flag parameter.
     * @return bool - If the parameter supports dynamic modification, true is returned. Otherwise, false is returned.
     */
    bool ValidateFlagName(const std::string &flagName);

    /**
     * @brief Dynamically update the value of the flag parameter.
     * @param[in] flagMap The flag parameter and value parsed from the configuration file
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateFlagParameter(const std::unordered_map<std::string, std::string> &flagMap);

    /**
     * @brief Cut off all spaces before and after a string.
     * @param[in/out] value Value to trim.
     */
    void TrimSpace(std::string &value);

    /**
     * @brief  Set handler to validateSpecial.
     * @param[in] validator  Value to set.
     */
    void SetValidateSpecial(const std::function<bool(const std::string &, const std::string &)> &validator);

    /**
     * @brief Dry-run special validation for one pending dynamic update.
     * @param[in] flagMap Pending flag updates.
     * @param[in] flagName Flag name to validate.
     * @param[in] newVal Proposed new value.
     * @param[out] errMsg Error message when validation fails.
     * @return True when special constraints pass; false otherwise.
     */
    bool ValidateSpecialConstraint(const std::unordered_map<std::string, std::string> &flagMap,
                                   const std::string &flagName, const std::string &newVal,
                                   std::string &errMsg);

    /**
     * @brief Inject a handler that transactionally commits a group of related flags.
     * @details When set, UpdateFlagParameter delegates grouped-flag commit (e.g. log sampler sample-rate flags)
     *          to this handler instead of the built-in sampler logic. Pass nullptr to restore the default
     *          built-in handler. The handler receives the full pending flag map and returns true on success.
     * @param[in] handler Handler that commits a related flag group transactionally, or nullptr to reset.
     */
    void SetBatchCommitHandler(
        std::function<bool(const std::unordered_map<std::string, std::string> &)> handler);

private:
    using clock = std::chrono::steady_clock;

    const std::unordered_set<std::string> excludeGflags_{ "system_access_key", "system_secret_key",
                                                          "system_data_key",   "sc_encrypt_secret_key",
                                                          "tenant_access_key", "tenant_secret_key",
                                                          "obs_access_key",    "obs_secret_key" };

    // Setting the interval for checking the configuration file. (Default: 10s)
    const uint64_t TRIGGER_CONFIG_CHECK_NANO_INTERVAL = 10UL * 1000UL * 1000UL * 1000UL;

    // Set the size limit of the configuration file (Default: 1 MB). If the size exceeds the limit, the configuration
    // file will not be processed to prevent log explosion caused by malicious modification.
    const uint64_t FILE_SIZE_MAX_LIMIT = 1 * 1024 * 1024;

    // The last modification time of the configuration file.
    int64_t lastModifiedTime_ = -1;

    // Record the last time to check the configuration update.
    std::chrono::time_point<clock> preConfigCheckTime_;

    std::function<bool(const std::unordered_map<std::string, std::string> &, const std::string &)> isToHandle_;
    std::function<bool(const std::string &, const std::string &)> validateSpecial_;
    std::function<bool(const std::unordered_map<std::string, std::string> &)> batchCommitHandler_;

    /// @brief Process the flagfile and return the valid flag parameter in the map format.
    /// @param[in] configFilePath The path of the worker configuration file.
    /// @return Map of <flagName, flagValue> parsed from the configuration file.
    std::unordered_map<std::string, std::string> ProcessFlagFile(const std::string &configFilePath);

    /// @brief Check whether the flag parameter needs to be processed.
    /// @param[in] flagMap The flag parameters parsed from the configuration file.
    /// @param[in] flagName The flag parameter name to be judged.
    /// @return True if the flag parameter needs to be processed.
    bool IsToHandle(const std::unordered_map<std::string, std::string> &flagMap, const std::string &flagName);

    /// @brief Set the handler that decides whether a flag parameter needs to be processed.
    /// @param[in] handler Handler to set.
    void SetIsToHandle(
        std::function<bool(const std::unordered_map<std::string, std::string> &, const std::string &)> handler);

    /// @brief Validate a flag value against the injected special-constraint handler.
    /// @param[in] flagName Flag name.
    /// @param[in] newVal New value assigned to the flag.
    /// @return True if the special validation passes.
    bool ValidateSpecial(const std::string &flagName, const std::string &newVal);

    Status UpdateSingleFlag(const std::unordered_map<std::string, std::string> &flagMap,
                            const std::string &flagName, const std::string &newVal);

    void RollbackFlagValues(const std::unordered_map<std::string, std::string> &prevVals);

    bool ValidateAndCommitSamplerFlags(const std::unordered_map<std::string, std::string> &flagMap);

    bool CommitSamplerFlagsTransaction(
        const std::unordered_map<std::string, std::string> &candidates, const LogSampleUserConfig &cfg);
};
};      // namespace datasystem
#endif  // DATASYSTEM_COMMON_FLAGS_DYNAMIC_FLAG_CONFIG_H
