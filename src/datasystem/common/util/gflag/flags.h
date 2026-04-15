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
#ifndef DATASYSTEM_COMMON_UTIL_FLAGS_H
#define DATASYSTEM_COMMON_UTIL_FLAGS_H

#include <chrono>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
using GFlagsMap = std::unordered_map<std::string, FlagInfo>;
class Flags {
public:
    Flags() : preConfigCheckTime_(clock::now())
    {
    }

    ~Flags() = default;

    /**
     * @brief Obtains all gflag parameters and converts them to maps.
     * @return GFlagsMap - Map containing all flags.
     */
    GFlagsMap GetAllFlagsToMap();

    /**
     * @brief Obtains the non-default flag.
     * @param[in] defaultFlags Default values of all flags.
     * @return std::string - Strings containing all non-default values.
     */
    std::string GetNonDefaultFlags(const GFlagsMap &defaultFlags);

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
     * @brief Process the flagfile and return the valid flag parameter in the map format.
     * @param[in] configFilePath The path of the worker configuration file.
     * @return std::unordered_map<std::string, std::string> - <key:flagName, value:flagValue>
     */
    std::unordered_map<std::string, std::string> ProcessFlagFile(const std::string &configFilePath);

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
     * @brief Check whether the flag parameter needs to be processed.
     * @param[in] flagMap The flag parameter and value parsed from the configuration file
     * @param[in] flagName The flag parameter name to be judged
     * @return bool - If the flag parameter needs to be processed, true is returned. Otherwise, false is returned.
     */
    bool IsToHandle(const std::unordered_map<std::string, std::string> &flagMap, const std::string &flagName);

    /**
     * @brief Dynamically update the value of the flag parameter.
     * @param[in] flagMap The flag parameter and value parsed from the configuration file
     */
    void UpdateFlagParameter(const std::unordered_map<std::string, std::string> &flagMap);

    /**
     * @brief Cut off all spaces before and after a string.
     * @param[in/out] value Value to trim.
     */
    void TrimSpace(std::string &value);

    /**
     * @brief Set handler to isToHandle.
     * @param[in] handler Value to set.
     */
    void SetIsToHandle(
        std::function<bool(const std::unordered_map<std::string, std::string> &, const std::string &)> handler);

    /**
     * @brief  Set handler to validateSpecial.
     * @param[in] validator  Value to set.
     */
    void SetValidateSpecial(const std::function<bool(const std::string &, const std::string &)> &validator);

    /**
     * @brief Validate some special cases
     * @param[in] flagName Flag name.
     * @param[in] newVal New value assigned to the flag.
     */
    bool ValidateSpecial(const std::string &flagName, const std::string &newVal);

private:
    using clock = std::chrono::steady_clock;

    const std::unordered_set<std::string> excludeGflags_{ "system_access_key", "system_secret_key",
                                                          "system_data_key",   "sc_encrypt_secret_key",
                                                          "tenant_access_key", "tenant_secret_key",
                                                          "obs_access_key",    "obs_secret_key" };

    // Only the flag parameter in the flagNameTrustList can be dynamically modified.
    const std::unordered_set<std::string> flagNameTrustList_{ "v",
                                                              "log_async_queue_size",
                                                              "log_compress",
                                                              "log_rate_limit",
                                                              "max_log_file_num",
                                                              "arena_per_tenant",
                                                              "node_dead_timeout_s",
                                                              "client_reconnect_wait_s",
                                                              "spill_file_max_size_mb",
                                                              "spill_file_open_limit",
                                                              "spill_size_limit",
                                                              "heartbeat_interval_ms",
                                                              "add_node_wait_time_s",
                                                              "async_delete",
                                                              "auto_del_dead_node",
                                                              "cross_cluster_get_data_from_worker",
                                                              "enable_hash_ring_self_healing",
                                                              "shared_disk_arena_per_tenant",
                                                              "enable_lossless_data_exit_mode",
#ifdef WITH_TESTS
                                                              "inject_actions"
#endif
    };

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
};
};      // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_FLAGS_H
