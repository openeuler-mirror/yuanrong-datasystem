/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: JSON-based dynamic configuration updater.
 */
#ifndef DATASYSTEM_COMMON_UTIL_GFLAG_DYNAMIC_CONFIG_UPDATER_H
#define DATASYSTEM_COMMON_UTIL_GFLAG_DYNAMIC_CONFIG_UPDATER_H

#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/common/util/gflag/flags.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class DynamicConfigUpdater {
public:
    explicit DynamicConfigUpdater(Flags &flags);
    ~DynamicConfigUpdater() = default;

    DynamicConfigUpdater(const DynamicConfigUpdater &) = delete;
    DynamicConfigUpdater &operator=(const DynamicConfigUpdater &) = delete;
    DynamicConfigUpdater(DynamicConfigUpdater &&) = delete;
    DynamicConfigUpdater &operator=(DynamicConfigUpdater &&) = delete;

    /**
     * @brief Apply JSON config updates to modifiable flags.
     * @param[in] configJson JSON object mapping flag names to string values.
     * @param[in] apiPrefix Error message prefix, e.g. "UpdateConfig".
     * @return Status::OK() on success; aggregated errors otherwise.
     */
    Status ApplyJson(const std::string &configJson, const std::string &apiPrefix = "UpdateConfig");

private:
    bool ParseJsonToMap(const std::string &json, const std::string &apiPrefix,
                        std::unordered_map<std::string, std::string> &out, std::string &errMsg) const;

    Flags *flags_;
    std::mutex mutex_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_GFLAG_DYNAMIC_CONFIG_UPDATER_H
