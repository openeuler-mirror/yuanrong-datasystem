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
#include "datasystem/common/util/gflag/dynamic_config_updater.h"

#include <sstream>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "datasystem/common/flags/flag_manager.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/util/gflag/config_monitor_state.h"

namespace datasystem {
namespace {

std::string JoinErrors(const std::vector<std::string> &errors)
{
    std::ostringstream oss;
    for (size_t i = 0; i < errors.size(); ++i) {
        if (i > 0) {
            oss << "; ";
        }
        oss << errors[i];
    }
    return oss.str();
}

}  // namespace

DynamicConfigUpdater::DynamicConfigUpdater(Flags &flags) : flags_(&flags)
{
}

bool DynamicConfigUpdater::ParseJsonToMap(const std::string &json, const std::string &apiPrefix,
                                          std::unordered_map<std::string, std::string> &out,
                                          std::string &errMsg) const
{
    out.clear();
    nlohmann::json parsed;
    try {
        parsed = nlohmann::json::parse(json);
    } catch (const nlohmann::json::exception &ex) {
        errMsg = apiPrefix + ": invalid JSON: " + ex.what();
        return false;
    }
    if (!parsed.is_object()) {
        errMsg = apiPrefix + ": invalid JSON: top-level value must be an object";
        return false;
    }
    for (const auto &entry : parsed.items()) {
        if (!entry.value().is_string()) {
            errMsg = apiPrefix + ": invalid JSON: all values must be strings";
            return false;
        }
        out.emplace(entry.key(), entry.value().get<std::string>());
    }
    return true;
}

Status DynamicConfigUpdater::ApplyJson(const std::string &configJson, const std::string &apiPrefix)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!ConfigMonitorState::Instance().IsUpdateConfigAllowed()) {
        const std::string reason = apiPrefix + ": file monitor is enabled, API disabled";
        OperationLogger::Instance().LogConfigFailed("UpdateConfig", reason);
        return Status(StatusCode::K_INVALID, reason);
    }
    std::unordered_map<std::string, std::string> flagMap;
    std::string parseErr;
    if (!ParseJsonToMap(configJson, apiPrefix, flagMap, parseErr)) {
        OperationLogger::Instance().LogConfigFailed("UpdateConfig", parseErr);
        return Status(StatusCode::K_INVALID, parseErr);
    }
    if (flagMap.empty()) {
        return Status::OK();
    }
    std::vector<std::string> errors;
    auto *flagManager = FlagManager::GetInstance();
    for (const auto &kv : flagMap) {
        if (!flagManager->IsModifiableFlag(kv.first)) {
            const std::string msg = apiPrefix + ": flag '" + kv.first + "' not in trust list";
            errors.push_back(msg);
            OperationLogger::Instance().LogConfigFailed(kv.first, "not in trust list");
            continue;
        }
        std::string validateErr;
        if (!flagManager->ValidateChange(kv.first, kv.second, validateErr)) {
            const std::string msg = apiPrefix + ": flag '" + kv.first + "' invalid value: " + validateErr;
            errors.push_back(msg);
            OperationLogger::Instance().LogConfigFailed(kv.first, "invalid value: " + validateErr);
            continue;
        }
        std::string specialErr;
        if (!flags_->ValidateSpecialConstraint(flagMap, kv.first, kv.second, specialErr)) {
            const std::string msg = apiPrefix + ": flag '" + kv.first + "' " + specialErr;
            errors.push_back(msg);
            OperationLogger::Instance().LogConfigFailed(kv.first, specialErr);
        }
    }
    if (!errors.empty()) {
        return Status(StatusCode::K_INVALID, JoinErrors(errors));
    }
    RETURN_IF_NOT_OK(flags_->UpdateFlagParameter(flagMap));
    return Status::OK();
}

}  // namespace datasystem
