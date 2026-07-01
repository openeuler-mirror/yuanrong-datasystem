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
 * Description: Process-level file monitor state for UpdateConfig mutual exclusion.
 */
#ifndef DATASYSTEM_COMMON_FLAGS_CONFIG_MONITOR_STATE_H
#define DATASYSTEM_COMMON_FLAGS_CONFIG_MONITOR_STATE_H

#include <atomic>

namespace datasystem {

class ConfigMonitorState {
public:
    static ConfigMonitorState &Instance();

    void SetFileMonitorEnabled(bool enabled);
    bool IsFileMonitorEnabled() const;
    bool IsUpdateConfigAllowed() const;

private:
    ConfigMonitorState() = default;
    ~ConfigMonitorState() = default;
    ConfigMonitorState(const ConfigMonitorState &) = delete;
    ConfigMonitorState &operator=(const ConfigMonitorState &) = delete;
    ConfigMonitorState(ConfigMonitorState &&) = delete;
    ConfigMonitorState &operator=(ConfigMonitorState &&) = delete;

    std::atomic<bool> fileMonitorEnabled_{false};
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_FLAGS_CONFIG_MONITOR_STATE_H
