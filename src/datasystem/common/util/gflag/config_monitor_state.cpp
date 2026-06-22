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
#include "datasystem/common/util/gflag/config_monitor_state.h"

namespace datasystem {

ConfigMonitorState &ConfigMonitorState::Instance()
{
    static ConfigMonitorState instance;
    return instance;
}

void ConfigMonitorState::SetFileMonitorEnabled(bool enabled)
{
    fileMonitorEnabled_.store(enabled, std::memory_order_release);
}

bool ConfigMonitorState::IsFileMonitorEnabled() const
{
    return fileMonitorEnabled_.load(std::memory_order_acquire);
}

bool ConfigMonitorState::IsUpdateConfigAllowed() const
{
    return !IsFileMonitorEnabled();
}

}  // namespace datasystem
