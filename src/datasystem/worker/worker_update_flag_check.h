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
 * Description: Flag verification logic on the worker side.
 */
#ifndef WORKER_UPDATE_FLAG_CHECK_H
#define WORKER_UPDATE_FLAG_CHECK_H

#include <string>
#include <unordered_map>

namespace datasystem {

bool WorkerFlagValidateSpecial(const std::string &flagName, const std::string &newVal);

/**
 * @brief Check whether the value of node_dead_timeout_s is valid.
 * @param[in] value Change node_dead_timeout_s to this value.
 */
bool WorkerValidateNodeDeadTimeoutS(const uint32_t value);

/**
 * @brief Check whether the value of heartbeat_interval_ms is valid.
 * @param[in] value Change heartbeat_interval_ms to this value.
 */
bool WorkerValidateHeartbeatIntervalMs(const uint32_t value);
}  // namespace datasystem
#endif
