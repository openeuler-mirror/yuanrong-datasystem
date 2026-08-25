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

#include <cstdint>
#include <string>
#include <unordered_map>

namespace datasystem {

bool WorkerFlagValidateSpecial(const std::string &flagName, const std::string &newVal);

void AdjustNodeTimeoutFlags();

bool ValidateWatermarkFlags();

/**
 * @brief Validate the heat-based eviction strategy flags (strategy name, half-lives,
 *        threshold, max counter). Returns false (and logs) on any invalid value.
 */
bool ValidateHeatFlags();

/**
 * @brief Validate the heat-driven rebalance strategy flags (strategy name, hot threshold,
 *        and source/target usage and hot-ratio percents). Enforces heat rebalance ⟹
 *        heat eviction, fresh-insert-not-hot, and low source thresholds < high thresholds.
 *        Returns false (and logs) on any invalid value.
 */
bool ValidateRebalanceHeatFlags();

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

/**
 * @brief Check whether the value of scale_in_collect_window_ms is valid.
 * Valid range is [0, 5000]; 0 disables coalescing. Values outside the range are clamped to it.
 *
 * Note: scale_in_collect_window_ms is a static gflag, so it is not updated at runtime and this
 * validator is not wired into WorkerFlagValidateSpecial. Runtime protection is provided by the
 * startup-time AdjustScaleInCollectWindowMs clamp and by TopologyControllerOptions::IsValid().
 * This function is exposed for unit testing and for a future switch to a dynamic flag.
 * @param[in] value Change scale_in_collect_window_ms to this value.
 * @return True when value is within [0, 5000]; false otherwise.
 */
bool WorkerValidateScaleInCollectWindowMs(const uint32_t value);

/**
 * @brief Clamp scale_in_collect_window_ms into [0, 5000] at startup.
 */
void AdjustScaleInCollectWindowMs();
}  // namespace datasystem
#endif
