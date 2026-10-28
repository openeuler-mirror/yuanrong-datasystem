/*
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
 * Description: Timing policy for cluster membership coordination.
 */
#ifndef DATASYSTEM_CLUSTER_COORDINATION_TIMING_H
#define DATASYSTEM_CLUSTER_COORDINATION_TIMING_H

#include <algorithm>
#include <chrono>
#include <cstdint>

namespace datasystem {
constexpr int64_t MEMBERSHIP_KEEP_ALIVE_INTERVAL_DIVISOR = 3;
constexpr int64_t INITIAL_KEEPALIVE_RETRY_BUDGET_MULTIPLIER = 3;
constexpr int64_t MIN_MEMBERSHIP_KEEP_ALIVE_INTERVAL_MS = 100;
constexpr int64_t MAX_MEMBERSHIP_KEEP_ALIVE_INTERVAL_MS = 5'000;

inline int64_t GetMembershipKeepAliveIntervalMs(uint32_t nodeTimeoutS)
{
    const auto timeoutMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::seconds(nodeTimeoutS));
    return std::clamp(timeoutMs.count() / MEMBERSHIP_KEEP_ALIVE_INTERVAL_DIVISOR,
                      MIN_MEMBERSHIP_KEEP_ALIVE_INTERVAL_MS, MAX_MEMBERSHIP_KEEP_ALIVE_INTERVAL_MS);
}
}  // namespace datasystem

#endif  // DATASYSTEM_CLUSTER_COORDINATION_TIMING_H
