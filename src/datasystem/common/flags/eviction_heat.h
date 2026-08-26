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
 * Description: Validated configuration snapshots for Heat eviction and rebalance.
 */
#ifndef DATASYSTEM_COMMON_FLAGS_EVICTION_HEAT_H
#define DATASYSTEM_COMMON_FLAGS_EVICTION_HEAT_H

#include <cstdint>
#include <string>

namespace datasystem {

struct EvictionHeatConfig {
    double halfLifePrimaryS{ 600.0 };
    double halfLifeLocalS{ 300.0 };
    double threshold{ 2.0 };
    double initialCounter{ 2.0 };
    uint32_t maxCounter{ 256 };
};

struct RebalanceHeatConfig {
    double hotCounterThreshold{ 4.0 };
    uint32_t sourceUsagePercent{ 60 };
    uint32_t sourceHotRatioPercent{ 40 };
    uint32_t sourceUsagePercentLow{ 50 };
    uint32_t targetUsagePercent{ 50 };
    uint32_t targetHotRatioPercent{ 30 };
};

/**
 * @brief Snapshot validated FLAGS_eviction_* values before the worker starts serving.
 *
 * Tests may call this again before constructing the objects under test.
 */
void RefreshHeatFactors();

/**
 * @brief The selected eviction strategy ("clock" or "heat"), fixed at worker startup.
 */
const std::string &GetEvictionStrategy();

/**
 * @brief Return the process-lifetime eviction Heat configuration snapshot.
 */
const EvictionHeatConfig &GetEvictionHeatConfig();

/**
 * @brief Snapshot validated FLAGS_rebalance_* values before serving resource reports.
 */
void RefreshRebalanceHeatFactors();

/**
 * @brief The selected rebalance strategy ("memory" or "heat"), fixed at startup.
 */
const std::string &GetRebalanceStrategy();

/**
 * @brief Return the process-lifetime rebalance Heat configuration snapshot.
 */
const RebalanceHeatConfig &GetRebalanceHeatConfig();

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_FLAGS_EVICTION_HEAT_H
