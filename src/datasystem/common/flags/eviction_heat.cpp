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

#include "datasystem/common/flags/eviction_heat.h"

#include "datasystem/common/flags/flags.h"

DS_DECLARE_string(eviction_strategy);
DS_DECLARE_double(eviction_heat_half_life_primary_s);
DS_DECLARE_double(eviction_heat_half_life_local_s);
DS_DECLARE_double(eviction_heat_threshold);
DS_DECLARE_uint32(eviction_heat_max_counter);
DS_DECLARE_double(eviction_heat_initial_counter);
DS_DECLARE_string(rebalance_strategy);
DS_DECLARE_double(rebalance_heat_hot_counter_threshold);
DS_DECLARE_uint32(rebalance_heat_source_usage_percent);
DS_DECLARE_uint32(rebalance_heat_source_hot_ratio_percent);
DS_DECLARE_uint32(rebalance_heat_source_usage_percent_low);
DS_DECLARE_uint32(rebalance_heat_target_usage_percent);
DS_DECLARE_uint32(rebalance_heat_target_hot_ratio_percent);

namespace datasystem {
namespace {
std::string g_evictionStrategy{ "clock" };
EvictionHeatConfig g_evictionHeatConfig;
std::string g_rebalanceStrategy{ "memory" };
RebalanceHeatConfig g_rebalanceHeatConfig;
}  // namespace

void RefreshHeatFactors()
{
    g_evictionStrategy = FLAGS_eviction_strategy;
    g_evictionHeatConfig = { FLAGS_eviction_heat_half_life_primary_s, FLAGS_eviction_heat_half_life_local_s,
                             FLAGS_eviction_heat_threshold, FLAGS_eviction_heat_initial_counter,
                             FLAGS_eviction_heat_max_counter };
}

const std::string &GetEvictionStrategy()
{
    return g_evictionStrategy;
}

const EvictionHeatConfig &GetEvictionHeatConfig()
{
    return g_evictionHeatConfig;
}

void RefreshRebalanceHeatFactors()
{
    g_rebalanceStrategy = FLAGS_rebalance_strategy;
    g_rebalanceHeatConfig = {
        FLAGS_rebalance_heat_hot_counter_threshold,    FLAGS_rebalance_heat_source_usage_percent,
        FLAGS_rebalance_heat_source_hot_ratio_percent, FLAGS_rebalance_heat_source_usage_percent_low,
        FLAGS_rebalance_heat_target_usage_percent,     FLAGS_rebalance_heat_target_hot_ratio_percent
    };
}

const std::string &GetRebalanceStrategy()
{
    return g_rebalanceStrategy;
}

const RebalanceHeatConfig &GetRebalanceHeatConfig()
{
    return g_rebalanceHeatConfig;
}
}  // namespace datasystem
