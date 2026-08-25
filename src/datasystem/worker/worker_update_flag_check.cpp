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
#include "datasystem/worker/worker_update_flag_check.h"

#include <algorithm>
#include <cmath>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/eviction_heat.h"
#include "datasystem/common/flags/eviction_watermark.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/validator.h"

DS_DECLARE_int32(heartbeat_interval_ms);
DS_DECLARE_string(worker_address);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_uint32(node_dead_timeout_s);
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
DS_DECLARE_uint32(scale_in_collect_window_ms);

namespace {
constexpr uint32_t kMinNodeTimeoutS = 3;
constexpr uint32_t kMinNodeDeadTimeoutS = 3;
constexpr uint32_t kMaxLeaseRenewIntervalMs = 60 * MS_PER_SECOND;
constexpr uint32_t kLeaseRenewRetryTimes = 4;
constexpr uint32_t kMaxScaleInCollectWindowMs = 5'000;

uint32_t AdjustNodeDeadTimeoutS(uint32_t value)
{
    return value < kMinNodeDeadTimeoutS ? kMinNodeDeadTimeoutS : value;
}

uint32_t MaxHeartbeatIntervalMs()
{
    uint64_t timeoutBasedLimit = static_cast<uint64_t>(FLAGS_node_timeout_s) * MS_PER_SECOND / kLeaseRenewRetryTimes;
    return static_cast<uint32_t>(std::min<uint64_t>(kMaxLeaseRenewIntervalMs, timeoutBasedLimit));
}
}  // namespace

namespace datasystem {

bool StrToUint32(const std::string &str, uint32_t &result)
{
    try {
        size_t pos = 0;
        unsigned long value = std::stoul(str, &pos);
        if (pos != str.size()) {
            return false;
        }
        if (value > std::numeric_limits<uint32_t>::max()) {
            return false;
        }

        result = static_cast<uint32_t>(value);
        return true;
    } catch (const std::invalid_argument &) {
        return false;
    } catch (const std::out_of_range &) {
        return false;
    }
    return true;
}

bool WorkerFlagValidateSpecial(const std::string &flagName, const std::string &newVal)
{
    uint32_t result = 0;
    if (flagName == "node_dead_timeout_s" && StrToUint32(newVal, result)) {
        uint32_t adjusted = AdjustNodeDeadTimeoutS(result);
        if (!WorkerValidateNodeDeadTimeoutS(result)) {
            return true;
        }
        if (adjusted != result) {
            FLAGS_node_dead_timeout_s = adjusted;
            return true;
        }
    }
    if (flagName == "heartbeat_interval_ms" && StrToUint32(newVal, result)
        && !WorkerValidateHeartbeatIntervalMs(result)) {
        return true;
    }
    return false;
}

void AdjustNodeTimeoutFlags()
{
    if (FLAGS_node_timeout_s < kMinNodeTimeoutS) {
        FLAGS_node_timeout_s = kMinNodeTimeoutS;
    }
    if (FLAGS_node_dead_timeout_s < kMinNodeDeadTimeoutS) {
        FLAGS_node_dead_timeout_s = kMinNodeDeadTimeoutS;
    }
    AdjustScaleInCollectWindowMs();
    uint32_t maxHeartbeatIntervalMs = MaxHeartbeatIntervalMs();
    if (static_cast<uint32_t>(FLAGS_heartbeat_interval_ms) > maxHeartbeatIntervalMs) {
        LOG(WARNING) << "Adjust heartbeat_interval_ms from " << FLAGS_heartbeat_interval_ms << " to "
                     << maxHeartbeatIntervalMs
                     << " (must be no greater than min(60000, node_timeout_s * 1000 / 4))";
        FLAGS_heartbeat_interval_ms = static_cast<int32_t>(maxHeartbeatIntervalMs);
    }
}

namespace {
bool ValidateFiniteHeatFlag(const char *flagName, double value, bool allowZero)
{
    if (std::isfinite(value) && (allowZero ? value >= 0.0 : value > 0.0)) {
        return true;
    }
    LOG(ERROR) << FormatString("%s must be finite and %s 0, got: %g.", flagName, allowZero ? ">=" : ">", value);
    return false;
}

bool ValidateEvictionHeatValueRanges()
{
    return ValidateFiniteHeatFlag("eviction_heat_half_life_primary_s", FLAGS_eviction_heat_half_life_primary_s, false)
           && ValidateFiniteHeatFlag("eviction_heat_half_life_local_s", FLAGS_eviction_heat_half_life_local_s, false)
           && ValidateFiniteHeatFlag("eviction_heat_threshold", FLAGS_eviction_heat_threshold, true)
           && ValidateFiniteHeatFlag("eviction_heat_initial_counter", FLAGS_eviction_heat_initial_counter, true)
           && Validator::ValidateUint32("eviction_heat_max_counter", FLAGS_eviction_heat_max_counter);
}

bool ValidateEvictionHeatRelationships()
{
    if (FLAGS_eviction_heat_half_life_local_s > FLAGS_eviction_heat_half_life_primary_s) {
        LOG(ERROR) << "eviction_heat_half_life_local_s (" << FLAGS_eviction_heat_half_life_local_s
                   << ") must be <= eviction_heat_half_life_primary_s (" << FLAGS_eviction_heat_half_life_primary_s
                   << ") so local copies are evicted sooner.";
        return false;
    }
    if (FLAGS_eviction_heat_initial_counter < FLAGS_eviction_heat_threshold) {
        LOG(ERROR) << "eviction_heat_initial_counter (" << FLAGS_eviction_heat_initial_counter
                   << ") must be >= eviction_heat_threshold (" << FLAGS_eviction_heat_threshold
                   << ") so fresh inserts are not first-round eviction candidates.";
        return false;
    }
    if (static_cast<double>(FLAGS_eviction_heat_threshold) > static_cast<double>(FLAGS_eviction_heat_max_counter)) {
        LOG(ERROR) << "eviction_heat_threshold (" << FLAGS_eviction_heat_threshold
                   << ") must be <= eviction_heat_max_counter (" << FLAGS_eviction_heat_max_counter << ").";
        return false;
    }
    if (FLAGS_eviction_heat_initial_counter > static_cast<double>(FLAGS_eviction_heat_max_counter)) {
        LOG(ERROR) << "eviction_heat_initial_counter (" << FLAGS_eviction_heat_initial_counter
                   << ") must be <= eviction_heat_max_counter (" << FLAGS_eviction_heat_max_counter << ").";
        return false;
    }
    return true;
}

bool ValidateRebalanceHeatCounterRelationships()
{
    if (FLAGS_rebalance_strategy == "heat" && FLAGS_eviction_strategy != "heat") {
        LOG(ERROR) << "rebalance_strategy=heat requires eviction_strategy=heat (heat counters are only maintained "
                      "by the heat eviction strategy).";
        return false;
    }
    if (!ValidateFiniteHeatFlag("rebalance_heat_hot_counter_threshold", FLAGS_rebalance_heat_hot_counter_threshold,
                                true)) {
        return false;
    }
    if (FLAGS_rebalance_strategy == "heat"
        && FLAGS_eviction_heat_initial_counter >= FLAGS_rebalance_heat_hot_counter_threshold) {
        LOG(ERROR) << "eviction_heat_initial_counter (" << FLAGS_eviction_heat_initial_counter
                   << ") must be < rebalance_heat_hot_counter_threshold ("
                   << FLAGS_rebalance_heat_hot_counter_threshold
                   << ") so fresh inserts are not counted as hot data for heat rebalance.";
        return false;
    }
    if (FLAGS_rebalance_strategy == "heat"
        && FLAGS_rebalance_heat_hot_counter_threshold >= static_cast<double>(FLAGS_eviction_heat_max_counter)) {
        LOG(ERROR) << "rebalance_heat_hot_counter_threshold (" << FLAGS_rebalance_heat_hot_counter_threshold
                   << ") must be < eviction_heat_max_counter (" << FLAGS_eviction_heat_max_counter
                   << ") so objects can become hot via cache hits.";
        return false;
    }
    return true;
}

bool ValidateRebalanceHeatSourceThresholds()
{
    if (FLAGS_rebalance_strategy == "heat"
        && FLAGS_rebalance_heat_source_usage_percent_low >= FLAGS_rebalance_heat_source_usage_percent) {
        LOG(ERROR) << "rebalance_heat_source_usage_percent_low (" << FLAGS_rebalance_heat_source_usage_percent_low
                   << ") must be < rebalance_heat_source_usage_percent ("
                   << FLAGS_rebalance_heat_source_usage_percent
                   << ") so the two crossed source-trigger paths remain distinct.";
        return false;
    }
    return true;
}
}  // namespace

bool ValidateHeatFlags()
{
    if (FLAGS_eviction_strategy != "clock" && FLAGS_eviction_strategy != "heat") {
        LOG(ERROR) << "eviction_strategy must be \"clock\" or \"heat\", got: \"" << FLAGS_eviction_strategy << "\".";
        return false;
    }
    return ValidateEvictionHeatValueRanges() && ValidateEvictionHeatRelationships();
}

bool ValidateRebalanceHeatFlags()
{
    if (FLAGS_rebalance_strategy != "memory" && FLAGS_rebalance_strategy != "heat") {
        LOG(ERROR) << "rebalance_strategy must be \"memory\" or \"heat\", got: \"" << FLAGS_rebalance_strategy
                   << "\".";
        return false;
    }
    return ValidateRebalanceHeatCounterRelationships() && ValidateRebalanceHeatSourceThresholds();
}

bool ValidateWatermarkFlags()
{
    if (!Validator::ValidateEvictionWatermarkRatioPair()) {
        return false;
    }
    if (!Validator::ValidateSpillWatermarkRatioPair()) {
        return false;
    }
    RefreshWatermarkFactors();
    if (!ValidateHeatFlags()) {
        return false;
    }
    RefreshHeatFactors();
    if (!ValidateRebalanceHeatFlags()) {
        return false;
    }
    RefreshRebalanceHeatFactors();
    return true;
}

bool WorkerValidateNodeDeadTimeoutS(const uint32_t value)
{
    if (value <= FLAGS_node_timeout_s) {
        LOG(ERROR) << "The value of node_dead_timeout_s must be greater than the value of node_timeout_s.";
        return false;
    }
    return true;
}

bool WorkerValidateHeartbeatIntervalMs(const uint32_t value)
{
    uint32_t maxHeartbeatIntervalMs = MaxHeartbeatIntervalMs();
    if (value > maxHeartbeatIntervalMs) {
        LOG(ERROR) << "The value of heartbeat_interval_ms must be no greater than min(60000, "
                   << "node_timeout_s * 1000 / 4). current max: " << maxHeartbeatIntervalMs;
        return false;
    }
    return true;
}

bool WorkerValidateScaleInCollectWindowMs(const uint32_t value)
{
    if (value > kMaxScaleInCollectWindowMs) {
        LOG(ERROR) << "The value of scale_in_collect_window_ms must be no greater than " << kMaxScaleInCollectWindowMs
                   << ". current value: " << value;
        return false;
    }
    return true;
}

void AdjustScaleInCollectWindowMs()
{
    if (FLAGS_scale_in_collect_window_ms > kMaxScaleInCollectWindowMs) {
        LOG(WARNING) << "Adjust scale_in_collect_window_ms from " << FLAGS_scale_in_collect_window_ms << " to "
                     << kMaxScaleInCollectWindowMs << " (hard upper bound)";
        FLAGS_scale_in_collect_window_ms = kMaxScaleInCollectWindowMs;
    }
}
}  // namespace datasystem
