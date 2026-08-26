/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

#include "datasystem/common/flags/common_flags.h"

#include <cmath>
#include <sstream>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rdma/urma_send_lane.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"

DS_DECLARE_uint64(urma_max_write_size_mb);

namespace {
bool ValidateEnableUrma(const char *flagName, bool value)
{
    (void)flagName;
#ifdef USE_URMA
    (void)value;
    return true;
#else
    if (value) {
        LOG(ERROR) << FormatString("Worker not build with URMA framework, but %s set to true", flagName);
        return false;
    }
    return true;
#endif
}

bool ValidateSharedMemoryDistributionPolicy(const char *flagName, const std::string &value)
{
    if (value == "none" || value == "interleave_all_numa" || value == "interleave_affinity_numa") {
        return true;
    }
    LOG(ERROR) << FormatString(
        "Invalid %s value: %s. Optional values are 'none', 'interleave_all_numa', 'interleave_affinity_numa'.",
        flagName, value);
    return false;
}

bool ValidateUbTransportArenaNum(const char *flagName, uint32_t value)
{
    constexpr uint32_t kMinArenaNum = 1;
    constexpr uint32_t kMaxArenaNum = 32;
    if (value < kMinArenaNum || value > kMaxArenaNum) {
        LOG(ERROR) << FormatString("The %s flag is %u, which must be between %u and %u.", flagName, value,
                                   kMinArenaNum, kMaxArenaNum);
        return false;
    }
    return true;
}

bool ValidateUbNumaRrType(const char *flagName, uint32_t value)
{
    constexpr auto kMaxType = datasystem::UbNumaRrType::PER_POST;
    if (value > static_cast<uint32_t>(kMaxType)) {
        LOG(ERROR) << FormatString("The %s flag is %u, which must be between 0 and %u.", flagName, value,
                                   static_cast<uint32_t>(kMaxType));
        return false;
    }
    return true;
}

bool ValidateUbNumaSrcChipPolicy(const char *flagName, uint32_t value)
{
    constexpr auto kMaxPolicy = datasystem::UbNumaSrcChipPolicy::ROUND_ROBIN_WITH_AFFINITY;
    if (value > static_cast<uint32_t>(kMaxPolicy)) {
        LOG(ERROR) << FormatString("The %s flag is %u, which must be between 0 and %u.", flagName, value,
                                   static_cast<uint32_t>(kMaxPolicy));
        return false;
    }
    return true;
}

bool ValidateDataPlacementPolicy(const char *flagName, const std::string &value)
{
    const std::string normalized = StringToUpper(Trim(value));
    if (normalized == "PREFERRED_SAME_NODE" ||
        normalized == "REQUIRED_SAME_NODE" ||
        normalized == "PREFERRED_META_OWNER") {
        return true;
    }
    LOG(ERROR) << FormatString(
        "Invalid %s value: '%s'. Valid values are PREFERRED_SAME_NODE, REQUIRED_SAME_NODE, PREFERRED_META_OWNER.",
        flagName, value);
    return false;
}

bool ValidateEnableRdma(const char *flagName, bool value)
{
    (void)flagName;
#ifdef USE_RDMA
    (void)value;
    return true;
#else
    if (value) {
        LOG(ERROR) << FormatString("Worker not build with UCX RDMA framework, but %s set to true", flagName);
        return false;
    }
    return true;
#endif
}

bool ValidateRemoteH2DLinkType(const char *flagName, const std::string &value)
{
    if (value == "ROCE" || value == "HCCS") {
        return true;
    }
    LOG(ERROR) << FormatString("Invalid %s: '%s'. Must be 'ROCE' or 'HCCS'.", flagName, value);
    return false;
}

bool ValidateRemoteH2DHccsBufferPool(const char *flagName, const std::string &value)
{
    uint64_t count = 0;
    uint64_t size = 0;
    char colon = 0;
    char trailing = 0;
    std::istringstream iss(value);
    iss >> count >> colon >> size;
    const bool enabledConfig = count > 0 && size > 0;
    const bool disabledConfig = value == "0:0";
    if (!iss.fail() && colon == ':' && !(iss >> trailing) && (enabledConfig || disabledConfig)) {
        return true;
    }
    LOG(ERROR) << FormatString(
        "Invalid %s: '%s'. Expected \"<count>:<size>\" with both values positive, or \"0:0\" to disable it.",
        flagName, value);
    return false;
}

bool ValidateEnableRemoteH2D(const char *flagName, bool value)
{
    (void)flagName;
#ifdef BUILD_HETERO
    (void)value;
    // Fixme: Conflict with URMA.
    return true;
#else
    if (value) {
        LOG(ERROR) << FormatString("Worker not build with Ascend support, but %s set to true", flagName);
        return false;
    }
    return true;
#endif
}

bool ValidateIoThreadNice(const char *flagName, int32_t value)
{
    constexpr int32_t kMinNice = -20;
    constexpr int32_t kMaxNice = 19;
    if (value < kMinNice || value > kMaxNice) {
        LOG(ERROR) << FormatString("The %s flag is %d, which must be between %d and %d.", flagName, value, kMinNice,
                                   kMaxNice);
        return false;
    }
    return true;
}

bool ValidateUrmaMaxWriteSize(const char *flagName, uint64_t value)
{
    constexpr uint64_t kMinWriteSizeMb = 1;
    constexpr uint64_t kMaxWriteSizeMb = 2 * 1024;
    if (value < kMinWriteSizeMb || value > kMaxWriteSizeMb) {
        LOG(ERROR) << "The " << flagName << " flag is " << value << " MB, which must be between " << kMinWriteSizeMb
                   << " MB and " << kMaxWriteSizeMb << " MB.";
        return false;
    }
    return true;
}

bool ValidatePercent(const char *flagName, uint32_t value)
{
    constexpr uint32_t kMinPercent = 1;
    constexpr uint32_t kMaxPercent = 100;
    if (value < kMinPercent || value > kMaxPercent) {
        LOG(ERROR) << FormatString("The %s flag is %u, which must be between %u and %u.", flagName, value,
                                   kMinPercent, kMaxPercent);
        return false;
    }
    return true;
}

bool ValidateUrmaFailoverSuccessRateRatio(const char *flagName, double value)
{
    constexpr double kMinSuccessRateRatio = 0.0;
    constexpr double kMaxSuccessRateRatio = 1.0;
    if (!std::isfinite(value) || value < kMinSuccessRateRatio || value > kMaxSuccessRateRatio) {
        LOG(ERROR) << FormatString("The %s flag is %g, which must be between 0.0 and 1.0.", flagName, value);
        return false;
    }
    return true;
}

bool ValidateUrmaFailoverMinSampleCount(const char *flagName, uint32_t value)
{
    if (value == 0) {
        LOG(ERROR) << FormatString("The %s flag must be greater than 0, got %u.", flagName, value);
        return false;
    }
    return true;
}

bool ValidateSampleRateRange(const char *flagName, double value)
{
    if (!std::isfinite(value) || value < 0.0 || value > 1.0) {
        LOG(ERROR) << FormatString("The %s flag is %f, which must be a finite value in [0.0, 1.0].", flagName, value);
        return false;
    }
    return true;
}

bool ValidateOptionalLogName(const char *flagName, const std::string &value)
{
    if (value.empty() || Validator::ValidateLogName(value)) {
        return true;
    }
    LOG(ERROR) << FormatString("The value of %s flag is %s, which must contain only a-z, A-Z, 0-9, and underscore.",
                               flagName, value);
    return false;
}
}  // namespace

DS_DEFINE_validator(l2_cache_type, &Validator::ValidateL2CacheType);
DS_DEFINE_validator(sdk_data_placement_policy, &ValidateDataPlacementPolicy);
DS_DEFINE_validator(io_thread_nice, &ValidateIoThreadNice);
DS_DEFINE_validator(node_timeout_s, &Validator::ValidateNodeTimeout);
DS_DEFINE_validator(eviction_reserve_mem_threshold_mb, &Validator::ValidateEvictReserveMemThreshold);
DS_DEFINE_validator(eviction_high_watermark_ratio, &Validator::ValidateWatermarkHighRatio);
DS_DEFINE_validator(eviction_low_watermark_ratio, &Validator::ValidateWatermarkLowRatio);
DS_DEFINE_validator(spill_high_watermark_ratio, &Validator::ValidateWatermarkHighRatio);
DS_DEFINE_validator(spill_low_watermark_ratio, &Validator::ValidateWatermarkLowRatio);
DS_DEFINE_validator(enable_urma, &ValidateEnableUrma);
DS_DEFINE_validator(ub_numa_rr_type, &ValidateUbNumaRrType);
DS_DEFINE_validator(ub_numa_src_chip_policy, &ValidateUbNumaSrcChipPolicy);
DS_DEFINE_validator(urma_max_write_size_mb, &ValidateUrmaMaxWriteSize);
DS_DEFINE_validator(urma_send_jetty_lane_pool_size, &Validator::ValidateUint32);
DS_DEFINE_validator(urma_send_jetty_lane_refill_extra_size, &Validator::ValidateUint32);
DS_DEFINE_validator(urma_failover_success_rate_ratio, &ValidateUrmaFailoverSuccessRateRatio);
DS_DEFINE_validator(urma_failover_min_sample_count, &ValidateUrmaFailoverMinSampleCount);
DS_DEFINE_validator(shared_memory_distribution_policy, &ValidateSharedMemoryDistributionPolicy);
DS_DEFINE_validator(ub_transport_arena_num, &ValidateUbTransportArenaNum);
DS_DEFINE_validator(enable_remote_h2d, &ValidateEnableRemoteH2D);
DS_DEFINE_validator(remote_h2d_link_type, &ValidateRemoteH2DLinkType);
DS_DEFINE_validator(remote_h2d_hccs_buffer_pool, &ValidateRemoteH2DHccsBufferPool);
DS_DEFINE_validator(enable_rdma, &ValidateEnableRdma);
DS_DEFINE_validator(rebalance_source_usage_percent, &ValidatePercent);
DS_DEFINE_validator(rebalance_usage_gap_percent, &ValidatePercent);
DS_DEFINE_validator(rebalance_task_report_grace_ms, &Validator::ValidateUint32);
DS_DEFINE_validator(rebalance_heat_source_usage_percent, &ValidatePercent);
DS_DEFINE_validator(rebalance_heat_source_hot_ratio_percent, &ValidatePercent);
DS_DEFINE_validator(rebalance_heat_source_usage_percent_low, &ValidatePercent);
DS_DEFINE_validator(rebalance_heat_target_usage_percent, &ValidatePercent);
DS_DEFINE_validator(rebalance_heat_target_hot_ratio_percent, &ValidatePercent);
DS_DEFINE_validator(monitor_config_file, &Validator::ValidatePathString);
DS_DEFINE_validator(unix_domain_socket_dir, &Validator::ValidateUnixDomainSocketDir);
DS_DEFINE_validator(log_filename, &ValidateOptionalLogName);
DS_DEFINE_validator(shared_disk_directory, &Validator::ValidatePathString);
DS_DEFINE_validator(distributed_disk_path, &Validator::ValidatePathString);
DS_DEFINE_validator(encrypt_kit, &Validator::ValidateEncryptKit);
DS_DEFINE_validator(etcd_address, &Validator::ValidateEtcdAddresses);
DS_DEFINE_validator(request_sample_rate, &ValidateSampleRateRange);
DS_DEFINE_validator(access_sample_rate, &ValidateSampleRateRange);
DS_DEFINE_validator(diagnostic_sample_rate, &ValidateSampleRateRange);

namespace datasystem {
void LinkCommonFlagsValidators()
{
}
}  // namespace datasystem
