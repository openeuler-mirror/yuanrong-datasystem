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

#include "datasystem/common/util/gflag/common_gflags.h"

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/validator.h"

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

bool ValidateUrmaMode(const char *flagName, const std::string &value)
{
    (void)flagName;
    (void)value;
#ifdef USE_URMA
    if (value == "IB") {
        return true;
    }
#ifdef URMA_OVER_UB
    if (value == "UB") {
        return true;
    }
#endif
    return false;
#else
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

bool ValidateZmqClientIoThread(const char *flagName, int32_t value)
{
    constexpr int32_t kMinValue = 1;
    constexpr int32_t kMaxValue = 32;
    if (value < kMinValue || value > kMaxValue) {
        LOG(ERROR) << FormatString("The %s flag is %d, which must be between %d and %d.", flagName, value, kMinValue,
                                   kMaxValue);
        return false;
    }
    return true;
}
}  // namespace

DS_DEFINE_validator(l2_cache_type, &Validator::ValidateL2CacheType);
DS_DEFINE_validator(io_thread_nice, &ValidateIoThreadNice);
DS_DEFINE_validator(zmq_client_io_thread, &ValidateZmqClientIoThread);
DS_DEFINE_validator(zmq_chunk_sz, &Validator::ValidateInt32);
DS_DEFINE_validator(node_timeout_s, &Validator::ValidateNodeTimeout);
DS_DEFINE_validator(eviction_reserve_mem_threshold_mb, &Validator::ValidateEvictReserveMemThreshold);
DS_DEFINE_validator(enable_urma, &ValidateEnableUrma);
DS_DEFINE_validator(urma_mode, &ValidateUrmaMode);
DS_DEFINE_validator(shared_memory_distribution_policy, &ValidateSharedMemoryDistributionPolicy);
DS_DEFINE_validator(enable_remote_h2d, &ValidateEnableRemoteH2D);
DS_DEFINE_validator(enable_rdma, &ValidateEnableRdma);
DS_DEFINE_validator(monitor_config_file, &Validator::ValidatePathString);
DS_DEFINE_validator(unix_domain_socket_dir, &Validator::ValidateUnixDomainSocketDir);
DS_DEFINE_validator(log_filename, &Validator::ValidateEligibleChar);
DS_DEFINE_validator(curve_key_dir, &Validator::ValidatePathString);
DS_DEFINE_validator(shared_disk_directory, &Validator::ValidatePathString);
DS_DEFINE_validator(distributed_disk_path, &Validator::ValidatePathString);
DS_DEFINE_validator(encrypt_kit, &Validator::ValidateEncryptKit);
DS_DEFINE_validator(etcd_address, &Validator::ValidateEtcdAddresses);
