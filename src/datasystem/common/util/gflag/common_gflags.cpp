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

DS_DEFINE_bool(cache_rpc_session, true, "Deprecated: This flag is deprecated and will be removed in future releases.");
DS_DEFINE_string(l2_cache_type, "none",
                 "L2 cache type, optional value: 'obs' or 'none', default is none");
DS_DEFINE_validator(l2_cache_type, &Validator::ValidateL2CacheType);
DS_DEFINE_int32(zmq_client_io_context, 5,
                "Optimize the performance of the client stub. Default value 5. "
                "The higher the throughput, the higher the value, but should be in range [1, 32]");
DS_DEFINE_int32(zmq_chunk_sz, 1048576, "Parallel payload split chunk size. Default to 1048756 bytes");
DS_DEFINE_validator(zmq_chunk_sz, &Validator::ValidateInt32);

DS_DEFINE_uint32(node_timeout_s, 60, "maximum time interval before a node is considered lost");
DS_DEFINE_validator(node_timeout_s, &Validator::ValidateNodeTimeout);
DS_DEFINE_uint32(node_dead_timeout_s, 300, "maximum time interval for the master to determine node death");
DS_DEFINE_uint64(stream_idle_time_s, 5 * 60, "stream idle time. default 300s (5 minutes)");
DS_DEFINE_int64(payload_nocopy_threshold, 1048576L * 100L, "minimum payload size to trigger no memory copy");
DS_DEFINE_bool(enable_multi_stubs, false, "deprecated");
DS_DEFINE_bool(enable_tcp_direct_for_multi_stubs, false, "deprecated");
DS_DEFINE_bool(log_monitor, true, "Indicates whether to enable log monitoring, default is true.");
DS_DEFINE_bool(auto_del_dead_node, true, "Decide whether to remove the node from hash ring or not when node is dead");
DS_DEFINE_bool(enable_huge_tlb, false,
               "enable_huge_tlb can improve memory access and reducing the overhead of page table,"
               "default is disable.");
DS_DEFINE_bool(enable_data_replication, true, "Allow data's replica to be cached locally, default is true");
DS_DEFINE_uint32(
    eviction_reserve_mem_threshold_mb, 10240,
    "The reserved memory (MB) is determined by min(shared_memory_size_mb*0.2, eviction_reserve_mem_threshold_mb). "
    "Eviction begins when memory drops below this threshold.The valid range is 100-102400.");
DS_DEFINE_validator(eviction_reserve_mem_threshold_mb, &Validator::ValidateEvictReserveMemThreshold);
DS_DEFINE_bool(spill_to_remote_worker, false, "It indicates that when node resources are insufficient, "
               "it supports spilling memory to the memory of other nodes.");

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
}  // namespace

DS_DEFINE_bool(enable_urma, false, "Option to turn on urma for OC worker to worker data transfer, default false.");
DS_DEFINE_validator(enable_urma, &ValidateEnableUrma);
DS_DEFINE_string(urma_mode, "UB", "Option to enable URMA over IB or UB, default UB to run with URMA over UB.");
DS_DEFINE_validator(urma_mode, &ValidateUrmaMode);
DS_DEFINE_bool(enable_remote_h2d, false, "Option to turn on Remote H2D, default false.");
DS_DEFINE_validator(enable_remote_h2d, &ValidateEnableRemoteH2D);

DS_DEFINE_uint32(urma_poll_size, 8, "Number of complete record to poll at a time, 16 is the max this device can poll");
DS_DEFINE_uint32(urma_connection_size, 16, "Number of jfs and jfr pair");
DS_DEFINE_bool(urma_register_whole_arena, true,
               "Register the whole arena as segment during init, otherwise, register each object as a segment.");
DS_DEFINE_bool(urma_event_mode, false, "Uses interrupt mode to poll completion events.");
DS_DEFINE_bool(enable_worker_worker_batch_get, false, "Enable worker->worker OC batch get, default false.");

DS_DEFINE_bool(enable_rdma, false, "Option to turn on rdma for OC worker to worker data transfer, default false.");
DS_DEFINE_validator(enable_rdma, &ValidateEnableRdma);
DS_DEFINE_bool(rdma_register_whole_arena, true,
               "Register the whole arena as segment during init, otherwise, register each object as a segment.");
