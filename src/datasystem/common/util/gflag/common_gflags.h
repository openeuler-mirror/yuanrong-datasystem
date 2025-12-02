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

#ifndef DATASYSTEM_COMMON_UTIL_COMMON_GFLAGS_H
#define DATASYSTEM_COMMON_UTIL_COMMON_GFLAGS_H

#include "datasystem/common/flags/flags.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_int32(zmq_client_io_context);
DS_DECLARE_int32(zmq_chunk_sz);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_uint64(stream_idle_time_s);
DS_DECLARE_int64(payload_nocopy_threshold);
DS_DECLARE_bool(enable_multi_stubs);
DS_DECLARE_bool(enable_tcp_direct_for_multi_stubs);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_bool(enable_worker_worker_batch_get);
DS_DECLARE_bool(urma_register_whole_arena);
DS_DECLARE_bool(enable_rdma);
DS_DECLARE_bool(rdma_register_whole_arena);
#endif  // DATASYSTEM_COMMON_UTIL_COMMON_GFLAGS_H
