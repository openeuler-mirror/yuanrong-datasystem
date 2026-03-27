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

DS_DECLARE_int32(zmq_client_io_context);
DS_DECLARE_int32(zmq_chunk_sz);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_uint64(stream_idle_time_s);
DS_DECLARE_int64(payload_nocopy_threshold);
DS_DECLARE_bool(enable_multi_stubs);
DS_DECLARE_bool(enable_tcp_direct_for_multi_stubs);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_bool(enable_worker_worker_batch_get);
DS_DECLARE_bool(enable_urma);
DS_DECLARE_bool(urma_register_whole_arena);
DS_DECLARE_uint32(urma_poll_size);
DS_DECLARE_uint32(urma_connection_size);
DS_DECLARE_bool(urma_event_mode);
DS_DECLARE_string(urma_mode);
DS_DECLARE_bool(enable_transport_fallback);
DS_DECLARE_bool(enable_rdma);
DS_DECLARE_bool(rdma_register_whole_arena);
DS_DECLARE_bool(enable_remote_h2d);
DS_DECLARE_string(l2_cache_type);
DS_DECLARE_string(log_dir);
DS_DECLARE_string(monitor_config_file);
DS_DECLARE_string(unix_domain_socket_dir);
DS_DECLARE_string(log_filename);
DS_DECLARE_string(curve_key_dir);
DS_DECLARE_string(shared_disk_directory);
DS_DECLARE_string(encrypt_kit);
DS_DECLARE_string(cluster_name);
DS_DECLARE_string(log_monitor_exporter);
DS_DECLARE_uint32(eviction_reserve_mem_threshold_mb);
DS_DECLARE_string(etcd_address);
DS_DECLARE_int32(oc_worker_worker_direct_port);
DS_DECLARE_int32(sc_worker_worker_direct_port);
#endif  // DATASYSTEM_COMMON_UTIL_COMMON_GFLAGS_H
