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

#ifndef DATASYSTEM_COMMON_FLAGS_COMMON_FLAGS_H
#define DATASYSTEM_COMMON_FLAGS_COMMON_FLAGS_H

#include "datasystem/common/flags/flags.h"

DS_DECLARE_int32(zmq_client_io_context);
DS_DECLARE_int32(zmq_client_io_thread);
DS_DECLARE_int32(io_thread_nice);
DS_DECLARE_int32(zmq_chunk_sz);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_uint64(stream_idle_time_s);
DS_DECLARE_int64(payload_nocopy_threshold);
DS_DECLARE_bool(enable_multi_stubs);
DS_DECLARE_bool(enable_tcp_direct_for_multi_stubs);
DS_DECLARE_bool(log_monitor);
DS_DECLARE_bool(json_log_monitor);
DS_DECLARE_bool(enable_data_replication);
DS_DECLARE_bool(enable_worker_worker_batch_get);
DS_DECLARE_bool(enable_urma);
DS_DECLARE_bool(enable_ub_numa_affinity);
DS_DECLARE_string(shared_memory_distribution_policy);
DS_DECLARE_bool(urma_register_whole_arena);
DS_DECLARE_uint32(urma_poll_size);
DS_DECLARE_uint32(urma_connection_size);
DS_DECLARE_bool(urma_event_mode);
// DEPRECATED: This flag is no longer used and will be removed in a future version.
DS_DECLARE_string(urma_mode);
DS_DECLARE_bool(enable_transport_fallback);
DS_DECLARE_double(urma_failover_success_rate_ratio);
DS_DECLARE_uint32(urma_failover_min_sample_count);
DS_DECLARE_bool(enable_rdma);
DS_DECLARE_bool(rdma_register_whole_arena);
DS_DECLARE_bool(enable_remote_h2d);
DS_DECLARE_string(remote_h2d_link_type);
DS_DECLARE_string(remote_h2d_hccs_buffer_pool);
DS_DECLARE_string(l2_cache_type);
DS_DECLARE_string(distributed_disk_path);
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
DS_DECLARE_double(eviction_high_watermark_ratio);
DS_DECLARE_double(eviction_low_watermark_ratio);
DS_DECLARE_double(spill_high_watermark_ratio);
DS_DECLARE_double(spill_low_watermark_ratio);
DS_DECLARE_bool(enable_memory_rebalance);
DS_DECLARE_uint32(rebalance_source_usage_percent);
DS_DECLARE_uint32(rebalance_usage_gap_percent);
DS_DECLARE_uint64(rebalance_max_migrate_bytes_per_round);
DS_DECLARE_uint32(rebalance_cooldown_s);
DS_DECLARE_uint32(rebalance_task_report_grace_ms);
DS_DECLARE_string(etcd_address);
DS_DECLARE_int32(oc_worker_worker_direct_port);
DS_DECLARE_int32(sc_worker_worker_direct_port);
DS_DECLARE_bool(enable_pipeline_h2d);
DS_DECLARE_int32(pipeline_h2d_thread_num);
DS_DECLARE_double(request_sample_rate);
DS_DECLARE_double(access_sample_rate);
DS_DECLARE_double(diagnostic_sample_rate);
DS_DECLARE_uint64(slow_log_process_slower_than);
DS_DECLARE_uint64(slow_log_rpc_slower_than);
DS_DECLARE_uint64(client_slow_log_process_slower_than);
DS_DECLARE_uint64(client_slow_log_rpc_slower_than);
DS_DECLARE_bool(use_brpc);
DS_DECLARE_int32(brpc_server_num_threads);
DS_DECLARE_int32(brpc_max_concurrency);
DS_DECLARE_bool(brpc_enable_builtin_services);
#endif  // DATASYSTEM_COMMON_FLAGS_COMMON_FLAGS_H
