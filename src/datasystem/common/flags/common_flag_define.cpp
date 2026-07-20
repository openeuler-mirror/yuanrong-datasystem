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

#include "datasystem/common/flags/flags.h"

// In the co-process scenario of client and worker, the following flags variables are redefined in both libdatasystem.so
// and libdatasystem_worker.so, which will cause a double free error. These libraries need to be packaged as static
// libraries to avoid this issue.

DS_DEFINE_string(l2_cache_type, "none",
                 "L2 cache type, optional value: 'obs', 'sfs', 'distributed_disk' or 'none', default is none");
DS_DEFINE_string(distributed_disk_path, "",
                 "Root path of distributed disk storage. This flag takes effect only when l2_cache_type is "
                 "'distributed_disk'.");
DS_DEFINE_string(log_dir, GetStringFromEnv("GOOGLE_LOG_DIR", ""),
                 "If specified, logfiles are written into this directory instead of the default logging directory.");
DS_DEFINE_string(monitor_config_file, "~/datasystem/config/datasystem.config",
                 "Path of the flag configuration file. The application code dynamically modifies the configuration by "
                 "monitoring whether the flagfile is updated.");
DS_DEFINE_string(
    unix_domain_socket_dir, "~/datasystem/unix_domain_socket_dir",
    "The directory to store unix domain socket file. The UDS generates temporary files in this path. Max lenth: 80");
DS_DEFINE_string(log_filename, "",
                 "Prefix of log filename, default is program invocation short name. Use standard characters only.");
DS_DEFINE_string(curve_key_dir, "",
                 "The directory to find ZMQ curve key files. This path must be specified "
                 "when zmq authentication is enabled. Path must be less than 4095 characters (PATH_MAX).");
DS_DEFINE_string(
    shared_disk_directory, "",
    "Disk cache data placement directory, default value is empty, indicating that disk cache is not enabled.");
DS_DEFINE_string(encrypt_kit, "plaintext", "Choose the encryptService, can be 'plaintext'. Default is 'plaintext'.");
DS_DEFINE_string(
    cluster_name, "",
    "cluster_name is typically used in scenarios where multiple AZ datasystem share a single etcd cluster, "
    "allowing different clusters to be distinguished by the cluster_name.");
DS_DEFINE_string(
    log_monitor_exporter, "harddisk",
    "Specify the type of exporter, either harddisk or backend. Only takes effect when log_monitor is true.");
DS_DEFINE_bool(rdma_register_whole_arena, true,
               "Register the whole arena as segment during init, otherwise, register each object as a segment.");
DS_DEFINE_bool(enable_rdma, false, "Option to turn on rdma for OC worker to worker data transfer, default false.");
DS_DEFINE_uint32(urma_poll_size, 8, "Number of complete record to poll at a time, 16 is the max this device can poll");
DS_DEFINE_uint64(urma_max_write_size_mb, 4, "Maximum URMA write size, unit is MB. Valid range is [1, 2048].");
DS_DEFINE_uint32(urma_connection_size, 0, "[DEPRECATED] No longer used. JFS/JFR are created per-connection.");
DS_DEFINE_uint32(urma_send_jetty_lane_pool_size, 200,
                 "Process-level target number of reusable URMA send Jetty lanes. Each lane is leased by one logical "
                 "transfer and may carry multiple chunk WRs until that transfer settles.");
DS_DEFINE_uint32(urma_send_jetty_lane_refill_extra_size, 200,
                 "Maximum number of retiring or pending URMA send Jetty lanes allowed above the active pool size.");
DS_DEFINE_bool(urma_register_whole_arena, true,
               "Register the whole arena as segment during init, otherwise, register each object as a segment.");
DS_DEFINE_bool(enable_ub_numa_affinity, false,
               "Enable UB numa affinity optimization when URMA and whole-arena registration are both enabled.");
DS_DEFINE_string(shared_memory_distribution_policy, "none",
                 "Shared memory NUMA distribution policy. Optional values: "
                 "'none', 'interleave_all_numa', 'interleave_affinity_numa'.");
DS_DEFINE_bool(urma_event_mode, false, "Uses interrupt mode to poll completion events.");
DS_DEFINE_bool(enable_urma_perf, false, "Enable performance logging for URMA operations");
DS_DEFINE_bool(enable_worker_worker_batch_get, false, "Enable worker->worker OC batch get, default false.");
DS_DEFINE_bool(enable_remote_h2d, false, "Option to turn on Remote H2D, default false.");
DS_DEFINE_string(remote_h2d_link_type, "ROCE", "Link type for Remote H2D: ROCE or HCCS.");
DS_DEFINE_string(remote_h2d_hccs_buffer_pool, "4:8",
                 "HIXL buffer-pool spec for HCCS RH2D, format \"<count>:<size>\" (both positive). "
                 "Only consumed when remote_h2d_link_type=HCCS.");
DS_DEFINE_string(urma_mode, "UB", "[DEPRECATED] This flag is no longer used and will be removed in a future version.");
DS_DEFINE_bool(enable_urma, false, "Option to turn on urma for OC worker to worker data transfer, default false.");
DS_DEFINE_bool(enable_transport_fallback, true, "Enable the fast transport fallback to tcp transport.");
DS_DEFINE_double_dynamic(urma_failover_success_rate_ratio, 0.5,
                 "Client-side URMA data-plane success-rate ratio threshold for worker failover. If the window success "
                 "rate is below this ratio, the client tries to switch worker. 0.0 disables URMA failover.");
DS_DEFINE_uint32_dynamic(urma_failover_min_sample_count, 5,
                 "Minimum URMA data-plane samples per client_dead_timeout_s window before failover evaluation.");
DS_DEFINE_uint32(
    eviction_reserve_mem_threshold_mb, 10240,
    "The reserved memory (MB) is determined by min(shared_memory_size_mb*0.1, eviction_reserve_mem_threshold_mb). "
    "Eviction begins when memory drops below this threshold.The valid range is 100-102400.");
DS_DEFINE_double(eviction_high_watermark_ratio, 0.9,
                 "Memory usage high watermark (ratio of available shared memory, 0.0-1.0). Eviction starts when "
                 "occupied memory reaches max(ratio * memory, memory - eviction_reserve_mem_threshold_mb). Must be "
                 "greater than eviction_low_watermark_ratio. Valid range: 0.02-1.0.");
DS_DEFINE_double(eviction_low_watermark_ratio, 0.8,
                 "Memory usage low watermark (ratio, 0.0-1.0). Background eviction runs until usage is at or below "
                 "this ratio. Must be less than eviction_high_watermark_ratio. Valid range: 0.01-0.99.");
DS_DEFINE_double(spill_high_watermark_ratio, 0.8,
                 "Spill directory usage high watermark (ratio of spill_size_limit, 0.0-1.0). Valid range: 0.02-1.0. "
                 "Must be greater than spill_low_watermark_ratio.");
DS_DEFINE_double(spill_low_watermark_ratio, 0.6,
                 "Spill directory usage low watermark (ratio of spill_size_limit, 0.0-1.0). Valid range: 0.01-0.99. "
                 "Must be less than spill_high_watermark_ratio.");
DS_DEFINE_uint32_dynamic(node_dead_timeout_s, 300, "maximum time interval for the master to determine node death");
DS_DEFINE_uint64(stream_idle_time_s, 5 * 60, "stream idle time. default 300s (5 minutes)");
DS_DEFINE_int64(payload_nocopy_threshold, 1048576L * 100L, "minimum payload size to trigger no memory copy");
DS_DEFINE_bool(enable_multi_stubs, false, "deprecated");
DS_DEFINE_bool(enable_tcp_direct_for_multi_stubs, false, "deprecated");
DS_DEFINE_bool_dynamic(log_monitor, true, "Indicates whether to enable log monitoring, default is true.");
DS_DEFINE_bool_dynamic(json_log_monitor, true,
                       "Indicates whether to enable JSON log monitoring for kv_resource.log and kv_metrics.log.");
DS_DEFINE_bool_dynamic(auto_del_dead_node, true, "Remove dead node from hash ring when enabled.");
DS_DEFINE_bool(enable_huge_tlb, false,
               "enable_huge_tlb can improve memory access and reducing the overhead of page table,"
               "default is disable.");
DS_DEFINE_bool(enable_data_replication, true,
               "Allow data replica cache locally; mainly for performance validation, keep enabled unless needed.");

DS_DEFINE_bool(enable_memory_rebalance, false,
    "Enable master-scheduled memory rebalance across workers. The master periodically schedules tasks to migrate "
    "primary object replicas from high-usage workers to under-utilized workers.");
DS_DEFINE_uint32(rebalance_source_usage_percent, 70,
                 "Memory usage percent at or above which a ready worker can be selected as memory rebalance source.");
DS_DEFINE_uint32(rebalance_usage_gap_percent, 20,
                 "Minimum memory usage percent gap between source and target for memory rebalance.");
DS_DEFINE_uint64(rebalance_max_migrate_bytes_per_round, 1024ul * 1024ul * 1024ul,
                 "Maximum bytes assigned to a single memory rebalance task.");
DS_DEFINE_uint32(rebalance_cooldown_s, 60, "Cooldown seconds for workers after a failed or expired rebalance task.");
DS_DEFINE_uint32(rebalance_task_report_grace_ms, 60000, "rebalance task report grace ms.");
DS_DEFINE_uint32(node_timeout_s, 60, "maximum time interval before a node is considered lost");
DS_DEFINE_int32(io_thread_nice, 0,
                "Nice value for selected IO threads. Valid range is [-20, 19]. 0 skips nice adjustment and preserves "
                "the thread's inherited nice value. Only non-zero values call setpriority; negative values usually "
                "require appropriate privileges.");
DS_DEFINE_bool(enable_sched_runtime, true,
               "Enable selected threads to set the Linux scheduler runtime. Default is true.");
DS_DEFINE_int32(zmq_client_io_context, 5,
                "Optimize the performance of the client stub. Default value 5. "
                "The higher the throughput, the higher the value, but should be in range [1, 32]");
DS_DEFINE_int32(zmq_client_io_thread, 1,
                "Optimize the performance of the client stub. Default value 1. "
                "The higher the throughput, the higher the value, but should be in range [1, 32]");
DS_DEFINE_int32(zmq_chunk_sz, 1048576, "Parallel payload split chunk size. Default to 1048756 bytes");
DS_DEFINE_bool(cache_rpc_session, true, "Deprecated: This flag is deprecated and will be removed in future releases.");
DS_DEFINE_string(etcd_address, "", "Address of ETCD server");
DS_DEFINE_string(coordinator_address, "",
                 "Address of datasystem coordinator service. Empty means coordinator mode is disabled.");
DS_DEFINE_int32(oc_worker_worker_direct_port, 0,
                "Direct tcp/ip port for WorkerWorkerOCService. 0 -- disable this direction connection");
DS_DEFINE_int32(sc_worker_worker_direct_port, 0,
                "Direct tcp/ip port for WorkerWorkerSCService. 0 -- disable this direction connection");
DS_DEFINE_bool(enable_pipeline_h2d, false, "Enable pipeline H2D. Default is false");
DS_DEFINE_int32(pipeline_h2d_thread_num, 64, "Pipeline H2D worker thread number. Default value 64");
DS_DEFINE_uint64_dynamic(slow_log_process_slower_than, 2000,
                 "In-process processing latency threshold (microseconds) for slow-log and latency summary. "
                 "Default 2000 (2ms). 0 means disabled. When enabled, requests whose in-process phases "
                 "exceed this threshold will include a latency summary in the access log.");
DS_DEFINE_uint64_dynamic(slow_log_rpc_slower_than, 5000,
                 "RPC operation latency threshold (microseconds) for slow-log and latency summary. "
                 "Default 5000 (5ms). 0 means disabled. When enabled, requests whose RPC phases exceed this threshold "
                 "will include a latency summary in the access log.");
DS_DEFINE_bool(enable_leaving_intercept, false,
               "Reject object-cache write requests after the local worker starts topology scale-in draining.");
DS_DEFINE_string(sdk_data_placement_policy, "PREFERRED_SAME_NODE",
                 "SDK write placement policy, read once during routing initialization: PREFERRED_SAME_NODE, "
                 "REQUIRED_SAME_NODE, or PREFERRED_META_OWNER. Use PREFERRED_META_OWNER to preserve legacy "
                 "metadata-owner placement.");
DS_DEFINE_bool(use_brpc, GetBoolFromEnv("DATASYSTEM_USE_BRPC", false),
               "Use brpc instead of ZMQ for RPC communication.");
DS_DEFINE_int32(brpc_server_num_threads, 64, "Number of brpc server worker threads.");
DS_DEFINE_int32(brpc_max_concurrency, 128,
                "Max concurrent in-flight RPCs per brpc server. 0=unlimited. "
                "Recommended: num_threads * 2 (e.g. 128 for 64 threads). When exceeded, "
                "brpc returns ELIMIT immediately so a slow handler cannot exhaust bthreads "
                "and cause OOM. Must not be smaller than brpc_server_num_threads.");
DS_DEFINE_bool(brpc_enable_builtin_services, false,
               "Expose brpc built-in HTTP services (/flags, /pprof, /vars, /status) on the RPC port. "
               "Default false to match the ZMQ security baseline (no HTTP endpoint); the /flags and "
               "/pprof endpoints allow gflag mutation and memory dumps. Set true only for debugging "
               "on a trusted network.");
DS_DEFINE_bool(brpc_enable_circuit_breaker, false,
               "Enable brpc circuit breaker on client->worker channels. Default false (off); set true to "
               "let brpc auto-isolate peers with high error rates (EMA window: 1500 samples / 10% short, "
               "3000 samples / 5% long; ELIMIT is ignored). When the breaker isolates a socket, new RPCs "
               "fast-fail EHOSTDOWN; health-check (3s interval) revives the socket automatically. The "
               "per-channel BrpcChannelConfig::enable_circuit_breaker still controls individual channels "
               "when this global flag is true (mesh channels have it off by default).");
DS_DEFINE_uint64_dynamic(client_slow_log_process_slower_than, 2000,
                 "Client-side in-process processing latency threshold (microseconds) for slow-log and latency "
                 "summary. Default 2000 (2ms). 0 means disabled. When enabled, requests whose in-process phases "
                 "exceed this threshold will include a latency summary in the access log.");
DS_DEFINE_uint64_dynamic(client_slow_log_rpc_slower_than, 5000,
                 "Client-side RPC operation latency threshold (microseconds) for slow-log and latency summary. "
                 "Default 5000 (5ms). 0 means disabled. When enabled, requests whose RPC phases exceed this threshold "
                 "will include a latency summary in the access log.");
