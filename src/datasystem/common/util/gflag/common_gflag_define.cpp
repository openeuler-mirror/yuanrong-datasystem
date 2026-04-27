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
DS_DEFINE_uint32(urma_connection_size, 0, "[DEPRECATED] No longer used. JFS/JFR are created per-connection.");
DS_DEFINE_bool(urma_register_whole_arena, true,
               "Register the whole arena as segment during init, otherwise, register each object as a segment.");
DS_DEFINE_bool(enable_ub_numa_affinity, false,
               "Enable UB numa affinity optimization when URMA and whole-arena registration are both enabled.");
DS_DEFINE_string(shared_memory_distribution_policy, "none",
                 "Shared memory NUMA distribution policy. Optional values: "
                 "'none', 'interleave_all_numa', 'interleave_affinity_numa'.");
DS_DEFINE_bool(urma_event_mode, false, "Uses interrupt mode to poll completion events.");
DS_DEFINE_bool(enable_worker_worker_batch_get, false, "Enable worker->worker OC batch get, default false.");
DS_DEFINE_bool(enable_remote_h2d, false, "Option to turn on Remote H2D, default false.");
DS_DEFINE_string(urma_mode, "UB", "Option to enable URMA over IB or UB, default UB to run with URMA over UB.");
DS_DEFINE_bool(enable_urma, false, "Option to turn on urma for OC worker to worker data transfer, default false.");
DS_DEFINE_bool(enable_transport_fallback, true, "Enable the fast transport fallback to tcp transport.");
DS_DEFINE_uint32(
    eviction_reserve_mem_threshold_mb, 10240,
    "The reserved memory (MB) is determined by min(shared_memory_size_mb*0.1, eviction_reserve_mem_threshold_mb). "
    "Eviction begins when memory drops below this threshold.The valid range is 100-102400.");
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

DS_DEFINE_bool(spill_to_remote_worker, false,
               "It indicates that when node resources are insufficient, "
               "it supports spilling memory to the memory of other nodes.");
DS_DEFINE_uint32(node_timeout_s, 60, "maximum time interval before a node is considered lost");
DS_DEFINE_int32(io_thread_nice, -15,
                "Nice value applied to selected IO threads with setpriority. Valid range is [-20, 19].");
DS_DEFINE_int32(zmq_client_io_context, 5,
                "Optimize the performance of the client stub. Default value 5. "
                "The higher the throughput, the higher the value, but should be in range [1, 32]");
DS_DEFINE_int32(zmq_chunk_sz, 1048576, "Parallel payload split chunk size. Default to 1048756 bytes");
DS_DEFINE_bool(cache_rpc_session, true, "Deprecated: This flag is deprecated and will be removed in future releases.");
DS_DEFINE_string(etcd_address, "", "Address of ETCD server");
DS_DEFINE_int32(oc_worker_worker_direct_port, 0,
                "Direct tcp/ip port for WorkerWorkerOCService. 0 -- disable this direction connection");
DS_DEFINE_int32(sc_worker_worker_direct_port, 0,
                "Direct tcp/ip port for WorkerWorkerSCService. 0 -- disable this direction connection");
DS_DEFINE_bool(enable_pipeline_h2d, false, "Enable pipeline H2D. Default is false");
DS_DEFINE_int32(pipeline_h2d_thread_num, 64, "Pipeline H2D worker thread number. Default value 64");
