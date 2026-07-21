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

#include "datasystem/common/metrics/kv_metrics.h"

#include <mutex>

#include "datasystem/common/util/status_helper.h"

namespace datasystem::metrics {
namespace {
constexpr MetricDesc KV_METRIC_DESCS[] = {
    { 0, "client_put_request_total", MetricType::COUNTER, "count" },
    { 1, "client_put_error_total", MetricType::COUNTER, "count" },
    { 2, "client_get_request_total", MetricType::COUNTER, "count" },
    { 3, "client_get_error_total", MetricType::COUNTER, "count" },
    { 4, "client_rpc_create_latency", MetricType::HISTOGRAM, "us" },
    { 5, "client_rpc_publish_latency", MetricType::HISTOGRAM, "us" },
    { 6, "client_rpc_get_latency", MetricType::HISTOGRAM, "us" },
    { 7, "client_put_urma_write_total_bytes", MetricType::COUNTER, "bytes" },
    { 8, "client_put_tcp_write_total_bytes", MetricType::COUNTER, "bytes" },
    { 9, "client_get_urma_read_total_bytes", MetricType::COUNTER, "bytes" },
    { 10, "client_get_tcp_read_total_bytes", MetricType::COUNTER, "bytes" },
    { 11, "worker_rpc_create_meta_latency", MetricType::HISTOGRAM, "us" },
    { 12, "worker_rpc_query_meta_latency", MetricType::HISTOGRAM, "us" },
    { 13, "worker_rpc_remote_get_outbound_latency", MetricType::HISTOGRAM, "us" },
    { 14, "worker_process_create_latency", MetricType::HISTOGRAM, "us" },
    { 15, "worker_process_publish_latency", MetricType::HISTOGRAM, "us" },
    { 16, "worker_process_get_latency", MetricType::HISTOGRAM, "us" },
    { 17, "urma_write_latency", MetricType::HISTOGRAM, "us" },
    { 18, "urma_wait_latency", MetricType::HISTOGRAM, "us" },
    { 19, "worker_tcp_write_latency", MetricType::HISTOGRAM, "us" },
    { 20, "worker_to_client_total_bytes", MetricType::COUNTER, "bytes" },
    { 21, "worker_object_count", MetricType::GAUGE, "count" },
    { 22, "worker_allocated_memory_size", MetricType::GAUGE, "bytes" },
    { 23, "zmq_send_failure_total", MetricType::COUNTER, "count" },
    { 24, "zmq_receive_failure_total", MetricType::COUNTER, "count" },
    { 25, "zmq_send_try_again_total", MetricType::COUNTER, "count" },
    { 26, "zmq_receive_try_again_total", MetricType::COUNTER, "count" },
    { 27, "zmq_network_error_total", MetricType::COUNTER, "count" },
    { 28, "zmq_last_error_number", MetricType::GAUGE, "" },
    { 29, "zmq_gateway_recreate_total", MetricType::COUNTER, "count" },
    { 30, "zmq_event_disconnect_total", MetricType::COUNTER, "count" },
    { 31, "zmq_event_handshake_failure_total", MetricType::COUNTER, "count" },
    { 32, "zmq_send_io_latency", MetricType::HISTOGRAM, "us" },
    { 33, "zmq_receive_io_latency", MetricType::HISTOGRAM, "us" },
    { 34, "zmq_rpc_serialize_latency", MetricType::HISTOGRAM, "us" },
    { 35, "zmq_rpc_deserialize_latency", MetricType::HISTOGRAM, "us" },
    { 36, "zmq_server_task_delay", MetricType::HISTOGRAM, "us" },
    // RPC Queue Flow Latency (values via RecordLatencyMetric: ns sampled, histogram stores us)
    { 37, "zmq_client_req_queuing_latency", MetricType::HISTOGRAM, "us" },
    { 38, "zmq_client_rsp_queuing_latency", MetricType::HISTOGRAM, "us" },
    { 39, "zmq_server_req_queuing_latency", MetricType::HISTOGRAM, "us" },
    { 40, "zmq_server_exec_latency", MetricType::HISTOGRAM, "us" },
    { 41, "zmq_server_rsp_queuing_latency", MetricType::HISTOGRAM, "us" },
    { 42, "zmq_rpc_e2e_latency", MetricType::HISTOGRAM, "us" },
    { 43, "zmq_rpc_network_latency", MetricType::HISTOGRAM, "us" },
    // Alloc/free counters: requested/logical bytes (see Allocator::AllocateMemory/FreeMemory comments).
    { 44, "worker_allocator_alloc_bytes_total", MetricType::COUNTER, "bytes" },
    { 45, "worker_allocator_free_bytes_total", MetricType::COUNTER, "bytes" },
    { 46, "worker_shm_unit_created_total", MetricType::COUNTER, "count" },
    { 47, "worker_shm_unit_destroyed_total", MetricType::COUNTER, "count" },
    { 48, "worker_shm_ref_add_total", MetricType::COUNTER, "count" },
    { 49, "worker_shm_ref_remove_total", MetricType::COUNTER, "count" },
    { 50, "worker_shm_ref_table_size", MetricType::GAUGE, "count" },
    { 51, "worker_shm_ref_table_bytes", MetricType::GAUGE, "bytes" },
    { 52, "worker_remove_client_refs_total", MetricType::COUNTER, "count" },
    { 53, "worker_object_erase_total", MetricType::COUNTER, "count" },
    { 54, "master_object_meta_table_size", MetricType::GAUGE, "count" },
    // Depth of TTL time queue (timedObj_); not a separate count of failedObjects_ retry metadata alone.
    { 55, "master_ttl_pending_size", MetricType::GAUGE, "count" },
    { 56, "master_ttl_fire_total", MetricType::COUNTER, "count" },
    { 57, "master_ttl_delete_success_total", MetricType::COUNTER, "count" },
    { 58, "master_ttl_delete_failed_total", MetricType::COUNTER, "count" },
    { 59, "master_ttl_retry_total", MetricType::COUNTER, "count" },
    { 60, "client_async_release_queue_size", MetricType::GAUGE, "count" },
    { 61, "client_dec_ref_skipped_total", MetricType::COUNTER, "count" },
    { 62, "urma_import_jfr", MetricType::HISTOGRAM, "us" },
    { 63, "urma_inflight_wr_count", MetricType::HISTOGRAM, "count" },
    { 64, "urma_nanosleep_latency", MetricType::HISTOGRAM, "us" },
    { 65, "worker_rpc_remote_get_inbound_latency", MetricType::HISTOGRAM, "us" },
    { 66, "worker_get_threadpool_queue_latency", MetricType::HISTOGRAM, "us" },
    { 67, "worker_get_threadpool_exec_latency", MetricType::HISTOGRAM, "us" },
    { 68, "worker_get_meta_addr_hashring_latency", MetricType::HISTOGRAM, "us" },
    { 69, "worker_get_post_query_meta_phase_latency", MetricType::HISTOGRAM, "us" },
    { 70, "worker_inflight_remote_get_request", MetricType::GAUGE, "count" },
    { 71, "zmq_server_poll_handle_latency", MetricType::HISTOGRAM, "us" },
    { 72, "urma_connection_setup_latency", MetricType::HISTOGRAM, "us" },
    { 73, "urma_jetty_create_latency", MetricType::HISTOGRAM, "us" },
    { 74, "urma_jetty_recreate_latency", MetricType::HISTOGRAM, "us" },
    { 75, "client_exist_request_total", MetricType::COUNTER, "count" },
    { 76, "client_exist_error_total", MetricType::COUNTER, "count" },
    { 77, "worker_process_exist_latency", MetricType::HISTOGRAM, "us" },
    { 78, "client_put_shm_write_total_bytes", MetricType::COUNTER, "bytes" },
    { 79, "client_put_local_write_total_bytes", MetricType::COUNTER, "bytes" },
    { 80, "brpc_client_req_framework_latency", MetricType::HISTOGRAM, "us" },
    { 81, "brpc_remote_processing_latency", MetricType::HISTOGRAM, "us" },
    { 82, "brpc_client_rsp_framework_latency", MetricType::HISTOGRAM, "us" },
    { 83, "brpc_server_req_queue_latency", MetricType::HISTOGRAM, "us" },
    { 84, "brpc_server_exec_latency", MetricType::HISTOGRAM, "us" },
    { 85, "brpc_server_rsp_queue_latency", MetricType::HISTOGRAM, "us" },
    { 86, "brpc_rpc_e2e_latency", MetricType::HISTOGRAM, "us" },
    { 87, "brpc_rpc_network_residual_latency", MetricType::HISTOGRAM, "us" },
    { 88, "worker_from_client_shm_total_bytes", MetricType::COUNTER, "bytes" },
    { 89, "worker_from_client_local_total_bytes", MetricType::COUNTER, "bytes" },
    { 90, "worker_from_client_tcp_total_bytes", MetricType::COUNTER, "bytes" },
    { 91, "worker_from_client_urma_total_bytes", MetricType::COUNTER, "bytes" },
    { 92, "client_get_shm_read_total_bytes", MetricType::COUNTER, "bytes" },
    { 93, "client_exist_redirect_total", MetricType::COUNTER, "count" },
    { 94, "client_exist_connection_retry_total", MetricType::COUNTER, "count" },
    { 95, "client_direct_batch_get_rpc_total", MetricType::COUNTER, "count" },
    { 96, "client_direct_batch_get_object_total", MetricType::COUNTER, "count" },
    { 97, "client_direct_batch_get_replica_retry_total", MetricType::COUNTER, "count" },
    { 98, "client_direct_batch_get_ub_split_total", MetricType::COUNTER, "count" },
    { 99, "client_direct_batch_get_tcp_fallback_total", MetricType::COUNTER, "count" },
    { 100, "client_create_request_total", MetricType::COUNTER, "count" },
    { 101, "client_create_error_total", MetricType::COUNTER, "count" },
    { 102, "client_create_allocated_bytes", MetricType::COUNTER, "bytes" },
    { 103, "worker_create_allocated_bytes", MetricType::COUNTER, "bytes" },
    { 104, "worker_kv_event_published_batches_total", MetricType::COUNTER, "count" },
    { 105, "worker_kv_event_published_events_total", MetricType::COUNTER, "count" },
    { 106, "worker_kv_event_dropped_total", MetricType::COUNTER, "count" },
    { 107, "worker_kv_event_skipped_unparsed_keys_total", MetricType::COUNTER, "count" },
};
static_assert(sizeof(KV_METRIC_DESCS) / sizeof(KV_METRIC_DESCS[0]) == static_cast<size_t>(KvMetricId::KV_METRIC_END));

std::mutex g_initMutex;
bool g_inited = false;
}  // namespace

Status InitKvMetrics()
{
    std::lock_guard<std::mutex> lock(g_initMutex);
    if (g_inited) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(Init(KV_METRIC_DESCS, sizeof(KV_METRIC_DESCS) / sizeof(KV_METRIC_DESCS[0])));
    g_inited = true;
    return Status::OK();
}

const MetricDesc *GetKvMetricDescs(size_t &count)
{
    count = sizeof(KV_METRIC_DESCS) / sizeof(KV_METRIC_DESCS[0]);
    return KV_METRIC_DESCS;
}

void ResetKvMetricsForTest()
{
    std::lock_guard<std::mutex> lock(g_initMutex);
    ResetForTest();
    g_inited = false;
}
}  // namespace datasystem::metrics
