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

#ifndef DATASYSTEM_COMMON_METRICS_KV_METRICS_H
#define DATASYSTEM_COMMON_METRICS_KV_METRICS_H

#include <cstddef>
#include <cstdint>

#include "datasystem/common/metrics/metrics.h"

namespace datasystem::metrics {
enum class KvMetricId : uint16_t {
    // Confirm no external numeric ID dependency before deleting intermediate entries
    // Confirm no external numeric ID dependency before deleting intermediate entries
    CLIENT_PUT_REQUEST_TOTAL = 0,
    CLIENT_PUT_ERROR_TOTAL = 1,
    CLIENT_GET_REQUEST_TOTAL = 2,
    CLIENT_GET_ERROR_TOTAL = 3,
    CLIENT_RPC_CREATE_LATENCY = 4,
    CLIENT_RPC_PUBLISH_LATENCY = 5,
    CLIENT_RPC_GET_LATENCY = 6,
    CLIENT_PUT_URMA_WRITE_TOTAL_BYTES = 7,
    CLIENT_PUT_TCP_WRITE_TOTAL_BYTES = 8,
    CLIENT_GET_URMA_READ_TOTAL_BYTES = 9,
    CLIENT_GET_TCP_READ_TOTAL_BYTES = 10,
    WORKER_RPC_CREATE_META_LATENCY = 11,
    WORKER_RPC_QUERY_META_LATENCY = 12,
    /** Worker (caller) -> remote worker GetObject* */
    WORKER_RPC_REMOTE_GET_OUTBOUND_LATENCY = 13,
    WORKER_PROCESS_CREATE_LATENCY = 14,
    WORKER_PROCESS_PUBLISH_LATENCY = 15,
    WORKER_PROCESS_GET_LATENCY = 16,
    URMA_WRITE_LATENCY = 17,
    URMA_WAIT_LATENCY = 18,
    WORKER_TCP_WRITE_LATENCY = 19,
    WORKER_TO_CLIENT_TOTAL_BYTES = 20,
    WORKER_OBJECT_COUNT = 21,
    WORKER_ALLOCATED_MEMORY_SIZE = 22,
    // RPC Queue Flow Latency
    // Memory Allocator Metrics
    ALLOCATOR_ALLOC_BYTES_TOTAL = 44,
    ALLOCATOR_FREE_BYTES_TOTAL = 45,
    SHM_UNIT_CREATED_TOTAL = 46,
    SHM_UNIT_DESTROYED_TOTAL = 47,
    WORKER_SHM_REF_ADD_TOTAL = 48,
    WORKER_SHM_REF_REMOVE_TOTAL = 49,
    WORKER_SHM_REF_TABLE_SIZE = 50,
    WORKER_SHM_REF_TABLE_BYTES = 51,
    WORKER_REMOVE_CLIENT_REFS_TOTAL = 52,
    WORKER_OBJECT_ERASE_TOTAL = 53,
    MASTER_OBJECT_META_TABLE_SIZE = 54,
    MASTER_TTL_PENDING_SIZE = 55,
    MASTER_TTL_FIRE_TOTAL = 56,
    MASTER_TTL_DELETE_SUCCESS_TOTAL = 57,
    MASTER_TTL_DELETE_FAILED_TOTAL = 58,
    MASTER_TTL_RETRY_TOTAL = 59,
    CLIENT_ASYNC_RELEASE_QUEUE_SIZE = 60,
    CLIENT_DEC_REF_SKIPPED_TOTAL = 61,
    // URMA
    URMA_IMPORT_JFR = 62,
    URMA_INFLIGHT_WR_COUNT = 63,
    URMA_NANOSLEEP_LATENCY = 64,
    /** Other worker's pull: GetObject* service path */
    WORKER_RPC_REMOTE_GET_INBOUND_LATENCY = 65,
    /** MsgQ: submit -> thread start */
    WORKER_GET_THREADPOOL_QUEUE_LATENCY = 66,
    /** Thread pool: ProcessGetObjectRequest */
    WORKER_GET_THREADPOOL_EXEC_LATENCY = 67,
    /** Non-central: hash/resolve master address */
    WORKER_GET_META_ADDR_HASHRING_LATENCY = 68,
    /** After QueryMetadataFromMaster OK: local follow-up */
    WORKER_GET_POST_QUERY_META_PHASE_LATENCY = 69,
    /** Worker outbound BatchGetObjectRemote requests currently in progress */
    WORKER_INFLIGHT_REMOTE_GET_REQUEST = 70,
    /** ZMQ server poll handle latency */
    // Append new metric families here to preserve existing numeric IDs.
    URMA_CONNECTION_SETUP_LATENCY = 72,
    URMA_JETTY_CREATE_LATENCY = 73,
    URMA_JETTY_RECREATE_LATENCY = 74,
    CLIENT_EXIST_REQUEST_TOTAL = 75,
    CLIENT_EXIST_ERROR_TOTAL = 76,
    WORKER_PROCESS_EXIST_LATENCY = 77,
    CLIENT_PUT_SHM_WRITE_TOTAL_BYTES = 78,
    CLIENT_PUT_LOCAL_WRITE_TOTAL_BYTES = 79,
    BRPC_CLIENT_REQ_FRAMEWORK_LATENCY = 80,
    BRPC_REMOTE_PROCESSING_LATENCY = 81,
    BRPC_CLIENT_RSP_FRAMEWORK_LATENCY = 82,
    BRPC_SERVER_REQ_QUEUE_LATENCY = 83,
    BRPC_SERVER_EXEC_LATENCY = 84,
    BRPC_SERVER_RSP_QUEUE_LATENCY = 85,
    BRPC_RPC_E2E_LATENCY = 86,
    BRPC_RPC_NETWORK_RESIDUAL_LATENCY = 87,
    WORKER_FROM_CLIENT_SHM_TOTAL_BYTES = 88,
    WORKER_FROM_CLIENT_LOCAL_TOTAL_BYTES = 89,
    WORKER_FROM_CLIENT_TCP_TOTAL_BYTES = 90,
    WORKER_FROM_CLIENT_URMA_TOTAL_BYTES = 91,
    CLIENT_GET_SHM_READ_TOTAL_BYTES = 92,
    CLIENT_EXIST_REDIRECT_TOTAL = 93,
    CLIENT_EXIST_CONNECTION_RETRY_TOTAL = 94,
    CLIENT_DIRECT_BATCH_GET_RPC_TOTAL = 95,
    CLIENT_DIRECT_BATCH_GET_OBJECT_TOTAL = 96,
    CLIENT_DIRECT_BATCH_GET_REPLICA_RETRY_TOTAL = 97,
    CLIENT_DIRECT_BATCH_GET_UB_SPLIT_TOTAL = 98,
    CLIENT_DIRECT_BATCH_GET_TCP_FALLBACK_TOTAL = 99,
    CLIENT_CREATE_REQUEST_TOTAL = 100,
    CLIENT_CREATE_ERROR_TOTAL = 101,
    CLIENT_CREATE_ALLOCATED_BYTES = 102,
    WORKER_CREATE_ALLOCATED_BYTES = 103,
    WORKER_KV_EVENT_PUBLISHED_BATCHES_TOTAL = 104,
    WORKER_KV_EVENT_PUBLISHED_EVENTS_TOTAL = 105,
    WORKER_KV_EVENT_DROPPED_TOTAL = 106,
    WORKER_KV_EVENT_SKIPPED_UNPARSED_KEYS_TOTAL = 107,
    // Shared-memory OOMs classified by extent-hook outcome:
    // FRESH includes mmap allocation or extent commit failure; it does not prove a new contiguous extent is absent.
    // REUSABLE means jemalloc OOM without observing either failure in this allocation attempt.
    SHM_FRESH_EXTENT_OOM_TOTAL = 108,
    SHM_REUSABLE_EXTENT_OOM_TOTAL = 109,
    // SHM transporter observability — appended at the end to preserve existing numeric IDs (review 181252307).
    CLIENT_SHM_MMAP_SUCCESS_TOTAL = 110,
    CLIENT_SHM_MMAP_FALLBACK_TOTAL = 111,
    CLIENT_SHM_ZERO_COPY_SET_TOTAL = 112,
    CLIENT_SHM_PAYLOAD_FALLBACK_SET_TOTAL = 113,
    CLIENT_SHM_GET_DEGRADE_TO_TRANSPORT_TOTAL = 114,
    // ExpiredRequestInterceptor: count of client-expired brpc requests dropped before the handler.
    BRPC_EXPIRED_REQUEST_DROP_TOTAL = 115,
    // Brpc RPC latency for FAILED requests (cntl.Failed() == true). Separate buckets so overload
    // failure residuals (e.g. deadline-exceeded rejects) do not pollute the success e2e/network
    // residual histograms, which would mislead operators into blaming the network for a server-side
    // reject/queue bottleneck. Appended at the end to preserve existing numeric IDs.
    BRPC_RPC_E2E_FAIL_LATENCY = 116,
    BRPC_RPC_NETWORK_RESIDUAL_FAIL_LATENCY = 117,
    // Coordinator RPC requests dispatched to service handlers. Append-only to preserve existing numeric IDs.
    COORDINATOR_RPC_PUT_REQUEST_TOTAL = 118,
    COORDINATOR_RPC_RANGE_REQUEST_TOTAL = 119,
    COORDINATOR_RPC_DELETE_RANGE_REQUEST_TOTAL = 120,
    COORDINATOR_RPC_WATCH_RANGE_REQUEST_TOTAL = 121,
    COORDINATOR_RPC_CANCEL_WATCH_REQUEST_TOTAL = 122,
    COORDINATOR_RPC_KEEP_ALIVE_REQUEST_TOTAL = 123,
    COORDINATOR_RPC_GET_COORDINATOR_ID_REQUEST_TOTAL = 124,
    COORDINATOR_RPC_REPORT_TOPOLOGY_RECOVERY_CANDIDATE_REQUEST_TOTAL = 125,
    COORDINATOR_RPC_GET_CLUSTER_RAW_SNAPSHOT_REQUEST_TOTAL = 126,
    COORDINATOR_RPC_GET_RAFT_BOOTSTRAP_STATE_REQUEST_TOTAL = 127,
    COORDINATOR_RPC_ENSURE_LEADER_MEMBERSHIP_REQUEST_TOTAL = 128,
    COORDINATOR_RPC_REPORT_WORKER_LIVENESS_REQUEST_TOTAL = 129,
    // Successfully delivered watch-notification RPC batches and the events carried by them.
    COORDINATOR_WATCH_NOTIFICATION_SENT_BATCHES_TOTAL = 130,
    COORDINATOR_WATCH_NOTIFICATION_SENT_EVENTS_TOTAL = 131,
    // Serialized EventReqPb bytes in successfully delivered business notifications; excludes transport framing.
    COORDINATOR_WATCH_NOTIFICATION_SENT_BYTES_TOTAL = 132,
    COORDINATOR_WATCH_RPC_LATENCY = 133,
    COORDINATOR_WATCH_RPC_FAILURE_TOTAL = 134,
    COORDINATOR_WATCH_CHANNELS = 135,
    COORDINATOR_WATCH_FAN_OUT_EVENTS_TOTAL = 136,
    COORDINATOR_WATCH_NOTIFICATION_INFLIGHT_REQUESTS = 137,
    COORDINATOR_WATCH_NOTIFICATION_INFLIGHT_BYTES = 138,
    COORDINATOR_WATCH_PROBE_INFLIGHT_REQUESTS = 139,
    COORDINATOR_WATCH_PROBE_INFLIGHT_BYTES = 140,
    KV_METRIC_END = 141,
};

Status InitKvMetrics();
const MetricDesc *GetKvMetricDescs(size_t &count);
void ResetKvMetricsForTest();
}  // namespace datasystem::metrics

#define DS_METRIC_JOIN2(a, b) a##b
#define DS_METRIC_JOIN(a, b) DS_METRIC_JOIN2(a, b)
#define METRIC_INC(metricId) ::datasystem::metrics::GetCounter(static_cast<uint16_t>(metricId)).Inc()
#define METRIC_ADD(metricId, value) ::datasystem::metrics::GetCounter(static_cast<uint16_t>(metricId)).Inc(value)
#define METRIC_ERROR_IF(cond, metricId) \
    do {                                \
        if (cond) {                     \
            METRIC_INC(metricId);       \
        }                               \
    } while (false)
#define METRIC_TIMER(metricId) \
    ::datasystem::metrics::ScopedTimer DS_METRIC_JOIN(metricsScopedTimer, __LINE__)(static_cast<uint16_t>(metricId))

#endif  // DATASYSTEM_COMMON_METRICS_KV_METRICS_H
