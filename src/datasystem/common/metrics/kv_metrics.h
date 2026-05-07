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
    CLIENT_PUT_REQUEST_TOTAL = 0,
    CLIENT_PUT_ERROR_TOTAL,
    CLIENT_GET_REQUEST_TOTAL,
    CLIENT_GET_ERROR_TOTAL,
    CLIENT_RPC_CREATE_LATENCY,
    CLIENT_RPC_PUBLISH_LATENCY,
    CLIENT_RPC_GET_LATENCY,
    CLIENT_PUT_URMA_WRITE_TOTAL_BYTES,
    CLIENT_PUT_TCP_WRITE_TOTAL_BYTES,
    CLIENT_GET_URMA_READ_TOTAL_BYTES,
    CLIENT_GET_TCP_READ_TOTAL_BYTES,
    WORKER_RPC_CREATE_META_LATENCY,
    WORKER_RPC_QUERY_META_LATENCY,
    /** Worker (caller) -> remote worker GetObject* */
    WORKER_RPC_REMOTE_GET_OUTBOUND_LATENCY,
    WORKER_PROCESS_CREATE_LATENCY,
    WORKER_PROCESS_PUBLISH_LATENCY,
    WORKER_PROCESS_GET_LATENCY,
    WORKER_URMA_WRITE_LATENCY,
    WORKER_URMA_WAIT_LATENCY,
    WORKER_TCP_WRITE_LATENCY,
    WORKER_TO_CLIENT_TOTAL_BYTES,
    WORKER_FROM_CLIENT_TOTAL_BYTES,
    WORKER_OBJECT_COUNT,
    WORKER_ALLOCATED_MEMORY_SIZE,
    ZMQ_SEND_FAILURE_TOTAL,
    ZMQ_RECEIVE_FAILURE_TOTAL,
    ZMQ_SEND_TRY_AGAIN_TOTAL,
    ZMQ_RECEIVE_TRY_AGAIN_TOTAL,
    ZMQ_NETWORK_ERROR_TOTAL,
    ZMQ_LAST_ERROR_NUMBER,
    ZMQ_GATEWAY_RECREATE_TOTAL,
    ZMQ_EVENT_DISCONNECT_TOTAL,
    ZMQ_EVENT_HANDSHAKE_FAILURE_TOTAL,
    ZMQ_SEND_IO_LATENCY,
    ZMQ_RECEIVE_IO_LATENCY,
    ZMQ_RPC_SERIALIZE_LATENCY,
    ZMQ_RPC_DESERIALIZE_LATENCY,
    ZMQ_SERVER_TASK_DELAY,
    // RPC Queue Flow Latency
    ZMQ_CLIENT_REQ_QUEUING_LATENCY,
    ZMQ_CLIENT_RSP_QUEUING_LATENCY,
    ZMQ_SERVER_REQ_QUEUING_LATENCY,
    ZMQ_SERVER_EXEC_LATENCY,
    ZMQ_SERVER_RSP_QUEUING_LATENCY,
    ZMQ_RPC_E2E_LATENCY,
    ZMQ_RPC_NETWORK_LATENCY,
    // Memory Allocator Metrics
    WORKER_ALLOCATOR_ALLOC_BYTES_TOTAL,
    WORKER_ALLOCATOR_FREE_BYTES_TOTAL,
    WORKER_SHM_UNIT_CREATED_TOTAL,
    WORKER_SHM_UNIT_DESTROYED_TOTAL,
    WORKER_SHM_REF_ADD_TOTAL,
    WORKER_SHM_REF_REMOVE_TOTAL,
    WORKER_SHM_REF_TABLE_SIZE,
    WORKER_SHM_REF_TABLE_BYTES,
    WORKER_REMOVE_CLIENT_REFS_TOTAL,
    WORKER_OBJECT_ERASE_TOTAL,
    MASTER_OBJECT_META_TABLE_SIZE,
    MASTER_TTL_PENDING_SIZE,
    MASTER_TTL_FIRE_TOTAL,
    MASTER_TTL_DELETE_SUCCESS_TOTAL,
    MASTER_TTL_DELETE_FAILED_TOTAL,
    MASTER_TTL_RETRY_TOTAL,
    CLIENT_ASYNC_RELEASE_QUEUE_SIZE,
    CLIENT_DEC_REF_SKIPPED_TOTAL,
    // URMA
    URMA_IMPORT_JFR,
    URMA_INFLIGHT_WR_COUNT,
    URMA_NANOSLEEP_LATENCY,
    /** Other worker's pull: GetObject* service path */
    WORKER_RPC_REMOTE_GET_INBOUND_LATENCY,
    /** MsgQ: submit -> thread start */
    WORKER_GET_THREADPOOL_QUEUE_LATENCY,
    /** Thread pool: ProcessGetObjectRequest */
    WORKER_GET_THREADPOOL_EXEC_LATENCY,
    /** Non-central: hash/resolve master address */
    WORKER_GET_META_ADDR_HASHRING_LATENCY,
    /** After QueryMetadataFromMaster OK: local follow-up */
    WORKER_GET_POST_QUERY_META_PHASE_LATENCY,
    /** Worker outbound BatchGetObjectRemote requests currently in progress */
    WORKER_INFLIGHT_REMOTE_GET_REQUEST,
    /** ZMQ server poll handle latency */
    ZMQ_SERVER_POLL_HANDLE_LATENCY,
    KV_METRIC_END
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
