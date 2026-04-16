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
    WORKER_RPC_GET_REMOTE_OBJECT_LATENCY,
    WORKER_PROCESS_CREATE_LATENCY,
    WORKER_PROCESS_PUBLISH_LATENCY,
    WORKER_PROCESS_GET_LATENCY,
    WORKER_URMA_WRITE_LATENCY,
    WORKER_TCP_WRITE_LATENCY,
    WORKER_TO_CLIENT_TOTAL_BYTES,
    WORKER_FROM_CLIENT_TOTAL_BYTES,
    WORKER_OBJECT_COUNT,
    WORKER_ALLOCATED_MEMORY_SIZE,
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
