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
    { 13, "worker_rpc_get_remote_object_latency", MetricType::HISTOGRAM, "us" },
    { 14, "worker_process_create_latency", MetricType::HISTOGRAM, "us" },
    { 15, "worker_process_publish_latency", MetricType::HISTOGRAM, "us" },
    { 16, "worker_process_get_latency", MetricType::HISTOGRAM, "us" },
    { 17, "worker_urma_write_latency", MetricType::HISTOGRAM, "us" },
    { 18, "worker_tcp_write_latency", MetricType::HISTOGRAM, "us" },
    { 19, "worker_to_client_total_bytes", MetricType::COUNTER, "bytes" },
    { 20, "worker_from_client_total_bytes", MetricType::COUNTER, "bytes" },
    { 21, "worker_object_count", MetricType::GAUGE, "count" },
    { 22, "worker_allocated_memory_size", MetricType::GAUGE, "bytes" },
};
static_assert(sizeof(KV_METRIC_DESCS) / sizeof(KV_METRIC_DESCS[0])
              == static_cast<size_t>(KvMetricId::KV_METRIC_END));

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
