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

/**
 * Description: Schema mapping ResMetricName to JSON field names and the ods record whitelist.
 */

#include "datasystem/common/metrics/resource_json_schema.h"

namespace datasystem {
namespace {
// DESC_TABLE is indexed by static_cast<size_t>(ResMetricName). The table holds one descriptor per
// real metric (0..RES_METRICS_END-1); RES_METRICS_END is the sentinel, so the count equals its value.
constexpr size_t DESC_COUNT = static_cast<size_t>(ResMetricName::RES_METRICS_END);

// X-macro group names from res_metrics.def. Used only to assert DESC_TABLE stays in sync with def:
// adding a new metric group in def without a matching DESC_TABLE entry trips the static_assert below.
#define METRIC_NAME(name, ...) #name,
constexpr const char *RES_METRIC_GROUP_NAMES[] = {
#include "datasystem/common/metrics/res_metrics.def"
};
#undef METRIC_NAME
constexpr size_t RES_METRIC_GROUP_COUNT = sizeof(RES_METRIC_GROUP_NAMES) / sizeof(RES_METRIC_GROUP_NAMES[0]);
static_assert(RES_METRIC_GROUP_COUNT == DESC_COUNT + 1, "DESC_TABLE out of sync with res_metrics.def");

// fieldNames are omitted for recordGroup=false entries: AppendGroupJson returns early for them,
// so the sub-field names would be dead data. Only ods-recorded groups carry their field list.
const std::array<ResourceFieldDesc, DESC_COUNT> DESC_TABLE = {
    ResourceFieldDesc{ { "memory_usage", "physical_memory_usage", "total_limit", "worker_share_memory_usage",
        "sc_memory_usage", "sc_memory_limit" },
        { true, true, true, true, false, false }, '/', true, "shared_memory" },
    ResourceFieldDesc{ { "space_usage", "physical_space_usage", "total_limit", "worker_spill_hard_disk_usage" },
        { true, true, true, true }, '/', true, "spill_hard_disk" },
    ResourceFieldDesc{ { "active_client_count" }, { true }, '/', true, "active_client_count" },
    ResourceFieldDesc{ { "object_count" }, { true }, '/', true, "object_count" },
    ResourceFieldDesc{ { "object_size" }, { true }, '/', true, "object_size" },
    ResourceFieldDesc{ { "idle_num", "current_total_num", "max_thread_num", "waiting_task_num", "thread_pool_usage" },
        { true, true, true, true, true }, '/', true, "worker_oc_service_thread_pool" },
    ResourceFieldDesc{ { "idle_num", "current_total_num", "max_thread_num", "waiting_task_num", "thread_pool_usage" },
        { true, true, true, true, true }, '/', true, "worker_worker_oc_service_thread_pool" },
    ResourceFieldDesc{ { "idle_num", "current_total_num", "max_thread_num", "waiting_task_num", "thread_pool_usage" },
        { true, true, true, true, true }, '/', true, "master_worker_oc_service_thread_pool" },
    ResourceFieldDesc{ { "idle_num", "current_total_num", "max_thread_num", "waiting_task_num", "thread_pool_usage" },
        { true, true, true, true, true }, '/', true, "master_oc_service_thread_pool" },
    ResourceFieldDesc{ { "current_size", "total_limit", "etcd_queue_usage" }, { true, true, true }, '/', true,
        "etcd_queue" },
    ResourceFieldDesc{ { "success_rate" }, { true }, '/', true, "etcd_request_success_rate" },
    ResourceFieldDesc{ {}, {}, '/', false, "obs_request_success_rate" },
    ResourceFieldDesc{ { "idle_num", "current_total_num", "max_thread_num", "waiting_task_num", "thread_pool_usage" },
        { true, true, true, true, true }, '/', true, "master_async_tasks_thread_pool" },
    ResourceFieldDesc{ {}, {}, '/', false, "stream_count" },
    ResourceFieldDesc{ {}, {}, '/', false, "worker_sc_service_thread_pool" },
    ResourceFieldDesc{ {}, {}, '/', false, "worker_worker_sc_service_thread_pool" },
    ResourceFieldDesc{ {}, {}, '/', false, "master_worker_sc_service_thread_pool" },
    ResourceFieldDesc{ {}, {}, '/', false, "master_sc_service_thread_pool" },
    ResourceFieldDesc{ {}, {}, '/', false, "stream_remote_send_success_rate" },
    ResourceFieldDesc{ {}, {}, '/', false, "shared_disk" },
    ResourceFieldDesc{ {}, {}, '/', false, "sc_local_cache" },
    ResourceFieldDesc{ { "mem_hit_num", "disk_hit_num", "l2_hit_num", "remote_hit_num", "miss_num" },
        { true, true, true, true, true }, '/', true, "oc_hit_num" },
    ResourceFieldDesc{ { "leak_count" }, { true }, '/', true, "brpc_stream_leak_count" },
    ResourceFieldDesc{ { "queue_size" }, { true }, '/', true, "deferred_cleanup_queue_size" },
    ResourceFieldDesc{ { "cumul_spill_in_count", "cumul_spill_in_bytes", "cumul_spill_in_fail",
        "cumul_spill_out_count", "cumul_spill_out_bytes",
        "cumul_spill_evict_count", "cumul_spill_evict_bytes",
        "current_hour_spill_in_count", "current_hour_spill_in_bytes", "current_hour_spill_in_fail",
        "current_hour_spill_out_count", "current_hour_spill_out_bytes",
        "current_hour_spill_evict_count", "current_hour_spill_evict_bytes" },
        { true, true, true, true, true, true, true, true, true, true, true, true, true, true },
        '/', true, "spill_io_stats" },
};

const ResourceFieldDesc NULL_DESC{ {}, {}, '\0', false, "" };
}  // namespace

const ResourceFieldDesc &GetResourceFieldDesc(ResMetricName name)
{
    auto idx = static_cast<size_t>(name);
    if (idx < DESC_COUNT) {
        return DESC_TABLE[idx];
    }
    return NULL_DESC;
}

void SplitResourceFields(const std::string &raw, char sep, std::vector<std::string> &tokens)
{
    tokens.clear();
    if (sep == '\0') {
        tokens.emplace_back(raw);
        return;
    }
    size_t start = 0;
    size_t pos = 0;
    while ((pos = raw.find(sep, start)) != std::string::npos) {
        tokens.emplace_back(raw.substr(start, pos - start));
        start = pos + 1;
    }
    tokens.emplace_back(raw.substr(start));
}
}  // namespace datasystem
