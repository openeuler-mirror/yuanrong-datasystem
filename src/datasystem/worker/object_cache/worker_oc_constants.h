/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_OC_CONSTANTS_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_OC_CONSTANTS_H

#include <cstdint>

namespace datasystem {
namespace object_cache {

constexpr int32_t WORKER_BATCH_GET_PARALLELISM = 16;
constexpr int32_t WORKER_BATCH_GET_PARALLEL_THRESHOLD = 100;
constexpr uint64_t WORKER_BATCH_GET_AGGREGATE_KEY_MAX_COUNT = 1'024;
constexpr uint64_t WORKER_BATCH_GET_REQUESTER_AGGREGATE_OBJECT_SIZE_THRESHOLD = 1'048'576;
constexpr uint64_t WORKER_BATCH_GET_AGGREGATE_OBJECT_MAX_SIZE = 65'536;
constexpr uint64_t WORKER_BATCH_GET_AGGREGATE_GROUP_MAX_SIZE = 2'097'152;

constexpr bool ShouldUseParallelWorkerBatchGet(int32_t requestCount, bool isPipelineRequest,
                                               bool isFastTransportEnabled)
{
    return !isPipelineRequest && requestCount > WORKER_BATCH_GET_PARALLEL_THRESHOLD && isFastTransportEnabled;
}

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_OC_CONSTANTS_H
