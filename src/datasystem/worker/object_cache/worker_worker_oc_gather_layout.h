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

/** Description: Declares worker Batch Get aggregate-gather layout validation. */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_WORKER_OC_GATHER_LAYOUT_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_WORKER_OC_GATHER_LAYOUT_H

#include <cstdint>
#include <vector>

#include "datasystem/protos/worker_object.pb.h"

namespace datasystem {
namespace object_cache {

struct AggregateGatherSubgroup {
    uint64_t startIndex = 0;
    uint64_t requestCount = 0;
    uint64_t byteSize = 0;
};

/**
 * @brief Build aggregate-gather subgroups and prove that every subgroup has a compatible remote target layout.
 * @return True only when the producer explicitly allows gather and every size, boundary, identity, and offset is valid.
 */
bool BuildAggregateGatherPlan(const BatchGetObjectRemoteReqPb &request, uint64_t metadataSize,
                              uint64_t maxObjectSize, uint64_t maxSubgroupBytes, uint64_t maxSubgroupObjects,
                              std::vector<AggregateGatherSubgroup> &subgroups);

/**
 * @brief Decide whether the receiver may use aggregate gather for this production request.
 */
bool ShouldUseAggregateGather(const BatchGetObjectRemoteReqPb &request, bool isPipelineRequest,
                              uint64_t metadataSize, uint64_t maxObjectSize, uint64_t maxSubgroupBytes,
                              uint64_t maxSubgroupObjects, std::vector<AggregateGatherSubgroup> &subgroups);

/**
 * @brief Attach or clear the producer's aggregate-memory attestation.
 */
void SetAggregateGatherAttestation(bool allRequestsUseAggregateMemory, uint64_t metadataSize,
                                   BatchGetObjectRemoteReqPb &request);

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_WORKER_OC_GATHER_LAYOUT_H
