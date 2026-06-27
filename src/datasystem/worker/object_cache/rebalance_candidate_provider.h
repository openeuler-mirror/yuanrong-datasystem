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
 * Description: Selects memory rebalance candidates from the eviction list.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_REBALANCE_CANDIDATE_PROVIDER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_REBALANCE_CANDIDATE_PROVIDER_H

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>

#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

class RebalanceCandidateProvider {
public:
    RebalanceCandidateProvider(std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                               std::shared_ptr<ObjectTable> objectTable);
    ~RebalanceCandidateProvider() = default;

    /**
     * @brief Select candidate object keys and sizes for a rebalance task.
     * @param[in] targetBytes The maximum bytes expected in this local batch.
     * @param[in] maxObjectCount The maximum object count expected in this local batch.
     * @param[out] candidates The selected object key to data size map.
     * @return Status of the call.
     */
    Status Select(uint64_t targetBytes, size_t maxObjectCount, std::unordered_map<std::string, uint64_t> &candidates);

private:
    Status TryGetObjectSize(const std::string &objectKey, uint64_t &objectSize);

    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    std::shared_ptr<ObjectTable> objectTable_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif
