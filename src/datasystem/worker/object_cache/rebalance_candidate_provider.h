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
 * Description: Selects memory rebalance candidate objects from the eviction list.
 *
 * The base owns the shared object validation + rebalancing-mark logic (TryGetObjectSize). The memory
 * strategy scans oldest eviction-list nodes; the heat strategy scans hot nodes (heat > threshold) sorted
 * by ascending heat so the least-hot hot primary copies migrate first. After a successful batch, the heat
 * Source-side eviction-list cleanup is committed transactionally by AsyncResourceReleaser when the exact migrated
 * object version is removed; candidate providers never infer cleanup from an object-table lookup.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_REBALANCE_CANDIDATE_PROVIDER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_REBALANCE_CANDIDATE_PROVIDER_H

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

struct RebalanceCandidateSession {
    std::vector<EvictionList::Node> candidateWindow;
    size_t nextCandidate{ 0 };
};

class RebalanceCandidateProvider {
public:
    using ObjectHeatMap = std::unordered_map<std::string, double>;

    virtual ~RebalanceCandidateProvider() = default;

    /**
     * @brief Select candidate object keys and sizes for a rebalance task.
     * @param[in] targetBytes The maximum bytes expected in this local batch.
     * @param[in] maxObjectCount The maximum object count expected in this local batch.
     * @param[out] candidates The selected object key to data size map.
     * @param[out] objectHeats Point-in-time heat snapshots; empty for the memory strategy.
     * @param[in] skipKeys Optional set of object keys to skip during scanning (previously
     *            skipped due to metadata-not-found within the same task).
     * @return Status of the call.
     */
    virtual Status Select(RebalanceCandidateSession &session, uint64_t targetBytes, size_t maxObjectCount,
                          std::unordered_map<std::string, uint64_t> &candidates, ObjectHeatMap &objectHeats,
                          const std::unordered_set<std::string> *skipKeys = nullptr) = 0;

protected:
    RebalanceCandidateProvider(std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                               std::shared_ptr<ObjectTable> objectTable);
    Status TryGetObjectSize(const std::string &objectKey, uint64_t &objectSize);

    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    std::shared_ptr<ObjectTable> objectTable_;
};

class MemoryRebalanceCandidateProvider : public RebalanceCandidateProvider {
public:
    MemoryRebalanceCandidateProvider(std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                                     std::shared_ptr<ObjectTable> objectTable)
        : RebalanceCandidateProvider(std::move(evictionManager), std::move(objectTable))
    {
    }
    ~MemoryRebalanceCandidateProvider() override = default;
    Status Select(RebalanceCandidateSession &session, uint64_t targetBytes, size_t maxObjectCount,
                  std::unordered_map<std::string, uint64_t> &candidates, ObjectHeatMap &objectHeats,
                  const std::unordered_set<std::string> *skipKeys = nullptr) override;
};

class HeatRebalanceCandidateProvider : public RebalanceCandidateProvider {
public:
    HeatRebalanceCandidateProvider(std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                                   std::shared_ptr<ObjectTable> objectTable)
        : RebalanceCandidateProvider(std::move(evictionManager), std::move(objectTable))
    {
    }
    ~HeatRebalanceCandidateProvider() override = default;
    Status Select(RebalanceCandidateSession &session, uint64_t targetBytes, size_t maxObjectCount,
                  std::unordered_map<std::string, uint64_t> &candidates, ObjectHeatMap &objectHeats,
                  const std::unordered_set<std::string> *skipKeys = nullptr) override;
};

/**
 * @brief Build the rebalance candidate provider selected by the rebalance_strategy flag.
 */
std::unique_ptr<RebalanceCandidateProvider> MakeRebalanceCandidateProvider(
    std::shared_ptr<WorkerOcEvictionManager> evictionManager, std::shared_ptr<ObjectTable> objectTable);

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_REBALANCE_CANDIDATE_PROVIDER_H
