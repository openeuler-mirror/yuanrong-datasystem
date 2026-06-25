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
 * Description: Topology algorithm registry.
 */
#include "datasystem/topology/algorithm/algorithm_registry.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

Status AlgorithmRegistry::RegisterAlgorithm(std::unique_ptr<const ITopologyAlgorithm> algorithm)
{
    CHECK_FAIL_RETURN_STATUS(algorithm != nullptr, K_INVALID, "topology algorithm is null");
    auto id = algorithm->GetAlgorithmId();
    CHECK_FAIL_RETURN_STATUS(!id.empty(), K_INVALID, "topology algorithm id is empty");
    CHECK_FAIL_RETURN_STATUS(entries_.find(id) == entries_.end(), K_INVALID, "topology algorithm is duplicated");

    const auto *routing = dynamic_cast<const IRoutingAlgorithm *>(algorithm.get());
    const auto *planning = dynamic_cast<const IPlanningAlgorithm *>(algorithm.get());
    CHECK_FAIL_RETURN_STATUS(routing != nullptr || planning != nullptr, K_INVALID, "topology algorithm has no facet");

    try {
        entries_.reserve(entries_.size() + 1);
        routingFacets_.reserve(routingFacets_.size() + 1);
        planningFacets_.reserve(planningFacets_.size() + 1);
        if (routing != nullptr) {
            routingFacets_.emplace(id, routing);
        }
        if (planning != nullptr) {
            planningFacets_.emplace(id, planning);
        }
        entries_.emplace(id, std::move(algorithm));
    } catch (...) {
        routingFacets_.erase(id);
        planningFacets_.erase(id);
        entries_.erase(id);
        throw;
    }
    return Status::OK();
}

Status AlgorithmRegistry::ResolveRouting(const AlgorithmId &id, const IRoutingAlgorithm *&algorithm) const
{
    algorithm = nullptr;
    auto iter = routingFacets_.find(id);
    CHECK_FAIL_RETURN_STATUS(iter != routingFacets_.end(), K_NOT_FOUND, "routing algorithm is not registered");
    algorithm = iter->second;
    return Status::OK();
}

Status AlgorithmRegistry::ResolvePlanning(const AlgorithmId &id, const IPlanningAlgorithm *&algorithm) const
{
    algorithm = nullptr;
    auto iter = planningFacets_.find(id);
    CHECK_FAIL_RETURN_STATUS(iter != planningFacets_.end(), K_NOT_FOUND, "planning algorithm is not registered");
    algorithm = iter->second;
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
