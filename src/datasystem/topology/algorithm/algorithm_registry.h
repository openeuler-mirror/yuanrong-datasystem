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
#ifndef DATASYSTEM_TOPOLOGY_ALGORITHM_ALGORITHM_REGISTRY_H
#define DATASYSTEM_TOPOLOGY_ALGORITHM_ALGORITHM_REGISTRY_H

#include <memory>
#include <unordered_map>

#include "datasystem/topology/algorithm/topology_algorithm.h"

namespace datasystem {
namespace topology {

class IAlgorithmRegistry {
public:
    virtual ~IAlgorithmRegistry() = default;

    /**
     * @brief Register one read-only topology algorithm implementation.
     * @param[in] algorithm Algorithm object whose ownership is transferred to the registry.
     * @return K_OK on success; K_INVALID when algorithm is null, id is empty, duplicated, or has no facet.
     */
    virtual Status RegisterAlgorithm(std::unique_ptr<const ITopologyAlgorithm> algorithm) = 0;

    /**
     * @brief Resolve one routing algorithm by id.
     * @param[in] id Algorithm id selected by placement policy.
     * @param[out] algorithm Non-owning algorithm pointer valid until registry destruction.
     * @return K_OK on success; K_NOT_FOUND when id is unknown.
     */
    virtual Status ResolveRouting(const AlgorithmId &id, const IRoutingAlgorithm *&algorithm) const = 0;

    /**
     * @brief Resolve one planning algorithm by id.
     * @param[in] id Algorithm id selected by Coordinator planning.
     * @param[out] algorithm Non-owning algorithm pointer valid until registry destruction.
     * @return K_OK on success; K_NOT_FOUND when id is unknown or has no planning facet.
     */
    virtual Status ResolvePlanning(const AlgorithmId &id, const IPlanningAlgorithm *&algorithm) const = 0;
};

class AlgorithmRegistry final : public IAlgorithmRegistry {
public:
    AlgorithmRegistry() = default;
    ~AlgorithmRegistry() override = default;
    AlgorithmRegistry(const AlgorithmRegistry &) = delete;
    AlgorithmRegistry &operator=(const AlgorithmRegistry &) = delete;
    AlgorithmRegistry(AlgorithmRegistry &&) = delete;
    AlgorithmRegistry &operator=(AlgorithmRegistry &&) = delete;

    Status RegisterAlgorithm(std::unique_ptr<const ITopologyAlgorithm> algorithm) override;
    Status ResolveRouting(const AlgorithmId &id, const IRoutingAlgorithm *&algorithm) const override;
    Status ResolvePlanning(const AlgorithmId &id, const IPlanningAlgorithm *&algorithm) const override;

private:
    std::unordered_map<AlgorithmId, std::unique_ptr<const ITopologyAlgorithm>> entries_;
    std::unordered_map<AlgorithmId, const IRoutingAlgorithm *> routingFacets_;
    std::unordered_map<AlgorithmId, const IPlanningAlgorithm *> planningFacets_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_ALGORITHM_ALGORITHM_REGISTRY_H
