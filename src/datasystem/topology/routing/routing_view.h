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
 * Description: R0 routing snapshot view.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_VIEW_H
#define DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_VIEW_H

#include <memory>
#include <shared_mutex>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/algorithm/topology_algorithm.h"
#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/repository/topology_repository.h"
#include "datasystem/topology/routing/routing_snapshot.h"

namespace datasystem {
namespace topology {

class IRoutingView {
public:
    virtual ~IRoutingView() = default;

    /**
     * @brief Load the currently published immutable routing snapshot.
     * @param[out] snapshot Published snapshot.
     * @return K_OK if a snapshot is available, K_NOT_READY if no snapshot has been published.
     *
     * Request threads only load a local immutable snapshot. This method must not perform repository/backend IO,
     * CAS/List/Watch, task scan, migration, recovery, cleanup, or success-path logging.
     */
    virtual Status GetSnapshot(std::shared_ptr<const RoutingSnapshot> &snapshot) const = 0;
};

class RoutingView final : public IRoutingView {
public:
    RoutingView() = default;
    ~RoutingView() override = default;

    Status GetSnapshot(std::shared_ptr<const RoutingSnapshot> &snapshot) const override;

    /**
     * @brief Publish an immutable routing snapshot.
     * @param[in] snapshot Fully constructed snapshot.
     *
     * The caller must build the snapshot before calling Publish. Publish only swaps the shared pointer.
     */
    void Publish(std::shared_ptr<const RoutingSnapshot> snapshot);

    /**
     * @brief Apply one raw backend event through the repository committed-topology decoder.
     * @param[in] repository Repository contract that classifies and decodes the event.
     * @param[in] event Raw coordination-backend event.
     * @return K_OK for applied or ignored events; K_INVALID for malformed exact committed topology payload.
     *
     * Only exact committed topology updates publish a new routing snapshot. Task, notify, and unrelated subkeys are
     * ignored, and exact deletes keep the last usable snapshot published.
     */
    Status ApplyCommittedTopologyEvent(ITopologyRoutingRepository &repository, const CoordinationEvent &event,
                                       const IRoutingAlgorithm &algorithm);

    /**
     * @brief Publish a routing snapshot from the current committed topology.
     * @param[in] repository Repository used to read the committed topology.
     * @return K_OK if the snapshot was published; K_NOT_FOUND/K_NOT_READY if committed topology is not yet usable.
     *
     * This is for topology background/startup paths that already allow repository IO. Request paths must continue to
     * use GetSnapshot only.
     */
    Status ApplyCommittedTopology(ITopologyRoutingRepository &repository, const IRoutingAlgorithm &algorithm);

private:
    mutable std::shared_timed_mutex mutex_;
    std::shared_ptr<const RoutingSnapshot> snapshot_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_ROUTING_VIEW_H
