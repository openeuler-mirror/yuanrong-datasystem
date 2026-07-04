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
 * Description: Background owner that rebuilds routing snapshots from committed topology facts.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_SNAPSHOT_REBUILDER_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_SNAPSHOT_REBUILDER_H

#include <mutex>

#include "datasystem/topology/algorithm/topology_algorithm.h"
#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/model/topology_types.h"
#include "datasystem/topology/repository/topology_repository.h"
#include "datasystem/topology/routing/routing_view.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

struct TopologySnapshotRebuilderStats {
    uint64_t successfulRebuilds{ 0 };
    uint64_t failedRebuilds{ 0 };
    uint64_t ignoredEvents{ 0 };
    int64_t lastTopologyVersion{ 0 };
    Revision lastTopologyRevision{ 0 };
    Status lastStatus;
};

class TopologySnapshotRebuilder final {
public:
    TopologySnapshotRebuilder(RoutingView &routingView, ITopologyRoutingRepository &repository,
                              const IRoutingAlgorithm &routingAlgorithm);
    ~TopologySnapshotRebuilder() = default;
    TopologySnapshotRebuilder(const TopologySnapshotRebuilder &) = delete;
    TopologySnapshotRebuilder &operator=(const TopologySnapshotRebuilder &) = delete;
    TopologySnapshotRebuilder(TopologySnapshotRebuilder &&) = delete;
    TopologySnapshotRebuilder &operator=(TopologySnapshotRebuilder &&) = delete;

    /**
     * @brief Read committed topology/task facts and publish one immutable routing snapshot.
     * @return K_OK if a snapshot was published; repository, codec, and algorithm errors are returned unchanged.
     */
    Status RebuildFromCommittedTopology();

    /**
     * @brief Decode one committed-topology backend event and publish a new snapshot when the event is relevant.
     * @param[in] event Raw event delivered by ICoordinationBackend.
     * @return K_OK for applied or ignored events; K_INVALID for malformed exact committed-topology payload.
     */
    Status ApplyCommittedTopologyEvent(const CoordinationEvent &event);

    /**
     * @brief Return a copy of rebuild counters and last observed topology version.
     */
    TopologySnapshotRebuilderStats GetStats() const;

private:
    void RecordSuccess(int64_t topologyVersion, Revision revision);
    void RecordFailure(const Status &status);
    void RecordIgnoredEvent();

    RoutingView &routingView_;
    ITopologyRoutingRepository &repository_;
    const IRoutingAlgorithm &routingAlgorithm_;
    mutable std::mutex mutex_;
    TopologySnapshotRebuilderStats stats_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_SNAPSHOT_REBUILDER_H
