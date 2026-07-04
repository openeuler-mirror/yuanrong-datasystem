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
 * Description: Local membership snapshot manager for common topology.
 */
#ifndef DATASYSTEM_TOPOLOGY_MEMBERSHIP_CLUSTER_MEMBERSHIP_H
#define DATASYSTEM_TOPOLOGY_MEMBERSHIP_CLUSTER_MEMBERSHIP_H

#include <memory>
#include <mutex>

#include "datasystem/topology/membership/cluster_registry.h"
#include "datasystem/topology/membership/membership_types.h"

namespace datasystem {
namespace topology {

enum class MembershipRuntimeState {
    NOT_READY,
    REBUILDING,
    READY,
    DEGRADED,
    STOPPED,
};

class ClusterMembership final {
public:
    ClusterMembership(IClusterRegistry &registry, TopologyNodeId localNodeId);
    ~ClusterMembership();
    ClusterMembership(const ClusterMembership &) = delete;
    ClusterMembership &operator=(const ClusterMembership &) = delete;
    ClusterMembership(ClusterMembership &&) = delete;
    ClusterMembership &operator=(ClusterMembership &&) = delete;

    /**
     * @brief Rebuild membership snapshot through the existing cluster table.
     * @return K_OK on success; registry status on list failure.
     */
    Status Rebuild();

    /**
     * @brief Stop publishing membership snapshots.
     */
    void Stop();

    /**
     * @brief Apply one coordination-backend event that was delivered by the shared ICoordinationBackend event
     * dispatcher.
     * @param[in] event Raw coordination-backend event.
     * @return K_OK on success; K_NOT_FOUND for unrelated events; K_NOT_READY before first rebuild.
     */
    Status HandleStoreEvent(const CoordinationEvent &event);

    /**
     * @brief Get the latest immutable membership snapshot.
     * @param[out] snapshot Snapshot pointer.
     * @return K_OK when available; K_NOT_READY before first rebuild or after Stop.
     */
    Status GetSnapshot(std::shared_ptr<const MembershipSnapshot> &snapshot) const;
    Status GetRecord(const TopologyNodeId &nodeId, MembershipRecord &record) const;
    Status GetReadyEndpoint(const TopologyNodeId &nodeId, TopologyEndpoint &endpoint) const;
    Status ListReadyMembers(std::vector<MembershipRecord> &members) const;

    /**
     * @brief Get the last published membership snapshot for diagnostic/read-state queries.
     * @param[out] snapshot Snapshot pointer.
     * @return K_OK when a last-good snapshot exists; K_NOT_READY before first rebuild or after Stop.
     */
    Status GetLastSnapshot(std::shared_ptr<const MembershipSnapshot> &snapshot) const;

    /**
     * @brief Build a local scale-in request from the latest snapshot.
     * @param[in] reason Local scale-in reason.
     * @param[out] request Request to submit to the Coordinator topology boundary.
     * @param[out] observedRevision Membership revision used for validation.
     * @return K_OK on success; K_INVALID for invalid reason; K_NOT_READY/K_NOT_FOUND when local member is unavailable.
     */
    Status BuildLocalScaleInRequest(ScaleInReason reason, ScaleInRequest &request, Revision &observedRevision) const;

    /**
     * @brief Return current internal runtime state for tests and diagnostics.
     * @return Internal runtime state.
     */
    MembershipRuntimeState RuntimeState() const;

private:
    Status ApplyMembershipEvent(const MembershipWatchEvent &event);
    void PublishSnapshot(std::shared_ptr<const MembershipSnapshot> snapshot, MembershipRuntimeState state);
    static Status ValidateScaleInReason(ScaleInReason reason);

    IClusterRegistry &registry_;
    TopologyNodeId localNodeId_;
    mutable std::mutex mutex_;
    std::shared_ptr<const MembershipSnapshot> snapshot_;
    MembershipRuntimeState state_{ MembershipRuntimeState::NOT_READY };
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_MEMBERSHIP_CLUSTER_MEMBERSHIP_H
