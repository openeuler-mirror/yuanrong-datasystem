/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Process-local endpoint observations composed with immutable topology.
 */
#ifndef DATASYSTEM_CLUSTER_MEMBERSHIP_MEMBERSHIP_ENDPOINT_VIEW_H
#define DATASYSTEM_CLUSTER_MEMBERSHIP_MEMBERSHIP_ENDPOINT_VIEW_H

#include <atomic>
#include <chrono>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

/**
 * @brief One version/id-fenced process-local endpoint probe result.
 */
struct EndpointObservation {
    MemberIdentity identity;
    uint64_t topologyVersion{ 0 };
    EndpointAvailability availability{ EndpointAvailability::UNKNOWN };
    std::chrono::steady_clock::time_point observedAt;
};

/**
 * @brief Combined query result; topology state remains globally authoritative.
 */
struct MemberEndpoint {
    MemberIdentity identity;
    MemberState topologyState{ MemberState::INITIAL };
    EndpointAvailability localAvailability{ EndpointAvailability::UNKNOWN };
};

/**
 * @brief Thread-safe composition of immutable topology identity and local endpoint observations.
 * The view owns only transient observations. It does not watch membership,
 * decide Failure, select placement owners or write backend state.
 */
class MembershipEndpointView final {
public:
    /**
     * @brief Bind the process-local endpoint view to the published Snapshot state.
     * @param[in] snapshots Snapshot holder that outlives this view.
     */
    explicit MembershipEndpointView(const TopologySnapshotState &snapshots);

    /**
     * @brief Destroy all process-local observations.
     */
    ~MembershipEndpointView() = default;

    /**
     * @brief Disable copying a view bound to Snapshot state.
     */
    MembershipEndpointView(const MembershipEndpointView &) = delete;

    /**
     * @brief Disable copy assignment of a view bound to Snapshot state.
     */
    MembershipEndpointView &operator=(const MembershipEndpointView &) = delete;

    /**
     * @brief Publish one local observation bound to exact topology version and identity.
     * @param[in] observation Version/id-fenced process-local observation.
     * @return K_OK on success; K_INVALID when the current Snapshot no longer matches.
     */
    Status UpdateObservation(const EndpointObservation &observation);

    /**
     * @brief Remove observations not belonging to the current Snapshot; idempotent.
     */
    void RemoveStaleObservations();

    /**
     * @brief Resolve topology identity/state and local availability by address.
     * @param[in] address Canonical member address.
     * @param[out] endpoint Combined query result.
     * @return K_OK, K_NOT_FOUND, or K_NOT_READY before the first Snapshot.
     */
    Status ResolveByAddress(const std::string &address, MemberEndpoint &endpoint) const;

    /**
     * @brief Resolve topology identity/state and local availability by id.
     * @param[in] id Exact 16-byte membership id.
     * @param[out] endpoint Combined query result.
     * @return K_OK, K_NOT_FOUND, K_NOT_READY, or K_INVALID for a non-16-byte id.
     */
    Status ResolveById(const std::string &id, MemberEndpoint &endpoint) const;

    /**
     * @brief Load the current immutable topology Snapshot for member-set queries.
     * @param[out] snapshot Shared immutable Snapshot.
     * @return K_OK, or K_NOT_READY before the first authoritative topology read.
     */
    Status GetSnapshot(std::shared_ptr<const TopologySnapshot> &snapshot) const;

private:
    /**
     * @brief Resolve a current member's usable local observation.
     * @param[in] member Current immutable topology member.
     * @param[in] topologyVersion Current Snapshot version.
     * @return Current matching availability or UNKNOWN.
     */
    EndpointAvailability ResolveLocalAvailability(const Member &member, uint64_t topologyVersion) const;

    const TopologySnapshotState &snapshots_;
    // Mirrors whether observationsByAddress_ is empty so empty-table readers can avoid mutex_.
    std::atomic<bool> hasObservations_{ false };
    // Protects observationsByAddress_; writers update hasObservations_ while holding this mutex.
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, EndpointObservation> observationsByAddress_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_MEMBERSHIP_MEMBERSHIP_ENDPOINT_VIEW_H
