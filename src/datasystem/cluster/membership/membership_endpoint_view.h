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
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

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
 * The view owns transient observations and a membership cache. It does not own membership watches,
 * decide Failure, select placement owners or write backend state.
 */
class MembershipEndpointView final {
public:
    /**
     * @brief Bind the process-local endpoint view to the published Snapshot state.
     * @param[in] snapshots Snapshot holder that outlives this view.
     */
    explicit MembershipEndpointView(const TopologySnapshotState &snapshots, bool enableWriteRedirect = false);

    bool SupportsWriteRedirect() const noexcept;

    // The runtime commits these advisory updates only while owning the source watch.
    Status UpdateMembership(const std::string &address, MembershipValue value, int64_t revision);
    Status DeleteMembership(const std::string &address, int64_t revision);

    void ClearMemberships();
    // A full refresh establishes completeness; individual watch events cannot prove it after a clear.
    Status GetHostIds(std::unordered_map<std::string, std::string> &hostIds) const;
    Status RefreshMemberships(const std::vector<MembershipRecord> &members, int64_t revision);

    std::vector<std::string> GetWriteCandidates(const std::string &excludedAddress,
                                                const std::string &selectionKey,
                                                size_t maxCandidates) const;

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
     * @return K_OK; K_NOT_FOUND when the address is absent; K_NOT_READY before the first Snapshot.
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

    /**
     * @brief Wait for the process-local topology watch to publish at least one version.
     * @param[in] minimumVersion Minimum acceptable topology version.
     * @param[in] deadline Absolute wait deadline.
     * @param[out] snapshot Current immutable Snapshot; unchanged on failure.
     * @return K_OK when ready; K_TRY_AGAIN when the local watch remains behind at the deadline.
     */
    Status WaitForSnapshotVersion(uint64_t minimumVersion, std::chrono::steady_clock::time_point deadline,
                                  std::shared_ptr<const TopologySnapshot> &snapshot) const;

private:
    Status SetMembershipLocked(const std::string &address, std::optional<MembershipValue> value, int64_t revision);
    /**
     * @brief Resolve a current member's usable local observation.
     * @param[in] member Current immutable topology member.
     * @param[in] topologyVersion Current Snapshot version.
     * @return Current matching availability or UNKNOWN.
     */
    EndpointAvailability ResolveLocalAvailability(const Member &member, uint64_t topologyVersion) const;

    const TopologySnapshotState &snapshots_;
    const bool writeRedirectEnabled_;
    struct MembershipState {
        std::optional<MembershipValue> value;
        int64_t revision{ 0 };
        size_t readyIndex{ 0 };
    };
    // Reject delayed events for entries already removed by a full snapshot, which no longer have per-member revisions.
    int64_t snapshotRevision_{ 0 };
    mutable std::shared_mutex membershipMutex_;
    std::unordered_map<std::string, MembershipState> memberships_;
    std::vector<std::string> readyCandidateAddresses_;
    // Mirrors whether observationsByAddress_ is empty so empty-table readers can avoid mutex_.
    std::atomic<bool> hasObservations_{ false };
    // Protects observationsByAddress_; writers update hasObservations_ while holding this mutex.
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, EndpointObservation> observationsByAddress_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_MEMBERSHIP_MEMBERSHIP_ENDPOINT_VIEW_H
