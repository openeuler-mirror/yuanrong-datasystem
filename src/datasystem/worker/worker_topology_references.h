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
 * Description: Narrow cluster-topology references injected into Worker business owners.
 */
#ifndef DATASYSTEM_WORKER_WORKER_TOPOLOGY_REFERENCES_H
#define DATASYSTEM_WORKER_WORKER_TOPOLOGY_REFERENCES_H

#include <atomic>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem::worker {

/**
 * @brief Borrowed narrow views and Worker-owned immutable/runtime facts.
 */
struct WorkerTopologyReferences {
    const cluster::PlacementFacade *placement{ nullptr };
    cluster::MembershipEndpointView *membership{ nullptr };
    std::string localAddress;
    std::string metadataAddress;
    bool centralizedMetadata{ false };
    bool localMetadataMaster{ false };
    bool restart{ false };
    bool controlBackendAvailableAtStartup{ true };
    const std::atomic<bool> *localExiting{ nullptr };
};

/**
 * @brief Identity- and epoch-bound metadata migration RPC fence.
 */
struct TopologyMigrationFence {
    uint64_t topologyVersion{ 0 };
    uint64_t batchEpoch{ 0 };
    cluster::MemberIdentity source;
    cluster::MemberIdentity target;
};

/**
 * @brief Load the current immutable Snapshot through the narrow membership view.
 * @param[in] references Worker-owned topology dependencies.
 * @param[out] snapshot Current immutable topology snapshot.
 * @return K_OK on success; K_NOT_READY when topology dependencies or the snapshot are unavailable.
 */
Status LoadTopologySnapshot(const WorkerTopologyReferences *references,
                            std::shared_ptr<const cluster::TopologySnapshot> &snapshot);

/**
 * @brief Reject delayed migration RPCs that no longer match the local committed topology epoch.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] fence RPC-carried migration fence.
 * @return K_OK for the current ordinary batch; K_TRY_AGAIN for a lagging local Snapshot; K_INVALID when stale.
 */
Status ValidateTopologyMigrationFence(const WorkerTopologyReferences *references, const TopologyMigrationFence &fence);

/**
 * @brief Validate optional migration RPC fence fields before applying remote metadata.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] topologyVersion Exact topology version, or zero for a non-topology migration.
 * @param[in] batchEpoch Exact ordinary-batch epoch, or zero for a non-topology migration.
 * @param[in] sourceMemberId Exact binary source member id, or empty for a non-topology migration.
 * @param[in] targetMemberId Exact binary target member id, or empty for a non-topology migration.
 * @param[in] sourceAddress Canonical source member address carried by the RPC.
 * @return K_OK for a non-topology request or current fence; K_TRY_AGAIN when local state lags; K_INVALID otherwise.
 */
Status ValidateTopologyMigrationRequest(const WorkerTopologyReferences *references, uint64_t topologyVersion,
                                        uint64_t batchEpoch, const std::string &sourceMemberId,
                                        const std::string &targetMemberId, const std::string &sourceAddress);

/**
 * @brief Verify that the local committed member can serve topology-routed traffic.
 * @param[in] references Worker-owned topology dependencies.
 * @return K_OK when the local member may serve; an availability status otherwise.
 */
Status CheckTopologyServingReady(const WorkerTopologyReferences *references);

/**
 * @brief Return committed member addresses, including orderly-leaving owners.
 * @param[in] references Worker-owned topology dependencies.
 * @return Committed member addresses, or an empty set when no snapshot is available.
 */
std::set<std::string> GetValidTopologyMembers(const WorkerTopologyReferences *references);

/**
 * @brief Return ACTIVE member addresses only.
 * @param[in] references Worker-owned topology dependencies.
 * @return ACTIVE member addresses, or an empty set when no snapshot is available.
 */
std::set<std::string> GetActiveTopologyMembers(const WorkerTopologyReferences *references);

/**
 * @brief Return FAILED member addresses.
 * @param[in] references Worker-owned topology dependencies.
 * @return FAILED member addresses, or an empty set when no snapshot is available.
 */
std::unordered_set<std::string> GetFailedTopologyMembers(const WorkerTopologyReferences *references);

/**
 * @brief Return whether one address is PRE_LEAVING.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] address Canonical member address.
 * @return True when the current snapshot marks address PRE_LEAVING.
 */
bool IsTopologyMemberPreLeaving(const WorkerTopologyReferences *references, const std::string &address);

/**
 * @brief Return whether the Worker host has started its exit gate.
 * @param[in] references Worker-owned topology dependencies.
 * @return True when the local lifecycle gate is exiting.
 */
bool IsLocalTopologyMemberExiting(const WorkerTopologyReferences *references);

/**
 * @brief Select the next committed member in canonical member order.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] address Member address from which selection starts.
 * @param[out] standbyAddress Selected successor address.
 * @return K_OK when a successor exists; an input or availability status otherwise.
 */
Status GetStandbyTopologyMember(const WorkerTopologyReferences *references, const std::string &address,
                                std::string &standbyAddress);

/**
 * @brief Select the next ACTIVE member, excluding every member leaving in the same batch.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] address Address from which selection starts; the address itself may already be leaving.
 * @param[out] standbyAddress Selected ACTIVE successor address.
 * @return K_OK when an ACTIVE successor exists; an input or availability status otherwise.
 */
Status GetActiveStandbyTopologyMember(const WorkerTopologyReferences *references, const std::string &address,
                                      std::string &standbyAddress);

/**
 * @brief Resolve the local stable member id, falling back to canonical address before admission.
 * @param[in] references Worker-owned topology dependencies.
 * @return Stable member id, canonical address fallback, or an empty string for invalid dependencies.
 */
std::string GetLocalTopologyMemberId(const WorkerTopologyReferences *references);

/**
 * @brief Project the local topology identity to the printable Worker UUID contract.
 * @param[in] references Worker-owned topology dependencies.
 * @return Printable identity required by the existing Worker-facing contract.
 */
std::string GetLocalWorkerUuid(const WorkerTopologyReferences *references);

/**
 * @brief Resolve one key's metadata owner, honoring Worker centralized-metadata mode.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] key Business key used for placement.
 * @param[out] address Resolved metadata owner.
 * @return K_OK on success; an input, topology, or address parsing status otherwise.
 */
Status ResolveTopologyOwner(const WorkerTopologyReferences *references, std::string_view key, HostPort &address);

/**
 * @brief Evaluate whether one key must be redirected away from this Worker.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] key Business key used for placement.
 * @param[out] redirect Whether the request should be redirected.
 * @param[out] moving Whether the key belongs to an in-flight ScaleOut transfer range.
 * @param[out] targetAddress Redirect destination when redirect is true.
 * @param[out] topologyVersion Exact Snapshot version used for the decision when requested.
 * @return K_OK on success; an input or topology resolution status otherwise.
 */
Status EvaluateTopologyRedirect(const WorkerTopologyReferences *references, std::string_view key, bool &redirect,
                                bool &moving, std::string &targetAddress, uint64_t *topologyVersion = nullptr);

/**
 * @brief Check current topology/local endpoint evidence for one address.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] address Member endpoint to check.
 * @param[in] allowUnknown Whether local UNKNOWN endpoint evidence is acceptable.
 * @return K_OK when the endpoint is usable; an input or availability status otherwise.
 */
Status CheckTopologyMemberConnection(const WorkerTopologyReferences *references, const HostPort &address,
                                     bool allowUnknown = false);

/**
 * @brief Return committed member addresses as parsed HostPort values.
 * @param[in] references Worker-owned topology dependencies.
 * @param[out] addresses Parsed committed member endpoints.
 * @return K_OK on success; a topology or address parsing status otherwise.
 */
Status GetTopologyMemberAddresses(const WorkerTopologyReferences *references, std::vector<HostPort> &addresses);

/**
 * @brief Select up to `count` ACTIVE peers other than the local Worker.
 * @param[in] references Worker-owned topology dependencies.
 * @param[in] count Maximum number of peers to return.
 * @param[out] addresses Selected ACTIVE peer addresses.
 * @return K_OK on success; an input or topology availability status otherwise.
 */
Status GetActiveTopologyPeers(const WorkerTopologyReferences *references, uint32_t count,
                              std::vector<std::string> &addresses);

}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_TOPOLOGY_REFERENCES_H
