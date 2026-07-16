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
 * Description: Narrow Worker cluster-topology query helpers.
 */
#include "datasystem/worker/worker_topology_references.h"

#include <algorithm>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem::worker {
namespace {
constexpr char TOPOLOGY_READINESS_PROBE_KEY[] = "topology-readiness-probe";

bool IsCommitted(cluster::MemberState state)
{
    return state == cluster::MemberState::ACTIVE || state == cluster::MemberState::PRE_LEAVING
           || state == cluster::MemberState::LEAVING;
}
}  // namespace

Status LoadTopologySnapshot(const WorkerTopologyReferences *references,
                            std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    CHECK_FAIL_RETURN_STATUS(references != nullptr && references->membership != nullptr, K_NOT_READY,
                             "Worker topology membership view is not available.");
    return references->membership->GetSnapshot(snapshot);
}

Status ValidateTopologyMigrationFence(const WorkerTopologyReferences *references, const TopologyMigrationFence &fence)
{
    CHECK_FAIL_RETURN_STATUS(fence.topologyVersion > 0 && fence.batchEpoch > 0, K_INVALID,
                             "invalid topology migration fence version");
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(LoadTopologySnapshot(references, snapshot));
    CHECK_FAIL_RETURN_STATUS(snapshot->Version() >= fence.topologyVersion, K_TRY_AGAIN,
                             "local topology Snapshot is behind migration fence");
    CHECK_FAIL_RETURN_STATUS(snapshot->Version() == fence.topologyVersion && snapshot->GetActiveBatch().has_value()
                                 && snapshot->GetActiveBatch()->epoch == fence.batchEpoch,
                             K_INVALID, "stale topology migration fence");
    const cluster::Member *source = nullptr;
    const cluster::Member *target = nullptr;
    RETURN_IF_NOT_OK(snapshot->FindMemberByAddress(fence.source.address, source));
    RETURN_IF_NOT_OK(snapshot->FindMemberByAddress(fence.target.address, target));
    CHECK_FAIL_RETURN_STATUS(source->identity == fence.source && target->identity == fence.target
                                 && references != nullptr && references->localAddress == fence.target.address,
                             K_INVALID, "topology migration participant generation is stale");
    const auto type = snapshot->GetActiveBatch()->type;
    const bool scaleOut = type == cluster::TopologyChangeType::SCALE_OUT && IsCommitted(source->state)
                          && target->state == cluster::MemberState::JOINING;
    const bool scaleIn = type == cluster::TopologyChangeType::SCALE_IN && source->state == cluster::MemberState::LEAVING
                         && IsCommitted(target->state);
    CHECK_FAIL_RETURN_STATUS(scaleOut || scaleIn, K_INVALID, "topology migration participant state is stale");
    return Status::OK();
}

Status ValidateTopologyMigrationRequest(const WorkerTopologyReferences *references, uint64_t topologyVersion,
                                        uint64_t batchEpoch, const std::string &sourceMemberId,
                                        const std::string &targetMemberId, const std::string &sourceAddress)
{
    const bool hasFence = topologyVersion != 0 || batchEpoch != 0 || !sourceMemberId.empty() || !targetMemberId.empty();
    if (!hasFence) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(topologyVersion > 0 && batchEpoch > 0 && !sourceMemberId.empty() && !targetMemberId.empty()
                                 && !sourceAddress.empty(),
                             K_INVALID, "incomplete topology migration RPC fence");
    CHECK_FAIL_RETURN_STATUS(references != nullptr && !references->localAddress.empty(), K_NOT_READY,
                             "Worker topology references are unavailable for migration fence validation");
    TopologyMigrationFence fence{ topologyVersion, batchEpoch, cluster::MemberIdentity{ sourceMemberId, sourceAddress },
                                  cluster::MemberIdentity{ targetMemberId, references->localAddress } };
    return ValidateTopologyMigrationFence(references, fence);
}

Status CheckTopologyServingReady(const WorkerTopologyReferences *references)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(LoadTopologySnapshot(references, snapshot));
    const cluster::Member *local = nullptr;
    auto rc = snapshot->FindMemberByAddress(references->localAddress, local);
    if (rc.GetCode() == K_NOT_FOUND) {
        RETURN_STATUS(K_NOT_READY, "local topology member is not admitted");
    }
    RETURN_IF_NOT_OK(rc);
    const bool committed = local->state == cluster::MemberState::ACTIVE
                           || local->state == cluster::MemberState::PRE_LEAVING
                           || local->state == cluster::MemberState::LEAVING;
    CHECK_FAIL_RETURN_STATUS(committed, K_NOT_READY, "local topology member is not committed");
    CHECK_FAIL_RETURN_STATUS(references->placement != nullptr, K_NOT_READY, "topology placement facade is unavailable");
    cluster::PlacementDecision decision;
    return references->placement->Locate(TOPOLOGY_READINESS_PROBE_KEY, decision);
}

std::set<std::string> GetValidTopologyMembers(const WorkerTopologyReferences *references)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    if (LoadTopologySnapshot(references, snapshot).IsError()) {
        return {};
    }
    std::set<std::string> members;
    for (const auto *member : snapshot->CommittedMembers()) {
        members.emplace(member->identity.address);
    }
    return members;
}

std::set<std::string> GetActiveTopologyMembers(const WorkerTopologyReferences *references)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    if (LoadTopologySnapshot(references, snapshot).IsError()) {
        return {};
    }
    std::set<std::string> members;
    for (const auto &member : snapshot->Members()) {
        if (member.state == cluster::MemberState::ACTIVE) {
            members.emplace(member.identity.address);
        }
    }
    return members;
}

std::unordered_set<std::string> GetFailedTopologyMembers(const WorkerTopologyReferences *references)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    if (LoadTopologySnapshot(references, snapshot).IsError()) {
        return {};
    }
    std::unordered_set<std::string> members;
    for (const auto &member : snapshot->Members()) {
        if (member.state == cluster::MemberState::FAILED) {
            members.emplace(member.identity.address);
        }
    }
    return members;
}

bool IsTopologyMemberPreLeaving(const WorkerTopologyReferences *references, const std::string &address)
{
    if (references == nullptr || references->membership == nullptr) {
        return false;
    }
    cluster::MemberEndpoint endpoint;
    return references->membership->ResolveByAddress(address, endpoint).IsOk()
           && endpoint.topologyState == cluster::MemberState::PRE_LEAVING;
}

bool IsLocalTopologyMemberExiting(const WorkerTopologyReferences *references)
{
    return references != nullptr && references->localExiting != nullptr && references->localExiting->load();
}

Status GetStandbyTopologyMember(const WorkerTopologyReferences *references, const std::string &address,
                                std::string &standbyAddress)
{
    auto members = GetValidTopologyMembers(references);
    CHECK_FAIL_RETURN_STATUS(members.size() > 1, K_NOT_FOUND, "No standby topology member is available.");
    auto current = members.find(address);
    CHECK_FAIL_RETURN_STATUS(current != members.end(), K_NOT_FOUND, "Topology member is not committed.");
    auto next = std::next(current);
    standbyAddress = next == members.end() ? *members.begin() : *next;
    return Status::OK();
}

Status GetActiveStandbyTopologyMember(const WorkerTopologyReferences *references, const std::string &address,
                                      std::string &standbyAddress)
{
    auto members = GetActiveTopologyMembers(references);
    CHECK_FAIL_RETURN_STATUS(!members.empty(), K_NOT_FOUND, "No ACTIVE topology member is available.");
    auto next = members.upper_bound(address);
    const auto &candidate = next == members.end() ? *members.begin() : *next;
    CHECK_FAIL_RETURN_STATUS(candidate != address, K_NOT_FOUND, "No ACTIVE standby topology member is available.");
    standbyAddress = candidate;
    return Status::OK();
}

std::string GetLocalTopologyMemberId(const WorkerTopologyReferences *references)
{
    if (references == nullptr || references->membership == nullptr) {
        return references == nullptr ? "" : references->localAddress;
    }
    cluster::MemberEndpoint endpoint;
    return references->membership->ResolveByAddress(references->localAddress, endpoint).IsOk()
               ? endpoint.identity.id
               : references->localAddress;
}

std::string GetLocalWorkerUuid(const WorkerTopologyReferences *references)
{
    auto id = GetLocalTopologyMemberId(references);
    return id.size() == UUID_SIZE ? BytesUuidToString(id) : id;
}

Status ResolveTopologyOwner(const WorkerTopologyReferences *references, std::string_view key, HostPort &address)
{
    CHECK_FAIL_RETURN_STATUS(references != nullptr, K_NOT_READY, "Worker topology references are not available.");
    if (references->centralizedMetadata) {
        return address.ParseString(references->metadataAddress);
    }
    CHECK_FAIL_RETURN_STATUS(references->placement != nullptr, K_NOT_READY,
                             "Worker topology placement facade is not available.");
    cluster::PlacementDecision decision;
    RETURN_IF_NOT_OK(references->placement->Locate(key, decision));
    return address.ParseString(decision.committedOwnerAddress);
}

Status EvaluateTopologyRedirect(const WorkerTopologyReferences *references, std::string_view key, bool &redirect,
                                bool &moving, std::string &targetAddress, uint64_t *topologyVersion)
{
    redirect = false;
    moving = false;
    targetAddress.clear();
    if (topologyVersion != nullptr) {
        *topologyVersion = 0;
    }
    CHECK_FAIL_RETURN_STATUS(references != nullptr, K_NOT_READY, "Worker topology references are not available.");
    if (references->centralizedMetadata) {
        targetAddress = references->metadataAddress;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(references->placement != nullptr, K_NOT_READY,
                             "Worker topology placement facade is not available.");
    cluster::RedirectDecision decision;
    RETURN_IF_NOT_OK(references->placement->EvaluateRedirect(key, references->localAddress, decision));
    redirect = decision.action == cluster::RedirectAction::REDIRECT;
    moving = decision.action == cluster::RedirectAction::WAIT;
    targetAddress = decision.GetRedirectTargetAddress();
    if (topologyVersion != nullptr) {
        *topologyVersion = decision.topologyVersion;
    }
    return Status::OK();
}

Status CheckTopologyMemberConnection(const WorkerTopologyReferences *references, const HostPort &address,
                                     bool allowUnknown)
{
    CHECK_FAIL_RETURN_STATUS(references != nullptr && references->membership != nullptr, K_NOT_READY,
                             "Worker topology membership view is not available.");
    cluster::MemberEndpoint endpoint;
    auto rc = references->membership->ResolveByAddress(address.ToString(), endpoint);
    if (allowUnknown && (rc.GetCode() == K_NOT_READY || rc.GetCode() == K_NOT_FOUND)) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    CHECK_FAIL_RETURN_STATUS(endpoint.localAvailability != cluster::EndpointAvailability::UNREACHABLE, K_MASTER_TIMEOUT,
                             "Disconnected from topology member " + address.ToString());
    return Status::OK();
}

Status GetTopologyMemberAddresses(const WorkerTopologyReferences *references, std::vector<HostPort> &addresses)
{
    addresses.clear();
    for (const auto &member : GetValidTopologyMembers(references)) {
        HostPort address;
        RETURN_IF_NOT_OK(address.ParseString(member));
        addresses.emplace_back(std::move(address));
    }
    return Status::OK();
}

Status GetActiveTopologyPeers(const WorkerTopologyReferences *references, uint32_t count,
                              std::vector<std::string> &addresses)
{
    addresses.clear();
    CHECK_FAIL_RETURN_STATUS(count > 0, K_INVALID, "Required topology member count is zero.");
    for (const auto &member : GetActiveTopologyMembers(references)) {
        if (references != nullptr && member == references->localAddress) {
            continue;
        }
        addresses.emplace_back(member);
        if (addresses.size() == count) {
            break;
        }
    }
    return Status::OK();
}

}  // namespace datasystem::worker
