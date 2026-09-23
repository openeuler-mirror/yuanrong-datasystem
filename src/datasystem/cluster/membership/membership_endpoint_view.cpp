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
#include "datasystem/cluster/membership/membership_endpoint_view.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>

#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

MembershipEndpointView::MembershipEndpointView(const TopologySnapshotState &snapshots, bool enableWriteRedirect)
    : snapshots_(snapshots), writeRedirectEnabled_(enableWriteRedirect)
{
}

bool MembershipEndpointView::SupportsWriteRedirect() const noexcept
{
    return writeRedirectEnabled_;
}

Status MembershipEndpointView::UpdateMembership(const std::string &address, MembershipValue value, int64_t revision)
{
    CHECK_FAIL_RETURN_STATUS(revision > 0, K_INVALID, "Membership revision must be positive");
    std::lock_guard<std::shared_mutex> lock(membershipMutex_);
    RETURN_OK_IF_TRUE(revision <= snapshotRevision_);
    return SetMembershipLocked(address, std::move(value), revision);
}

Status MembershipEndpointView::DeleteMembership(const std::string &address, int64_t revision)
{
    CHECK_FAIL_RETURN_STATUS(revision > 0, K_INVALID, "Membership revision must be positive");
    std::lock_guard<std::shared_mutex> lock(membershipMutex_);
    RETURN_OK_IF_TRUE(revision <= snapshotRevision_);
    // Until a periodic Range reaches this DELETE revision, older PUTs can still pass the snapshot revision fence.
    return SetMembershipLocked(address, std::nullopt, revision);
}

Status MembershipEndpointView::SetMembershipLocked(const std::string &address, std::optional<MembershipValue> value,
                                                   int64_t revision)
{
    const bool ready = value.has_value() && value->lifecycleState == MemberLifecycleState::READY;
    const auto found = memberships_.find(address);
    if (found != memberships_.end() && found->second.revision >= revision) {
        return Status::OK();
    }
    constexpr size_t MAX_WRITE_CANDIDATES = 20'000;
    CHECK_FAIL_RETURN_STATUS(found != memberships_.end() || memberships_.size() < MAX_WRITE_CANDIDATES,
                             K_TRY_AGAIN, "Membership candidate view requires a new snapshot");
    auto [current, inserted] = memberships_.try_emplace(address);
    (void)inserted;
    auto &state = current->second;
    if ((state.value.has_value() && state.value->lifecycleState == MemberLifecycleState::READY) != ready) {
        if (ready) {
            state.readyIndex = readyCandidateAddresses_.size();
            readyCandidateAddresses_.emplace_back(address);
        } else {
            const auto lastIndex = readyCandidateAddresses_.size() - 1;
            if (state.readyIndex != lastIndex) {
                auto moved = memberships_.find(readyCandidateAddresses_.back());
                CHECK_FAIL_RETURN_STATUS(moved != memberships_.end(), K_RUNTIME_ERROR,
                                         "Membership ready candidate index is inconsistent");
                readyCandidateAddresses_[state.readyIndex] = std::move(readyCandidateAddresses_.back());
                moved->second.readyIndex = state.readyIndex;
            }
            readyCandidateAddresses_.pop_back();
        }
    }
    state.value = std::move(value);
    state.revision = revision;
    return Status::OK();
}

void MembershipEndpointView::ClearMemberships()
{
    std::lock_guard<std::shared_mutex> lock(membershipMutex_);
    snapshotRevision_ = 0;
    memberships_.clear();
    readyCandidateAddresses_.clear();
}

Status MembershipEndpointView::GetHostIds(std::unordered_map<std::string, std::string> &hostIds) const
{
    std::shared_lock<std::shared_mutex> lock(membershipMutex_);
    hostIds.clear();
    CHECK_FAIL_RETURN_STATUS(snapshotRevision_ > 0, K_NOT_READY, "Membership snapshot is not ready");
    hostIds.reserve(memberships_.size());
    for (const auto &[address, state] : memberships_) {
        if (state.value.has_value() && !state.value->hostId.empty()) {
            hostIds.emplace(address, state.value->hostId);
        }
    }
    return Status::OK();
}

Status MembershipEndpointView::RefreshMemberships(const std::vector<MembershipRecord> &members, int64_t revision)
{
    CHECK_FAIL_RETURN_STATUS(revision > 0, K_INVALID, "Membership snapshot revision must be positive");
    std::lock_guard<std::shared_mutex> lock(membershipMutex_);
    RETURN_OK_IF_TRUE(revision < snapshotRevision_);
    MembershipEndpointView refreshed(snapshots_, writeRedirectEnabled_);
    refreshed.memberships_.reserve(members.size());
    refreshed.readyCandidateAddresses_.reserve(members.size());
    for (const auto &[address, state] : memberships_) {
        if (state.revision > revision) {
            RETURN_IF_NOT_OK(refreshed.SetMembershipLocked(address, state.value, state.revision));
        }
    }
    for (const auto &member : members) {
        RETURN_IF_NOT_OK(refreshed.SetMembershipLocked(
            member.address, MembershipValue{ member.timestamp, member.state, member.hostId, {} }, revision));
    }
    memberships_.swap(refreshed.memberships_);
    readyCandidateAddresses_.swap(refreshed.readyCandidateAddresses_);
    snapshotRevision_ = revision;
    return Status::OK();
}

std::vector<std::string> MembershipEndpointView::GetWriteCandidates(
    const std::string &excludedAddress, const std::string &selectionKey, size_t maxCandidates) const
{
    if (maxCandidates == 0) {
        return {};
    }
    std::shared_ptr<const TopologySnapshot> snapshot;
    if (snapshots_.Load(snapshot).IsError()) {
        return {};
    }
    std::shared_lock<std::shared_mutex> observationLock;
    if (hasObservations_.load(std::memory_order_acquire)) {
        observationLock = std::shared_lock<std::shared_mutex>(mutex_);
    }
    std::vector<std::string> candidates;
    candidates.reserve(maxCandidates);
    std::shared_lock<std::shared_mutex> candidateLock(membershipMutex_);
    if (readyCandidateAddresses_.empty()) {
        return {};
    }
    constexpr size_t CANDIDATE_SCAN_FACTOR = 4;
    const size_t scanLimit = std::min(readyCandidateAddresses_.size(), maxCandidates * CANDIDATE_SCAN_FACTOR);
    const auto hash = std::hash<std::string>{};
    size_t seed = hash(selectionKey);
    seed ^= hash(excludedAddress) + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
    const size_t start = seed % readyCandidateAddresses_.size();
    for (size_t scanned = 0; scanned < scanLimit; ++scanned) {
        const auto &address = readyCandidateAddresses_[(start + scanned) % readyCandidateAddresses_.size()];
        if (address == excludedAddress) {
            continue;
        }
        const Member *member = nullptr;
        if (snapshot->FindMemberByAddress(address, member).IsError() || member->state != MemberState::ACTIVE) {
            continue;
        }
        if (observationLock.owns_lock()) {
            const auto observed = observationsByAddress_.find(address);
            if (observed != observationsByAddress_.end() && observed->second.identity == member->identity
                && observed->second.topologyVersion == snapshot->Version()
                && observed->second.availability == EndpointAvailability::UNREACHABLE) {
                continue;
            }
        }
        candidates.emplace_back(address);
        if (candidates.size() == maxCandidates) {
            break;
        }
    }
    return candidates;
}

Status MembershipEndpointView::UpdateObservation(const EndpointObservation &observation)
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    const Member *member = nullptr;
    auto rc = snapshot->FindMemberByAddress(observation.identity.address, member);
    CHECK_FAIL_RETURN_STATUS(rc.IsOk() && member != nullptr && member->identity == observation.identity
                                 && snapshot->Version() == observation.topologyVersion,
                             K_INVALID, "Endpoint observation is stale");
    auto current = observationsByAddress_.find(observation.identity.address);
    CHECK_FAIL_RETURN_STATUS(
        current == observationsByAddress_.end() || current->second.topologyVersion <= observation.topologyVersion,
        K_INVALID, "Endpoint observation version regressed");
    observationsByAddress_[observation.identity.address] = observation;
    hasObservations_.store(true, std::memory_order_release);
    return Status::OK();
}

void MembershipEndpointView::RemoveStaleObservations()
{
    std::lock_guard<std::shared_mutex> lock(mutex_);
    std::shared_ptr<const TopologySnapshot> snapshot;
    if (snapshots_.Load(snapshot).IsError()) {
        return;
    }
    for (auto iter = observationsByAddress_.begin(); iter != observationsByAddress_.end();) {
        const Member *member = nullptr;
        const bool current = snapshot != nullptr && snapshot->FindMemberByAddress(iter->first, member).IsOk()
                             && member != nullptr && member->identity == iter->second.identity
                             && snapshot->Version() == iter->second.topologyVersion;
        iter = current ? std::next(iter) : observationsByAddress_.erase(iter);
    }
    hasObservations_.store(!observationsByAddress_.empty(), std::memory_order_release);
}

EndpointAvailability MembershipEndpointView::ResolveLocalAvailability(const Member &member,
                                                                      uint64_t topologyVersion) const
{
    if (member.state == MemberState::FAILED) {
        return EndpointAvailability::UNREACHABLE;
    }
    if (!hasObservations_.load(std::memory_order_acquire)) {
        return EndpointAvailability::UNKNOWN;
    }
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto iter = observationsByAddress_.find(member.identity.address);
    if (iter == observationsByAddress_.end() || !(iter->second.identity == member.identity)
        || iter->second.topologyVersion != topologyVersion) {
        return EndpointAvailability::UNKNOWN;
    }
    return iter->second.availability;
}

Status MembershipEndpointView::ResolveByAddress(const std::string &address, MemberEndpoint &endpoint) const
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    const Member *member = nullptr;
    RETURN_IF_NOT_OK(snapshot->FindMemberByAddress(address, member));
    endpoint = { member->identity, member->state, ResolveLocalAvailability(*member, snapshot->Version()) };
    return Status::OK();
}

Status MembershipEndpointView::ResolveById(const std::string &id, MemberEndpoint &endpoint) const
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    const Member *member = nullptr;
    RETURN_IF_NOT_OK(snapshot->FindMemberById(id, member));
    endpoint = { member->identity, member->state, ResolveLocalAvailability(*member, snapshot->Version()) };
    return Status::OK();
}

Status MembershipEndpointView::GetSnapshot(std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    return snapshots_.Load(snapshot);
}

Status MembershipEndpointView::WaitForSnapshotVersion(
    uint64_t minimumVersion, std::chrono::steady_clock::time_point deadline,
    std::shared_ptr<const TopologySnapshot> &snapshot) const
{
    return snapshots_.WaitForVersion(minimumVersion, deadline, snapshot);
}

}  // namespace datasystem::cluster
