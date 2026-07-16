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

#include <iterator>
#include <memory>
#include <mutex>

#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

MembershipEndpointView::MembershipEndpointView(const TopologySnapshotState &snapshots) : snapshots_(snapshots)
{
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

}  // namespace datasystem::cluster
