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
#include "datasystem/client/cluster_query/cluster_query_projector.h"

#include <algorithm>
#include <map>
#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem::client::cluster_query {
namespace {

using Lifecycle = cluster::MemberLifecycleState;
using MemberState = cluster::MemberState;

bool IsDraining(MemberState state)
{
    return state == MemberState::PRE_LEAVING || state == MemberState::LEAVING;
}

bool IsStarting(Lifecycle state)
{
    return state == Lifecycle::STARTING || state == Lifecycle::RESTARTING
           || state == Lifecycle::DOWNGRADE_RESTARTING;
}

}  // namespace

Status ClusterQueryProjector::ValidateRouteKeys(const std::vector<std::string_view> &keys)
{
    CHECK_FAIL_RETURN_STATUS(!keys.empty() && keys.size() <= MAX_QUERY_KEYS, K_INVALID,
                             "query key count is outside the supported range");
    size_t totalBytes = 0;
    for (const auto key : keys) {
        CHECK_FAIL_RETURN_STATUS(!key.empty() && key.size() <= MAX_QUERY_KEY_BYTES, K_INVALID,
                                 "query key is empty or exceeds byte limit");
        CHECK_FAIL_RETURN_STATUS(totalBytes <= MAX_QUERY_TOTAL_KEY_BYTES - key.size(), K_INVALID,
                                 "query keys exceed total byte limit");
        totalBytes += key.size();
    }
    return Status::OK();
}

void ClusterQueryProjector::BuildRanges(const cluster::TopologySnapshot &topology,
                                        std::map<std::string, std::vector<QueryHashRange>> &ranges) const
{
    std::vector<std::pair<uint32_t, std::string>> owners;
    for (const auto *member : topology.CommittedMembers()) {
        for (const auto ringToken : member->tokens) {
            owners.emplace_back(ringToken, member->identity.address);
        }
    }
    if (owners.empty()) {
        return;
    }
    std::sort(owners.begin(), owners.end());
    const bool fullRing = owners.size() == 1;
    for (size_t index = 0; index < owners.size(); ++index) {
        const size_t previous = index == 0 ? owners.size() - 1 : index - 1;
        ranges[owners[index].second].push_back({ owners[previous].first, owners[index].first, fullRing });
    }
}

ClusterNodeHealth ClusterQueryProjector::DeriveHealth(const cluster::Member &member,
                                                      const MembershipObservation *membership) const noexcept
{
    if (membership == nullptr) {
        return ClusterNodeHealth::UNHEALTHY;
    }
    if (member.state == MemberState::FAILED) {
        return ClusterNodeHealth::UNHEALTHY;
    }
    if (IsDraining(member.state) || membership->lifecycleState == Lifecycle::EXITING) {
        return ClusterNodeHealth::DRAINING;
    }
    if (membership->lifecycleState == Lifecycle::RECOVERING) {
        return ClusterNodeHealth::RECOVERING;
    }
    if (member.state == MemberState::INITIAL || member.state == MemberState::JOINING
        || IsStarting(membership->lifecycleState)) {
        return ClusterNodeHealth::STARTING;
    }
    if (member.state == MemberState::ACTIVE && membership->lifecycleState == Lifecycle::READY) {
        return ClusterNodeHealth::HEALTHY;
    }
    return ClusterNodeHealth::UNHEALTHY;
}

Status ClusterQueryProjector::BuildCluster(const ClusterSourceSnapshot &snapshot, ClusterQueryResult &result) const
{
    CHECK_FAIL_RETURN_STATUS(snapshot.topology != nullptr, K_NOT_READY, "cluster topology is unavailable");
    ClusterQueryResult built;
    built.topologyVersion = snapshot.topology->Version();
    std::map<std::string, const cluster::Member *> members;
    for (const auto &member : snapshot.topology->Members()) {
        members.emplace(member.identity.address, &member);
    }
    std::map<std::string, const MembershipObservation *> memberships;
    for (const auto &membership : snapshot.memberships) {
        memberships.emplace(membership.address, &membership);
    }
    std::map<std::string, std::vector<QueryHashRange>> ranges;
    BuildRanges(*snapshot.topology, ranges);
    for (const auto &[address, member] : members) {
        const auto membership = memberships.count(address) == 0 ? nullptr : memberships.at(address);
        built.nodes.emplace_back(
            ClusterNodeDiagnostic{ address, DeriveHealth(*member, membership), member->state, ranges[address] });
    }
    result = std::move(built);
    return Status::OK();
}

Status ClusterQueryProjector::Route(const ClusterSourceSnapshot &snapshot, const std::vector<std::string_view> &keys,
                                    RouteQueryResult &result) const
{
    RETURN_IF_NOT_OK(ValidateRouteKeys(keys));
    CHECK_FAIL_RETURN_STATUS(snapshot.topology != nullptr, K_NOT_READY, "cluster topology is unavailable");
    std::map<std::string, WorkerRouteDiagnostic> grouped;
    for (const auto key : keys) {
        const cluster::Member *owner = nullptr;
        RETURN_IF_NOT_OK(algorithm_.LocateOwner(*snapshot.topology, algorithm_.Hash(key), owner));
        auto &route = grouped[owner->identity.address];
        route.workerAddress = owner->identity.address;
        route.keys.emplace_back(key);
    }
    std::map<std::string, const MembershipObservation *> memberships;
    for (const auto &membership : snapshot.memberships) {
        memberships.emplace(membership.address, &membership);
    }
    for (const auto &member : snapshot.topology->Members()) {
        if (grouped.count(member.identity.address) > 0) {
            const auto membership = memberships.count(member.identity.address) == 0
                                        ? nullptr
                                        : memberships.at(member.identity.address);
            grouped[member.identity.address].health = DeriveHealth(member, membership);
        }
    }
    RouteQueryResult built;
    built.topologyVersion = snapshot.topology->Version();
    for (auto &entry : grouped) {
        built.routes.emplace_back(std::move(entry.second));
    }
    result = std::move(built);
    return Status::OK();
}

}  // namespace datasystem::client::cluster_query
