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
 * Description: Resolves Worker metadata owners from prebound deployment facts.
 */
#include "datasystem/worker/metadata_route_resolver.h"

#include <algorithm>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::worker {
namespace {
template <typename RouteGroups>
void RecordFailures(const std::vector<std::string> &keys, const Status &status, RouteGroups &result)
{
    result.failures.reserve(keys.size());
    for (const auto &key : keys) {
        result.failures.insert_or_assign(key, status);
    }
}

Status GetParsedOwner(const std::string &ownerAddress,
                      std::unordered_map<std::string_view, HostPort> &parsedOwners, const HostPort *&owner)
{
    auto cached = parsedOwners.find(ownerAddress);
    if (cached != parsedOwners.end()) {
        owner = &cached->second;
        return Status::OK();
    }
    HostPort parsed;
    Status rc = parsed.ParseString(ownerAddress);
    if (rc.IsError()) {
        return rc;
    }
    auto [position, inserted] = parsedOwners.emplace(ownerAddress, std::move(parsed));
    (void)inserted;
    owner = &position->second;
    return Status::OK();
}

template <typename AddOwner>
void AddResolvedOwner(const std::string &key, const std::string &ownerAddress,
                      std::unordered_map<std::string_view, HostPort> &parsedOwners,
                      std::unordered_map<std::string, Status> &failures, AddOwner &&addOwner)
{
    const HostPort *owner = nullptr;
    Status rc = GetParsedOwner(ownerAddress, parsedOwners, owner);
    if (rc.IsError()) {
        failures.insert_or_assign(key, std::move(rc));
        return;
    }
    // Consume the cached owner by reference before the next insertion to avoid copying its host string per key.
    addOwner(*owner);
}
}  // namespace

MetadataRouteResolver::MetadataRouteResolver(const cluster::PlacementFacade *placement) : placement_(placement)
{
}

Status MetadataRouteResolver::ResolveOwner(std::string_view key, HostPort &owner) const
{
    if (placement_ == nullptr) {
        return Status(K_NOT_READY, "Topology placement facade is not provided.");
    }
    cluster::PlacementDecision decision;
    RETURN_IF_NOT_OK(placement_->Locate(key, decision));
    HostPort resolved;
    RETURN_IF_NOT_OK(resolved.ParseString(decision.committedOwnerAddress));
    owner = std::move(resolved);
    return Status::OK();
}

MetaOwnerRouteGroups MetadataRouteResolver::GroupOwners(const std::vector<std::string> &keys) const
{
    MetaOwnerRouteGroups result;
    if (keys.empty()) {
        return result;
    }
    if (placement_ == nullptr) {
        RecordFailures(keys, Status(K_NOT_READY, "Topology placement facade is not provided."), result);
        return result;
    }
    std::vector<std::string_view> keyViews(keys.begin(), keys.end());
    cluster::BatchPlacementDecision decision;
    Status rc = placement_->LocateBatch(keyViews, decision);
    if (rc.IsError()) {
        RecordFailures(keys, rc, result);
        return result;
    }
    result.topologyVersion = decision.topologyVersion;
    std::unordered_map<std::string_view, HostPort> parsedOwners;
    for (size_t index = 0; index < keys.size(); ++index) {
        if (index >= decision.items.size()) {
            result.failures.insert_or_assign(keys[index], Status(K_NOT_FOUND, "Placement decision is missing."));
            continue;
        }
        auto &item = decision.items[index];
        if (item.status.IsError()) {
            result.failures.insert_or_assign(keys[index], std::move(item.status));
            continue;
        }
        AddResolvedOwner(keys[index], item.decision.committedOwnerAddress, parsedOwners, result.failures,
                         [&](const HostPort &owner) { result.groups[owner].emplace_back(keys[index]); });
    }
    return result;
}

IndexedMetaOwnerRouteGroups MetadataRouteResolver::GroupIndexedOwners(const std::vector<std::string> &keys) const
{
    IndexedMetaOwnerRouteGroups result;
    if (keys.empty()) {
        return result;
    }
    if (placement_ == nullptr) {
        RecordFailures(keys, Status(K_NOT_READY, "Topology placement facade is not provided."), result);
        return result;
    }
    std::vector<std::string_view> keyViews(keys.begin(), keys.end());
    cluster::BatchPlacementDecision decision;
    Status rc = placement_->LocateBatch(keyViews, decision);
    if (rc.IsError()) {
        RecordFailures(keys, rc, result);
        return result;
    }
    result.topologyVersion = decision.topologyVersion;
    std::unordered_map<std::string_view, HostPort> parsedOwners;
    for (size_t index = 0; index < keys.size(); ++index) {
        if (index >= decision.items.size()) {
            result.failures.insert_or_assign(keys[index], Status(K_NOT_FOUND, "Placement decision is missing."));
            continue;
        }
        auto &item = decision.items[index];
        if (item.status.IsError()) {
            result.failures.insert_or_assign(keys[index], std::move(item.status));
            continue;
        }
        AddResolvedOwner(keys[index], item.decision.committedOwnerAddress, parsedOwners, result.failures,
                         [&](const HostPort &owner) { result.groups[owner].emplace_back(keys[index], index); });
    }
    return result;
}

Status MetadataRouteResolver::ResolveNewMetaOwner(std::string_view key, HostPort &owner) const
{
    if (placement_ == nullptr) {
        return Status(K_NOT_READY, "Topology placement facade is not provided.");
    }
    cluster::PlacementDecision decision;
    RETURN_IF_NOT_OK(placement_->LocateSurvivingOwner(key, decision));
    HostPort resolved;
    RETURN_IF_NOT_OK(resolved.ParseString(decision.committedOwnerAddress));
    owner = std::move(resolved);
    return Status::OK();
}

IndexedMetaOwnerRouteGroups MetadataRouteResolver::GroupIndexedNewMetaOwners(
    const std::vector<std::string> &keys) const
{
    if (keys.empty()) {
        return GroupIndexedOwners(keys);
    }
    IndexedMetaOwnerRouteGroups result;
    if (placement_ == nullptr) {
        RecordFailures(keys, Status(K_NOT_READY, "Topology placement facade is not provided."), result);
        return result;
    }
    std::vector<std::string_view> keyViews(keys.begin(), keys.end());
    cluster::BatchPlacementDecision decision;
    Status rc = placement_->LocateSurvivingOwnerBatch(keyViews, decision);
    if (rc.IsError()) {
        RecordFailures(keys, rc, result);
        return result;
    }
    result.topologyVersion = decision.topologyVersion;
    std::unordered_map<std::string_view, HostPort> parsedOwners;
    for (size_t index = 0; index < keys.size(); ++index) {
        if (index >= decision.items.size()) {
            result.failures.insert_or_assign(keys[index], Status(K_NOT_FOUND, "Placement decision is missing."));
            continue;
        }
        auto &item = decision.items[index];
        if (item.status.IsError()) {
            result.failures.insert_or_assign(keys[index], std::move(item.status));
            continue;
        }
        AddResolvedOwner(keys[index], item.decision.committedOwnerAddress, parsedOwners, result.failures,
                         [&](const HostPort &owner) { result.groups[owner].emplace_back(keys[index], index); });
    }
    return result;
}

MetaOwnerRouteGroups MetadataRouteResolver::GroupMigrateTargets(const std::vector<std::string> &keys) const
{
    MetaOwnerRouteGroups result;
    if (keys.empty()) {
        return result;
    }
    if (placement_ == nullptr) {
        RecordFailures(keys, Status(K_NOT_READY, "Topology placement facade is not provided."), result);
        return result;
    }
    const size_t chunkSize = cluster::PlacementFacade::kMaxBatchKeys;
    const size_t chunkCount = (keys.size() + chunkSize - 1) / chunkSize;
    if (chunkCount > 1) {
        LOG(WARNING) << FormatString("[Migrate Data] GroupMigrateTargets splits %zu keys into %zu redirect batches",
                                     keys.size(), chunkCount);
    }
    for (size_t offset = 0; offset < keys.size(); offset += chunkSize) {
        const size_t count = std::min(chunkSize, keys.size() - offset);
        std::vector<std::string> chunkKeys(keys.begin() + static_cast<std::ptrdiff_t>(offset),
                                           keys.begin() + static_cast<std::ptrdiff_t>(offset + count));
        AppendRedirectChunk(result, chunkKeys);
    }
    return result;
}

void MetadataRouteResolver::AppendRedirectChunk(MetaOwnerRouteGroups &result,
                                                const std::vector<std::string> &chunkKeys) const
{
    std::vector<std::string_view> keyViews(chunkKeys.begin(), chunkKeys.end());
    cluster::BatchRedirectDecision decision;
    Status rc = placement_->EvaluateRedirectBatch(keyViews, decision);
    if (rc.IsError()) {
        RecordFailures(chunkKeys, rc, result);
        return;
    }
    result.topologyVersion = result.topologyVersion == 0
                                 ? decision.topologyVersion
                                 : std::min(result.topologyVersion, decision.topologyVersion);
    // The cache views decision-owned address strings, so it must not outlive one chunk's decision.
    std::unordered_map<std::string_view, HostPort> parsedOwners;
    for (size_t index = 0; index < chunkKeys.size(); ++index) {
        if (index >= decision.decisions.size()) {
            result.failures.insert_or_assign(chunkKeys[index],
                                             Status(K_NOT_FOUND, "Placement decision is missing."));
            continue;
        }
        const auto &route = decision.decisions[index];
        // A ScaleOut WAIT or a plain LOCAL decision has no ScaleIn takeover target; the committed owner keeps
        // such keys on the caller's standby demotion path instead of following the prospective override.
        const std::string &targetAddress = route.action == cluster::RedirectAction::REDIRECT
                                               ? route.GetRedirectTargetAddress()
                                               : route.committedOwnerAddress;
        AddResolvedOwner(chunkKeys[index], targetAddress, parsedOwners, result.failures,
                         [&](const HostPort &owner) { result.groups[owner].emplace_back(chunkKeys[index]); });
    }
}

}  // namespace datasystem::worker
