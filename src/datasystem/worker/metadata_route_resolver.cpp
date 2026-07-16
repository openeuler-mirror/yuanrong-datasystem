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

#include <utility>

namespace datasystem::worker {
namespace {
Status ValidateCentralizedOwner(const MetadataRouteOptions &options)
{
    if (options.masterAddress.Empty()) {
        return Status(K_INVALID, "Centralized metadata owner address is empty.");
    }
    return Status::OK();
}

template <typename RouteGroups>
void RecordFailures(const std::vector<std::string> &keys, const Status &status, RouteGroups &result)
{
    result.failures.reserve(keys.size());
    for (const auto &key : keys) {
        result.failures.insert_or_assign(key, status);
    }
}

Status GetParsedOwner(const cluster::PlacementDecision &decision,
                      std::unordered_map<std::string_view, HostPort> &parsedOwners, const HostPort *&owner)
{
    auto cached = parsedOwners.find(decision.committedOwnerAddress);
    if (cached != parsedOwners.end()) {
        owner = &cached->second;
        return Status::OK();
    }
    HostPort parsed;
    Status rc = parsed.ParseString(decision.committedOwnerAddress);
    if (rc.IsError()) {
        return rc;
    }
    auto [position, inserted] = parsedOwners.emplace(decision.committedOwnerAddress, std::move(parsed));
    (void)inserted;
    owner = &position->second;
    return Status::OK();
}

template <typename AddOwner>
void AddResolvedOwner(const std::string &key, const cluster::PlacementDecision &decision,
                      std::unordered_map<std::string_view, HostPort> &parsedOwners,
                      std::unordered_map<std::string, Status> &failures, AddOwner &&addOwner)
{
    const HostPort *owner = nullptr;
    Status rc = GetParsedOwner(decision, parsedOwners, owner);
    if (rc.IsError()) {
        failures.insert_or_assign(key, std::move(rc));
        return;
    }
    // Consume the cached owner by reference before the next insertion to avoid copying its host string per key.
    addOwner(*owner);
}
}  // namespace

MetadataRouteResolver::MetadataRouteResolver(const cluster::PlacementFacade *placement, MetadataRouteOptions options)
    : placement_(placement), options_(std::move(options))
{
}

Status MetadataRouteResolver::ResolveOwner(std::string_view key, HostPort &owner) const
{
    HostPort resolved;
    if (options_.centralizedMode) {
        Status rc = ValidateCentralizedOwner(options_);
        if (rc.IsError()) {
            return rc;
        }
        resolved = options_.masterAddress;
    } else {
        if (placement_ == nullptr) {
            return Status(K_NOT_READY, "Topology placement facade is not provided.");
        }
        cluster::PlacementDecision decision;
        Status rc = placement_->Locate(key, decision);
        if (rc.IsError()) {
            return rc;
        }
        rc = resolved.ParseString(decision.committedOwnerAddress);
        if (rc.IsError()) {
            return rc;
        }
    }
    owner = std::move(resolved);
    return Status::OK();
}

MetaOwnerRouteGroups MetadataRouteResolver::GroupOwners(const std::vector<std::string> &keys) const
{
    MetaOwnerRouteGroups result;
    if (keys.empty()) {
        return result;
    }
    if (options_.centralizedMode) {
        Status rc = ValidateCentralizedOwner(options_);
        if (rc.IsError()) {
            RecordFailures(keys, rc, result);
            return result;
        }
        result.groups[options_.masterAddress] = keys;
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
        AddResolvedOwner(keys[index], item.decision, parsedOwners, result.failures,
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
    if (options_.centralizedMode) {
        Status rc = ValidateCentralizedOwner(options_);
        if (rc.IsError()) {
            RecordFailures(keys, rc, result);
            return result;
        }
        auto &group = result.groups[options_.masterAddress];
        group.reserve(keys.size());
        for (size_t index = 0; index < keys.size(); ++index) {
            group.emplace_back(keys[index], index);
        }
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
        AddResolvedOwner(keys[index], item.decision, parsedOwners, result.failures,
                         [&](const HostPort &owner) { result.groups[owner].emplace_back(keys[index], index); });
    }
    return result;
}

}  // namespace datasystem::worker
