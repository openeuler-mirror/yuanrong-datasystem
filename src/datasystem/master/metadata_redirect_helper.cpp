/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Module responsible for managing the redirect metadata logic on the master.
 */
#include "datasystem/master/metadata_redirect_helper.h"

#include <utility>

namespace datasystem {
namespace master {
MetadataRedirectHelper::MetadataRedirectHelper(const cluster::PlacementFacade *placement, bool centralizedMetadata,
                                               HostPort metadataAddress)
    : placement_(placement), centralizedMetadata_(centralizedMetadata), metadataAddress_(std::move(metadataAddress))
{
}

void MetadataRedirectHelper::Shutdown()
{
    placement_.store(nullptr, std::memory_order_release);
}

Status MetadataRedirectHelper::EvaluateMetadataRedirect(std::string_view key, MetaRedirectDecision &decision) const
{
    MetaRedirectDecision built;
    if (centralizedMetadata_) {
        CHECK_FAIL_RETURN_STATUS(!metadataAddress_.Empty(), K_INVALID,
                                 "Centralized metadata Master address is empty");
        built.targetAddress = metadataAddress_.ToString();
        decision = std::move(built);
        return Status::OK();
    }
    const auto *placement = placement_.load(std::memory_order_acquire);
    CHECK_FAIL_RETURN_STATUS(placement != nullptr, K_NOT_READY, "Master metadata placement is not available");
    cluster::RedirectDecision route;
    RETURN_IF_NOT_OK(placement->EvaluateRedirect(key, route));
    built.redirect = route.action == cluster::RedirectAction::REDIRECT;
    built.moving = route.action == cluster::RedirectAction::WAIT;
    if (!built.moving) {
        built.targetAddress = route.GetRedirectTargetAddress();
    }
    built.topologyVersion = route.topologyVersion;
    decision = std::move(built);
    return Status::OK();
}

Status MetadataRedirectHelper::EvaluateMetadataRedirectBatch(
    const std::vector<std::string_view> &keys, std::vector<MetaRedirectDecision> &decisions) const
{
    std::vector<MetaRedirectDecision> built;
    built.reserve(keys.size());
    if (centralizedMetadata_) {
        CHECK_FAIL_RETURN_STATUS(!metadataAddress_.Empty(), K_INVALID,
                                 "Centralized metadata Master address is empty");
        built.resize(keys.size());
        for (auto &item : built) {
            item.targetAddress = metadataAddress_.ToString();
        }
    } else {
        const auto *placement = placement_.load(std::memory_order_acquire);
        CHECK_FAIL_RETURN_STATUS(placement != nullptr, K_NOT_READY, "Master metadata placement is not available");
        cluster::BatchRedirectDecision routes;
        RETURN_IF_NOT_OK(placement->EvaluateRedirectBatch(keys, routes));
        for (const auto &route : routes.decisions) {
            MetaRedirectDecision item;
            item.redirect = route.action == cluster::RedirectAction::REDIRECT;
            item.moving = route.action == cluster::RedirectAction::WAIT;
            item.topologyVersion = route.topologyVersion;
            if (!item.moving) {
                item.targetAddress = route.GetRedirectTargetAddress();
            }
            built.emplace_back(std::move(item));
        }
    }
    decisions = std::move(built);
    return Status::OK();
}

Status MetadataRedirectHelper::ApplyMetadataAvailability(const std::string &id, MetaRedirectDecision &decision)
{
    if (decision.moving || !MetaIsFound(id)) {
        return Status::OK();
    }
    decision.redirect = ItemIsMigrating(id);
    decision.moving = decision.redirect;
    if (decision.moving) {
        INJECT_POINT("CheckNeedToRedirectOrNot.delay", []() { return Status::OK(); });
    }
    return Status::OK();
}

Status MetadataRedirectHelper::ResolveMetadataOwner(std::string_view key, HostPort &owner) const
{
    HostPort resolved;
    if (centralizedMetadata_) {
        CHECK_FAIL_RETURN_STATUS(!metadataAddress_.Empty(), K_INVALID,
                                 "Centralized metadata Master address is empty");
        resolved = metadataAddress_;
    } else {
        const auto *placement = placement_.load(std::memory_order_acquire);
        CHECK_FAIL_RETURN_STATUS(placement != nullptr, K_NOT_READY, "Master metadata placement is not available");
        cluster::PlacementDecision route;
        RETURN_IF_NOT_OK(placement->Locate(key, route));
        RETURN_IF_NOT_OK(resolved.ParseString(route.committedOwnerAddress));
    }
    owner = std::move(resolved);
    return Status::OK();
}

Status MetadataRedirectHelper::CheckNeedToRedirectOrNot(const std::string &id, bool &needRedirect,
                                                        std::string &newMetaAddr, uint64_t &topologyVersion)
{
    MetaRedirectDecision route;
    RETURN_IF_NOT_OK(EvaluateMetadataRedirect(id, route));
    RETURN_IF_NOT_OK(ApplyMetadataAvailability(id, route));
    needRedirect = route.redirect || route.moving;
    newMetaAddr = std::move(route.targetAddress);
    topologyVersion = route.topologyVersion;
    return Status::OK();
}

Status MetadataRedirectHelper::GroupRedirctObjectByNewMetaAddr(
    std::vector<std::string> &ids,
    std::map<std::pair<uint64_t, std::string>, std::vector<std::string>> &redirectMap, bool &moving)
{
    if (ids.empty()) {
        moving = false;
        return Status::OK();
    }
    std::vector<std::string_view> keys;
    keys.reserve(ids.size());
    for (const auto &id : ids) {
        keys.emplace_back(id);
    }
    std::vector<MetaRedirectDecision> decisions;
    RETURN_IF_NOT_OK(EvaluateMetadataRedirectBatch(keys, decisions));
    std::vector<std::string> retained;
    retained.reserve(ids.size());
    bool batchMoving = false;
    for (size_t index = 0; index < ids.size(); ++index) {
        auto &decision = decisions[index];
        RETURN_IF_NOT_OK(ApplyMetadataAvailability(ids[index], decision));
        INJECT_POINT("redirect.query.delete", [&decision](std::string addr) {
            decision.targetAddress = std::move(addr);
            decision.redirect = true;
            return Status::OK();
        });
        if (decision.moving) {
            batchMoving = true;
        } else if (decision.redirect) {
            auto [iter, inserted] = redirectMap.try_emplace(
                std::make_pair(decision.topologyVersion, std::move(decision.targetAddress)));
            (void)inserted;
            iter->second.emplace_back(std::move(ids[index]));
        } else {
            retained.emplace_back(std::move(ids[index]));
        }
    }
    ids.swap(retained);
    moving = batchMoving;
    return Status::OK();
}

bool MetadataRedirectHelper::ItemIsMigrating(const std::string &id)
{
    TbbMigratingTable::const_accessor accessor;
    if (migratingItems_.find(accessor, id)) {
        return true;
    }
    return false;
}
}  // namespace master
}  // namespace datasystem
