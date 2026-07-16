/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Single-Snapshot foreground cluster placement facade.
 */
#include "datasystem/cluster/routing/placement_facade.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
// Callers enforce operation-specific limits; this guard matches the largest public foreground batch contract.
constexpr size_t MAX_BATCH_KEYS = 100'000;
}

PlacementFacade::PlacementFacade(const TopologySnapshotState &snapshots, const IRoutingAlgorithm &algorithm,
                                 std::string localAddress)
    : snapshots_(snapshots), algorithm_(algorithm), localAddress_(std::move(localAddress))
{
}

Status PlacementFacade::LocateInSnapshot(const TopologySnapshot &snapshot, uint32_t token,
                                         PlacementDecision &decision) const
{
    const Member *owner = nullptr;
    RETURN_IF_NOT_OK(algorithm_.LocateOwner(snapshot, token, owner));
    CHECK_FAIL_RETURN_STATUS(owner != nullptr, K_RUNTIME_ERROR, "routing algorithm returned a null owner");
    decision = { snapshot.Version(), owner->identity.address };
    return Status::OK();
}

Status PlacementFacade::Locate(std::string_view placementKey, PlacementDecision &decision) const
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    return LocateInSnapshot(*snapshot, algorithm_.Hash(placementKey), decision);
}

Status PlacementFacade::LocateToken(uint32_t token, PlacementDecision &decision) const
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    return LocateInSnapshot(*snapshot, token, decision);
}

Status PlacementFacade::LocateBatch(const std::vector<std::string_view> &placementKeys,
                                    BatchPlacementDecision &decision) const
{
    CHECK_FAIL_RETURN_STATUS(!placementKeys.empty() && placementKeys.size() <= MAX_BATCH_KEYS, K_INVALID,
                             "invalid cluster placement batch size");
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    BatchPlacementDecision built;
    built.topologyVersion = snapshot->Version();
    built.items.reserve(placementKeys.size());
    for (auto key : placementKeys) {
        PlacementDecision item;
        auto status = LocateInSnapshot(*snapshot, algorithm_.Hash(key), item);
        built.items.emplace_back(BatchPlacementItem{ std::move(item), std::move(status) });
    }
    decision = std::move(built);
    return Status::OK();
}

Status PlacementFacade::EvaluateRedirect(std::string_view placementKey, RedirectDecision &decision) const
{
    CHECK_FAIL_RETURN_STATUS(!localAddress_.empty(), K_INVALID, "local cluster member address is empty");
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    return EvaluateRedirectInSnapshot(*snapshot, placementKey, decision);
}

Status PlacementFacade::EvaluateRedirectInSnapshot(const TopologySnapshot &snapshot, std::string_view placementKey,
                                                    RedirectDecision &decision) const
{
    const uint32_t token = algorithm_.Hash(placementKey);
    PlacementDecision placement;
    RETURN_IF_NOT_OK(LocateInSnapshot(snapshot, token, placement));
    RedirectDecision built;
    built.topologyVersion = placement.topologyVersion;
    built.committedOwnerAddress = std::move(placement.committedOwnerAddress);
    built.action = built.committedOwnerAddress == localAddress_ ? RedirectAction::LOCAL : RedirectAction::REDIRECT;
    const auto &batch = snapshot.GetActiveBatch();
    const bool isOrdinaryBatch =
        batch.has_value()
        && (batch->type == TopologyChangeType::SCALE_OUT || batch->type == TopologyChangeType::SCALE_IN);
    if (isOrdinaryBatch && built.action == RedirectAction::LOCAL) {
        const Member *prospective = nullptr;
        RETURN_IF_NOT_OK(algorithm_.LocateProspectiveOwner(snapshot, token, prospective));
        CHECK_FAIL_RETURN_STATUS(prospective != nullptr, K_RUNTIME_ERROR,
                                 "routing algorithm returned a null prospective owner");
        if (prospective->identity.address != built.committedOwnerAddress) {
            if (batch->type == TopologyChangeType::SCALE_OUT) {
                built.action = RedirectAction::WAIT;
            } else {
                built.redirectTargetAddress = prospective->identity.address;
                built.action = RedirectAction::REDIRECT;
            }
        }
    }
    decision = std::move(built);
    return Status::OK();
}

Status PlacementFacade::EvaluateRedirectBatch(const std::vector<std::string_view> &placementKeys,
                                              BatchRedirectDecision &decision) const
{
    CHECK_FAIL_RETURN_STATUS(!localAddress_.empty(), K_INVALID, "local cluster member address is empty");
    CHECK_FAIL_RETURN_STATUS(!placementKeys.empty() && placementKeys.size() <= MAX_BATCH_KEYS, K_INVALID,
                             "invalid cluster redirect batch size");
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    BatchRedirectDecision built;
    built.topologyVersion = snapshot->Version();
    built.decisions.reserve(placementKeys.size());
    for (auto key : placementKeys) {
        RedirectDecision item;
        RETURN_IF_NOT_OK(EvaluateRedirectInSnapshot(*snapshot, key, item));
        built.decisions.emplace_back(std::move(item));
    }
    decision = std::move(built);
    return Status::OK();
}

Status PlacementFacade::IsLocalOwner(std::string_view placementKey, bool &isLocal) const
{
    RedirectDecision decision;
    RETURN_IF_NOT_OK(EvaluateRedirect(placementKey, decision));
    isLocal = decision.committedOwnerAddress == localAddress_;
    return Status::OK();
}

}  // namespace datasystem::cluster
