/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Single-Snapshot foreground cluster placement facade.
 */
#ifndef DATASYSTEM_CLUSTER_ROUTING_PLACEMENT_FACADE_H
#define DATASYSTEM_CLUSTER_ROUTING_PLACEMENT_FACADE_H

#include "datasystem/cluster/algorithm/topology_algorithm.h"
#include "datasystem/cluster/routing/placement_types.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"

namespace datasystem::cluster {

class PlacementFacade final {
public:
    /**
     * @brief Bind non-owned Snapshot and algorithm dependencies.
     * @param[in] snapshots Snapshot state that outlives this facade.
     * @param[in] algorithm Routing algorithm that outlives this facade.
     */
    PlacementFacade(const TopologySnapshotState &snapshots, const IRoutingAlgorithm &algorithm);

    /**
     * @brief Destroy the stateless foreground facade.
     */
    ~PlacementFacade() = default;

    /**
     * @brief Disable copying non-owned dependencies.
     */
    PlacementFacade(const PlacementFacade &) = delete;

    /**
     * @brief Disable copy assignment of non-owned dependencies.
     */
    PlacementFacade &operator=(const PlacementFacade &) = delete;

    /**
     * @brief Locate by key.
     * @param[in] placementKey Key.
     * @param[out] decision Decision.
     * @return Status.
     */
    Status Locate(std::string_view placementKey, PlacementDecision &decision) const;

    /**
     * @brief Locate by token.
     * @param[in] token Token.
     * @param[out] decision Decision.
     * @return Status.
     */
    Status LocateToken(uint32_t token, PlacementDecision &decision) const;

    /**
     * @brief Batch locate on one Snapshot.
     * @param[in] placementKeys Keys.
     * @param[out] decision Batch decision.
     * @return Status.
     */
    Status LocateBatch(const std::vector<std::string_view> &placementKeys, BatchPlacementDecision &decision) const;

    /**
     * @brief Evaluate local redirect.
     * @param[in] placementKey Key.
     * @param[in] localAddress Local address.
     * @param[out] decision Redirect decision.
     * @return Status.
     */
    Status EvaluateRedirect(std::string_view placementKey, const std::string &localAddress,
                            RedirectDecision &decision) const;

    /**
     * @brief Check committed local ownership.
     * @param[in] placementKey Key.
     * @param[in] localAddress Local address.
     * @param[out] isLocal Result.
     * @return Status.
     */
    Status IsLocalOwner(std::string_view placementKey, const std::string &localAddress, bool &isLocal) const;

private:
    /**
     * @brief Locate without reloading Snapshot.
     * @param[in] snapshot Snapshot.
     * @param[in] token Token.
     * @param[out] decision Decision.
     * @return Status.
     */
    Status LocateInSnapshot(const TopologySnapshot &snapshot, uint32_t token, PlacementDecision &decision) const;
    const TopologySnapshotState &snapshots_;
    const IRoutingAlgorithm &algorithm_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_ROUTING_PLACEMENT_FACADE_H
