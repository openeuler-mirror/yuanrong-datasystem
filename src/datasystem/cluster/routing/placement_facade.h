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
     * @brief Bind non-owned Snapshot and algorithm dependencies plus immutable local identity.
     * @param[in] snapshots Snapshot state that outlives this facade.
     * @param[in] algorithm Routing algorithm that outlives this facade.
     * @param[in] localAddress Canonical local member address.
     */
    PlacementFacade(const TopologySnapshotState &snapshots, const IRoutingAlgorithm &algorithm,
                    std::string localAddress);

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
     * @brief Locate the committed owner for one key on one Snapshot.
     * @param[in] placementKey Binary-safe placement key.
     * @param[out] decision Owner address and topology version; unchanged on failure.
     * @return K_OK, K_NOT_READY, or placement validation status.
     */
    Status Locate(std::string_view placementKey, PlacementDecision &decision) const;

    /**
     * @brief Locate the committed owner for one token on one Snapshot.
     * @param[in] token Placement token.
     * @param[out] decision Owner address and topology version; unchanged on failure.
     * @return K_OK, K_NOT_READY, or placement validation status.
     */
    Status LocateToken(uint32_t token, PlacementDecision &decision) const;

    /**
     * @brief Locate multiple keys against the same Snapshot.
     * @param[in] placementKeys Binary-safe placement keys.
     * @param[out] decision Per-key decision/status items and one topology version; unchanged on batch failure.
     * @return K_OK after evaluating every key; K_INVALID or K_NOT_READY when the batch itself cannot be evaluated.
     */
    Status LocateBatch(const std::vector<std::string_view> &placementKeys, BatchPlacementDecision &decision) const;

    /**
     * @brief Evaluate whether this prebound local member should serve, redirect, or wait.
     * @param[in] placementKey Binary-safe placement key.
     * @param[out] decision Local/redirect/wait decision; unchanged on failure.
     * @return K_OK, K_NOT_READY, or placement validation status.
     */
    Status EvaluateRedirect(std::string_view placementKey, RedirectDecision &decision) const;

    /**
     * @brief Evaluate multiple redirect decisions against the same Snapshot.
     * @param[in] placementKeys Binary-safe placement keys.
     * @param[out] decision Batch decisions and one topology version; unchanged on failure.
     * @return K_OK, K_NOT_READY, or placement validation status.
     */
    Status EvaluateRedirectBatch(const std::vector<std::string_view> &placementKeys,
                                 BatchRedirectDecision &decision) const;

    /**
     * @brief Check committed ownership for the prebound local member.
     * @param[in] placementKey Binary-safe placement key.
     * @param[out] isLocal Ownership result; unchanged on failure.
     * @return K_OK, K_NOT_READY, or placement validation status.
     */
    Status IsLocalOwner(std::string_view placementKey, bool &isLocal) const;

private:
    /**
     * @brief Locate without reloading Snapshot.
     * @param[in] snapshot Snapshot.
     * @param[in] token Token.
     * @param[out] decision Decision.
     * @return Status.
     */
    Status LocateInSnapshot(const TopologySnapshot &snapshot, uint32_t token, PlacementDecision &decision) const;

    /**
     * @brief Evaluate one redirect decision without reloading Snapshot.
     * @param[in] snapshot Snapshot shared by one foreground operation.
     * @param[in] placementKey Binary-safe placement key.
     * @param[out] decision Local/redirect/wait decision.
     * @return K_OK or placement validation status.
     */
    Status EvaluateRedirectInSnapshot(const TopologySnapshot &snapshot, std::string_view placementKey,
                                      RedirectDecision &decision) const;
    const TopologySnapshotState &snapshots_;
    const IRoutingAlgorithm &algorithm_;
    const std::string localAddress_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_ROUTING_PLACEMENT_FACADE_H
