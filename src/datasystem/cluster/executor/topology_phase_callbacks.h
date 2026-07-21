/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Business-neutral cluster topology callback contracts.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_PHASE_CALLBACKS_H
#define DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_PHASE_CALLBACKS_H

#include <chrono>
#include <memory>
#include <utility>

#include "datasystem/cluster/executor/cancellation_token.h"
#include "datasystem/cluster/executor/key_filter.h"
#include "datasystem/cluster/executor/storage_scan_plan.h"
#include "datasystem/cluster/executor/topology_cleanup_effect.h"
#include "datasystem/cluster/executor/topology_phase_action.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

/**
 * @brief Fully copied callback facts plus invocation-lifetime opaque scope references.
 */
struct TopologyCallbackContext {
    TopologyPhaseAction action;
    std::string businessOperationId;
    std::chrono::steady_clock::time_point deadline;
    const CancellationToken &cancellation;
    const IKeyFilter &keyFilter;
    const StorageScanPlan &storageScanPlan;
};

/**
 * @brief Move-only prepared cleanup authorized behind a fence gate and applied outside it.
 */
class TopologyPreparedCleanup final {
public:
    /**
     * @brief Bind a short local authorization and its bounded idempotent effect.
     * @param[in] authorize No-IO local authorization.
     * @param[in] apply Bounded idempotent effect run after authorization.
     */
    TopologyPreparedCleanup(std::function<Status()> authorize, TopologyCleanupEffect apply)
        : authorize_(std::move(authorize)), apply_(std::move(apply))
    {
    }

    /**
     * @brief Release owned closures without running authorization or Apply.
     */
    ~TopologyPreparedCleanup() = default;

    /**
     * @brief Disable copying a one-shot cleanup.
     */
    TopologyPreparedCleanup(const TopologyPreparedCleanup &) = delete;

    /**
     * @brief Disable copy assignment.
     */
    TopologyPreparedCleanup &operator=(const TopologyPreparedCleanup &) = delete;

    /**
     * @brief Move a prepared cleanup.
     * @param[in] other Cleanup whose owned effect is transferred.
     */
    TopologyPreparedCleanup(TopologyPreparedCleanup &&other) noexcept = default;

    /**
     * @brief Move-assign a prepared cleanup.
     * @param[in] other Cleanup whose owned effect is transferred.
     * @return This cleanup.
     */
    TopologyPreparedCleanup &operator=(TopologyPreparedCleanup &&other) noexcept = default;

private:
    friend class TopologyTaskExecutor;

    /**
     * @brief Authorize the prepared effect inside the Snapshot publication gate.
     * @return Authorization status.
     */
    Status Authorize()
    {
        return authorize_ == nullptr ? Status(K_INVALID, "empty prepared topology cleanup authorization")
                                     : authorize_();
    }

    /**
     * @brief Apply the authorized idempotent effect on the callback pool without holding the publication gate.
     * @param[in] deadline Original callback attempt deadline.
     * @param[in] cancellation Executor-owned cooperative cancellation signal.
     * @return Effect status.
     */
    Status Apply(std::chrono::steady_clock::time_point deadline, const CancellationToken &cancellation)
    {
        return apply_ == nullptr ? Status(K_INVALID, "empty prepared topology cleanup effect")
                                 : apply_(deadline, cancellation);
    }

    std::function<Status()> authorize_;
    TopologyCleanupEffect apply_;
};

/**
 * @brief One task/phase callback boundary implemented by business adapters.
 */
class ITopologyPhaseCallbacks {
public:
    /**
     * @brief Destroy the callback boundary.
     */
    virtual ~ITopologyPhaseCallbacks() = default;

    /**
     * @brief Execute one ScaleOut task phase.
     * @param[in] context Opaque callback context.
     * @return Status.
     */
    virtual Status OnScaleOut(const TopologyCallbackContext &context) = 0;

    /**
     * @brief Execute one ScaleIn task phase.
     * @param[in] context Opaque callback context.
     * @return Status.
     */
    virtual Status OnScaleIn(const TopologyCallbackContext &context) = 0;

    /**
     * @brief Execute one ScaleIn data-drain task phase after source metadata migration is complete.
     * @param[in] context Opaque callback context.
     * @return Status.
     */
    virtual Status OnScaleInDataDrain(const TopologyCallbackContext &context) = 0;

    /**
     * @brief Prepare ScaleIn cleanup.
     * @param[in] context Opaque callback context.
     * @param[out] cleanup Prepared local effect.
     * @return Status.
     */
    virtual Status PrepareScaleInCleanup(const TopologyCallbackContext &context,
                                         std::unique_ptr<TopologyPreparedCleanup> &cleanup) = 0;

    /**
     * @brief Execute one best-effort Failure task.
     * @param[in] context Opaque callback context.
     * @return Status.
     */
    virtual Status OnFailure(const TopologyCallbackContext &context) = 0;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_PHASE_CALLBACKS_H
