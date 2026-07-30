/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Business-neutral cluster topology callback contracts.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_PHASE_CALLBACKS_H
#define DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_PHASE_CALLBACKS_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

#include "datasystem/cluster/executor/key_filter.h"
#include "datasystem/cluster/executor/storage_scan_plan.h"
#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

class TopologyTaskExecutor;

/**
 * @brief Executor-owned cooperative cancellation signal.
 */
class CancellationToken final {
public:
    /**
     * @brief Construct one independently cancellable shared state.
     */
    CancellationToken() : state_(std::make_shared<State>())
    {
    }

    /**
     * @brief Destroy the token.
     */
    ~CancellationToken() = default;

    /**
     * @brief Share cancellation state with bounded asynchronous business work.
     * @param[in] other Token whose cancellation state is shared.
     */
    CancellationToken(const CancellationToken &other) = default;

    /**
     * @brief Share-assign cancellation state with bounded asynchronous business work.
     * @param[in] other Token whose cancellation state is shared.
     * @return This token.
     */
    CancellationToken &operator=(const CancellationToken &other) = default;

    /**
     * @brief Return true after cancellation.
     * @return Cooperative cancellation state.
     */
    bool IsCancelled() const noexcept
    {
        return state_->cancelled.load();
    }

    /**
     * @brief Wait until cancellation or deadline.
     * @param[in] deadline Absolute deadline.
     * @return True when cancelled; false on deadline.
     */
    bool WaitUntil(std::chrono::steady_clock::time_point deadline) const
    {
        std::unique_lock<std::mutex> lock(state_->mutex);
        return state_->changed.wait_until(lock, deadline, [this] { return state_->cancelled.load(); });
    }

    /**
     * @brief Commit one source-side mutation only while this callback attempt is still authoritative.
     * @param[in] deadline Absolute callback deadline.
     * @param[in] commit Local mutation to execute at the commit boundary.
     * @return Commit status, or a cancellation/deadline error before the mutation starts.
     */
    template <typename Function>
    Status CommitSourceMutationIfActive(std::chrono::steady_clock::time_point deadline, Function &&commit) const
    {
        std::lock_guard<std::mutex> lock(state_->commitMutex);
        if (state_->cancelled.load()) {
            return Status(K_NOT_READY, "topology callback cancelled before source commit");
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return Status(K_RPC_DEADLINE_EXCEEDED, "topology callback expired before source commit");
        }
        return std::forward<Function>(commit)();
    }

    /**
     * @brief Wait until a source-side mutation already inside the commit boundary has completed.
     */
    void SynchronizeSourceMutations() const
    {
        std::lock_guard<std::mutex> lock(state_->commitMutex);
    }

private:
    friend class TopologyTaskExecutor;

    struct State {
        std::atomic<bool> cancelled{ false };
        // Coordinates changed waiters with cancelled; cancelled remains authoritative.
        mutable std::mutex mutex;
        std::condition_variable changed;
        // Serializes source-side mutations with callback return after cancellation or deadline.
        mutable std::mutex commitMutex;
    };

    /**
     * @brief Request cooperative cancellation and wake waiters.
     */
    void Cancel() noexcept
    {
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->cancelled.store(true);
        }
        state_->changed.notify_all();
    }

    std::shared_ptr<State> state_;
};

/**
 * @brief Algorithm-neutral callback participant facts.
 */
struct TopologyPhaseAction {
    std::string taskId;
    uint64_t topologyVersion{ 0 };
    uint64_t batchEpoch{ 0 };
    MemberIdentity executor;
    std::optional<MemberIdentity> source;
    std::optional<MemberIdentity> target;
    std::optional<MemberIdentity> failed;
};

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
 * @brief Bounded cleanup effect executed on the callback pool after Snapshot-gated authorization.
 */
using TopologyCleanupEffect =
    std::function<Status(std::chrono::steady_clock::time_point, const CancellationToken &)>;

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
