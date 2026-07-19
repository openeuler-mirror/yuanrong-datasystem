/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Fenced worker-local exact topology task executor.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_TASK_EXECUTOR_H
#define DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_TASK_EXECUTOR_H

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem::cluster {

class CoordinationEventDispatcher;
class TopologySnapshot;
class TopologySnapshotState;

/**
 * @brief Callback concurrency, retry, deadline and drain limits.
 */
struct TopologyTaskExecutorOptions {
    size_t callbackThreads{ 4 };
    size_t callbackQueueCapacity{ 1'024 };
    std::chrono::seconds ordinaryCallbackDeadline{ 10 };
    std::chrono::minutes ordinaryMemberWindow{ 3 };
    uint32_t ordinaryMaxAttempts{ 8 };
    std::chrono::milliseconds backoffInitial{ 100 };
    std::chrono::seconds backoffMaximum{ 30 };
    std::chrono::seconds failureCallbackDeadline{ 5 };
    std::chrono::seconds failureBatchWindow{ 30 };
    std::chrono::seconds ordinaryDrain{ 3 };
    std::chrono::seconds failureDrain{ 1 };
};

/**
 * @brief Move-only callback-pool result returned to the Worker serial loop.
 */
struct TopologyCallbackCompletion {
    TopologyExecutionFence fence;
    std::string businessOperationId;
    std::chrono::steady_clock::time_point deadline{};
    Status status;
    std::unique_ptr<TopologyPreparedCleanup> preparedCleanup;
};

/**
 * @brief Low-cardinality, no-IO Executor diagnostic snapshot.
 */
struct TopologyTaskExecutorDiagnostics {
    bool running{ false };
    size_t queuedCallbacks{ 0 };
    size_t inFlightCallbacks{ 0 };
    uint64_t succeeded{ 0 };
    uint64_t failed{ 0 };
    uint64_t stale{ 0 };
    uint64_t cancelled{ 0 };
    std::string lastError;
};

/**
 * @brief Worker-local exact-task executor with internal fences and opaque callback scopes.
 */
class TopologyTaskExecutor final {
public:
    /**
     * @brief Construct one Executor.
     * @param[in] localAddress Local canonical address.
     * @param[in] repository Repository dependency.
     * @param[in] snapshots Snapshot dependency.
     * @param[in] callbacks Business callback dependency.
     * @param[in] dispatcher Completion dispatcher.
     * @param[in] options Bounded execution options.
     */
    TopologyTaskExecutor(std::string localAddress, TopologyRepository &repository,
                         const TopologySnapshotState &snapshots, ITopologyPhaseCallbacks &callbacks,
                         CoordinationEventDispatcher &dispatcher, TopologyTaskExecutorOptions options);

    /**
     * @brief Cancel and join accepted callbacks before releasing Executor state.
     */
    ~TopologyTaskExecutor();

    /**
     * @brief Disable copying a callback-pool owner.
     */
    TopologyTaskExecutor(const TopologyTaskExecutor &) = delete;

    /**
     * @brief Disable copy assignment.
     */
    TopologyTaskExecutor &operator=(const TopologyTaskExecutor &) = delete;

    /**
     * @brief Start the bounded callback pool.
     * @return Operation status.
     */
    Status Start();

    /**
     * @brief Admit tasks referenced by own notify.
     * @param[in] notify Complete notify.
     * @return Operation status.
     */
    Status HandleNotify(const TopologyTaskNotify &notify);

    /**
     * @brief Process callback completion and fenced progress.
     * @param[in] completion Completion.
     * @return Status.
     */
    Status HandleCompletion(TopologyCallbackCompletion completion);

    /**
     * @brief Run retry/deadline work without sleeping workers.
     * @param[in] now Monotonic time.
     * @return Status.
     */
    Status HandleTick(std::chrono::steady_clock::time_point now);

    /**
     * @brief Cancel and bounded-drain.
     * @param[in] deadline Absolute deadline.
     * @return Status.
     */
    Status Stop(std::chrono::steady_clock::time_point deadline);

    /**
     * @brief Return no-IO diagnostics.
     * @return Current diagnostics.
     */
    TopologyTaskExecutorDiagnostics GetDiagnostics() const;

private:
    /**
     * @brief Rebuild a complete task fence.
     * @param[in] task Exact task.
     * @param[in] phase Phase.
     * @param[out] fence Complete fence.
     * @return Status.
     */
    Status BuildExecutionFence(const TopologyTask &task, TopologyCallbackPhase phase,
                               TopologyExecutionFence &fence) const;

    /**
     * @brief Build a fence against one exact authority Snapshot.
     * @param[in] task Task.
     * @param[in] phase Phase.
     * @param[in] snapshot Exact authority Snapshot.
     * @param[out] fence Complete fence.
     * @return Status.
     */
    Status BuildExecutionFence(const TopologyTask &task, TopologyCallbackPhase phase, const TopologySnapshot &snapshot,
                               TopologyExecutionFence &fence) const;

    /**
     * @brief Build callback facts.
     * @param[in] fence Complete fence.
     * @param[in] topologyVersion Version.
     * @param[out] action Callback action.
     * @return Status.
     */
    Status BuildPhaseAction(const TopologyExecutionFence &fence, uint64_t topologyVersion,
                            TopologyPhaseAction &action) const;

    /**
     * @brief Build filter and scan plan from one range set.
     * @param[in] fence Complete fence.
     * @param[out] filter Opaque predicate.
     * @param[out] plan Opaque scan plan.
     * @return Status.
     */
    Status BuildOpaqueScope(const TopologyExecutionFence &fence, std::unique_ptr<IKeyFilter> &filter,
                            std::unique_ptr<StorageScanPlan> &plan) const;

    /**
     * @brief Revalidate one fence from exact authority.
     * @param[in] fence Complete fence.
     * @return Status.
     */
    Status ValidateFence(const TopologyExecutionFence &fence) const;

    /**
     * @brief Revalidate one fence and return the same exact-read authority Snapshot.
     * @param[in] fence Complete fence.
     * @param[out] latest Exact authoritative Snapshot; unchanged on failure.
     * @return Status.
     */
    Status ValidateFence(const TopologyExecutionFence &fence, std::shared_ptr<const TopologySnapshot> &latest) const;

    Status BuildScaleInMetadataGateForNotify(const TopologyTaskNotify &notify, const TopologySnapshot &snapshot,
                                             uint64_t epoch, std::string &gate) const;

    Status RefreshNotifyEpochLocked(uint64_t epoch);

    Status SubmitNotifiedTasks(const TopologyTaskNotify &notify, uint64_t epoch);

    /**
     * @brief Apply admission and enqueue callback.
     * @param[in] task Exact task.
     * @param[in] fence Validated fence consumed by the callback work item.
     * @return Status.
     */
    Status SubmitCallback(const TopologyTask &task, TopologyExecutionFence fence, bool allowScaleInDataDrain = false);

    /**
     * @brief Admit one callback while mutex_ is held.
     * @param[in] task Exact task.
     * @param[in] fence Validated fence.
     * @param[in] operation Stable operation id.
     * @param[out] cancellation New cancellation token when a callback must be submitted.
     * @return Admission status.
     */
    Status AdmitCallbackLocked(const TopologyTask &task, const TopologyExecutionFence &fence,
                               const std::string &operation, bool allowScaleInDataDrain,
                               std::shared_ptr<CancellationToken> &cancellation);

    /**
     * @brief Validate the total callback window while mutex_ is held.
     * @param[in] fence Validated fence.
     * @param[in] now Current monotonic time.
     * @return Window status.
     */
    Status ValidateCallbackWindowLocked(const TopologyExecutionFence &fence, std::chrono::steady_clock::time_point now);

    /**
     * @brief Execute one synchronous business callback and post completion.
     * @param[in] fence Validated execution fence.
     * @param[in] operation Stable operation id.
     * @param[in] cancellation Shared cancellation token.
     */
    void ExecuteCallback(TopologyExecutionFence fence, std::string operation,
                         std::shared_ptr<CancellationToken> cancellation) noexcept;

    /**
     * @brief Validate and invoke one callback body.
     * @param[in] fence Fence copy.
     * @param[in] operation Stable business operation id.
     * @param[in] cancellation Shared cancellation token.
     * @param[out] cleanup Optional prepared cleanup.
     * @param[out] deadline Effective deadline shared by callback preparation and cleanup application.
     * @return Validation, callback, or deadline status.
     */
    Status RunCallback(const TopologyExecutionFence &fence, const std::string &operation,
                       const CancellationToken &cancellation, std::unique_ptr<TopologyPreparedCleanup> &cleanup,
                       std::chrono::steady_clock::time_point &deadline);

    /**
     * @brief Resolve the per-attempt deadline against the phase total budget.
     * @param[in] fence Fence copy.
     * @param[out] deadline Effective callback deadline.
     * @return K_OK or K_INVALID when the corresponding budget ledger is missing.
     */
    Status ResolveCallbackDeadline(const TopologyExecutionFence &fence,
                                   std::chrono::steady_clock::time_point &deadline) const;

    /**
     * @brief Release callback accounting and recover from completion-submit failure.
     * @param[in] phase Callback phase needed for retry classification.
     * @param[in] operation Stable operation id.
     * @param[in] callbackStatus Callback result.
     * @param[in] submitStatus Completion enqueue result.
     */
    void FinishCallbackBody(TopologyCallbackPhase phase, const std::string &operation,
                            const Status &callbackStatus, const Status &submitStatus) noexcept;

    /**
     * @brief Invoke one business callback with exception isolation.
     * @param[in] fence Validated execution fence.
     * @param[in] context Callback context.
     * @param[out] cleanup Optional prepared cleanup.
     * @return Callback status.
     */
    Status InvokeCallback(const TopologyExecutionFence &fence, const TopologyCallbackContext &context,
                          std::unique_ptr<TopologyPreparedCleanup> &cleanup) noexcept;

    /**
     * @brief Authorize cleanup behind final validation and Snapshot publication serialization.
     * @param[in] fence Fence.
     * @param[in] cleanup Prepared effect to authorize.
     * @return Status.
     */
    Status TryAuthorizeCleanup(const TopologyExecutionFence &fence, TopologyPreparedCleanup &cleanup);

    /**
     * @brief Submit one authorized cleanup effect to the existing callback pool.
     * @param[in] fence Fence copied to the effect completion.
     * @param[in] operation Stable operation id.
     * @param[in] deadline Original callback attempt deadline.
     * @param[in] cleanup Authorized move-only cleanup.
     * @return K_OK on submission; retryable lifecycle/admission status otherwise.
     */
    Status SubmitCleanupApply(const TopologyExecutionFence &fence, const std::string &operation,
                              std::chrono::steady_clock::time_point deadline,
                              std::unique_ptr<TopologyPreparedCleanup> cleanup);

    /**
     * @brief Execute one authorized cleanup effect and post its completion.
     * @param[in] fence Fence copy.
     * @param[in] operation Stable operation id.
     * @param[in] deadline Original callback attempt deadline.
     * @param[in] cleanup Shared owned cleanup holder.
     * @param[in] cancellation Shared Executor-owned cancellation token.
     */
    void ExecuteCleanupApply(TopologyExecutionFence fence, std::string operation,
                             std::chrono::steady_clock::time_point deadline,
                             std::shared_ptr<TopologyPreparedCleanup> cleanup,
                             std::shared_ptr<CancellationToken> cancellation) noexcept;

    /**
     * @brief Commit completed callback progress, including terminal best-effort Failure results.
     * @param[in] completion Completion.
     * @param[in] operation Stable operation id.
     * @return Progress status.
     */
    Status CompleteProgress(TopologyCallbackCompletion &completion, const std::string &operation);

    /**
     * @brief Record ScaleIn metadata completion and defer data drain until the source gate is complete.
     * @param[in] completion Metadata callback completion.
     * @param[in] operation Stable operation id.
     * @return Completion status.
     */
    Status CompleteScaleInMetadata(TopologyCallbackCompletion &completion, const std::string &operation);

    /**
     * @brief Check whether all admitted metadata callbacks for a ScaleIn source are complete.
     * @param[in] operation Stable operation id whose gate should be checked.
     * @param[out] ready True when data drain may start.
     * @return K_OK or K_INVALID when the operation has no ScaleIn gate.
     */
    Status IsScaleInMetadataGateReadyForOperationLocked(const std::string &operation, bool &ready) const;

    /**
     * @brief Return true when no admitted metadata callback is pending in a ScaleIn source gate.
     * @param[in] gate Stable source/batch gate key.
     * @return Gate readiness.
     */
    bool IsScaleInMetadataGateReadyLocked(const std::string &gate) const;

    /**
     * @brief Return true when one ScaleIn operation is in the data-drain stage.
     * @param[in] operation Stable operation id.
     * @return Stage state.
     */
    bool IsScaleInDataDrainReady(const std::string &operation) const;

    /**
     * @brief Schedule all metadata-complete operations in the same ScaleIn source gate.
     * @param[in] gate Stable source/batch gate key.
     */
    void ScheduleScaleInDataDrainReadyLocked(const std::string &gate);

    /**
     * @brief Erase all ScaleIn gate ledgers for one operation while mutex_ is held.
     * @param[in] operation Stable operation id.
     */
    void EraseScaleInOperationLocked(const std::string &operation);

    /**
     * @brief Classify and record callback/progress failure.
     * @param[in] fence Fence.
     * @param[in] operation Stable operation id.
     * @param[in] status Failure status.
     * @param[in] progressFailure True when callback succeeded but progress failed.
     * @return Completion status.
     */
    Status CompleteFailure(const TopologyExecutionFence &fence, const std::string &operation, const Status &status,
                           bool progressFailure);

    /**
     * @brief Release stale operation state.
     * @param[in] operation Stable operation id.
     * @param[in] status Stale status.
     * @return Same status.
     */
    Status CompleteStale(const std::string &operation, const Status &status);

    /**
     * @brief Discard completion after cancellation/Stop.
     * @param[in] operation Stable operation id.
     * @return True when discarded.
     */
    bool DiscardIfStopping(const std::string &operation);

    /**
     * @brief Schedule one bounded retry while mutex_ is held.
     * @param[in] phase Callback phase needed for retry policy.
     * @param[in] operation Stable operation id.
     * @return True when scheduled.
     */
    bool ScheduleRetryLocked(TopologyCallbackPhase phase, const std::string &operation);

    /**
     * @brief Preserve a due operation after a transient rebuild or admission failure.
     * @param[in] operation Stable operation id.
     * @param[in] task Pending task.
     * @param[in] status Failure to retain for diagnostics.
     */
    void PreserveDueOperation(const std::string &operation, const TopologyTask &task, const Status &status);

    /**
     * @brief Classify ordinary retryable errors.
     * @param[in] status Callback status.
     * @return Classification.
     */
    bool IsOrdinaryRetryable(const Status &status) const noexcept;

    std::string localAddress_;
    TopologyRepository &repository_;
    const TopologySnapshotState &snapshots_;
    ITopologyPhaseCallbacks &callbacks_;
    CoordinationEventDispatcher &dispatcher_;
    TopologyTaskExecutorOptions options_;
    std::unique_ptr<ThreadPool> callbackPool_;
    // Protects callbackPool_, lifecycle flags, callback accounting, epoch state, retry ledgers, pending and in-flight
    // operations, ordinary/failure deadlines, and diagnostics_.
    mutable std::mutex mutex_;
    // Uses mutex_ to signal changes to callbackBodies_ while Stop() drains callbacks.
    std::condition_variable drained_;
    bool started_{ false };
    bool stopping_{ false };
    size_t callbackBodies_{ 0 };
    uint64_t currentEpoch_{ 0 };
    std::unordered_map<std::string, uint32_t> attemptsByOperation_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> nextAttemptByOperation_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> ordinaryDeadlineByMember_;
    std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> failureDeadlineByEpoch_;
    std::unordered_map<std::string, std::shared_ptr<CancellationToken>> inFlightByOperation_;
    std::unordered_map<std::string, TopologyTask> pendingByOperation_;
    // Operations whose business callback finished and only need an idempotent progress CAS retry.
    std::unordered_set<std::string> progressReadyByOperation_;
    // Operations admitted for metadata migration but not yet metadata-complete, grouped by ScaleIn source and batch.
    std::unordered_map<std::string, std::unordered_set<std::string>> scaleInMetadataPendingByGate_;
    // Operation-to-gate ownership for ScaleIn callbacks admitted from a validated fence.
    std::unordered_map<std::string, std::string> scaleInMetadataGateByOperation_;
    // ScaleIn operations whose metadata callback has completed and whose next callback should drain local data.
    std::unordered_set<std::string> scaleInMetadataDoneByOperation_;
    // Failure callbacks whose best-effort business step failed but must still advance task progress.
    std::unordered_set<std::string> bestEffortFailureByOperation_;
    TopologyTaskExecutorDiagnostics diagnostics_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_TASK_EXECUTOR_H
