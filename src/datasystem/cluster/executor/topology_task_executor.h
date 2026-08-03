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
#include <map>
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
     * @param[in] restartHandler Existing process-restart cleanup callback.
     * @param[in] options Bounded execution options.
     */
    TopologyTaskExecutor(std::string localAddress, TopologyRepository &repository,
                         TopologySnapshotState &snapshots, ITopologyPhaseCallbacks &callbacks,
                         CoordinationEventDispatcher &dispatcher,
                         std::function<Status(const std::map<std::string, int64_t> &,
                                              RestartEffectMode)> restartHandler,
                         TopologyTaskExecutorOptions options);

    /**
     * @brief Construct an Executor without centralized restart effects.
     * @param[in] localAddress Local canonical address.
     * @param[in] repository Repository dependency.
     * @param[in] snapshots Snapshot dependency.
     * @param[in] callbacks Business callback dependency.
     * @param[in] dispatcher Completion dispatcher.
     * @param[in] options Bounded execution options.
     */
    TopologyTaskExecutor(std::string localAddress, TopologyRepository &repository,
                         TopologySnapshotState &snapshots, ITopologyPhaseCallbacks &callbacks,
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
    struct OperationState {
        TopologyTask task;
        uint32_t attempts{ 0 };
        std::chrono::steady_clock::time_point nextAttempt;
        std::shared_ptr<CancellationToken> cancellation;
        std::string scaleInMetadataGate;
        bool progressReady{ false };
        bool scaleInMetadataDone{ false };
        bool bestEffortFailure{ false };
    };

    Status BuildExecutionFence(const TopologyTask &task, TopologyCallbackPhase phase,
                               TopologyExecutionFence &fence) const;

    Status BuildExecutionFence(const TopologyTask &task, TopologyCallbackPhase phase, const TopologySnapshot &snapshot,
                               TopologyExecutionFence &fence) const;

    Status BuildPhaseAction(const TopologyExecutionFence &fence, uint64_t topologyVersion,
                            TopologyPhaseAction &action) const;

    Status BuildOpaqueScope(const TopologyExecutionFence &fence, std::unique_ptr<IKeyFilter> &filter,
                            std::unique_ptr<StorageScanPlan> &plan) const;

    Status ValidateFence(const TopologyExecutionFence &fence) const;

    Status ValidateFence(const TopologyExecutionFence &fence, std::shared_ptr<const TopologySnapshot> &latest) const;

    Status BuildScaleInMetadataGateForNotify(const TopologyTaskNotify &notify, const TopologySnapshot &snapshot,
                                             uint64_t epoch, std::string &gate) const;

    Status RefreshNotifyEpochLocked(uint64_t epoch);

    Status SubmitNotifiedTasks(const TopologyTaskNotify &notify, uint64_t epoch);

    // Restart effects share the callback pool, lifecycle drain, and per-generation deduplication.
    Status HandleRestartFacts(const std::map<std::string, int64_t> &restartFacts);

    Status InvokeRestartEffects(const std::map<std::string, int64_t> &restartFacts) noexcept;

    void FinishRestartEffects(const std::map<std::string, int64_t> &restartFacts,
                              const Status &callbackStatus) noexcept;

    // Admission records ownership under mutex_ before callback work crosses into the pool.
    Status SubmitCallback(const TopologyTask &task, TopologyExecutionFence fence, bool allowScaleInDataDrain = false);

    Status AdmitCallbackLocked(const TopologyTask &task, const TopologyExecutionFence &fence,
                               const std::string &operation, bool allowScaleInDataDrain,
                               std::shared_ptr<CancellationToken> &cancellation);

    Status ValidateCallbackWindowLocked(const TopologyExecutionFence &fence, std::chrono::steady_clock::time_point now);

    void ExecuteCallback(TopologyExecutionFence fence, std::string operation,
                         std::shared_ptr<CancellationToken> cancellation) noexcept;

    Status RunCallback(const TopologyExecutionFence &fence, const std::string &operation,
                       const CancellationToken &cancellation, std::unique_ptr<TopologyPreparedCleanup> &cleanup,
                       std::chrono::steady_clock::time_point &deadline);

    Status ResolveCallbackDeadline(const TopologyExecutionFence &fence,
                                   std::chrono::steady_clock::time_point &deadline) const;

    void FinishCallbackBody(TopologyCallbackPhase phase, const std::string &operation,
                            const Status &callbackStatus, const Status &submitStatus) noexcept;

    Status InvokeCallback(const TopologyExecutionFence &fence, const TopologyCallbackContext &context,
                          std::unique_ptr<TopologyPreparedCleanup> &cleanup) noexcept;

    // Scale-in cleanup separates lock-held, no-IO authorization from the idempotent pool-side effect.
    Status TryAuthorizeCleanup(const TopologyExecutionFence &fence, TopologyPreparedCleanup &cleanup);

    Status SubmitCleanupApply(const TopologyExecutionFence &fence, const std::string &operation,
                              std::chrono::steady_clock::time_point deadline,
                              std::unique_ptr<TopologyPreparedCleanup> cleanup);

    void ExecuteCleanupApply(TopologyExecutionFence fence, std::string operation,
                             std::chrono::steady_clock::time_point deadline,
                             std::shared_ptr<TopologyPreparedCleanup> cleanup,
                             std::shared_ptr<CancellationToken> cancellation) noexcept;

    Status CompleteProgress(TopologyCallbackCompletion &completion, const std::string &operation);

    Status CompleteScaleInMetadata(TopologyCallbackCompletion &completion, const std::string &operation);

    Status IsScaleInMetadataGateReadyForOperationLocked(const std::string &operation, bool &ready) const;

    bool IsScaleInMetadataGateReadyLocked(const std::string &gate) const;

    bool IsScaleInDataDrainReady(const std::string &operation) const;

    void ScheduleScaleInDataDrainReadyLocked(const std::string &gate);

    bool ReleaseInFlightLocked(OperationState &state);

    void ScheduleOperationLocked(const std::string &operation, OperationState &state,
                                 std::chrono::steady_clock::time_point nextAttempt);

    void ClearOperationScheduleLocked(const std::string &operation, OperationState &state);

    bool EraseOperationLocked(const std::string &operation);

    Status CompleteFailure(const TopologyExecutionFence &fence, const std::string &operation, const Status &status,
                           bool progressFailure);

    Status CompleteStale(const std::string &operation, const Status &status);

    bool DiscardIfStopping(const std::string &operation);

    bool ScheduleRetryLocked(TopologyCallbackPhase phase, const std::string &operation);

    void PreserveDueOperation(const std::string &operation, const TopologyTask &task, const Status &status);

    bool IsOrdinaryRetryable(const Status &status) const noexcept;

    std::string localAddress_;
    TopologyRepository &repository_;
    TopologySnapshotState &snapshots_;
    ITopologyPhaseCallbacks &callbacks_;
    CoordinationEventDispatcher &dispatcher_;
    std::function<Status(const std::map<std::string, int64_t> &, RestartEffectMode)> restartHandler_;
    TopologyTaskExecutorOptions options_;
    std::unique_ptr<ThreadPool> callbackPool_;
    // Protects callbackPool_, lifecycle flags, callback accounting, epoch state, retry ledgers, pending and in-flight
    // operations, ordinary/failure deadlines, restart deduplication state, and diagnostics_.
    mutable std::mutex mutex_;
    // Uses mutex_ to signal changes to callbackBodies_ while Stop() drains callbacks.
    std::condition_variable drained_;
    bool started_{ false };
    bool stopping_{ false };
    size_t callbackBodies_{ 0 };
    size_t inFlightOperations_{ 0 };
    uint64_t currentEpoch_{ 0 };
    // One mutex-protected state record owns every per-operation retry, callback, progress and ScaleIn stage fact.
    std::unordered_map<std::string, OperationState> operations_;
    // Sparse reverse index for scheduled operations; retry timing remains authoritative in OperationState.
    std::unordered_set<std::string> scheduledOperations_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> ordinaryDeadlineByMember_;
    std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> failureDeadlineByEpoch_;
    // Operations admitted for metadata migration but not yet metadata-complete, grouped by ScaleIn source and batch.
    std::unordered_map<std::string, std::unordered_set<std::string>> scaleInMetadataPendingByGate_;
    // Successful restart generations; mutex_ protects handler admission, Stop drain and deduplication updates.
    std::map<std::string, int64_t> completedRestartTimestampsByAddress_;
    // True while the callback pool owns the only admitted restart batch; mutex_ prevents duplicate admission.
    bool restartBatchInFlight_{ false };
    TopologyTaskExecutorDiagnostics diagnostics_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_TOPOLOGY_TASK_EXECUTOR_H
