/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Fenced worker-local exact topology task executor.
 */
#include "datasystem/cluster/executor/topology_task_executor.h"

#include <algorithm>
#include <atomic>
#include <exception>
#include <functional>
#include <random>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>

#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/executor/hash_key_filter.h"
#include "datasystem/cluster/executor/storage_scan_plan.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr char EXECUTOR_POOL_NAME[] = "topology-callback";
constexpr uint32_t MAX_BACKOFF_SHIFT = 20;
constexpr size_t MAX_CALLBACK_THREADS = 16;
constexpr size_t MAX_CALLBACK_QUEUE_CAPACITY = 1'024;
constexpr int32_t EXECUTOR_AUTHORITY_READ_TIMEOUT_MS = 3'000;
constexpr size_t TASK_DIAGNOSTIC_PREFIX_SIZE = 12;
constexpr size_t OPERATION_ID_PREFIX_SIZE = sizeof("op-") - 1;
std::atomic<uint64_t> g_retrySeed{ 0 };

bool ValidOptions(const TopologyTaskExecutorOptions &options)
{
    const auto maximumBackoff = std::chrono::duration_cast<std::chrono::milliseconds>(options.backoffMaximum);
    return options.callbackThreads > 0 && options.callbackThreads <= MAX_CALLBACK_THREADS
           && options.callbackQueueCapacity > 0 && options.callbackQueueCapacity <= MAX_CALLBACK_QUEUE_CAPACITY
           && options.ordinaryMaxAttempts > 0 && options.backoffInitial.count() > 0
           && options.backoffInitial <= maximumBackoff && options.failureDrain < options.failureCallbackDeadline
           && options.failureCallbackDeadline < options.failureBatchWindow
           && options.ordinaryDrain < options.ordinaryCallbackDeadline
           && options.ordinaryCallbackDeadline < options.ordinaryMemberWindow && options.failureDrain.count() > 0
           && options.ordinaryDrain.count() > 0;
}

bool IsCommitted(MemberState state)
{
    return state == MemberState::ACTIVE || state == MemberState::PRE_LEAVING || state == MemberState::LEAVING;
}

std::vector<TokenRange> UnfinishedRanges(const TopologyTask &task)
{
    std::vector<TokenRange> ranges;
    std::visit(
        [&](const auto &value) {
            const auto &taskRanges = [&]() -> const std::vector<TopologyTaskRange> &{
                if constexpr (std::is_same_v<std::decay_t<decltype(value)>, TopologyMigrateTask>) {
                    return value.sourceRanges;
                } else {
                    return value.recoveryRanges;
                }
            }();
            for (const auto &range : taskRanges) {
                if (!range.finished) {
                    ranges.push_back(range.range);
                }
            }
        },
        task);
    return ranges;
}

bool SameFence(const TopologyExecutionFence &left, const TopologyExecutionFence &right)
{
    return left.taskId == right.taskId && left.taskKind == right.taskKind && left.batchType == right.batchType
           && left.batchEpoch == right.batchEpoch && left.phase == right.phase && left.executor == right.executor
           && left.source == right.source && left.target == right.target && left.failed == right.failed
           && left.ranges == right.ranges;
}

TopologyCallbackPhase TaskPhase(const TopologyTask &task)
{
    if (std::holds_alternative<TopologyDeleteTask>(task)) {
        return TopologyCallbackPhase::FAILURE;
    }
    const auto &migrate = std::get<TopologyMigrateTask>(task);
    return migrate.type == TopologyChangeType::SCALE_OUT ? TopologyCallbackPhase::SCALE_OUT
                                                         : TopologyCallbackPhase::SCALE_IN;
}

std::chrono::milliseconds RetryDelay(uint32_t attempt, const TopologyTaskExecutorOptions &options)
{
    const uint32_t shift = std::min<uint32_t>(attempt > 0 ? attempt - 1 : 0, MAX_BACKOFF_SHIFT);
    const auto multiplier = int64_t{ 1 } << shift;
    const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(options.backoffMaximum);
    const auto initial = options.backoffInitial.count();
    const auto capCount = initial > maximum.count() / multiplier ? maximum.count() : initial * multiplier;
    const auto cap = std::chrono::milliseconds(capCount);
    thread_local std::mt19937 generator(static_cast<uint32_t>(
        g_retrySeed.fetch_add(1, std::memory_order_relaxed) ^
        std::hash<std::thread::id>{}(std::this_thread::get_id())));
    std::uniform_int_distribution<int64_t> distribution(0, cap.count());
    return std::chrono::milliseconds(distribution(generator));
}
}  // namespace

TopologyTaskExecutor::TopologyTaskExecutor(std::string localAddress, TopologyRepository &repository,
                                           const TopologySnapshotState &snapshots, ITopologyPhaseCallbacks &callbacks,
                                           CoordinationEventDispatcher &dispatcher, TopologyTaskExecutorOptions options)
    : localAddress_(std::move(localAddress)),
      repository_(repository),
      snapshots_(snapshots),
      callbacks_(callbacks),
      dispatcher_(dispatcher),
      options_(options)
{
}

TopologyTaskExecutor::~TopologyTaskExecutor()
{
    LOG_IF_ERROR(Stop(std::chrono::steady_clock::time_point::max()),
                 "Stop cluster topology task Executor during destruction");
}

Status TopologyTaskExecutor::Start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!started_ && !localAddress_.empty() && ValidOptions(options_), K_INVALID,
                             "invalid or already started topology task Executor");
    try {
        callbackPool_ =
            std::make_unique<ThreadPool>(options_.callbackThreads, options_.callbackThreads, EXECUTOR_POOL_NAME, false);
    } catch (const std::exception &error) {
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("start topology callback pool failed: ") + error.what());
    }
    started_ = true;
    stopping_ = false;
    diagnostics_.running = true;
    return Status::OK();
}

Status TopologyTaskExecutor::BuildExecutionFence(const TopologyTask &task, TopologyCallbackPhase phase,
                                                 TopologyExecutionFence &fence) const
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    return BuildExecutionFence(task, phase, *snapshot, fence);
}

Status TopologyTaskExecutor::BuildExecutionFence(const TopologyTask &task, TopologyCallbackPhase phase,
                                                 const TopologySnapshot &snapshot, TopologyExecutionFence &fence) const
{
    CHECK_FAIL_RETURN_STATUS(snapshot.GetActiveBatch().has_value(), K_INVALID, "task has no active topology batch");
    TopologyExecutionFence built;
    built.phase = phase;
    built.ranges = UnfinishedRanges(task);
    CHECK_FAIL_RETURN_STATUS(!built.ranges.empty(), K_NOT_READY, "topology task scope is already finished");
    const Member *executor = nullptr;
    std::visit(
        [&](const auto &value) {
            built.taskId = value.taskId;
            built.batchEpoch = value.epoch;
        },
        task);
    built.taskKind =
        std::holds_alternative<TopologyMigrateTask>(task) ? TopologyTaskKind::MIGRATE : TopologyTaskKind::DELETE_MEMBER;
    built.batchType = snapshot.GetActiveBatch()->type;
    CHECK_FAIL_RETURN_STATUS(TopologyTaskMaterializer::BuildTaskId(task) == built.taskId
                                 && built.batchEpoch == snapshot.GetActiveBatch()->epoch,
                             K_INVALID, "topology task identity or epoch is stale");
    const auto executorAddress = std::visit([](const auto &value) { return value.executorAddress; }, task);
    RETURN_IF_NOT_OK(snapshot.FindMemberByAddress(executorAddress, executor));
    CHECK_FAIL_RETURN_STATUS(executorAddress == localAddress_, K_INVALID, "topology task executor is not local");
    built.executor = executor->identity;
    if (std::holds_alternative<TopologyMigrateTask>(task)) {
        const auto &migrate = std::get<TopologyMigrateTask>(task);
        const Member *target = nullptr;
        RETURN_IF_NOT_OK(snapshot.FindMemberByAddress(migrate.targetAddress, target));
        CHECK_FAIL_RETURN_STATUS(migrate.type == built.batchType, K_INVALID, "migrate task type is stale");
        if (migrate.type == TopologyChangeType::SCALE_OUT) {
            CHECK_FAIL_RETURN_STATUS(phase == TopologyCallbackPhase::SCALE_OUT && IsCommitted(executor->state)
                                         && target->state == MemberState::JOINING,
                                     K_INVALID, "invalid ScaleOut task participants");
        } else {
            CHECK_FAIL_RETURN_STATUS(phase == TopologyCallbackPhase::SCALE_IN && executor->state == MemberState::LEAVING
                                         && IsCommitted(target->state),
                                     K_INVALID, "invalid ScaleIn task participants");
        }
        built.source = executor->identity;
        built.target = target->identity;
    } else {
        const auto &remove = std::get<TopologyDeleteTask>(task);
        const Member *failed = nullptr;
        RETURN_IF_NOT_OK(snapshot.FindMemberByAddress(remove.failedAddress, failed));
        CHECK_FAIL_RETURN_STATUS(phase == TopologyCallbackPhase::FAILURE
                                     && built.batchType == TopologyChangeType::FAILURE
                                     && executor->state == MemberState::ACTIVE && failed->state == MemberState::FAILED,
                                 K_INVALID, "invalid Failure task participants");
        built.failed = failed->identity;
    }
    fence = std::move(built);
    return Status::OK();
}

Status TopologyTaskExecutor::BuildPhaseAction(const TopologyExecutionFence &fence, uint64_t topologyVersion,
                                              TopologyPhaseAction &action) const
{
    const bool ordinary = fence.phase != TopologyCallbackPhase::FAILURE;
    CHECK_FAIL_RETURN_STATUS(
        (ordinary && fence.source.has_value() && fence.target.has_value() && !fence.failed.has_value())
            || (!ordinary && !fence.source.has_value() && !fence.target.has_value() && fence.failed.has_value()),
        K_INVALID, "illegal topology callback participant matrix");
    action = {
        fence.taskId, topologyVersion, fence.batchEpoch, fence.executor, fence.source, fence.target, fence.failed
    };
    return Status::OK();
}

Status TopologyTaskExecutor::BuildOpaqueScope(const TopologyExecutionFence &fence, std::unique_ptr<IKeyFilter> &filter,
                                              std::unique_ptr<StorageScanPlan> &plan) const
{
    CHECK_FAIL_RETURN_STATUS(!fence.ranges.empty(), K_INVALID, "empty topology callback scope");
    auto ranges = fence.ranges;
    std::sort(ranges.begin(), ranges.end(), [](const auto &left, const auto &right) {
        return std::tie(left.from, left.end) < std::tie(right.from, right.end);
    });
    for (size_t index = 0; index < ranges.size(); ++index) {
        CHECK_FAIL_RETURN_STATUS(ranges[index].from <= ranges[index].end, K_INVALID, "invalid topology callback range");
        CHECK_FAIL_RETURN_STATUS(index == 0 || ranges[index - 1].end < ranges[index].from, K_INVALID,
                                 "overlapping topology callback ranges");
    }
    filter = std::make_unique<HashKeyFilter>(ranges);
    plan = StorageScanPlan::CreateHash(ranges);
    return Status::OK();
}

Status TopologyTaskExecutor::ValidateFence(const TopologyExecutionFence &fence) const
{
    std::shared_ptr<const TopologySnapshot> latest;
    return ValidateFence(fence, latest);
}

Status TopologyTaskExecutor::ValidateFence(const TopologyExecutionFence &fence,
                                           std::shared_ptr<const TopologySnapshot> &latest) const
{
    TopologyTask task;
    RETURN_IF_NOT_OK(repository_.ReadTask(fence.taskKind, fence.taskId, fence.batchType, fence.batchEpoch, task));
    TopologyReader reader(repository_);
    std::shared_ptr<const TopologySnapshot> candidate;
    RETURN_IF_NOT_OK(reader.Read(EXECUTOR_AUTHORITY_READ_TIMEOUT_MS, candidate));
    TopologyExecutionFence observed;
    RETURN_IF_NOT_OK(BuildExecutionFence(task, fence.phase, *candidate, observed));
    CHECK_FAIL_RETURN_STATUS(SameFence(fence, observed), K_INVALID, "topology execution fence is stale");
    latest = std::move(candidate);
    return Status::OK();
}

Status TopologyTaskExecutor::HandleNotify(const TopologyTaskNotify &notify)
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(snapshots_.Load(snapshot));
    CHECK_FAIL_RETURN_STATUS(snapshot->GetActiveBatch().has_value() && snapshot->GetActiveBatch()->type == notify.type,
                             K_INVALID, "notify does not match active topology batch");
    VLOG(1) << "CLUSTER_TASK action=notify type=" << static_cast<uint32_t>(notify.type)
            << " epoch=" << snapshot->GetActiveBatch()->epoch << " task_count=" << notify.taskIds.size();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto epoch = snapshot->GetActiveBatch()->epoch;
        if (currentEpoch_ != 0 && currentEpoch_ != epoch) {
            for (auto &[operation, cancellation] : inFlightByOperation_) {
                cancellation->Cancel();
            }
            CHECK_FAIL_RETURN_STATUS(inFlightByOperation_.empty(), K_TRY_AGAIN,
                                     "old topology callback epoch is still draining");
            attemptsByOperation_.clear();
            nextAttemptByOperation_.clear();
            ordinaryDeadlineByMember_.clear();
            failureDeadlineByEpoch_.clear();
            pendingByOperation_.clear();
            progressReadyByOperation_.clear();
            bestEffortFailureByOperation_.clear();
        }
        currentEpoch_ = epoch;
    }
    auto firstError = Status::OK();
    for (const auto &taskId : notify.taskIds) {
        const auto kind =
            notify.type == TopologyChangeType::FAILURE ? TopologyTaskKind::DELETE_MEMBER : TopologyTaskKind::MIGRATE;
        TopologyTask task;
        auto rc = repository_.ReadTask(kind, taskId, notify.type, snapshot->GetActiveBatch()->epoch, task);
        if (rc.IsError()) {
            if (firstError.IsOk()) {
                firstError = rc;
            }
            continue;
        }
        TopologyExecutionFence fence;
        rc = BuildExecutionFence(task, TaskPhase(task), fence);
        if (rc.GetCode() == K_NOT_READY) {
            continue;
        }
        if (rc.IsError()) {
            if (firstError.IsOk()) {
                firstError = rc;
            }
            continue;
        }
        rc = SubmitCallback(task, std::move(fence));
        if (rc.IsError() && firstError.IsOk()) {
            firstError = rc;
        }
    }
    return firstError;
}

Status TopologyTaskExecutor::SubmitCallback(const TopologyTask &task, TopologyExecutionFence fence)
{
    const auto operation = TopologyTaskMaterializer::BuildBusinessOperationId(fence.phase, fence);
    const auto phase = fence.phase;
    CHECK_FAIL_RETURN_STATUS(operation.size() > OPERATION_ID_PREFIX_SIZE, K_RUNTIME_ERROR,
                             "build topology callback operation digest failed");
    std::shared_ptr<CancellationToken> cancellation;
    std::lock_guard<std::mutex> lock(mutex_);
    RETURN_IF_NOT_OK(AdmitCallbackLocked(task, fence, operation, cancellation));
    if (cancellation == nullptr) {
        return Status::OK();
    }
    try {
        // Keep callbackPool_ protected until the work item is accepted so Stop() cannot reset it concurrently.
        callbackPool_->Execute([this, fence = std::move(fence), operation, cancellation]() mutable {
            ExecuteCallback(std::move(fence), std::move(operation), std::move(cancellation));
        });
    } catch (const std::exception &error) {
        --callbackBodies_;
        inFlightByOperation_.erase(operation);
        diagnostics_.queuedCallbacks = callbackBodies_;
        ++diagnostics_.failed;
        diagnostics_.lastError = error.what();
        if (!ScheduleRetryLocked(phase, operation)) {
            pendingByOperation_.erase(operation);
        }
        drained_.notify_all();
        RETURN_STATUS(K_RUNTIME_ERROR, std::string("enqueue topology callback failed: ") + error.what());
    } catch (...) {
        --callbackBodies_;
        inFlightByOperation_.erase(operation);
        diagnostics_.queuedCallbacks = callbackBodies_;
        ++diagnostics_.failed;
        diagnostics_.lastError = "enqueue topology callback failed with an unknown exception";
        if (!ScheduleRetryLocked(phase, operation)) {
            pendingByOperation_.erase(operation);
        }
        drained_.notify_all();
        RETURN_STATUS(K_RUNTIME_ERROR, diagnostics_.lastError);
    }
    return Status::OK();
}

Status TopologyTaskExecutor::AdmitCallbackLocked(const TopologyTask &task, const TopologyExecutionFence &fence,
                                                 const std::string &operation,
                                                 std::shared_ptr<CancellationToken> &cancellation)
{
    CHECK_FAIL_RETURN_STATUS(started_ && !stopping_, K_NOT_READY, "topology task Executor is stopping");
    if (inFlightByOperation_.count(operation) > 0) {
        return Status::OK();
    }
    const auto now = std::chrono::steady_clock::now();
    pendingByOperation_[operation] = task;
    if (progressReadyByOperation_.count(operation) > 0) {
        nextAttemptByOperation_[operation] = now;
        return Status::OK();
    }
    RETURN_IF_NOT_OK(ValidateCallbackWindowLocked(fence, now));
    const size_t capacity = options_.callbackThreads + options_.callbackQueueCapacity;
    CHECK_FAIL_RETURN_STATUS(inFlightByOperation_.size() < capacity, K_TRY_AGAIN, "topology callback queue is full");
    cancellation = std::make_shared<CancellationToken>();
    inFlightByOperation_[operation] = cancellation;
    nextAttemptByOperation_.erase(operation);
    ++attemptsByOperation_[operation];
    ++callbackBodies_;
    diagnostics_.queuedCallbacks = callbackBodies_;
    diagnostics_.inFlightCallbacks = inFlightByOperation_.size();
    return Status::OK();
}

Status TopologyTaskExecutor::ValidateCallbackWindowLocked(const TopologyExecutionFence &fence,
                                                          std::chrono::steady_clock::time_point now)
{
    if (fence.phase == TopologyCallbackPhase::FAILURE) {
        const auto iter = failureDeadlineByEpoch_.emplace(fence.batchEpoch, now + options_.failureBatchWindow).first;
        CHECK_FAIL_RETURN_STATUS(now < iter->second, K_RPC_DEADLINE_EXCEEDED,
                                 "Failure topology callback window exhausted");
        return Status::OK();
    }
    const auto iter = ordinaryDeadlineByMember_.emplace(fence.executor.id, now + options_.ordinaryMemberWindow).first;
    CHECK_FAIL_RETURN_STATUS(now < iter->second, K_RPC_DEADLINE_EXCEEDED,
                             "ordinary topology callback window exhausted");
    return Status::OK();
}

void TopologyTaskExecutor::ExecuteCallback(TopologyExecutionFence fence, std::string operation,
                                           std::shared_ptr<CancellationToken> cancellation) noexcept
{
    const auto phase = fence.phase;
    auto callbackStatus = Status::OK();
    auto submitStatus = Status(K_RUNTIME_ERROR, "topology callback completion was not submitted");
    std::unique_ptr<TopologyPreparedCleanup> cleanup;
    std::chrono::steady_clock::time_point deadline;
    try {
        callbackStatus = RunCallback(fence, operation, *cancellation, cleanup, deadline);
        TopologyCallbackCompletion completion{ std::move(fence), operation, deadline, callbackStatus,
                                               std::move(cleanup) };
        submitStatus = dispatcher_.SubmitCompletion(std::move(completion));
    } catch (const std::exception &error) {
        callbackStatus = Status(K_RUNTIME_ERROR, std::string("topology callback body threw: ") + error.what());
    } catch (...) {
        callbackStatus = Status(K_RUNTIME_ERROR, "topology callback body threw an unknown exception");
    }
    FinishCallbackBody(phase, operation, callbackStatus, submitStatus);
}

Status TopologyTaskExecutor::RunCallback(const TopologyExecutionFence &fence, const std::string &operation,
                                         const CancellationToken &cancellation,
                                         std::unique_ptr<TopologyPreparedCleanup> &cleanup,
                                         std::chrono::steady_clock::time_point &deadline)
{
    std::shared_ptr<const TopologySnapshot> snapshot;
    std::unique_ptr<IKeyFilter> filter;
    std::unique_ptr<StorageScanPlan> plan;
    TopologyPhaseAction action;
    auto rc = ValidateFence(fence, snapshot);
    if (rc.IsOk()) {
        rc = BuildPhaseAction(fence, snapshot->Version(), action);
    }
    if (rc.IsOk()) {
        rc = BuildOpaqueScope(fence, filter, plan);
    }
    if (rc.IsOk()) {
        rc = ResolveCallbackDeadline(fence, deadline);
    }
    if (rc.IsOk() && cancellation.IsCancelled()) {
        rc = Status(K_NOT_READY, "topology callback cancelled");
    }
    if (rc.IsOk()) {
        TopologyCallbackContext context{ action, operation, deadline, cancellation, *filter, *plan };
        rc = InvokeCallback(fence, context, cleanup);
        if (std::chrono::steady_clock::now() >= deadline && !cancellation.IsCancelled()) {
            rc = Status(K_RPC_DEADLINE_EXCEEDED, "topology callback exceeded its deadline");
        }
    }
    return rc;
}

Status TopologyTaskExecutor::ResolveCallbackDeadline(const TopologyExecutionFence &fence,
                                                     std::chrono::steady_clock::time_point &deadline) const
{
    const bool failure = fence.phase == TopologyCallbackPhase::FAILURE;
    const auto callbackBudget = failure ? options_.failureCallbackDeadline : options_.ordinaryCallbackDeadline;
    deadline = std::chrono::steady_clock::now() + callbackBudget;
    std::lock_guard<std::mutex> lock(mutex_);
    if (failure) {
        auto iter = failureDeadlineByEpoch_.find(fence.batchEpoch);
        CHECK_FAIL_RETURN_STATUS(iter != failureDeadlineByEpoch_.end(), K_INVALID,
                                 "missing Failure callback budget ledger");
        deadline = std::min(deadline, iter->second);
    } else {
        auto iter = ordinaryDeadlineByMember_.find(fence.executor.id);
        CHECK_FAIL_RETURN_STATUS(iter != ordinaryDeadlineByMember_.end(), K_INVALID,
                                 "missing ordinary callback budget ledger");
        deadline = std::min(deadline, iter->second);
    }
    return Status::OK();
}

void TopologyTaskExecutor::FinishCallbackBody(TopologyCallbackPhase phase, const std::string &operation,
                                              const Status &callbackStatus, const Status &submitStatus) noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    --callbackBodies_;
    diagnostics_.queuedCallbacks = callbackBodies_;
    drained_.notify_all();
    if (submitStatus.IsError()) {
        inFlightByOperation_.erase(operation);
        ++diagnostics_.failed;
        bool scheduled = false;
        try {
            diagnostics_.lastError = submitStatus.ToString();
            const bool failurePhase = phase == TopologyCallbackPhase::FAILURE;
            if (failurePhase) {
                progressReadyByOperation_.insert(operation);
                if (callbackStatus.IsError()) {
                    bestEffortFailureByOperation_.insert(operation);
                }
            }
            const bool retryableCallback = failurePhase || callbackStatus.IsOk() || IsOrdinaryRetryable(callbackStatus);
            scheduled = !stopping_ && retryableCallback && ScheduleRetryLocked(phase, operation);
            if (scheduled) {
                diagnostics_.lastError += "; completion rescheduled";
            }
        } catch (...) {
            // The callback worker must never terminate the process on diagnostic or retry-ledger allocation failure.
            nextAttemptByOperation_.erase(operation);
        }
        if (!scheduled) {
            pendingByOperation_.erase(operation);
            nextAttemptByOperation_.erase(operation);
        }
    }
}

Status TopologyTaskExecutor::InvokeCallback(const TopologyExecutionFence &fence, const TopologyCallbackContext &context,
                                            std::unique_ptr<TopologyPreparedCleanup> &cleanup) noexcept
{
    try {
        if (fence.phase == TopologyCallbackPhase::SCALE_OUT) {
            return callbacks_.OnScaleOut(context);
        }
        if (fence.phase == TopologyCallbackPhase::FAILURE) {
            return callbacks_.OnFailure(context);
        }
        auto rc = callbacks_.OnScaleIn(context);
        if (rc.IsOk()) {
            rc = callbacks_.PrepareScaleInCleanup(context, cleanup);
        }
        if (rc.IsOk() && cleanup == nullptr) {
            return Status(K_INVALID, "missing prepared topology cleanup");
        }
        return rc;
    } catch (const std::exception &error) {
        return Status(K_RUNTIME_ERROR, std::string("topology callback threw: ") + error.what());
    } catch (...) {
        return Status(K_RUNTIME_ERROR, "topology callback threw an unknown exception");
    }
}

Status TopologyTaskExecutor::TryAuthorizeCleanup(const TopologyExecutionFence &fence,
                                                 TopologyPreparedCleanup &cleanup)
{
    std::shared_ptr<const TopologySnapshot> latest;
    RETURN_IF_NOT_OK(ValidateFence(fence, latest));
    try {
        return snapshots_.AuthorizeCleanupIfCurrent(*latest, [&cleanup] { return cleanup.Authorize(); });
    } catch (const std::exception &error) {
        return Status(K_RUNTIME_ERROR, std::string("topology cleanup authorization threw: ") + error.what());
    } catch (...) {
        return Status(K_RUNTIME_ERROR, "topology cleanup authorization threw an unknown exception");
    }
}

Status TopologyTaskExecutor::SubmitCleanupApply(const TopologyExecutionFence &fence, const std::string &operation,
                                                std::chrono::steady_clock::time_point deadline,
                                                std::unique_ptr<TopologyPreparedCleanup> cleanup)
{
    std::shared_ptr<TopologyPreparedCleanup> ownedCleanup;
    try {
        ownedCleanup = std::shared_ptr<TopologyPreparedCleanup>(std::move(cleanup));
    } catch (const std::exception &error) {
        return Status(K_TRY_AGAIN, std::string("own topology cleanup effect failed: ") + error.what());
    } catch (...) {
        return Status(K_TRY_AGAIN, "own topology cleanup effect failed with an unknown exception");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(started_ && !stopping_ && callbackPool_ != nullptr, K_NOT_READY,
                             "topology cleanup Executor is stopping");
    const auto iter = inFlightByOperation_.find(operation);
    CHECK_FAIL_RETURN_STATUS(iter != inFlightByOperation_.end(), K_INVALID,
                             "topology cleanup operation is no longer in flight");
    ++callbackBodies_;
    diagnostics_.queuedCallbacks = callbackBodies_;
    try {
        callbackPool_->Execute(
            [this, fence, operation, deadline, ownedCleanup, cancellation = iter->second]() mutable {
                ExecuteCleanupApply(std::move(fence), std::move(operation), deadline, std::move(ownedCleanup),
                                    std::move(cancellation));
            });
    } catch (const std::exception &error) {
        --callbackBodies_;
        diagnostics_.queuedCallbacks = callbackBodies_;
        drained_.notify_all();
        return Status(K_TRY_AGAIN, std::string("enqueue topology cleanup effect failed: ") + error.what());
    } catch (...) {
        --callbackBodies_;
        diagnostics_.queuedCallbacks = callbackBodies_;
        drained_.notify_all();
        return Status(K_TRY_AGAIN, "enqueue topology cleanup effect failed with an unknown exception");
    }
    return Status::OK();
}

void TopologyTaskExecutor::ExecuteCleanupApply(TopologyExecutionFence fence, std::string operation,
                                               std::chrono::steady_clock::time_point deadline,
                                               std::shared_ptr<TopologyPreparedCleanup> cleanup,
                                               std::shared_ptr<CancellationToken> cancellation) noexcept
{
    const auto phase = fence.phase;
    auto effectStatus = Status::OK();
    auto submitStatus = Status(K_RUNTIME_ERROR, "topology cleanup completion was not submitted");
    try {
        if (cancellation->IsCancelled()) {
            effectStatus = Status(K_NOT_READY, "topology cleanup cancelled");
        } else if (std::chrono::steady_clock::now() >= deadline) {
            effectStatus = Status(K_RPC_DEADLINE_EXCEEDED, "topology cleanup deadline exceeded");
        } else {
            effectStatus = cleanup->Apply(deadline, *cancellation);
        }
    } catch (const std::exception &error) {
        effectStatus = Status(K_RUNTIME_ERROR, std::string("topology cleanup effect threw: ") + error.what());
    } catch (...) {
        effectStatus = Status(K_RUNTIME_ERROR, "topology cleanup effect threw an unknown exception");
    }
    try {
        TopologyCallbackCompletion completion{ std::move(fence), operation, deadline, effectStatus, nullptr };
        submitStatus = dispatcher_.SubmitCompletion(std::move(completion));
    } catch (const std::exception &error) {
        submitStatus =
            Status(K_RUNTIME_ERROR, std::string("submit topology cleanup completion threw: ") + error.what());
    } catch (...) {
        submitStatus = Status(K_RUNTIME_ERROR, "submit topology cleanup completion threw an unknown exception");
    }
    FinishCallbackBody(phase, operation, effectStatus, submitStatus);
}

bool TopologyTaskExecutor::IsOrdinaryRetryable(const Status &status) const noexcept
{
    return status.GetCode() == K_TRY_AGAIN || status.GetCode() == K_NOT_READY || status.GetCode() == K_RPC_CANCELLED
           || status.GetCode() == K_RPC_DEADLINE_EXCEEDED || status.GetCode() == K_RPC_UNAVAILABLE;
}

Status TopologyTaskExecutor::HandleCompletion(TopologyCallbackCompletion completion)
{
    const auto operation = TopologyTaskMaterializer::BuildBusinessOperationId(completion.fence.phase, completion.fence);
    if (completion.businessOperationId != operation) {
        return CompleteStale(operation, Status(K_INVALID, "topology completion operation id mismatch"));
    }
    if (DiscardIfStopping(operation)) {
        return Status::OK();
    }
    const bool needsAuthorization = completion.status.IsOk() && completion.preparedCleanup != nullptr;
    auto rc = Status::OK();
    if (!needsAuthorization) {
        rc = ValidateFence(completion.fence);
        if (rc.IsError()) {
            return CompleteStale(operation, rc);
        }
    }
    if (needsAuthorization) {
        if (std::chrono::steady_clock::now() >= completion.deadline) {
            rc = Status(K_RPC_DEADLINE_EXCEEDED, "topology cleanup authorization exceeded callback deadline");
        } else {
            rc = TryAuthorizeCleanup(completion.fence, *completion.preparedCleanup);
        }
        if (rc.IsOk()) {
            rc = SubmitCleanupApply(completion.fence, operation, completion.deadline,
                                    std::move(completion.preparedCleanup));
            if (rc.IsOk()) {
                return Status::OK();
            }
        }
    } else {
        rc = completion.status;
    }
    if (rc.IsOk()) {
        return CompleteProgress(completion, operation);
    }
    if (completion.fence.phase == TopologyCallbackPhase::FAILURE) {
        LOG(WARNING) << "CLUSTER_FAILURE action=recover epoch=" << completion.fence.batchEpoch
                     << " task_prefix=" << completion.fence.taskId.substr(0, TASK_DIAGNOSTIC_PREFIX_SIZE)
                     << " outcome=best_effort_failed status=" << rc.ToString();
        completion.status = rc;
        return CompleteProgress(completion, operation);
    }
    return CompleteFailure(completion.fence, operation, rc, false);
}

Status TopologyTaskExecutor::CompleteProgress(TopologyCallbackCompletion &completion, const std::string &operation)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        progressReadyByOperation_.insert(operation);
        if (completion.fence.phase == TopologyCallbackPhase::FAILURE && completion.status.IsError()) {
            bestEffortFailureByOperation_.insert(operation);
            diagnostics_.lastError = completion.status.ToString();
        }
    }
    TaskProgressOutcome outcome;
    auto rc = repository_.MarkTaskScopeFinished(completion.fence, outcome);
    if (rc.IsError()) {
        return CompleteFailure(completion.fence, operation, rc, true);
    }
    if (outcome != TaskProgressOutcome::UPDATED && outcome != TaskProgressOutcome::ALREADY_FINISHED) {
        return CompleteStale(operation, Status(K_INVALID, "topology task progress fence is stale"));
    }
    std::lock_guard<std::mutex> lock(mutex_);
    inFlightByOperation_.erase(operation);
    pendingByOperation_.erase(operation);
    nextAttemptByOperation_.erase(operation);
    progressReadyByOperation_.erase(operation);
    const bool bestEffortFailure = bestEffortFailureByOperation_.erase(operation) > 0;
    if (bestEffortFailure) {
        ++diagnostics_.failed;
    } else {
        ++diagnostics_.succeeded;
    }
    VLOG(1) << "CLUSTER_TASK action=progress type=" << static_cast<uint32_t>(completion.fence.batchType)
            << " epoch=" << completion.fence.batchEpoch
            << " task_prefix=" << completion.fence.taskId.substr(0, TASK_DIAGNOSTIC_PREFIX_SIZE)
            << " outcome=" << (bestEffortFailure ? "best_effort_failed" : "finished");
    return Status::OK();
}

Status TopologyTaskExecutor::CompleteFailure(const TopologyExecutionFence &fence, const std::string &operation,
                                             const Status &status, bool progressFailure)
{
    std::lock_guard<std::mutex> lock(mutex_);
    inFlightByOperation_.erase(operation);
    ++diagnostics_.failed;
    diagnostics_.lastError = status.ToString();
    const bool failurePhase = fence.phase == TopologyCallbackPhase::FAILURE;
    if ((progressFailure || (!failurePhase && IsOrdinaryRetryable(status)))
        && ScheduleRetryLocked(fence.phase, operation)) {
        return progressFailure ? status : Status::OK();
    }
    pendingByOperation_.erase(operation);
    nextAttemptByOperation_.erase(operation);
    progressReadyByOperation_.erase(operation);
    bestEffortFailureByOperation_.erase(operation);
    return status;
}

Status TopologyTaskExecutor::CompleteStale(const std::string &operation, const Status &status)
{
    std::lock_guard<std::mutex> lock(mutex_);
    inFlightByOperation_.erase(operation);
    pendingByOperation_.erase(operation);
    nextAttemptByOperation_.erase(operation);
    progressReadyByOperation_.erase(operation);
    bestEffortFailureByOperation_.erase(operation);
    ++diagnostics_.stale;
    diagnostics_.lastError = status.ToString();
    return status;
}

bool TopologyTaskExecutor::DiscardIfStopping(const std::string &operation)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (started_ && !stopping_) {
        return false;
    }
    const auto cancelled = inFlightByOperation_.erase(operation);
    pendingByOperation_.erase(operation);
    nextAttemptByOperation_.erase(operation);
    progressReadyByOperation_.erase(operation);
    bestEffortFailureByOperation_.erase(operation);
    diagnostics_.cancelled += cancelled;
    return true;
}

bool TopologyTaskExecutor::ScheduleRetryLocked(TopologyCallbackPhase phase, const std::string &operation)
{
    if (pendingByOperation_.count(operation) == 0) {
        return false;
    }
    if (phase != TopologyCallbackPhase::FAILURE
        && attemptsByOperation_[operation] >= options_.ordinaryMaxAttempts) {
        return false;
    }
    nextAttemptByOperation_[operation] =
        std::chrono::steady_clock::now() + RetryDelay(attemptsByOperation_[operation], options_);
    return true;
}

Status TopologyTaskExecutor::HandleTick(std::chrono::steady_clock::time_point now)
{
    std::vector<std::pair<std::string, TopologyTask>> due;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(started_ && !stopping_, K_NOT_READY, "topology task Executor is stopping");
        for (const auto &[operation, nextAttempt] : nextAttemptByOperation_) {
            if (nextAttempt <= now && inFlightByOperation_.count(operation) == 0) {
                auto task = pendingByOperation_.find(operation);
                if (task != pendingByOperation_.end()) {
                    due.emplace_back(operation, task->second);
                }
            }
        }
    }
    auto firstError = Status::OK();
    for (const auto &[operation, task] : due) {
        TopologyExecutionFence fence;
        auto rc = BuildExecutionFence(task, TaskPhase(task), fence);
        bool progressOnly = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            progressOnly = progressReadyByOperation_.count(operation) > 0;
        }
        if (rc.IsError()) {
            PreserveDueOperation(operation, task, rc);
        } else if (progressOnly) {
            TopologyCallbackCompletion completion{ fence, operation, {}, Status::OK(), nullptr };
            rc = CompleteProgress(completion, operation);
        } else {
            rc = SubmitCallback(task, std::move(fence));
            if (rc.IsError()) {
                PreserveDueOperation(operation, task, rc);
            }
        }
        if (rc.IsError() && firstError.IsOk()) {
            firstError = rc;
        }
    }
    return firstError;
}

void TopologyTaskExecutor::PreserveDueOperation(const std::string &operation, const TopologyTask &task,
                                                const Status &status)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_ || pendingByOperation_.count(operation) == 0) {
        return;
    }
    diagnostics_.lastError = status.ToString();
    if (status.GetCode() == K_INVALID || status.GetCode() == K_NOT_FOUND) {
        pendingByOperation_.erase(operation);
        nextAttemptByOperation_.erase(operation);
        progressReadyByOperation_.erase(operation);
        bestEffortFailureByOperation_.erase(operation);
        ++diagnostics_.stale;
        return;
    }
    pendingByOperation_[operation] = task;
    nextAttemptByOperation_[operation] = std::chrono::steady_clock::now() + options_.backoffInitial;
}

Status TopologyTaskExecutor::Stop(std::chrono::steady_clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (!started_) {
        return Status::OK();
    }
    stopping_ = true;
    for (auto &[operation, cancellation] : inFlightByOperation_) {
        cancellation->Cancel();
    }
    bool hasFailure = false;
    for (const auto &[operation, task] : pendingByOperation_) {
        hasFailure = hasFailure || std::holds_alternative<TopologyDeleteTask>(task);
    }
    const auto drain = hasFailure ? options_.failureDrain : options_.ordinaryDrain;
    const auto effectiveDeadline = std::min(deadline, std::chrono::steady_clock::now() + drain);
    Status drainStatus;
    if (!drained_.wait_until(lock, effectiveDeadline, [this] { return callbackBodies_ == 0; })) {
        drainStatus = Status(K_RPC_DEADLINE_EXCEEDED, "topology callback drain deadline exceeded");
    }
    lock.unlock();
    // Joining accepted callbacks is required for object lifetime safety. The process lifecycle manager enforces the
    // outer termination budget if a callback violates cooperative cancellation.
    callbackPool_.reset();
    lock.lock();
    diagnostics_.cancelled += inFlightByOperation_.size();
    inFlightByOperation_.clear();
    pendingByOperation_.clear();
    attemptsByOperation_.clear();
    nextAttemptByOperation_.clear();
    ordinaryDeadlineByMember_.clear();
    failureDeadlineByEpoch_.clear();
    progressReadyByOperation_.clear();
    bestEffortFailureByOperation_.clear();
    started_ = false;
    diagnostics_.running = false;
    diagnostics_.inFlightCallbacks = 0;
    return drainStatus;
}

TopologyTaskExecutorDiagnostics TopologyTaskExecutor::GetDiagnostics() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto diagnostics = diagnostics_;
    diagnostics.inFlightCallbacks = inFlightByOperation_.size();
    return diagnostics;
}

}  // namespace datasystem::cluster
