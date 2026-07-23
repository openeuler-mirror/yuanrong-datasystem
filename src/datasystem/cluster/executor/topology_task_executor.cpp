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
#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem::cluster {
namespace {
constexpr char EXECUTOR_POOL_NAME[] = "topology-callback";
constexpr uint32_t MAX_BACKOFF_SHIFT = 20;
constexpr size_t MAX_CALLBACK_THREADS = 16;
constexpr size_t MAX_CALLBACK_QUEUE_CAPACITY = 1'024;
constexpr int32_t EXECUTOR_AUTHORITY_READ_TIMEOUT_MS = 3'000;
constexpr size_t OPERATION_ID_PREFIX_SIZE = sizeof("op-") - 1;
std::atomic<uint64_t> g_retrySeed{ 0 };

const char *CallbackPhaseName(TopologyCallbackPhase phase)
{
    switch (phase) {
        case TopologyCallbackPhase::SCALE_OUT:
            return "SCALE_OUT";
        case TopologyCallbackPhase::SCALE_IN:
            return "SCALE_IN";
        case TopologyCallbackPhase::SCALE_IN_CLEANUP:
            return "SCALE_IN_CLEANUP";
        case TopologyCallbackPhase::FAILURE:
            return "FAILURE";
        default:
            return "UNKNOWN";
    }
}

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

std::string ScaleInMetadataGateKey(uint64_t epoch, const std::string &sourceId)
{
    return std::to_string(epoch) + ":" + sourceId;
}

Status ScaleInMetadataGateKey(const TopologyExecutionFence &fence, std::string &gate)
{
    CHECK_FAIL_RETURN_STATUS(fence.phase == TopologyCallbackPhase::SCALE_IN && fence.source.has_value(), K_INVALID,
                             "invalid ScaleIn metadata gate fence");
    gate = ScaleInMetadataGateKey(fence.batchEpoch, fence.source->id);
    return Status::OK();
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
    const auto activeBatch = snapshot->GetActiveBatch();
    if (!activeBatch.has_value() || activeBatch->type != notify.type) {
        VLOG(TOPOLOGY_VERBOSE_LOG_LEVEL)
            << "CLUSTER_TASK action=ignore_stale_notify type=" << static_cast<uint32_t>(notify.type)
            << " type_name=" << TopologyChangeTypeName(notify.type)
            << " topology_version=" << snapshot->Version();
        return Status::OK();
    }
    const auto epoch = activeBatch->epoch;
    std::string scaleInMetadataGate;
    RETURN_IF_NOT_OK(BuildScaleInMetadataGateForNotify(notify, *snapshot, epoch, scaleInMetadataGate));
    LOG(INFO) << "CLUSTER_TASK action=notify type=" << static_cast<uint32_t>(notify.type)
              << " type_name=" << TopologyChangeTypeName(notify.type)
              << " epoch=" << epoch
              << " local_address=" << localAddress_ << " task_count=" << notify.taskIds.size();
    if (notify.type == TopologyChangeType::SCALE_IN) {
        LOG(INFO) << "CLUSTER_SCALE_IN action=notify"
                  << " epoch=" << epoch
                  << " executor=" << localAddress_
                  << " task_count=" << notify.taskIds.size()
                  << " gate_prefix=" << TopologyDiagnosticPrefix(scaleInMetadataGate);
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        RETURN_IF_NOT_OK(RefreshNotifyEpochLocked(epoch));
    }
    return SubmitNotifiedTasks(notify, epoch);
}

Status TopologyTaskExecutor::BuildScaleInMetadataGateForNotify(const TopologyTaskNotify &notify,
                                                               const TopologySnapshot &snapshot, uint64_t epoch,
                                                               std::string &gate) const
{
    gate.clear();
    if (notify.type != TopologyChangeType::SCALE_IN) {
        return Status::OK();
    }
    const Member *source = nullptr;
    RETURN_IF_NOT_OK(snapshot.FindMemberByAddress(localAddress_, source));
    gate = ScaleInMetadataGateKey(epoch, source->identity.id);
    return Status::OK();
}

Status TopologyTaskExecutor::RefreshNotifyEpochLocked(uint64_t epoch)
{
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
        scaleInMetadataPendingByGate_.clear();
        scaleInMetadataGateByOperation_.clear();
        scaleInMetadataDoneByOperation_.clear();
        bestEffortFailureByOperation_.clear();
    }
    currentEpoch_ = epoch;
    return Status::OK();
}

Status TopologyTaskExecutor::SubmitNotifiedTasks(const TopologyTaskNotify &notify, uint64_t epoch)
{
    auto firstError = Status::OK();
    for (const auto &taskId : notify.taskIds) {
        const auto kind =
            notify.type == TopologyChangeType::FAILURE ? TopologyTaskKind::DELETE_MEMBER : TopologyTaskKind::MIGRATE;
        TopologyTask task;
        auto rc = repository_.ReadTask(kind, taskId, notify.type, epoch, task);
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

Status TopologyTaskExecutor::SubmitCallback(const TopologyTask &task, TopologyExecutionFence fence,
                                            bool allowScaleInDataDrain)
{
    const auto operation = TopologyTaskMaterializer::BuildBusinessOperationId(fence.phase, fence);
    const auto phase = fence.phase;
    CHECK_FAIL_RETURN_STATUS(operation.size() > OPERATION_ID_PREFIX_SIZE, K_RUNTIME_ERROR,
                             "build topology callback operation digest failed");
    const auto batchType = fence.batchType;
    const auto batchEpoch = fence.batchEpoch;
    const auto taskPrefix = TopologyDiagnosticPrefix(fence.taskId);
    const auto taskScope = TopologyTaskScopeForLog(fence);
    const auto stage = TopologyCallbackStageName(phase, allowScaleInDataDrain);
    std::shared_ptr<CancellationToken> cancellation;
    std::unique_lock<std::mutex> lock(mutex_);
    RETURN_IF_NOT_OK(AdmitCallbackLocked(task, fence, operation, allowScaleInDataDrain, cancellation));
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
            EraseScaleInOperationLocked(operation);
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
            EraseScaleInOperationLocked(operation);
        }
        drained_.notify_all();
        RETURN_STATUS(K_RUNTIME_ERROR, diagnostics_.lastError);
    }
    lock.unlock();
    LOG(INFO) << "CLUSTER_TASK action=callback_submitted phase=" << CallbackPhaseName(phase)
              << " stage=" << stage << " stage_event=start"
              << " type=" << static_cast<uint32_t>(batchType)
              << " type_name=" << TopologyChangeTypeName(batchType)
              << " epoch=" << batchEpoch
              << " task_prefix=" << taskPrefix
              << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
              << " local_address=" << localAddress_ << " " << taskScope;
    return Status::OK();
}

Status TopologyTaskExecutor::AdmitCallbackLocked(const TopologyTask &task, const TopologyExecutionFence &fence,
                                                 const std::string &operation, bool allowScaleInDataDrain,
                                                 std::shared_ptr<CancellationToken> &cancellation)
{
    CHECK_FAIL_RETURN_STATUS(started_ && !stopping_, K_NOT_READY, "topology task Executor is stopping");
    if (inFlightByOperation_.count(operation) > 0) {
        return Status::OK();
    }
    const auto now = std::chrono::steady_clock::now();
    std::string gate;
    if (fence.phase == TopologyCallbackPhase::SCALE_IN) {
        RETURN_IF_NOT_OK(ScaleInMetadataGateKey(fence, gate));
        scaleInMetadataGateByOperation_[operation] = gate;
    }
    pendingByOperation_[operation] = task;
    if (progressReadyByOperation_.count(operation) > 0) {
        nextAttemptByOperation_[operation] = now;
        return Status::OK();
    }
    if (!allowScaleInDataDrain && scaleInMetadataDoneByOperation_.count(operation) > 0) {
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
    if (fence.phase == TopologyCallbackPhase::SCALE_IN && !allowScaleInDataDrain) {
        scaleInMetadataPendingByGate_[gate].insert(operation);
    }
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
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID("task;" + GetStringUuid());
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
    const auto start = std::chrono::steady_clock::now();
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
    const bool scaleInDataDrain =
        fence.phase == TopologyCallbackPhase::SCALE_IN && IsScaleInDataDrainReady(operation);
    LOG(INFO) << "CLUSTER_TASK action=callback_finished phase=" << CallbackPhaseName(fence.phase)
              << " stage=" << TopologyCallbackStageName(fence.phase, scaleInDataDrain) << " stage_event=finish"
              << " type=" << static_cast<uint32_t>(fence.batchType)
              << " type_name=" << TopologyChangeTypeName(fence.batchType)
              << " epoch=" << fence.batchEpoch
              << " task_prefix=" << TopologyDiagnosticPrefix(fence.taskId)
              << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
              << " local_address=" << localAddress_
              << " " << TopologyTaskScopeForLog(fence)
              << " elapsed_ms=" << DurationMs(start, std::chrono::steady_clock::now())
              << " status=" << rc.ToString();
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
            EraseScaleInOperationLocked(operation);
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
        const bool dataDrain = IsScaleInDataDrainReady(context.businessOperationId);
        auto rc = dataDrain ? callbacks_.OnScaleInDataDrain(context) : callbacks_.OnScaleIn(context);
        if (dataDrain && rc.IsOk()) {
            rc = callbacks_.PrepareScaleInCleanup(context, cleanup);
        }
        if (dataDrain && rc.IsOk() && cleanup == nullptr) {
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
    {
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
    }
    LOG(INFO) << "CLUSTER_TASK action=cleanup_submitted stage=cleanup stage_event=start"
              << " type_name=" << TopologyChangeTypeName(fence.batchType) << " epoch=" << fence.batchEpoch
              << " task_prefix=" << TopologyDiagnosticPrefix(fence.taskId)
              << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
              << " local_address=" << localAddress_ << " " << TopologyTaskScopeForLog(fence);
    return Status::OK();
}

void TopologyTaskExecutor::ExecuteCleanupApply(TopologyExecutionFence fence, std::string operation,
                                               std::chrono::steady_clock::time_point deadline,
                                               std::shared_ptr<TopologyPreparedCleanup> cleanup,
                                               std::shared_ptr<CancellationToken> cancellation) noexcept
{
    const auto start = std::chrono::steady_clock::now();
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
    LOG(INFO) << "CLUSTER_TASK action=cleanup_finished stage=cleanup stage_event=finish"
              << " type_name=" << TopologyChangeTypeName(fence.batchType) << " epoch=" << fence.batchEpoch
              << " task_prefix=" << TopologyDiagnosticPrefix(fence.taskId)
              << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
              << " local_address=" << localAddress_ << " " << TopologyTaskScopeForLog(fence)
              << " elapsed_ms=" << DurationMs(start, std::chrono::steady_clock::now())
              << " status=" << effectStatus.ToString();
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
        if (completion.fence.phase == TopologyCallbackPhase::SCALE_IN && completion.status.IsOk()
            && completion.preparedCleanup == nullptr && !IsScaleInDataDrainReady(operation)) {
            return CompleteScaleInMetadata(completion, operation);
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
                     << " task_prefix=" << TopologyDiagnosticPrefix(completion.fence.taskId)
                     << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
                     << " stage=failure_recovery stage_event=finish "
                     << TopologyTaskScopeForLog(completion.fence)
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
    bool bestEffortFailure = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        inFlightByOperation_.erase(operation);
        pendingByOperation_.erase(operation);
        nextAttemptByOperation_.erase(operation);
        progressReadyByOperation_.erase(operation);
        EraseScaleInOperationLocked(operation);
        bestEffortFailure = bestEffortFailureByOperation_.erase(operation) > 0;
        if (bestEffortFailure) {
            ++diagnostics_.failed;
        } else {
            ++diagnostics_.succeeded;
        }
    }
    LOG(INFO) << "CLUSTER_TASK action=progress type=" << static_cast<uint32_t>(completion.fence.batchType)
              << " type_name=" << TopologyChangeTypeName(completion.fence.batchType)
              << " phase=" << CallbackPhaseName(completion.fence.phase)
              << " stage=task_completion stage_event=finish"
              << " epoch=" << completion.fence.batchEpoch
              << " task_prefix=" << TopologyDiagnosticPrefix(completion.fence.taskId)
              << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
              << " local_address=" << localAddress_
              << " " << TopologyTaskScopeForLog(completion.fence)
              << " outcome=" << (bestEffortFailure ? "best_effort_failed" : "finished")
              << " repository_outcome=" << static_cast<uint32_t>(outcome);
    if (completion.fence.phase == TopologyCallbackPhase::SCALE_IN) {
        LOG(INFO) << "CLUSTER_SCALE_IN action=progress"
                  << " epoch=" << completion.fence.batchEpoch
                  << " source=" << (completion.fence.source.has_value() ? completion.fence.source->address : "")
                  << " source_id_prefix="
                  << (completion.fence.source.has_value() ? MemberIdForLog(completion.fence.source->id) : "")
                  << " target=" << (completion.fence.target.has_value() ? completion.fence.target->address : "")
                  << " target_id_prefix="
                  << (completion.fence.target.has_value() ? MemberIdForLog(completion.fence.target->id) : "")
                  << " executor=" << completion.fence.executor.address
                  << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
                  << " task_prefix=" << TopologyDiagnosticPrefix(completion.fence.taskId)
                  << " outcome=" << (bestEffortFailure ? "best_effort_failed" : "finished");
    }
    return Status::OK();
}

Status TopologyTaskExecutor::CompleteScaleInMetadata(TopologyCallbackCompletion &completion,
                                                     const std::string &operation)
{
    CHECK_FAIL_RETURN_STATUS(completion.fence.source.has_value(), K_INVALID,
                             "ScaleIn metadata completion lacks source");
    auto rc = repository_.MarkScaleInMetadataDone({ completion.fence.batchEpoch, completion.fence.source->id,
                                                    completion.fence.taskId, operation });
    if (rc.IsError()) {
        return CompleteFailure(completion.fence, operation, rc, false);
    }
    std::string gate;
    RETURN_IF_NOT_OK(ScaleInMetadataGateKey(completion.fence, gate));
    const auto now = std::chrono::steady_clock::now();
    bool ready = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        inFlightByOperation_.erase(operation);
        scaleInMetadataGateByOperation_[operation] = gate;
        scaleInMetadataDoneByOperation_.insert(operation);
        auto pending = scaleInMetadataPendingByGate_.find(gate);
        if (pending != scaleInMetadataPendingByGate_.end()) {
            pending->second.erase(operation);
            ready = pending->second.empty();
            if (ready) {
                scaleInMetadataPendingByGate_.erase(pending);
            }
        } else {
            ready = true;
        }
        nextAttemptByOperation_[operation] = ready ? now : now + options_.backoffInitial;
        if (ready) {
            ScheduleScaleInDataDrainReadyLocked(gate);
        }
        diagnostics_.inFlightCallbacks = inFlightByOperation_.size();
    }
    LOG(INFO) << "CLUSTER_SCALE_IN action=metadata_done stage=metadata_migration stage_event=finish"
              << " checkpoint=persisted checkpoint_scope=task source_gate=" << (ready ? "ready" : "waiting")
              << " epoch=" << completion.fence.batchEpoch
              << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
              << " task_prefix=" << TopologyDiagnosticPrefix(completion.fence.taskId)
              << " " << TopologyTaskScopeForLog(completion.fence);
    return Status::OK();
}

Status TopologyTaskExecutor::IsScaleInMetadataGateReadyForOperationLocked(const std::string &operation,
                                                                          bool &ready) const
{
    ready = false;
    auto iter = scaleInMetadataGateByOperation_.find(operation);
    CHECK_FAIL_RETURN_STATUS(iter != scaleInMetadataGateByOperation_.end(), K_INVALID,
                             "ScaleIn metadata gate is missing");
    ready = IsScaleInMetadataGateReadyLocked(iter->second);
    return Status::OK();
}

bool TopologyTaskExecutor::IsScaleInMetadataGateReadyLocked(const std::string &gate) const
{
    auto iter = scaleInMetadataPendingByGate_.find(gate);
    return iter == scaleInMetadataPendingByGate_.end() || iter->second.empty();
}

bool TopologyTaskExecutor::IsScaleInDataDrainReady(const std::string &operation) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return scaleInMetadataDoneByOperation_.count(operation) > 0;
}

void TopologyTaskExecutor::ScheduleScaleInDataDrainReadyLocked(const std::string &gate)
{
    const auto now = std::chrono::steady_clock::now();
    for (const auto &[operation, operationGate] : scaleInMetadataGateByOperation_) {
        if (operationGate == gate && scaleInMetadataDoneByOperation_.count(operation) > 0
            && pendingByOperation_.count(operation) > 0) {
            nextAttemptByOperation_[operation] = now;
        }
    }
}

void TopologyTaskExecutor::EraseScaleInOperationLocked(const std::string &operation)
{
    auto gate = scaleInMetadataGateByOperation_.find(operation);
    if (gate != scaleInMetadataGateByOperation_.end()) {
        auto pending = scaleInMetadataPendingByGate_.find(gate->second);
        if (pending != scaleInMetadataPendingByGate_.end()) {
            pending->second.erase(operation);
            if (pending->second.empty()) {
                scaleInMetadataPendingByGate_.erase(pending);
            }
        }
        scaleInMetadataGateByOperation_.erase(gate);
    }
    scaleInMetadataDoneByOperation_.erase(operation);
}

Status TopologyTaskExecutor::CompleteFailure(const TopologyExecutionFence &fence, const std::string &operation,
                                             const Status &status, bool progressFailure)
{
    const auto taskScope = TopologyTaskScopeForLog(fence);
    bool retryScheduled = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        inFlightByOperation_.erase(operation);
        ++diagnostics_.failed;
        diagnostics_.lastError = status.ToString();
        const bool failurePhase = fence.phase == TopologyCallbackPhase::FAILURE;
        retryScheduled = (progressFailure || (!failurePhase && IsOrdinaryRetryable(status)))
                         && ScheduleRetryLocked(fence.phase, operation);
        if (!retryScheduled) {
            pendingByOperation_.erase(operation);
            nextAttemptByOperation_.erase(operation);
            progressReadyByOperation_.erase(operation);
            EraseScaleInOperationLocked(operation);
            bestEffortFailureByOperation_.erase(operation);
        }
    }
    if (retryScheduled) {
        LOG(WARNING) << "CLUSTER_TASK action=callback_failed phase=" << CallbackPhaseName(fence.phase)
                     << " type=" << static_cast<uint32_t>(fence.batchType)
                     << " type_name=" << TopologyChangeTypeName(fence.batchType)
                     << " epoch=" << fence.batchEpoch
                     << " task_prefix=" << TopologyDiagnosticPrefix(fence.taskId)
                     << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
                     << " local_address=" << localAddress_ << " " << taskScope
                     << " retry_scheduled=true status=" << status.ToString();
        return progressFailure ? status : Status::OK();
    }
    LOG(ERROR) << "CLUSTER_TASK action=callback_failed phase=" << CallbackPhaseName(fence.phase)
               << " type=" << static_cast<uint32_t>(fence.batchType)
               << " type_name=" << TopologyChangeTypeName(fence.batchType)
               << " epoch=" << fence.batchEpoch
               << " task_prefix=" << TopologyDiagnosticPrefix(fence.taskId)
               << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
               << " local_address=" << localAddress_ << " " << taskScope
               << " retry_scheduled=false status=" << status.ToString();
    return status;
}

Status TopologyTaskExecutor::CompleteStale(const std::string &operation, const Status &status)
{
    std::lock_guard<std::mutex> lock(mutex_);
    inFlightByOperation_.erase(operation);
    pendingByOperation_.erase(operation);
    nextAttemptByOperation_.erase(operation);
    progressReadyByOperation_.erase(operation);
    EraseScaleInOperationLocked(operation);
    bestEffortFailureByOperation_.erase(operation);
    ++diagnostics_.stale;
    diagnostics_.lastError = status.ToString();
    LOG(WARNING) << "CLUSTER_TASK action=completion_stale local_address=" << localAddress_
                 << " operation_prefix=" << TopologyDiagnosticPrefix(operation)
                 << " status=" << status.ToString();
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
    EraseScaleInOperationLocked(operation);
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
        bool scaleInMetadataDone = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            progressOnly = progressReadyByOperation_.count(operation) > 0;
            scaleInMetadataDone = scaleInMetadataDoneByOperation_.count(operation) > 0;
        }
        if (rc.IsError()) {
            PreserveDueOperation(operation, task, rc);
        } else if (progressOnly) {
            TopologyCallbackCompletion completion{ fence, operation, {}, Status::OK(), nullptr };
            rc = CompleteProgress(completion, operation);
        } else if (scaleInMetadataDone) {
            bool ready = false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                rc = IsScaleInMetadataGateReadyForOperationLocked(operation, ready);
            }
            if (rc.IsError()) {
                PreserveDueOperation(operation, task, rc);
            } else if (ready) {
                rc = SubmitCallback(task, std::move(fence), true);
                if (rc.IsError()) {
                    PreserveDueOperation(operation, task, rc);
                }
            } else {
                PreserveDueOperation(operation, task, Status(K_NOT_READY, "ScaleIn metadata gate is waiting"));
            }
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
        EraseScaleInOperationLocked(operation);
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
    scaleInMetadataPendingByGate_.clear();
    scaleInMetadataGateByOperation_.clear();
    scaleInMetadataDoneByOperation_.clear();
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
