/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/worker/rebalance_executor.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <thread>
#include <utility>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/math_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"

namespace datasystem {
namespace worker {
namespace {
static const std::string RESOURCE_MONITOR_MASTER = "RESOURCE_MONITOR";
constexpr int REPORT_RESULT_RETRY_TIMES = 3;
constexpr int REPORT_RESULT_RETRY_INTERVAL_MS = 100;
constexpr uint64_t REBALANCE_BATCH_MAX_BYTES = 64ULL * 1024 * 1024;
constexpr size_t REBALANCE_BATCH_MAX_OBJECTS = 512;

#ifdef WITH_TESTS
uint64_t SubtractOffsetOrZero(uint64_t value, int64_t offset)
{
    auto absOffset = static_cast<uint64_t>(-(offset + 1)) + 1;
    return value > absOffset ? value - absOffset : 0;
}
#endif
}  // namespace

RebalanceExecutor::RebalanceExecutor(RebalanceExecutorConfig config)
    : localAddress_(std::move(config.localAddress)),
      metadataRoute_(config.metadataRoute),
      membership_(config.membership),
      endpointPolicy_(config.endpointPolicy),
      exitRequested_(config.exitRequested),
      ubAdmission_(config.ubAdmission),
      akSkManager_(std::move(config.akSkManager)),
      objectTable_(std::move(config.objectTable)),
      evictionManager_(std::move(config.evictionManager)),
      apiManager_(std::move(config.apiManager)),
      candidateProvider_(MakeRebalanceCandidateProvider(evictionManager_, objectTable_)),
      executorPool_(0, 1, "RebalanceExecutor")
{
}

void RebalanceExecutor::Submit(const master::RebalanceTaskPb &task, std::string assignedMasterAddress)
{
    if (task.task_id().empty()) {
        return;
    }

    bool duplicate = false;
    bool busy = false;
    bool paused = false;
    std::string runningTaskId;
    std::shared_ptr<const TerminalResult> terminalReplay;
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        if (terminalResult_ != nullptr && terminalResult_->task.task_id() == task.task_id()) {
            // A predecessor may be retried while its successor is already running. Replay the exact terminal result
            // before reporting busy; otherwise master can turn a completed migration into a false failure.
            terminalReplay = terminalResult_;
        } else if (running_) {
            duplicate = runningTaskId_ == task.task_id();
            busy = !duplicate;
            runningTaskId = runningTaskId_;
        } else if (admissionPaused_) {
            paused = true;
        } else {
            running_ = true;
            runningTaskId_ = task.task_id();
        }
    }
    if (paused) {
        SubmitRejectedResult(task, "source worker rebalance admission is paused");
        return;
    }
    if (duplicate) {
        LOG(INFO) << FormatString("Ignore duplicated rebalance task %s because it is already running", task.task_id());
        return;
    }
    if (busy) {
        SubmitBusyResult(task, runningTaskId);
        return;
    }
    if (terminalReplay != nullptr) {
        LOG(INFO) << FormatString("Replay terminal result for completed rebalance task %s", task.task_id());
        SubmitTerminalResult(std::move(terminalReplay));
        return;
    }

    // Do not block the resource-report thread; run data migration in the single-task executor.
    auto traceId = Trace::Instance().GetTraceID();
    try {
        executorPool_.Execute([this, task, assignedMasterAddress = std::move(assignedMasterAddress), traceId]() {
            ExecuteSubmittedTask(task, assignedMasterAddress, traceId);
        });
    } catch (const std::exception &e) {
        LOG(ERROR) << "Submit rebalance task " << task.task_id() << " failed: " << e.what();
        HandleSubmitFailure(task, e.what());
    }
}

void RebalanceExecutor::ExecuteSubmittedTask(const master::RebalanceTaskPb &task,
                                             const std::string &assignedMasterAddress, const std::string &traceId)
{
    SetRequestContext(nullptr);
    ScopedRequestContext ctx(traceId);
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
    try {
        Execute(task, assignedMasterAddress);
    } catch (const std::exception &e) {
        LOG(ERROR) << "Execute rebalance task " << task.task_id() << " failed by exception: " << e.what();
        HandleSubmitFailure(task, e.what());
    } catch (...) {
        LOG(ERROR) << "Execute rebalance task " << task.task_id() << " failed by unknown exception";
        HandleSubmitFailure(task, "unknown exception");
    }
}

void RebalanceExecutor::HandleSubmitFailure(const master::RebalanceTaskPb &task, const std::string &reason)
{
    TerminalResult result{ task, master::REBALANCE_TASK_FAILED, 0, 0, 0, master::REBALANCE_FAILURE_SOURCE, reason };
    CacheTerminalResultAndMarkDone(result);
    master::ReportRebalanceResultRspPb rsp;
    (void)ReportResult(result, rsp);
}

Status RebalanceExecutor::PauseAndCheckDrained()
{
    std::lock_guard<std::mutex> lock(taskMutex_);
    admissionPaused_ = true;
    cancelRequested_.store(true, std::memory_order_release);
    INJECT_POINT_NO_RETURN("RebalanceExecutor.PauseAndCheckDrained.afterCancelRequested");
    RETURN_OK_IF_TRUE(!running_);
    RETURN_STATUS(K_TRY_AGAIN, "Source rebalance task is still draining");
}

void RebalanceExecutor::Resume()
{
    std::lock_guard<std::mutex> lock(taskMutex_);
    cancelRequested_.store(false, std::memory_order_release);
    admissionPaused_ = false;
}

void RebalanceExecutor::SubmitBusyResult(const master::RebalanceTaskPb &task, const std::string &runningTaskId)
{
    LOG(WARNING) << FormatString("Reject rebalance task %s because task %s is still running", task.task_id(),
                                 runningTaskId);
    try {
        auto traceId = Trace::Instance().GetTraceID();
        executorPool_.Execute([this, task, traceId]() {
            SetRequestContext(nullptr);
            ScopedRequestContext ctx(traceId);
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            ReportFailure(task, master::REBALANCE_FAILURE_SOURCE, "source worker is busy");
        });
    } catch (const std::exception &e) {
        LOG(ERROR) << "Submit busy rebalance result " << task.task_id() << " failed: " << e.what();
    } catch (...) {
        LOG(ERROR) << "Submit busy rebalance result " << task.task_id() << " failed by unknown exception";
    }
}

void RebalanceExecutor::SubmitRejectedResult(const master::RebalanceTaskPb &task, const std::string &reason)
{
    LOG(WARNING) << FormatString("Reject rebalance task %s: %s", task.task_id(), reason);
    try {
        auto traceId = Trace::Instance().GetTraceID();
        executorPool_.Execute([this, task, reason, traceId]() {
            SetRequestContext(nullptr);
            ScopedRequestContext ctx(traceId);
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            ReportFailure(task, master::REBALANCE_FAILURE_SOURCE, reason);
        });
    } catch (const std::exception &e) {
        LOG(ERROR) << "Submit rejected rebalance result " << task.task_id() << " failed: " << e.what();
    }
}

Status RebalanceExecutor::ValidateTask(const master::RebalanceTaskPb &task, HostPort &targetAddr,
                                       uint64_t localDeadlineMs, master::RebalanceFailureSidePb &failureSide) const
{
    if (task.source_worker() != localAddress_.ToString()) {
        failureSide = master::REBALANCE_FAILURE_SOURCE;
        RETURN_STATUS(K_INVALID, FormatString("Task source %s is not local worker %s", task.source_worker(),
                                              localAddress_.ToString()));
    }
    if (task.target_worker().empty()) {
        failureSide = master::REBALANCE_FAILURE_TARGET;
        RETURN_STATUS(K_INVALID, "Rebalance target worker is empty");
    }
    if (task.max_bytes() == 0) {
        failureSide = master::REBALANCE_FAILURE_SOURCE;
        RETURN_STATUS(K_INVALID, "Rebalance max bytes is zero");
    }
    if (IsExpired(localDeadlineMs)) {
        failureSide = master::REBALANCE_FAILURE_CONTROL_PLANE;
        RETURN_STATUS(K_RUNTIME_ERROR, "Rebalance task is expired");
    }
    CHECK_FAIL_RETURN_STATUS(evictionManager_ != nullptr, K_RUNTIME_ERROR, "Eviction manager is not initialized");
    Status rc;
    if (task.has_eviction_policy_fence()) {
        rc = evictionManager_->ValidateRebalancePolicy(static_cast<uint32_t>(task.source_eviction_policy()),
                                                       task.source_eviction_policy_epoch());
        if (rc.IsError()) {
            failureSide = master::REBALANCE_FAILURE_SOURCE;
            return rc;
        }
    }
    rc = targetAddr.ParseString(task.target_worker());
    if (rc.IsError() || targetAddr == localAddress_) {
        failureSide = master::REBALANCE_FAILURE_TARGET;
        return rc.IsError() ? rc : Status(K_INVALID, "Rebalance target can not be local worker");
    }
    failureSide = master::REBALANCE_FAILURE_TARGET;
    return CheckTargetAdmission(targetAddr);
}

Status RebalanceExecutor::CheckTargetAdmission(const HostPort &targetAddr) const
{
    CHECK_FAIL_RETURN_STATUS(endpointPolicy_ != nullptr, K_NOT_READY,
                             "Rebalance endpoint policy is not initialized");
    RETURN_IF_NOT_OK(endpointPolicy_->CheckDataPlaneAdmission(
        targetAddr, object_cache::DataPlaneAdmissionRole::REBALANCE_TARGET));
    if (IsUrmaEnabled() && ubAdmission_ != nullptr) {
        RETURN_IF_NOT_OK(ubAdmission_->CheckWriteTarget(targetAddr, UbOperationKind::MIGRATION_WRITE));
    }
    return Status::OK();
}

bool RebalanceExecutor::IsCancellationRequested() const
{
    return cancelRequested_.load(std::memory_order_acquire);
}

Status RebalanceExecutor::SelectCandidates(object_cache::RebalanceCandidateSession &session, uint64_t maxBytes,
                                           std::unordered_map<std::string, uint64_t> &candidates,
                                           ObjectHeatMap &objectHeats,
                                           const std::unordered_set<std::string> &skipKeys)
{
#ifdef WITH_TESTS
    if (selectHook_ != nullptr) {
        return selectHook_(maxBytes, candidates, objectHeats, skipKeys);
    }
#endif
    // Apply a local batch cap so a large master task does not reserve too many eviction-list objects at once.
    const auto batchBytes = std::min(maxBytes, REBALANCE_BATCH_MAX_BYTES);
    RETURN_IF_NOT_OK(candidateProvider_->Select(session, batchBytes, REBALANCE_BATCH_MAX_OBJECTS, candidates,
                                                objectHeats, &skipKeys));
    CHECK_FAIL_RETURN_STATUS(!candidates.empty(), K_NOT_FOUND, "No object can be selected for rebalance");
    return Status::OK();
}

RebalanceExecutor::MigrateResult RebalanceExecutor::MigrateToTarget(const master::RebalanceTaskPb &task,
                                                                    const HostPort &targetAddr,
                                                                    const std::vector<std::string> &objectKeys,
                                                                    const ObjectHeatMap &objectHeats,
                                                                    object_cache::DataMigrator &migrator)
{
#ifdef WITH_TESTS
    if (migrateHook_ != nullptr) {
        return migrateHook_(task, targetAddr, objectKeys, objectHeats);
    }
#endif
    object_cache::DataMigrator::TargetMigrationOptions options{ .isSlotMigration = false,
                                                                 .objectHeats = objectHeats,
                                                                 .rebalancePolicyFence =
                                                                     BuildRebalancePolicyFence(task),
                                                                 .cancellation = &cancelRequested_ };
    return migrator.MigrateToTargetNode(objectKeys, targetAddr, nullptr, options).get();
}

object_cache::RebalancePolicyFence RebalanceExecutor::BuildRebalancePolicyFence(
    const master::RebalanceTaskPb &task)
{
    object_cache::RebalancePolicyFence fence;
    // Old masters do not populate the policy fields. Preserve that absence on the worker-to-worker request instead
    // of turning the proto defaults (UNSPECIFIED/0) into an enabled fence that a new target must reject.
    fence.enabled = task.has_eviction_policy_fence();
    if (fence.enabled) {
        fence.targetPolicy = static_cast<uint32_t>(task.target_eviction_policy());
        fence.targetEpoch = task.target_eviction_policy_epoch();
        fence.taskId = task.task_id();
    }
    return fence;
}

uint64_t RebalanceExecutor::NowMsForExpiryCheck() const
{
    auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    INJECT_POINT_NO_RETURN("RebalanceExecutor.NowMsForExpiryCheck.addOffsetMs", [&nowMs](int64_t offsetMs) {
        if (offsetMs < 0) {
            nowMs = SubtractOffsetOrZero(nowMs, offsetMs);
            return;
        }
        nowMs = SaturatingAdd(nowMs, static_cast<uint64_t>(offsetMs));
    });
    return nowMs;
}

uint64_t RebalanceExecutor::BuildLocalDeadlineMs(const master::RebalanceTaskPb &task) const
{
    if (task.timeout_ms() == 0) {
        return task.deadline_ms();
    }
    return SaturatingAdd(NowMsForExpiryCheck(), task.timeout_ms());
}

bool RebalanceExecutor::IsExpired(uint64_t localDeadlineMs) const
{
    return localDeadlineMs != 0 && NowMsForExpiryCheck() > localDeadlineMs;
}

bool RebalanceExecutor::IsAssignedMasterUnavailable(const master::RebalanceTaskPb &task,
                                                    ExecutionStats &stats) const
{
    if (membership_ == nullptr || stats.assignedMasterAddress.empty()) {
        return false;
    }
    cluster::MemberEndpoint endpoint;
    auto rc = membership_->ResolveByAddress(stats.assignedMasterAddress, endpoint);
    if (rc.GetCode() == K_NOT_READY) {
        return false;
    }
    bool unavailable = false;
    std::string cause;
    if (rc.GetCode() == K_NOT_FOUND) {
        unavailable = true;
        cause = rc.ToString();
    } else if (rc.IsError()) {
        // ResolveByAddress documents only K_NOT_READY and K_NOT_FOUND errors for address lookup. Preserve the
        // data-safety fence if that contract is ever violated: a false expiry is retryable, but continuing a stale
        // task can mutate placement after its assigning master has disappeared.
        LOG(ERROR) << FormatString("Unexpected topology lookup failure for rebalance task %s, assigned master %s: %s",
                                   task.task_id(), stats.assignedMasterAddress, rc.ToString());
        unavailable = true;
        cause = rc.ToString();
    } else if (endpoint.topologyState == cluster::MemberState::FAILED) {
        unavailable = true;
        cause = "topology state is FAILED";
    } else if (endpoint.localAvailability == cluster::EndpointAvailability::UNREACHABLE) {
        unavailable = true;
        cause = "endpoint is UNREACHABLE";
    }
    if (!unavailable) {
        return false;
    }
    stats.status = master::REBALANCE_TASK_EXPIRED;
    stats.failureSide = master::REBALANCE_FAILURE_CONTROL_PLANE;
    stats.failedReason = FormatString("Assigned cluster master %s is unavailable: %s", stats.assignedMasterAddress,
                                      cause);
    LOG(WARNING) << FormatString(
        "Stop rebalance task %s because assigned cluster master %s is unavailable, topologyState=%d, "
        "localAvailability=%d, status=%s",
        task.task_id(), stats.assignedMasterAddress, static_cast<int>(endpoint.topologyState),
        static_cast<int>(endpoint.localAvailability), rc.ToString());
    return true;
}

Status RebalanceExecutor::ClassifyBatchResult(const MigrateResult &result, const HostPort &targetAddr,
                                              uint64_t batchMigratedBytes, ExecutionStats &stats)
{
    stats.lastBatchAllSkipped = false;
    if (result.status.IsError()) {
        stats.failureSide = ClassifyMigrationFailure(result, targetAddr);
        stats.failedReason = result.status.ToString();
        return result.status;
    }
    if (!result.failedIds.empty()) {
        stats.failureSide = ClassifyMigrationFailure(result, targetAddr);
        stats.failedReason = "some objects failed";
        RETURN_STATUS(K_RUNTIME_ERROR, stats.failedReason);
    }
    if (batchMigratedBytes == 0) {
        if (result.skipIds.empty()) {
            stats.failureSide = master::REBALANCE_FAILURE_NO_CANDIDATE;
            stats.failedReason = "No object migrated in this batch";
        } else {
            stats.lastBatchAllSkipped = true;
            stats.failureSide = master::REBALANCE_FAILURE_SOURCE;
            stats.failedReason = "All candidates skipped (metadata not found)";
        }
        RETURN_STATUS(K_NOT_FOUND, stats.failedReason);
    }
    return Status::OK();
}

Status RebalanceExecutor::ExecuteBatch(object_cache::RebalanceCandidateSession &candidateSession,
                                       const master::RebalanceTaskPb &task, const HostPort &targetAddr,
                                       ExecutionStats &stats, object_cache::DataMigrator &migrator,
                                       std::unordered_set<std::string> &taskSkippedKeys)
{
    std::unordered_map<std::string, uint64_t> candidates;
    ObjectHeatMap objectHeats;
    auto rc = SelectCandidates(candidateSession, task.max_bytes() - stats.migratedBytes, candidates, objectHeats,
                               taskSkippedKeys);
    if (rc.IsError()) {
        stats.lastBatchAllSkipped = false;
        stats.failureSide = rc.GetCode() == K_NOT_FOUND ? master::REBALANCE_FAILURE_NO_CANDIDATE
                                                        : master::REBALANCE_FAILURE_SOURCE;
        return rc;
    }
    // SelectCandidates marks selected objects as rebalancing; the marks must be released after this batch.
    Raii unmarkRebalancingObjects([this, &candidates]() {
        if (evictionManager_ == nullptr) {
            return;
        }
        for (const auto &candidate : candidates) {
            evictionManager_->UnmarkRebalancingObject(candidate.first);
        }
    });

    std::vector<std::string> objectKeys;
    objectKeys.reserve(candidates.size());
    for (const auto &candidate : candidates) {
        objectKeys.emplace_back(candidate.first);
    }

    // Reuse the SPILL migration path; after a successful batch, the lower layer switches the primary copy to
    // the target worker through ReplacePrimary. Under rebalance_keep_local_copy the MigrateType is
    // REBALANCE_KEEP_LOCAL: the source's objectTable entry is kept and demoted to non-primary instead of erased.
    auto result = MigrateToTarget(task, targetAddr, objectKeys, objectHeats, migrator);
    auto batchMigratedBytes = CalculateMigratedBytes(candidates, result);
    stats.migratedBytes += batchMigratedBytes;
    stats.migratedObjects += result.successIds.size();
    stats.failedObjects += result.failedIds.size();
    stats.skippedObjects += result.skipIds.size();
    taskSkippedKeys.insert(result.skipIds.begin(), result.skipIds.end());
    if (result.targetRemainBytes != UINT64_MAX) {
        stats.targetRemainBytes = result.targetRemainBytes;
    }
    return ClassifyBatchResult(result, targetAddr, batchMigratedBytes, stats);
}

void RebalanceExecutor::ExecuteBatches(const master::RebalanceTaskPb &task, const HostPort &targetAddr,
                                       ExecutionStats &stats, uint64_t localDeadlineMs)
{
    // max_bytes is the target amount assigned by master; split it into bounded local batches to avoid reserving
    // too many objects at once.
    // taskSkippedKeys tracks objects skipped (metadata-not-found) within this task so SelectCandidates
    // can skip them in subsequent batches and reach valid candidates behind them. Lifetime is task-scoped:
    // destroyed when ExecuteBatches returns, so each new task starts fresh.
    std::unordered_set<std::string> taskSkippedKeys;
    std::unique_ptr<object_cache::DataMigrator> migrator;
    object_cache::RebalanceCandidateSession candidateSession;
    while (stats.migratedBytes < task.max_bytes()) {
        if (ShouldStopBeforeBatch(targetAddr, stats)) {
            break;
        }
        if (IsCancellationRequested()) {
            stats.failureSide = master::REBALANCE_FAILURE_CONTROL_PLANE;
            stats.failedReason = "Rebalance task cancelled for eviction policy update";
            break;
        }
        if (task.has_eviction_policy_fence()) {
            auto fenceRc = evictionManager_->ValidateRebalancePolicy(
                static_cast<uint32_t>(task.source_eviction_policy()), task.source_eviction_policy_epoch());
            if (fenceRc.IsError()) {
                stats.failureSide = master::REBALANCE_FAILURE_SOURCE;
                stats.failedReason = fenceRc.ToString();
                break;
            }
        }
        if (IsExpired(localDeadlineMs) || IsExitRequested()) {
            stats.status = master::REBALANCE_TASK_EXPIRED;
            stats.failureSide = master::REBALANCE_FAILURE_CONTROL_PLANE;
            stats.failedReason = IsExitRequested() ? "Source worker is exiting" : "Rebalance task is expired";
            break;
        }
        if (IsAssignedMasterUnavailable(task, stats)) {
            break;
        }
        auto initRc = EnsureMigratorInitialized(task, migrator);
        if (initRc.IsError()) {
            stats.failureSide = master::REBALANCE_FAILURE_SOURCE;
            stats.failedReason = "Rebalance topology dependencies are not initialized";
            break;
        }
        auto rc = ExecuteBatch(candidateSession, task, targetAddr, stats, *migrator, taskSkippedKeys);
        if (rc.IsError()) {
            if (ShouldRetryBatchFailure(rc, task, stats)) {
                continue;
            }
            break;
        }
    }
}

bool RebalanceExecutor::IsTopologyBatchActive() const
{
    if (membership_ == nullptr) {
        return false;
    }
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    auto rc = membership_->GetSnapshot(snapshot);
    return rc.IsOk() && snapshot != nullptr && snapshot->GetActiveBatch().has_value();
}

bool RebalanceExecutor::ShouldStopBeforeBatch(const HostPort &targetAddr, ExecutionStats &stats) const
{
    auto rc = CheckTargetAdmission(targetAddr);
    if (rc.IsError()) {
        stats.failureSide = master::REBALANCE_FAILURE_TARGET;
        stats.failedReason = rc.ToString();
        return true;
    }
    if (!IsTopologyBatchActive()) {
        return false;
    }
    stats.failureSide = master::REBALANCE_FAILURE_CONTROL_PLANE;
    stats.failedReason = "Rebalance task stopped because a topology batch is active";
    stats.candidatesExhausted = stats.migratedBytes > 0;
    return true;
}

bool RebalanceExecutor::ShouldRetryBatchFailure(const Status &status, const master::RebalanceTaskPb &task,
                                                ExecutionStats &stats)
{
    // An all-skipped batch after partial success can retry: taskSkippedKeys lets selection scan past missing metadata.
    if (status.GetCode() == K_NOT_FOUND && stats.lastBatchAllSkipped && stats.migratedBytes > 0
        && stats.migratedBytes < task.max_bytes()) {
        return true;
    }
    stats.candidatesExhausted = status.GetCode() == K_NOT_FOUND;
    if (stats.failedReason.empty()) {
        stats.failedReason =
            stats.migratedBytes == 0 ? status.ToString() : "No more object can be selected for rebalance";
    }
    return false;
}

Status RebalanceExecutor::EnsureMigratorInitialized(
    const master::RebalanceTaskPb &task, std::unique_ptr<object_cache::DataMigrator> &migrator)
{
    if (migrator != nullptr) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(metadataRoute_ != nullptr && membership_ != nullptr && endpointPolicy_ != nullptr
                                 && exitRequested_ != nullptr,
                             K_RUNTIME_ERROR, "Rebalance topology dependencies are not initialized");
    const auto migrateType = FLAGS_rebalance_keep_local_copy ? MigrateType::REBALANCE_KEEP_LOCAL : MigrateType::SPILL;
    migrator = std::make_unique<object_cache::DataMigrator>(
        migrateType, *metadataRoute_, *membership_, *endpointPolicy_, exitRequested_, localAddress_, akSkManager_,
        objectTable_, task.task_id(), 0);
    migrator->SetUbAdmission(ubAdmission_);
    migrator->Init();
    return Status::OK();
}

void RebalanceExecutor::ClassifyBatchResult(const master::RebalanceTaskPb &task, bool masterUnavailable,
                                            ExecutionStats &stats)
{
    if (masterUnavailable) {
        return;
    }
    bool targetReached = stats.migratedBytes >= task.max_bytes();
    bool partialCompleted = stats.migratedBytes > 0 && stats.candidatesExhausted;
    if (stats.status != master::REBALANCE_TASK_EXPIRED && stats.failedObjects == 0
        && (targetReached || partialCompleted)) {
        stats.status = master::REBALANCE_TASK_SUCCEEDED;
        stats.failureSide = master::REBALANCE_FAILURE_UNKNOWN;
        stats.failedReason.clear();
    }
}

void RebalanceExecutor::LogBatchResult(const master::RebalanceTaskPb &task, const ExecutionStats &stats,
                                       uint64_t costMs)
{
    LOG(INFO) << FormatString(
        "Finish rebalance task %s, status: %d, maxBytes: %llu, migratedBytes: %llu, migratedObjects: %llu, "
        "failedObjects: %llu, skippedObjects: %llu, costMs: %llu, reason: %s",
        task.task_id(), static_cast<int>(stats.status),
        static_cast<unsigned long long>(task.max_bytes()),
        static_cast<unsigned long long>(stats.migratedBytes),
        static_cast<unsigned long long>(stats.migratedObjects),
        static_cast<unsigned long long>(stats.failedObjects),
        static_cast<unsigned long long>(stats.skippedObjects),
        static_cast<unsigned long long>(costMs), stats.failedReason);
}

void RebalanceExecutor::Execute(master::RebalanceTaskPb task, std::string assignedMasterAddress)
{
    master::RebalanceTaskPb currentTask = task;
    bool hasMoreBatches = true;
    while (hasMoreBatches) {
        if (IsExitRequested()) {
            break;
        }
        ExecutionStats stats;
        stats.assignedMasterAddress = assignedMasterAddress;
        Timer timer;
        if (stats.assignedMasterAddress.empty()) {
            stats.failureSide = master::REBALANCE_FAILURE_CONTROL_PLANE;
            stats.failedReason = "Assigned cluster master address is empty";
        }
        LOG(INFO) << "Start rebalance task " << currentTask.task_id() << ", master: " << stats.assignedMasterAddress;
        HostPort targetAddr;
        auto localDeadlineMs = BuildLocalDeadlineMs(currentTask);
        auto rc = stats.failedReason.empty()
                      ? ValidateTask(currentTask, targetAddr, localDeadlineMs, stats.failureSide)
                      : Status(K_INVALID, stats.failedReason);
        if (rc.IsError()) {
            if (stats.failedReason.empty()) {
                stats.failedReason = rc.ToString();
            }
            if (IsExpired(localDeadlineMs)) {
                stats.status = master::REBALANCE_TASK_EXPIRED;
            }
        } else {
            ExecuteBatches(currentTask, targetAddr, stats, localDeadlineMs);
        }
        bool masterUnavailable = IsAssignedMasterUnavailable(currentTask, stats);
        ClassifyBatchResult(currentTask, masterUnavailable, stats);
        LogBatchResult(currentTask, stats, timer.ElapsedMilliSecond());
        hasMoreBatches = ReportTaskResultAndAdvance(stats, masterUnavailable, currentTask);
    }
    MarkTaskDone();
}

bool RebalanceExecutor::ReportTaskResultAndAdvance(const ExecutionStats &stats, bool masterUnavailable,
                                                   master::RebalanceTaskPb &currentTask)
{
    TerminalResult result{ currentTask, stats.status, stats.migratedBytes, stats.migratedObjects,
                           stats.failedObjects, stats.failureSide, stats.failedReason, stats.targetRemainBytes };
    CacheTerminalResult(result);
    master::ReportRebalanceResultRspPb rsp;
    (void)ReportResult(result, rsp);
    // Only a successful batch can yield a next task; a dead master or any failure stops the loop.
    if (masterUnavailable || stats.status != master::REBALANCE_TASK_SUCCEEDED
        || !rsp.has_next_rebalance_task() || rsp.next_rebalance_task().task_id().empty()) {
        return false;
    }
    auto nextTask = rsp.next_rebalance_task();
    {
        // Only the shared id requires synchronization. Publish the successor id before its execution can start.
        std::lock_guard<std::mutex> lock(taskMutex_);
        runningTaskId_ = nextTask.task_id();
    }
    currentTask = std::move(nextTask);
    return true;
}

uint64_t RebalanceExecutor::CalculateMigratedBytes(const std::unordered_map<std::string, uint64_t> &candidates,
                                                   const MigrateResult &result) const
{
    uint64_t migratedBytes = 0;
    for (const auto &objectKey : result.successIds) {
        auto it = candidates.find(objectKey);
        if (it != candidates.end()) {
            migratedBytes += it->second;
        }
    }
    return migratedBytes;
}

Status RebalanceExecutor::GetWorkerMasterApi(std::shared_ptr<WorkerMasterOCApi> &workerMasterApi) const
{
    CHECK_FAIL_RETURN_STATUS(apiManager_ != nullptr, K_RUNTIME_ERROR,
                             "Rebalance executor is not initialized");
    return apiManager_->GetWorkerMasterApi(RESOURCE_MONITOR_MASTER, workerMasterApi);
}

master::RebalanceFailureSidePb RebalanceExecutor::ClassifyMigrationFailure(const MigrateResult &result,
                                                                           const HostPort &targetAddr) const
{
    if (result.ubFailureDetail.has_value()) {
        HostPort operatorWorker;
        if (operatorWorker.ParseString(result.ubFailureDetail->operator_worker()).IsOk()) {
            if (operatorWorker == localAddress_) {
                return master::REBALANCE_FAILURE_SOURCE;
            }
            if (operatorWorker == targetAddr) {
                return master::REBALANCE_FAILURE_TARGET;
            }
        }
    }
    if (ubAdmission_ != nullptr) {
        if (ubAdmission_->CheckWriteTarget(localAddress_, UbOperationKind::MIGRATION_WRITE).IsError()) {
            return master::REBALANCE_FAILURE_SOURCE;
        }
        if (ubAdmission_->CheckWriteTarget(targetAddr, UbOperationKind::MIGRATION_WRITE).IsError()) {
            return master::REBALANCE_FAILURE_TARGET;
        }
    }
    if (result.status.GetCode() == K_OUT_OF_MEMORY || result.status.GetCode() == K_NO_SPACE) {
        return master::REBALANCE_FAILURE_TARGET;
    }
    return master::REBALANCE_FAILURE_UNKNOWN;
}

void RebalanceExecutor::ReportFailure(const master::RebalanceTaskPb &task,
                                      master::RebalanceFailureSidePb failureSide, const std::string &reason)
{
    TerminalResult result{ task, master::REBALANCE_TASK_FAILED, 0, 0, 0, failureSide, reason };
    master::ReportRebalanceResultRspPb rsp;
    (void)ReportResult(result, rsp);
}

Status RebalanceExecutor::ReportResult(const TerminalResult &result, master::ReportRebalanceResultRspPb &rspOut)
{
    master::ReportRebalanceResultReqPb req;
    req.set_task_id(result.task.task_id());
    req.set_source_worker(localAddress_.ToString());
    req.set_target_worker(result.task.target_worker());
    req.set_status(result.status);
    req.set_migrated_bytes(result.migratedBytes);
    req.set_migrated_objects(result.migratedObjects);
    req.set_failed_objects(result.failedObjects);
    req.set_failed_reason(result.failedReason);
    req.set_failure_side(result.failureSide);
    // Fresh per-batch feedback for master's next-batch decision. target_remain_bytes uses
    // UINT64_MAX as the "no batch sent" sentinel.
    req.set_target_remain_bytes(result.targetRemainBytes);
#ifdef WITH_TESTS
    if (reportHook_ != nullptr) {
        return reportHook_(req, rspOut);
    }
#endif

    for (int i = 0; i < REPORT_RESULT_RETRY_TIMES; ++i) {
        if (IsExitRequested()) {
            break;
        }
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
        auto rc = GetWorkerMasterApi(workerMasterApi);
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("Get worker master api failed, taskId: %s, retry: %d, rc: %s",
                                         result.task.task_id(), i, rc.ToString());
            std::this_thread::sleep_for(std::chrono::milliseconds(REPORT_RESULT_RETRY_INTERVAL_MS));
            continue;
        }

        rc = workerMasterApi->ReportRebalanceResult(req, rspOut);
        if (rc.IsOk()) {
            return Status::OK();
        }
        LOG(WARNING) << FormatString("Report rebalance result failed, taskId: %s, retry: %d, rc: %s",
                                     result.task.task_id(),
                                     i, rc.ToString());
        rspOut.Clear();  // a failed attempt may have left partial data; start clean next try
        std::this_thread::sleep_for(std::chrono::milliseconds(REPORT_RESULT_RETRY_INTERVAL_MS));
    }
    LOG(ERROR) << FormatString(
        "Report rebalance result ultimately failed after %d retries, taskId: %s. "
        "The task will expire at deadline on master side.",
        REPORT_RESULT_RETRY_TIMES, result.task.task_id());
    RETURN_STATUS(K_RPC_UNAVAILABLE, "Report rebalance result failed after all retries");
}

void RebalanceExecutor::SubmitTerminalResult(std::shared_ptr<const TerminalResult> result)
{
    const std::string taskId = result->task.task_id();
    try {
        executorPool_.Execute([this, result = std::move(result)]() {
            master::ReportRebalanceResultRspPb rsp;
            (void)ReportResult(*result, rsp);
        });
    } catch (const std::exception &e) {
        LOG(ERROR) << "Submit terminal rebalance result " << taskId << " failed: " << e.what();
    } catch (...) {
        LOG(ERROR) << "Submit terminal rebalance result " << taskId << " failed by unknown exception";
    }
}

void RebalanceExecutor::CacheTerminalResultAndMarkDone(TerminalResult result)
{
    CacheTerminalResult(std::move(result));
    MarkTaskDone();
}

void RebalanceExecutor::CacheTerminalResult(TerminalResult result)
{
    auto terminalResult = std::make_shared<const TerminalResult>(std::move(result));
    std::lock_guard<std::mutex> lock(taskMutex_);
    terminalResult_ = std::move(terminalResult);
}

void RebalanceExecutor::MarkTaskDone()
{
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        running_ = false;
        runningTaskId_.clear();
    }
}

}  // namespace worker
}  // namespace datasystem
