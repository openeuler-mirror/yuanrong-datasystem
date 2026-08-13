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
      candidateProvider_(evictionManager_, objectTable_),
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
    std::string runningTaskId;
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        if (running_) {
            duplicate = runningTaskId_ == task.task_id();
            busy = !duplicate;
            runningTaskId = runningTaskId_;
        } else {
            running_ = true;
            runningTaskId_ = task.task_id();
        }
    }
    if (duplicate) {
        LOG(INFO) << FormatString("Ignore duplicated rebalance task %s because it is already running", task.task_id());
        return;
    }
    if (busy) {
        SubmitBusyResult(task, runningTaskId);
        return;
    }

    // Do not block the resource-report thread; run data migration in the single-task executor.
    auto traceId = Trace::Instance().GetTraceID();
    try {
        executorPool_.Execute([this, task, assignedMasterAddress = std::move(assignedMasterAddress), traceId]() {
            SetRequestContext(nullptr);
            ScopedRequestContext ctx(traceId);
            try {
                Execute(task, assignedMasterAddress);
            } catch (const std::exception &e) {
                LOG(ERROR) << "Execute rebalance task " << task.task_id() << " failed by exception: " << e.what();
                ReportFailure(task, master::REBALANCE_FAILURE_SOURCE, e.what());
                MarkTaskDone();
            } catch (...) {
                LOG(ERROR) << "Execute rebalance task " << task.task_id() << " failed by unknown exception";
                ReportFailure(task, master::REBALANCE_FAILURE_SOURCE, "unknown exception");
                MarkTaskDone();
            }
        });
    } catch (const std::exception &e) {
        LOG(ERROR) << "Submit rebalance task " << task.task_id() << " failed: " << e.what();
        MarkTaskDone();
        ReportFailure(task, master::REBALANCE_FAILURE_SOURCE, e.what());
    }
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
            ReportFailure(task, master::REBALANCE_FAILURE_SOURCE, "source worker is busy");
        });
    } catch (const std::exception &e) {
        LOG(ERROR) << "Submit busy rebalance result " << task.task_id() << " failed: " << e.what();
    } catch (...) {
        LOG(ERROR) << "Submit busy rebalance result " << task.task_id() << " failed by unknown exception";
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
    auto rc = targetAddr.ParseString(task.target_worker());
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

Status RebalanceExecutor::SelectCandidates(uint64_t maxBytes, std::unordered_map<std::string, uint64_t> &candidates,
                                           const std::unordered_set<std::string> &skipKeys)
{
#ifdef WITH_TESTS
    if (selectHook_ != nullptr) {
        return selectHook_(maxBytes, candidates, skipKeys);
    }
#endif
    // Apply a local batch cap so a large master task does not reserve too many eviction-list objects at once.
    const auto batchBytes = std::min(maxBytes, REBALANCE_BATCH_MAX_BYTES);
    RETURN_IF_NOT_OK(candidateProvider_.Select(batchBytes, REBALANCE_BATCH_MAX_OBJECTS, candidates, &skipKeys));
    CHECK_FAIL_RETURN_STATUS(!candidates.empty(), K_NOT_FOUND, "No object can be selected for rebalance");
    return Status::OK();
}

RebalanceExecutor::MigrateResult RebalanceExecutor::MigrateToTarget(const master::RebalanceTaskPb &task,
                                                                    const HostPort &targetAddr,
                                                                    const std::vector<std::string> &objectKeys,
                                                                    object_cache::DataMigrator &migrator)
{
    (void)task;
#ifdef WITH_TESTS
    if (migrateHook_ != nullptr) {
        return migrateHook_(task, targetAddr, objectKeys);
    }
#else
    (void)task;
#endif
    object_cache::DataMigrator::TargetMigrationOptions options{ .isSlotMigration = false };
    return migrator.MigrateToTargetNode(objectKeys, targetAddr, nullptr, options).get();
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

Status RebalanceExecutor::ExecuteBatch(const master::RebalanceTaskPb &task, const HostPort &targetAddr,
                                       ExecutionStats &stats, object_cache::DataMigrator &migrator,
                                       std::unordered_set<std::string> &taskSkippedKeys)
{
    auto rc = CheckTargetAdmission(targetAddr);
    if (rc.IsError()) {
        stats.failureSide = master::REBALANCE_FAILURE_TARGET;
        return rc;
    }
    std::unordered_map<std::string, uint64_t> candidates;
    rc = SelectCandidates(task.max_bytes() - stats.migratedBytes, candidates, taskSkippedKeys);
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
    // the target worker through ReplacePrimary.
    auto result = MigrateToTarget(task, targetAddr, objectKeys, migrator);
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
    while (stats.migratedBytes < task.max_bytes()) {
        if (IsExpired(localDeadlineMs) || IsExitRequested()) {
            stats.status = master::REBALANCE_TASK_EXPIRED;
            stats.failureSide = master::REBALANCE_FAILURE_CONTROL_PLANE;
            stats.failedReason = "Rebalance task is expired";
            break;
        }
        if (IsAssignedMasterUnavailable(task, stats)) {
            break;
        }
        if (migrator == nullptr) {
            if (metadataRoute_ == nullptr || membership_ == nullptr || endpointPolicy_ == nullptr
                || exitRequested_ == nullptr) {
                stats.failureSide = master::REBALANCE_FAILURE_SOURCE;
                stats.failedReason = "Rebalance topology dependencies are not initialized";
                break;
            }
            migrator = std::make_unique<object_cache::DataMigrator>(
                MigrateType::SPILL, *metadataRoute_, *membership_, *endpointPolicy_, exitRequested_, localAddress_,
                akSkManager_, objectTable_, task.task_id(), 0);
            migrator->SetUbAdmission(ubAdmission_);
            migrator->Init();
        }
        auto rc = ExecuteBatch(task, targetAddr, stats, *migrator, taskSkippedKeys);
        if (rc.IsError()) {
            // When the entire batch was skipped (metadata-not-found) but the task had partial success,
            // retry instead of breaking: SelectCandidates now has taskSkippedKeys and will scan past
            // the skipped objects to find valid candidates behind them. This prevents a large
            // meta-not-found object at the eviction-list head from starving subsequent batches.
            // Only retry when there was partial success (migratedBytes > 0); if the first batch was
            // all-skip, breaking is the right behavior since all candidates had no metadata.
            if (rc.GetCode() == K_NOT_FOUND && stats.lastBatchAllSkipped
                && stats.migratedBytes > 0 && stats.migratedBytes < task.max_bytes()) {
                continue;
            }
            stats.candidatesExhausted = rc.GetCode() == K_NOT_FOUND;
            if (stats.failedReason.empty()) {
                stats.failedReason =
                    stats.migratedBytes == 0 ? rc.ToString() : "No more object can be selected for rebalance";
            }
            break;
        }
    }
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

void RebalanceExecutor::ReportEmptyMasterAndDone(const master::RebalanceTaskPb &task)
{
    ExecutionStats stats;
    stats.failureSide = master::REBALANCE_FAILURE_CONTROL_PLANE;
    stats.failedReason = "Assigned cluster master address is empty";
    master::ReportRebalanceResultRspPb rsp;
    ReportResult(task, stats, rsp);
    MarkTaskDone();
}

void RebalanceExecutor::Execute(master::RebalanceTaskPb task, std::string assignedMasterAddress)
{
    // Hoisted: assignedMasterAddress is a function parameter that does not change between batches.
    if (assignedMasterAddress.empty()) {
        ReportEmptyMasterAndDone(task);
        return;
    }
    master::RebalanceTaskPb currentTask = task;
    bool hasMoreBatches = true;
    while (hasMoreBatches) {
        if (IsExitRequested()) {
            break;
        }
        ExecutionStats stats;
        stats.assignedMasterAddress = assignedMasterAddress;
        Timer timer;
        LOG(INFO) << "Start rebalance task " << currentTask.task_id() << ", master: " << stats.assignedMasterAddress;
        HostPort targetAddr;
        auto localDeadlineMs = BuildLocalDeadlineMs(currentTask);
        auto rc = ValidateTask(currentTask, targetAddr, localDeadlineMs, stats.failureSide);
        if (rc.IsError()) {
            stats.failedReason = rc.ToString();
            if (IsExpired(localDeadlineMs)) {
                stats.status = master::REBALANCE_TASK_EXPIRED;
            }
            master::ReportRebalanceResultRspPb rsp;
            ReportResult(currentTask, stats, rsp);
            break;
        }
        ExecuteBatches(currentTask, targetAddr, stats, localDeadlineMs);
        bool masterUnavailable = IsAssignedMasterUnavailable(currentTask, stats);
        ClassifyBatchResult(currentTask, masterUnavailable, stats);
        LogBatchResult(currentTask, stats, timer.ElapsedMilliSecond());
        master::ReportRebalanceResultRspPb rsp;
        ReportResult(currentTask, stats, rsp);
        // Only a successful batch can yield a next task; a dead master or any failure stops the loop.
        if (masterUnavailable || stats.status != master::REBALANCE_TASK_SUCCEEDED
            || !rsp.has_next_rebalance_task() || rsp.next_rebalance_task().task_id().empty()) {
            hasMoreBatches = false;
        } else {
            // Copy proto and update runningTaskId_ under one lock so a heartbeat continuation
            // cannot observe a stale predecessor id. Safe -- Submit/MarkTaskDone hold briefly.
            std::lock_guard<std::mutex> lock(taskMutex_);
            currentTask = rsp.next_rebalance_task();
            runningTaskId_ = currentTask.task_id();
        }
    }
    MarkTaskDone();
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
    ExecutionStats stats;
    stats.failureSide = failureSide;
    stats.failedReason = reason;
    master::ReportRebalanceResultRspPb rsp;
    ReportResult(task, stats, rsp);
}

void RebalanceExecutor::ReportResult(const master::RebalanceTaskPb &task, const ExecutionStats &stats,
                                     master::ReportRebalanceResultRspPb &rspOut)
{
    master::ReportRebalanceResultReqPb req;
    req.set_task_id(task.task_id());
    req.set_source_worker(localAddress_.ToString());
    req.set_target_worker(task.target_worker());
    req.set_status(stats.status);
    req.set_migrated_bytes(stats.migratedBytes);
    req.set_migrated_objects(stats.migratedObjects);
    req.set_failed_objects(stats.failedObjects);
    req.set_failed_reason(stats.failedReason);
    req.set_failure_side(stats.failureSide);
    // Fresh per-batch feedback for master's next-batch decision. target_remain_bytes uses
    // UINT64_MAX as the "no batch sent" sentinel.
    req.set_target_remain_bytes(stats.targetRemainBytes);
#ifdef WITH_TESTS
    if (reportHook_ != nullptr) {
        reportHook_(req, rspOut);
        return;
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
                                         task.task_id(), i, rc.ToString());
            std::this_thread::sleep_for(std::chrono::milliseconds(REPORT_RESULT_RETRY_INTERVAL_MS));
            continue;
        }

        rc = workerMasterApi->ReportRebalanceResult(req, rspOut);
        if (rc.IsOk()) {
            return;
        }
        LOG(WARNING) << FormatString("Report rebalance result failed, taskId: %s, retry: %d, rc: %s", task.task_id(),
                                     i, rc.ToString());
        rspOut.Clear();  // a failed attempt may have left partial data; start clean next try
        std::this_thread::sleep_for(std::chrono::milliseconds(REPORT_RESULT_RETRY_INTERVAL_MS));
    }
    LOG(ERROR) << FormatString(
        "Report rebalance result ultimately failed after %d retries, taskId: %s. "
        "The task will expire at deadline on master side.",
        REPORT_RESULT_RETRY_TIMES, task.task_id());
}

void RebalanceExecutor::MarkTaskDone()
{
    std::lock_guard<std::mutex> lock(taskMutex_);
    running_ = false;
    runningTaskId_.clear();
}

}  // namespace worker
}  // namespace datasystem
