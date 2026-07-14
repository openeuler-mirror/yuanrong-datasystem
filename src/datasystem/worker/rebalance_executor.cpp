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

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/math_util.h"
#include "datasystem/common/util/raii.h"
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
      topologyEngine_(config.topologyEngine),
      akSkManager_(std::move(config.akSkManager)),
      objectTable_(std::move(config.objectTable)),
      evictionManager_(std::move(config.evictionManager)),
      apiManager_(std::move(config.apiManager)),
      candidateProvider_(evictionManager_, objectTable_),
      executorPool_(0, 1, "RebalanceExecutor")
{
}

void RebalanceExecutor::Submit(const master::RebalanceTaskPb &task)
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
    try {
        executorPool_.Execute([this, task]() {
            try {
                Execute(task);
            } catch (const std::exception &e) {
                LOG(ERROR) << "Execute rebalance task " << task.task_id() << " failed by exception: " << e.what();
                ReportResult(task, master::REBALANCE_TASK_FAILED, 0, 0, 0, e.what());
                MarkTaskDone();
            } catch (...) {
                LOG(ERROR) << "Execute rebalance task " << task.task_id() << " failed by unknown exception";
                ReportResult(task, master::REBALANCE_TASK_FAILED, 0, 0, 0, "unknown exception");
                MarkTaskDone();
            }
        });
    } catch (const std::exception &e) {
        LOG(ERROR) << "Submit rebalance task " << task.task_id() << " failed: " << e.what();
        MarkTaskDone();
        ReportResult(task, master::REBALANCE_TASK_FAILED, 0, 0, 0, e.what());
    }
}

void RebalanceExecutor::SubmitBusyResult(const master::RebalanceTaskPb &task, const std::string &runningTaskId)
{
    LOG(WARNING) << FormatString("Reject rebalance task %s because task %s is still running", task.task_id(),
                                 runningTaskId);
    try {
        executorPool_.Execute([this, task]() {
            ReportResult(task, master::REBALANCE_TASK_FAILED, 0, 0, 0, "source worker is busy");
        });
    } catch (const std::exception &e) {
        LOG(ERROR) << "Submit busy rebalance result " << task.task_id() << " failed: " << e.what();
    } catch (...) {
        LOG(ERROR) << "Submit busy rebalance result " << task.task_id() << " failed by unknown exception";
    }
}

Status RebalanceExecutor::ValidateTask(const master::RebalanceTaskPb &task, HostPort &targetAddr,
                                       uint64_t localDeadlineMs) const
{
    CHECK_FAIL_RETURN_STATUS(task.source_worker() == localAddress_.ToString(), K_INVALID,
                             FormatString("Task source %s is not local worker %s", task.source_worker(),
                                          localAddress_.ToString()));
    CHECK_FAIL_RETURN_STATUS(!task.target_worker().empty(), K_INVALID, "Rebalance target worker is empty");
    CHECK_FAIL_RETURN_STATUS(task.max_bytes() > 0, K_INVALID, "Rebalance max bytes is zero");
    CHECK_FAIL_RETURN_STATUS(!IsExpired(localDeadlineMs), K_RUNTIME_ERROR, "Rebalance task is expired");
    RETURN_IF_NOT_OK(targetAddr.ParseString(task.target_worker()));
    CHECK_FAIL_RETURN_STATUS(targetAddr != localAddress_, K_INVALID,
                             FormatString("Rebalance target %s can not be local worker", task.target_worker()));
    return Status::OK();
}

Status RebalanceExecutor::SelectCandidates(uint64_t maxBytes, std::unordered_map<std::string, uint64_t> &candidates)
{
#ifdef WITH_TESTS
    if (selectHook_ != nullptr) {
        return selectHook_(maxBytes, candidates);
    }
#endif
    // Apply a local batch cap so a large master task does not reserve too many eviction-list objects at once.
    const auto batchBytes = std::min(maxBytes, REBALANCE_BATCH_MAX_BYTES);
    RETURN_IF_NOT_OK(candidateProvider_.Select(batchBytes, REBALANCE_BATCH_MAX_OBJECTS, candidates));
    CHECK_FAIL_RETURN_STATUS(!candidates.empty(), K_NOT_FOUND, "No object can be selected for rebalance");
    return Status::OK();
}

RebalanceExecutor::MigrateResult RebalanceExecutor::MigrateToTarget(const master::RebalanceTaskPb &task,
                                                                    const HostPort &targetAddr,
                                                                    const std::vector<std::string> &objectKeys,
                                                                    object_cache::DataMigrator &migrator)
{
#ifdef WITH_TESTS
    if (migrateHook_ != nullptr) {
        return migrateHook_(task, targetAddr, objectKeys);
    }
#else
    (void)task;
#endif
    return migrator.MigrateToTargetNode(objectKeys, targetAddr, nullptr, false, 0, false).get();
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

Status RebalanceExecutor::ExecuteBatch(const master::RebalanceTaskPb &task, const HostPort &targetAddr,
                                       ExecutionStats &stats, object_cache::DataMigrator &migrator)
{
    std::unordered_map<std::string, uint64_t> candidates;
    RETURN_IF_NOT_OK(SelectCandidates(task.max_bytes() - stats.migratedBytes, candidates));
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
    stats.failedObjects += result.failedIds.size() + result.skipIds.size();
    if (result.status.IsError()) {
        stats.failedReason = result.status.ToString();
        return result.status;
    }
    if (!result.failedIds.empty() || !result.skipIds.empty()) {
        stats.failedReason = "some objects failed or skipped";
        RETURN_STATUS(K_RUNTIME_ERROR, stats.failedReason);
    }
    if (batchMigratedBytes == 0) {
        stats.failedReason = "No object migrated in this batch";
        RETURN_STATUS(K_RUNTIME_ERROR, stats.failedReason);
    }
    return Status::OK();
}

void RebalanceExecutor::ExecuteBatches(const master::RebalanceTaskPb &task, const HostPort &targetAddr,
                                       ExecutionStats &stats, uint64_t localDeadlineMs)
{
    // max_bytes is the target amount assigned by master; split it into bounded local batches to avoid reserving
    // too many objects at once.
    std::unique_ptr<object_cache::DataMigrator> migrator;
    while (stats.migratedBytes < task.max_bytes()) {
        if (IsExpired(localDeadlineMs)) {
            stats.status = master::REBALANCE_TASK_EXPIRED;
            stats.failedReason = "Rebalance task is expired";
            break;
        }
        if (migrator == nullptr) {
            migrator = std::make_unique<object_cache::DataMigrator>(
                MigrateType::SPILL, topologyEngine_, localAddress_, akSkManager_, objectTable_, task.task_id(), 0);
            migrator->Init();
        }
        auto rc = ExecuteBatch(task, targetAddr, stats, *migrator);
        if (rc.IsError()) {
            stats.candidatesExhausted = rc.GetCode() == K_NOT_FOUND;
            if (stats.failedReason.empty()) {
                stats.failedReason =
                    stats.migratedBytes == 0 ? rc.ToString() : "No more object can be selected for rebalance";
            }
            break;
        }
    }
}

void RebalanceExecutor::Execute(master::RebalanceTaskPb task)
{
    ExecutionStats stats;
    Timer timer;
    do {
        HostPort targetAddr;
        auto localDeadlineMs = BuildLocalDeadlineMs(task);
        auto rc = ValidateTask(task, targetAddr, localDeadlineMs);
        if (rc.IsError()) {
            stats.failedReason = rc.ToString();
            if (IsExpired(localDeadlineMs)) {
                stats.status = master::REBALANCE_TASK_EXPIRED;
            }
            break;
        }

        ExecuteBatches(task, targetAddr, stats, localDeadlineMs);
        bool targetReached = stats.migratedBytes >= task.max_bytes();
        bool partialCompleted = stats.migratedBytes > 0 && stats.candidatesExhausted;
        if (stats.status != master::REBALANCE_TASK_EXPIRED && stats.failedObjects == 0
            && (targetReached || partialCompleted)) {
            stats.status = master::REBALANCE_TASK_SUCCEEDED;
            stats.failedReason.clear();
        }
    } while (false);

    LOG(INFO) << FormatString(
        "Finish rebalance task %s, status: %d, migratedBytes: %llu, migratedObjects: %llu, failedObjects: %llu, "
        "costMs: %llu, reason: %s",
        task.task_id(), static_cast<int>(stats.status), static_cast<unsigned long long>(stats.migratedBytes),
        static_cast<unsigned long long>(stats.migratedObjects), static_cast<unsigned long long>(stats.failedObjects),
        static_cast<unsigned long long>(timer.ElapsedMilliSecond()), stats.failedReason);
    ReportResult(task, stats.status, stats.migratedBytes, stats.migratedObjects, stats.failedObjects,
                 stats.failedReason);
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
    CHECK_FAIL_RETURN_STATUS(topologyEngine_ != nullptr && apiManager_ != nullptr, K_RUNTIME_ERROR,
                             "Rebalance executor is not initialized");
    HostPort masterAddr;
    RETURN_IF_NOT_OK(worker::ResolveTopologyOwner(topologyEngine_, RESOURCE_MONITOR_MASTER, masterAddr));
    workerMasterApi = apiManager_->GetWorkerMasterApi(masterAddr);
    RETURN_RUNTIME_ERROR_IF_NULL(workerMasterApi);
    return Status::OK();
}

void RebalanceExecutor::ReportResult(const master::RebalanceTaskPb &task, master::RebalanceTaskStatusPb status,
                                     uint64_t migratedBytes, uint64_t migratedObjects, uint64_t failedObjects,
                                     const std::string &failedReason)
{
#ifdef WITH_TESTS
    if (reportHook_ != nullptr) {
        reportHook_(task, status, migratedBytes, migratedObjects, failedObjects, failedReason);
        return;
    }
#endif
    master::ReportRebalanceResultReqPb req;
    master::ReportRebalanceResultRspPb rsp;
    req.set_task_id(task.task_id());
    req.set_source_worker(localAddress_.ToString());
    req.set_target_worker(task.target_worker());
    req.set_status(status);
    req.set_migrated_bytes(migratedBytes);
    req.set_migrated_objects(migratedObjects);
    req.set_failed_objects(failedObjects);
    req.set_failed_reason(failedReason);

    for (int i = 0; i < REPORT_RESULT_RETRY_TIMES; ++i) {
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
        auto rc = GetWorkerMasterApi(workerMasterApi);
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("Get worker master api failed, taskId: %s, retry: %d, rc: %s",
                                         task.task_id(), i, rc.ToString());
            std::this_thread::sleep_for(std::chrono::milliseconds(REPORT_RESULT_RETRY_INTERVAL_MS));
            continue;
        }

        rc = workerMasterApi->ReportRebalanceResult(req, rsp);
        if (rc.IsOk()) {
            return;
        }
        LOG(WARNING) << FormatString("Report rebalance result failed, taskId: %s, retry: %d, rc: %s", task.task_id(),
                                     i, rc.ToString());
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
