/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Bounded topology task executor.
 */
#include "datasystem/topology/executor/topology_task_executor.h"

#include <algorithm>
#include <set>
#include <tuple>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {

bool IsRecoveryNotify(TaskNotifyType type)
{
    return type == TaskNotifyType::PASSIVE_SCALE_IN;
}

bool IsValidTaskId(const TaskId &taskId)
{
    return !taskId.empty();
}

const char *TaskTypeName(TaskNotifyType type)
{
    return IsRecoveryNotify(type) ? "recovery" : "transfer";
}

}  // namespace

TopologyTaskExecutor::TopologyTaskExecutor(ITopologyTaskExecutionRepository &repository,
                                           ITopologyPhaseCallbacks &callbacks,
                                           TopologyTaskExecutorOptions options)
    : repository_(repository), callbacks_(callbacks), options_(options)
{
}

Status TopologyTaskExecutor::SubmitNotify(const TaskNotify &notify)
{
    CHECK_FAIL_RETURN_STATUS(!notify.nodeAddress.empty(), K_INVALID, "notify node address is empty");
    std::set<QueuedTask> newTasks;
    for (const auto &taskId : notify.taskIds) {
        CHECK_FAIL_RETURN_STATUS(IsValidTaskId(taskId), K_INVALID, "notify task id is empty");
        QueuedTask task{ notify.type, taskId };
        if (queued_.find(task) == queued_.end()) {
            newTasks.insert(task);
        }
    }
    CHECK_FAIL_RETURN_STATUS(queue_.size() + newTasks.size() <= options_.maxQueuedTasks, K_TRY_AGAIN,
                             "topology task queue is full");
    for (const auto &taskId : notify.taskIds) {
        RETURN_IF_NOT_OK(QueueTask(QueuedTask{ notify.type, taskId }));
    }
    return Status::OK();
}

Status TopologyTaskExecutor::Drain()
{
    Status finalStatus = Status::OK();
    while (!queue_.empty()) {
        auto task = queue_.front();
        queue_.pop_front();
        queued_.erase(task);
        auto rc = ExecuteQueuedTask(task);
        RecordTaskStatus(rc);
        if (rc.IsOk()) {
            retryAttempts_.erase(task);
            continue;
        }
        if (rc.GetCode() == K_TRY_AGAIN && RequeueRetryableTask(task)) {
            LOG(WARNING) << "Topology task retry scheduled, type: " << TaskTypeName(task.type)
                         << ", taskId: " << task.taskId << ", status: " << rc.ToString();
            continue;
        }
        LOG(WARNING) << "Topology task execution blocked, type: " << TaskTypeName(task.type)
                     << ", taskId: " << task.taskId << ", status: " << rc.ToString();
        if (finalStatus.IsOk()) {
            finalStatus = rc;
        }
    }
    return finalStatus;
}

Status TopologyTaskExecutor::RescanUnfinished(const TopologyNodeId &executorNodeId)
{
    TaskFilter filter;
    filter.executorNodeId = executorNodeId;
    filter.unfinishedOnly = true;
    std::vector<TransferTaskRecord> transferTasks;
    RETURN_IF_NOT_OK(repository_.ListTransferTaskRecords(filter, transferTasks));
    for (const auto &task : transferTasks) {
        RETURN_IF_NOT_OK(QueueTask(QueuedTask{ TaskNotifyType::SCALE_OUT, task.taskId }));
    }
    std::vector<RecoveryTaskRecord> recoveryTasks;
    RETURN_IF_NOT_OK(repository_.ListRecoveryTaskRecords(filter, recoveryTasks));
    for (const auto &task : recoveryTasks) {
        RETURN_IF_NOT_OK(QueueTask(QueuedTask{ TaskNotifyType::PASSIVE_SCALE_IN, task.taskId }));
    }
    return Drain();
}

TopologyTaskExecutorStats TopologyTaskExecutor::GetStats() const
{
    auto stats = stats_;
    stats.queuedTasks = queue_.size();
    return stats;
}

Status TopologyTaskExecutor::QueueTask(const QueuedTask &task)
{
    if (queued_.find(task) != queued_.end()) {
        ++stats_.duplicateNotifies;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(queue_.size() < options_.maxQueuedTasks, K_TRY_AGAIN, "topology task queue is full");
    queue_.push_back(task);
    queued_.insert(task);
    stats_.queuedTasks = queue_.size();
    return Status::OK();
}

Status TopologyTaskExecutor::ExecuteQueuedTask(const QueuedTask &task)
{
    if (IsRecoveryNotify(task.type)) {
        return ExecuteRecoveryTask(task.taskId);
    }
    return ExecuteTransferTask(task.taskId);
}

Status TopologyTaskExecutor::ExecuteTransferTask(const TaskId &taskId)
{
    TaskFilter filter;
    filter.unfinishedOnly = true;
    std::vector<TransferTaskRecord> tasks;
    RETURN_IF_NOT_OK(repository_.ListTransferTaskRecords(filter, tasks));
    auto iter = std::find_if(tasks.begin(), tasks.end(), [&taskId](const auto &task) {
        return task.taskId == taskId;
    });
    CHECK_FAIL_RETURN_STATUS(iter != tasks.end(), K_NOT_FOUND, "transfer task not found, taskId: " + taskId);
    std::vector<TaskProgressUpdate> progress;
    RETURN_IF_NOT_OK(callbacks_.ExecuteTransferTask(*iter, progress));
    if (!progress.empty()) {
        RETURN_IF_NOT_OK(ReportTransferProgressWithRetry(taskId, progress));
    }
    return Status::OK();
}

Status TopologyTaskExecutor::ExecuteRecoveryTask(const TaskId &taskId)
{
    TaskFilter filter;
    filter.unfinishedOnly = true;
    std::vector<RecoveryTaskRecord> tasks;
    RETURN_IF_NOT_OK(repository_.ListRecoveryTaskRecords(filter, tasks));
    auto iter = std::find_if(tasks.begin(), tasks.end(), [&taskId](const auto &task) {
        return task.taskId == taskId;
    });
    CHECK_FAIL_RETURN_STATUS(iter != tasks.end(), K_NOT_FOUND, "recovery task not found, taskId: " + taskId);
    std::vector<TaskProgressUpdate> progress;
    RETURN_IF_NOT_OK(callbacks_.ExecuteRecoveryTask(*iter, progress));
    if (!progress.empty()) {
        RETURN_IF_NOT_OK(ReportRecoveryProgressWithRetry(taskId, progress));
    }
    return Status::OK();
}

Status TopologyTaskExecutor::ReportTransferProgressWithRetry(const TaskId &taskId,
                                                             const std::vector<TaskProgressUpdate> &progress)
{
    Status lastStatus;
    for (uint32_t attempt = 0; attempt <= options_.maxRetryAttempts; ++attempt) {
        auto rc = repository_.ReportTransferProgressBatch(taskId, progress);
        if (rc.GetCode() != K_TRY_AGAIN) {
            return rc;
        }
        lastStatus = rc;
    }
    return lastStatus;
}

Status TopologyTaskExecutor::ReportRecoveryProgressWithRetry(const TaskId &taskId,
                                                             const std::vector<TaskProgressUpdate> &progress)
{
    Status lastStatus;
    for (uint32_t attempt = 0; attempt <= options_.maxRetryAttempts; ++attempt) {
        auto rc = repository_.ReportRecoveryProgressBatch(taskId, progress);
        if (rc.GetCode() != K_TRY_AGAIN) {
            return rc;
        }
        lastStatus = rc;
    }
    return lastStatus;
}

void TopologyTaskExecutor::RecordTaskStatus(const Status &status)
{
    stats_.lastStatus = status;
    if (status.IsOk()) {
        ++stats_.completedTasks;
    } else if (status.GetCode() == K_TRY_AGAIN) {
        ++stats_.retryableFailures;
    } else {
        ++stats_.blockedFailures;
    }
    stats_.queuedTasks = queue_.size();
}

bool TopologyTaskExecutor::RequeueRetryableTask(const QueuedTask &task)
{
    auto &attempts = retryAttempts_[task];
    if (attempts >= options_.maxRetryAttempts || queue_.size() >= options_.maxQueuedTasks) {
        LOG(WARNING) << "Topology task retry exhausted, type: " << TaskTypeName(task.type)
                     << ", taskId: " << task.taskId << ", attempts: " << attempts;
        return false;
    }
    ++attempts;
    queue_.push_back(task);
    queued_.insert(task);
    stats_.queuedTasks = queue_.size();
    return true;
}

}  // namespace topology
}  // namespace datasystem
