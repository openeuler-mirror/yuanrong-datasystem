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
#ifndef DATASYSTEM_TOPOLOGY_EXECUTOR_TOPOLOGY_TASK_EXECUTOR_H
#define DATASYSTEM_TOPOLOGY_EXECUTOR_TOPOLOGY_TASK_EXECUTOR_H

#include <deque>
#include <map>
#include <set>
#include <tuple>

#include "datasystem/topology/executor/topology_phase_callbacks.h"
#include "datasystem/topology/repository/topology_repository.h"

namespace datasystem {
namespace topology {

struct TopologyTaskExecutorOptions {
    static constexpr size_t DEFAULT_MAX_QUEUED_TASKS = 1024;
    static constexpr uint32_t DEFAULT_MAX_RETRY_ATTEMPTS = 3;

    size_t maxQueuedTasks{ DEFAULT_MAX_QUEUED_TASKS };
    uint32_t maxRetryAttempts{ DEFAULT_MAX_RETRY_ATTEMPTS };
};

struct TopologyTaskExecutorStats {
    uint64_t queuedTasks{ 0 };
    uint64_t duplicateNotifies{ 0 };
    uint64_t completedTasks{ 0 };
    uint64_t retryableFailures{ 0 };
    uint64_t blockedFailures{ 0 };
    Status lastStatus;
};

class TopologyTaskExecutor final {
public:
    TopologyTaskExecutor(ITopologyTaskExecutionRepository &repository, ITopologyPhaseCallbacks &callbacks,
                         TopologyTaskExecutorOptions options = {});
    ~TopologyTaskExecutor() = default;
    TopologyTaskExecutor(const TopologyTaskExecutor &) = delete;
    TopologyTaskExecutor &operator=(const TopologyTaskExecutor &) = delete;

    /**
     * @brief Queue task ids from one notify payload.
     * @param[in] notify Notify payload produced by C3.
     * @return K_OK on success; K_TRY_AGAIN if the bounded queue is full.
     */
    Status SubmitNotify(const TaskNotify &notify);

    /**
     * @brief Execute all currently queued task ids synchronously.
     * @return K_OK when all queued tasks complete; last task error otherwise.
     */
    Status Drain();

    /**
     * @brief Rescan unfinished tasks for one executor node and execute them.
     * @param[in] executorNodeId Executor node id.
     * @return K_OK when scan and execution complete.
     */
    Status RescanUnfinished(const TopologyNodeId &executorNodeId);

    /**
     * @brief Return executor counters for diagnostics.
     */
    TopologyTaskExecutorStats GetStats() const;

private:
    struct QueuedTask {
        TaskNotifyType type{ TaskNotifyType::SCALE_OUT };
        TaskId taskId;

        bool operator<(const QueuedTask &other) const
        {
            return std::tie(type, taskId) < std::tie(other.type, other.taskId);
        }
    };

    Status QueueTask(const QueuedTask &task);
    Status ExecuteQueuedTask(const QueuedTask &task);
    Status ExecuteTransferTask(const TaskId &taskId);
    Status ExecuteRecoveryTask(const TaskId &taskId);
    Status ReportTransferProgressWithRetry(const TaskId &taskId, const std::vector<TaskProgressUpdate> &progress);
    Status ReportRecoveryProgressWithRetry(const TaskId &taskId, const std::vector<TaskProgressUpdate> &progress);
    bool RequeueRetryableTask(const QueuedTask &task);
    void RecordTaskStatus(const Status &status);

    ITopologyTaskExecutionRepository &repository_;
    ITopologyPhaseCallbacks &callbacks_;
    TopologyTaskExecutorOptions options_;
    std::deque<QueuedTask> queue_;
    std::set<QueuedTask> queued_;
    std::map<QueuedTask, uint32_t> retryAttempts_;
    TopologyTaskExecutorStats stats_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_EXECUTOR_TOPOLOGY_TASK_EXECUTOR_H
