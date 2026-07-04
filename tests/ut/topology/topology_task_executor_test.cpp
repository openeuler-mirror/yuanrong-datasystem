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
 * Description: Topology task executor unit tests.
 */
#include "datasystem/topology/executor/topology_task_executor.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/util/status_helper.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/testing/fake_topology_repository.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char NODE_A[] = "node-a";
constexpr char NODE_B[] = "node-b";
constexpr char TRANSFER_TASK_ID[] = "migrate-v1-to-v2-node-a-node-b-0";
constexpr char RECOVERY_TASK_ID[] = "delete-v1-to-v2-node-b-node-a-0";
constexpr char MISSING_TASK_ID[] = "missing-topology-task";
constexpr uint32_t RANGE_BEGIN = 10;
constexpr uint32_t RANGE_END = 20;

TransferTaskRecord MakeTransferTask()
{
    TransferTaskRecord task;
    task.taskId = TRANSFER_TASK_ID;
    task.executorNodeId = NODE_A;
    task.sourceNodeId = NODE_A;
    task.targetNodeId = NODE_B;
    task.createdTopologyVersion = 1;
    task.targetTopologyVersion = 2;
    task.ranges = { TokenRange{ RANGE_BEGIN, RANGE_END, NODE_A, false } };
    return task;
}

RecoveryTaskRecord MakeRecoveryTask()
{
    RecoveryTaskRecord task;
    task.taskId = RECOVERY_TASK_ID;
    task.executorNodeId = NODE_A;
    task.failedNodeId = NODE_B;
    task.recoveryNodeId = NODE_A;
    task.createdTopologyVersion = 1;
    task.targetTopologyVersion = 2;
    task.ranges = { TokenRange{ RANGE_BEGIN, RANGE_END, NODE_A, false } };
    return task;
}

TaskProgressUpdate MakeProgress(const TaskId &taskId, const TokenRange &range)
{
    TaskProgressUpdate update;
    update.taskId = taskId;
    update.nodeId = range.nodeId;
    update.range = range;
    update.range.finished = true;
    return update;
}

class TestTaskCallbacks final : public ITopologyPhaseCallbacks {
public:
    Status ExecuteTransferTask(const TransferTaskRecord &task,
                               std::vector<TaskProgressUpdate> &progress) override
    {
        ++transferCalls;
        RETURN_IF_NOT_OK(MaybeFail());
        progress.clear();
        for (const auto &range : task.ranges) {
            progress.emplace_back(MakeProgress(task.taskId, range));
        }
        return Status::OK();
    }

    Status ExecuteRecoveryTask(const RecoveryTaskRecord &task,
                               std::vector<TaskProgressUpdate> &progress) override
    {
        ++recoveryCalls;
        RETURN_IF_NOT_OK(MaybeFail());
        progress.clear();
        for (const auto &range : task.ranges) {
            progress.emplace_back(MakeProgress(task.taskId, range));
        }
        return Status::OK();
    }

    Status MaybeFail()
    {
        if (retryFailuresBeforeSuccess > 0) {
            --retryFailuresBeforeSuccess;
            RETURN_STATUS(K_TRY_AGAIN, "callback retry");
        }
        if (blockedFailure) {
            RETURN_STATUS(K_RUNTIME_ERROR, "callback blocked");
        }
        return Status::OK();
    }

    uint32_t retryFailuresBeforeSuccess{ 0 };
    bool blockedFailure{ false };
    uint64_t transferCalls{ 0 };
    uint64_t recoveryCalls{ 0 };
};

TaskNotify MakeNotify(TaskNotifyType type, std::vector<TaskId> taskIds)
{
    TaskNotify notify;
    notify.nodeAddress = NODE_A;
    notify.type = type;
    notify.taskIds = std::move(taskIds);
    return notify;
}

}  // namespace

TEST(TopologyTaskExecutorTest, DuplicateNotifyExecutesTaskOnce)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedTransferTask(MakeTransferTask()));
    TestTaskCallbacks callbacks;
    TopologyTaskExecutor executor(repository, callbacks);

    DS_ASSERT_OK(executor.SubmitNotify(MakeNotify(TaskNotifyType::SCALE_OUT, { TRANSFER_TASK_ID, TRANSFER_TASK_ID })));
    DS_ASSERT_OK(executor.Drain());

    auto stats = executor.GetStats();
    EXPECT_EQ(callbacks.transferCalls, 1);
    EXPECT_EQ(stats.duplicateNotifies, 1);
    EXPECT_EQ(stats.completedTasks, 1);
    TopologyTaskSummary summary;
    DS_ASSERT_OK(repository.GetTaskSummary(summary));
    EXPECT_EQ(summary.unfinishedTransferTasks, 0);
}

TEST(TopologyTaskExecutorTest, QueueFullReturnsTryAgainWithoutUnboundedGrowth)
{
    FakeTopologyRepository repository;
    TestTaskCallbacks callbacks;
    TopologyTaskExecutorOptions options;
    options.maxQueuedTasks = 1;
    TopologyTaskExecutor executor(repository, callbacks, options);

    auto rc = executor.SubmitNotify(MakeNotify(TaskNotifyType::SCALE_OUT, { "task-1", "task-2" }));
    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(executor.GetStats().queuedTasks, 0);
}

TEST(TopologyTaskExecutorTest, RetryableCallbackFailureIsBoundedAndCanEventuallySucceed)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedTransferTask(MakeTransferTask()));
    TestTaskCallbacks callbacks;
    callbacks.retryFailuresBeforeSuccess = 2;
    TopologyTaskExecutorOptions options;
    options.maxRetryAttempts = 2;
    TopologyTaskExecutor executor(repository, callbacks, options);

    DS_ASSERT_OK(executor.SubmitNotify(MakeNotify(TaskNotifyType::SCALE_OUT, { TRANSFER_TASK_ID })));
    DS_ASSERT_OK(executor.Drain());

    auto stats = executor.GetStats();
    EXPECT_EQ(callbacks.transferCalls, 3);
    EXPECT_EQ(stats.retryableFailures, 2);
    EXPECT_EQ(stats.completedTasks, 1);
}

TEST(TopologyTaskExecutorTest, RetryExhaustionReturnsTryAgain)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedTransferTask(MakeTransferTask()));
    TestTaskCallbacks callbacks;
    callbacks.retryFailuresBeforeSuccess = 2;
    TopologyTaskExecutorOptions options;
    options.maxRetryAttempts = 1;
    TopologyTaskExecutor executor(repository, callbacks, options);

    DS_ASSERT_OK(executor.SubmitNotify(MakeNotify(TaskNotifyType::SCALE_OUT, { TRANSFER_TASK_ID })));
    EXPECT_EQ(executor.Drain().GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(callbacks.transferCalls, 2);
}

TEST(TopologyTaskExecutorTest, ProgressCasConflictDoesNotReplayCallback)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedTransferTask(MakeTransferTask()));
    repository.InjectTransferProgressConflict();
    TestTaskCallbacks callbacks;
    TopologyTaskExecutor executor(repository, callbacks);

    DS_ASSERT_OK(executor.SubmitNotify(MakeNotify(TaskNotifyType::SCALE_OUT, { TRANSFER_TASK_ID })));
    DS_ASSERT_OK(executor.Drain());

    EXPECT_EQ(callbacks.transferCalls, 1);
    TopologyTaskSummary summary;
    DS_ASSERT_OK(repository.GetTaskSummary(summary));
    EXPECT_EQ(summary.unfinishedTransferTasks, 0);
}

TEST(TopologyTaskExecutorTest, RescanUnfinishedRecoveryTaskExecutesFromRepository)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedRecoveryTask(MakeRecoveryTask()));
    TestTaskCallbacks callbacks;
    TopologyTaskExecutor executor(repository, callbacks);

    DS_ASSERT_OK(executor.RescanUnfinished(NODE_A));

    EXPECT_EQ(callbacks.recoveryCalls, 1);
    TopologyTaskSummary summary;
    DS_ASSERT_OK(repository.GetTaskSummary(summary));
    EXPECT_EQ(summary.unfinishedRecoveryTasks, 0);
}

TEST(TopologyTaskExecutorTest, NonRetryableCallbackFailureIsDiagnosedAsBlocked)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedTransferTask(MakeTransferTask()));
    TestTaskCallbacks callbacks;
    callbacks.blockedFailure = true;
    TopologyTaskExecutor executor(repository, callbacks);

    DS_ASSERT_OK(executor.SubmitNotify(MakeNotify(TaskNotifyType::SCALE_OUT, { TRANSFER_TASK_ID })));
    EXPECT_EQ(executor.Drain().GetCode(), K_RUNTIME_ERROR);

    auto stats = executor.GetStats();
    EXPECT_EQ(stats.blockedFailures, 1);
    EXPECT_EQ(stats.completedTasks, 0);
}

TEST(TopologyTaskExecutorTest, MissingTaskStatusCarriesTaskId)
{
    FakeTopologyRepository repository;
    TestTaskCallbacks callbacks;
    TopologyTaskExecutor executor(repository, callbacks);

    DS_ASSERT_OK(executor.SubmitNotify(MakeNotify(TaskNotifyType::SCALE_OUT, { MISSING_TASK_ID })));
    auto rc = executor.Drain();
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);

    auto stats = executor.GetStats();
    EXPECT_NE(stats.lastStatus.ToString().find(MISSING_TASK_ID), std::string::npos);
    EXPECT_EQ(stats.blockedFailures, 1);
}

}  // namespace topology
}  // namespace datasystem
