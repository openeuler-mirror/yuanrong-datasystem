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
 * Description: Topology control loop unit tests.
 */
#include "datasystem/topology/orchestration/topology_control_loop.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/algorithm/hash_algorithm.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/testing/fake_topology_repository.h"
#include "tests/ut/topology/testing/topology_test_utils.h"

namespace datasystem {
namespace topology {
namespace {

constexpr uint32_t NODE_A_TOKEN = 100;
constexpr uint32_t NODE_B_TOKEN = 200;
constexpr int64_t VERSION_1 = 1;
constexpr char NODE_A[] = "node-a";
constexpr char NODE_B[] = "node-b";

TopologyDescriptor MakeSingleNodeTopology()
{
    return testing::MakeTopologyForTest({ testing::MakeTopologyNodeForTest(NODE_A, { NODE_A_TOKEN }) }, VERSION_1);
}

TopologyDescriptor MakeTwoNodeTopology()
{
    return testing::MakeTopologyForTest({
        testing::MakeTopologyNodeForTest(NODE_A, { NODE_A_TOKEN }),
        testing::MakeTopologyNodeForTest(NODE_B, { NODE_B_TOKEN }),
    },
                                        VERSION_1);
}

Status CountTasks(FakeTopologyRepository &repository, uint64_t &transferTasks, uint64_t &recoveryTasks)
{
    TaskFilter filter;
    std::vector<TransferTaskRecord> transfers;
    RETURN_IF_NOT_OK(repository.ListTransferTaskRecords(filter, transfers));
    std::vector<RecoveryTaskRecord> recoveries;
    RETURN_IF_NOT_OK(repository.ListRecoveryTaskRecords(filter, recoveries));
    transferTasks = transfers.size();
    recoveryTasks = recoveries.size();
    return Status::OK();
}

const TopologyNode *FindMember(const TopologyDescriptor &topology, const TopologyNodeId &nodeId)
{
    auto iter = std::find_if(topology.members.begin(), topology.members.end(), [&nodeId](const auto &member) {
        return member.nodeId == nodeId;
    });
    return iter == topology.members.end() ? nullptr : &(*iter);
}

Status FinishAllTransferTasks(FakeTopologyRepository &repository)
{
    TaskFilter filter;
    std::vector<TransferTaskRecord> tasks;
    RETURN_IF_NOT_OK(repository.ListTransferTaskRecords(filter, tasks));
    for (const auto &task : tasks) {
        for (const auto &range : task.ranges) {
            if (range.finished) {
                continue;
            }
            TaskProgressUpdate update;
            update.taskId = task.taskId;
            update.nodeId = range.nodeId;
            update.range = range;
            update.range.finished = true;
            RETURN_IF_NOT_OK(repository.ReportTransferProgress(task.taskId, update));
        }
    }
    return Status::OK();
}

Status FinishAllRecoveryTasks(FakeTopologyRepository &repository)
{
    TaskFilter filter;
    std::vector<RecoveryTaskRecord> tasks;
    RETURN_IF_NOT_OK(repository.ListRecoveryTaskRecords(filter, tasks));
    for (const auto &task : tasks) {
        for (const auto &range : task.ranges) {
            if (range.finished) {
                continue;
            }
            TaskProgressUpdate update;
            update.taskId = task.taskId;
            update.nodeId = range.nodeId;
            update.range = range;
            update.range.finished = true;
            RETURN_IF_NOT_OK(repository.ReportRecoveryProgress(task.taskId, update));
        }
    }
    return Status::OK();
}

TransferTaskRecord MakeTransferTask()
{
    TransferTaskRecord task;
    task.taskId = "migrate-v1-to-v2-node-a-node-b-0";
    task.executorNodeId = NODE_A;
    task.sourceNodeId = NODE_A;
    task.targetNodeId = NODE_B;
    task.createdTopologyVersion = VERSION_1;
    task.targetTopologyVersion = VERSION_1 + 1;
    task.ranges = { TokenRange{ NODE_A_TOKEN, NODE_B_TOKEN, NODE_A, false } };
    return task;
}

}  // namespace

TEST(TopologyControlLoopTest, FirstInitCreatesCommittedTopologyWithoutTasks)
{
    FakeTopologyRepository repository;
    HashAlgorithm algorithm(HashAlgorithm::DEFAULT_ALGORITHM_ID, 1);
    TopologyControlLoop controlLoop(repository, algorithm);
    TopologyReconcileRequest request;
    request.readyNodeIds = { NODE_A, NODE_B };
    TopologyReconcileResult result;

    DS_ASSERT_OK(controlLoop.Reconcile(request, result));
    EXPECT_EQ(result.kind, TopologyChangeKind::FIRST_INIT);
    EXPECT_EQ(result.createdTransferTasks, 0);
    EXPECT_EQ(result.createdRecoveryTasks, 0);

    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    EXPECT_EQ(committed.members.size(), 2);
    uint64_t transferTasks = 0;
    uint64_t recoveryTasks = 0;
    DS_ASSERT_OK(CountTasks(repository, transferTasks, recoveryTasks));
    EXPECT_EQ(transferTasks, 0);
    EXPECT_EQ(recoveryTasks, 0);
}

TEST(TopologyControlLoopTest, ScaleOutCreatesTransitionThenActivatesAfterProgress)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedCommittedTopology(MakeSingleNodeTopology()));
    HashAlgorithm algorithm(HashAlgorithm::DEFAULT_ALGORITHM_ID, 1);
    TopologyControlLoop controlLoop(repository, algorithm);
    TopologyReconcileRequest request;
    request.readyNodeIds = { NODE_A, NODE_B };
    TopologyReconcileResult result;

    DS_ASSERT_OK(controlLoop.Reconcile(request, result));
    EXPECT_EQ(result.kind, TopologyChangeKind::SCALE_OUT);
    EXPECT_GT(result.createdTransferTasks, 0);
    EXPECT_EQ(result.createdRecoveryTasks, 0);
    EXPECT_GT(result.updatedNotifies, 0);

    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    auto member = FindMember(committed, NODE_B);
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(member->state, TopologyNodeState::JOINING);

    DS_ASSERT_OK(FinishAllTransferTasks(repository));
    DS_ASSERT_OK(controlLoop.Reconcile(request, result));
    EXPECT_EQ(result.kind, TopologyChangeKind::ACTIVATE_PENDING);
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    member = FindMember(committed, NODE_B);
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(member->state, TopologyNodeState::ACTIVE);

    uint64_t transferTasks = 0;
    uint64_t recoveryTasks = 0;
    DS_ASSERT_OK(CountTasks(repository, transferTasks, recoveryTasks));
    EXPECT_GT(transferTasks, 0);
    EXPECT_EQ(recoveryTasks, 0);
}

TEST(TopologyControlLoopTest, ActiveScaleInCreatesTransferTasks)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedCommittedTopology(MakeTwoNodeTopology()));
    HashAlgorithm algorithm(HashAlgorithm::DEFAULT_ALGORITHM_ID, 1);
    TopologyControlLoop controlLoop(repository, algorithm);
    TopologyReconcileRequest request;
    request.orderlyLeavingNodeIds = { NODE_B };
    TopologyReconcileResult result;

    DS_ASSERT_OK(controlLoop.Reconcile(request, result));
    EXPECT_EQ(result.kind, TopologyChangeKind::ACTIVE_SCALE_IN);
    EXPECT_GT(result.createdTransferTasks, 0);
    EXPECT_EQ(result.createdRecoveryTasks, 0);

    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    auto member = FindMember(committed, NODE_B);
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(member->state, TopologyNodeState::PRE_LEAVING);
}

TEST(TopologyControlLoopTest, ActiveScaleInActivatesWithoutRepeatingLeavingFact)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedCommittedTopology(MakeTwoNodeTopology()));
    HashAlgorithm algorithm(HashAlgorithm::DEFAULT_ALGORITHM_ID, 1);
    TopologyControlLoop controlLoop(repository, algorithm);
    TopologyReconcileRequest request;
    request.orderlyLeavingNodeIds = { NODE_B };
    TopologyReconcileResult result;

    DS_ASSERT_OK(controlLoop.Reconcile(request, result));
    DS_ASSERT_OK(FinishAllTransferTasks(repository));

    TopologyReconcileRequest activationRequest;
    DS_ASSERT_OK(controlLoop.Reconcile(activationRequest, result));
    EXPECT_EQ(result.kind, TopologyChangeKind::ACTIVATE_PENDING);

    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    auto member = FindMember(committed, NODE_A);
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(member->state, TopologyNodeState::ACTIVE);
    EXPECT_EQ(FindMember(committed, NODE_B), nullptr);
}

TEST(TopologyControlLoopTest, PassiveFailureCreatesRecoveryTasks)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedCommittedTopology(MakeTwoNodeTopology()));
    HashAlgorithm algorithm(HashAlgorithm::DEFAULT_ALGORITHM_ID, 1);
    TopologyControlLoop controlLoop(repository, algorithm);
    TopologyReconcileRequest request;
    request.failedNodeIds = { NODE_B };
    TopologyReconcileResult result;

    DS_ASSERT_OK(controlLoop.Reconcile(request, result));
    EXPECT_EQ(result.kind, TopologyChangeKind::PASSIVE_FAILURE);
    EXPECT_EQ(result.createdTransferTasks, 0);
    EXPECT_GT(result.createdRecoveryTasks, 0);

    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    auto member = FindMember(committed, NODE_B);
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(member->state, TopologyNodeState::LEAVING);
}

TEST(TopologyControlLoopTest, PassiveFailureActivatesWithoutRepeatingFailedFact)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedCommittedTopology(MakeTwoNodeTopology()));
    HashAlgorithm algorithm(HashAlgorithm::DEFAULT_ALGORITHM_ID, 1);
    TopologyControlLoop controlLoop(repository, algorithm);
    TopologyReconcileRequest request;
    request.failedNodeIds = { NODE_B };
    TopologyReconcileResult result;

    DS_ASSERT_OK(controlLoop.Reconcile(request, result));
    DS_ASSERT_OK(FinishAllRecoveryTasks(repository));

    TopologyReconcileRequest activationRequest;
    DS_ASSERT_OK(controlLoop.Reconcile(activationRequest, result));
    EXPECT_EQ(result.kind, TopologyChangeKind::ACTIVATE_PENDING);

    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    auto member = FindMember(committed, NODE_A);
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(member->state, TopologyNodeState::ACTIVE);
    EXPECT_EQ(FindMember(committed, NODE_B), nullptr);
}

TEST(TopologyControlLoopTest, AllMemberOrderlyExitCommitsEmptyTopology)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedCommittedTopology(MakeSingleNodeTopology()));
    HashAlgorithm algorithm(HashAlgorithm::DEFAULT_ALGORITHM_ID, 1);
    TopologyControlLoop controlLoop(repository, algorithm);
    TopologyReconcileRequest request;
    request.orderlyLeavingNodeIds = { NODE_A };
    TopologyReconcileResult result;

    DS_ASSERT_OK(controlLoop.Reconcile(request, result));
    EXPECT_EQ(result.kind, TopologyChangeKind::ORDERLY_CLUSTER_SHUTDOWN);

    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    EXPECT_TRUE(committed.members.empty());
    EXPECT_EQ(committed.version, result.committedVersion);
}

TEST(TopologyControlLoopTest, StaleMutationPlanReturnsCasConflict)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedCommittedTopology(MakeSingleNodeTopology()));
    TopologyMutationApplier applier(repository);
    TopologyChangePlan plan;
    plan.kind = TopologyChangeKind::ORDERLY_CLUSTER_SHUTDOWN;
    plan.expectedTopology = MakeSingleNodeTopology();
    plan.expectedRevision = 0;
    plan.nextTopology = plan.expectedTopology;
    plan.nextTopology.members.clear();
    plan.transferTasks = { MakeTransferTask() };
    TopologyReconcileResult result;

    EXPECT_EQ(applier.Apply(plan, result).GetCode(), K_INVALID);
    uint64_t transferTasks = 0;
    uint64_t recoveryTasks = 0;
    DS_ASSERT_OK(CountTasks(repository, transferTasks, recoveryTasks));
    EXPECT_EQ(transferTasks, 0);
    EXPECT_EQ(recoveryTasks, 0);
}

TEST(TopologyControlLoopTest, NotifyFailureRollsBackTasksAndKeepsCommittedTopology)
{
    FakeTopologyRepository repository;
    DS_ASSERT_OK(repository.SeedCommittedTopology(MakeSingleNodeTopology()));
    repository.InjectNotifyFailure(Status(K_RUNTIME_ERROR, "notify failed"));
    HashAlgorithm algorithm(HashAlgorithm::DEFAULT_ALGORITHM_ID, 1);
    TopologyControlLoop controlLoop(repository, algorithm);
    TopologyReconcileRequest request;
    request.readyNodeIds = { NODE_A, NODE_B };
    TopologyReconcileResult result;

    EXPECT_EQ(controlLoop.Reconcile(request, result).GetCode(), K_RUNTIME_ERROR);

    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repository.GetCommittedTopology(committed, revision));
    EXPECT_EQ(committed.members.size(), 1);
    EXPECT_EQ(committed.members[0].nodeId, NODE_A);
    uint64_t transferTasks = 0;
    uint64_t recoveryTasks = 0;
    DS_ASSERT_OK(CountTasks(repository, transferTasks, recoveryTasks));
    EXPECT_EQ(transferTasks, 0);
    EXPECT_EQ(recoveryTasks, 0);
}

}  // namespace topology
}  // namespace datasystem
