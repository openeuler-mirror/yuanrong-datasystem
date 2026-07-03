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
 * Description: In-memory topology module scale tests.
 */
#include "tests/ut/topology/testing/fake_topology_repository.h"
#include "tests/ut/topology/testing/fake_membership_view.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "datasystem/topology/algorithm/hash_algorithm.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char HASH_ALGORITHM_ID[] = "hash";
constexpr int BASE_WORKER_NUM = 2000;
constexpr int CHANGE_WORKER_NUM = 500;
constexpr int SCALE_BUDGET_MS = 2000;
constexpr uint32_t TOKEN_STRIDE = 2;

TopologyNodeId WorkerName(int index)
{
    return "worker-" + std::to_string(index);
}

TopologyNodeId NewWorkerName(int index)
{
    return "worker-new-" + std::to_string(index);
}

PlacementUnit MakeHashUnit(uint32_t token)
{
    PlacementUnit unit;
    unit.algorithmId = HASH_ALGORITHM_ID;
    unit.unitType = "hash-token";
    unit.opaqueUnit = std::to_string(token);
    return unit;
}

TopologyDescriptor MakeTopologyWithRange(int begin, int end, Revision version)
{
    TopologyDescriptor topology;
    topology.version = version;
    topology.clusterHasInit = true;
    topology.members.reserve(static_cast<size_t>(end - begin));
    for (int i = begin; i < end; ++i) {
        topology.members.push_back(
            { WorkerName(i), TopologyNodeState::ACTIVE, { static_cast<uint32_t>(i) * TOKEN_STRIDE + 1 } });
    }
    return topology;
}

MembershipSnapshot MakeMembershipSnapshot(const TopologyDescriptor &topology)
{
    MembershipSnapshot snapshot;
    snapshot.revision = topology.version;
    for (const auto &worker : topology.members) {
        MembershipRecord record;
        record.nodeId = worker.nodeId;
        record.endpoint.host = worker.nodeId;
        record.endpoint.port = 8080;
        record.lifecycleState = MemberLifecycleState::READY;
        snapshot.members[worker.nodeId] = record;
    }
    return snapshot;
}

TransferTaskRecord MakeTransferTask(const TopologyNodeId &target, const TopologyNodeId &source, uint32_t begin)
{
    TransferTaskRecord task;
    task.taskId = target + "|" + source;
    task.targetNodeId = target;
    task.sourceNodeId = source;
    task.ranges = { { begin, begin + 1, source, false } };
    return task;
}

RecoveryTaskRecord MakeRecoveryTask(const TopologyNodeId &failed, const TopologyNodeId &recovery, uint32_t begin)
{
    RecoveryTaskRecord task;
    task.taskId = failed + "|" + recovery;
    task.failedNodeId = failed;
    task.recoveryNodeId = recovery;
    task.ranges = { { begin, begin + 1, recovery, false } };
    return task;
}

TEST(TopologyMemoryScaleModuleTest, ScaleOut2000NodesAdd500Within2s)
{
    auto topology = MakeTopologyWithRange(0, BASE_WORKER_NUM, 10);
    topology.members.reserve(BASE_WORKER_NUM + CHANGE_WORKER_NUM);
    FakeTopologyRepository repo;
    for (int i = 0; i < CHANGE_WORKER_NUM; ++i) {
        auto target = NewWorkerName(i);
        topology.members.push_back(
            { target, TopologyNodeState::ACTIVE, { static_cast<uint32_t>(i) * TOKEN_STRIDE } });
        DS_ASSERT_OK(repo.SeedTransferTask(MakeTransferTask(target, WorkerName(i), static_cast<uint32_t>(i))));
    }
    topology.version = 11;
    DS_ASSERT_OK(repo.SeedCommittedTopology(topology));

    FakeMembershipView endpointView;
    DS_ASSERT_OK(endpointView.SeedSnapshot(MakeMembershipSnapshot(topology)));

    HashAlgorithm algorithm;
    auto begin = std::chrono::steady_clock::now();
    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(committed, revision));
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm.BuildRoutingState(committed, state));
    std::vector<TransferTaskRecord> transferTasks;
    DS_ASSERT_OK(repo.ListTransferTaskRecords({}, transferTasks));
    std::vector<MembershipRecord> readyWorkers;
    DS_ASSERT_OK(endpointView.ListReadyMembers(readyWorkers));
    for (int i = 0; i < CHANGE_WORKER_NUM; ++i) {
        LogicalOwner owner;
        DS_ASSERT_OK(algorithm.Route(*state, MakeHashUnit(static_cast<uint32_t>(i) * TOKEN_STRIDE), owner));
        EXPECT_EQ(owner.nodeId, NewWorkerName(i));
    }
    auto costMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);

    EXPECT_EQ(transferTasks.size(), static_cast<size_t>(CHANGE_WORKER_NUM));
    EXPECT_EQ(readyWorkers.size(), static_cast<size_t>(BASE_WORKER_NUM + CHANGE_WORKER_NUM));
    EXPECT_LT(costMs.count(), SCALE_BUDGET_MS);
}

TEST(TopologyMemoryScaleModuleTest, ScaleIn2000NodesRemove500Within2s)
{
    auto topology = MakeTopologyWithRange(CHANGE_WORKER_NUM, BASE_WORKER_NUM, 20);
    FakeTopologyRepository repo;
    for (int i = 0; i < CHANGE_WORKER_NUM; ++i) {
        DS_ASSERT_OK(repo.SeedRecoveryTask(
            MakeRecoveryTask(WorkerName(i), WorkerName(i + CHANGE_WORKER_NUM), static_cast<uint32_t>(i))));
    }
    DS_ASSERT_OK(repo.SeedCommittedTopology(topology));

    FakeMembershipView endpointView;
    DS_ASSERT_OK(endpointView.SeedSnapshot(MakeMembershipSnapshot(topology)));

    HashAlgorithm algorithm;
    auto begin = std::chrono::steady_clock::now();
    TopologyDescriptor committed;
    Revision revision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(committed, revision));
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm.BuildRoutingState(committed, state));
    std::vector<RecoveryTaskRecord> recoveryTasks;
    DS_ASSERT_OK(repo.ListRecoveryTaskRecords({}, recoveryTasks));
    std::vector<MembershipRecord> readyWorkers;
    DS_ASSERT_OK(endpointView.ListReadyMembers(readyWorkers));
    for (int i = CHANGE_WORKER_NUM; i < CHANGE_WORKER_NUM * 2; ++i) {
        LogicalOwner owner;
        DS_ASSERT_OK(algorithm.Route(*state, MakeHashUnit(static_cast<uint32_t>(i) * TOKEN_STRIDE + 1), owner));
        EXPECT_EQ(owner.nodeId, WorkerName(i));
    }
    auto costMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);

    EXPECT_EQ(recoveryTasks.size(), static_cast<size_t>(CHANGE_WORKER_NUM));
    EXPECT_EQ(readyWorkers.size(), static_cast<size_t>(BASE_WORKER_NUM - CHANGE_WORKER_NUM));
    EXPECT_LT(costMs.count(), SCALE_BUDGET_MS);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
