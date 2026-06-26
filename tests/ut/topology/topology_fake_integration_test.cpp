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
 * Description: Topology fake integration tests for A/B contracts.
 */
#include "tests/ut/topology/testing/fake_topology_change_requester.h"
#include "tests/ut/topology/testing/fake_topology_repository.h"
#include "tests/ut/topology/testing/fake_worker_directory.h"

#include <initializer_list>
#include <memory>
#include <type_traits>

#include "gtest/gtest.h"

#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/algorithm/hash_algorithm.h"
#include "datasystem/topology/repository/topology_repository_codec.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char HASH_ALGORITHM_ID[] = "hash";
constexpr char WORKER_A[] = "worker-a";
constexpr char WORKER_B[] = "worker-b";
constexpr char WORKER_C[] = "worker-c";

TopologyDescriptor MakeTopology(std::initializer_list<TopologyWorker> workers, Revision version)
{
    TopologyDescriptor topology;
    topology.version = version;
    topology.clusterHasInit = true;
    topology.workers.assign(workers.begin(), workers.end());
    return topology;
}

PlacementUnit MakeHashUnit(uint32_t token)
{
    PlacementUnit unit;
    unit.algorithmId = HASH_ALGORITHM_ID;
    unit.unitType = "hash-token";
    unit.opaqueUnit = std::to_string(token);
    return unit;
}

TransferTaskRecord MakeTransferTask(const WorkerId &target, const WorkerId &source, uint32_t begin, uint32_t end)
{
    TransferTaskRecord task;
    task.taskId = target + "|" + source;
    task.targetWorkerId = target;
    task.sourceWorkerId = source;
    task.ranges = { { begin, end, source, false } };
    return task;
}

RecoveryTaskRecord MakeRecoveryTask(const WorkerId &failed, const WorkerId &recovery, uint32_t begin, uint32_t end)
{
    RecoveryTaskRecord task;
    task.taskId = failed + "|" + recovery;
    task.failedWorkerId = failed;
    task.recoveryWorkerId = recovery;
    task.ranges = { { begin, end, recovery, false } };
    return task;
}

MembershipSnapshot MakeReadySnapshot(std::initializer_list<WorkerId> workers, Revision revision)
{
    MembershipSnapshot snapshot;
    snapshot.revision = revision;
    for (const auto &workerId : workers) {
        WorkerRecord record;
        record.workerId = workerId;
        record.endpoint.host = workerId;
        record.endpoint.port = 8080;
        record.serviceState = WorkerServiceState::READY;
        snapshot.workers[workerId] = record;
    }
    return snapshot;
}

TEST(TopologyFakeIntegrationTest, RoutingConsumesCommittedTopologyOnly)
{
    FakeTopologyRepository repo;
    DS_ASSERT_OK(repo.SeedCommittedTopology(MakeTopology({ { WORKER_A, WorkerTopologyState::ACTIVE, { 100 } } }, 10)));

    HashAlgorithm algorithm;
    TopologyDescriptor topology;
    Revision revision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(topology, revision));
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm.BuildRoutingState(topology, state));

    DS_ASSERT_OK(repo.SeedCommittedTopology(MakeTopology(
        { { WORKER_A, WorkerTopologyState::ACTIVE, { 100 } }, { WORKER_B, WorkerTopologyState::ACTIVE, { 60 } } },
        11)));

    LogicalOwner owner;
    DS_ASSERT_OK(algorithm.Route(*state, MakeHashUnit(50), owner));
    EXPECT_EQ(owner.workerId, WORKER_A);

    DS_ASSERT_OK(repo.GetCommittedTopology(topology, revision));
    DS_ASSERT_OK(algorithm.BuildRoutingState(topology, state));
    DS_ASSERT_OK(algorithm.Route(*state, MakeHashUnit(50), owner));
    EXPECT_EQ(owner.workerId, WORKER_B);
}

TEST(TopologyFakeIntegrationTest, TaskExecutorReadsNotifyThenTaskFact)
{
    FakeTopologyRepository repo;
    auto task = MakeTransferTask(WORKER_C, WORKER_A, 1, 10);
    DS_ASSERT_OK(repo.SeedTransferTask(task));

    TaskFilter filter;
    filter.workerId = WORKER_A;
    filter.unfinishedOnly = true;
    std::vector<TransferTaskRecord> tasks;
    DS_ASSERT_OK(repo.ListTransferTaskRecords(filter, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_EQ(tasks[0].taskId, task.taskId);

    TaskProgressUpdate progress;
    progress.taskId = task.taskId;
    progress.workerId = WORKER_A;
    progress.range = { 1, 10, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportTransferProgress(task.taskId, progress));

    DS_ASSERT_OK(repo.ListTransferTaskRecords(filter, tasks));
    EXPECT_TRUE(tasks.empty());
}

TEST(TopologyFakeIntegrationTest, ProgressConflictRequiresReload)
{
    FakeTopologyRepository repo;
    auto task = MakeTransferTask(WORKER_C, WORKER_A, 1, 10);
    DS_ASSERT_OK(repo.SeedTransferTask(task));
    repo.InjectTransferProgressConflict();

    TaskProgressUpdate progress;
    progress.taskId = task.taskId;
    progress.workerId = WORKER_A;
    progress.range = { 1, 10, WORKER_A, true };
    EXPECT_EQ(repo.ReportTransferProgress(task.taskId, progress).GetCode(), K_TRY_AGAIN);

    std::vector<TransferTaskRecord> tasks;
    DS_ASSERT_OK(repo.ListTransferTaskRecords({}, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_FALSE(tasks[0].ranges[0].finished);

    DS_ASSERT_OK(repo.ReportTransferProgress(task.taskId, progress));
}

TEST(TopologyFakeIntegrationTest, FakeRepositoryRejectsMalformedSeedData)
{
    FakeTopologyRepository repo;
    EXPECT_EQ(repo.SeedCommittedTopology({}).GetCode(), K_INVALID);

    auto topology = MakeTopology({ { WORKER_A, WorkerTopologyState::ACTIVE, { 1 } } }, -1);
    EXPECT_EQ(repo.SeedCommittedTopology(topology).GetCode(), K_INVALID);

    auto transfer = MakeTransferTask(WORKER_C, WORKER_A, 1, 10);
    transfer.taskId.clear();
    EXPECT_EQ(repo.SeedTransferTask(transfer).GetCode(), K_INVALID);

    transfer = MakeTransferTask(WORKER_C, WORKER_A, 1, 10);
    transfer.sourceWorkerId.clear();
    EXPECT_EQ(repo.SeedTransferTask(transfer).GetCode(), K_INVALID);

    transfer = MakeTransferTask(WORKER_C, WORKER_A, 1, 10);
    transfer.targetWorkerId.clear();
    EXPECT_EQ(repo.SeedTransferTask(transfer).GetCode(), K_INVALID);

    transfer = MakeTransferTask(WORKER_C, WORKER_A, 1, 10);
    transfer.ranges.clear();
    EXPECT_EQ(repo.SeedTransferTask(transfer).GetCode(), K_INVALID);

    transfer = MakeTransferTask(WORKER_C, WORKER_A, 10, 10);
    EXPECT_EQ(repo.SeedTransferTask(transfer).GetCode(), K_INVALID);

    transfer = MakeTransferTask(WORKER_C, WORKER_A, 1, 10);
    transfer.ranges[0].workerId.clear();
    EXPECT_EQ(repo.SeedTransferTask(transfer).GetCode(), K_INVALID);

    auto recovery = MakeRecoveryTask(WORKER_B, WORKER_A, 20, 30);
    recovery.taskId.clear();
    EXPECT_EQ(repo.SeedRecoveryTask(recovery).GetCode(), K_INVALID);

    recovery = MakeRecoveryTask(WORKER_B, WORKER_A, 20, 30);
    recovery.failedWorkerId.clear();
    EXPECT_EQ(repo.SeedRecoveryTask(recovery).GetCode(), K_INVALID);

    recovery = MakeRecoveryTask(WORKER_B, WORKER_A, 20, 30);
    recovery.recoveryWorkerId.clear();
    EXPECT_EQ(repo.SeedRecoveryTask(recovery).GetCode(), K_INVALID);

    recovery = MakeRecoveryTask(WORKER_B, WORKER_A, 20, 30);
    recovery.ranges.clear();
    EXPECT_EQ(repo.SeedRecoveryTask(recovery).GetCode(), K_INVALID);
}

TEST(TopologyFakeIntegrationTest, FakeRepositoryFiltersAndReportsRecoveryTasks)
{
    FakeTopologyRepository repo;
    auto recovery = MakeRecoveryTask(WORKER_B, WORKER_A, 20, 30);
    DS_ASSERT_OK(repo.SeedRecoveryTask(recovery));

    TaskFilter filter;
    filter.workerId = WORKER_C;
    filter.unfinishedOnly = true;
    std::vector<RecoveryTaskRecord> recoveryTasks;
    DS_ASSERT_OK(repo.ListRecoveryTaskRecords(filter, recoveryTasks));
    EXPECT_TRUE(recoveryTasks.empty());

    filter.workerId = WORKER_A;
    DS_ASSERT_OK(repo.ListRecoveryTaskRecords(filter, recoveryTasks));
    ASSERT_EQ(recoveryTasks.size(), 1ul);
    EXPECT_EQ(recoveryTasks[0].taskId, recovery.taskId);

    TaskProgressUpdate progress;
    progress.taskId = recovery.taskId;
    progress.workerId = WORKER_A;
    progress.range = { 20, 30, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportRecoveryProgress(recovery.taskId, progress));

    DS_ASSERT_OK(repo.ListRecoveryTaskRecords(filter, recoveryTasks));
    EXPECT_TRUE(recoveryTasks.empty());

    progress.taskId = recovery.taskId;
    progress.range = { 21, 30, WORKER_A, true };
    EXPECT_EQ(repo.ReportRecoveryProgress(recovery.taskId, progress).GetCode(), K_INVALID);
}

TEST(TopologyFakeIntegrationTest, FakeRepositoryReportsTransferRangeNotFound)
{
    FakeTopologyRepository repo;
    auto task = MakeTransferTask(WORKER_C, WORKER_A, 1, 10);
    DS_ASSERT_OK(repo.SeedTransferTask(task));

    TaskProgressUpdate progress;
    progress.taskId = task.taskId;
    progress.workerId = WORKER_A;
    progress.range = { 2, 10, WORKER_A, true };
    EXPECT_EQ(repo.ReportTransferProgress(task.taskId, progress).GetCode(), K_INVALID);
}

TEST(TopologyFakeIntegrationTest, FakeRepositoryRejectsMalformedTopologyEvents)
{
    FakeTopologyRepository repo;
    TopologyWatchEvent typed;

    CoordinationEvent event;
    event.type = CoordinationEventType::PUT;
    event.key = "/datasystem/ring/bad";
    EXPECT_EQ(repo.HandleCommittedTopologyEvent(event, typed).GetCode(), K_NOT_FOUND);

    event.key = "/datasystem/ring";
    event.value = "not-a-ring";
    EXPECT_EQ(repo.HandleCommittedTopologyEvent(event, typed).GetCode(), K_INVALID);
}

TEST(TopologyFakeIntegrationTest, FakeRepositoryDecodesCommittedTopologyEvents)
{
    FakeTopologyRepository repo;
    TopologyWatchEvent typed;
    CoordinationEvent event;
    event.type = CoordinationEventType::DELETE;
    event.key = "/datasystem/ring";
    event.revision = 17;

    DS_ASSERT_OK(repo.HandleCommittedTopologyEvent(event, typed));
    EXPECT_EQ(typed.type, TopologyWatchEventType::DELETED);
    EXPECT_EQ(typed.revision, 17);

    event.type = CoordinationEventType::PUT;
    event.revision = 18;
    TopologyRepositoryCodec codec;
    DS_ASSERT_OK(
        codec.EncodeTopology(MakeTopology({ { WORKER_A, WorkerTopologyState::ACTIVE, { 1 } } }, 1), event.value));
    DS_ASSERT_OK(repo.HandleCommittedTopologyEvent(event, typed));
    EXPECT_EQ(typed.type, TopologyWatchEventType::UPDATED);
    EXPECT_EQ(typed.revision, 18);
    EXPECT_EQ(typed.topology.version, 18);
    ASSERT_EQ(typed.topology.workers.size(), 1ul);
    EXPECT_EQ(typed.topology.workers[0].workerId, WORKER_A);
}

TEST(TopologyFakeIntegrationTest, ScaleInRequesterAcceptRejectAndQueueFull)
{
    FakeTopologyChangeRequester requester(1);
    ScaleInRequest request;
    request.workerId = WORKER_A;
    request.reason = ScaleInReason::ORDERLY_SHUTDOWN;

    requester.SetAvailable(false);
    EXPECT_EQ(requester.SubmitScaleInRequest(request, 1).GetCode(), K_NOT_READY);

    requester.SetAvailable(true);
    DS_ASSERT_OK(requester.SubmitScaleInRequest(request, 1));
    EXPECT_EQ(requester.SubmittedRequests().size(), 1ul);
    EXPECT_EQ(requester.SubmitScaleInRequest(request, 2).GetCode(), K_WRITE_BACK_QUEUE_FULL);
}

TEST(TopologyFakeIntegrationTest, FakesDoNotExposeRingMutation)
{
    FakeTopologyRepository repo;
    static_assert(std::is_base_of<ITopologyRepository, FakeTopologyRepository>::value,
                  "fake repository must implement the B-facing repository contract");
    static_assert(!std::is_base_of<ICoordinationBackend, FakeTopologyRepository>::value,
                  "fake repository must not expose raw ring/store mutation");

    ITopologyRepository *contract = &repo;
    DS_ASSERT_OK(repo.SeedCommittedTopology(MakeTopology({ { WORKER_A, WorkerTopologyState::ACTIVE, { 1 } } }, 1)));
    TopologyDescriptor topology;
    Revision revision = 0;
    DS_ASSERT_OK(contract->GetCommittedTopology(topology, revision));
    EXPECT_EQ(topology.workers.size(), 1ul);
}

TEST(TopologyFakeIntegrationTest, WorkerDirectoryFakeProvidesReadyView)
{
    FakeWorkerDirectory directory;
    std::vector<WorkerRecord> workers;
    EXPECT_EQ(directory.ListReadyWorkers(workers).GetCode(), K_NOT_READY);

    DS_ASSERT_OK(directory.SeedSnapshot(MakeReadySnapshot({ WORKER_A, WORKER_B }, 3)));
    WorkerEndpoint endpoint;
    DS_ASSERT_OK(directory.GetReadyEndpoint(WORKER_A, endpoint));
    EXPECT_EQ(endpoint.host, WORKER_A);

    workers.clear();
    DS_ASSERT_OK(directory.ListReadyWorkers(workers));
    EXPECT_EQ(workers.size(), 2ul);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
