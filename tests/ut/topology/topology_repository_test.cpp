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
 * Description: Topology repository tests.
 */
#include "datasystem/topology/repository/topology_repository.h"

#include <chrono>
#include <string>

#include "gtest/gtest.h"

#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/protos/hash_ring_v2.pb.h"
#include "datasystem/topology/repository/topology_key_helper.h"
#include "datasystem/topology/repository/topology_repository_codec.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/fake_coordination_backend.h"
#include "tests/ut/topology/testing/topology_test_utils.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char WORKER_A[] = "127.0.0.1:7001";
constexpr char WORKER_B[] = "127.0.0.1:7002";
constexpr char WORKER_C[] = "127.0.0.1:7003";
constexpr char WORKER_D[] = "127.0.0.1:7004";
constexpr char WORKER_E[] = "127.0.0.1:7005";
constexpr char MIGRATE_TASK_ID[] = "migrate-change-1-127.0.0.1:7002-127.0.0.1:7001";
constexpr char DELETE_TASK_ID[] = "delete-change-2-127.0.0.1:7003-127.0.0.1:7001";

std::string SerializeRing(const v2::HashRingPb &ring)
{
    std::string bytes;
    EXPECT_TRUE(ring.SerializeToString(&bytes));
    return bytes;
}

std::string SerializeLegacyRing(const HashRingPb &ring)
{
    std::string bytes;
    EXPECT_TRUE(ring.SerializeToString(&bytes));
    return bytes;
}

TopologyNode MakeTopologyNode(const TopologyNodeId &nodeId, TopologyNodeState state, uint32_t token)
{
    TopologyNode worker;
    worker.nodeId = nodeId;
    worker.state = state;
    worker.tokens = { token };
    return worker;
}

v2::HashRingPb MakeRing()
{
    v2::HashRingPb ring;
    ring.set_schema_version("1");
    ring.set_version(5);
    ring.set_cluster_has_init(true);
    auto &workerA = (*ring.mutable_workers())[WORKER_A];
    workerA.set_state(v2::WorkerPb::ACTIVE);
    workerA.add_hash_tokens(10);
    auto &workerB = (*ring.mutable_workers())[WORKER_B];
    workerB.set_state(v2::WorkerPb::ACTIVE);
    workerB.add_hash_tokens(20);
    return ring;
}

TransferTaskRecord MakeMigrateTaskRecord()
{
    TransferTaskRecord task;
    task.taskId = MIGRATE_TASK_ID;
    task.taskRevision = 3;
    task.executorNodeId = WORKER_B;
    task.sourceNodeId = WORKER_B;
    task.targetNodeId = WORKER_A;
    task.createdTopologyVersion = 10;
    task.targetTopologyVersion = 11;
    task.status = TaskTerminalStatus::RUNNING;
    task.ranges = { { 1, 10, WORKER_B, false }, { 20, 30, WORKER_B, true } };
    return task;
}

RecoveryTaskRecord MakeDeleteNodeTaskRecord()
{
    RecoveryTaskRecord task;
    task.taskId = DELETE_TASK_ID;
    task.taskRevision = 4;
    task.executorNodeId = WORKER_A;
    task.failedNodeId = WORKER_C;
    task.recoveryNodeId = WORKER_A;
    task.createdTopologyVersion = 12;
    task.targetTopologyVersion = 13;
    task.status = TaskTerminalStatus::RUNNING;
    task.ranges = { { 100, 200, WORKER_A, false } };
    return task;
}

TEST(TopologyRepositoryTest, ReadsCommittedTopologyFromCoordinationBackendRing)
{
    FakeCoordinationBackend store;
    auto ring = MakeRing();
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, "", SerializeRing(ring)));
    TopologyRepository repo(store);

    TopologyDescriptor topology;
    Revision revision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(topology, revision));

    EXPECT_GT(revision, 0);
    EXPECT_EQ(topology.version, 5);
    EXPECT_TRUE(topology.clusterHasInit);
    ASSERT_EQ(topology.members.size(), 2ul);
    EXPECT_EQ(topology.members[0].nodeId, WORKER_A);
    EXPECT_EQ(topology.members[0].state, TopologyNodeState::ACTIVE);
}

TEST(TopologyRepositoryTest, TryCreateCommittedTopologyIsCreateOnly)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    auto topology = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
        },
        1);

    Revision createdRevision = 0;
    DS_ASSERT_OK(repo.TryCreateCommittedTopology(topology, createdRevision));
    EXPECT_GT(createdRevision, 0);

    auto conflicting = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_B, { 20, 30 }),
        },
        1);
    Revision ignoredRevision = 0;
    EXPECT_EQ(repo.TryCreateCommittedTopology(conflicting, ignoredRevision).GetCode(), K_TRY_AGAIN);

    TopologyDescriptor stored;
    Revision readRevision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(stored, readRevision));
    ASSERT_EQ(stored.members.size(), 1ul);
    EXPECT_EQ(stored.version, 1);
    EXPECT_TRUE(stored.clusterHasInit);
    EXPECT_EQ(stored.members[0].nodeId, testing::WORKER_A);
    EXPECT_EQ(stored.members[0].tokens, std::vector<uint32_t>({ 1, 10 }));
}

TEST(TopologyRepositoryTest, TryCreateCommittedTopologyDoesNotOverwriteExistingEmptyRootKey)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, "", ""));
    TopologyRepository repo(store);
    auto topology = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
        },
        1);

    Revision ignoredRevision = 0;
    EXPECT_EQ(repo.TryCreateCommittedTopology(topology, ignoredRevision).GetCode(), K_TRY_AGAIN);

    std::string rawValue;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, "", rawValue));
    EXPECT_TRUE(rawValue.empty());
}

TEST(TopologyRepositoryTest, TryUpdateCommittedTopologyRequiresExpectedRevisionAndTopology)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    auto topology = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
        },
        1);
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(store, topology));

    TopologyDescriptor expected;
    Revision expectedRevision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(expected, expectedRevision));

    auto next = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
            testing::MakeTopologyNodeForTest(testing::WORKER_B, { 20, 30 }),
        },
        2);
    Revision updatedRevision = 0;
    DS_ASSERT_OK(repo.TryUpdateCommittedTopology(expected, expectedRevision, next, updatedRevision));
    EXPECT_GT(updatedRevision, expectedRevision);

    TopologyDescriptor stored;
    Revision storedRevision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(stored, storedRevision));
    EXPECT_EQ(stored.version, 2);
    EXPECT_EQ(stored.members.size(), 2ul);

    auto conflictingNext = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
            testing::MakeTopologyNodeForTest(testing::WORKER_C, { 40, 50 }),
        },
        3);
    Revision ignoredRevision = 0;
    EXPECT_EQ(repo.TryUpdateCommittedTopology(expected, expectedRevision, conflictingNext, ignoredRevision).GetCode(),
              K_TRY_AGAIN);

    TopologyDescriptor wrongExpected = stored;
    wrongExpected.members.pop_back();
    EXPECT_EQ(
        repo.TryUpdateCommittedTopology(wrongExpected, storedRevision, conflictingNext, ignoredRevision).GetCode(),
        K_TRY_AGAIN);
}

TEST(TopologyRepositoryTest, TryUpdateCommittedTopologyReturnsTryAgainOnCasConflict)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    auto topology = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
        },
        1);
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(store, topology));

    TopologyDescriptor expected;
    Revision expectedRevision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(expected, expectedRevision));
    auto next = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
            testing::MakeTopologyNodeForTest(testing::WORKER_B, { 20, 30 }),
        },
        2);
    store.InjectCasConflictsForTest(1);

    Revision ignoredRevision = 0;
    EXPECT_EQ(repo.TryUpdateCommittedTopology(expected, expectedRevision, next, ignoredRevision).GetCode(),
              K_TRY_AGAIN);

    TopologyDescriptor stored;
    Revision storedRevision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(stored, storedRevision));
    EXPECT_EQ(stored.version, 1);
    EXPECT_EQ(stored.members.size(), 1ul);
}

TEST(TopologyRepositoryTest, TryUpdateCommittedTopologyReturnsNotFoundWhenRootIsMissing)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    auto expected = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
        },
        1);
    auto next = testing::MakeTopologyForTest(
        {
            testing::MakeTopologyNodeForTest(testing::WORKER_A, { 1, 10 }),
            testing::MakeTopologyNodeForTest(testing::WORKER_B, { 20, 30 }),
        },
        2);

    Revision ignoredRevision = 0;
    EXPECT_EQ(repo.TryUpdateCommittedTopology(expected, 1, next, ignoredRevision).GetCode(), K_NOT_FOUND);
}

TEST(TopologyRepositoryTest, EncodesAndDecodesTopologyStates)
{
    TopologyRepositoryCodec codec;
    TopologyDescriptor topology;
    topology.version = 42;
    topology.clusterHasInit = false;
    topology.members = {
        MakeTopologyNode(WORKER_D, TopologyNodeState::ACTIVE, 4),
        MakeTopologyNode(WORKER_A, TopologyNodeState::INITIAL, 1),
        MakeTopologyNode(WORKER_C, TopologyNodeState::LEAVING, 3),
        MakeTopologyNode(WORKER_B, TopologyNodeState::JOINING, 2),
        MakeTopologyNode(WORKER_E, TopologyNodeState::PRE_LEAVING, 5),
    };

    std::string bytes;
    DS_ASSERT_OK(codec.EncodeTopology(topology, bytes));

    TopologyDescriptor decoded;
    DS_ASSERT_OK(codec.DecodeTopology(bytes, decoded));
    ASSERT_EQ(decoded.members.size(), 5ul);
    EXPECT_EQ(decoded.version, 42);
    EXPECT_FALSE(decoded.clusterHasInit);
    EXPECT_EQ(decoded.members[0].state, TopologyNodeState::INITIAL);
    EXPECT_EQ(decoded.members[1].state, TopologyNodeState::JOINING);
    EXPECT_EQ(decoded.members[2].state, TopologyNodeState::LEAVING);
    EXPECT_EQ(decoded.members[3].state, TopologyNodeState::ACTIVE);
    EXPECT_EQ(decoded.members[4].state, TopologyNodeState::PRE_LEAVING);
}

TEST(TopologyRepositoryTest, RejectsInvalidTopologyCodecInputs)
{
    TopologyRepositoryCodec codec;
    TopologyDescriptor topology;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeTopology(topology, bytes));
    TopologyDescriptor decodedEmptyTopology;
    DS_ASSERT_OK(codec.DecodeTopology(bytes, decodedEmptyTopology));
    EXPECT_TRUE(decodedEmptyTopology.members.empty());

    topology.version = -1;
    topology.members = { MakeTopologyNode(WORKER_A, TopologyNodeState::ACTIVE, 1) };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.version = 0;
    topology.members = { MakeTopologyNode(WORKER_A, static_cast<TopologyNodeState>(99), 1) };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.members = { MakeTopologyNode("bad/id", TopologyNodeState::ACTIVE, 1) };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.members = { MakeTopologyNode("", TopologyNodeState::ACTIVE, 1) };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.members = { MakeTopologyNode("..", TopologyNodeState::ACTIVE, 1) };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.members = {
        MakeTopologyNode(WORKER_A, TopologyNodeState::ACTIVE, 1),
        MakeTopologyNode(WORKER_A, TopologyNodeState::JOINING, 2),
    };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    auto ring = MakeRing();
    (*ring.mutable_workers())[WORKER_A].set_state(static_cast<v2::WorkerPb::StatePb>(99));
    TopologyDescriptor decoded;
    EXPECT_EQ(codec.DecodeTopology(SerializeRing(ring), decoded).GetCode(), K_INVALID);

    ring.Clear();
    EXPECT_EQ(codec.DecodeTopology(SerializeRing(ring), decoded).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, MigrateTaskCodecRoundTripsRequiredFields)
{
    TopologyRepositoryCodec codec;
    auto task = MakeMigrateTaskRecord();
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(task, bytes));
    TransferTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeMigrateTask(bytes, task.taskId, decoded));
    EXPECT_EQ(decoded.taskId, task.taskId);
    EXPECT_EQ(decoded.executorNodeId, WORKER_B);
    EXPECT_EQ(decoded.sourceNodeId, WORKER_B);
    EXPECT_EQ(decoded.targetNodeId, WORKER_A);
    EXPECT_EQ(decoded.createdTopologyVersion, 0);
    EXPECT_EQ(decoded.targetTopologyVersion, 0);
    ASSERT_EQ(decoded.ranges.size(), 2ul);
    EXPECT_FALSE(decoded.ranges[0].finished);
    EXPECT_TRUE(decoded.ranges[1].finished);
}

TEST(TopologyRepositoryTest, MigrateTaskCodecRestoresVersionWindowFromGeneratedTaskId)
{
    TopologyRepositoryCodec codec;
    auto task = MakeMigrateTaskRecord();
    task.taskId = "migrate-v10-to-v11-0-from-127.0.0.1:7002-to-127.0.0.1:7001";
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(task, bytes));

    TransferTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeMigrateTask(bytes, task.taskId, decoded));
    EXPECT_EQ(decoded.createdTopologyVersion, 10);
    EXPECT_EQ(decoded.targetTopologyVersion, 11);
}

TEST(TopologyRepositoryTest, DeleteNodeTaskCodecRoundTripsRequiredFields)
{
    TopologyRepositoryCodec codec;
    auto task = MakeDeleteNodeTaskRecord();
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeDeleteNodeTask(task, bytes));
    RecoveryTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeDeleteNodeTask(bytes, task.taskId, decoded));
    EXPECT_EQ(decoded.taskId, task.taskId);
    EXPECT_EQ(decoded.executorNodeId, WORKER_A);
    EXPECT_EQ(decoded.failedNodeId, WORKER_C);
    EXPECT_EQ(decoded.recoveryNodeId, WORKER_A);
    ASSERT_EQ(decoded.ranges.size(), 1ul);
    EXPECT_FALSE(decoded.ranges[0].finished);
}

TEST(TopologyRepositoryTest, DeleteNodeTaskCodecRestoresVersionWindowFromGeneratedTaskId)
{
    TopologyRepositoryCodec codec;
    auto task = MakeDeleteNodeTaskRecord();
    task.taskId = "delete-v12-to-v13-0-failed-127.0.0.1:7003-recovery-127.0.0.1:7001";
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeDeleteNodeTask(task, bytes));

    RecoveryTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeDeleteNodeTask(bytes, task.taskId, decoded));
    EXPECT_EQ(decoded.createdTopologyVersion, 12);
    EXPECT_EQ(decoded.targetTopologyVersion, 13);
}

TEST(TopologyRepositoryTest, ApplyTransferProgressUpdatesOnlyMatchingRange)
{
    TopologyRepositoryCodec codec;
    auto task = MakeMigrateTaskRecord();
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(task, bytes));

    TaskProgressUpdate update;
    update.taskId = MIGRATE_TASK_ID;
    update.nodeId = WORKER_B;
    update.range = { 1, 10, WORKER_B, true };

    std::string nextBytes;
    DS_ASSERT_OK(codec.ApplyTransferProgressToTask(bytes, update, nextBytes));
    TransferTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeMigrateTask(nextBytes, task.taskId, decoded));
    ASSERT_EQ(decoded.ranges.size(), 2ul);
    EXPECT_TRUE(decoded.ranges[0].finished);
    EXPECT_TRUE(decoded.ranges[1].finished);
}

TEST(TopologyRepositoryTest, ApplyRecoveryProgressRejectsMismatchedWorkerAndRange)
{
    TopologyRepositoryCodec codec;
    auto task = MakeDeleteNodeTaskRecord();
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeDeleteNodeTask(task, bytes));

    TaskProgressUpdate update;
    update.taskId = DELETE_TASK_ID;
    update.nodeId = WORKER_B;
    update.range = { 100, 200, WORKER_A, true };
    std::string nextBytes;
    EXPECT_EQ(codec.ApplyRecoveryProgressToTask(bytes, update, nextBytes).GetCode(), K_INVALID);

    update.nodeId = WORKER_A;
    update.range = { 101, 200, WORKER_A, true };
    EXPECT_EQ(codec.ApplyRecoveryProgressToTask(bytes, update, nextBytes).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, DecodeTopologyRejectsLegacyHashRingBytes)
{
    TopologyRepositoryCodec codec;
    HashRingPb ring;
    ring.set_cluster_id("legacy-cluster-id");
    ring.set_cluster_has_init(true);
    auto &worker = (*ring.mutable_workers())[WORKER_A];
    worker.set_state(WorkerPb::ACTIVE);
    worker.set_need_scale_down(true);
    worker.add_hash_tokens(10);
    (*ring.mutable_add_node_info())[WORKER_C];
    (*ring.mutable_del_node_info())[WORKER_B];

    TopologyDescriptor topology;
    EXPECT_EQ(codec.DecodeTopology(SerializeLegacyRing(ring), topology).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, NotifyCodecRoundTripsTaskIds)
{
    TopologyRepositoryCodec codec;
    TaskNotify notify;
    notify.nodeAddress = WORKER_A;
    notify.type = TaskNotifyType::SCALE_OUT;
    notify.taskIds = { MIGRATE_TASK_ID };

    std::string bytes;
    DS_ASSERT_OK(codec.EncodeNotify(notify, bytes));
    TaskNotify decoded;
    DS_ASSERT_OK(codec.DecodeNotify(bytes, WORKER_A, decoded));
    EXPECT_EQ(decoded.nodeAddress, WORKER_A);
    EXPECT_EQ(decoded.type, TaskNotifyType::SCALE_OUT);
    ASSERT_EQ(decoded.taskIds.size(), 1ul);
    EXPECT_EQ(decoded.taskIds[0], MIGRATE_TASK_ID);
}

TEST(TopologyRepositoryTest, NotifyCodecAllowsEmptyDoorbell)
{
    TopologyRepositoryCodec codec;
    TaskNotify notify;
    notify.nodeAddress = WORKER_A;
    notify.type = TaskNotifyType::SCALE_OUT;

    std::string bytes;
    DS_ASSERT_OK(codec.EncodeNotify(notify, bytes));
    TaskNotify decoded;
    DS_ASSERT_OK(codec.DecodeNotify(bytes, WORKER_A, decoded));
    EXPECT_EQ(decoded.nodeAddress, WORKER_A);
    EXPECT_EQ(decoded.type, TaskNotifyType::SCALE_OUT);
    EXPECT_TRUE(decoded.taskIds.empty());
}

TEST(TopologyRepositoryTest, ListsMigrateTasksFromSubKeys)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(MakeMigrateTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, "notify/127.0.0.1:7001", "not-a-task"));

    std::vector<TransferTaskRecord> tasks;
    TaskFilter filter;
    filter.nodeId = WORKER_A;
    filter.unfinishedOnly = true;
    DS_ASSERT_OK(repo.ListTransferTaskRecords(filter, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_EQ(tasks[0].taskId, MIGRATE_TASK_ID);
    EXPECT_GE(tasks[0].taskRevision, 0);
    EXPECT_EQ(tasks[0].executorNodeId, WORKER_B);
    EXPECT_EQ(tasks[0].sourceNodeId, WORKER_B);
    EXPECT_EQ(tasks[0].targetNodeId, WORKER_A);
}

TEST(TopologyRepositoryTest, ListsMigrateTasksCanFilterByExecutorWorker)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(MakeMigrateTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));

    TaskFilter targetWorkerFilter;
    targetWorkerFilter.executorNodeId = WORKER_A;
    std::vector<TransferTaskRecord> tasks;
    DS_ASSERT_OK(repo.ListTransferTaskRecords(targetWorkerFilter, tasks));
    EXPECT_TRUE(tasks.empty());

    TaskFilter executorWorkerFilter;
    executorWorkerFilter.executorNodeId = WORKER_B;
    DS_ASSERT_OK(repo.ListTransferTaskRecords(executorWorkerFilter, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_EQ(tasks[0].taskId, MIGRATE_TASK_ID);
    EXPECT_EQ(tasks[0].executorNodeId, WORKER_B);
}

TEST(TopologyRepositoryTest, ListsDeleteNodeTasksFromSubKeys)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeDeleteNodeTask(MakeDeleteNodeTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey(DELETE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));

    std::vector<RecoveryTaskRecord> tasks;
    TaskFilter filter;
    filter.nodeId = WORKER_A;
    filter.unfinishedOnly = true;
    DS_ASSERT_OK(repo.ListRecoveryTaskRecords(filter, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_EQ(tasks[0].taskId, DELETE_TASK_ID);
    EXPECT_EQ(tasks[0].recoveryNodeId, WORKER_A);
    EXPECT_EQ(tasks[0].failedNodeId, WORKER_C);
}

TEST(TopologyRepositoryTest, DeleteRecoveryTaskRecordRemovesOnlyMatchingTask)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    Revision revision = 0;
    auto firstTask = MakeDeleteNodeTaskRecord();
    DS_ASSERT_OK(repo.TryCreateRecoveryTaskRecord(firstTask, revision));
    auto secondTask = MakeDeleteNodeTaskRecord();
    secondTask.taskId = "delete-change-3-127.0.0.1:7003-127.0.0.1:7002";
    secondTask.executorNodeId = WORKER_B;
    secondTask.recoveryNodeId = WORKER_B;
    secondTask.ranges = { { 300, 400, WORKER_B, false } };
    DS_ASSERT_OK(repo.TryCreateRecoveryTaskRecord(secondTask, revision));

    DS_ASSERT_OK(repo.DeleteRecoveryTaskRecord(firstTask.taskId));
    DS_ASSERT_OK(repo.DeleteRecoveryTaskRecord(firstTask.taskId));

    std::vector<RecoveryTaskRecord> tasks;
    DS_ASSERT_OK(repo.ListRecoveryTaskRecords({}, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_EQ(tasks[0].taskId, secondTask.taskId);
}

TEST(TopologyRepositoryTest, DeleteTransferTaskRecordRemovesOnlyMatchingTask)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    Revision revision = 0;
    auto firstTask = MakeMigrateTaskRecord();
    DS_ASSERT_OK(repo.TryCreateTransferTaskRecord(firstTask, revision));
    auto secondTask = MakeMigrateTaskRecord();
    secondTask.taskId = "migrate-change-3-127.0.0.1:7002-127.0.0.1:7003";
    secondTask.executorNodeId = WORKER_A;
    secondTask.sourceNodeId = WORKER_A;
    secondTask.targetNodeId = WORKER_C;
    secondTask.ranges = { { 300, 400, WORKER_A, false } };
    DS_ASSERT_OK(repo.TryCreateTransferTaskRecord(secondTask, revision));

    DS_ASSERT_OK(repo.DeleteTransferTaskRecord(firstTask.taskId));
    DS_ASSERT_OK(repo.DeleteTransferTaskRecord(firstTask.taskId));

    std::vector<TransferTaskRecord> tasks;
    DS_ASSERT_OK(repo.ListTransferTaskRecords({}, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_EQ(tasks[0].taskId, secondTask.taskId);
}

TEST(TopologyRepositoryTest, ReportsTransferProgressToTaskKeyNotRing)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, "", SerializeRing(MakeRing())));
    std::string originalRingBytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, "", originalRingBytes));

    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(MakeMigrateTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));

    TaskProgressUpdate update;
    update.taskId = MIGRATE_TASK_ID;
    update.nodeId = WORKER_B;
    update.range = { 1, 10, WORKER_B, true };
    DS_ASSERT_OK(repo.ReportTransferProgress(update.taskId, update));

    std::string updatedTaskBytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, key, updatedTaskBytes));
    TransferTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeMigrateTask(updatedTaskBytes, MIGRATE_TASK_ID, decoded));
    EXPECT_TRUE(decoded.ranges[0].finished);

    std::string currentRingBytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, "", currentRingBytes));
    EXPECT_EQ(currentRingBytes, originalRingBytes);
}

TEST(TopologyRepositoryTest, ReportsTransferProgressWhenCasResultIsHiddenDuringCallback)
{
    FakeCoordinationBackend store;
    store.SetExposeCasResultBeforeProcessForTest(false);
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;

    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(MakeMigrateTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));

    TaskProgressUpdate update;
    update.taskId = MIGRATE_TASK_ID;
    update.nodeId = WORKER_B;
    update.range = { 1, 10, WORKER_B, true };
    DS_ASSERT_OK(repo.ReportTransferProgress(update.taskId, update));

    std::string updatedTaskBytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, key, updatedTaskBytes));
    TransferTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeMigrateTask(updatedTaskBytes, MIGRATE_TASK_ID, decoded));
    EXPECT_TRUE(decoded.ranges[0].finished);
}

TEST(TopologyRepositoryTest, DuplicateTransferTaskCreateFailsWhenCasResultIsHiddenDuringCallback)
{
    FakeCoordinationBackend store;
    store.SetExposeCasResultBeforeProcessForTest(false);
    TopologyRepository repo(store);
    auto task = MakeMigrateTaskRecord();
    Revision revision = 0;
    DS_ASSERT_OK(repo.TryCreateTransferTaskRecord(task, revision));

    EXPECT_EQ(repo.TryCreateTransferTaskRecord(task, revision).GetCode(), K_TRY_AGAIN);
}

TEST(TopologyRepositoryTest, UpdatesCommittedTopologyWhenCasResultIsHiddenDuringCallback)
{
    FakeCoordinationBackend store;
    store.SetExposeCasResultBeforeProcessForTest(false);
    TopologyRepository repo(store);

    TopologyDescriptor current;
    current.version = 1;
    current.clusterHasInit = true;
    current.members = { MakeTopologyNode(WORKER_A, TopologyNodeState::ACTIVE, 10) };
    Revision revision = 0;
    DS_ASSERT_OK(repo.TryCreateCommittedTopology(current, revision));

    TopologyDescriptor next = current;
    next.version = 2;
    next.members.push_back(MakeTopologyNode(WORKER_B, TopologyNodeState::ACTIVE, 20));
    DS_ASSERT_OK(repo.TryUpdateCommittedTopology(current, revision, next, revision));

    TopologyDescriptor committed;
    DS_ASSERT_OK(repo.GetCommittedTopology(committed, revision));
    EXPECT_EQ(committed.version, 2);
    ASSERT_EQ(committed.members.size(), 2ul);
    EXPECT_EQ(committed.members[1].nodeId, WORKER_B);
}

TEST(TopologyRepositoryTest, ReportsRecoveryProgressToTaskKeyNotRing)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, "", SerializeRing(MakeRing())));
    std::string originalRingBytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, "", originalRingBytes));

    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeDeleteNodeTask(MakeDeleteNodeTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey(DELETE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));

    TaskProgressUpdate update;
    update.taskId = DELETE_TASK_ID;
    update.nodeId = WORKER_A;
    update.range = { 100, 200, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportRecoveryProgress(update.taskId, update));

    std::string updatedTaskBytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, key, updatedTaskBytes));
    RecoveryTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeDeleteNodeTask(updatedTaskBytes, DELETE_TASK_ID, decoded));
    EXPECT_TRUE(decoded.ranges[0].finished);

    std::string currentRingBytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, "", currentRingBytes));
    EXPECT_EQ(currentRingBytes, originalRingBytes);
}

TEST(TopologyRepositoryTest, RetriesCasConflictWhenReportingProgress)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(MakeMigrateTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));
    store.InjectCasConflictsForTest(1);

    TaskProgressUpdate update;
    update.taskId = MIGRATE_TASK_ID;
    update.nodeId = WORKER_B;
    update.range = { 1, 10, WORKER_B, true };
    DS_ASSERT_OK(repo.ReportTransferProgress(update.taskId, update));

    std::string updatedTaskBytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, key, updatedTaskBytes));
    TransferTaskRecord decoded;
    DS_ASSERT_OK(codec.DecodeMigrateTask(updatedTaskBytes, MIGRATE_TASK_ID, decoded));
    EXPECT_TRUE(decoded.ranges[0].finished);
}

TEST(TopologyRepositoryTest, ReturnsTryAgainWhenCasConflictRetryIsExhausted)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(MakeMigrateTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));
    store.InjectCasConflictsForTest(10);

    TaskProgressUpdate update;
    update.taskId = MIGRATE_TASK_ID;
    update.nodeId = WORKER_B;
    update.range = { 1, 10, WORKER_B, true };
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_TRY_AGAIN);
}

TEST(TopologyRepositoryTest, RejectsInvalidProgressUpdates)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    std::string migrateKey;
    std::string deleteKey;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(MakeMigrateTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, migrateKey));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, migrateKey, bytes));
    DS_ASSERT_OK(codec.EncodeDeleteNodeTask(MakeDeleteNodeTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey(DELETE_TASK_ID, deleteKey));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, deleteKey, bytes));

    TaskProgressUpdate update;
    update.taskId = MIGRATE_TASK_ID;
    update.nodeId = WORKER_A;
    update.range = { 1, 10, WORKER_B, true };
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_INVALID);

    update.nodeId = WORKER_B;
    update.range.finished = false;
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_INVALID);

    update.range.finished = true;
    update.taskId = DELETE_TASK_ID;
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_NOT_FOUND);

    update.taskId = MIGRATE_TASK_ID;
    update.range = { 2, 10, WORKER_B, true };
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_INVALID);

    TaskProgressUpdate recoveryUpdate;
    recoveryUpdate.taskId = MIGRATE_TASK_ID;
    recoveryUpdate.nodeId = WORKER_A;
    recoveryUpdate.range = { 1, 10, WORKER_B, true };
    EXPECT_EQ(repo.ReportRecoveryProgress(recoveryUpdate.taskId, recoveryUpdate).GetCode(), K_NOT_FOUND);

    recoveryUpdate.taskId = DELETE_TASK_ID;
    recoveryUpdate.range = { 101, 200, WORKER_A, true };
    EXPECT_EQ(repo.ReportRecoveryProgress(recoveryUpdate.taskId, recoveryUpdate).GetCode(), K_INVALID);

    update = {};
    update.taskId = MIGRATE_TASK_ID;
    update.nodeId = WORKER_B;
    update.range = { 1, 10, WORKER_B, true };
    EXPECT_EQ(repo.ReportTransferProgress("migrate-other-task", update).GetCode(), K_INVALID);

    recoveryUpdate.taskId = DELETE_TASK_ID;
    recoveryUpdate.nodeId = WORKER_A;
    recoveryUpdate.range = { 100, 200, WORKER_A, true };
    EXPECT_EQ(repo.ReportRecoveryProgress("delete-other-task", recoveryUpdate).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, RejectsMalformedRingAndTaskSubkeys)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, "", "not-a-protobuf"));
    TopologyRepository repo(store);

    TopologyDescriptor topology;
    Revision revision = 0;
    EXPECT_EQ(repo.GetCommittedTopology(topology, revision).GetCode(), K_INVALID);

    std::string key;
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, "not-a-task"));
    std::vector<TransferTaskRecord> tasks;
    EXPECT_EQ(repo.ListTransferTaskRecords({}, tasks).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, ListsLargeScaleTransferTasksWithinTwoSeconds)
{
    constexpr int TASK_NUM = 500;
    FakeCoordinationBackend store;
    TopologyRepositoryCodec codec;
    for (int i = 0; i < TASK_NUM; ++i) {
        auto task = MakeMigrateTaskRecord();
        task.taskId = "migrate-change-large-" + std::to_string(i);
        task.sourceNodeId = "worker-" + std::to_string(i);
        task.executorNodeId = task.sourceNodeId;
        task.targetNodeId = "worker-new-" + std::to_string(i);
        task.ranges = { { static_cast<uint32_t>(i), static_cast<uint32_t>(i + 1), task.sourceNodeId, false } };
        std::string key;
        std::string bytes;
        DS_ASSERT_OK(codec.EncodeMigrateTask(task, bytes));
        DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(task.taskId, key));
        DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));
    }
    TopologyRepository repo(store);

    auto begin = std::chrono::steady_clock::now();
    std::vector<TransferTaskRecord> transferTasks;
    DS_ASSERT_OK(repo.ListTransferTaskRecords({}, transferTasks));
    auto costMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);

    EXPECT_EQ(transferTasks.size(), static_cast<size_t>(TASK_NUM));
    EXPECT_LT(costMs.count(), 2000);
}

TEST(TopologyRepositoryTest, DecodesCommittedTopologyEventAndIgnoresUnrelatedEvent)
{
    auto ring = MakeRing();
    FakeCoordinationBackend store;
    TopologyRepository repo(store);

    CoordinationEvent event;
    event.type = CoordinationEventType::PUT;
    event.key = "/datasystem/ring/";
    event.value = SerializeRing(ring);
    event.revision = 7;

    TopologyWatchEvent typed;
    DS_ASSERT_OK(repo.HandleCommittedTopologyEvent(event, typed));
    EXPECT_EQ(typed.type, TopologyWatchEventType::UPDATED);
    EXPECT_EQ(typed.revision, 7);
    EXPECT_EQ(typed.topology.version, 5);

    event.key = "/datasystem/cluster/127.0.0.1:7001";
    EXPECT_EQ(repo.HandleCommittedTopologyEvent(event, typed).GetCode(), K_NOT_FOUND);
}

TEST(TopologyRepositoryTest, HandlesDeletedCommittedTopologyEvent)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);

    CoordinationEvent event;
    event.type = CoordinationEventType::DELETE;
    event.key = "/datasystem/ring";
    event.revision = 8;

    TopologyWatchEvent typed;
    DS_ASSERT_OK(repo.HandleCommittedTopologyEvent(event, typed));
    EXPECT_EQ(typed.type, TopologyWatchEventType::DELETED);
    EXPECT_EQ(typed.revision, 8);
}

TEST(TopologyRepositoryTest, FillsProgressTaskIdFromApiArgument)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    TopologyRepositoryCodec codec;
    std::string key;
    std::string bytes;
    DS_ASSERT_OK(codec.EncodeMigrateTask(MakeMigrateTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildMigrateTaskKey(MIGRATE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));

    TaskProgressUpdate transferUpdate;
    transferUpdate.nodeId = WORKER_B;
    transferUpdate.range = { 1, 10, WORKER_B, true };
    DS_ASSERT_OK(repo.ReportTransferProgress(MIGRATE_TASK_ID, transferUpdate));

    DS_ASSERT_OK(codec.EncodeDeleteNodeTask(MakeDeleteNodeTaskRecord(), bytes));
    DS_ASSERT_OK(TopologyKeyHelper::BuildDeleteNodeTaskKey(DELETE_TASK_ID, key));
    DS_ASSERT_OK(store.PutForTest(COORDINATION_HASHRING_TABLE, key, bytes));

    TaskProgressUpdate recoveryUpdate;
    recoveryUpdate.nodeId = WORKER_A;
    recoveryUpdate.range = { 100, 200, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportRecoveryProgress(DELETE_TASK_ID, recoveryUpdate));
}

TEST(TopologyRepositoryTest, TryCreateTransferTaskRecordIsCreateOnly)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    auto task = MakeMigrateTaskRecord();

    Revision createdRevision = 0;
    DS_ASSERT_OK(repo.TryCreateTransferTaskRecord(task, createdRevision));
    EXPECT_GT(createdRevision, 0);

    auto conflicting = task;
    conflicting.targetNodeId = WORKER_C;
    Revision ignoredRevision = 0;
    EXPECT_EQ(repo.TryCreateTransferTaskRecord(conflicting, ignoredRevision).GetCode(), K_TRY_AGAIN);

    std::vector<TransferTaskRecord> tasks;
    DS_ASSERT_OK(repo.ListTransferTaskRecords({}, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_EQ(tasks[0].taskId, task.taskId);
    EXPECT_EQ(tasks[0].targetNodeId, task.targetNodeId);
}

TEST(TopologyRepositoryTest, TryCreateRecoveryTaskRecordIsCreateOnly)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    auto task = MakeDeleteNodeTaskRecord();

    Revision createdRevision = 0;
    DS_ASSERT_OK(repo.TryCreateRecoveryTaskRecord(task, createdRevision));
    EXPECT_GT(createdRevision, 0);

    auto conflicting = task;
    conflicting.failedNodeId = WORKER_B;
    Revision ignoredRevision = 0;
    EXPECT_EQ(repo.TryCreateRecoveryTaskRecord(conflicting, ignoredRevision).GetCode(), K_TRY_AGAIN);

    std::vector<RecoveryTaskRecord> tasks;
    DS_ASSERT_OK(repo.ListRecoveryTaskRecords({}, tasks));
    ASSERT_EQ(tasks.size(), 1ul);
    EXPECT_EQ(tasks[0].taskId, task.taskId);
    EXPECT_EQ(tasks[0].recoveryNodeId, task.recoveryNodeId);
}

TEST(TopologyRepositoryTest, UpsertTaskNotifyOverwritesDoorbell)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);

    TaskNotify first;
    first.nodeAddress = WORKER_A;
    first.type = TaskNotifyType::SCALE_OUT;
    first.taskIds = { MIGRATE_TASK_ID };
    Revision firstRevision = 0;
    DS_ASSERT_OK(repo.UpsertTaskNotify(first, firstRevision));
    EXPECT_GT(firstRevision, 0);

    constexpr char NEXT_MIGRATE_TASK_ID[] = "migrate-change-2-127.0.0.1:7002-127.0.0.1:7001";
    TaskNotify second;
    second.nodeAddress = WORKER_A;
    second.type = TaskNotifyType::SCALE_OUT;
    second.taskIds = { NEXT_MIGRATE_TASK_ID };
    Revision secondRevision = 0;
    DS_ASSERT_OK(repo.UpsertTaskNotify(second, secondRevision));
    EXPECT_GT(secondRevision, firstRevision);

    std::string key;
    DS_ASSERT_OK(TopologyKeyHelper::BuildNotifyKey(WORKER_A, key));
    std::string bytes;
    DS_ASSERT_OK(store.Get(COORDINATION_HASHRING_TABLE, key, bytes));
    TopologyRepositoryCodec codec;
    TaskNotify decoded;
    DS_ASSERT_OK(codec.DecodeNotify(bytes, WORKER_A, decoded));
    EXPECT_EQ(decoded.nodeAddress, WORKER_A);
    ASSERT_EQ(decoded.taskIds.size(), 1ul);
    EXPECT_EQ(decoded.taskIds[0], NEXT_MIGRATE_TASK_ID);
}

TEST(TopologyRepositoryTest, UpsertTaskNotifySkipsIdenticalDoorbellWrite)
{
    FakeCoordinationBackend store;
    std::vector<CoordinationEvent> events;
    store.SetEventHandler([&events](CoordinationEvent event) { events.emplace_back(std::move(event)); });
    TopologyRepository repo(store);

    TaskNotify notify;
    notify.nodeAddress = WORKER_A;
    notify.type = TaskNotifyType::SCALE_OUT;
    notify.taskIds = { MIGRATE_TASK_ID };
    Revision firstRevision = 0;
    DS_ASSERT_OK(repo.UpsertTaskNotify(notify, firstRevision));
    EXPECT_GT(firstRevision, 0);
    ASSERT_EQ(events.size(), 1ul);

    Revision secondRevision = 0;
    DS_ASSERT_OK(repo.UpsertTaskNotify(notify, secondRevision));
    EXPECT_EQ(secondRevision, firstRevision);
    EXPECT_EQ(events.size(), 1ul);
}

TEST(TopologyRepositoryTest, UpsertTaskNotifyToleratesDoorbellDeletedAfterWrite)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);

    TaskNotify notify;
    notify.nodeAddress = WORKER_A;
    notify.type = TaskNotifyType::SCALE_OUT;
    notify.taskIds = { MIGRATE_TASK_ID };

    std::string key;
    DS_ASSERT_OK(TopologyKeyHelper::BuildNotifyKey(WORKER_A, key));
    store.DeleteKeyAfterNextCasWriteForTest(COORDINATION_HASHRING_TABLE, key);

    Revision revision = 0;
    DS_ASSERT_OK(repo.UpsertTaskNotify(notify, revision));
    EXPECT_EQ(revision, 0);

    std::string bytes;
    EXPECT_EQ(store.Get(COORDINATION_HASHRING_TABLE, key, bytes).GetCode(), K_NOT_FOUND);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
