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
#include "datasystem/common/topology/repository/topology_repository.h"

#include <chrono>
#include <string>

#include "gtest/gtest.h"

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/common/topology/repository/topology_key_helper.h"
#include "datasystem/common/topology/repository/topology_repository_codec.h"
#include "tests/ut/common.h"
#include "tests/ut/common/topology/fake_cluster_store.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char WORKER_A[] = "127.0.0.1:7001";
constexpr char WORKER_B[] = "127.0.0.1:7002";
constexpr char WORKER_C[] = "127.0.0.1:7003";
constexpr char WORKER_D[] = "127.0.0.1:7004";

std::string SerializeRing(const HashRingPb &ring)
{
    std::string bytes;
    EXPECT_TRUE(ring.SerializeToString(&bytes));
    return bytes;
}

HashRingPb MakeRing()
{
    HashRingPb ring;
    ring.set_cluster_has_init(true);
    auto &workerA = (*ring.mutable_workers())[WORKER_A];
    workerA.set_state(WorkerPb::ACTIVE);
    workerA.add_hash_tokens(10);
    auto &workerB = (*ring.mutable_workers())[WORKER_B];
    workerB.set_state(WorkerPb::ACTIVE);
    workerB.add_hash_tokens(20);
    return ring;
}

void AddRange(ChangeNodePb &change, const std::string &worker, uint32_t from, uint32_t end, bool finished = false)
{
    auto *range = change.add_changed_ranges();
    range->set_workerid(worker);
    range->set_from(from);
    range->set_end(end);
    range->set_finished(finished);
}

TEST(TopologyRepositoryTest, ReadsCommittedTopologyFromClusterStoreRing)
{
    FakeClusterStore store;
    auto ring = MakeRing();
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", SerializeRing(ring)));
    TopologyRepository repo(store);

    TopologyDescriptor topology;
    Revision revision = 0;
    DS_ASSERT_OK(repo.GetCommittedTopology(topology, revision));

    EXPECT_GT(revision, 0);
    EXPECT_EQ(topology.version, revision);
    EXPECT_TRUE(topology.clusterHasInit);
    ASSERT_EQ(topology.workers.size(), 2ul);
    EXPECT_EQ(topology.workers[0].workerId, WORKER_A);
    EXPECT_EQ(topology.workers[0].state, WorkerTopologyState::ACTIVE);
}

TEST(TopologyRepositoryTest, EncodesAndDecodesTopologyStates)
{
    TopologyRepositoryCodec codec;
    TopologyDescriptor topology;
    topology.clusterHasInit = false;
    topology.workers = {
        { WORKER_D, WorkerTopologyState::ACTIVE, { 4 } },
        { WORKER_A, WorkerTopologyState::INITIAL, { 1 } },
        { WORKER_C, WorkerTopologyState::LEAVING, { 3 } },
        { WORKER_B, WorkerTopologyState::JOINING, { 2 } },
    };

    std::string bytes;
    DS_ASSERT_OK(codec.EncodeTopology(topology, bytes));

    TopologyDescriptor decoded;
    DS_ASSERT_OK(codec.DecodeTopology(bytes, decoded));
    ASSERT_EQ(decoded.workers.size(), 4ul);
    EXPECT_FALSE(decoded.clusterHasInit);
    EXPECT_EQ(decoded.workers[0].state, WorkerTopologyState::INITIAL);
    EXPECT_EQ(decoded.workers[1].state, WorkerTopologyState::JOINING);
    EXPECT_EQ(decoded.workers[2].state, WorkerTopologyState::LEAVING);
    EXPECT_EQ(decoded.workers[3].state, WorkerTopologyState::ACTIVE);
}

TEST(TopologyRepositoryTest, RejectsInvalidTopologyCodecInputs)
{
    TopologyRepositoryCodec codec;
    TopologyDescriptor topology;
    std::string bytes;
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.version = -1;
    topology.workers = { { WORKER_A, WorkerTopologyState::ACTIVE, { 1 } } };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.version = 0;
    topology.workers = { { WORKER_A, static_cast<WorkerTopologyState>(99), { 1 } } };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.workers = { { "bad/id", WorkerTopologyState::ACTIVE, { 1 } } };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.workers = { { "", WorkerTopologyState::ACTIVE, { 1 } } };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    topology.workers = { { "..", WorkerTopologyState::ACTIVE, { 1 } } };
    EXPECT_EQ(codec.EncodeTopology(topology, bytes).GetCode(), K_INVALID);

    HashRingPb ring = MakeRing();
    (*ring.mutable_workers())[WORKER_A].set_state(static_cast<WorkerPb::StatePb>(99));
    TopologyDescriptor decoded;
    EXPECT_EQ(codec.DecodeTopology(SerializeRing(ring), decoded).GetCode(), K_INVALID);

    ring.Clear();
    EXPECT_EQ(codec.DecodeTopology(SerializeRing(ring), decoded).GetCode(), K_INVALID);

    ring = MakeRing();
    (*ring.mutable_add_node_info())["bad/target"];
    std::vector<TransferTaskRecord> transferTasks;
    EXPECT_EQ(codec.DecodeTransferTasksFromRing(SerializeRing(ring), transferTasks).GetCode(), K_INVALID);

    ring = MakeRing();
    (*ring.mutable_del_node_info())["bad/failed"];
    std::vector<RecoveryTaskRecord> recoveryTasks;
    EXPECT_EQ(codec.DecodeRecoveryTasksFromRing(SerializeRing(ring), recoveryTasks).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, RejectsInvalidProgressCodecInputs)
{
    TopologyRepositoryCodec codec;
    auto ring = MakeRing();
    AddRange((*ring.mutable_add_node_info())[WORKER_C], WORKER_A, 1, 10);
    AddRange((*ring.mutable_del_node_info())[WORKER_B], WORKER_A, 20, 30);
    auto bytes = SerializeRing(ring);
    std::string nextBytes;

    TaskProgressUpdate update;
    update.workerId = WORKER_A;
    update.range = { 1, 10, WORKER_A, true };
    EXPECT_EQ(codec.ApplyTransferProgressToRing(bytes, update, nextBytes).GetCode(), K_INVALID);

    update.taskId = "bad-task-id";
    EXPECT_EQ(codec.ApplyTransferProgressToRing(bytes, update, nextBytes).GetCode(), K_INVALID);

    update.taskId = std::string(WORKER_C) + "|" + WORKER_A;
    update.workerId = "bad/id";
    EXPECT_EQ(codec.ApplyTransferProgressToRing(bytes, update, nextBytes).GetCode(), K_INVALID);

    update.workerId = WORKER_A;
    update.range = { 10, 10, WORKER_A, true };
    EXPECT_EQ(codec.ApplyTransferProgressToRing(bytes, update, nextBytes).GetCode(), K_INVALID);

    update.range = { 1, 10, "", true };
    EXPECT_EQ(codec.ApplyTransferProgressToRing(bytes, update, nextBytes).GetCode(), K_INVALID);

    update.range = { 1, 10, std::string("bad") + static_cast<char>(1), true };
    EXPECT_EQ(codec.ApplyTransferProgressToRing(bytes, update, nextBytes).GetCode(), K_INVALID);

    update.taskId = std::string(WORKER_B) + "|" + WORKER_A;
    update.workerId = WORKER_B;
    update.range = { 20, 30, WORKER_A, true };
    EXPECT_EQ(codec.ApplyRecoveryProgressToRing(bytes, update, nextBytes).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, ParsesCommittedTopologyKeys)
{
    EXPECT_EQ(TopologyKeyHelper::CommittedTopologyKey(), "/datasystem/ring");

    TopologyKeyParts parts;
    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/ring", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::COMMITTED_TOPOLOGY);

    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/ring/", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::COMMITTED_TOPOLOGY);

    DS_ASSERT_OK(TopologyKeyHelper::Parse("/datasystem/ring/migrate_tasks/task", parts));
    EXPECT_EQ(parts.type, TopologyKeyType::UNRELATED);
}

TEST(TopologyRepositoryTest, ListsTransferAndRecoveryTasksFromRing)
{
    FakeClusterStore store;
    auto ring = MakeRing();
    auto &addInfo = (*ring.mutable_add_node_info())[WORKER_C];
    AddRange(addInfo, WORKER_A, 1, 10);
    AddRange(addInfo, WORKER_B, 10, 20, true);
    auto &delInfo = (*ring.mutable_del_node_info())[WORKER_B];
    AddRange(delInfo, WORKER_A, 20, 30);
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", SerializeRing(ring)));
    TopologyRepository repo(store);

    std::vector<TransferTaskRecord> transferTasks;
    TaskFilter filter;
    filter.workerId = WORKER_A;
    filter.unfinishedOnly = true;
    DS_ASSERT_OK(repo.ListTransferTaskRecords(filter, transferTasks));
    ASSERT_EQ(transferTasks.size(), 1ul);
    EXPECT_EQ(transferTasks[0].taskId, std::string(WORKER_C) + "|" + WORKER_A);
    EXPECT_GT(transferTasks[0].ringRevision, 0);
    EXPECT_EQ(transferTasks[0].targetWorkerId, WORKER_C);
    EXPECT_EQ(transferTasks[0].sourceWorkerId, WORKER_A);
    ASSERT_EQ(transferTasks[0].ranges.size(), 1ul);
    EXPECT_EQ(transferTasks[0].ranges[0].workerId, WORKER_A);

    std::vector<RecoveryTaskRecord> recoveryTasks;
    DS_ASSERT_OK(repo.ListRecoveryTaskRecords(filter, recoveryTasks));
    ASSERT_EQ(recoveryTasks.size(), 1ul);
    EXPECT_EQ(recoveryTasks[0].taskId, std::string(WORKER_B) + "|" + WORKER_A);
    EXPECT_EQ(recoveryTasks[0].failedWorkerId, WORKER_B);
    EXPECT_EQ(recoveryTasks[0].recoveryWorkerId, WORKER_A);

    filter = {};
    DS_ASSERT_OK(repo.ListTransferTaskRecords(filter, transferTasks));
    ASSERT_EQ(transferTasks.size(), 2ul);
}

TEST(TopologyRepositoryTest, ReportsProgressByCasUpdatingRing)
{
    FakeClusterStore store;
    auto ring = MakeRing();
    auto &addInfo = (*ring.mutable_add_node_info())[WORKER_C];
    AddRange(addInfo, WORKER_A, 1, 10);
    auto &delInfo = (*ring.mutable_del_node_info())[WORKER_B];
    AddRange(delInfo, WORKER_A, 20, 30);
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", SerializeRing(ring)));
    TopologyRepository repo(store);

    TaskProgressUpdate transferUpdate;
    transferUpdate.taskId = std::string(WORKER_C) + "|" + WORKER_A;
    transferUpdate.workerId = WORKER_A;
    transferUpdate.range = { 1, 10, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportTransferProgress(transferUpdate.taskId, transferUpdate));

    TaskProgressUpdate recoveryUpdate;
    recoveryUpdate.taskId = std::string(WORKER_B) + "|" + WORKER_A;
    recoveryUpdate.workerId = WORKER_A;
    recoveryUpdate.range = { 20, 30, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportRecoveryProgress(recoveryUpdate.taskId, recoveryUpdate));

    std::string bytes;
    DS_ASSERT_OK(store.Get(ETCD_RING_PREFIX, "", bytes));
    HashRingPb updated;
    ASSERT_TRUE(updated.ParseFromString(bytes));
    EXPECT_TRUE(updated.add_node_info().at(WORKER_C).changed_ranges(0).finished());
    EXPECT_TRUE(updated.del_node_info().at(WORKER_B).changed_ranges(0).finished());
}

TEST(TopologyRepositoryTest, RetriesCasConflictWhenReportingProgress)
{
    FakeClusterStore store;
    auto ring = MakeRing();
    auto &addInfo = (*ring.mutable_add_node_info())[WORKER_C];
    AddRange(addInfo, WORKER_A, 1, 10);
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", SerializeRing(ring)));
    store.InjectCasConflictsForTest(1);
    TopologyRepository repo(store);

    TaskProgressUpdate update;
    update.taskId = std::string(WORKER_C) + "|" + WORKER_A;
    update.workerId = WORKER_A;
    update.range = { 1, 10, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportTransferProgress(update.taskId, update));

    std::string bytes;
    DS_ASSERT_OK(store.Get(ETCD_RING_PREFIX, "", bytes));
    HashRingPb updated;
    ASSERT_TRUE(updated.ParseFromString(bytes));
    EXPECT_TRUE(updated.add_node_info().at(WORKER_C).changed_ranges(0).finished());
}

TEST(TopologyRepositoryTest, ReturnsTryAgainWhenCasConflictRetryIsExhausted)
{
    FakeClusterStore store;
    auto ring = MakeRing();
    auto &addInfo = (*ring.mutable_add_node_info())[WORKER_C];
    AddRange(addInfo, WORKER_A, 1, 10);
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", SerializeRing(ring)));
    store.InjectCasConflictsForTest(10);
    TopologyRepository repo(store);

    TaskProgressUpdate update;
    update.taskId = std::string(WORKER_C) + "|" + WORKER_A;
    update.workerId = WORKER_A;
    update.range = { 1, 10, WORKER_A, true };
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_TRY_AGAIN);
}

TEST(TopologyRepositoryTest, RejectsInvalidProgressUpdates)
{
    FakeClusterStore store;
    auto ring = MakeRing();
    auto &addInfo = (*ring.mutable_add_node_info())[WORKER_C];
    AddRange(addInfo, WORKER_A, 1, 10);
    auto &delInfo = (*ring.mutable_del_node_info())[WORKER_B];
    AddRange(delInfo, WORKER_A, 20, 30);
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", SerializeRing(ring)));
    TopologyRepository repo(store);

    TaskProgressUpdate update;
    update.taskId = std::string(WORKER_C) + "|" + WORKER_A;
    update.workerId = WORKER_B;
    update.range = { 1, 10, WORKER_A, true };
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_INVALID);

    update.workerId = WORKER_A;
    update.range.finished = false;
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_INVALID);

    update.range.finished = true;
    update.taskId = std::string(WORKER_B) + "|" + WORKER_A;
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_NOT_FOUND);

    update.taskId = std::string(WORKER_C) + "|" + WORKER_A;
    update.range = { 2, 10, WORKER_A, true };
    EXPECT_EQ(repo.ReportTransferProgress(update.taskId, update).GetCode(), K_INVALID);

    TaskProgressUpdate recoveryUpdate;
    recoveryUpdate.taskId = std::string(WORKER_C) + "|" + WORKER_A;
    recoveryUpdate.workerId = WORKER_A;
    recoveryUpdate.range = { 1, 10, WORKER_A, true };
    EXPECT_EQ(repo.ReportRecoveryProgress(recoveryUpdate.taskId, recoveryUpdate).GetCode(), K_NOT_FOUND);

    recoveryUpdate.taskId = std::string(WORKER_B) + "|" + WORKER_A;
    recoveryUpdate.range = { 21, 30, WORKER_A, true };
    EXPECT_EQ(repo.ReportRecoveryProgress(recoveryUpdate.taskId, recoveryUpdate).GetCode(), K_INVALID);

    update = {};
    update.taskId = std::string(WORKER_C) + "|" + WORKER_A;
    update.workerId = WORKER_A;
    update.range = { 1, 10, WORKER_A, true };
    EXPECT_EQ(repo.ReportTransferProgress(std::string(WORKER_C) + "|" + WORKER_B, update).GetCode(), K_INVALID);

    recoveryUpdate.taskId = std::string(WORKER_B) + "|" + WORKER_A;
    recoveryUpdate.workerId = WORKER_A;
    recoveryUpdate.range = { 20, 30, WORKER_A, true };
    EXPECT_EQ(repo.ReportRecoveryProgress(std::string(WORKER_B) + "|" + WORKER_C, recoveryUpdate).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, RejectsMalformedRing)
{
    FakeClusterStore store;
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", "not-a-protobuf"));
    TopologyRepository repo(store);

    TopologyDescriptor topology;
    Revision revision = 0;
    EXPECT_EQ(repo.GetCommittedTopology(topology, revision).GetCode(), K_INVALID);

    std::vector<TransferTaskRecord> tasks;
    EXPECT_EQ(repo.ListTransferTaskRecords({}, tasks).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryTest, ListsLargeScaleTransferTasksWithinTwoSeconds)
{
    constexpr int EXISTING_WORKER_NUM = 2000;
    constexpr int ADD_WORKER_NUM = 500;

    HashRingPb ring;
    ring.set_cluster_has_init(true);
    for (int i = 0; i < EXISTING_WORKER_NUM; ++i) {
        auto workerId = "worker-" + std::to_string(i);
        auto &worker = (*ring.mutable_workers())[workerId];
        worker.set_state(WorkerPb::ACTIVE);
        worker.add_hash_tokens(static_cast<uint32_t>(i));
    }
    for (int i = 0; i < ADD_WORKER_NUM; ++i) {
        auto targetId = "worker-new-" + std::to_string(i);
        auto sourceId = "worker-" + std::to_string(i);
        auto &worker = (*ring.mutable_workers())[targetId];
        worker.set_state(WorkerPb::JOINING);
        worker.add_hash_tokens(static_cast<uint32_t>(EXISTING_WORKER_NUM + i));
        AddRange((*ring.mutable_add_node_info())[targetId], sourceId, static_cast<uint32_t>(i),
                 static_cast<uint32_t>(i + 1));
    }
    FakeClusterStore store;
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", SerializeRing(ring)));
    TopologyRepository repo(store);

    auto begin = std::chrono::steady_clock::now();
    std::vector<TransferTaskRecord> transferTasks;
    DS_ASSERT_OK(repo.ListTransferTaskRecords({}, transferTasks));
    auto costMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);

    EXPECT_EQ(transferTasks.size(), static_cast<size_t>(ADD_WORKER_NUM));
    EXPECT_LT(costMs.count(), 2000);
}

TEST(TopologyRepositoryTest, DecodesCommittedTopologyEventAndIgnoresUnrelatedEvent)
{
    auto ring = MakeRing();
    FakeClusterStore store;
    TopologyRepository repo(store);

    ClusterStoreEvent event;
    event.type = ClusterStoreEventType::PUT;
    event.key = "/datasystem/ring/";
    event.value = SerializeRing(ring);
    event.revision = 7;

    TopologyWatchEvent typed;
    DS_ASSERT_OK(repo.HandleCommittedTopologyEvent(event, typed));
    EXPECT_EQ(typed.type, TopologyWatchEventType::UPDATED);
    EXPECT_EQ(typed.topology.version, 7);

    event.key = "/datasystem/cluster/127.0.0.1:7001";
    EXPECT_EQ(repo.HandleCommittedTopologyEvent(event, typed).GetCode(), K_NOT_FOUND);
}

TEST(TopologyRepositoryTest, HandlesDeletedCommittedTopologyEvent)
{
    FakeClusterStore store;
    TopologyRepository repo(store);

    ClusterStoreEvent event;
    event.type = ClusterStoreEventType::DELETE;
    event.key = "/datasystem/ring";
    event.revision = 8;

    TopologyWatchEvent typed;
    DS_ASSERT_OK(repo.HandleCommittedTopologyEvent(event, typed));
    EXPECT_EQ(typed.type, TopologyWatchEventType::DELETED);
    EXPECT_EQ(typed.revision, 8);
}

TEST(TopologyRepositoryTest, FillsProgressTaskIdFromApiArgument)
{
    FakeClusterStore store;
    auto ring = MakeRing();
    AddRange((*ring.mutable_add_node_info())[WORKER_C], WORKER_A, 1, 10);
    AddRange((*ring.mutable_del_node_info())[WORKER_B], WORKER_A, 20, 30);
    DS_ASSERT_OK(store.PutForTest(ETCD_RING_PREFIX, "", SerializeRing(ring)));
    TopologyRepository repo(store);

    TaskProgressUpdate transferUpdate;
    transferUpdate.workerId = WORKER_A;
    transferUpdate.range = { 1, 10, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportTransferProgress(std::string(WORKER_C) + "|" + WORKER_A, transferUpdate));

    TaskProgressUpdate recoveryUpdate;
    recoveryUpdate.workerId = WORKER_A;
    recoveryUpdate.range = { 20, 30, WORKER_A, true };
    DS_ASSERT_OK(repo.ReportRecoveryProgress(std::string(WORKER_B) + "|" + WORKER_A, recoveryUpdate));
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
