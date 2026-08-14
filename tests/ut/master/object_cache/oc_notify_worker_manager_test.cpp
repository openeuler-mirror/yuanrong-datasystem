/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Test notify worker manager class.
 */
#include "datasystem/master/object_cache/oc_notify_worker_manager.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/format.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/worker/cluster_event_type.h"

DS_DECLARE_string(rocksdb_store_dir);
DS_DECLARE_string(rocksdb_write_mode);

using namespace ::testing;
using namespace datasystem::master;
namespace datasystem {
namespace master {
class OCNotifyWorkerManagerTest : public ut::CommonTest {
public:
    void SetUp()
    {
        rocksdbWriteMode_ = FLAGS_rocksdb_write_mode;
        FLAGS_rocksdb_write_mode = "sync";
        rocksStore_ = RocksStore::GetInstance(ut::GetTestCaseDataDir() + "/rocksdb");
        objectStore_ = std::make_shared<ObjectMetaStore>(rocksStore_.get(), nullptr);
        objectStore_->Init();
        hostPort_.ParseString("127.0.0.1:30001");
        akSkManager_ = std::make_shared<AkSkManager>(0);
    }

    void TearDown() override
    {
        (void)inject::Clear("master.rocksdb.put");
        (void)inject::Clear("master.rocksdb.delete");
        objectStore_.reset();
        rocksStore_.reset();
        FLAGS_rocksdb_write_mode = rocksdbWriteMode_;
    }

    std::shared_ptr<RocksStore> rocksStore_;
    std::shared_ptr<ObjectMetaStore> objectStore_;
    HostPort hostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::string rocksdbWriteMode_;

    std::vector<AsyncWorkerOpSnapshot> SnapshotWorkerOps(OCNotifyWorkerManager &manager, const std::string &worker)
    {
        return manager.SnapshotAsyncWorkerOps(worker);
    }

    Status ClearSnapshotOps(OCNotifyWorkerManager &manager, const std::string &worker,
                            const std::vector<AsyncWorkerOpSnapshot> &snapshots)
    {
        return manager.ClearAsyncWorkerOpSnapshots(worker, snapshots);
    }

    Status QueueAsyncDelete(
        OCNotifyWorkerManager &manager, std::vector<PendingDeleteNotification> &notifications,
        std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas,
        std::unordered_set<std::string> &failedObjects)
    {
        return manager.AsyncNotifyWorkerDelete(notifications, replicas, failedObjects);
    }

    std::vector<AsyncWorkerOpSnapshot> SelectAcknowledgedDeletes(OCNotifyWorkerManager &manager,
                                                                 const std::vector<AsyncWorkerOpSnapshot> &snapshots,
                                                                 const DeleteObjectRspPb &response)
    {
        return manager.SelectAcknowledgedDeleteSnapshots(snapshots, response);
    }

    NotifyWorkerOp ParseNotifyWorkerOp(const ObjectAsyncOpDetailPb &pb)
    {
        return OCNotifyWorkerManager::ParseNotifyWorkerOpFromMigration(pb);
    }

    std::vector<AsyncWorkerOpSnapshot> SnapshotDeleteBatch(OCNotifyWorkerManager &manager, const std::string &worker)
    {
        return manager.SnapshotAsyncDeleteReplayBatch(worker);
    }
};

TEST_F(OCNotifyWorkerManagerTest, DISABLED_TestAsyncSendUpdateObject)
{
    inject::Set("OCNotifyWorkerManager.CheckWorkerIsHealth.worker.unhealthy", "return(K_WORKER_ABNORMAL)");
    inject::Set("OCNotifyWorkerManager.NoNeedRecoveryMeta", "return(K_OK)");
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    EXPECT_EQ(manager->Init(), Status::OK());
    std::string worker1 = "127.0.0.1:40001";
    std::string worker2 = "127.0.0.1:40002";
    std::string worker3 = "127.0.0.1:40003";
    std::string objectKey = "test0001:127.0.0.1:30001";

    {
        ObjectMeta objectMeta;
        objectMeta.meta.set_object_key(objectKey);
        objectMeta.locations[worker2] = AckState::ACK;
        objectMeta.locations[worker3] = AckState::ACK;
        EXPECT_EQ(manager->AsyncSendUpdateObject(objectKey, worker1, objectMeta), Status::OK());
    }

    {
        ObjectMeta objectMeta;
        objectMeta.meta.set_object_key(objectKey);
        objectMeta.locations[worker1] = AckState::ACK;
        objectMeta.locations[worker2] = AckState::ACK;
        EXPECT_EQ(manager->AsyncSendUpdateObject(objectKey, worker3, objectMeta), Status::OK());
    }

    {
        std::vector<std::pair<std::string, std::string>> result;
        EXPECT_EQ(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, result), Status::OK());
        EXPECT_EQ(result[0].first, worker1 + "_" + objectKey);
        EXPECT_EQ(result[1].first, worker2 + "_" + objectKey);
    }

    manager.reset();
    manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    EXPECT_EQ(manager->Init(), Status::OK());
    {
        ObjectMeta objectMeta;
        objectMeta.meta.set_object_key(objectKey);
        objectMeta.locations[worker1] = AckState::ACK;
        objectMeta.locations[worker3] = AckState::ACK;
        EXPECT_EQ(manager->AsyncSendUpdateObject(objectKey, worker2, objectMeta), Status::OK());
    }

    {
        std::vector<std::pair<std::string, std::string>> result;
        EXPECT_EQ(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, result), Status::OK());
        EXPECT_EQ(result[0].first, worker1 + "_" + objectKey);
        EXPECT_EQ(result[1].first, worker3 + "_" + objectKey);
    }
}

TEST_F(OCNotifyWorkerManagerTest, TestInsertAsyncWorkerOpReleasesTableLockBeforePersistence)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "test_insert_async_worker_op";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };
    constexpr int rocksPutSleepMs = 300;
    constexpr int maxExpectedCheckMs = 100;
    constexpr int pollIntervalMs = 5;
    constexpr int maxPollMs = 1000;

    DS_ASSERT_OK(inject::Set("master.rocksdb.put", FormatString("1*sleep(%d)", rocksPutSleepMs)));
    Status insertRc;
    std::thread insertThread([&] { insertRc = manager->InsertAsyncWorkerOp(worker, objectKey, op); });

    int64_t maxCheckMs = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(maxPollMs);
    while (std::chrono::steady_clock::now() < deadline && inject::GetExecuteCount("master.rocksdb.put") == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
    }
    ASSERT_GT(inject::GetExecuteCount("master.rocksdb.put"), 0U);
    auto start = std::chrono::steady_clock::now();
    bool existsBeforePersistence =
        manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    maxCheckMs = std::max(maxCheckMs, static_cast<int64_t>(elapsed.count()));

    insertThread.join();
    DS_ASSERT_OK(inject::Clear("master.rocksdb.put"));
    DS_ASSERT_OK(insertRc);
    ASSERT_FALSE(existsBeforePersistence);
    ASSERT_TRUE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));
    EXPECT_LT(maxCheckMs, maxExpectedCheckMs);
}

TEST_F(OCNotifyWorkerManagerTest, TestInsertAsyncWorkerOpPersistenceFailureDoesNotPublish)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "failed_insert_must_not_publish";
    auto op = NotifyWorkerOp::Delete(12345);

    DS_ASSERT_OK(inject::Set("master.rocksdb.put", "1*return(K_KVSTORE_ERROR)"));
    EXPECT_EQ(manager->InsertAsyncWorkerOp(worker, objectKey, op).GetCode(), StatusCode::K_KVSTORE_ERROR);
    DS_ASSERT_OK(inject::Clear("master.rocksdb.put"));

    EXPECT_FALSE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::DELETE));
    std::vector<std::pair<std::string, std::string>> persistedOps;
    DS_ASSERT_OK(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, persistedOps));
    EXPECT_TRUE(persistedOps.empty());
}

TEST_F(OCNotifyWorkerManagerTest, TestNoPersistInsertWaitsForPersistedInsertCommit)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "serialize_recovery_insert";
    constexpr int rocksPutSleepMs = 300;
    DS_ASSERT_OK(inject::Set("master.rocksdb.put", FormatString("1*sleep(%d)", rocksPutSleepMs)));

    Status persistedRc;
    std::thread persisted(
        [&] { persistedRc = manager->InsertAsyncWorkerOp(worker, objectKey, NotifyWorkerOp::Delete(1)); });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (inject::GetExecuteCount("master.rocksdb.put") == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    if (inject::GetExecuteCount("master.rocksdb.put") == 0) {
        persisted.join();
        FAIL() << "Persisted insert did not reach the RocksDB barrier";
    }
    auto start = std::chrono::steady_clock::now();
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, NotifyWorkerOp::Delete(2), false));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    persisted.join();
    DS_ASSERT_OK(inject::Clear("master.rocksdb.put"));

    DS_ASSERT_OK(persistedRc);
    EXPECT_GE(elapsed.count(), rocksPutSleepMs / 2);
    auto snapshots = SnapshotWorkerOps(*manager, worker);
    ASSERT_EQ(snapshots.size(), 1U);
    EXPECT_EQ(snapshots.front().op.delObjectVersion, 2U);
}

TEST_F(OCNotifyWorkerManagerTest, TestAsyncDeleteReplayBatchIsBoundedAndFair)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    constexpr size_t objectCount = 2'500;
    for (size_t i = 0; i < objectCount; ++i) {
        DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, FormatString("object-%05d", static_cast<int>(i)),
                                                  NotifyWorkerOp::Delete(i), false));
    }

    auto first = SnapshotDeleteBatch(*manager, worker);
    auto second = SnapshotDeleteBatch(*manager, worker);
    ASSERT_EQ(first.size(), 1'000U);
    ASSERT_EQ(second.size(), 1'000U);
    std::unordered_set<std::string> selected;
    for (const auto &snapshot : first) {
        selected.emplace(snapshot.objectKey);
    }
    for (const auto &snapshot : second) {
        selected.emplace(snapshot.objectKey);
    }
    EXPECT_EQ(selected.size(), 2'000U);
}

TEST_F(OCNotifyWorkerManagerTest, TestAsyncWorkerDeletePreservesObjectVersion)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "async_delete_version";
    constexpr uint64_t objectVersion = 12345;
    constexpr uint32_t writeMode = static_cast<uint32_t>(WriteMode::WRITE_THROUGH_L2_CACHE);
    std::vector<PendingDeleteNotification> notifications = { { objectKey, worker, writeMode, objectVersion } };
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> replicas;
    replicas[worker].emplace(objectKey, std::make_pair(objectVersion, writeMode));
    std::unordered_set<std::string> failedObjects;

    DS_ASSERT_OK(QueueAsyncDelete(*manager, notifications, replicas, failedObjects));

    EXPECT_TRUE(failedObjects.empty());
    EXPECT_TRUE(replicas[worker].empty());
    auto snapshots = SnapshotWorkerOps(*manager, worker);
    ASSERT_EQ(snapshots.size(), 1);
    EXPECT_EQ(snapshots.front().objectKey, objectKey);
    EXPECT_EQ(snapshots.front().op.delObjectVersion, objectVersion);

    manager.reset();
    manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    DS_ASSERT_OK(manager->RecoverCacheInvalidAndRemoveMeta(true));
    snapshots = SnapshotWorkerOps(*manager, worker);
    ASSERT_EQ(snapshots.size(), 1);
    EXPECT_EQ(snapshots.front().op.delObjectVersion, objectVersion);

    constexpr uint64_t newerObjectVersion = objectVersion + 1;
    notifications.front().deleteVersion = newerObjectVersion;
    replicas[worker].emplace(objectKey, std::make_pair(newerObjectVersion, writeMode));
    DS_ASSERT_OK(QueueAsyncDelete(*manager, notifications, replicas, failedObjects));
    snapshots = SnapshotWorkerOps(*manager, worker);
    ASSERT_EQ(snapshots.size(), 1);
    EXPECT_EQ(snapshots.front().op.delObjectVersion, newerObjectVersion);
}

TEST_F(OCNotifyWorkerManagerTest, TestAsyncWorkerDeletePersistenceFailureKeepsReplicaPending)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "async_delete_persistence_failure";
    constexpr uint64_t objectVersion = 12345;
    constexpr uint32_t writeMode = static_cast<uint32_t>(WriteMode::WRITE_THROUGH_L2_CACHE);
    std::vector<PendingDeleteNotification> notifications = { { objectKey, worker, writeMode, objectVersion } };
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> replicas;
    replicas[worker].emplace(objectKey, std::make_pair(objectVersion, writeMode));
    std::unordered_set<std::string> failedObjects;

    DS_ASSERT_OK(inject::Set("master.rocksdb.put", "1*return(K_KVSTORE_ERROR)"));
    EXPECT_EQ(QueueAsyncDelete(*manager, notifications, replicas, failedObjects).GetCode(),
              StatusCode::K_KVSTORE_ERROR);
    DS_ASSERT_OK(inject::Clear("master.rocksdb.put"));

    EXPECT_EQ(failedObjects, std::unordered_set<std::string>{ objectKey });
    EXPECT_EQ(replicas[worker].count(objectKey), 1U);
    EXPECT_FALSE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::DELETE));
}

TEST_F(OCNotifyWorkerManagerTest, TestMigrationAsyncDeleteRoundTripPreservesVersion)
{
    auto deleteOp = NotifyWorkerOp::Delete(12345);
    ObjectAsyncOpDetailPb pb;

    FillNotifyWorkerOpDetailPb(deleteOp, pb);
    auto restored = ParseNotifyWorkerOp(pb);

    EXPECT_EQ(restored.type, NotifyWorkerOpType::DELETE);
    EXPECT_EQ(restored.delObjectVersion, deleteOp.delObjectVersion);
}

TEST_F(OCNotifyWorkerManagerTest, TestAsyncDeleteHasPeriodicConsumer)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "periodic_async_delete";
    const std::string injectPoint = "OCNotifyWorkerManager.ProcessAsyncDeleteNotifyOpImpl";
    auto op = NotifyWorkerOp::Delete(12345);
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, op));
    manager->SetFaultWorker(worker, true);
    DS_ASSERT_OK(inject::Set(injectPoint, "call()"));
    DS_ASSERT_OK(manager->Init());

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline && inject::GetExecuteCount(injectPoint) == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    EXPECT_GT(inject::GetExecuteCount(injectPoint), 0U);
    DS_ASSERT_OK(inject::Clear(injectPoint));
}

TEST_F(OCNotifyWorkerManagerTest, TestSnapshotClearKeepsNewerAsyncWorkerOp)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string clearedObjectKey = "snapshot_clear_old_op";
    const std::string newerObjectKey = "snapshot_clear_newer_op";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };

    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, clearedObjectKey, op));
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, newerObjectKey, op));
    auto snapshots = SnapshotWorkerOps(*manager, worker);

    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, newerObjectKey, op));
    DS_ASSERT_OK(ClearSnapshotOps(*manager, worker, snapshots));

    ASSERT_FALSE(manager->CheckExistAsyncWorkerOp(worker, clearedObjectKey, NotifyWorkerOpType::CACHE_INVALID));
    ASSERT_TRUE(manager->CheckExistAsyncWorkerOp(worker, newerObjectKey, NotifyWorkerOpType::CACHE_INVALID));
}

TEST_F(OCNotifyWorkerManagerTest, TestDeleteSnapshotClearKeepsNewerVersion)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "delete_snapshot_newer_version";
    auto oldDelete = NotifyWorkerOp::Delete(12345);
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, oldDelete));
    auto snapshots = SnapshotWorkerOps(*manager, worker);

    auto newDelete = NotifyWorkerOp::Delete(12346);
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, newDelete));
    DS_ASSERT_OK(ClearSnapshotOps(*manager, worker, snapshots));

    snapshots = SnapshotWorkerOps(*manager, worker);
    ASSERT_EQ(snapshots.size(), 1U);
    EXPECT_EQ(snapshots.front().op.delObjectVersion, newDelete.delObjectVersion);
}

TEST_F(OCNotifyWorkerManagerTest, TestAsyncWorkerDeleteMergeKeepsLargestVersion)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "delete_merge_largest_version";
    constexpr uint64_t newerDeleteVersion = 12346;
    constexpr uint64_t olderDeleteVersion = 12345;

    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, NotifyWorkerOp::Delete(newerDeleteVersion)));
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, NotifyWorkerOp::Delete(olderDeleteVersion)));

    auto snapshots = SnapshotWorkerOps(*manager, worker);
    ASSERT_EQ(snapshots.size(), 1U);
    EXPECT_EQ(snapshots.front().op.delObjectVersion, newerDeleteVersion);

    manager.reset();
    manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    DS_ASSERT_OK(manager->RecoverCacheInvalidAndRemoveMeta(true));
    snapshots = SnapshotWorkerOps(*manager, worker);
    ASSERT_EQ(snapshots.size(), 1U);
    EXPECT_EQ(snapshots.front().op.delObjectVersion, newerDeleteVersion);
}

TEST_F(OCNotifyWorkerManagerTest, TestAsyncDeleteAcknowledgesOnlySuccessfulObjects)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    auto op = NotifyWorkerOp::Delete(0);
    std::vector<AsyncWorkerOpSnapshot> snapshots = { { "success", op, 1 }, { "failed", op, 2 } };
    DeleteObjectRspPb response;
    response.add_failed_object_keys("failed");
    response.mutable_last_rc()->set_error_code(StatusCode::K_WORKER_TIMEOUT);

    auto acknowledged = SelectAcknowledgedDeletes(*manager, snapshots, response);
    ASSERT_EQ(acknowledged.size(), 1U);
    EXPECT_EQ(acknowledged.front().objectKey, "success");

    response.clear_failed_object_keys();
    acknowledged = SelectAcknowledgedDeletes(*manager, snapshots, response);
    EXPECT_TRUE(acknowledged.empty());
}

TEST_F(OCNotifyWorkerManagerTest, TestClearAsyncWorkerOpKeepsRetryStateOnPersistenceFailure)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "clear_persistence_failure";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };

    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, op));
    DS_ASSERT_OK(inject::Set("master.rocksdb.delete", "1*return(K_KVSTORE_ERROR)"));

    EXPECT_EQ(manager->ClearAsyncWorkerOp(worker).GetCode(), StatusCode::K_KVSTORE_ERROR);
    EXPECT_TRUE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));
    std::vector<std::pair<std::string, std::string>> persistedOps;
    DS_ASSERT_OK(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, persistedOps));
    EXPECT_EQ(persistedOps.size(), 1);

    DS_ASSERT_OK(inject::Clear("master.rocksdb.delete"));
    DS_ASSERT_OK(manager->ClearAsyncWorkerOp(worker));
    EXPECT_FALSE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));
    persistedOps.clear();
    DS_ASSERT_OK(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, persistedOps));
    EXPECT_TRUE(persistedOps.empty());
}

TEST_F(OCNotifyWorkerManagerTest, TestDeadWorkerRejectsDelayedAsyncWorkerOp)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "dead_worker_delayed_op";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };

    manager->SetFaultWorker(worker, true);
    DS_ASSERT_OK(manager->ClearAsyncWorkerOp(worker));
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, op));

    EXPECT_FALSE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));
    std::vector<std::pair<std::string, std::string>> persistedOps;
    DS_ASSERT_OK(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, persistedOps));
    EXPECT_TRUE(persistedOps.empty());
}

TEST_F(OCNotifyWorkerManagerTest, TestTransientFaultWorkerRetainsAsyncWorkerOp)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "transient_fault_worker_op";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };

    manager->SetFaultWorker(worker, false);
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, op));

    EXPECT_TRUE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));
}

TEST_F(OCNotifyWorkerManagerTest, TestFaultWorkerCanOnlyUpgradeToDead)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "upgraded_dead_worker_op";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };

    manager->SetFaultWorker(worker, false);
    manager->SetFaultWorker(worker, true);
    manager->SetFaultWorker(worker, false);
    DS_ASSERT_OK(manager->InsertAsyncWorkerOp(worker, objectKey, op));

    EXPECT_FALSE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));
}

TEST_F(OCNotifyWorkerManagerTest, TestRemoveDeadWorkerEventClearsFaultWorker)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";

    DS_ASSERT_OK(manager->Init());
    manager->SetFaultWorker(worker, true);
    EXPECT_TRUE(manager->CheckWorkerIsHealthy(worker).IsError());

    RemoveDeadWorkerEvent::GetInstance().NotifyAll(worker);

    EXPECT_TRUE(manager->CheckWorkerIsHealthy(worker).IsOk());
}

TEST_F(OCNotifyWorkerManagerTest, TestDeadWorkerClearsConcurrentAsyncWorkerOp)
{
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, nullptr);
    const std::string worker = "127.0.0.1:40001";
    const std::string objectKey = "concurrent_dead_worker_op";
    NotifyWorkerOp op = { .type = NotifyWorkerOpType::CACHE_INVALID };
    constexpr int rocksPutSleepMs = 300;
    constexpr int pollIntervalMs = 5;
    constexpr int maxPollMs = 1000;

    DS_ASSERT_OK(inject::Set("master.rocksdb.put", FormatString("1*sleep(%d)", rocksPutSleepMs)));
    Status insertRc;
    std::thread insertThread([&] { insertRc = manager->InsertAsyncWorkerOp(worker, objectKey, op); });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(maxPollMs);
    while (std::chrono::steady_clock::now() < deadline && inject::GetExecuteCount("master.rocksdb.put") == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
    }
    ASSERT_GT(inject::GetExecuteCount("master.rocksdb.put"), 0U);

    manager->SetFaultWorker(worker, true);
    DS_ASSERT_OK(manager->ClearAsyncWorkerOp(worker));
    insertThread.join();

    DS_ASSERT_OK(insertRc);
    EXPECT_FALSE(manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));
}

TEST_F(OCNotifyWorkerManagerTest, TestChangePrimaryCopy)
{
    auto ocMetaManager = std::make_shared<OCMetadataManager>(
        akSkManager_, nullptr, nullptr, nullptr, "127.0.0.1:900", nullptr, nullptr, false, HostPort(), "", nullptr,
        "workerId");
    auto manager = std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, ocMetaManager.get());

    BINEXPECT_CALL(&OCNotifyWorkerManager::SendChangePrimaryCopy, (_, _, _)).WillRepeatedly(Return(Status::OK()));
    std::string newPrimaryCopy = "127.0.0.1:902";
    const int argumentIndex = 3;
    BINEXPECT_CALL(&OCMetadataManager::ReselectPrimaryCopy, (_, _, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<argumentIndex>(newPrimaryCopy), Return(Status::OK())));

    std::thread t([] {
        const int timeoutMs = 100;
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
        datasystem::g_exitFlag = 1;
        datasystem::g_termSignalCv.notify_all();
    });
    std::unordered_map<std::string, std::unordered_set<std::string>> input;
    input["127.0.0.1:901"].insert("key1");
    input["127.0.0.1:901"].insert("key2");
    manager->ProcessChangePrimaryCopy(input, false);
    t.join();
    RELEASE_STUBS
}
}  // namespace master
}  // namespace datasystem
