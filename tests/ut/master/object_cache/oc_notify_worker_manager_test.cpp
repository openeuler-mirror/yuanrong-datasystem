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
#include <string>
#include <thread>
#include <vector>

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/format.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"

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
        (void)inject::Clear("OCNotifyWorkerManager.NotifyOpToWorker");
        objectStore_.reset();
        rocksStore_.reset();
        FLAGS_rocksdb_write_mode = rocksdbWriteMode_;
    }

    std::shared_ptr<RocksStore> rocksStore_;
    std::shared_ptr<ObjectMetaStore> objectStore_;
    HostPort hostPort_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::string rocksdbWriteMode_;

    std::vector<OCNotifyWorkerManager::AsyncWorkerOpSnapshot> SnapshotWorkerOps(OCNotifyWorkerManager &manager,
                                                                                const std::string &worker)
    {
        return manager.SnapshotAsyncWorkerOps(worker);
    }

    Status ClearSnapshotOps(OCNotifyWorkerManager &manager, const std::string &worker,
                            const std::vector<OCNotifyWorkerManager::AsyncWorkerOpSnapshot> &snapshots)
    {
        return manager.ClearAsyncWorkerOpSnapshots(worker, snapshots);
    }

    void AttachNotifyManager(const std::shared_ptr<OCMetadataManager> &metadataManager)
    {
        metadataManager->objectStore_ = objectStore_;
        DS_ASSERT_OK(metadataManager->InitGlobalRef());
        metadataManager->notifyWorkerManager_ =
            std::make_unique<OCNotifyWorkerManager>(objectStore_, true, akSkManager_, metadataManager.get());
    }

    Status InsertMetadataNotifyOp(const std::shared_ptr<OCMetadataManager> &metadataManager, const std::string &worker,
                                  const std::string &objectKey, NotifyWorkerOpType type)
    {
        return metadataManager->notifyWorkerManager_->InsertAsyncWorkerOp(worker, objectKey, { type });
    }

    bool HasMetadataNotifyOp(const std::shared_ptr<OCMetadataManager> &metadataManager, const std::string &worker,
                             const std::string &objectKey, NotifyWorkerOpType type)
    {
        return metadataManager->notifyWorkerManager_->CheckExistAsyncWorkerOp(worker, objectKey, type);
    }

    Status CommitPromotion(const std::shared_ptr<OCMetadataManager> &metadataManager, const std::string &objectKey,
                           const std::string &oldPrimary, const std::string &newPrimary, uint64_t expectedVersion)
    {
        bool oldPrimaryFencePersisted = false;
        return metadataManager->CommitPrimaryCopyPromotion(objectKey, oldPrimary, newPrimary, expectedVersion,
                                                           oldPrimaryFencePersisted);
    }

    Status ReconcileTimeoutPromotion(const std::shared_ptr<OCMetadataManager> &metadataManager,
                                     const std::string &oldPrimary)
    {
        return metadataManager->ReconcilePrimaryCopyByWorkerTimeout(oldPrimary);
    }

    Status ReconcileLocalIsolationPromotion(const std::shared_ptr<OCMetadataManager> &metadataManager,
                                            const std::string &oldPrimary)
    {
        return metadataManager->ProcessWorkerLocalIsolation(oldPrimary);
    }

    Status ReconcileNetworkRecovery(const std::shared_ptr<OCMetadataManager> &metadataManager,
                                    const std::string &workerAddr)
    {
        return metadataManager->ProcessWorkerNetworkRecovery(workerAddr, 1, false);
    }

    Status CheckMetadataWorkerHealth(const std::shared_ptr<OCMetadataManager> &metadataManager,
                                     const std::string &worker)
    {
        return metadataManager->notifyWorkerManager_->CheckWorkerIsHealthy(worker);
    }

    void SetMetadataWorkerFault(const std::shared_ptr<OCMetadataManager> &metadataManager, const std::string &worker)
    {
        metadataManager->notifyWorkerManager_->SetFaultWorker(worker);
    }

    void ExpectNetworkRecoveryPushOk()
    {
        BINEXPECT_CALL(&OCNotifyWorkerManager::NotifyOpToWorker, (_, _)).WillRepeatedly(Return(Status::OK()));
        BINEXPECT_CALL(&OCNotifyWorkerManager::PushMetaToWorker, (_, _, _)).WillRepeatedly(Return(Status::OK()));
    }

    std::shared_ptr<OCMetadataManager> MakeMetadataManager()
    {
        return std::make_shared<OCMetadataManager>(akSkManager_, nullptr, nullptr, nullptr, "127.0.0.1:900", nullptr,
                                                   nullptr, false, HostPort(), "", nullptr, "workerId");
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

    bool observedPendingOp = false;
    int64_t maxCheckMs = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(maxPollMs);
    while (std::chrono::steady_clock::now() < deadline) {
        auto start = std::chrono::steady_clock::now();
        bool exists = manager->CheckExistAsyncWorkerOp(worker, objectKey, NotifyWorkerOpType::CACHE_INVALID);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        maxCheckMs = std::max(maxCheckMs, static_cast<int64_t>(elapsed.count()));
        if (exists) {
            observedPendingOp = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
    }

    insertThread.join();
    DS_ASSERT_OK(inject::Clear("master.rocksdb.put"));
    DS_ASSERT_OK(insertRc);
    ASSERT_TRUE(observedPendingOp);
    EXPECT_LT(maxCheckMs, maxExpectedCheckMs);
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

TEST_F(OCNotifyWorkerManagerTest, ReselectPrimaryCopyRequiresAcknowledgedLocation)
{
    auto metadataManager = MakeMetadataManager();
    const std::string objectKey = "reselect_acknowledged_location";
    const std::string oldPrimary = "127.0.0.1:901";
    const std::string candidate = "127.0.0.1:902";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor insertAccessor;
    ASSERT_TRUE(shard.table.insert(insertAccessor, objectKey));
    insertAccessor->second.meta.set_object_key(objectKey);
    insertAccessor->second.meta.set_primary_address(oldPrimary);
    insertAccessor->second.locations[oldPrimary] = AckState::ACK;
    insertAccessor->second.locations[candidate] = AckState::UNACK;
    insertAccessor.release();
    BINEXPECT_CALL(&OCNotifyWorkerManager::CheckWorkerIsHealthy, (_))
        .WillRepeatedly(Return(Status(K_WORKER_ABNORMAL, "old primary unavailable")));

    TbbMetaTable::accessor accessor;
    std::string selectedPrimary;
    auto rc = metadataManager->ReselectPrimaryCopy(objectKey, {}, accessor, selectedPrimary);
    EXPECT_EQ(rc.GetCode(), K_UNKNOWN_ERROR);
    accessor.release();

    TbbMetaTable::accessor updateAccessor;
    ASSERT_TRUE(shard.table.find(updateAccessor, objectKey));
    updateAccessor->second.locations[candidate] = AckState::ACK;
    updateAccessor.release();
    DS_ASSERT_OK(metadataManager->ReselectPrimaryCopy(objectKey, {}, accessor, selectedPrimary));
    EXPECT_EQ(selectedPrimary, candidate);
    RELEASE_STUBS
}

TEST_F(OCNotifyWorkerManagerTest, WorkerTimeoutWithoutAcknowledgedCopyKeepsPendingOperations)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "timeout_without_acknowledged_copy";
    const std::string oldPrimary = "127.0.0.1:901";
    DS_ASSERT_OK(InsertMetadataNotifyOp(metadataManager, oldPrimary, objectKey, NotifyWorkerOpType::CACHE_INVALID));
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(oldPrimary);
    accessor->second.locations[oldPrimary] = AckState::ACK;
    accessor.release();

    auto rc = metadataManager->ProcessWorkerTimeout(oldPrimary, false, true);

    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_TRUE(HasMetadataNotifyOp(metadataManager, oldPrimary, objectKey, NotifyWorkerOpType::CACHE_INVALID));
}

TEST_F(OCNotifyWorkerManagerTest, TopologyCleanupKeepsLocalPrimaryWhenNoAcknowledgedReplacementExists)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "topology_cleanup_local_only_primary";
    const std::string failedWorker = "127.0.0.1:901";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(failedWorker);
    accessor->second.locations[failedWorker] = AckState::ACK;
    accessor.release();
    cluster::TopologyPhaseAction action;
    action.failed = cluster::MemberIdentity{ "failed-worker", failedWorker };
    cluster::CancellationToken cancellation;

    DS_ASSERT_OK(metadataManager->CleanupTopologyFailedMember(
        action, "topology-cleanup-local-only-primary", std::chrono::steady_clock::now() + std::chrono::seconds(5),
        cancellation));
}

TEST_F(OCNotifyWorkerManagerTest, LocalIsolationPromotesAcknowledgedPeerWithoutRemovingOldLocation)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "local_isolation_promotes_ack_peer";
    const std::string oldPrimary = "127.0.0.1:901";
    const std::string candidate = "127.0.0.1:902";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(oldPrimary);
    accessor->second.meta.set_version(1);
    accessor->second.locations[oldPrimary] = AckState::ACK;
    accessor->second.locations[candidate] = AckState::ACK;
    accessor.release();
    std::unordered_set<std::string> promoted{ objectKey };
    BINEXPECT_CALL(&OCNotifyWorkerManager::SendChangePrimaryCopy, (_, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<2>(promoted), Return(Status::OK())));
    BINEXPECT_CALL(&OCNotifyWorkerManager::AsyncNotifyOpToWorker, (_, _)).WillRepeatedly(Return());

    DS_ASSERT_OK(ReconcileLocalIsolationPromotion(metadataManager, oldPrimary));

    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.meta.primary_address(), candidate);
    EXPECT_NE(accessor->second.locations.find(oldPrimary), accessor->second.locations.end());
    accessor.release();
    EXPECT_TRUE(HasMetadataNotifyOp(metadataManager, oldPrimary, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    EXPECT_EQ(CheckMetadataWorkerHealth(metadataManager, oldPrimary).GetCode(), K_WORKER_ABNORMAL);
    RELEASE_STUBS
}

TEST_F(OCNotifyWorkerManagerTest, RecoveredOldPrimaryDoesNotOverrideMasterPrimary)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "recovered_old_primary_does_not_override_master_primary";
    const std::string oldPrimary = "127.0.0.1:901";
    const std::string candidate = "127.0.0.1:902";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(oldPrimary);
    accessor->second.meta.set_version(1);
    accessor->second.locations[oldPrimary] = AckState::ACK;
    accessor->second.locations[candidate] = AckState::ACK;
    accessor.release();
    std::unordered_set<std::string> promoted{ objectKey };
    BINEXPECT_CALL(&OCNotifyWorkerManager::SendChangePrimaryCopy, (_, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<2>(promoted), Return(Status::OK())));
    BINEXPECT_CALL(&OCNotifyWorkerManager::AsyncNotifyOpToWorker, (_, _)).WillRepeatedly(Return());
    DS_ASSERT_OK(ReconcileLocalIsolationPromotion(metadataManager, oldPrimary));

    ExpectNetworkRecoveryPushOk();
    DS_ASSERT_OK(ReconcileNetworkRecovery(metadataManager, oldPrimary));
    RequestMetaFromWorkerRspPb rsp;
    rsp.set_address(oldPrimary);
    auto *recoveredMeta = rsp.add_metas();
    recoveredMeta->set_object_key(objectKey);
    recoveredMeta->set_primary_address(oldPrimary);
    recoveredMeta->set_version(1);
    recoveredMeta->set_is_recovered(true);
    BINEXPECT_CALL(&OCNotifyWorkerManager::RequestMetaFromWorker, (_, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<2>(rsp), Return(Status::OK())));
    DS_ASSERT_OK(metadataManager->RequestMetaFromWorker(candidate, oldPrimary));

    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.meta.primary_address(), candidate);
    EXPECT_NE(accessor->second.locations.find(oldPrimary), accessor->second.locations.end());
    EXPECT_NE(accessor->second.locations.find(candidate), accessor->second.locations.end());
    accessor.release();
    EXPECT_TRUE(HasMetadataNotifyOp(metadataManager, oldPrimary, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    RELEASE_STUBS
}

TEST_F(OCNotifyWorkerManagerTest, PrimaryFenceAndCacheInvalidOpsAreBothRetained)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "combined_primary_fence_and_cache_invalid";
    const std::string reverseObjectKey = objectKey + "_reverse";
    const std::string worker = "127.0.0.1:901";

    DS_ASSERT_OK(InsertMetadataNotifyOp(metadataManager, worker, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    DS_ASSERT_OK(InsertMetadataNotifyOp(metadataManager, worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));
    EXPECT_TRUE(HasMetadataNotifyOp(metadataManager, worker, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    EXPECT_TRUE(HasMetadataNotifyOp(metadataManager, worker, objectKey, NotifyWorkerOpType::CACHE_INVALID));

    DS_ASSERT_OK(InsertMetadataNotifyOp(metadataManager, worker, reverseObjectKey, NotifyWorkerOpType::CACHE_INVALID));
    DS_ASSERT_OK(
        InsertMetadataNotifyOp(metadataManager, worker, reverseObjectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    EXPECT_TRUE(
        HasMetadataNotifyOp(metadataManager, worker, reverseObjectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    EXPECT_TRUE(HasMetadataNotifyOp(metadataManager, worker, reverseObjectKey, NotifyWorkerOpType::CACHE_INVALID));
}

TEST_F(OCNotifyWorkerManagerTest, PromotionVersionRaceDoesNotPersistPrimaryFences)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "promotion_version_race";
    const std::string oldPrimary = "127.0.0.1:901";
    const std::string newPrimary = "127.0.0.1:902";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(oldPrimary);
    accessor->second.meta.set_version(2);
    accessor->second.locations[oldPrimary] = AckState::ACK;
    accessor->second.locations[newPrimary] = AckState::ACK;
    accessor.release();

    auto rc = CommitPromotion(metadataManager, objectKey, oldPrimary, newPrimary, 1);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    EXPECT_FALSE(HasMetadataNotifyOp(metadataManager, oldPrimary, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    EXPECT_FALSE(HasMetadataNotifyOp(metadataManager, newPrimary, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
}

TEST_F(OCNotifyWorkerManagerTest, PromotionMetadataPersistenceFailureKeepsAcceptedCandidate)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "promotion_metadata_persistence_failure";
    const std::string oldPrimary = "127.0.0.1:901";
    const std::string newPrimary = "127.0.0.1:902";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(oldPrimary);
    accessor->second.meta.set_version(1);
    accessor->second.locations[oldPrimary] = AckState::ACK;
    accessor->second.locations[newPrimary] = AckState::ACK;
    accessor.release();
    BINEXPECT_CALL(&OCNotifyWorkerManager::CheckWorkerIsHealthy, (_)).WillRepeatedly(Return(Status::OK()));
    std::unordered_set<std::string> promoted{ objectKey };
    BINEXPECT_CALL(&OCNotifyWorkerManager::SendChangePrimaryCopy, (_, _, _))
        .WillRepeatedly(DoAll(SetArgReferee<2>(promoted), Return(Status::OK())));
    BINEXPECT_CALL(&OCNotifyWorkerManager::AsyncNotifyOpToWorker, (_, _)).WillRepeatedly(Return());
    DS_ASSERT_OK(inject::Set("master.rocksdb.put", "1*sleep(0)->1*return(K_RUNTIME_ERROR)"));

    auto rc = ReconcileTimeoutPromotion(metadataManager, oldPrimary);
    DS_ASSERT_OK(inject::Clear("master.rocksdb.put"));

    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(HasMetadataNotifyOp(metadataManager, oldPrimary, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    EXPECT_FALSE(HasMetadataNotifyOp(metadataManager, newPrimary, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.meta.primary_address(), oldPrimary);
    accessor.release();

    DS_ASSERT_OK(ReconcileTimeoutPromotion(metadataManager, oldPrimary));
    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.meta.primary_address(), newPrimary);
    RELEASE_STUBS
}

TEST_F(OCNotifyWorkerManagerTest, NetworkRecoveryStopsBeforeMetadataPushWhenFenceDeliveryFails)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string recoveringWorker = "127.0.0.1:901";
    const Status fenceFailure(K_RPC_UNAVAILABLE, "ownership fence delivery failed");
    DS_ASSERT_OK(inject::Set("OCNotifyWorkerManager.NotifyOpToWorker", "return(K_RPC_UNAVAILABLE)"));
    BINEXPECT_CALL(&OCNotifyWorkerManager::AsyncPushMetaToWorker, (_, _, _)).Times(0);

    auto rc = metadataManager->ProcessWorkerNetworkRecovery(recoveringWorker, 1, false);

    EXPECT_EQ(rc.GetCode(), fenceFailure.GetCode());
    EXPECT_EQ(CheckMetadataWorkerHealth(metadataManager, recoveringWorker).GetCode(), K_WORKER_ABNORMAL);
    RELEASE_STUBS
}

TEST_F(OCNotifyWorkerManagerTest, NetworkRecoveryKeepsLocalPrimaryWhenNoAcknowledgedReplacementExists)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "network_recovery_local_only_primary";
    const std::string recoveringWorker = "127.0.0.1:901";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(recoveringWorker);
    accessor->second.locations[recoveringWorker] = AckState::ACK;
    accessor.release();
    SetMetadataWorkerFault(metadataManager, recoveringWorker);
    ExpectNetworkRecoveryPushOk();

    DS_ASSERT_OK(ReconcileNetworkRecovery(metadataManager, recoveringWorker));

    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.meta.primary_address(), recoveringWorker);
    accessor.release();
    EXPECT_EQ(CheckMetadataWorkerHealth(metadataManager, recoveringWorker), Status::OK());
    RELEASE_STUBS
}

TEST_F(OCNotifyWorkerManagerTest, PromotionRpcUncertaintyDoesNotInvalidateAcknowledgedCandidate)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "promotion_rpc_uncertain";
    const std::string oldPrimary = "127.0.0.1:901";
    const std::string candidate = "127.0.0.1:902";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(oldPrimary);
    accessor->second.locations[oldPrimary] = AckState::ACK;
    accessor->second.locations[candidate] = AckState::ACK;
    accessor.release();
    BINEXPECT_CALL(&OCNotifyWorkerManager::CheckWorkerIsHealthy, (_)).WillRepeatedly(Return(Status::OK()));
    BINEXPECT_CALL(&OCNotifyWorkerManager::SendChangePrimaryCopy, (_, _, _))
        .WillOnce(Return(Status(K_RPC_UNAVAILABLE, "promotion response lost")));

    auto rc = ReconcileTimeoutPromotion(metadataManager, oldPrimary);

    EXPECT_EQ(rc.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_FALSE(HasMetadataNotifyOp(metadataManager, candidate, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    RELEASE_STUBS
}

TEST_F(OCNotifyWorkerManagerTest, AcceptedPromotionVersionRaceDoesNotInvalidateCandidate)
{
    auto metadataManager = MakeMetadataManager();
    AttachNotifyManager(metadataManager);
    const std::string objectKey = "accepted_promotion_version_race";
    const std::string oldPrimary = "127.0.0.1:901";
    const std::string candidate = "127.0.0.1:902";
    auto &shard = metadataManager->GetShardFor(objectKey);
    TbbMetaTable::accessor accessor;
    ASSERT_TRUE(shard.table.insert(accessor, objectKey));
    accessor->second.meta.set_object_key(objectKey);
    accessor->second.meta.set_primary_address(oldPrimary);
    accessor->second.meta.set_version(1);
    accessor->second.locations[oldPrimary] = AckState::ACK;
    accessor->second.locations[candidate] = AckState::ACK;
    accessor.release();
    BINEXPECT_CALL(&OCNotifyWorkerManager::CheckWorkerIsHealthy, (_)).WillRepeatedly(Return(Status::OK()));
    BINEXPECT_CALL(&OCNotifyWorkerManager::SendChangePrimaryCopy, (_, _, _))
        .WillOnce(Invoke([&](const std::string &, const std::unordered_set<std::string> &,
                             std::unordered_set<std::string> &successIds) {
            TbbMetaTable::accessor updateAccessor;
            EXPECT_TRUE(shard.table.find(updateAccessor, objectKey));
            updateAccessor->second.meta.set_version(2);
            successIds.emplace(objectKey);
            return Status::OK();
        }));

    auto rc = ReconcileTimeoutPromotion(metadataManager, oldPrimary);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    EXPECT_FALSE(HasMetadataNotifyOp(metadataManager, candidate, objectKey, NotifyWorkerOpType::PRIMARY_COPY_INVALID));
    RELEASE_STUBS
}

TEST_F(OCNotifyWorkerManagerTest, TestChangePrimaryCopy)
{
    auto ocMetaManager = MakeMetadataManager();
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
