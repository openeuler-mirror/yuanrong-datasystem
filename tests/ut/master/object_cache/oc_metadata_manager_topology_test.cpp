/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Test topology-fenced object metadata mutations.
 */
#include <atomic>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/master/object_cache/expired_object_manager.h"
#include "ut/common.h"

#define private public
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#undef private

DS_DECLARE_string(rocksdb_write_mode);

namespace datasystem::master {
namespace {
constexpr char LOCAL_ADDRESS[] = "127.0.0.1:30001";
constexpr char TARGET_ADDRESS[] = "127.0.0.1:30002";
constexpr char SURVIVOR_ADDRESS[] = "127.0.0.1:30003";

class OCMetadataManagerTopologyTest : public ut::CommonTest {
public:
    void SetUp() override
    {
        oldWriteMode_ = FLAGS_rocksdb_write_mode;
        FLAGS_rocksdb_write_mode = "sync";
        rocksStore_ = RocksStore::GetInstance(ut::GetTestCaseDataDir() + "/rocksdb");
        akSkManager_ = std::make_shared<AkSkManager>(0);
        localExiting_.store(true);
    }

    void TearDown() override
    {
        (void)inject::Clear("master.rocksdb.put");
        (void)inject::Clear("OCNotifyWorkerManager.CheckWorkerIsHealth.worker.unhealthy");
        rocksStore_.reset();
        FLAGS_rocksdb_write_mode = oldWriteMode_;
    }

    void InsertPrimaryWithCopy(OCMetadataManager &manager, const std::string &objectKey)
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
        std::unique_lock<std::shared_timed_mutex> lock(shard.mutex);
        (void)shard.table.insert(accessor, objectKey);
        accessor->second.meta.set_object_key(objectKey);
        accessor->second.meta.set_primary_address(LOCAL_ADDRESS);
        accessor->second.locations[LOCAL_ADDRESS] = AckState::ACK;
        accessor->second.locations[TARGET_ADDRESS] = AckState::ACK;
    }

    std::string oldWriteMode_;
    std::shared_ptr<RocksStore> rocksStore_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::atomic<bool> localExiting_{ false };
};

TEST_F(OCMetadataManagerTopologyTest, TopologyOperationCanChangePrimaryWhileOrdinaryExitingRequestIsFenced)
{
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, nullptr, nullptr, false,
                              HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    DS_ASSERT_OK(manager.objectStore_->Init());
    const std::string objectKey = "topology_primary_handoff";
    InsertPrimaryWithCopy(manager, objectKey);

    EXPECT_EQ(manager.ChangePrimaryCopy(TARGET_ADDRESS, objectKey, "").GetCode(), K_TRY_AGAIN);
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
        std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        EXPECT_EQ(accessor->second.meta.primary_address(), LOCAL_ADDRESS);
    }

    DS_ASSERT_OK(manager.ChangePrimaryCopy(TARGET_ADDRESS, objectKey, "scale-in-operation"));
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
        std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        EXPECT_EQ(accessor->second.meta.primary_address(), TARGET_ADDRESS);
    }
}

TEST_F(OCMetadataManagerTopologyTest, RestartBatchRemovesAllAffectedLocationsInOneOperation)
{
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, nullptr, nullptr, false,
                              HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    DS_ASSERT_OK(manager.objectStore_->Init());
    const std::string objectKey = "restart_batch_locations";
    InsertPrimaryWithCopy(manager, objectKey);
    {
        TbbMetaTable::accessor accessor;
        auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
        std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        accessor->second.locations[SURVIVOR_ADDRESS] = AckState::ACK;
        accessor->second.meta.set_primary_address(SURVIVOR_ADDRESS);
    }
    const std::map<std::string, int64_t> restartFacts{
        { LOCAL_ADDRESS, 100 },
        { TARGET_ADDRESS, 200 },
    };

    DS_ASSERT_OK(manager.RemoveMetaByWorkers(restartFacts));

    TbbMetaTable::const_accessor accessor;
    auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
    std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
    ASSERT_TRUE(shard.table.find(accessor, objectKey));
    EXPECT_EQ(accessor->second.locations.count(LOCAL_ADDRESS), 0U);
    EXPECT_EQ(accessor->second.locations.count(TARGET_ADDRESS), 0U);
    EXPECT_EQ(accessor->second.locations.count(SURVIVOR_ADDRESS), 1U);
}

TEST_F(OCMetadataManagerTopologyTest, NestedMigrationPersistenceFailureIsReturnedToCaller)
{
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, nullptr, nullptr, false,
                              HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    DS_ASSERT_OK(manager.objectStore_->Init());
    manager.nestedRefManager_ = std::make_unique<OCNestedManager>(manager.objectStore_, false);
    MetaForMigrationPb metadata;
    metadata.set_object_key("nested-migration-persistence-failure");
    metadata.set_nested_ref(1);
    DS_ASSERT_OK(inject::Set("master.rocksdb.put", "1*return(K_RUNTIME_ERROR)"));

    EXPECT_EQ(manager.SaveNestedMigrationMetadata(metadata).GetCode(), K_RUNTIME_ERROR);
}

TEST_F(OCMetadataManagerTopologyTest, SynchronousRestartPushReturnsDeliveryFailure)
{
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, nullptr, nullptr, false,
                              HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    manager.globalRefTable_ = std::make_unique<object_cache::ObjectGlobalRefTable<ImmutableString>>();
    manager.notifyWorkerManager_ =
        std::make_unique<OCNotifyWorkerManager>(manager.objectStore_, true, akSkManager_, &manager);
    DS_ASSERT_OK(inject::Set("OCNotifyWorkerManager.CheckWorkerIsHealth.worker.unhealthy",
                             "return(K_WORKER_ABNORMAL)"));

    EXPECT_EQ(manager.notifyWorkerManager_->PushMetaToWorker(TARGET_ADDRESS, 1, true).GetCode(),
              K_WORKER_ABNORMAL);
}

TEST_F(OCMetadataManagerTopologyTest, RedirectableRemoveMetaWaitsInsteadOfFailingWhenLocalNodeIsExiting)
{
    cluster::TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, 2 };
    topology.members = {
        cluster::Member{ { std::string(16, 'a'), LOCAL_ADDRESS }, cluster::MemberState::LEAVING,
                         { std::numeric_limits<uint32_t>::max() } },
        cluster::Member{ { std::string(16, 'b'), TARGET_ADDRESS }, cluster::MemberState::ACTIVE, { 0 } },
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(cluster::TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    cluster::HashAlgorithm algorithm;
    cluster::PlacementFacade placement(snapshots, algorithm, LOCAL_ADDRESS);
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, &placement, nullptr,
                              false, HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");

    const std::string objectKey = "topology_remove_meta_handoff";
    InsertPrimaryWithCopy(manager, objectKey);

    RemoveMetaReqPb request;
    request.set_address(LOCAL_ADDRESS);
    request.set_cause(RemoveMetaReqPb::EVICTION);
    request.set_version(UINT64_MAX);
    request.set_redirect(true);
    request.add_ids(objectKey);
    auto *objectVersion = request.add_id_with_version();
    objectVersion->set_id(objectKey);
    objectVersion->set_version(UINT64_MAX);
    RemoveMetaRspPb response;

    DS_ASSERT_OK(manager.RemoveMetaLocation(request, LOCAL_ADDRESS, response));

    EXPECT_TRUE(response.meta_is_moving());
    EXPECT_EQ(response.failed_ids_size(), 0);
    EXPECT_EQ(response.success_ids_size(), 0);
}

TEST_F(OCMetadataManagerTopologyTest, CreateMultiMetaDoesNotMutateSourceDuringScaleOutWait)
{
    cluster::TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_OUT, 2 };
    topology.members = {
        cluster::Member{ { std::string(16, 'a'), LOCAL_ADDRESS }, cluster::MemberState::ACTIVE, { 0 } },
        cluster::Member{ { std::string(16, 'b'), TARGET_ADDRESS }, cluster::MemberState::JOINING,
                         { std::numeric_limits<uint32_t>::max() } },
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(cluster::TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    cluster::HashAlgorithm algorithm;
    cluster::PlacementFacade placement(snapshots, algorithm, LOCAL_ADDRESS);
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, &placement, nullptr,
                              false, HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    const std::string objectKey = "scale_out_create_multi_wait";
    CreateMultiMetaReqPb request;
    request.set_address(LOCAL_ADDRESS);
    request.set_redirect(true);
    request.add_metas()->set_object_key(objectKey);
    CreateMultiMetaRspPb response;

    DS_ASSERT_OK(manager.CreateMultiMeta(request, response));

    EXPECT_TRUE(response.meta_is_moving());
    auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
    TbbMetaTable::const_accessor accessor;
    std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
    EXPECT_FALSE(shard.table.find(accessor, objectKey));
}

TEST_F(OCMetadataManagerTopologyTest, AsyncDeleteAllCopyMetaDoesNotQueueRedirectedKeys)
{
    cluster::TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, 2 };
    topology.members = {
        cluster::Member{ { std::string(16, 'a'), LOCAL_ADDRESS }, cluster::MemberState::LEAVING,
                         { std::numeric_limits<uint32_t>::max() } },
        cluster::Member{ { std::string(16, 'b'), TARGET_ADDRESS }, cluster::MemberState::ACTIVE, { 0 } },
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(cluster::TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    cluster::HashAlgorithm algorithm;
    cluster::PlacementFacade placement(snapshots, algorithm, LOCAL_ADDRESS);
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, &placement, nullptr,
                              false, HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    manager.expiredObjectManager_ = std::make_unique<ExpiredObjectManager>(LOCAL_ADDRESS, &manager);
    const std::string objectKey = "async_delete_redirected_key";
    DeleteAllCopyMetaReqPb request;
    request.set_address(LOCAL_ADDRESS);
    request.set_redirect(true);
    request.set_async_delete(true);
    auto *objectVersion = request.add_ids_with_version();
    objectVersion->set_id(objectKey);
    objectVersion->set_version(1);
    DeleteAllCopyMetaRspPb response;

    DS_ASSERT_OK(manager.DeleteAllCopyMeta(request, response));

    ASSERT_EQ(response.info_size(), 1);
    EXPECT_EQ(response.info(0).redirect_meta_address(), TARGET_ADDRESS);
    EXPECT_EQ(response.info(0).topology_version(), 2U);
    ASSERT_EQ(response.info(0).change_meta_ids_size(), 1);
    EXPECT_EQ(response.info(0).change_meta_ids(0), objectKey);
    EXPECT_EQ(response.failed_object_keys_size(), 0);
    EXPECT_EQ(manager.expiredObjectManager_->GetExpiredObject().count(objectKey), 0U);
}

TEST_F(OCMetadataManagerTopologyTest, DeleteAllCopyMetaDoesNotDeleteMetadataThatReappeared)
{
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, nullptr, nullptr, false,
                              HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    const std::string objectKey = "delete_no_meta_reappeared";
    DeleteObjectMediator mediator(LOCAL_ADDRESS, { { objectKey, true } });
    mediator.AddHashObjsWithoutMeta(objectKey);
    InsertPrimaryWithCopy(manager, objectKey);
    std::unordered_map<std::string, DeleteStruct> deleteObjects{ { objectKey, DeleteStruct{} } };
    std::unordered_set<std::string> failedObjects;

    Status rc = manager.ClearMetaInfo(deleteObjects, false, failedObjects, mediator);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(failedObjects.count(objectKey), size_t(1));
    TbbMetaTable::const_accessor accessor;
    auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
    std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
    EXPECT_TRUE(shard.table.find(accessor, objectKey));
}

TEST_F(OCMetadataManagerTopologyTest, DeleteAllCopyMetaDoesNotDeleteObjectAlreadyClassifiedFailed)
{
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, nullptr, nullptr, false,
                              HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    const std::string objectKey = "delete_preclassified_failed";
    InsertPrimaryWithCopy(manager, objectKey);
    auto &shard = manager.metaShards_[manager.GetShardIndex(objectKey)];
    {
        TbbMetaTable::accessor accessor;
        std::unique_lock<std::shared_timed_mutex> lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        accessor->second.multiSetState = PENDING;
    }
    DeleteObjectMediator mediator(LOCAL_ADDRESS, { { objectKey, true } });
    std::unordered_map<std::string, DeleteStruct> deleteObjects{ { objectKey, DeleteStruct{} } };
    EXPECT_EQ(manager.GetMetaInfoAndSetDeleting(objectKey, deleteObjects.at(objectKey), mediator).GetCode(),
              K_NOT_FOUND);
    EXPECT_EQ(mediator.GetFailedObjs().count(objectKey), size_t(1));
    {
        TbbMetaTable::accessor accessor;
        std::unique_lock<std::shared_timed_mutex> lock(shard.mutex);
        ASSERT_TRUE(shard.table.find(accessor, objectKey));
        accessor->second.multiSetState = IDLE;
    }
    std::unordered_set<std::string> failedObjects;

    Status rc = manager.ClearMetaInfo(deleteObjects, false, failedObjects, mediator);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(failedObjects.count(objectKey), size_t(1));
    TbbMetaTable::const_accessor accessor;
    std::shared_lock<std::shared_timed_mutex> lock(shard.mutex);
    EXPECT_TRUE(shard.table.find(accessor, objectKey));
}

TEST_F(OCMetadataManagerTopologyTest, AsyncDeleteAllCopyMetaQueuesRequestVersion)
{
    OCMetadataManager manager(akSkManager_, rocksStore_.get(), nullptr, nullptr, LOCAL_ADDRESS, nullptr, nullptr, false,
                              HostPort(), LOCAL_ADDRESS, &localExiting_, "workerId");
    manager.expiredObjectManager_ = std::make_unique<ExpiredObjectManager>(LOCAL_ADDRESS, &manager);
    const std::string objectKey = "async_delete_request_version";
    DeleteAllCopyMetaReqPb request;
    request.set_address(LOCAL_ADDRESS);
    request.set_async_delete(true);
    auto *objectVersion = request.add_ids_with_version();
    objectVersion->set_id(objectKey);
    objectVersion->set_version(1);
    DeleteAllCopyMetaRspPb response;

    DS_ASSERT_OK(manager.DeleteAllCopyMeta(request, response));

    EXPECT_EQ(response.failed_object_keys_size(), 0);
    auto expiredObjects = manager.expiredObjectManager_->GetExpiredObject();
    ASSERT_TRUE(expiredObjects.count(objectKey) > 0);
    EXPECT_EQ(expiredObjects.at(objectKey), uint64_t(1));
}
}  // namespace
}  // namespace datasystem::master
