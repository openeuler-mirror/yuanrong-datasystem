/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Test topology object metadata migration completion.
 */
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"

#include <atomic>
#include <future>
#include <thread>
#include <utility>

#include <gtest/gtest.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/master/object_cache/oc_notify_worker_manager.h"
#include "ut/common.h"

DS_DECLARE_string(rocksdb_write_mode);

namespace datasystem::ut {
namespace {
constexpr auto OBSERVER_WAIT_TIMEOUT = std::chrono::seconds(2);
constexpr char DEAD_WORKER_ADDRESS[] = "127.0.0.1:12003";
constexpr char SURVIVING_WORKER_ADDRESS[] = "127.0.0.1:12004";

master::MetaForMigrationPb BuildMigratedMeta(const std::string &objectKey, bool useLegacyLocations)
{
    ObjectMetaPb objectMeta;
    objectMeta.set_object_key(objectKey);
    objectMeta.set_primary_address(DEAD_WORKER_ADDRESS);
    master::MetaForMigrationPb migratedMeta;
    migratedMeta.set_object_key(objectKey);
    migratedMeta.set_meta(objectMeta.SerializeAsString());
    if (useLegacyLocations) {
        migratedMeta.add_locations(SURVIVING_WORKER_ADDRESS);
        migratedMeta.add_locations(DEAD_WORKER_ADDRESS);
    } else {
        for (const auto *location : { SURVIVING_WORKER_ADDRESS, DEAD_WORKER_ADDRESS }) {
            auto *newLocation = migratedMeta.add_new_locations();
            newLocation->set_location(location);
            newLocation->set_ack(static_cast<int>(master::AckState::ACK));
        }
    }
    return migratedMeta;
}
}

class OCMetadataManagerForMigrationTest : public master::OCMetadataManager {
public:
    OCMetadataManagerForMigrationTest() : OCMetadataManager(nullptr, nullptr, nullptr, nullptr, "", nullptr, nullptr,
                                                            false, HostPort(), "", nullptr, "migration-test")
    {
    }

    OCMetadataManagerForMigrationTest(std::shared_ptr<AkSkManager> akSkManager, RocksStore *rocksStore)
        : OCMetadataManager(std::move(akSkManager), rocksStore, nullptr, nullptr, SURVIVING_WORKER_ADDRESS, nullptr,
                            nullptr, false, HostPort(), SURVIVING_WORKER_ADDRESS, nullptr, "migration-race-test")
    {
    }

    void MarkMigrating(const std::string &objectKey)
    {
        migratingItems_.insert({ objectKey, true });
    }

    void PrepareFailureDependencies()
    {
        expiredObjectManager_ = std::make_unique<master::ExpiredObjectManager>("", this);
    }

    Status PrepareMigrationReceiver()
    {
        RETURN_IF_NOT_OK(objectStore_->Init());
        expiredObjectManager_ = std::make_unique<master::ExpiredObjectManager>(SURVIVING_WORKER_ADDRESS, this);
        notifyWorkerManager_ = std::make_unique<master::OCNotifyWorkerManager>(objectStore_, true, akSkManager_, this);
        return Status::OK();
    }

    void MarkWorkerFault(const std::string &workerAddress, bool isDead)
    {
        notifyWorkerManager_->SetFaultWorker(workerAddress, isDead);
    }

    bool SaveMigratedMeta(const master::MetaForMigrationPb &meta, Status &status)
    {
        return SaveOneMeta(meta, status);
    }

    bool HasLocation(const std::string &objectKey, const std::string &workerAddress)
    {
        auto &shard = metaShards_[GetShardIndex(objectKey)];
        bthread::RWLockRdGuard lock(shard.mutex);
        master::TbbMetaTable::const_accessor accessor;
        return shard.table.find(accessor, objectKey) && accessor->second.locations.count(workerAddress) > 0;
    }

    std::string GetPrimaryAddress(const std::string &objectKey)
    {
        auto &shard = metaShards_[GetShardIndex(objectKey)];
        bthread::RWLockRdGuard lock(shard.mutex);
        master::TbbMetaTable::const_accessor accessor;
        return shard.table.find(accessor, objectKey) ? accessor->second.meta.primary_address() : "";
    }
};

class OCMigrateMetadataManagerTest : public CommonTest {
public:
    void VerifyTopologyMigrationRejectsPartialItemFailure()
    {
        master::OCMigrateMetadataManager::MigrateMetaInfo info;
        info.destAddr = "127.0.0.1:1";
        info.operationId = "task-operation-partial-failure";
        const auto key = std::make_pair(info.destAddr, info.operationId);
        std::promise<std::pair<Status, std::vector<std::string>>> result;
        master::TbbFutureThreadTable::accessor accessor;
        migrateManager_.futureThread_.emplace(accessor, key, result.get_future());
        accessor.release();
        result.set_value({ Status::OK(), { "object1" } });

        cluster::CancellationToken cancellation;
        auto rc = migrateManager_.RunTopologyMigration(
            nullptr, info, std::chrono::steady_clock::now() + std::chrono::seconds(1), cancellation);
        EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
        EXPECT_TRUE(migrateManager_.futureThread_.empty());
    }

    void VerifyMetadataRpcObserverUsesParsedTargetAndStopsOnShutdown()
    {
        size_t calls = 0;
        HostPort observedTarget;
        Status observedStatus;
        migrateManager_.metadataRpcObserver_ = [&](const HostPort &target, const Status &status) {
            ++calls;
            observedTarget = target;
            observedStatus = status;
        };
        const Status failure(K_RPC_UNAVAILABLE, "injected migration failure");

        migrateManager_.ObserveMetadataRpcResult("127.0.0.1:12002", failure);
        migrateManager_.ObserveMetadataRpcResult("invalid-target", failure);

        EXPECT_EQ(calls, 1U);
        EXPECT_EQ(observedTarget.ToString(), "127.0.0.1:12002");
        EXPECT_EQ(observedStatus.GetCode(), K_RPC_UNAVAILABLE);
        migrateManager_.Shutdown();
        migrateManager_.ObserveMetadataRpcResult("127.0.0.1:12002", failure);
        EXPECT_EQ(calls, 1U);
    }

    void VerifyMetadataRpcObserverUsesTransportResultBoundary()
    {
        auto akSkManager = std::make_shared<AkSkManager>();
        migrateManager_.akSkManager_ = akSkManager;
        std::vector<StatusCode> observedStatuses;
        migrateManager_.metadataRpcObserver_ = [&](const HostPort &, const Status &status) {
            observedStatuses.push_back(status.GetCode());
        };
        auto api = std::make_unique<master::MasterMasterOCApi>(
            HostPort("127.0.0.1", 12002), HostPort("127.0.0.1", 12001), akSkManager);
        master::OCMigrateMetadataManager::MigrateMetaInfo info;
        info.destAddr = "127.0.0.1:12002";
        master::MigrateMetadataReqPb req;
        std::vector<std::string> failedObjectKeys;
        ScopedRequestContext requestContext;
        GetRequestContext()->reqTimeoutDuration.Init(1'000);

        DS_ASSERT_OK(inject::Set("BatchMigrateMetadata.streamSendData", "return()"));
        Raii clearTransportInject([] { (void)inject::Clear("BatchMigrateMetadata.streamSendData"); });

        DS_ASSERT_OK(inject::Set("BatchMigrateMetadata.rpc.error", "return(K_NOT_AUTHORIZED)"));
        Raii clearPretransportInject([] { (void)inject::Clear("BatchMigrateMetadata.rpc.error"); });
        auto rc = migrateManager_.BatchMigrateMetadata(api, req, nullptr, failedObjectKeys, info);
        EXPECT_EQ(rc.GetCode(), K_NOT_AUTHORIZED);
        EXPECT_TRUE(observedStatuses.empty());
        DS_ASSERT_OK(inject::Clear("BatchMigrateMetadata.rpc.error"));

        info.topologyVersion = 1;
        info.deadline = std::chrono::steady_clock::now();
        rc = migrateManager_.BatchMigrateMetadata(api, req, nullptr, failedObjectKeys, info);
        EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
        EXPECT_TRUE(observedStatuses.empty());

        info.topologyVersion = 0;
        rc = migrateManager_.BatchMigrateMetadata(api, req, nullptr, failedObjectKeys, info);
        EXPECT_EQ(rc.GetCode(), K_RPC_UNAVAILABLE);
        ASSERT_EQ(observedStatuses.size(), 1U);
        EXPECT_EQ(observedStatuses.back(), K_RPC_UNAVAILABLE);

        DS_ASSERT_OK(inject::Clear("BatchMigrateMetadata.streamSendData"));
        auto metadataManager = std::make_shared<OCMetadataManagerForMigrationTest>();
        metadataManager->PrepareFailureDependencies();
        metadataManager->MarkMigrating("response-error-object");
        req.add_object_metas()->set_object_key("response-error-object");
        master::MigrateMetadataRspPb rsp;
        rc = migrateManager_.ApplyBatchMigrationResponse(req, rsp, metadataManager, failedObjectKeys, info, {});
        EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
        EXPECT_EQ(observedStatuses.size(), 1U);
        metadataManager->Shutdown();
    }

    void VerifyShutdownWaitsForMetadataRpcObserver()
    {
        std::promise<void> entered;
        std::promise<void> release;
        auto enteredFuture = entered.get_future();
        auto releaseFuture = release.get_future().share();
        std::atomic<bool> observerTimedOut{ false };
        migrateManager_.metadataRpcObserver_ = [&](const HostPort &, const Status &) {
            entered.set_value();
            observerTimedOut.store(releaseFuture.wait_for(OBSERVER_WAIT_TIMEOUT) != std::future_status::ready);
        };
        auto observe = std::async(std::launch::async, [&] {
            migrateManager_.ObserveMetadataRpcResult("127.0.0.1:12002", Status::OK());
        });
        std::atomic<bool> released{ false };
        auto releaseObserver = [&] {
            if (!released.exchange(true)) {
                release.set_value();
            }
        };
        Raii releaseOnExit(releaseObserver);
        ASSERT_EQ(enteredFuture.wait_for(OBSERVER_WAIT_TIMEOUT), std::future_status::ready);

        auto shutdown = std::async(std::launch::async, [&] { migrateManager_.Shutdown(); });
        EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
        releaseObserver();
        ASSERT_EQ(observe.wait_for(OBSERVER_WAIT_TIMEOUT), std::future_status::ready);
        observe.get();
        EXPECT_FALSE(observerTimedOut.load());
        ASSERT_EQ(shutdown.wait_for(OBSERVER_WAIT_TIMEOUT), std::future_status::ready);
        shutdown.get();
    }

protected:
    master::OCMigrateMetadataManager migrateManager_;
};

TEST_F(OCMigrateMetadataManagerTest, TopologyMigrationRejectsPartialItemFailure)
{
    VerifyTopologyMigrationRejectsPartialItemFailure();
}

TEST_F(OCMigrateMetadataManagerTest, MigrationFailureClearsMovingMarker)
{
    auto metadataManager = std::make_shared<OCMetadataManagerForMigrationTest>();
    metadataManager->PrepareFailureDependencies();
    const std::string objectKey = "failed-object";
    metadataManager->MarkMigrating(objectKey);
    ASSERT_TRUE(metadataManager->ItemIsMigrating(objectKey));
    master::MetaForMigrationPb metadata;
    metadata.set_object_key(objectKey);

    metadataManager->HandleMetaDataMigrationFailed(metadata);

    EXPECT_FALSE(metadataManager->ItemIsMigrating(objectKey));
    metadataManager->Shutdown();
}

TEST_F(OCMigrateMetadataManagerTest, MigrationDoesNotRestoreLocationCleanedByPassiveScaleDown)
{
    const auto previousWriteMode = FLAGS_rocksdb_write_mode;
    FLAGS_rocksdb_write_mode = "sync";
    Raii restoreWriteMode([&] { FLAGS_rocksdb_write_mode = previousWriteMode; });
    auto rocksStore = RocksStore::GetInstance(GetTestCaseDataDir() + "/passive_scale_down_migration");
    auto metadataManager =
        std::make_shared<OCMetadataManagerForMigrationTest>(std::make_shared<AkSkManager>(), rocksStore.get());
    DS_ASSERT_OK(metadataManager->PrepareMigrationReceiver());

    constexpr char objectKey[] = "passive_scale_down_cleanup_before_migration";
    constexpr char beforeSavePoint[] = "OCMetadataManager.SaveOneMeta.before_save";
    auto migratedMeta = BuildMigratedMeta(objectKey, false);
    DS_ASSERT_OK(inject::Set(beforeSavePoint, "1*pause()"));
    Status saveStatus;
    auto save = std::async(std::launch::async,
                           [&] { return metadataManager->SaveMigratedMeta(migratedMeta, saveStatus); });
    Raii clearInject([&] { (void)inject::Clear(beforeSavePoint); });
    const auto deadline = std::chrono::steady_clock::now() + OBSERVER_WAIT_TIMEOUT;
    while (inject::GetExecuteCount(beforeSavePoint) != 1 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(inject::GetExecuteCount(beforeSavePoint), 1U);

    metadataManager->MarkWorkerFault(DEAD_WORKER_ADDRESS, true);
    DS_ASSERT_OK(metadataManager->RemoveMetaByWorker(DEAD_WORKER_ADDRESS));
    DS_ASSERT_OK(inject::Clear(beforeSavePoint));
    ASSERT_TRUE(save.get()) << saveStatus.ToString();
    EXPECT_TRUE(metadataManager->HasLocation(objectKey, SURVIVING_WORKER_ADDRESS));
    EXPECT_FALSE(metadataManager->HasLocation(objectKey, DEAD_WORKER_ADDRESS));
    EXPECT_EQ(metadataManager->GetPrimaryAddress(objectKey), SURVIVING_WORKER_ADDRESS);

    constexpr char legacyObjectKey[] = "legacy_passive_scale_down_migration";
    auto legacyMigratedMeta = BuildMigratedMeta(legacyObjectKey, true);
    ASSERT_TRUE(metadataManager->SaveMigratedMeta(legacyMigratedMeta, saveStatus)) << saveStatus.ToString();
    EXPECT_TRUE(metadataManager->HasLocation(legacyObjectKey, SURVIVING_WORKER_ADDRESS));
    EXPECT_FALSE(metadataManager->HasLocation(legacyObjectKey, DEAD_WORKER_ADDRESS));

    metadataManager->Shutdown();
    rocksStore.reset();
}

TEST_F(OCMigrateMetadataManagerTest, MetadataRpcObserverUsesParsedTargetAndStopsOnShutdown)
{
    VerifyMetadataRpcObserverUsesParsedTargetAndStopsOnShutdown();
}

TEST_F(OCMigrateMetadataManagerTest, MetadataRpcObserverUsesTransportResultBoundary)
{
    VerifyMetadataRpcObserverUsesTransportResultBoundary();
}

TEST_F(OCMigrateMetadataManagerTest, ShutdownWaitsForMetadataRpcObserver)
{
    VerifyShutdownWaitsForMetadataRpcObserver();
}

}  // namespace datasystem::ut
