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

#include <gtest/gtest.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "ut/common.h"

namespace datasystem::ut {
namespace {
constexpr auto OBSERVER_WAIT_TIMEOUT = std::chrono::seconds(2);
}

class OCMetadataManagerForMigrationTest : public master::OCMetadataManager {
public:
    OCMetadataManagerForMigrationTest() : OCMetadataManager(nullptr, nullptr, nullptr, nullptr, "", nullptr, nullptr,
                                                            false, HostPort(), "", nullptr, "migration-test")
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
