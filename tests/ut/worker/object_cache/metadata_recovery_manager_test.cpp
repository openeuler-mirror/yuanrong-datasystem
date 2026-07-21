/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Unit tests for metadata recovery manager.
 */
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/cluster/routing/placement_facade.h"
#define private public
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"
#include "datasystem/worker/object_cache/metadata_recovery_selector.h"
#undef private
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "tests/ut/worker/object_cache/test_placement_facade.h"
#include "tests/ut/worker/object_cache/test_metadata_route.h"

using namespace ::testing;
using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
using MetadataTestPlacementFacade = TestPlacementFacade;

class TestWorkerMasterApiManager : public worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi> {
public:
    TestWorkerMasterApiManager(HostPort &workerAddr, const worker::MetadataRouteResolver &metadataRoute)
        : WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>(workerAddr, nullptr, metadataRoute)
    {
    }

    std::shared_ptr<worker::WorkerMasterOCApi> CreateWorkerMasterApi(const HostPort &masterAddress) override
    {
        auto iter = apiByAddr_.find(masterAddress.ToString());
        return iter == apiByAddr_.end() ? nullptr : iter->second;
    }

    std::shared_ptr<worker::WorkerMasterOCApi> GetWorkerMasterApi(const HostPort &masterAddress) override
    {
        auto iter = apiByAddr_.find(masterAddress.ToString());
        return iter == apiByAddr_.end() ? nullptr : iter->second;
    }

    void SetApi(const HostPort &masterAddress, const std::shared_ptr<worker::WorkerMasterOCApi> &api)
    {
        apiByAddr_[masterAddress.ToString()] = api;
    }

private:
    std::unordered_map<std::string, std::shared_ptr<worker::WorkerMasterOCApi>> apiByAddr_;
};

#define RETURN_UNSUPPORTED_MASTER_API(method, ...)                                      \
    Status method(__VA_ARGS__) override                                                \
    {                                                                                  \
        return Status(K_RUNTIME_ERROR, "unsupported test master API: " #method);        \
    }

class TestWorkerMasterOCApi : public worker::WorkerMasterOCApi {
public:
    TestWorkerMasterOCApi(const HostPort &masterAddr, const HostPort &localAddr)
        : WorkerMasterOCApi(localAddr, nullptr), masterAddr_(masterAddr)
    {
    }

    Status Init() override
    {
        return Status::OK();
    }

    RETURN_UNSUPPORTED_MASTER_API(CreateMeta, master::CreateMetaReqPb &, master::CreateMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(ReportResource, master::ResourceReportReqPb &, master::ResourceReportRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(ReportRebalanceResult, master::ReportRebalanceResultReqPb &,
                                  master::ReportRebalanceResultRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(CreateMultiMeta, master::CreateMultiMetaReqPb &, master::CreateMultiMetaRspPb &, bool)
    RETURN_UNSUPPORTED_MASTER_API(CreateCopyMeta, master::CreateCopyMetaReqPb &, master::CreateCopyMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(CreateMultiCopyMeta, master::CreateMultiCopyMetaReqPb &,
                                  master::CreateMultiCopyMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(QueryMeta, master::QueryMetaReqPb &, uint64_t, master::QueryMetaRspPb &,
                                  std::vector<RpcMessage> &)
    RETURN_UNSUPPORTED_MASTER_API(RemoveMeta, master::RemoveMetaReqPb &, master::RemoveMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GIncNestedRef, master::GIncNestedRefReqPb &, master::GIncNestedRefRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GDecNestedRef, master::GDecNestedRefReqPb &, master::GDecNestedRefRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(UpdateMeta, master::UpdateMetaReqPb &, master::UpdateMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(DeleteAllCopyMeta, master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GDecreaseMasterRef, const std::vector<std::string> &,
                                  std::unordered_set<std::string> &, std::vector<std::string> &, const std::string &)
    RETURN_UNSUPPORTED_MASTER_API(ReleaseGRefs, master::ReleaseGRefsReqPb &, master::ReleaseGRefsRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GIncreaseMasterRef, master::GIncreaseReqPb &, master::GIncreaseRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GDecreaseMasterRef, master::GDecreaseReqPb &, master::GDecreaseRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(QueryGlobalRefNum, QueryGlobalRefNumReqPb &, QueryGlobalRefNumRspCollectionPb &)

    Status PushMetadataToMaster(master::PushMetaToMasterReqPb &req, master::PushMetaToMasterRspPb &rsp) override
    {
        (void)rsp;
        for (const auto &meta : req.metas()) {
            isRecoveredFlags_.emplace_back(meta.is_recovered());
        }
        batchSizes_.emplace_back(req.metas_size());
        return returnStatus_;
    }

    RETURN_UNSUPPORTED_MASTER_API(RollbackSeal, const std::string &, uint32_t)
    RETURN_UNSUPPORTED_MASTER_API(Expire, master::ExpireReqPb &, master::ExpireRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(ReconcileMembershipChange, master::ReconciliationQueryPb &,
                                  master::ReconciliationRspPb &)

    std::string GetHostPort() override
    {
        return masterAddr_.ToString();
    }

    RETURN_UNSUPPORTED_MASTER_API(PutP2PMeta, PutP2PMetaReqPb &, PutP2PMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(
        SubscribeReceiveEvent, SubscribeReceiveEventReqPb &,
        std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>>,
        std::shared_ptr<AsyncRpcRequestManager> &)
    RETURN_UNSUPPORTED_MASTER_API(GetP2PMeta, GetP2PMetaReqPb &,
                                  std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>>,
                                  std::shared_ptr<AsyncRpcRequestManager> &)
    RETURN_UNSUPPORTED_MASTER_API(SendRootInfo, SendRootInfoReqPb &, SendRootInfoRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(RecvRootInfo, RecvRootInfoReqPb &,
                                  std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>>,
                                  std::shared_ptr<AsyncRpcRequestManager> &)
    RETURN_UNSUPPORTED_MASTER_API(GetDataInfo, GetDataInfoReqPb &,
                                  std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &,
                                  const int64_t, std::shared_ptr<AsyncRpcRequestManager> &)
    RETURN_UNSUPPORTED_MASTER_API(AckRecvFinish, AckRecvFinishReqPb &, AckRecvFinishRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(RemoveP2PLocation, RemoveP2PLocationReqPb &, RemoveP2PLocationRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GetObjectLocations, master::GetObjectLocationsReqPb &,
                                  master::GetObjectLocationsRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GetObjectLocations, master::GetObjectLocationsReqPb &,
                                  master::GetObjectLocationsRspPb &, int64_t)
    RETURN_UNSUPPORTED_MASTER_API(ReleaseMetaData, ReleaseMetaDataReqPb &, ReleaseMetaDataRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(ReplacePrimary, master::ReplacePrimaryReqPb &, master::ReplacePrimaryRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(PureQueryMeta, master::PureQueryMetaReqPb &, master::PureQueryMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(CheckObjectDataLocation, master::CheckObjectDataLocationReqPb &,
                                  master::CheckObjectDataLocationRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(RollbackMultiMeta, master::RollbackMultiMetaReqPb &, master::RollbackMultiMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GetMetaInfo, GetMetaInfoReqPb &, GetMetaInfoRspPb &)

    const std::vector<int> &GetBatchSizes() const
    {
        return batchSizes_;
    }

    const std::vector<bool> &GetIsRecoveredFlags() const
    {
        return isRecoveredFlags_;
    }

private:
    HostPort masterAddr_;
    Status returnStatus_{ Status::OK() };
    std::vector<int> batchSizes_;
    std::vector<bool> isRecoveredFlags_;
};

class WorkerRemoteMasterRpcDiagnosticTest : public CommonTest {};

TEST_F(WorkerRemoteMasterRpcDiagnosticTest, SealCreateMetaTimeoutReportsRpcDiagnostic)
{
    HostPort local("127.0.0.1", 18500);
    HostPort master("127.0.0.1", 18501);
    worker::WorkerRemoteMasterOCApi api(master, local, nullptr);
    master::CreateMetaReqPb req;
    master::CreateMetaRspPb rsp;
    req.mutable_meta()->set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED));

    GetRequestContext()->reqTimeoutDuration.Init(0);
    Status rc = api.CreateMeta(req, rsp);
    GetRequestContext()->reqTimeoutDuration.Init();

    ASSERT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
    EXPECT_NE(rc.GetMsg().find("[" + local.ToString() + "]-CreateMeta->[" + master.ToString() + "]"), std::string::npos)
        << rc.ToString();
}

#undef RETURN_UNSUPPORTED_MASTER_API

class MetaDataRecoveryManagerTest : public CommonTest {
public:
    static void SetUpTestSuite()
    {
        DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->Init(64UL * 1024UL * 1024UL));
    }

    static void TearDownTestSuite()
    {
        datasystem::memory::Allocator::Instance()->Shutdown();
    }

    void SetUp() override
    {
        CommonTest::SetUp();

        localAddress_ = HostPort("127.0.0.1", 18500);
        objectTable_ = std::make_shared<ObjectTable>();
        memCpyThreadPool_ = std::make_shared<ThreadPool>(1);
        workerMasterApiManager_ =
            std::make_shared<TestWorkerMasterApiManager>(localAddress_, GetTestMetadataRoute());
        clusterAccess_.checkConnection = [](const HostPort &) { return Status::OK(); };
        recoveredContentSaver_ = [this](const ObjectMetaPb &meta,
                                         const std::shared_ptr<std::stringstream> &contentStream,
                                         const std::shared_ptr<SafeObjType> &entry) {
            const auto content = contentStream->str();
            std::vector<RpcMessage> payloads;
            RETURN_IF_NOT_OK(CopyAndSplitBuffer("", content.data(), content.size(), payloads));
            ObjectKV objectKV(meta.object_key(), *entry);
            RETURN_IF_NOT_OK(SaveBinaryObjectToMemory(objectKV, payloads, nullptr, memCpyThreadPool_, false));
            (*entry)->stateInfo.SetCacheInvalid(false);
            (*entry)->stateInfo.SetIncompleted(false);
            return Status::OK();
        };
        manager_ = std::make_unique<MetaDataRecoveryManager>(localAddress_, objectTable_, clusterAccess_,
                                                             workerMasterApiManager_, GetTestMetadataRoute(), 128,
                                                             nullptr, memCpyThreadPool_, recoveredContentSaver_);
    }

    void TearDown() override
    {
        manager_.reset();
        memCpyThreadPool_.reset();
        CommonTest::TearDown();
    }

    void AddObject(const std::string &objectKey, uint64_t version = 1, uint64_t dataSize = 1024)
    {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        obj->SetDataSize(dataSize);
        obj->SetCreateTime(version);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->stateInfo.SetPrimaryCopy(true);
        objectTable_->Insert(objectKey, std::move(obj));
    }

protected:
    HostPort localAddress_;
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<ThreadPool> memCpyThreadPool_;
    std::shared_ptr<TestWorkerMasterApiManager> workerMasterApiManager_;
    MetaDataRecoveryManager::ClusterAccess clusterAccess_;
    MetaDataRecoveryManager::RecoveredContentSaver recoveredContentSaver_;
    std::unique_ptr<MetaDataRecoveryManager> manager_;
};

TEST_F(MetaDataRecoveryManagerTest, BuildGroupedByMasterUsesTopologyPlacement)
{
    MetadataTestPlacementFacade placement;
    HostPort masterAddr;
    masterAddr.ParseString("127.0.0.1:18501");
    placement.SetOwner("key-ok", masterAddr);

    worker::MetadataRouteResolver metadataRoute(&placement, worker::MetadataRouteOptions{});
    MetaDataRecoveryManager manager(localAddress_, objectTable_, clusterAccess_, workerMasterApiManager_,
                                    metadataRoute, 128, nullptr, memCpyThreadPool_, recoveredContentSaver_);
    auto grouped = manager.BuildGroupedByMaster({ "key-ok", "key-missing" }, "");

    ASSERT_NE(grouped.find(masterAddr), grouped.end());
    EXPECT_THAT(grouped[masterAddr], ElementsAre("key-ok"));
    ASSERT_NE(grouped.find(HostPort()), grouped.end());
    EXPECT_THAT(grouped[HostPort()], ElementsAre("key-missing"));
}

ObjectMetaPb BuildRecoverMeta(const std::string &objectKey, WriteMode writeMode,
                              const std::string &primaryAddress = "127.0.0.1:18500")
{
    ObjectMetaPb meta;
    meta.set_object_key(objectKey);
    meta.set_data_size(2048);
    meta.set_version(9);
    meta.set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED));
    meta.set_primary_address(primaryAddress);
    auto *config = meta.mutable_config();
    config->set_write_mode(static_cast<uint32_t>(writeMode));
    config->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
    config->set_consistency_type(static_cast<uint32_t>(ConsistencyType::PRAM));
    config->set_cache_type(static_cast<uint32_t>(CacheType::MEMORY));
    return meta;
}

TEST_F(MetaDataRecoveryManagerTest, RecoverMetadataBatchSizeShouldNotExceed500)
{
    metrics::ResetKvMetricsForTest();
    Raii restoreMetrics([] {
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    ASSERT_TRUE(metrics::InitKvMetrics().IsOk());
    constexpr size_t totalObjects = 1201;
    std::vector<std::string> objectKeys;
    objectKeys.reserve(totalObjects);
    for (size_t i = 0; i < totalObjects; ++i) {
        auto objectKey = "recovery_obj_" + std::to_string(i);
        AddObject(objectKey);
        objectKeys.emplace_back(std::move(objectKey));
    }

    HostPort masterAddr("127.0.0.1", 18501);
    auto workerMasterApi = std::make_shared<TestWorkerMasterOCApi>(masterAddr, localAddress_);
    workerMasterApiManager_->SetApi(masterAddr, workerMasterApi);

    auto result = manager_->SendRecoverRequest(masterAddr, objectKeys);
    DS_ASSERT_OK(result.status);
    EXPECT_TRUE(result.failedIds.empty());

    const auto &batchSizes = workerMasterApi->GetBatchSizes();
    ASSERT_THAT(batchSizes, ElementsAre(500, 500, 201));
    for (const auto isRecovered : workerMasterApi->GetIsRecoveredFlags()) {
        EXPECT_TRUE(isRecovered);
    }
    std::string summary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        summary += part;
    }
    EXPECT_NE(summary.find("\"name\":\"worker_metadata_recovery_batch_latency\""), std::string::npos);
}

TEST_F(MetaDataRecoveryManagerTest, SuccessfulPushAfterConnectionProbeFailureCountsAsRecovered)
{
    const std::string objectKey = "probe-failed-push-recovered";
    AddObject(objectKey);
    HostPort masterAddr("127.0.0.1", 18501);
    auto workerMasterApi = std::make_shared<TestWorkerMasterOCApi>(masterAddr, localAddress_);
    workerMasterApiManager_->SetApi(masterAddr, workerMasterApi);
    MetaDataRecoveryManager::ClusterAccess transientProbeFailure{ [](const HostPort &) {
        return Status(K_RPC_UNAVAILABLE, "transient connection probe failure");
    } };
    MetaDataRecoveryManager manager(localAddress_, objectTable_, transientProbeFailure, workerMasterApiManager_,
                                    GetTestMetadataRoute());

    auto result = manager.SendRecoverRequest(masterAddr, { objectKey });

    EXPECT_TRUE(result.status.IsOk());
    EXPECT_TRUE(result.failedIds.empty());
    EXPECT_EQ(result.recoveredCount, 1U);
}

TEST_F(MetaDataRecoveryManagerTest, RecoverMetadataShouldReturnAllFailedIdsWhenMasterUnreachable)
{
    std::vector<std::string> objectKeys{ "obj_failed_1", "obj_failed_2", "obj_failed_3" };
    for (const auto &objectKey : objectKeys) {
        AddObject(objectKey);
    }

    HostPort unreachableMaster("127.0.0.1", 18502);
    auto result = manager_->SendRecoverRequest(unreachableMaster, objectKeys);
    DS_ASSERT_NOT_OK(result.status);
    EXPECT_EQ(result.status.GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(result.failedIds.size(), objectKeys.size());
}

TEST_F(MetaDataRecoveryManagerTest, RecoverLocalEntries)
{
    std::vector<ObjectMetaPb> recoverMetas;
    recoverMetas.emplace_back(BuildRecoverMeta("obj_l2", WriteMode::WRITE_THROUGH_L2_CACHE, "127.0.0.1:18501"));
    recoverMetas.emplace_back(BuildRecoverMeta("obj_mem_only", WriteMode::NONE_L2_CACHE));

    std::vector<std::string> recoveredObjectKeys;
    DS_ASSERT_OK(manager_->RecoverLocalEntries(recoverMetas, recoveredObjectKeys));
    ASSERT_EQ(recoveredObjectKeys, std::vector<std::string>({ "obj_l2" }));

    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get("obj_l2", entry));
    ASSERT_TRUE(entry->RLock().IsOk());
    EXPECT_EQ((*entry)->GetDataSize(), 2048);
    EXPECT_EQ((*entry)->GetCreateTime(), 9);
    EXPECT_EQ((*entry)->GetMetadataSize(), 128);
    EXPECT_EQ((*entry)->GetAddress(), "127.0.0.1:18500");
    EXPECT_TRUE((*entry)->stateInfo.IsPrimaryCopy());
    EXPECT_FALSE((*entry)->stateInfo.IsCacheInvalid());
    EXPECT_FALSE((*entry)->stateInfo.IsIncomplete());
    EXPECT_TRUE((*entry)->HasL2Cache());
    entry->RUnlock();

    std::shared_ptr<SafeObjType> skippedEntry;
    EXPECT_EQ(objectTable_->Get("obj_mem_only", skippedEntry).GetCode(), K_NOT_FOUND);
}

TEST_F(MetaDataRecoveryManagerTest, RecoverLocalEntriesLoadsPayloadIntoMemory)
{
    std::vector<ObjectMetaPb> recoverMetas;
    recoverMetas.emplace_back(BuildRecoverMeta("tenant/payload_obj", WriteMode::WRITE_THROUGH_L2_CACHE));
    auto content = std::make_shared<std::stringstream>();
    const std::string expected = "payload_for_restart_recovery";
    (*content) << expected;
    std::unordered_map<std::string, std::shared_ptr<std::stringstream>> recoveredContents{ { "tenant/payload_obj",
                                                                                             content } };

    std::vector<std::string> recoveredObjectKeys;
    DS_ASSERT_OK(manager_->RecoverLocalEntries(recoverMetas, recoveredContents, recoveredObjectKeys));
    ASSERT_EQ(recoveredObjectKeys, std::vector<std::string>({ "tenant/payload_obj" }));

    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get("tenant/payload_obj", entry));
    ASSERT_TRUE(entry->RLock().IsOk());
    ASSERT_NE((*entry)->GetShmUnit(), nullptr);
    ASSERT_EQ((*entry)->GetDataSize(), expected.size());
    ASSERT_FALSE((*entry)->stateInfo.IsCacheInvalid());
    auto *data = static_cast<const char *>((*entry)->GetShmUnit()->GetPointer()) + (*entry)->GetMetadataSize();
    EXPECT_EQ(std::string(data, expected.size()), expected);
    entry->RUnlock();
}

TEST_F(MetaDataRecoveryManagerTest, RecoverableLocalDataRebuildsOrUpdatesMetadata)
{
    const std::string objectKey = "tenant/recoverable_update_obj";
    constexpr uint64_t oldVersion = 3;
    AddObject(objectKey, oldVersion, 512);

    auto recoverMeta = BuildRecoverMeta(objectKey, WriteMode::WRITE_THROUGH_L2_CACHE, "127.0.0.1:18501");
    recoverMeta.set_version(oldVersion + 1);
    recoverMeta.set_data_size(4096);

    std::vector<std::string> recoveredObjectKeys;
    DS_ASSERT_OK(manager_->RecoverLocalEntries({ recoverMeta }, recoveredObjectKeys));
    ASSERT_THAT(recoveredObjectKeys, ElementsAre(objectKey));

    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
    ASSERT_TRUE(entry->RLock().IsOk());
    EXPECT_EQ((*entry)->GetCreateTime(), oldVersion + 1);
    EXPECT_EQ((*entry)->GetDataSize(), 4096);
    EXPECT_EQ((*entry)->GetMetadataSize(), 128);
    EXPECT_EQ((*entry)->GetAddress(), localAddress_.ToString());
    EXPECT_TRUE((*entry)->stateInfo.IsPrimaryCopy());
    EXPECT_TRUE((*entry)->HasL2Cache());
    entry->RUnlock();
}

TEST_F(MetaDataRecoveryManagerTest, RecoverLocalEntriesSkipsOlderMetaWhenLocalEntryIsNewer)
{
    const std::string objectKey = "tenant/newer_local_obj";
    constexpr uint64_t newerVersion = 10;
    constexpr uint64_t newerDataSize = 4096;
    AddObject(objectKey, newerVersion, newerDataSize);

    auto oldMeta = BuildRecoverMeta(objectKey, WriteMode::WRITE_THROUGH_L2_CACHE);
    oldMeta.set_version(newerVersion - 1);
    oldMeta.set_data_size(2048);
    auto oldContent = std::make_shared<std::stringstream>();
    (*oldContent) << "old_payload";
    std::unordered_map<std::string, std::shared_ptr<std::stringstream>> recoveredContents{ { objectKey, oldContent } };

    std::vector<std::string> recoveredObjectKeys;
    DS_ASSERT_OK(manager_->RecoverLocalEntries({ oldMeta }, recoveredContents, recoveredObjectKeys));
    EXPECT_TRUE(recoveredObjectKeys.empty());

    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
    ASSERT_TRUE(entry->RLock().IsOk());
    EXPECT_EQ((*entry)->GetCreateTime(), newerVersion);
    EXPECT_EQ((*entry)->GetDataSize(), newerDataSize);
    EXPECT_TRUE((*entry)->stateInfo.IsPrimaryCopy());
    entry->RUnlock();
}

class MetadataRecoverySelectorTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        objectTable_ = std::make_shared<ObjectTable>();
        selector_ = std::make_unique<MetadataRecoverySelector>(objectTable_);
    }

    void AddObject(const std::string &objectKey, WriteMode writeMode)
    {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        obj->modeInfo.SetWriteMode(writeMode);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        DS_ASSERT_OK(objectTable_->Insert(objectKey, std::move(obj)));
    }

    static std::unique_ptr<ObjCacheShmUnit> NewObject()
    {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        return obj;
    }

    static std::vector<std::string> ReadSnapshot(ObjectTable &table, size_t visitBudget)
    {
        auto cursor = table.BeginRecoverySnapshot();
        std::vector<std::string> result;
        bool done = false;
        while (!done) {
            std::vector<std::string> batch;
            auto status = table.NextRecoverySnapshotBatch(cursor, visitBudget, batch, done);
            EXPECT_TRUE(status.IsOk()) << status.ToString();
            if (status.IsError()) {
                break;
            }
            EXPECT_LE(batch.size(), visitBudget);
            result.insert(result.end(), batch.begin(), batch.end());
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    static std::string KeyInRecoveryShard(const std::string &prefix, size_t shard)
    {
        for (size_t suffix = 0;; ++suffix) {
            auto key = prefix + std::to_string(suffix);
            if (std::hash<std::string>{}(key) % 64 == shard) {
                return key;
            }
        }
    }

    static bool WaitForInject(const std::string &name, std::chrono::milliseconds timeout)
    {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (inject::GetExecuteCount(name) > 0) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return false;
    }

protected:
    std::shared_ptr<ObjectTable> objectTable_;
    std::unique_ptr<MetadataRecoverySelector> selector_;
};

TEST_F(MetadataRecoverySelectorTest, ObjectTableRecoverySnapshotTracksInsertPaths)
{
    DS_ASSERT_OK(objectTable_->Insert("unique", NewObject()));
    EXPECT_EQ(objectTable_->Insert("unique", NewObject()).GetCode(), K_DUPLICATED);

    auto sharedObject = std::make_shared<SafeObjType>(NewObject());
    DS_ASSERT_OK(objectTable_->Insert("shared", sharedObject));

    auto insertOrGetObject = std::make_shared<SafeObjType>(NewObject());
    objectTable_->InsertOrGet("insert-or-get", insertOrGetObject);
    auto duplicateObject = std::make_shared<SafeObjType>(NewObject());
    objectTable_->InsertOrGet("insert-or-get", duplicateObject);
    EXPECT_EQ(duplicateObject, insertOrGetObject);

    EXPECT_THAT(ReadSnapshot(*objectTable_, 2), ElementsAre("insert-or-get", "shared", "unique"));
}

TEST_F(MetadataRecoverySelectorTest, ObjectTableRecoverySnapshotTracksReservePaths)
{
    std::shared_ptr<SafeObjType> reserved;
    DS_ASSERT_OK(objectTable_->ReserveAndLock("reserve", reserved));
    reserved->WUnlock();

    std::shared_ptr<SafeObjType> reserveOrGet;
    bool isInsert = false;
    DS_ASSERT_OK(objectTable_->ReserveGetAndLock("reserve-or-get", reserveOrGet, isInsert));
    EXPECT_TRUE(isInsert);
    reserveOrGet->WUnlock();

    reserveOrGet.reset();
    DS_ASSERT_OK(objectTable_->ReserveGetAndLock("reserve-or-get", reserveOrGet, isInsert));
    EXPECT_FALSE(isInsert);
    reserveOrGet->WUnlock();

    EXPECT_THAT(ReadSnapshot(*objectTable_, 1), ElementsAre("reserve", "reserve-or-get"));
}

TEST_F(MetadataRecoverySelectorTest, ObjectTableRecoverySnapshotTracksEraseAndReinsertGeneration)
{
    DS_ASSERT_OK(objectTable_->Insert("erase-by-key", NewObject()));
    DS_ASSERT_OK(objectTable_->Insert("erase-by-object", NewObject()));
    DS_ASSERT_OK(objectTable_->Insert("reinsert", NewObject()));
    auto snapshot = objectTable_->BeginRecoverySnapshot();

    DS_ASSERT_OK(objectTable_->Erase("erase-by-key"));
    std::shared_ptr<SafeObjType> eraseEntry;
    DS_ASSERT_OK(objectTable_->Get("erase-by-object", eraseEntry));
    DS_ASSERT_OK(objectTable_->Erase("erase-by-object", *eraseEntry));
    EXPECT_EQ(objectTable_->Erase("missing").GetCode(), K_NOT_FOUND);
    DS_ASSERT_OK(objectTable_->Erase("reinsert"));
    DS_ASSERT_OK(objectTable_->Insert("reinsert", NewObject()));
    DS_ASSERT_OK(objectTable_->Insert("new-after-snapshot", NewObject()));

    std::vector<std::string> keys;
    bool done = false;
    while (!done) {
        std::vector<std::string> batch;
        DS_ASSERT_OK(objectTable_->NextRecoverySnapshotBatch(snapshot, 1, batch, done));
        keys.insert(keys.end(), batch.begin(), batch.end());
    }
    EXPECT_TRUE(keys.empty());
    EXPECT_THAT(ReadSnapshot(*objectTable_, 1), ElementsAre("new-after-snapshot", "reinsert"));
}

TEST_F(MetadataRecoverySelectorTest, ObjectTableRecoverySnapshotRejectsReinsertAfterKeyWasVisited)
{
    DS_ASSERT_OK(objectTable_->Insert("reinsert", NewObject()));
    auto snapshot = objectTable_->BeginRecoverySnapshot();

    std::vector<std::string> keys;
    bool done = false;
    DS_ASSERT_OK(objectTable_->NextRecoverySnapshotBatch(snapshot, 1, keys, done));
    ASSERT_THAT(keys, ElementsAre("reinsert"));

    DS_ASSERT_OK(objectTable_->Erase("reinsert"));
    DS_ASSERT_OK(objectTable_->Insert("reinsert", NewObject()));
    std::shared_ptr<SafeObjType> entry;
    EXPECT_EQ(objectTable_->GetRecoverySnapshotObject(snapshot, "reinsert", entry).GetCode(), K_NOT_FOUND);
}

TEST_F(MetadataRecoverySelectorTest, ObjectTableRecoverySnapshotBoundsEachVisitBatch)
{
    constexpr size_t objectCount = 130;
    constexpr size_t visitBudget = 7;
    for (size_t i = 0; i < objectCount; ++i) {
        DS_ASSERT_OK(objectTable_->Insert("key-" + std::to_string(i), NewObject()));
    }

    EXPECT_EQ(ReadSnapshot(*objectTable_, visitBudget).size(), objectCount);
}

TEST_F(MetadataRecoverySelectorTest, ObjectTableRecoverySnapshotFinishesDuringContinuousInsertion)
{
    constexpr size_t shard = 0;
    DS_ASSERT_OK(objectTable_->Insert(KeyInRecoveryShard("a-initial-", shard), NewObject()));
    DS_ASSERT_OK(objectTable_->Insert(KeyInRecoveryShard("b-tail-", shard), NewObject()));
    auto snapshot = objectTable_->BeginRecoverySnapshot();

    bool done = false;
    size_t round = 0;
    for (; round < 10 && !done; ++round) {
        std::vector<std::string> keys;
        DS_ASSERT_OK(objectTable_->NextRecoverySnapshotBatch(snapshot, 1, keys, done));
        for (size_t ahead = 0; ahead < 2; ++ahead) {
            auto prefix = FormatString("z-growing-%06zu-%zu-", round, ahead);
            DS_ASSERT_OK(objectTable_->Insert(KeyInRecoveryShard(prefix, shard), NewObject()));
        }
    }
    EXPECT_TRUE(done);
    EXPECT_LE(round, 3U);
}

TEST_F(MetadataRecoverySelectorTest, ObjectTableRecoverySnapshotStaysConsistentAfterSameKeyMutationRaces)
{
    constexpr size_t iterationCount = 100;
    for (size_t i = 0; i < iterationCount; ++i) {
        const std::string key = "race-" + std::to_string(i);
        DS_ASSERT_OK(objectTable_->Insert(key, NewObject()));
        std::promise<void> start;
        auto startFuture = start.get_future().share();
        auto eraseFuture = std::async(std::launch::async, [&] {
            startFuture.wait();
            return objectTable_->Erase(key);
        });
        auto insertFuture = std::async(std::launch::async, [&] {
            auto obj = NewObject();
            startFuture.wait();
            return objectTable_->Insert(key, std::move(obj));
        });
        start.set_value();
        (void)eraseFuture.get();
        (void)insertFuture.get();

        const bool inTable = objectTable_->Contains(key).IsOk();
        const auto snapshotKeys = ReadSnapshot(*objectTable_, 8);
        const bool inSnapshot = std::binary_search(snapshotKeys.begin(), snapshotKeys.end(), key);
        EXPECT_EQ(inSnapshot, inTable) << key;
        if (inTable) {
            DS_ASSERT_OK(objectTable_->Erase(key));
        }
    }
}

TEST_F(MetadataRecoverySelectorTest, ObjectTableDoesNotPublishBeforeRecoveryGenerationCommit)
{
    constexpr const char *injectPoint = "ObjectTable.Insert.AfterTableInsert";
    DS_ASSERT_OK(inject::Set(injectPoint, "1*call(200)"));
    Raii clearInject([injectPoint] { (void)inject::Clear(injectPoint); });

    auto insertFuture = std::async(std::launch::async, [&] { return objectTable_->Insert("pending", NewObject()); });
    ASSERT_TRUE(WaitForInject(injectPoint, std::chrono::seconds(1)));

    std::shared_ptr<SafeObjType> entry;
    auto getFuture = std::async(std::launch::async, [&] { return objectTable_->Get("pending", entry); });
    EXPECT_EQ(getFuture.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    auto snapshot = objectTable_->BeginRecoverySnapshot();

    DS_ASSERT_OK(insertFuture.get());
    DS_ASSERT_OK(getFuture.get());
    std::vector<std::string> keys;
    bool done = false;
    DS_ASSERT_OK(objectTable_->NextRecoverySnapshotBatch(snapshot, 1, keys, done));
    EXPECT_THAT(keys, ElementsAre("pending"));
}

TEST_F(MetadataRecoverySelectorTest, ObjectTableReserveDoesNotInvertObjectAndShardLockOrder)
{
    constexpr const char *injectPoint = "ObjectTable.ReserveGetAndLock.BeforeTableCall";
    DS_ASSERT_OK(objectTable_->Insert("locked", NewObject()));
    DS_ASSERT_OK(inject::Set(injectPoint, "1*call(200)"));
    Raii clearInject([injectPoint] { (void)inject::Clear(injectPoint); });

    std::promise<void> objectLocked;
    auto objectLockedFuture = objectLocked.get_future();
    std::promise<void> eraseAllowed;
    auto eraseAllowedFuture = eraseAllowed.get_future().share();
    auto eraseFuture = std::async(std::launch::async, [&] {
        std::shared_ptr<SafeObjType> entry;
        RETURN_IF_NOT_OK(objectTable_->GetAndLock("locked", entry));
        objectLocked.set_value();
        eraseAllowedFuture.wait();
        auto status = objectTable_->Erase("locked", *entry);
        entry->WUnlock();
        return status;
    });
    ASSERT_EQ(objectLockedFuture.wait_for(std::chrono::seconds(1)), std::future_status::ready);

    std::shared_ptr<SafeObjType> reserved;
    bool isInsert = false;
    auto reserveFuture = std::async(std::launch::async, [&] {
        auto status = objectTable_->ReserveGetAndLock("locked", reserved, isInsert);
        if (status.IsOk()) {
            reserved->WUnlock();
        }
        return status;
    });
    ASSERT_TRUE(WaitForInject(injectPoint, std::chrono::seconds(1)));
    eraseAllowed.set_value();

    ASSERT_EQ(eraseFuture.wait_for(std::chrono::milliseconds(500)), std::future_status::ready);
    DS_ASSERT_OK(eraseFuture.get());
    ASSERT_EQ(reserveFuture.wait_for(std::chrono::milliseconds(500)), std::future_status::ready);
    DS_ASSERT_OK(reserveFuture.get());
    EXPECT_TRUE(isInsert);
}

TEST_F(MetadataRecoverySelectorTest, SelectShouldRespectIncludeL2Flag)
{
    AddObject("mem_only", WriteMode::NONE_L2_CACHE);
    AddObject("l2_obj", WriteMode::WRITE_THROUGH_L2_CACHE);

    std::vector<std::string> objectKeys;
    selector_->Select([](const std::string &) { return true; }, false, objectKeys);
    std::sort(objectKeys.begin(), objectKeys.end());
    ASSERT_THAT(objectKeys, ElementsAre("mem_only"));

    objectKeys.clear();
    selector_->Select([](const std::string &) { return true; }, true, objectKeys);
    std::sort(objectKeys.begin(), objectKeys.end());
    ASSERT_THAT(objectKeys, ElementsAre("l2_obj", "mem_only"));
}

TEST_F(MetadataRecoverySelectorTest, SelectionPublishesBoundedScanMetrics)
{
    metrics::ResetKvMetricsForTest();
    Raii restoreMetrics([] {
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    ASSERT_TRUE(metrics::InitKvMetrics().IsOk());
    AddObject("candidate", WriteMode::NONE_L2_CACHE);

    std::vector<std::string> objectKeys;
    selector_->Select([](const std::string &) { return true; }, false, objectKeys);

    std::string summary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        summary += part;
    }
    EXPECT_NE(summary.find("\"name\":\"worker_object_table_lock_hold_latency\""), std::string::npos);
    EXPECT_NE(summary.find("\"name\":\"worker_recovery_candidate_count\""), std::string::npos);
}

TEST_F(MetadataRecoverySelectorTest, SelectionRequestShouldPreserveInputAndValidateRanges)
{
    ClearDataReqPb req;
    auto *range = req.add_ranges();
    range->set_from(10);
    range->set_end(20);
    auto selectReq = MetadataRecoverySelector::BuildSelectionRequest(req, true);
    ASSERT_FALSE(selectReq.Empty());
    ASSERT_EQ(selectReq.ranges.size(), 1);
    EXPECT_EQ(selectReq.ranges[0].first, 10);
    EXPECT_EQ(selectReq.ranges[0].second, 20);
    EXPECT_TRUE(selectReq.includeL2CacheIds);

    std::vector<std::string> objectKeys;
    auto status = selector_->Select(selectReq, objectKeys);
    DS_ASSERT_OK(status);
    EXPECT_TRUE(objectKeys.empty());

    MetadataRecoverySelector::SelectionRequest emptyReq;
    status = selector_->Select(emptyReq, objectKeys);
    DS_ASSERT_NOT_OK(status);
    EXPECT_EQ(status.GetCode(), K_INVALID);
}

TEST_F(MetadataRecoverySelectorTest, SelectionReleasesObjectTableLockBeforeMatchAndBatching)
{
    AddObject("existing", WriteMode::NONE_L2_CACHE);

    std::promise<void> matchEntered;
    auto matchEnteredFuture = matchEntered.get_future();
    std::promise<void> releaseMatch;
    auto releaseMatchFuture = releaseMatch.get_future().share();
    std::atomic<bool> firstMatch{ true };
    std::vector<std::string> objectKeys;
    auto selectionFuture = std::async(std::launch::async, [&] {
        selector_->Select(
            [&](const std::string &) {
                if (firstMatch.exchange(false)) {
                    matchEntered.set_value();
                    releaseMatchFuture.wait();
                }
                return true;
            },
            false, objectKeys);
    });

    if (matchEnteredFuture.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        releaseMatch.set_value();
        selectionFuture.wait();
        FAIL() << "metadata recovery selection did not enter the match phase";
        return;
    }

    std::promise<void> insertReady;
    auto insertReadyFuture = insertReady.get_future();
    std::promise<void> startInsert;
    auto startInsertFuture = startInsert.get_future().share();
    auto concurrentInsert = std::async(std::launch::async, [&] {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        insertReady.set_value();
        startInsertFuture.wait();
        return objectTable_->Insert("concurrent", std::move(obj));
    });

    if (insertReadyFuture.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        startInsert.set_value();
        releaseMatch.set_value();
        concurrentInsert.wait();
        selectionFuture.wait();
        FAIL() << "concurrent insert thread did not reach the start barrier";
        return;
    }
    startInsert.set_value();
    auto insertStatus = concurrentInsert.wait_for(std::chrono::seconds(1));
    if (insertStatus != std::future_status::ready) {
        releaseMatch.set_value();
    }
    EXPECT_EQ(insertStatus, std::future_status::ready)
        << "object-table insert stayed blocked after selection entered per-object matching";

    if (insertStatus == std::future_status::ready) {
        releaseMatch.set_value();
    }
    DS_ASSERT_OK(concurrentInsert.get());
    selectionFuture.get();
    EXPECT_THAT(objectKeys, ElementsAre("existing"));
}

TEST_F(MetadataRecoverySelectorTest, MetadataRecoveryUsesBoundedGenerationSnapshot)
{
    constexpr size_t objectCount = 1024;
    constexpr size_t snapshotBatchSize = 64;
    const std::string erasedTailKey = KeyInRecoveryShard("zz-existing-", 63);
    for (size_t i = 0; i < objectCount - 1; ++i) {
        AddObject("existing-" + std::to_string(i), WriteMode::NONE_L2_CACHE);
    }
    AddObject(erasedTailKey, WriteMode::NONE_L2_CACHE);

    std::promise<void> firstBatchCompleted;
    auto firstBatchCompletedFuture = firstBatchCompleted.get_future();
    std::promise<void> releaseBatchYield;
    auto releaseBatchYieldFuture = releaseBatchYield.get_future().share();
    std::atomic<bool> firstBatch{ true };
    MetadataRecoverySelector batchedSelector(objectTable_, snapshotBatchSize, [&] {
        if (firstBatch.exchange(false)) {
            firstBatchCompleted.set_value();
            releaseBatchYieldFuture.wait();
        }
    });

    std::vector<std::string> objectKeys;
    auto selectionFuture = std::async(std::launch::async, [&] {
        batchedSelector.Select([](const std::string &) { return true; }, false, objectKeys);
    });
    if (firstBatchCompletedFuture.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        releaseBatchYield.set_value();
        selectionFuture.wait();
        FAIL() << "metadata recovery selection did not yield after its first snapshot batch";
        return;
    }

    std::promise<void> insertReady;
    auto insertReadyFuture = insertReady.get_future();
    std::promise<void> eraseReady;
    auto eraseReadyFuture = eraseReady.get_future();
    std::promise<void> startMutations;
    auto startMutationsFuture = startMutations.get_future().share();
    auto concurrentInsert = std::async(std::launch::async, [&] {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        insertReady.set_value();
        startMutationsFuture.wait();
        return objectTable_->Insert("zzz-concurrent", std::move(obj));
    });
    auto concurrentErase = std::async(std::launch::async, [&] {
        eraseReady.set_value();
        startMutationsFuture.wait();
        return objectTable_->Erase(erasedTailKey);
    });

    const bool mutationsReady = insertReadyFuture.wait_for(std::chrono::seconds(1)) == std::future_status::ready
                                && eraseReadyFuture.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    startMutations.set_value();
    if (!mutationsReady) {
        releaseBatchYield.set_value();
        concurrentInsert.wait();
        concurrentErase.wait();
        selectionFuture.wait();
        FAIL() << "concurrent object-table mutations did not reach the start barrier";
        return;
    }

    auto insertStatus = concurrentInsert.wait_for(std::chrono::seconds(1));
    auto eraseStatus = concurrentErase.wait_for(std::chrono::seconds(1));
    if (insertStatus != std::future_status::ready || eraseStatus != std::future_status::ready) {
        releaseBatchYield.set_value();
    }
    EXPECT_EQ(insertStatus, std::future_status::ready);
    EXPECT_EQ(eraseStatus, std::future_status::ready);
    if (insertStatus == std::future_status::ready && eraseStatus == std::future_status::ready) {
        releaseBatchYield.set_value();
    }

    DS_ASSERT_OK(concurrentInsert.get());
    DS_ASSERT_OK(concurrentErase.get());
    selectionFuture.get();
    EXPECT_THAT(objectKeys, Not(Contains(erasedTailKey)));
    EXPECT_THAT(objectKeys, Not(Contains("zzz-concurrent")));
    EXPECT_EQ(objectKeys.size(), objectCount - 1);
}

}  // namespace ut
}  // namespace datasystem
