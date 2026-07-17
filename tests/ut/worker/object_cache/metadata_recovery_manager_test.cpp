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
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/shared_memory/allocator.h"
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
    RETURN_UNSUPPORTED_MASTER_API(DeleteAllCopyMeta, master::DeleteAllCopyMetaReqPb &,
                                  master::DeleteAllCopyMetaRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(GDecreaseMasterRef, const std::vector<std::string> &,
                                  std::unordered_set<std::string> &, std::vector<std::string> &,
                                  const std::string &)
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
    RETURN_UNSUPPORTED_MASTER_API(SubscribeReceiveEvent, SubscribeReceiveEventReqPb &,
                                  std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb,
                                                                           SubscribeReceiveEventReqPb>>,
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
    RETURN_UNSUPPORTED_MASTER_API(RollbackMultiMeta, master::RollbackMultiMetaReqPb &,
                                  master::RollbackMultiMetaRspPb &)
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
    std::unordered_map<std::string, std::shared_ptr<std::stringstream>> recoveredContents{
        { "tenant/payload_obj", content }
    };

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
    std::unordered_map<std::string, std::shared_ptr<std::stringstream>> recoveredContents{
        { objectKey, oldContent }
    };

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

protected:
    std::shared_ptr<ObjectTable> objectTable_;
    std::unique_ptr<MetadataRecoverySelector> selector_;
};

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

}  // namespace ut
}  // namespace datasystem
