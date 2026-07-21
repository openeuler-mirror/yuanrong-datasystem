/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Test interface to HashRingHealthCheck
 */

#include "datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "securec.h"

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#define private public
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"
#undef private
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/object_cache/worker_request_manager.h"
#include "tests/ut/worker/object_cache/test_placement_facade.h"

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_size_limit);
DS_DECLARE_uint32(arena_per_tenant);
DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DECLARE_uint32(max_client_num);

using namespace ::testing;
using namespace datasystem::object_cache;
using namespace datasystem::worker;

namespace datasystem {
namespace ut {

constexpr int64_t K_INJECT_WAIT_POLL_MS = 1;
bool WaitForInjectPointExecuteCount(const std::string &name, uint64_t expectedCount,
                                    std::chrono::milliseconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (inject::GetExecuteCount(name) >= expectedCount) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(K_INJECT_WAIT_POLL_MS));
    }
    return inject::GetExecuteCount(name) >= expectedCount;
}

using MigrateTestPlacementFacade = TestPlacementFacade;

#define RETURN_UNSUPPORTED_MASTER_API(method, ...)                                      \
    Status method(__VA_ARGS__) override                                                \
    {                                                                                  \
        return Status(K_RUNTIME_ERROR, "unsupported test master API: " #method);        \
    }

class MigrateTestWorkerMasterOCApi : public worker::WorkerMasterOCApi {
public:
    MigrateTestWorkerMasterOCApi(const HostPort &masterAddr, const HostPort &localAddr)
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
    Status CreateMultiCopyMeta(master::CreateMultiCopyMetaReqPb &req, master::CreateMultiCopyMetaRspPb &rsp) override
    {
        if (createMultiCopyMeta_) {
            return createMultiCopyMeta_(req, rsp);
        }
        return Status(K_RUNTIME_ERROR, "unsupported test master API: CreateMultiCopyMeta");
    }
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
    RETURN_UNSUPPORTED_MASTER_API(PushMetadataToMaster, master::PushMetaToMasterReqPb &,
                                  master::PushMetaToMasterRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(RollbackSeal, const std::string &, uint32_t)
    RETURN_UNSUPPORTED_MASTER_API(Expire, master::ExpireReqPb &, master::ExpireRspPb &)
    RETURN_UNSUPPORTED_MASTER_API(ReconcileMembershipChange, master::ReconciliationQueryPb &,
                                  master::ReconciliationRspPb &)

    std::string GetHostPort() override
    {
        return masterAddr_.ToString();
    }

    std::function<Status(master::CreateMultiCopyMetaReqPb &, master::CreateMultiCopyMetaRspPb &)>
        createMultiCopyMeta_;

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

private:
    HostPort masterAddr_;
};

#undef RETURN_UNSUPPORTED_MASTER_API

class MigrateTestWorkerMasterApiManager : public worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi> {
public:
    MigrateTestWorkerMasterApiManager(HostPort &workerAddr, const worker::MetadataRouteResolver &metadataRoute)
        : WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>(workerAddr, nullptr, metadataRoute)
    {
    }

    std::shared_ptr<worker::WorkerMasterOCApi> CreateWorkerMasterApi(const HostPort &masterAddress) override
    {
        auto iter = apiByAddr_.find(masterAddress.ToString());
        return iter == apiByAddr_.end() ? defaultApi_ : iter->second;
    }

    Status GetWorkerMasterApi(const HostPort &masterAddress, std::shared_ptr<worker::WorkerMasterOCApi> &api) override
    {
        api = CreateWorkerMasterApi(masterAddress);
        CHECK_FAIL_RETURN_STATUS(api != nullptr, K_RUNTIME_ERROR, "test worker master API is not configured");
        return Status::OK();
    }

    std::shared_ptr<worker::WorkerMasterOCApi> GetWorkerMasterApi(const HostPort &masterAddress) override
    {
        std::shared_ptr<worker::WorkerMasterOCApi> api;
        LOG_IF_ERROR(GetWorkerMasterApi(masterAddress, api), "GetWorkerMasterApi failed");
        return api;
    }

    void SetDefaultApi(const std::shared_ptr<worker::WorkerMasterOCApi> &api)
    {
        defaultApi_ = api;
    }

private:
    std::shared_ptr<worker::WorkerMasterOCApi> defaultApi_;
    std::unordered_map<std::string, std::shared_ptr<worker::WorkerMasterOCApi>> apiByAddr_;
};

class MigrateDataServiceTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        Init();
        const uint64_t memSize = 32 * 1024ul * 1024ul;
        FLAGS_arena_per_tenant = 1;
        allocator_ = datasystem::memory::Allocator::Instance();
        allocator_->Init(memSize);
        FLAGS_spill_directory = "./spill" + GetStringUuid();
        FLAGS_spill_size_limit = memSize;
        DS_ASSERT_OK(WorkerOcSpill::Instance()->Init());
    }

    void TearDown() override
    {
        if (allocator_ != nullptr) {
            allocator_->Shutdown();
            allocator_ = nullptr;
        }
        CommonTest::TearDown();
    }

    void Init()
    {
        objectTable_ = std::make_shared<ObjectTable>();
        workerMasterApiManager_ = std::make_shared<MigrateTestWorkerMasterApiManager>(localAddress_, metadataRoute_);
        WorkerOcServiceCrudParam param{
            .workerMasterApiManager = workerMasterApiManager_,
            .workerRequestManager = requestManager_,
            .memoryRefTable = nullptr,
            .objectTable = objectTable_,
            .evictionManager = nullptr,
            .workerDevOcManager = nullptr,
            .asyncPersistenceDelManager = nullptr,
            .asyncSendManager = nullptr,
            .metadataSize = 0,
            .persistenceApi = nullptr,
            .metadataRouteResolver = &metadataRoute_,
            .endpointPolicy = nullptr,
            .exitRequested = &localExiting_,
            .allowDirectoryLag = false,
        };
        threadPool_ = std::make_shared<ThreadPool>(MEMCOPY_THREAD_NUM);
        rateController_ =
            std::make_shared<MigrateDataRateController>(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul);
        impl_ = std::make_shared<WorkerOcServiceMigrateImpl>(param, threadPool_, nullptr, "127.0.0.1:18888",
                                                             rateController_);
        TimerQueue::GetInstance()->Initialize();
    }

    uint64_t GetMetaSize(uint64_t dataSize)
    {
        constexpr uint64_t defaultMetaSize = 10;
        return WorkerOcServiceCrudCommonApi::CanTransferByShm(dataSize) ? defaultMetaSize : 0;
    }

    Status CreateObject(const std::string &objectKey, uint64_t dataSize)
    {
        CHECK_FAIL_RETURN_STATUS(!objectTable_->Contains(objectKey), StatusCode::K_DUPLICATED, "object exist");
        const uint64_t metaSize = GetMetaSize(dataSize);
        const uint64_t needSize = dataSize + metaSize;

        auto ptr = std::make_unique<object_cache::ObjCacheShmUnit>();
        auto shmUnit = std::make_shared<ShmUnit>();
        RETURN_IF_NOT_OK(shmUnit->AllocateMemory("", needSize, false));
        if (metaSize > 0) {
            auto ret = memset_s(shmUnit->GetPointer(), metaSize, 0, metaSize);
            if (ret != EOK) {
                RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                        FormatString("[ObjectKey %s] Memset failed, errno: %d", objectKey, ret));
            }
        }
        ptr->SetShmUnit(shmUnit);
        ptr->SetDataSize(dataSize);
        ptr->SetMetadataSize(metaSize);
        ptr->SetCreateTime(1);
        ptr->SetLifeState(ObjectLifeState::OBJECT_SEALED);

        ptr->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
        ptr->modeInfo.SetCacheType(CacheType::MEMORY);
        ptr->stateInfo.SetDataFormat(DataFormat::BINARY);
        ptr->stateInfo.SetPrimaryCopy(true);
        ptr->stateInfo.SetSpillState(false);

        objectTable_->Insert(objectKey, std::move(ptr));
        return Status::OK();
    }

    void SetMemoryAvailable(bool available)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::IsMemoryAvailable, (_, _)).WillRepeatedly(Return(available));
    }

    void SetSpillAvailable(bool available)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::IsSpillAvaialble, (_)).WillRepeatedly(Return(available));
    }

    void SetDiskAvailable(bool available)
    {
        BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::IsDiskAvailable, (_)).WillRepeatedly(Return(available));
    }

    void CreateObjects(const std::string &prefix, uint64_t dataSize, uint32_t count, uint64_t version, bool needCreate,
                       bool needLock, MigrateDataReqPb &req)
    {
        for (uint32_t i = 0; i < count; ++i) {
            std::string objectKey = prefix + std::to_string(i);
            if (needCreate) {
                DS_ASSERT_OK(CreateObject(objectKey, dataSize));
                if (needLock) {
                    std::shared_ptr<SafeObjType> entry;
                    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
                    DS_ASSERT_OK(entry->WLock());
                }
            }

            auto info = req.add_objects();
            info->set_object_key(objectKey);
            info->set_version(version);
            info->set_data_size(dataSize);
        }
    }

    uint64_t GetMetadatSize() const
    {
        constexpr int alignment = 0x8;
        // Worker set lockId_ = 0(shm_guard), so we need client_nums + 1 bits slot.
        uint64_t metadataSize = FLAGS_max_client_num == 0 ? 0 : FLAGS_max_client_num / alignment + 1;
        metadataSize += sizeof(uint32_t) + sizeof(char);
        auto alignCeiling = [](uintptr_t addr, uintptr_t alignment) {
            return (addr + alignment - 1) & ~(alignment - 1);
        };
        metadataSize = alignCeiling(metadataSize, 0x40);
        return metadataSize;
    }

    void RouteObjectKeysByMasterHostPort2(const std::unordered_set<std::string> &objectKeys)
    {
        placement_.Clear();
        size_t size = objectKeys.size();
        size_t count = 0;
        size_t batch = 3;
        for (const auto &id : objectKeys) {
            HostPort masterAddr;
            if (count < size / batch) {
                masterAddr.ParseString("127.0.0.1:18481");
            } else if (count < (size / batch * 2)) {
                masterAddr.ParseString("127.0.0.1:18482");
            } else {
                masterAddr.ParseString("127.0.0.1:18483");
            }
            placement_.SetOwner(id, masterAddr);
            count++;
        }
    }

    void VerifyRequestHoldsMigrationAdmission(const std::string &injectPoint, std::function<Status()> request)
    {
        constexpr std::chrono::seconds schedulingTimeout(1);
        constexpr std::chrono::seconds closeBudget(2);
        constexpr std::chrono::milliseconds observationWindow(50);
        // Block the RPC at the afterAdmission inject point so the test can assert
        // that CloseIncomingMigrationAdmissionAndWait waits while admission is held.
        DS_ASSERT_OK(inject::Set(injectPoint, "pause()"));
        auto requestFuture = std::async(std::launch::async, std::move(request));
        // Wait until the RPC hits the inject point - admission is acquired and held.
        const bool requestAdmitted = WaitForInjectPointExecuteCount(injectPoint, 1, schedulingTimeout);

        auto closeFuture = std::async(std::launch::async, [this, closeBudget] {
            return impl_->CloseIncomingMigrationAdmissionAndWait(std::chrono::steady_clock::now() + closeBudget);
        });
        Status lateAdmission(K_RUNTIME_ERROR, "Migration admission gate did not close");
        const auto gateDeadline = std::chrono::steady_clock::now() + schedulingTimeout;
        do {
            lateAdmission = impl_->AcquireIncomingMigrationAdmission();
            if (lateAdmission.IsOk()) {
                impl_->ReleaseIncomingMigrationAdmission();
                std::this_thread::yield();
            }
        } while (lateAdmission.IsOk() && std::chrono::steady_clock::now() < gateDeadline);
        const auto closeStateWhileRequestPaused = closeFuture.wait_for(observationWindow);

        DS_ASSERT_OK(inject::Clear(injectPoint));
        (void)requestFuture.get();
        const auto closeStatus = closeFuture.get();
        EXPECT_TRUE(requestAdmitted);
        EXPECT_EQ(lateAdmission.GetCode(), StatusCode::K_NOT_READY);
        EXPECT_EQ(closeStateWhileRequestPaused, std::future_status::timeout);
        DS_EXPECT_OK(closeStatus);
    }

    void VerifyRequestReturnsFailureWhenDrainTimesOut(const std::string &injectPoint, std::function<Status()> request)
    {
        constexpr std::chrono::seconds schedulingTimeout(1);
        // Intentionally short: the pause() inject holds admission indefinitely, so drain
        // is guaranteed to time out regardless of the budget. drainTimedOut_ is set inside
        // the same lock as wait_until, so scheduling latency does not affect correctness.
        constexpr std::chrono::milliseconds closeBudget(100);
        // Block the RPC at the afterAdmission inject point so drain will time out.
        DS_ASSERT_OK(inject::Set(injectPoint, "pause()"));
        auto requestFuture = std::async(std::launch::async, std::move(request));
        // Wait until the RPC hits the inject point - admission is acquired and held.
        const bool requestAdmitted = WaitForInjectPointExecuteCount(injectPoint, 1, schedulingTimeout);

        auto closeFuture = std::async(std::launch::async, [this, closeBudget] {
            return impl_->CloseIncomingMigrationAdmissionAndWait(
                std::chrono::steady_clock::now() + closeBudget);
        });
        const auto closeStatus = closeFuture.get();
        EXPECT_EQ(closeStatus.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);

        DS_ASSERT_OK(inject::Clear(injectPoint));
        const auto requestStatus = requestFuture.get();

        EXPECT_TRUE(requestAdmitted);
        EXPECT_EQ(requestStatus.GetCode(), StatusCode::K_NOT_READY);
    }

    Status PureQueryMeta(const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::PureQueryMetaReqPb &req,
                         master::PureQueryMetaRspPb &rsp);

protected:
    MigrateTestPlacementFacade placement_;
    worker::MetadataRouteResolver metadataRoute_{ &placement_, worker::MetadataRouteOptions{} };
    HostPort localAddress_{ "127.0.0.1", 18482 };
    datasystem::memory::Allocator *allocator_{ nullptr };
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<MigrateTestWorkerMasterApiManager> workerMasterApiManager_;
    std::shared_ptr<ThreadPool> threadPool_;
    std::shared_ptr<WorkerOcServiceMigrateImpl> impl_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    WorkerRequestManager requestManager_;
    std::shared_ptr<MigrateDataRateController> rateController_;
    std::atomic<bool> localExiting_{ false };
};

TEST(MetaOwnerRouteGroupsTest, AppendFailuresToGroupDoesNotCreateEmptyGroupWithoutFailures)
{
    MetaOwnerRouteGroups grouped;
    AppendRouteFailures(grouped);
    EXPECT_TRUE(grouped.groups.empty());

    const std::string failedKey = "failed-key";
    grouped.failures.emplace(failedKey, Status(K_NOT_FOUND, "route failed"));
    AppendRouteFailures(grouped);
    ASSERT_EQ(grouped.groups.size(), size_t(1));
    auto iter = grouped.groups.find(HostPort());
    ASSERT_NE(iter, grouped.groups.end());
    EXPECT_THAT(iter->second, ElementsAre(failedKey));
}

TEST(MetaOwnerRouteGroupsTest, BuildGroupsFromTopologyPlacementAndKeepsPerKeyFailures)
{
    MigrateTestPlacementFacade placement;
    HostPort masterAddr;
    masterAddr.ParseString("127.0.0.1:18481");
    placement.SetOwner("ok-key", masterAddr);

    worker::MetadataRouteResolver metadataRoute(&placement, worker::MetadataRouteOptions{});
    auto grouped = metadataRoute.GroupOwners({ "ok-key", "missing-key" });
    ASSERT_EQ(grouped.groups.size(), size_t(1));
    auto iter = grouped.groups.find(masterAddr);
    ASSERT_NE(iter, grouped.groups.end());
    EXPECT_THAT(iter->second, ElementsAre("ok-key"));
    ASSERT_EQ(grouped.failures.size(), size_t(1));
    EXPECT_EQ(grouped.failures.at("missing-key").GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(MigrateDataServiceTest, TestDiskIOError)
{
    SetMemoryAvailable(false);
    SetSpillAvailable(false);
    BINEXPECT_CALL(&memory::Allocator::IsDiskAvailable, ()).WillRepeatedly(Return(false));

    constexpr int size = 100;
    MigrateDataReqPb req;
    for (int i = 0; i < size; ++i) {
        auto objInfo = req.add_objects();
        objInfo->set_object_key("HelloWorld_" + std::to_string(i));
    }

    MigrateDataRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_EQ(impl_->MigrateData(req, rsp, std::move(payloads)).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    ASSERT_EQ(rsp.success_ids_size(), 0);
    ASSERT_EQ(rsp.fail_ids_size(), size);
}

TEST_F(MigrateDataServiceTest, TestResourcesUnavailable)
{
    SetMemoryAvailable(false);
    SetSpillAvailable(false);
    SetDiskAvailable(false);

    constexpr int size = 100;
    MigrateDataReqPb req;
    for (int i = 0; i < size; ++i) {
        auto objInfo = req.add_objects();
        objInfo->set_object_key("HelloWorld_" + std::to_string(i));
    }

    MigrateDataRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_EQ(impl_->MigrateData(req, rsp, std::move(payloads)).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    ASSERT_EQ(rsp.success_ids_size(), 0);
    ASSERT_EQ(rsp.fail_ids_size(), size);
}

TEST_F(MigrateDataServiceTest, RejectsIncomingMigrationAfterLocalScaleInStarts)
{
    localExiting_.store(true);
    constexpr std::chrono::seconds closeBudget(1);
    DS_ASSERT_OK(impl_->CloseIncomingMigrationAdmissionAndWait(std::chrono::steady_clock::now() + closeBudget));
    MigrateDataReqPb req;
    req.set_type(MigrateType::SCALE_DOWN);
    req.add_objects()->set_object_key("late-object");
    MigrateDataRspPb rsp;
    EXPECT_EQ(impl_->MigrateData(req, rsp, {}).GetCode(), StatusCode::K_NOT_READY);
    EXPECT_EQ(rsp.scale_down_state(), MigrateDataRspPb::DATA_MIGRATION_STARTED);
    EXPECT_THAT(rsp.fail_ids(), ElementsAre("late-object"));

    MigrateDataReqPb probe;
    probe.set_type(MigrateType::SCALE_DOWN);
    MigrateDataRspPb probeRsp;
    EXPECT_EQ(impl_->MigrateData(probe, probeRsp, {}).GetCode(), StatusCode::K_NOT_READY);
    EXPECT_EQ(probeRsp.scale_down_state(), MigrateDataRspPb::DATA_MIGRATION_STARTED);

    MigrateDataDirectReqPb directReq;
    directReq.add_objects()->set_object_key("late-direct-object");
    MigrateDataDirectRspPb directRsp;
    EXPECT_EQ(impl_->MigrateDataDirect(directReq, directRsp).GetCode(), StatusCode::K_NOT_READY);
    EXPECT_THAT(directRsp.failed_object_keys(), ElementsAre("late-direct-object"));
}

TEST_F(MigrateDataServiceTest, SocketMigrationHoldsAdmissionUntilRequestReturns)
{
    MigrateDataReqPb req;
    req.set_type(MigrateType::SCALE_DOWN);
    MigrateDataRspPb rsp;
    VerifyRequestHoldsMigrationAdmission("WorkerOcServiceMigrateImpl.MigrateData.afterAdmission", [this, &req, &rsp] {
        return impl_->MigrateData(req, rsp, {});
    });
}

TEST_F(MigrateDataServiceTest, DirectMigrationHoldsAdmissionUntilRequestReturns)
{
    MigrateDataDirectReqPb req;
    req.add_objects()->set_object_key("guarded-direct-object");
    MigrateDataDirectRspPb rsp;
    VerifyRequestHoldsMigrationAdmission("WorkerOcServiceMigrateImpl.MigrateDataDirect.afterAdmission",
                                          [this, &req, &rsp] {
        return impl_->MigrateDataDirect(req, rsp);
    });
}

TEST_F(MigrateDataServiceTest, CloseMigrationAdmissionReturnsDeadlineExceeded)
{
    DS_ASSERT_OK(impl_->AcquireIncomingMigrationAdmission());
    const auto rc = impl_->CloseIncomingMigrationAdmissionAndWait(std::chrono::steady_clock::now());
    EXPECT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(impl_->AcquireIncomingMigrationAdmission().GetCode(), StatusCode::K_NOT_READY);
    impl_->ReleaseIncomingMigrationAdmission();
}

TEST_F(MigrateDataServiceTest, MigrateDataReturnsFailureWhenDrainTimesOut)
{
    MigrateDataReqPb req;
    req.set_type(MigrateType::SCALE_DOWN);
    req.add_objects()->set_object_key("drain-timeout-socket");
    MigrateDataRspPb rsp;
    VerifyRequestReturnsFailureWhenDrainTimesOut("WorkerOcServiceMigrateImpl.MigrateData.afterAdmission",
                                                   [this, &req, &rsp] {
        return impl_->MigrateData(req, rsp, {});
    });
}

TEST_F(MigrateDataServiceTest, MigrateDataDirectReturnsFailureWhenDrainTimesOut)
{
    MigrateDataDirectReqPb req;
    req.add_objects()->set_object_key("drain-timeout-direct");
    MigrateDataDirectRspPb rsp;
    VerifyRequestReturnsFailureWhenDrainTimesOut("WorkerOcServiceMigrateImpl.MigrateDataDirect.afterAdmission",
                                                   [this, &req, &rsp] {
        return impl_->MigrateDataDirect(req, rsp);
    });
}

TEST_F(MigrateDataServiceTest, TestLockNeedMigrateObjects)
{
    uint64_t elderVersion = 0;
    uint64_t nowVersion = 1;
    uint64_t newerVersion = 2;
    uint64_t expireCount = 100;
    uint64_t lockFailCount = 10;
    uint64_t newCreateCount = 40;
    uint64_t existCount = 50;

    MigrateDataReqPb req;
    CreateObjects("Expire_", 1, expireCount, elderVersion, true, false, req);
    CreateObjects("Locked_Failed_", 1, lockFailCount, nowVersion, true, true, req);
    CreateObjects("New_Created_", 1, newCreateCount, nowVersion, false, false, req);
    CreateObjects("Exist_", 1, existCount, newerVersion, true, false, req);

    LockedEntryMap lockedEntries;
    LockedEntryMap needModifyPrimary;
    std::unordered_set<std::string> successIds;
    std::unordered_set<std::string> failedIds;
    impl_->BatchLockForMigrateData(req.objects(), lockedEntries, successIds, failedIds, needModifyPrimary);
    ASSERT_EQ(lockedEntries.size(), newCreateCount + existCount);
    ASSERT_EQ(successIds.size(), expireCount);
    ASSERT_EQ(failedIds.size(), lockFailCount);
}

TEST_F(MigrateDataServiceTest, TestLockNeedMigrateObjectsFailed)
{
    DS_ASSERT_OK(inject::Set("SafeTable.ReserveGetAndLock.return", "1*call()"));
    uint64_t elderVersion = 0;
    uint64_t nowVersion = 1;
    uint64_t newerVersion = 2;
    uint64_t expireCount = 100;
    uint64_t lockFailCount = 10;
    uint64_t newCreateCount = 40;
    uint64_t existCount = 50;

    MigrateDataReqPb req;
    CreateObjects("Expire_", 1, expireCount, elderVersion, true, false, req);
    CreateObjects("Locked_Failed_", 1, lockFailCount, nowVersion, true, true, req);
    CreateObjects("New_Created_", 1, newCreateCount, nowVersion, false, false, req);
    CreateObjects("Exist_", 1, existCount, newerVersion, true, false, req);

    LockedEntryMap lockedEntries;
    LockedEntryMap needModifyPrimary;
    std::unordered_set<std::string> successIds;
    std::unordered_set<std::string> failedIds;
    impl_->BatchLockForMigrateData(req.objects(), lockedEntries, successIds, failedIds, needModifyPrimary);
}

TEST_F(MigrateDataServiceTest, ReplacePrimaryRetryFailed)
{
    Status status(StatusCode::K_RPC_UNAVAILABLE, "");
    const size_t retryTimes = 4;
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::ReplacePrimaryOnce, (_, _, _))
        .Times(retryTimes)
        .WillRepeatedly(Return(status));
    std::shared_ptr<WorkerMasterOCApi> remoteApi =
        std::make_shared<MigrateTestWorkerMasterOCApi>(HostPort("127.0.0.1:18481"), HostPort("127.0.0.1:18482"));
    master::ReplacePrimaryReqPb req;
    master::ReplacePrimaryRspPb rsp;
    DS_ASSERT_NOT_OK(impl_->ReplacePrimaryRetry(remoteApi, req, rsp));
}

TEST_F(MigrateDataServiceTest, PureQueryMetaMovingWithoutRedirectInfoRetries)
{
    constexpr size_t expectedRpcCalls = 2;
    size_t rpcCalls = 0;
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::PureQueryMetaOnce, (_, _, _))
        .Times(expectedRpcCalls)
        .WillRepeatedly(Invoke([&rpcCalls](const std::shared_ptr<worker::WorkerMasterOCApi> &,
                                          master::PureQueryMetaReqPb &, master::PureQueryMetaRspPb &rsp) {
            ++rpcCalls;
            rsp.set_meta_is_moving(rpcCalls == 1);
            return Status::OK();
        }));
    auto remoteApi = std::make_shared<MigrateTestWorkerMasterOCApi>(HostPort("127.0.0.1:18481"),
                                                                    HostPort("127.0.0.1:18482"));
    master::PureQueryMetaReqPb req;
    master::PureQueryMetaRspPb rsp;

    DS_ASSERT_OK(impl_->PureQueryMetaRetry(remoteApi, req, rsp));

    EXPECT_EQ(rpcCalls, expectedRpcCalls);
    EXPECT_FALSE(rsp.meta_is_moving());
}

TEST_F(MigrateDataServiceTest, DISABLED_TestQueryMetaFromMasterMeetsRPCError)
{
    LOG(INFO) << "Test query objects meta meets rpc error";
    Status status(StatusCode::K_RPC_UNAVAILABLE, "");
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::PureQueryMetaOnce, (_, _, _)).Times(4).WillRepeatedly(Return(status));

    std::shared_ptr<WorkerMasterOCApi> remoteApi =
        std::make_shared<MigrateTestWorkerMasterOCApi>(HostPort("127.0.0.1:18481"), HostPort("127.0.0.1:18482"));
    workerMasterApiManager_->SetDefaultApi(remoteApi);

    MigrateDataReqPb req;
    uint64_t elderVersion = 0;
    uint64_t newCreateCount = 40;
    uint64_t expireCount = 100;
    uint64_t nowVersion = 1;
    CreateObjects("Expire_", 1, expireCount, elderVersion, true, false, req);
    CreateObjects("New_Created_", 1, newCreateCount, nowVersion, false, false, req);
    std::unordered_set<std::string> routeKeys;
    for (uint64_t i = 0; i < newCreateCount; ++i) {
        routeKeys.emplace("New_Created_" + std::to_string(i));
    }
    RouteObjectKeysByMasterHostPort2(routeKeys);
    MigrateDataRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_EQ(impl_->MigrateData(req, rsp, std::move(payloads)).GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    ASSERT_EQ(rsp.fail_ids_size(), newCreateCount);
    ASSERT_EQ(rsp.success_ids_size(), expireCount);
}

size_t gCount = 9000;

Status MigrateDataServiceTest::PureQueryMeta(const std::shared_ptr<worker::WorkerMasterOCApi> &api,
                                             master::PureQueryMetaReqPb &req, master::PureQueryMetaRspPb &rsp)
{
    auto fillMeta = [](const std::string &id, master::PureQueryMetaRspPb &rsp) {
        if (id.find("Equal_Version") != std::string::npos) {
            auto meta = rsp.add_query_metas();
            meta->mutable_meta()->set_version(1);
            meta->mutable_meta()->set_object_key(id);
        } else if (id.find("Larger_Version") != std::string::npos) {
            auto meta = rsp.add_query_metas();
            meta->mutable_meta()->set_version(2);
            meta->mutable_meta()->set_object_key(id);
        } else if (id.find("Smaller_Version") != std::string::npos) {
            auto meta = rsp.add_query_metas();
            meta->mutable_meta()->set_version(0);
            meta->mutable_meta()->set_object_key(id);
        }
    };

    (void)api;
    int size = req.object_keys_size();
    int count = 0;
    RedirectMetaInfo *info = nullptr;
    for (const auto &id : req.object_keys()) {
        if (req.redirect() && count < size / 2) {
            if (info == nullptr) {
                info = rsp.add_info();
                info->set_redirect_meta_address("127.0.0.1:" + std::to_string(gCount++));
            }
            info->add_change_meta_ids(id);
        } else {
            fillMeta(id, rsp);
        }
        ++count;
    }
    return Status::OK();
}

TEST_F(MigrateDataServiceTest, TestQueryMetaFromMasterBasicFunction)
{
    LOG(INFO) << "Test query meta from master basic function";
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::PureQueryMetaOnce, (_, _, _))
        .Times(6)
        .WillRepeatedly(Invoke(this, &MigrateDataServiceTest::PureQueryMeta));

    std::shared_ptr<WorkerMasterOCApi> remoteApi =
        std::make_shared<MigrateTestWorkerMasterOCApi>(HostPort("127.0.0.1:18481"), HostPort("127.0.0.1:18482"));
    workerMasterApiManager_->SetDefaultApi(remoteApi);

    std::unordered_set<std::string> objectKeys;
    uint64_t count = 300;
    for (size_t i = 0; i < count; ++i) {
        if (i >= count / 2) {
            objectKeys.emplace("Absent_ID" + std::to_string(i));
        } else {
            objectKeys.emplace("Equal_Version" + std::to_string(i));
        }
    }
    RouteObjectKeysByMasterHostPort2(objectKeys);
    QueryMetaMap queryMetas;
    std::unordered_set<std::string> failedIds;
    DS_ASSERT_OK(impl_->QueryMasterMetadata(objectKeys, queryMetas, failedIds));
    ASSERT_EQ(queryMetas.size(), count / 2);
    ASSERT_TRUE(failedIds.empty());
}

TEST_F(MigrateDataServiceTest, TestQueryMasterMetadataError)
{
    Status status1(StatusCode::K_RUNTIME_ERROR, "");
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::QueryMasterMetadata, (_, _, _))
        .Times(1)
        .WillRepeatedly(Return(status1));

    MigrateDataReqPb req;
    uint64_t elderVersion = 0;
    uint64_t newCreateCount = 40;
    uint64_t expireCount = 100;
    uint64_t nowVersion = 1;
    CreateObjects("Expire_", 1, expireCount, elderVersion, true, false, req);
    CreateObjects("New_Created_", 1, newCreateCount, nowVersion, false, false, req);
    MigrateDataRspPb rsp;
    std::vector<RpcMessage> payloads;
    ASSERT_EQ(impl_->MigrateData(req, rsp, std::move(payloads)).GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(MigrateDataServiceTest, TestMigrateDataMeetsOOM)
{
    LOG(INFO) << "Test migrate data meets OOM";
}

TEST_F(MigrateDataServiceTest, TestAllocateAndAssignDataBasicFunction)
{
    BINEXPECT_CALL(&WorkerOcEvictionManager::Add, (_)).Times(1).WillRepeatedly(Return());
    BINEXPECT_CALL(&WorkerOcServiceCrudCommonApi::GetMetadataSize, ())
        .Times(1)
        .WillRepeatedly(Return(GetMetadatSize()));
    // Get offset and size, let all memory zone are 1.
    uint64_t size = 1024ul * 1024ul;
    void *pointer;
    int fd;
    ptrdiff_t offset;
    uint64_t mmapSize;
    DS_ASSERT_OK(datasystem::memory::Allocator::Instance()->AllocateMemory(DEFAULT_TENANT_ID, size, false, pointer, fd,
                                                                           offset, mmapSize));
    (void)memset_s((uint8_t *)pointer - offset, mmapSize, 0xff, mmapSize);

    std::string objectKey = "xxx";
    std::shared_ptr<SafeObjType> entry =
        std::make_shared<SafeObjType>(std::make_unique<object_cache::ObjCacheShmUnit>());
    (*entry)->modeInfo.SetCacheType(CacheType::MEMORY);

    std::vector<uint8_t> data(size, 0);
    std::vector<std::pair<const uint8_t *, uint64_t>> payloads = { { data.data(), data.size() } };
    DS_ASSERT_OK(impl_->AllocateAndAssignData(objectKey, entry, payloads, size, nullptr));
    auto shmUnit = (*entry)->GetShmUnit();
    ShmGuard guard(shmUnit, GetMetadatSize(), shmUnit->GetSize() - GetMetadatSize());
    DS_ASSERT_OK(guard.TryRLatch(true));
}

TEST_F(MigrateDataServiceTest, TestMemoryAvailableForSpill)
{
    LOG(INFO) << "Test CheckResource for SPILL type when memory is available";
    SetMemoryAvailable(true);

    MigrateDataReqPb req;
    req.set_type(MigrateType::SPILL);
    MigrateDataRspPb rsp;
    DS_ASSERT_OK(impl_->CheckResource(req, rsp));
    EXPECT_EQ(rsp.fail_ids_size(), 0);
}

TEST_F(MigrateDataServiceTest, TestOOMForSpill)
{
    LOG(INFO) << "Test CheckResource for SPILL type when oom";
    SetMemoryAvailable(false);

    constexpr uint32_t objectCount = 10;
    constexpr uint64_t dataSize = 1024;
    MigrateDataReqPb req;
    req.set_type(MigrateType::SPILL);
    for (uint32_t i = 0; i < objectCount; ++i) {
        auto objInfo = req.add_objects();
        objInfo->set_object_key("spill_fail_obj_" + std::to_string(i));
        objInfo->set_data_size(dataSize);
    }

    MigrateDataRspPb rsp;
    Status status = impl_->CheckResource(req, rsp);
    EXPECT_EQ(status.GetCode(), StatusCode::K_OUT_OF_MEMORY);
    EXPECT_EQ(rsp.success_ids_size(), 0);
    EXPECT_EQ(rsp.fail_ids_size(), objectCount);
}

TEST_F(MigrateDataServiceTest, TestInvalidMigrateType)
{
    LOG(INFO) << "Test CheckResource with invalid migrate type";

    constexpr int invalidTypeValue = 999;
    MigrateDataReqPb req;
    req.set_type(static_cast<MigrateType>(invalidTypeValue));
    MigrateDataRspPb rsp;
    ASSERT_EQ(impl_->CheckResource(req, rsp).GetCode(), StatusCode::K_INVALID);
}

TEST_F(MigrateDataServiceTest, TestSaveDataWithSpillType)
{
    BINEXPECT_CALL(&WorkerOcEvictionManager::Add, (_)).Times(1).WillRepeatedly(Return());
    std::shared_ptr<SafeObjType> entry =
        std::make_shared<SafeObjType>(std::make_unique<object_cache::ObjCacheShmUnit>());
    MigrateDataReqPb::ObjectInfoPb info;
    info.set_object_key("object1");
    constexpr uint64_t dataSize = 30 * 1024 * 1024;  // 30 MB is larger than memory high water for spill type
    info.set_data_size(dataSize);
    info.add_part_index(0);
    std::vector<RpcMessage> payloads(1);
    std::string data = "1";
    payloads[0].CopyString(data);
    // Will oom, don't spill to disk
    ASSERT_EQ(impl_->SaveDataWithObjectLocked(entry, info, payloads, MigrateType::SPILL, nullptr).GetCode(),
              StatusCode::K_OUT_OF_MEMORY);
    info.set_data_size(1);
    DS_ASSERT_OK(impl_->SaveDataWithObjectLocked(entry, info, payloads, MigrateType::SPILL, nullptr));
}

class MigrateL2DataServiceTest : public MigrateDataServiceTest {};

TEST_F(MigrateL2DataServiceTest, TestMigrateL2Data)
{

}

TEST_F(MigrateDataServiceTest, UsesInjectedRateController)
{
    ASSERT_EQ(impl_->rateController_, rateController_);
}

class NotifyRemoteGetMigrationTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        objectTable_ = std::make_shared<ObjectTable>();
        workerMasterApiManager_ = std::make_shared<MigrateTestWorkerMasterApiManager>(localAddress_, metadataRoute_);
        WorkerOcServiceCrudParam param{
            .workerMasterApiManager = workerMasterApiManager_,
            .workerRequestManager = requestManager_,
            .memoryRefTable = nullptr,
            .objectTable = objectTable_,
            .evictionManager = nullptr,
            .workerDevOcManager = nullptr,
            .asyncPersistenceDelManager = nullptr,
            .asyncSendManager = nullptr,
            .metadataSize = 0,
            .persistenceApi = nullptr,
            .metadataRouteResolver = &metadataRoute_,
            .endpointPolicy = nullptr,
            .exitRequested = nullptr,
            .allowDirectoryLag = false,
        };
        rateController_ =
            std::make_shared<MigrateDataRateController>(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul);
        impl_ = std::make_shared<WorkerOcServiceGetImpl>(param, nullptr, nullptr, nullptr, nullptr,
                                                         HostPort("127.0.0.1:18888"), rateController_);
    }

protected:
    void RouteObjectToMaster(const std::string &objectKey, const HostPort &masterAddress)
    {
        placement_.SetOwner(objectKey, masterAddress);
    }

    master::QueryMetaInfoPb MakeQueryMeta(uint64_t dataSize = 1)
    {
        master::QueryMetaInfoPb queryMeta;
        queryMeta.mutable_meta()->set_version(1);
        queryMeta.mutable_meta()->set_data_size(dataSize);
        queryMeta.mutable_meta()->mutable_config()->set_data_format(static_cast<uint32_t>(DataFormat::BINARY));
        return queryMeta;
    }

    MigrateTestPlacementFacade placement_;
    worker::MetadataRouteResolver metadataRoute_{ &placement_, worker::MetadataRouteOptions{} };
    HostPort localAddress_{ "127.0.0.1", 18888 };
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<MigrateTestWorkerMasterApiManager> workerMasterApiManager_;
    WorkerRequestManager requestManager_;
    std::shared_ptr<WorkerOcServiceGetImpl> impl_;
    std::shared_ptr<MigrateDataRateController> rateController_;
};

TEST_F(NotifyRemoteGetMigrationTest, PostProcessRemoteGetInNotificationClearsDeleteFlagWhenReplicationDisabled)
{
    const bool oldEnableDataReplication = FLAGS_enable_data_replication;
    Raii restoreFlag([oldEnableDataReplication]() { FLAGS_enable_data_replication = oldEnableDataReplication; });
    FLAGS_enable_data_replication = false;

    auto entry = std::make_shared<SafeObjType>();
    auto obj = std::make_unique<ObjCacheShmUnit>();
    obj->stateInfo.SetDataFormat(DataFormat::BINARY);
    obj->stateInfo.SetNeedToDelete(true);
    entry->SetRealObject(std::move(obj));

    auto untouchedEntry = std::make_shared<SafeObjType>();
    auto untouchedObj = std::make_unique<ObjCacheShmUnit>();
    untouchedObj->stateInfo.SetDataFormat(DataFormat::BINARY);
    untouchedObj->stateInfo.SetNeedToDelete(true);
    untouchedEntry->SetRealObject(std::move(untouchedObj));

    ASSERT_TRUE(entry->WLock().IsOk());
    ASSERT_TRUE(untouchedEntry->WLock().IsOk());
    ASSERT_TRUE(entry->IsWLockedByCurrentThread());
    ASSERT_TRUE(untouchedEntry->IsWLockedByCurrentThread());

    std::map<ReadKey, WorkerOcServiceGetImpl::LockedEntity> lockedEntries;
    lockedEntries.emplace(ReadKey("obj1", 0, 1), WorkerOcServiceGetImpl::LockedEntity{ entry, false });
    lockedEntries.emplace(ReadKey("obj2", 0, 1), WorkerOcServiceGetImpl::LockedEntity{ untouchedEntry, false });

    using NotifyRemoteGetGroup =
        std::unordered_map<std::string,
                           std::list<std::pair<std::list<WorkerOcServiceGetImpl::GetObjectInfo>, uint64_t>>>;
    NotifyRemoteGetGroup groupedQueryMetas;
    groupedQueryMetas.emplace("127.0.0.1:18889",
                              std::list<std::pair<std::list<WorkerOcServiceGetImpl::GetObjectInfo>, uint64_t>>{});
    std::vector<std::vector<std::string>> tempSuccessIds{ { "obj1" } };
    std::vector<std::vector<ReadKey>> tempNeedRetryIds(1);
    std::vector<std::unordered_set<std::string>> tempFailedIds(1);
    std::set<ReadKey> objectsNeedGetRemote;
    Status lastRc = Status::OK();
    NotifyRemoteGetRspPb rsp;
    QueryMetaMap queryMetas;
    uint64_t migratedBytes = 0;
    std::map<std::string, uint64_t> unconfirmedObjectVersions;
    std::unordered_set<std::string> failedConfirmationOwners;

    impl_->PostProcessRemoteGetInNotificationImpl(lockedEntries, groupedQueryMetas, tempSuccessIds, tempNeedRetryIds,
                                                  tempFailedIds, objectsNeedGetRemote, lastRc, rsp, queryMetas,
                                                  migratedBytes, unconfirmedObjectVersions, failedConfirmationOwners);

    EXPECT_FALSE(entry->Get()->stateInfo.IsNeedToDelete());
    EXPECT_TRUE(untouchedEntry->Get()->stateInfo.IsNeedToDelete());

    entry->WUnlock();
    untouchedEntry->WUnlock();
}

TEST_F(NotifyRemoteGetMigrationTest, UnconfirmedNotifyRemoteGetObjectIsFreedAndErasedBeforeUnlock)
{
    const std::string objectKey = "unconfirmed_notify_remote_get";
    auto object = std::make_unique<ObjCacheShmUnit>();
    object->SetCreateTime(42);
    object->stateInfo.SetDataFormat(DataFormat::BINARY);
    objectTable_->Insert(objectKey, std::move(object));

    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
    DS_ASSERT_OK(entry->WLock());
    std::map<ReadKey, WorkerOcServiceGetImpl::LockedEntity> lockedEntries;
    lockedEntries.emplace(ReadKey(objectKey), WorkerOcServiceGetImpl::LockedEntity{ entry, true });

    impl_->FreeAndUnlockUnconfirmedNotifyRemoteGetObjects({ { objectKey, 42 } }, lockedEntries);

    EXPECT_FALSE(objectTable_->Contains(objectKey));
    EXPECT_FALSE(entry->IsWLockedByCurrentThread());
}

TEST_F(NotifyRemoteGetMigrationTest, NotifyRemoteGetReturnsFailedKeyWhenMasterDoesNotConfirmCopyMeta)
{
    const bool oldEnableDataReplication = FLAGS_enable_data_replication;
    Raii restoreFlag([oldEnableDataReplication]() { FLAGS_enable_data_replication = oldEnableDataReplication; });
    FLAGS_enable_data_replication = true;
    const std::string objectKey = "notify_remote_get_unconfirmed";
    const HostPort masterAddress("127.0.0.1:18889");
    RouteObjectToMaster(objectKey, masterAddress);
    auto api = std::make_shared<MigrateTestWorkerMasterOCApi>(masterAddress, localAddress_);
    api->createMultiCopyMeta_ = [](master::CreateMultiCopyMetaReqPb &, master::CreateMultiCopyMetaRspPb &) {
        return Status::OK();  // Version-expired copy-meta requests are OK but deliberately unconfirmed.
    };
    workerMasterApiManager_->SetDefaultApi(api);

    auto entry = std::make_shared<SafeObjType>(std::make_unique<ObjCacheShmUnit>());
    entry->Get()->SetCreateTime(42);
    ASSERT_TRUE(entry->WLock().IsOk());
    std::map<ReadKey, WorkerOcServiceGetImpl::LockedEntity> lockedEntries;
    lockedEntries.emplace(ReadKey(objectKey, 0, 1), WorkerOcServiceGetImpl::LockedEntity{ entry, true });
    using NotifyRemoteGetGroup =
        std::unordered_map<std::string,
                           std::list<std::pair<std::list<WorkerOcServiceGetImpl::GetObjectInfo>, uint64_t>>>;
    NotifyRemoteGetGroup groupedQueryMetas;
    groupedQueryMetas.emplace("leaving-worker", std::list<std::pair<std::list<WorkerOcServiceGetImpl::GetObjectInfo>, uint64_t>>{});
    std::vector<std::vector<std::string>> tempSuccessIds{ { objectKey } };
    std::vector<std::vector<ReadKey>> tempNeedRetryIds(1);
    std::vector<std::unordered_set<std::string>> tempFailedIds(1);
    std::set<ReadKey> objectsNeedGetRemote;
    QueryMetaMap queryMetas{ { objectKey, MakeQueryMeta() } };
    Status lastRc = Status::OK();
    NotifyRemoteGetRspPb rsp;
    uint64_t migratedBytes = 0;
    std::map<std::string, uint64_t> unconfirmedObjectVersions;
    std::unordered_set<std::string> failedConfirmationOwners;
    ScopedRequestContext requestContext;

    impl_->PostProcessRemoteGetInNotificationImpl(lockedEntries, groupedQueryMetas, tempSuccessIds, tempNeedRetryIds,
                                                  tempFailedIds, objectsNeedGetRemote, lastRc, rsp, queryMetas,
                                                  migratedBytes, unconfirmedObjectVersions, failedConfirmationOwners);

    EXPECT_THAT(rsp.failed_object_keys(), Contains(objectKey));
    EXPECT_EQ(unconfirmedObjectVersions.at(objectKey), 42);
    EXPECT_EQ(migratedBytes, 1);
    entry->WUnlock();
}

TEST_F(NotifyRemoteGetMigrationTest, NotifyRemoteGetAcceptsOnlyExplicitlyConfirmedCopyMeta)
{
    const bool oldEnableDataReplication = FLAGS_enable_data_replication;
    Raii restoreFlag([oldEnableDataReplication]() { FLAGS_enable_data_replication = oldEnableDataReplication; });
    FLAGS_enable_data_replication = true;
    const std::string objectKey = "notify_remote_get_confirmed";
    const HostPort masterAddress("127.0.0.1:18889");
    RouteObjectToMaster(objectKey, masterAddress);
    auto api = std::make_shared<MigrateTestWorkerMasterOCApi>(masterAddress, localAddress_);
    api->createMultiCopyMeta_ = [objectKey](master::CreateMultiCopyMetaReqPb &, master::CreateMultiCopyMetaRspPb &rsp) {
        rsp.add_confirmed_object_keys(objectKey);
        return Status::OK();
    };
    workerMasterApiManager_->SetDefaultApi(api);
    QueryMetaMap queryMetas{ { objectKey, MakeQueryMeta() } };
    std::vector<std::string> confirmedIds;
    std::unordered_set<std::string> failedIds;
    std::unordered_set<std::string> failedConfirmationOwners;
    ScopedRequestContext requestContext;

    impl_->ConfirmCopyMetaForNotifyRemoteGet({ objectKey }, queryMetas, confirmedIds, failedIds,
                                              failedConfirmationOwners);

    EXPECT_THAT(confirmedIds, ElementsAre(objectKey));
    EXPECT_TRUE(failedIds.empty());
}

TEST_F(NotifyRemoteGetMigrationTest, NotifyRemoteGetRejectsCopyMetaPersistenceFailure)
{
    const bool oldEnableDataReplication = FLAGS_enable_data_replication;
    Raii restoreFlag([oldEnableDataReplication]() { FLAGS_enable_data_replication = oldEnableDataReplication; });
    FLAGS_enable_data_replication = true;
    const std::string objectKey = "notify_remote_get_persistence_failure";
    const HostPort masterAddress("127.0.0.1:18889");
    RouteObjectToMaster(objectKey, masterAddress);
    auto api = std::make_shared<MigrateTestWorkerMasterOCApi>(masterAddress, localAddress_);
    api->createMultiCopyMeta_ = [objectKey](master::CreateMultiCopyMetaReqPb &, master::CreateMultiCopyMetaRspPb &rsp) {
        rsp.add_failed_object_keys(objectKey);  // Master could not persist the newly added location.
        return Status::OK();
    };
    workerMasterApiManager_->SetDefaultApi(api);
    QueryMetaMap queryMetas{ { objectKey, MakeQueryMeta() } };
    std::vector<std::string> confirmedIds;
    std::unordered_set<std::string> failedIds;
    std::unordered_set<std::string> failedConfirmationOwners;
    ScopedRequestContext requestContext;

    impl_->ConfirmCopyMetaForNotifyRemoteGet({ objectKey }, queryMetas, confirmedIds, failedIds,
                                              failedConfirmationOwners);

    EXPECT_TRUE(confirmedIds.empty());
    EXPECT_THAT(failedIds, Contains(objectKey));
}

TEST_F(NotifyRemoteGetMigrationTest, NotifyRemoteGetShortCircuitsFailedConfirmationOwner)
{
    const bool oldEnableDataReplication = FLAGS_enable_data_replication;
    Raii restoreFlag([oldEnableDataReplication]() { FLAGS_enable_data_replication = oldEnableDataReplication; });
    FLAGS_enable_data_replication = true;
    const std::string objectKey = "notify_remote_get_confirmation_owner_failure";
    const HostPort masterAddress("127.0.0.1:18889");
    RouteObjectToMaster(objectKey, masterAddress);
    auto api = std::make_shared<MigrateTestWorkerMasterOCApi>(masterAddress, localAddress_);
    int requestCount = 0;
    api->createMultiCopyMeta_ = [&requestCount](master::CreateMultiCopyMetaReqPb &,
                                                master::CreateMultiCopyMetaRspPb &) {
        ++requestCount;
        return Status(K_RPC_UNAVAILABLE, "master unavailable");
    };
    workerMasterApiManager_->SetDefaultApi(api);
    QueryMetaMap queryMetas{ { objectKey, MakeQueryMeta() } };
    std::unordered_set<std::string> failedConfirmationOwners;
    ScopedRequestContext requestContext;
    std::vector<std::string> confirmedIds;
    std::unordered_set<std::string> failedIds;

    impl_->ConfirmCopyMetaForNotifyRemoteGet({ objectKey }, queryMetas, confirmedIds, failedIds,
                                              failedConfirmationOwners);
    confirmedIds.clear();
    failedIds.clear();
    impl_->ConfirmCopyMetaForNotifyRemoteGet({ objectKey }, queryMetas, confirmedIds, failedIds,
                                              failedConfirmationOwners);

    EXPECT_EQ(requestCount, 1);
    EXPECT_TRUE(confirmedIds.empty());
    EXPECT_THAT(failedIds, Contains(objectKey));
}

TEST_F(NotifyRemoteGetMigrationTest, UsesInjectedRateController)
{
    ASSERT_EQ(impl_->rateController_, rateController_);
}

TEST_F(NotifyRemoteGetMigrationTest, NotifyRemoteGetRateLimitUsesMigratedBytes)
{
    const uint64_t maxBandwidth = FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul;
    const uint64_t migratedBytes = maxBandwidth / 4;
    uint64_t firstAvailableBandwidth = maxBandwidth - migratedBytes;
    uint64_t firstRate = MigrateDataRateController::CalculateSmoothedRate(maxBandwidth / 2, firstAvailableBandwidth);
    ASSERT_EQ(firstRate, (maxBandwidth / 2 + firstAvailableBandwidth) / 2);

    ASSERT_EQ(MigrateDataRateController::CalculateSmoothedRate(firstRate, 0), 0);
}

TEST_F(MigrateDataServiceTest, MigrateDataDirectResponseSetsLimitRate)
{
    MigrateDataDirectReqPb req;
    req.set_worker_addr("127.0.0.1:18889");
    auto *object1 = req.add_objects();
    object1->set_object_key("object1");
    object1->set_data_size(1024);
    auto *object2 = req.add_objects();
    object2->set_object_key("object2");
    object2->set_data_size(1024);
    std::unordered_set<std::string> failedIds{ "object2" };
    uint64_t migratedBytes = object1->data_size();
    MigrateDataDirectRspPb rsp;

    impl_->FillMigrateDataDirectResponse(req, failedIds, false, migratedBytes, rsp);

    ASSERT_GT(rsp.limit_rate(), 0);
    ASSERT_EQ(rsp.failed_object_keys_size(), 1);
    ASSERT_EQ(rsp.failed_object_keys(0), "object2");
}

TEST_F(MigrateDataServiceTest, MigrateDataDirectResponseSetsZeroLimitRateWhenOom)
{
    MigrateDataDirectReqPb req;
    req.set_worker_addr("127.0.0.1:18889");
    std::unordered_set<std::string> failedIds;
    MigrateDataDirectRspPb rsp;

    impl_->FillMigrateDataDirectResponse(req, failedIds, true, 0, rsp);

    ASSERT_EQ(rsp.limit_rate(), 0);
    ASSERT_EQ(rsp.remain_bytes(), 0);
}
}  // namespace ut
}  // namespace datasystem
