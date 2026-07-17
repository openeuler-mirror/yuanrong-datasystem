/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test WorkerOcServiceImpl.
 */

#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gmock/gmock.h>

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "tests/ut/worker/object_cache/test_placement_facade.h"
#include "tests/ut/worker/object_cache/test_metadata_route.h"
#define private public
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#undef private

using namespace ::testing;

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
namespace {
using WorkerTestPlacementFacade = TestPlacementFacade;
using ClearDataRetryIds = WorkerOcServiceClearDataFlow::ClearDataRetryIds;
constexpr int64_t kMetaMovingRetryTimeoutMs = 1'000;
constexpr size_t kExpectedMetaMovingRpcCalls = 2;
constexpr uint64_t kMetaMovingSuccessVersion = 7;

class FakeWorkerMasterOCApi final : public worker::WorkerLocalMasterOCApi {
public:
    explicit FakeWorkerMasterOCApi(const HostPort &localAddr) : WorkerLocalMasterOCApi(nullptr, localAddr, nullptr)
    {
    }

    Status Init() override
    {
        return Status::OK();
    }

    Status GIncreaseMasterRef(master::GIncreaseReqPb &req, master::GIncreaseRspPb &rsp) override
    {
        requestedObjectKeys_.assign(req.object_keys().begin(), req.object_keys().end());
        rsp = response_;
        return status_;
    }

    void SetResponse(const master::GIncreaseRspPb &response)
    {
        response_ = response;
    }

    const std::vector<std::string> &RequestedObjectKeys() const
    {
        return requestedObjectKeys_;
    }

private:
    master::GIncreaseRspPb response_;
    Status status_{ Status::OK() };
    std::vector<std::string> requestedObjectKeys_;
};

class FakeWorkerMasterApiManager final : public worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi> {
public:
    FakeWorkerMasterApiManager(HostPort &workerAddr, const worker::MetadataRouteResolver &metadataRoute)
        : WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>(workerAddr, nullptr, metadataRoute)
    {
    }

    std::shared_ptr<worker::WorkerMasterOCApi> CreateWorkerMasterApi(const HostPort &masterAddress) override
    {
        (void)masterAddress;
        return api_;
    }

    std::shared_ptr<worker::WorkerMasterOCApi> GetWorkerMasterApi(const HostPort &masterAddress) override
    {
        (void)masterAddress;
        return api_;
    }

    void SetApi(std::shared_ptr<worker::WorkerMasterOCApi> api)
    {
        api_ = std::move(api);
    }

private:
    std::shared_ptr<worker::WorkerMasterOCApi> api_;
};
}  // namespace

class WorkerOcServiceImplTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        Init();
    }

    void Init()
    {
        objectTable_ = std::make_shared<object_cache::ObjectTable>();
        globalRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        localAddress_ = HostPort("127.0.0.1", 18481);
        DS_ASSERT_OK(topologyRuntime_.Init(localAddress_));
        endpointPolicy_ = std::make_unique<ObjectEndpointPolicy>(metadataRoute_,
                                                                 topologyRuntime_.Engine()->Membership());
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, localAddress_, localAddress_,
                                                                     metadataRoute_, nullptr);
        WorkerOcServiceCrudParam param{
            .workerMasterApiManager = nullptr,
            .workerRequestManager = requestManager_,
            .memoryRefTable = nullptr,
            .objectTable = objectTable_,
            .evictionManager = evictionManager_,
            .workerDevOcManager = nullptr,
            .asyncPersistenceDelManager = nullptr,
            .asyncSendManager = nullptr,
            .metadataSize = 0,
            .persistenceApi = nullptr,
            .metadataRouteResolver = &metadataRoute_,
            .endpointPolicy = endpointPolicy_.get(),
            .exitRequested = &exitRequested_,
            .allowDirectoryLag = false,
        };
        deleteProc_ = std::make_shared<WorkerOcServiceDeleteImpl>(param, nullptr, localAddress_, nullptr);
        gRefProc_ =
            std::make_shared<WorkerOcServiceGlobalReferenceImpl>(param, globalRefTable_, nullptr, localAddress_);
        impl_ = std::make_shared<WorkerOCServiceImpl>(
            localAddress_, localAddress_, objectTable_, nullptr, evictionManager_, nullptr, nullptr, nullptr,
            topologyRuntime_.Engine(), metadataRoute_, topologyRuntime_.Engine()->Membership(), &exitRequested_,
            topologyRuntime_.Engine()->IsRestart(), false);
        dataClearImpl_ = std::make_shared<WorkerOcServiceClearDataFlow>(
            objectTable_, globalRefTable_, nullptr, gRefProc_, deleteProc_, nullptr, metadataRoute_, *endpointPolicy_,
            localAddress_.ToString());
        impl_->InitServiceImpl();
    }

    void TearDown() override
    {
        RecoverMasterAppRefEvent::GetInstance().RemoveSubscriber(kRecoverMasterAppRefSubscriber);
        dataClearImpl_.reset();
        deleteProc_.reset();
        gRefProc_.reset();
        impl_.reset();
        evictionManager_.reset();
        RELEASE_STUBS  // Clear global stubs to prevent interference with concurrent tests
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
        DS_ASSERT_OK(objectTable_->Insert(objectKey, std::move(obj)));
    }

    void AddWorkerRef(const std::string &objectKey, const std::string &clientId = "client-id")
    {
        std::vector<std::string> objectKeys{ objectKey };
        std::vector<std::string> failIncIds;
        std::vector<std::string> firstIncIds;
        DS_ASSERT_OK(globalRefTable_->GIncreaseRef(ClientKey::Intern(clientId), objectKeys, failIncIds, firstIncIds));
        ASSERT_TRUE(failIncIds.empty());
    }

protected:
    static constexpr const char *kRecoverMasterAppRefSubscriber = "WorkerOcServiceImplTest.RecoverMasterAppRef";

    WorkerTestPlacementFacade placement_;
    worker::MetadataRouteResolver metadataRoute_{ &placement_, worker::MetadataRouteOptions{} };
    ObjectTopologyTestRuntime topologyRuntime_;
    std::unique_ptr<ObjectEndpointPolicy> endpointPolicy_;
    std::atomic<bool> exitRequested_{ false };
    HostPort localAddress_;
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable_;
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    WorkerRequestManager requestManager_;
    std::shared_ptr<WorkerOCServiceImpl> impl_;
    std::shared_ptr<WorkerOcServiceGlobalReferenceImpl> gRefProc_;
    std::shared_ptr<WorkerOcServiceDeleteImpl> deleteProc_;
    std::shared_ptr<WorkerOcServiceClearDataFlow> dataClearImpl_;
};

TEST_F(WorkerOcServiceImplTest, SingleMetaMovingWithoutRedirectInfoRetries)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(kMetaMovingRetryTimeoutMs);
    master::CreateMetaReqPb request;
    master::CreateMetaRspPb response;
    std::shared_ptr<worker::WorkerMasterOCApi> masterApi;
    size_t rpcCalls = 0;
    std::function<Status(master::CreateMetaReqPb &, master::CreateMetaRspPb &)> invoke =
        [&rpcCalls](master::CreateMetaReqPb &, master::CreateMetaRspPb &rsp) {
            ++rpcCalls;
            if (rpcCalls == 1) {
                rsp.set_meta_is_moving(true);
            } else {
                rsp.set_version(kMetaMovingSuccessVersion);
            }
            return Status::OK();
        };

    DS_ASSERT_OK(deleteProc_->RedirectRetryWhenMetaMoving(request, response, masterApi, invoke));

    EXPECT_EQ(rpcCalls, kExpectedMetaMovingRpcCalls);
    EXPECT_EQ(response.version(), kMetaMovingSuccessVersion);
}

TEST_F(WorkerOcServiceImplTest, BatchMetaMovingWithoutRedirectInfoRetries)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(kMetaMovingRetryTimeoutMs);
    master::DeleteAllCopyMetaReqPb request;
    master::DeleteAllCopyMetaRspPb response;
    size_t rpcCalls = 0;
    std::function<Status(master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &)> invoke =
        [&rpcCalls](master::DeleteAllCopyMetaReqPb &, master::DeleteAllCopyMetaRspPb &rsp) {
            ++rpcCalls;
            rsp.set_meta_is_moving(rpcCalls == 1);
            return Status::OK();
        };

    DS_ASSERT_OK(WorkerOcServiceCrudCommonApi::RedirectRetryWhenMetasMoving(request, response, invoke));

    EXPECT_EQ(rpcCalls, kExpectedMetaMovingRpcCalls);
    EXPECT_FALSE(response.meta_is_moving());
}

TEST_F(WorkerOcServiceImplTest, PayloadMetaMovingWithoutRedirectInfoRetries)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(kMetaMovingRetryTimeoutMs);
    master::QueryMetaReqPb request;
    master::QueryMetaRspPb response;
    std::vector<RpcMessage> payloads;
    size_t rpcCalls = 0;
    std::function<Status(master::QueryMetaReqPb &, master::QueryMetaRspPb &, std::vector<RpcMessage> &)> invoke =
        [&rpcCalls](master::QueryMetaReqPb &, master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &) {
            ++rpcCalls;
            rsp.set_meta_is_moving(rpcCalls == 1);
            return Status::OK();
        };

    DS_ASSERT_OK(deleteProc_->RedirectRetryWhenMetasMoving(request, response, payloads, invoke));

    EXPECT_EQ(rpcCalls, kExpectedMetaMovingRpcCalls);
    EXPECT_FALSE(response.meta_is_moving());
}

TEST_F(WorkerOcServiceImplTest, TestParallelClearData)
{
    std::vector<std::thread> threads;
    int threadCount = 5;
    int batchCount = 100;

    std::vector<std::string> objKeys{ "key1", "key2" };
    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([this, &objKeys, batchCount] {
            for (int n = 0; n < batchCount; n++) {
                dataClearImpl_->ClearObject(objKeys);
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    for (const auto &id : objKeys) {
        std::shared_ptr<SafeObjType> entry;
        auto rc = objectTable_->Get(id, entry);
        ASSERT_EQ(rc.GetCode(), K_NOT_FOUND);
    }
}

TEST_F(WorkerOcServiceImplTest, CollectDisconnectedClientRefIdsReturnsOnlyMissingClients)
{
    const auto liveClient = ClientKey::Intern("live-client");
    const auto staleClient = ClientKey::Intern("stale-client");
    DS_ASSERT_OK(worker::ClientManager::Instance().AddClient(liveClient, -1));
    Raii cleanup([&liveClient]() { worker::ClientManager::Instance().RemoveClient(liveClient); });

    std::vector<std::string> failIncIds;
    std::vector<std::string> firstIncIds;
    DS_ASSERT_OK(
        impl_->globalRefTable_->GIncreaseRef(liveClient, { "live-object" }, failIncIds, firstIncIds));
    failIncIds.clear();
    firstIncIds.clear();
    DS_ASSERT_OK(
        impl_->globalRefTable_->GIncreaseRef(staleClient, { "stale-object" }, failIncIds, firstIncIds));

    auto disconnectedClients = impl_->CollectDisconnectedClientRefIds();

    EXPECT_THAT(disconnectedClients, UnorderedElementsAre(staleClient));
}

TEST_F(WorkerOcServiceImplTest, CollectMissingSourceMasterRefsReturnsOnlyLiveLocalRefsOwnedBySourceMaster)
{
    const HostPort sourceMaster("127.0.0.1", 18481);
    const HostPort peerMaster("127.0.0.1", 18482);
    placement_.SetOwner("already-on-master", sourceMaster);
    placement_.SetOwner("missing-source-master", sourceMaster);
    placement_.SetOwner("missing-peer-master", peerMaster);

    auto addWorkerRef = [this](const std::string &objectKey, const std::string &clientId) {
        std::vector<std::string> failIncIds;
        std::vector<std::string> firstIncIds;
        DS_ASSERT_OK(
            impl_->globalRefTable_->GIncreaseRef(ClientKey::Intern(clientId), { objectKey }, failIncIds, firstIncIds));
        ASSERT_TRUE(failIncIds.empty());
    };
    addWorkerRef("already-on-master", "client-1");
    addWorkerRef("missing-source-master", "client-2");
    addWorkerRef("missing-peer-master", "client-3");

    std::unordered_map<std::string, std::unordered_set<ClientKey>> localRefTable;
    impl_->globalRefTable_->GetAllRef(localRefTable);
    std::unordered_set<std::string> sourceMasterRefIds{ "already-on-master" };
    EXPECT_THAT(localRefTable, Contains(Key("already-on-master")));
    EXPECT_THAT(localRefTable, Contains(Key("missing-source-master")));
    EXPECT_THAT(localRefTable, Contains(Key("missing-peer-master")));
    auto missingRefs = impl_->CollectMissingSourceMasterRefs(sourceMaster, localRefTable, sourceMasterRefIds);

    EXPECT_THAT(missingRefs, UnorderedElementsAre("missing-source-master"));
}

TEST_F(WorkerOcServiceImplTest, GIncreaseMasterRefWithLockFailsWhenMasterReplyHasOkStatusAndFailedKeys)
{
    const HostPort masterAddress("127.0.0.1", 18482);
    const std::string successObject = "restore-success";
    const std::string failedObject = "restore-failed";
    AddWorkerRef(successObject, "client-1");
    AddWorkerRef(failedObject, "client-2");

    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    master::GIncreaseRspPb response;
    response.mutable_last_rc()->set_error_code(K_OK);
    response.add_failed_object_keys(failedObject);
    api->SetResponse(response);

    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param{
        .workerMasterApiManager = apiManager,
        .workerRequestManager = requestManager_,
        .memoryRefTable = nullptr,
        .objectTable = objectTable_,
        .evictionManager = evictionManager_,
        .workerDevOcManager = nullptr,
        .asyncPersistenceDelManager = nullptr,
        .asyncSendManager = nullptr,
        .metadataSize = 0,
        .persistenceApi = nullptr,
        .metadataRouteResolver = &metadataRoute_,
        .endpointPolicy = endpointPolicy_.get(),
        .exitRequested = &exitRequested_,
        .allowDirectoryLag = false,
    };
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    std::vector<std::string> failedIds;
    auto rc = gRefProc.GIncreaseMasterRefWithLock(masterAddress, { successObject, failedObject }, failedIds);

    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_THAT(failedIds, UnorderedElementsAre(failedObject));
    EXPECT_THAT(api->RequestedObjectKeys(), UnorderedElementsAre(successObject, failedObject));
}

TEST_F(WorkerOcServiceImplTest, DISABLED_ClearDataImplDispatchesMatchedObjectsToClearAndRebuild)
{
    using GetMatchObjectIdsMethod = Status (WorkerOcServiceClearDataFlow::*)(const ClearDataReqPb &,
                                                                             std::vector<std::string> &);
    using ClearMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);
    using RebuildRefForMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);

    std::vector<std::string> matchObjIds{ "obj1", "obj2" };
    std::vector<std::string> clearObjIds;
    std::vector<std::string> rebuildObjIds;
    BINEXPECT_CALL((GetMatchObjectIdsMethod) & WorkerOcServiceClearDataFlow::GetMatchObjectIds, (_, _))
        .WillOnce(Invoke([&matchObjIds](const ClearDataReqPb &, std::vector<std::string> &outObjIds) {
            outObjIds = matchObjIds;
            return Status::OK();
        }));
    BINEXPECT_CALL((ClearMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _))
        .WillOnce(Invoke([&clearObjIds](const std::vector<std::string> &objIds,
                                        ClearDataRetryIds &) { clearObjIds = objIds; }));
    BINEXPECT_CALL((RebuildRefForMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::RebuildRefForMatchedObjects,
                   (_, _))
        .WillOnce(Invoke([&rebuildObjIds](const std::vector<std::string> &objIds,
                                          ClearDataRetryIds &) {
            rebuildObjIds = objIds;
        }));

    ClearDataRetryIds retryIds;
    ClearDataReqPb req;
    DS_ASSERT_OK(dataClearImpl_->ClearDataImpl(req, retryIds));
    EXPECT_THAT(clearObjIds, ElementsAreArray(matchObjIds));
    EXPECT_THAT(rebuildObjIds, ElementsAreArray(matchObjIds));
    EXPECT_TRUE(retryIds.Empty());
}

TEST_F(WorkerOcServiceImplTest, ClearDataImplReturnsWhenSelectObjectsFailed)
{
    using GetMatchObjectIdsMethod = Status (WorkerOcServiceClearDataFlow::*)(const ClearDataReqPb &,
                                                                             std::vector<std::string> &);
    using ClearMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);
    using RebuildRefForMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);

    Status selectFailed(StatusCode::K_RUNTIME_ERROR, "select failed");
    BINEXPECT_CALL((GetMatchObjectIdsMethod) & WorkerOcServiceClearDataFlow::GetMatchObjectIds, (_, _))
        .WillOnce(Return(selectFailed));
    BINEXPECT_CALL((ClearMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _)).Times(0);
    BINEXPECT_CALL((RebuildRefForMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::RebuildRefForMatchedObjects,
                   (_, _))
        .Times(0);

    ClearDataRetryIds retryIds;
    ClearDataReqPb req;
    auto rc = dataClearImpl_->ClearDataImpl(req, retryIds);
    EXPECT_EQ(rc.GetCode(), selectFailed.GetCode());
    EXPECT_EQ(rc.GetMsg(), selectFailed.GetMsg());
    EXPECT_TRUE(retryIds.Empty());
}

TEST_F(WorkerOcServiceImplTest, RebuildRefForMatchedObjectsShouldCollectRetryIds)
{
    using IncreaseMasterRefMethod = Status (WorkerOcServiceGlobalReferenceImpl::*)(
        std::function<bool(const std::string &)>, std::vector<std::string> &);

    AddObject("obj1");
    AddObject("obj2");
    AddObject("obj3");
    AddObject("obj4");
    AddWorkerRef("obj1", "client-1");
    AddWorkerRef("obj2", "client-2");
    AddWorkerRef("obj3", "client-3");

    RecoverMasterAppRefEvent::GetInstance().AddSubscriber(
        kRecoverMasterAppRefSubscriber,
        [](std::function<bool(const std::string &)> matchFunc, const std::string &) {
            EXPECT_TRUE(matchFunc("obj1"));
            EXPECT_FALSE(matchFunc("obj2"));
            EXPECT_TRUE(matchFunc("obj3"));
            EXPECT_FALSE(matchFunc("obj4"));
            return Status(StatusCode::K_RUNTIME_ERROR, "recover failed");
        });
    BINEXPECT_CALL((IncreaseMasterRefMethod) & WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRefWithLock, (_, _))
        .WillOnce(Invoke([](std::function<bool(const std::string &)> matchFunc, std::vector<std::string> &failedIds) {
            EXPECT_TRUE(matchFunc("obj1"));
            EXPECT_TRUE(matchFunc("obj2"));
            EXPECT_TRUE(matchFunc("obj3"));
            EXPECT_FALSE(matchFunc("obj4"));
            failedIds = { "obj2" };
            return Status(StatusCode::K_RUNTIME_ERROR, "increase failed");
        }));

    ClearDataRetryIds retryIds;
    dataClearImpl_->RebuildRefForMatchedObjects({ "obj1", "obj2", "obj3", "obj4" }, retryIds);

    EXPECT_THAT(retryIds.increaseFailedIds, UnorderedElementsAre("obj2"));
    EXPECT_THAT(retryIds.recoverAppRefFailedIds, UnorderedElementsAre("obj1", "obj3"));
}

TEST_F(WorkerOcServiceImplTest, ClearDataRetryImplShouldRouteFailedIdsToRetryStages)
{
    using ClearMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);
    using RetryIncreaseMasterRefMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);
    using RetryRecoverMasterAppRefMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);

    std::vector<std::string> clearObjIds;
    std::vector<std::string> increaseObjIds;
    std::vector<std::string> recoverObjIds;
    BINEXPECT_CALL((ClearMatchedObjectsMethod) & WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _))
        .WillOnce(Invoke([&clearObjIds](const std::vector<std::string> &objIds,
                                        ClearDataRetryIds &retryIds) {
            clearObjIds = objIds;
            retryIds.clearFailedIds.emplace("clear-next");
        }));
    BINEXPECT_CALL((RetryIncreaseMasterRefMethod) & WorkerOcServiceClearDataFlow::RetryIncreaseMasterRef, (_, _))
        .WillOnce(Invoke([&increaseObjIds](const std::vector<std::string> &objIds,
                                           ClearDataRetryIds &retryIds) {
            increaseObjIds = objIds;
            retryIds.increaseFailedIds.emplace("increase-next");
        }));
    BINEXPECT_CALL((RetryRecoverMasterAppRefMethod) &
                       WorkerOcServiceClearDataFlow::RetryRecoverMasterAppRef,
                   (_, _))
        .WillOnce(Invoke([&recoverObjIds](const std::vector<std::string> &objIds,
                                          ClearDataRetryIds &retryIds) {
            recoverObjIds = objIds;
            retryIds.recoverAppRefFailedIds.emplace("recover-next");
        }));

    ClearDataRetryIds retryIds;
    retryIds.clearFailedIds = { "clear-1", "clear-2" };
    retryIds.increaseFailedIds = { "increase-1" };
    retryIds.recoverAppRefFailedIds = { "recover-1", "recover-2" };

    ClearDataRetryIds nextRetryIds;
    ClearDataReqPb req;
    dataClearImpl_->ClearDataRetryImpl(req, retryIds, nextRetryIds);

    EXPECT_THAT(clearObjIds, UnorderedElementsAre("clear-1", "clear-2"));
    EXPECT_THAT(increaseObjIds, UnorderedElementsAre("increase-1"));
    EXPECT_THAT(recoverObjIds, UnorderedElementsAre("recover-1", "recover-2"));
    EXPECT_THAT(nextRetryIds.clearFailedIds, UnorderedElementsAre("clear-next"));
    EXPECT_THAT(nextRetryIds.increaseFailedIds, UnorderedElementsAre("increase-next"));
    EXPECT_THAT(nextRetryIds.recoverAppRefFailedIds, UnorderedElementsAre("recover-next"));
}

}  // namespace ut
}  // namespace datasystem
