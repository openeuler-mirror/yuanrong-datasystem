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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gmock/gmock.h>

#include "../../../common/binmock/binmock.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "tests/ut/worker/object_cache/test_metadata_route.h"
#include "tests/ut/worker/object_cache/test_placement_facade.h"
#include "ut/common.h"
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
constexpr int64_t K_META_MOVING_RETRY_TIMEOUT_MS = 1'000;
constexpr size_t K_EXPECTED_META_MOVING_RPC_CALLS = 2;
constexpr uint64_t K_META_MOVING_SUCCESS_VERSION = 7;
using WorkerMasterOCApiManager = worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>;

constexpr const char *K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT =
    "WorkerOcServiceGlobalReferenceImpl.SleepForRefMovingRetry.beforeSleep";
constexpr int64_t K_INJECT_WAIT_POLL_MS = 1;
constexpr uint64_t K_FIRST_INJECT_EXECUTE_COUNT = 1;
constexpr int K_EXPECTED_REF_MOVING_GROUP_RPC_CALLS = 3;
constexpr const char *K_LOCAL_TEST_HOST = "127.0.0.1";
constexpr uint16_t K_PEER_MASTER_PORT = 18482;
constexpr int64_t K_WAIT_FIRST_MOVING_CALL_TIMEOUT_MS = 1000;
constexpr int64_t K_WAIT_RETRY_SLEEP_INJECT_TIMEOUT_MS = 1000;
constexpr int64_t K_LOCK_PROBE_TIMEOUT_MS = 1000;

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

class FakeWorkerMasterOCApi final : public worker::WorkerLocalMasterOCApi {
public:
    explicit FakeWorkerMasterOCApi(const HostPort &localAddr) : WorkerLocalMasterOCApi(nullptr, localAddr, nullptr)
    {
    }

    ~FakeWorkerMasterOCApi() override = default;

    Status Init() override
    {
        return Status::OK();
    }

    Status GIncreaseMasterRef(master::GIncreaseReqPb &req, master::GIncreaseRspPb &rsp) override
    {
        bool returnMoving = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++increaseCallCount_;
            requestedObjectKeys_.assign(req.object_keys().begin(), req.object_keys().end());
            if (returnRefMovingOnce_) {
                returnRefMovingOnce_ = false;
                firstRefMovingCallSeen_ = true;
                returnMoving = true;
            }
        }
        if (returnMoving) {
            cv_.notify_all();
            rsp.Clear();
            rsp.set_ref_is_moving(true);
            return Status::OK();
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            requestedObjectKeys_.assign(req.object_keys().begin(), req.object_keys().end());
        }
        rsp = response_;
        return status_;
    }

    Status GDecreaseMasterRef(master::GDecreaseReqPb &req, master::GDecreaseRspPb &rsp) override
    {
        bool returnMoving = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++decreaseCallCount_;
            requestedObjectKeys_.assign(req.object_keys().begin(), req.object_keys().end());
            if (returnRefMovingOnce_) {
                returnRefMovingOnce_ = false;
                firstRefMovingCallSeen_ = true;
                returnMoving = true;
            }
        }
        if (returnMoving) {
            cv_.notify_all();
            rsp.Clear();
            rsp.set_ref_is_moving(true);
            return Status::OK();
        }
        rsp = decreaseResponse_;
        return Status::OK();
    }

    void SetResponse(const master::GIncreaseRspPb &response)
    {
        response_ = response;
    }

    void SetDecreaseResponse(const master::GDecreaseRspPb &response)
    {
        decreaseResponse_ = response;
    }

    std::vector<std::string> RequestedObjectKeys() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return requestedObjectKeys_;
    }

    void SetReturnRefMovingOnce()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        returnRefMovingOnce_ = true;
        firstRefMovingCallSeen_ = false;
    }

    bool WaitForFirstRefMovingCall(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] { return firstRefMovingCallSeen_; });
    }

    int IncreaseCallCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return increaseCallCount_;
    }

    int DecreaseCallCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return decreaseCallCount_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    master::GIncreaseRspPb response_;
    master::GDecreaseRspPb decreaseResponse_;
    Status status_{ Status::OK() };
    std::vector<std::string> requestedObjectKeys_;
    bool returnRefMovingOnce_{ false };
    bool firstRefMovingCallSeen_{ false };
    int increaseCallCount_{ 0 };
    int decreaseCallCount_{ 0 };
};

class FakeWorkerMasterApiManager final : public worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi> {
public:
    FakeWorkerMasterApiManager(HostPort &workerAddr, const worker::MetadataRouteResolver &metadataRoute)
        : WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>(workerAddr, nullptr, metadataRoute)
    {
    }

    ~FakeWorkerMasterApiManager() override = default;

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

class TestDistributedTopology final {
public:
    TestDistributedTopology(const HostPort &localAddress, const HostPort &peerAddress)
        : metadataRoute_(&placement_, worker::MetadataRouteOptions{}),
          membership_(snapshots_),
          endpointPolicy_(metadataRoute_, membership_)
    {
        initStatus_ = Init(localAddress, peerAddress);
    }

    ~TestDistributedTopology() = default;

    const Status &InitStatus() const
    {
        return initStatus_;
    }

    void SetOwner(const std::string &objectKey, const HostPort &address)
    {
        placement_.SetOwner(objectKey, address);
    }

    const worker::MetadataRouteResolver *Route() const
    {
        return &metadataRoute_;
    }

    const ObjectEndpointPolicy *EndpointPolicy() const
    {
        return &endpointPolicy_;
    }

private:
    static constexpr size_t MEMBER_ID_SIZE = 16;
    static constexpr size_t SHA256_HEX_SIZE = 64;
    static constexpr uint64_t TOPOLOGY_VERSION = 1;
    static constexpr char LOCAL_MEMBER_ID_FILL = 'l';
    static constexpr char PEER_MEMBER_ID_FILL = 'p';
    static constexpr char DIGEST_FILL = 'b';
    static constexpr uint32_t LOCAL_MEMBER_TOKEN = 1;
    static constexpr uint32_t PEER_MEMBER_TOKEN = 2;

    Status Init(const HostPort &localAddress, const HostPort &peerAddress)
    {
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = TOPOLOGY_VERSION;
        topology.members = {
            cluster::Member{ { std::string(MEMBER_ID_SIZE, LOCAL_MEMBER_ID_FILL), localAddress.ToString() },
                             cluster::MemberState::ACTIVE, { LOCAL_MEMBER_TOKEN } },
            cluster::Member{ { std::string(MEMBER_ID_SIZE, PEER_MEMBER_ID_FILL), peerAddress.ToString() },
                             cluster::MemberState::ACTIVE, { PEER_MEMBER_TOKEN } }
        };
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        RETURN_IF_NOT_OK(cluster::TopologySnapshot::Create(std::move(topology), TOPOLOGY_VERSION,
                                                           std::string(SHA256_HEX_SIZE, DIGEST_FILL), snapshot));
        cluster::SnapshotUpdateOutcome outcome;
        RETURN_IF_NOT_OK(snapshots_.Publish(std::move(snapshot), outcome));
        return Status::OK();
    }

    WorkerTestPlacementFacade placement_;
    worker::MetadataRouteResolver metadataRoute_;
    cluster::TopologySnapshotState snapshots_;
    cluster::MembershipEndpointView membership_;
    ObjectEndpointPolicy endpointPolicy_;
    Status initStatus_;
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
        WorkerOcServiceCrudParam param = MakeCrudParam();
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

    WorkerOcServiceCrudParam MakeCrudParam(std::shared_ptr<WorkerMasterOCApiManager> apiManager = nullptr,
                                           const worker::MetadataRouteResolver *metadataRoute = nullptr,
                                           const ObjectEndpointPolicy *endpointPolicy = nullptr)
    {
        return WorkerOcServiceCrudParam{
            .workerMasterApiManager = std::move(apiManager),
            .workerRequestManager = requestManager_,
            .memoryRefTable = nullptr,
            .objectTable = objectTable_,
            .evictionManager = evictionManager_,
            .workerDevOcManager = nullptr,
            .asyncPersistenceDelManager = nullptr,
            .asyncSendManager = nullptr,
            .metadataSize = 0,
            .persistenceApi = nullptr,
            .metadataRouteResolver = metadataRoute == nullptr ? &metadataRoute_ : metadataRoute,
            .endpointPolicy = endpointPolicy == nullptr ? endpointPolicy_.get() : endpointPolicy,
            .exitRequested = &exitRequested_,
            .allowDirectoryLag = false,
        };
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
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
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
                rsp.set_version(K_META_MOVING_SUCCESS_VERSION);
            }
            return Status::OK();
        };

    DS_ASSERT_OK(deleteProc_->RedirectRetryWhenMetaMoving(request, response, masterApi, invoke));

    EXPECT_EQ(rpcCalls, K_EXPECTED_META_MOVING_RPC_CALLS);
    EXPECT_EQ(response.version(), K_META_MOVING_SUCCESS_VERSION);
}

TEST_F(WorkerOcServiceImplTest, BatchMetaMovingWithoutRedirectInfoRetries)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
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

    EXPECT_EQ(rpcCalls, K_EXPECTED_META_MOVING_RPC_CALLS);
    EXPECT_FALSE(response.meta_is_moving());
}

TEST_F(WorkerOcServiceImplTest, PayloadMetaMovingWithoutRedirectInfoRetries)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
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

    EXPECT_EQ(rpcCalls, K_EXPECTED_META_MOVING_RPC_CALLS);
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
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    std::vector<std::string> failedIds;
    auto rc = gRefProc.GIncreaseMasterRefWithLock(masterAddress, { successObject, failedObject }, failedIds);

    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_THAT(failedIds, UnorderedElementsAre(failedObject));
    EXPECT_THAT(api->RequestedObjectKeys(), UnorderedElementsAre(successObject, failedObject));
}

TEST_F(WorkerOcServiceImplTest, UpdateMasterForFirstIdsRollsBackAllPendingGroupsOnRefMoving)
{
    const HostPort peerAddress(K_LOCAL_TEST_HOST, K_PEER_MASTER_PORT);
    const std::string firstObject = "moving-first-group-object";
    const std::string secondObject = "moving-second-group-object";
    const std::string clientId = "client-1";
    TestDistributedTopology topology(localAddress_, peerAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(firstObject, localAddress_);
    topology.SetOwner(secondObject, peerAddress);

    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetReturnRefMovingOnce();
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    std::vector<std::string> objectKeys{ firstObject, secondObject };
    std::vector<std::string> failIncIds;
    std::vector<std::string> firstIncIds;
    DS_ASSERT_OK(globalRefTable_->GIncreaseRef(ClientKey::Intern(clientId), objectKeys, failIncIds, firstIncIds));
    ASSERT_THAT(firstIncIds, UnorderedElementsAre(firstObject, secondObject));

    GIncreaseReqPb req;
    req.set_address(clientId);
    req.set_client_id(clientId);
    std::vector<std::string> retryIncIds;
    auto rc = gRefProc.UpdateMasterForFirstIds(req, firstIncIds, failIncIds, &retryIncIds);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    EXPECT_TRUE(failIncIds.empty());
    EXPECT_THAT(retryIncIds, UnorderedElementsAre(firstObject, secondObject));
    std::unordered_map<std::string, std::unordered_set<ClientKey>> localRefTable;
    globalRefTable_->GetAllRef(localRefTable);
    EXPECT_THAT(localRefTable, Not(Contains(Key(firstObject))));
    EXPECT_THAT(localRefTable, Not(Contains(Key(secondObject))));
}

TEST_F(WorkerOcServiceImplTest, UpdateMasterForFinishedIdsRollsBackAllPendingGroupsOnRefMoving)
{
    const HostPort peerAddress(K_LOCAL_TEST_HOST, K_PEER_MASTER_PORT);
    const std::string firstObject = "moving-finished-first-group-object";
    const std::string secondObject = "moving-finished-second-group-object";
    const ClientKey clientId = ClientKey::Intern("client-1");
    TestDistributedTopology topology(localAddress_, peerAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(firstObject, localAddress_);
    topology.SetOwner(secondObject, peerAddress);

    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetReturnRefMovingOnce();
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    AddWorkerRef(firstObject, clientId.ToString());
    AddWorkerRef(secondObject, clientId.ToString());
    std::vector<std::string> objectKeys{ firstObject, secondObject };
    std::vector<std::string> failDecIds;
    std::vector<std::string> finishDecIds;
    DS_ASSERT_OK(globalRefTable_->GDecreaseRef(clientId, objectKeys, failDecIds, finishDecIds));
    ASSERT_THAT(finishDecIds, UnorderedElementsAre(firstObject, secondObject));

    std::unordered_set<std::string> unAliveIds;
    std::vector<std::string> retryDecIds;
    auto rc = gRefProc.UpdateMasterForFinishedIds(clientId, finishDecIds, unAliveIds, failDecIds, &retryDecIds);

    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);
    EXPECT_TRUE(failDecIds.empty());
    EXPECT_THAT(retryDecIds, UnorderedElementsAre(firstObject, secondObject));
    std::unordered_map<std::string, std::unordered_set<ClientKey>> localRefTable;
    globalRefTable_->GetAllRef(localRefTable);
    ASSERT_THAT(localRefTable, Contains(Key(firstObject)));
    ASSERT_THAT(localRefTable, Contains(Key(secondObject)));
    EXPECT_THAT(localRefTable[firstObject], Contains(clientId));
    EXPECT_THAT(localRefTable[secondObject], Contains(clientId));
}

TEST_F(WorkerOcServiceImplTest, UpdateMasterForFinishedIdsKeepsLocalDecreaseOnNonRefMovingErrorWithoutFailedKeys)
{
    const std::string objectKey = "non-ref-moving-dec-error";
    const ClientKey clientId = ClientKey::Intern("client-1");
    const HostPort peerAddress(K_LOCAL_TEST_HOST, K_PEER_MASTER_PORT);
    TestDistributedTopology topology(localAddress_, peerAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(objectKey, localAddress_);
    AddWorkerRef(objectKey, clientId.ToString());

    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    master::GDecreaseRspPb response;
    response.mutable_last_rc()->set_error_code(K_KVSTORE_ERROR);
    response.mutable_last_rc()->set_error_msg("injected kv store error");
    api->SetDecreaseResponse(response);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failDecIds;
    std::vector<std::string> finishDecIds;
    DS_ASSERT_OK(globalRefTable_->GDecreaseRef(clientId, objectKeys, failDecIds, finishDecIds));
    ASSERT_THAT(finishDecIds, ElementsAre(objectKey));

    std::unordered_set<std::string> unAliveIds;
    std::vector<std::string> retryDecIds;
    auto rc = gRefProc.UpdateMasterForFinishedIds(clientId, finishDecIds, unAliveIds, failDecIds, &retryDecIds);

    EXPECT_EQ(rc.GetCode(), K_KVSTORE_ERROR);
    EXPECT_TRUE(failDecIds.empty());
    EXPECT_TRUE(retryDecIds.empty());
    std::unordered_map<std::string, std::unordered_set<ClientKey>> localRefTable;
    globalRefTable_->GetAllRef(localRefTable);
    EXPECT_THAT(localRefTable, Not(Contains(Key(objectKey))));
}

TEST_F(WorkerOcServiceImplTest, GIncreaseMasterRefWithLockRetriesAllMasterGroupsAfterRefMoving)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort peerAddress(K_LOCAL_TEST_HOST, K_PEER_MASTER_PORT);
    const std::string firstObject = "moving-restore-first-group-object";
    const std::string secondObject = "moving-restore-second-group-object";
    TestDistributedTopology topology(localAddress_, peerAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(firstObject, localAddress_);
    topology.SetOwner(secondObject, peerAddress);
    AddWorkerRef(firstObject, "client-1");
    AddWorkerRef(secondObject, "client-2");

    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetReturnRefMovingOnce();
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    std::vector<std::string> failedIds;
    auto rc = gRefProc.GIncreaseMasterRefWithLock([](const std::string &) { return true; }, failedIds);

    DS_EXPECT_OK(rc);
    EXPECT_TRUE(failedIds.empty());
    EXPECT_EQ(api->IncreaseCallCount(), K_EXPECTED_REF_MOVING_GROUP_RPC_CALLS);
}

TEST_F(WorkerOcServiceImplTest, GDecreaseRemoteClientIdRetriesAllMasterGroupsAfterRefMoving)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort peerAddress(K_LOCAL_TEST_HOST, K_PEER_MASTER_PORT);
    const std::string firstObject = "moving-remote-dec-first-group-object";
    const std::string secondObject = "moving-remote-dec-second-group-object";
    TestDistributedTopology topology(localAddress_, peerAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(firstObject, localAddress_);
    topology.SetOwner(secondObject, peerAddress);

    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetReturnRefMovingOnce();
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    std::vector<std::string> failedIds;
    auto rc =
        gRefProc.GDecreaseRefWithLockWithRemoteClientId({ firstObject, secondObject }, "remote-client", failedIds);

    DS_EXPECT_OK(rc);
    EXPECT_TRUE(failedIds.empty());
    EXPECT_EQ(api->DecreaseCallCount(), K_EXPECTED_REF_MOVING_GROUP_RPC_CALLS);
}

TEST_F(WorkerOcServiceImplTest, GIncreaseRefReleasesGRefLockBeforeRefMovingSleep)
{
    const std::string objectKey = "moving-ref-client-object";
    const std::string clientId = "client-1";

    bool savedSkipAuthenticate = FLAGS_skip_authenticate;
    FLAGS_skip_authenticate = true;
    Raii restoreSkipAuthenticate([savedSkipAuthenticate] { FLAGS_skip_authenticate = savedSkipAuthenticate; });

    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetReturnRefMovingOnce();
    const HostPort peerAddress(K_LOCAL_TEST_HOST, K_PEER_MASTER_PORT);
    TestDistributedTopology topology(localAddress_, peerAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(objectKey, localAddress_);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);

    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    GIncreaseReqPb req;
    req.set_address(clientId);
    req.set_client_id(clientId);
    req.add_object_keys(objectKey);
    GIncreaseRspPb rsp;

    DS_ASSERT_OK(inject::Set(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT, "pause"));
    auto refFuture = std::async(std::launch::async, [&gRefProc, &req, &rsp] {
        return gRefProc.GIncreaseRef(req, rsp);
    });
    auto clearRetrySleepInject =
        Raii([]() { (void)inject::Clear(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT); });

    ASSERT_TRUE(api->WaitForFirstRefMovingCall(std::chrono::milliseconds(K_WAIT_FIRST_MOVING_CALL_TIMEOUT_MS)));
    ASSERT_TRUE(WaitForInjectPointExecuteCount(
        K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT, K_FIRST_INJECT_EXECUTE_COUNT,
        std::chrono::milliseconds(K_WAIT_RETRY_SLEEP_INJECT_TIMEOUT_MS)));
    auto lockProbe = std::async(std::launch::async, [&gRefProc, &objectKey, &api] {
        std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
        gRefProc.BatchGRefLock(std::vector<std::string>{ objectKey }, false, lockedEntries);
        int callCount = api->IncreaseCallCount();
        gRefProc.BatchGRefUnlock(lockedEntries);
        return callCount;
    });

    ASSERT_EQ(lockProbe.wait_for(std::chrono::milliseconds(K_LOCK_PROBE_TIMEOUT_MS)), std::future_status::ready);
    const int callCountWhenProbeLocked = lockProbe.get();

    DS_ASSERT_OK(inject::Clear(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT));
    Status refStatus = refFuture.get();
    EXPECT_EQ(callCountWhenProbeLocked, 1)
        << "gRef lock should be released before the metadata moving retry sends the second master RPC.";
    DS_EXPECT_OK(refStatus);
    EXPECT_EQ(rsp.last_rc().error_code(), K_OK);
    EXPECT_EQ(rsp.failed_object_keys_size(), 0);
}

TEST_F(WorkerOcServiceImplTest, GIncreaseMasterRefWithLockReleasesGRefLockBeforeRefMovingSleep)
{
    const HostPort masterAddress(K_LOCAL_TEST_HOST, K_PEER_MASTER_PORT);
    const std::string objectKey = "moving-ref-object";
    AddWorkerRef(objectKey, "client-1");

    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetReturnRefMovingOnce();
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    WorkerOcServiceGlobalReferenceImpl gRefProc(param, globalRefTable_, nullptr, localAddress_);

    std::vector<std::string> failedIds;
    DS_ASSERT_OK(inject::Set(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT, "pause"));
    auto refFuture = std::async(std::launch::async, [&gRefProc, &masterAddress, &objectKey, &failedIds] {
        return gRefProc.GIncreaseMasterRefWithLock(masterAddress, { objectKey }, failedIds);
    });
    auto clearRetrySleepInject =
        Raii([]() { (void)inject::Clear(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT); });

    ASSERT_TRUE(api->WaitForFirstRefMovingCall(std::chrono::milliseconds(K_WAIT_FIRST_MOVING_CALL_TIMEOUT_MS)));
    ASSERT_TRUE(WaitForInjectPointExecuteCount(
        K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT, K_FIRST_INJECT_EXECUTE_COUNT,
        std::chrono::milliseconds(K_WAIT_RETRY_SLEEP_INJECT_TIMEOUT_MS)));
    auto lockProbe = std::async(std::launch::async, [&gRefProc, &objectKey, &api] {
        std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
        gRefProc.BatchGRefLock(std::vector<std::string>{ objectKey }, false, lockedEntries);
        int callCount = api->IncreaseCallCount();
        gRefProc.BatchGRefUnlock(lockedEntries);
        return callCount;
    });

    ASSERT_EQ(lockProbe.wait_for(std::chrono::milliseconds(K_LOCK_PROBE_TIMEOUT_MS)), std::future_status::ready);
    const int callCountWhenProbeLocked = lockProbe.get();

    DS_ASSERT_OK(inject::Clear(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT));
    Status refStatus = refFuture.get();
    EXPECT_EQ(callCountWhenProbeLocked, 1)
        << "gRef lock should be released before the metadata moving retry sends the second master RPC.";
    DS_EXPECT_OK(refStatus);
    EXPECT_TRUE(failedIds.empty());
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
