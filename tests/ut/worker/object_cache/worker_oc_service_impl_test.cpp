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

#include <atomic>
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
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"
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
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"
#include "datasystem/worker/worker_health_check.h"
#include "datasystem/worker/runtime/worker_runtime_state.h"
#include "tests/ut/worker/object_cache/test_metadata_route.h"
#include "tests/ut/worker/object_cache/test_placement_facade.h"
#include "ut/common.h"

DS_DECLARE_bool(enable_reconciliation);
DS_DECLARE_bool(enable_metadata_recovery);
DS_DECLARE_string(l2_cache_type);

#define private public
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#undef private
#include "datasystem/worker/object_cache/master_worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/object_cache_recovery_state.h"

using namespace testing;

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
using RecoverMetadataWithSummaryMethod = MetaDataRecoveryManager::RecoverySummary (MetaDataRecoveryManager::*)(
    const std::vector<std::string> &, std::string);
using RecoverMetadataOfDataMethod = Status (WorkerOCServiceImpl::*)(const std::vector<std::string> &,
                                                                    std::vector<std::string> &, std::string);
using RetryFailedMetadataRecoveryMethod = WorkerOcServiceClearDataFlow::RetryMetadataRecoveryResult (
    WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &);
using GetObjectFromAnywhereMethod = Status (WorkerOCServiceImpl::*)(const ReadKey &, const master::QueryMetaInfoPb &,
                                                                    std::vector<RpcMessage> &);
using FilterObjectsNeedClearByMasterMethod = void (WorkerOcServiceClearDataFlow::*)(
    const std::vector<std::string> &, std::vector<std::string> &, std::unordered_set<std::string> &,
    std::unordered_map<std::string, uint64_t> &);

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

bool MarkRuntimeRunning(worker::WorkerRuntimeFacade &runtime, const std::string &detail = "ready")
{
    worker::WorkerRunningEvidence evidence{ true, true, true, true, true, true };
    return runtime.TryCompleteRecovery(evidence, detail);
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

    Status CheckObjectDataLocation(master::CheckObjectDataLocationReqPb &req,
                                   master::CheckObjectDataLocationRspPb &rsp) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            requestedObjectKeys_.clear();
            for (const auto &objectVersion : req.object_versions()) {
                requestedObjectKeys_.emplace_back(objectVersion.object_key());
            }
        }
        rsp = locationResponse_;
        return locationStatus_;
    }

    void SetResponse(const master::GIncreaseRspPb &response)
    {
        response_ = response;
    }

    void SetDecreaseResponse(const master::GDecreaseRspPb &response)
    {
        decreaseResponse_ = response;
    }

    void SetLocationResponse(const master::CheckObjectDataLocationRspPb &response)
    {
        locationResponse_ = response;
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
    master::CheckObjectDataLocationRspPb locationResponse_;
    Status status_{ Status::OK() };
    Status locationStatus_{ Status::OK() };
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

class EmptySlotRecoveryStore final : public SlotRecoveryStore {
public:
    EmptySlotRecoveryStore() = default;

    Status Init() override
    {
        return Status::OK();
    }

    Status ListIncidents(std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents) override
    {
        incidents.clear();
        return Status::OK();
    }
};

class RecordingPersistenceApi final : public PersistenceApi {
public:
    Status Init() override
    {
        return Status::OK();
    }

    Status Save(const std::string &, uint64_t, int64_t, const std::shared_ptr<std::iostream> &, uint64_t, WriteMode,
                uint32_t) override
    {
        return Status::OK();
    }

    Status Get(const std::string &, uint64_t, int64_t, std::shared_ptr<std::stringstream> &) override
    {
        return Status::OK();
    }

    Status GetWithoutVersion(const std::string &, int64_t, uint64_t, std::shared_ptr<std::stringstream> &) override
    {
        return Status::OK();
    }

    Status Del(const std::string &objectKey, uint64_t maxVerToDelete, bool deleteAllVersion, uint64_t,
               const uint64_t *const objectVersion, bool listIncompleteVersions) override
    {
        deletedObjectKey = objectKey;
        deletedMaxVersion = maxVerToDelete;
        deletedObjectVersion = objectVersion == nullptr ? 0 : *objectVersion;
        deleteAll = deleteAllVersion;
        listIncomplete = listIncompleteVersions;
        ++deleteCount;
        return deleteStatus;
    }

    Status PreloadSlot(const std::string &, uint32_t, const SlotPreloadCallback &) override
    {
        return Status::OK();
    }

    Status MergeSlot(const std::string &, uint32_t) override
    {
        return Status::OK();
    }

    Status CleanupLocalSlots() override
    {
        return Status::OK();
    }

    std::string GetL2CacheRequestSuccessRate() const override
    {
        return {};
    }

    Status deleteStatus{ Status::OK() };
    std::string deletedObjectKey;
    uint64_t deletedMaxVersion{ 0 };
    uint64_t deletedObjectVersion{ 0 };
    size_t deleteCount{ 0 };
    bool deleteAll{ false };
    bool listIncomplete{ false };
};

class TestSlotRecoveryManager final : public SlotRecoveryManager {
public:
    explicit TestSlotRecoveryManager(std::shared_ptr<SlotRecoveryStore> store)
    {
        store_ = std::move(store);
    }
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

    void AddObject(const std::string &objectKey, uint64_t version = 1, uint64_t dataSize = 1024,
                   WriteMode writeMode = WriteMode::NONE_L2_CACHE)
    {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        obj->SetDataSize(dataSize);
        obj->SetCreateTime(version);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        obj->modeInfo.SetWriteMode(writeMode);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->stateInfo.SetPrimaryCopy(true);
        DS_ASSERT_OK(objectTable_->Insert(objectKey, std::move(obj)));
    }

    std::shared_ptr<WorkerOcServiceDeleteImpl> CreateDeleteProc(const std::shared_ptr<PersistenceApi> &persistenceApi)
    {
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
            .persistenceApi = persistenceApi,
            .metadataRouteResolver = &metadataRoute_,
            .endpointPolicy = endpointPolicy_.get(),
            .exitRequested = &exitRequested_,
            .allowDirectoryLag = false,
        };
        return std::make_shared<WorkerOcServiceDeleteImpl>(param, nullptr, localAddress_, nullptr);
    }

    std::shared_ptr<WorkerOCServiceImpl> CreateWorkerOCService(const std::shared_ptr<AkSkManager> &akSkManager)
    {
        return std::make_shared<WorkerOCServiceImpl>(
            localAddress_, localAddress_, objectTable_, akSkManager, evictionManager_, nullptr, nullptr, nullptr,
            topologyRuntime_.Engine(), metadataRoute_, topologyRuntime_.Engine()->Membership(), &exitRequested_,
            topologyRuntime_.Engine()->IsRestart(), false);
    }

    WorkerWorkerOCServiceImpl CreateWorkerWorkerOCService(std::shared_ptr<AkSkManager> akSkManager = nullptr)
    {
        return WorkerWorkerOCServiceImpl(
            impl_, std::move(akSkManager), topologyRuntime_.Engine()->Membership(), [] { return true; },
            [] { return cluster::ControlBackendObservation{}; });
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

    Status Reconcile(const PushMetaToWorkerReqPb &req)
    {
        return impl_->Reconciliation(req);
    }

    uint16_t ReconciliationCount() const
    {
        return impl_->numRecon_;
    }

    static bool HasCompleteReconciliationSet(const std::set<std::string> &expected,
                                             const std::unordered_set<std::string> &completed)
    {
        return WorkerOCServiceImpl::HasCompleteReconciliationSet(expected, completed);
    }

    struct BestEffortRetryCase {
        std::string recoveredKey;
        std::string clearedKey;
        std::string unresolvedKey;
        uint64_t clearedVersion;
    };

    BestEffortRetryCase PrepareBestEffortRetryCase()
    {
        BestEffortRetryCase retryCase{
            .recoveredKey = "batch-recovered-on-retry",
            .clearedKey = "batch-cleared-after-retry",
            .unresolvedKey = "batch-unresolved-after-retry",
            .clearedVersion = 0,
        };
        AddObject(retryCase.recoveredKey);
        AddObject(retryCase.clearedKey);
        AddObject(retryCase.unresolvedKey);
        std::shared_ptr<SafeObjType> clearedEntry;
        auto status = objectTable_->Get(retryCase.clearedKey, clearedEntry);
        EXPECT_TRUE(status.IsOk());
        if (status.IsError()) {
            return retryCase;
        }
        status = clearedEntry->RLock();
        EXPECT_TRUE(status.IsOk());
        if (status.IsError()) {
            return retryCase;
        }
        retryCase.clearedVersion = clearedEntry->Get()->GetCreateTime();
        clearedEntry->RUnlock();
        return retryCase;
    }

    std::unique_ptr<MetaDataRecoveryManager> UseMetadataRecoveryManager()
    {
        auto metadataRecoveryManager = std::make_unique<MetaDataRecoveryManager>(
            localAddress_, objectTable_, MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
        dataClearImpl_->metadataRecoveryManager_ = metadataRecoveryManager.get();
        return metadataRecoveryManager;
    }

    static void ExpectBestEffortRecoverySummary(const BestEffortRetryCase &retryCase)
    {
        BINEXPECT_CALL((RecoverMetadataWithSummaryMethod)&MetaDataRecoveryManager::RecoverMetadataWithSummary,
                       (ElementsAre(retryCase.recoveredKey, retryCase.clearedKey, retryCase.unresolvedKey), ""))
            .Times(1)
            .WillOnce(Invoke([&retryCase](const std::vector<std::string> &objectKeys, const std::string &) {
                MetaDataRecoveryManager::RecoverySummary summary;
                summary.requestedCount = objectKeys.size();
                summary.recoveredCount = 1;
                summary.failedIds = { retryCase.clearedKey, retryCase.unresolvedKey };
                return summary;
            }));
    }

    static void ExpectBestEffortClearCheck(const BestEffortRetryCase &retryCase)
    {
        BINEXPECT_CALL(
            (FilterObjectsNeedClearByMasterMethod)&WorkerOcServiceClearDataFlow::FilterObjectsNeedClearByMaster,
            (_, _, _, _))
            .WillOnce(
                Invoke([&retryCase](const std::vector<std::string> &objectKeys, std::vector<std::string> &needClear,
                                    std::unordered_set<std::string> &failedIds,
                                    std::unordered_map<std::string, uint64_t> &versions) {
                    EXPECT_THAT(objectKeys, UnorderedElementsAre(retryCase.clearedKey, retryCase.unresolvedKey));
                    needClear.emplace_back(retryCase.clearedKey);
                    failedIds.emplace(retryCase.unresolvedKey);
                    versions.emplace(retryCase.clearedKey, retryCase.clearedVersion);
                }));
    }

    static void AssertBestEffortRetryCounters(const BestEffortRetryCase &retryCase,
                                              const WorkerOcServiceClearDataFlow::RetryMetadataRecoveryResult &result)
    {
        EXPECT_EQ(result.recoveredCount, 1U);
        EXPECT_EQ(result.clearedCount, 1U);
        EXPECT_EQ(result.unresolvedCount, 1U);
        EXPECT_THAT(result.unresolvedIds, UnorderedElementsAre(retryCase.unresolvedKey));
        EXPECT_TRUE(result.status.IsError());
    }

    void AssertBestEffortRetryObjects(const BestEffortRetryCase &retryCase)
    {
        std::shared_ptr<SafeObjType> entry;
        EXPECT_TRUE(objectTable_->Get(retryCase.recoveredKey, entry).IsOk());
        EXPECT_EQ(objectTable_->Get(retryCase.clearedKey, entry).GetCode(), StatusCode::K_NOT_FOUND);
        EXPECT_TRUE(objectTable_->Get(retryCase.unresolvedKey, entry).IsOk());
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

TEST_F(WorkerOcServiceImplTest, RetryFailedMetadataRecoveryRetainsObjectRecoveredOnRetry)
{
    const std::string objectKey = "recovered-on-retry";
    AddObject(objectKey);
    MetaDataRecoveryManager metadataRecoveryManager(localAddress_, objectTable_,
                                                    MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    dataClearImpl_->metadataRecoveryManager_ = &metadataRecoveryManager;

    BINEXPECT_CALL((RecoverMetadataWithSummaryMethod)&MetaDataRecoveryManager::RecoverMetadataWithSummary,
                   (ElementsAre(objectKey), ""))
        .Times(1)
        .WillOnce(Invoke([&objectKey](const std::vector<std::string> &objectKeys, std::string standbyAddr) {
            EXPECT_THAT(objectKeys, ElementsAre(objectKey));
            EXPECT_TRUE(standbyAddr.empty());
            MetaDataRecoveryManager::RecoverySummary summary;
            summary.requestedCount = objectKeys.size();
            summary.recoveredCount = objectKeys.size();
            return summary;
        }));

    auto result = dataClearImpl_->RetryFailedMetadataRecoveryAndClearUnrecoverable({ objectKey });

    EXPECT_EQ(result.recoveredCount, 1U);
    EXPECT_EQ(result.clearedCount, 0U);
    EXPECT_EQ(result.unresolvedCount, 0U);
    EXPECT_TRUE(result.status.IsOk());
    std::shared_ptr<SafeObjType> entry;
    EXPECT_TRUE(objectTable_->Get(objectKey, entry).IsOk());
}

TEST_F(WorkerOcServiceImplTest, RetryFailedMetadataRecoveryClearsObjectStillUnrecoverableAfterRetry)
{
    constexpr const char *kInjectPoint = "WorkerOcServiceClearDataFlow.BeforeClearUnrecoverableObjects";
    const std::string objectKey = "unrecoverable-after-retry";
    AddObject(objectKey);
    MetaDataRecoveryManager metadataRecoveryManager(localAddress_, objectTable_,
                                                    MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    dataClearImpl_->metadataRecoveryManager_ = &metadataRecoveryManager;
    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
    DS_ASSERT_OK(entry->RLock());
    const auto objectVersion = entry->Get()->GetCreateTime();
    entry->RUnlock();
    ASSERT_TRUE(inject::Set(kInjectPoint, "call()").IsOk());
    Raii clearInject([kInjectPoint] { (void)inject::Clear(kInjectPoint); });

    BINEXPECT_CALL((RecoverMetadataWithSummaryMethod)&MetaDataRecoveryManager::RecoverMetadataWithSummary,
                   (ElementsAre(objectKey), ""))
        .Times(1)
        .WillOnce(Invoke([&objectKey](const std::vector<std::string> &objectKeys, std::string standbyAddr) {
            EXPECT_THAT(objectKeys, ElementsAre(objectKey));
            EXPECT_TRUE(standbyAddr.empty());
            MetaDataRecoveryManager::RecoverySummary summary;
            summary.requestedCount = objectKeys.size();
            summary.failedIds = objectKeys;
            return summary;
        }));
    BINEXPECT_CALL((FilterObjectsNeedClearByMasterMethod)&WorkerOcServiceClearDataFlow::FilterObjectsNeedClearByMaster,
                   (ElementsAre(objectKey), _, _, _))
        .WillOnce(Invoke([&objectKey, objectVersion](
                             const std::vector<std::string> &, std::vector<std::string> &needClear,
                             std::unordered_set<std::string> &, std::unordered_map<std::string, uint64_t> &versions) {
            needClear.emplace_back(objectKey);
            versions.emplace(objectKey, objectVersion);
        }));

    auto result = dataClearImpl_->RetryFailedMetadataRecoveryAndClearUnrecoverable({ objectKey });

    EXPECT_EQ(result.recoveredCount, 0U);
    EXPECT_EQ(result.clearedCount, 1U);
    EXPECT_EQ(result.unresolvedCount, 0U);
    EXPECT_TRUE(result.status.IsOk());
    EXPECT_EQ(inject::GetExecuteCount(kInjectPoint), 1U);
    EXPECT_EQ(objectTable_->Get(objectKey, entry).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(WorkerOcServiceImplTest, RetryFailedMetadataRecoveryKeepsObjectWhenMasterProofIsUnavailable)
{
    const std::string objectKey = "unresolved-master-proof";
    AddObject(objectKey);
    MetaDataRecoveryManager metadataRecoveryManager(localAddress_, objectTable_,
                                                    MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    dataClearImpl_->metadataRecoveryManager_ = &metadataRecoveryManager;

    BINEXPECT_CALL((RecoverMetadataWithSummaryMethod)&MetaDataRecoveryManager::RecoverMetadataWithSummary,
                   (ElementsAre(objectKey), ""))
        .Times(1)
        .WillOnce(Invoke([&objectKey](const std::vector<std::string> &objectKeys, std::string) {
            MetaDataRecoveryManager::RecoverySummary summary;
            summary.requestedCount = objectKeys.size();
            summary.failedIds = objectKeys;
            return summary;
        }));
    BINEXPECT_CALL((FilterObjectsNeedClearByMasterMethod)&WorkerOcServiceClearDataFlow::FilterObjectsNeedClearByMaster,
                   (ElementsAre(objectKey), _, _, _))
        .WillOnce(Invoke([&objectKey](const std::vector<std::string> &, std::vector<std::string> &,
                                      std::unordered_set<std::string> &failedIds,
                                      std::unordered_map<std::string, uint64_t> &) { failedIds.emplace(objectKey); }));

    auto result = dataClearImpl_->RetryFailedMetadataRecoveryAndClearUnrecoverable({ objectKey });

    EXPECT_EQ(result.recoveredCount, 0U);
    EXPECT_EQ(result.clearedCount, 0U);
    EXPECT_EQ(result.unresolvedCount, 1U);
    EXPECT_THAT(result.unresolvedIds, UnorderedElementsAre(objectKey));
    EXPECT_TRUE(result.status.IsError());
    std::shared_ptr<SafeObjType> entry;
    EXPECT_TRUE(objectTable_->Get(objectKey, entry).IsOk());
}

TEST_F(WorkerOcServiceImplTest, RetryFailedMetadataRecoveryRejectsUnknownFailureWithoutCleanup)
{
    const std::string objectKey = "original-failed-object";
    AddObject(objectKey);
    MetaDataRecoveryManager metadataRecoveryManager(localAddress_, objectTable_,
                                                    MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    dataClearImpl_->metadataRecoveryManager_ = &metadataRecoveryManager;

    BINEXPECT_CALL((RecoverMetadataWithSummaryMethod)&MetaDataRecoveryManager::RecoverMetadataWithSummary,
                   (ElementsAre(objectKey), ""))
        .Times(1)
        .WillOnce(Invoke([](const std::vector<std::string> &objectKeys, std::string) {
            MetaDataRecoveryManager::RecoverySummary summary;
            summary.requestedCount = objectKeys.size();
            summary.failedIds.emplace_back("unknown-failed-object");
            return summary;
        }));
    BINEXPECT_CALL((FilterObjectsNeedClearByMasterMethod)&WorkerOcServiceClearDataFlow::FilterObjectsNeedClearByMaster,
                   (_, _, _, _))
        .Times(0);

    auto result = dataClearImpl_->RetryFailedMetadataRecoveryAndClearUnrecoverable({ objectKey });

    EXPECT_EQ(result.clearedCount, 0U);
    EXPECT_EQ(result.unresolvedCount, 1U);
    EXPECT_THAT(result.unresolvedIds, UnorderedElementsAre(objectKey));
    EXPECT_TRUE(result.status.IsError());
    std::shared_ptr<SafeObjType> entry;
    EXPECT_TRUE(objectTable_->Get(objectKey, entry).IsOk());
}

TEST_F(WorkerOcServiceImplTest, RetryFailedMetadataRecoveryRejectsInconsistentCountsWithoutCleanup)
{
    const std::string objectKey = "inconsistently-counted-object";
    AddObject(objectKey);
    MetaDataRecoveryManager metadataRecoveryManager(localAddress_, objectTable_,
                                                    MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    dataClearImpl_->metadataRecoveryManager_ = &metadataRecoveryManager;

    BINEXPECT_CALL((RecoverMetadataWithSummaryMethod)&MetaDataRecoveryManager::RecoverMetadataWithSummary,
                   (ElementsAre(objectKey), ""))
        .Times(1)
        .WillOnce(Invoke([&objectKey](const std::vector<std::string> &objectKeys, std::string) {
            MetaDataRecoveryManager::RecoverySummary summary;
            summary.requestedCount = objectKeys.size();
            summary.recoveredCount = 1;
            summary.failedIds.emplace_back(objectKey);
            return summary;
        }));
    BINEXPECT_CALL((FilterObjectsNeedClearByMasterMethod)&WorkerOcServiceClearDataFlow::FilterObjectsNeedClearByMaster,
                   (_, _, _, _))
        .Times(0);

    auto result = dataClearImpl_->RetryFailedMetadataRecoveryAndClearUnrecoverable({ objectKey });

    EXPECT_EQ(result.clearedCount, 0U);
    EXPECT_EQ(result.unresolvedCount, 1U);
    EXPECT_THAT(result.unresolvedIds, UnorderedElementsAre(objectKey));
    EXPECT_TRUE(result.status.IsError());
    std::shared_ptr<SafeObjType> entry;
    EXPECT_TRUE(objectTable_->Get(objectKey, entry).IsOk());
}

TEST_F(WorkerOcServiceImplTest, RetryFailedMetadataRecoveryBestEffortDoesNotBlockRecoveredEntries)
{
    auto retryCase = PrepareBestEffortRetryCase();
    auto metadataRecoveryManager = UseMetadataRecoveryManager();
    ExpectBestEffortRecoverySummary(retryCase);
    ExpectBestEffortClearCheck(retryCase);

    auto result = dataClearImpl_->RetryFailedMetadataRecoveryAndClearUnrecoverable(
        { retryCase.recoveredKey, retryCase.clearedKey, retryCase.unresolvedKey });

    AssertBestEffortRetryCounters(retryCase, result);
    AssertBestEffortRetryObjects(retryCase);
}

TEST_F(WorkerOcServiceImplTest, CheckObjectDataLocationRejectsIncompleteOrForeignPartition)
{
    const std::string objectKey = "requested-object";
    AddObject(objectKey);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);

    auto verifyRejectedResponse = [this, &api, &objectKey](const master::CheckObjectDataLocationRspPb &response) {
        api->SetLocationResponse(response);
        std::vector<std::string> needClearObjectKeys;
        std::unordered_set<std::string> failedIds;
        std::unordered_map<std::string, uint64_t> queriedVersions;

        dataClearImpl_->CheckNeedClearObjectsByMasterInBatches(api, { objectKey }, needClearObjectKeys, failedIds,
                                                               queriedVersions);

        EXPECT_TRUE(needClearObjectKeys.empty());
        EXPECT_THAT(failedIds, UnorderedElementsAre(objectKey));
    };

    master::CheckObjectDataLocationRspPb incompleteResponse;
    verifyRejectedResponse(incompleteResponse);

    master::CheckObjectDataLocationRspPb foreignResponse;
    foreignResponse.add_no_need_clear_object_keys(objectKey);
    foreignResponse.add_need_clear_object_keys("foreign-object");
    verifyRejectedResponse(foreignResponse);
}

TEST_F(WorkerOcServiceImplTest, AuthoritativeClearDoesNotDeleteNewerSameKeyVersion)
{
    const std::string objectKey = "newer-same-key-version";
    AddObject(objectKey);
    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
    DS_ASSERT_OK(entry->RLock());
    const auto currentVersion = entry->Get()->GetCreateTime();
    entry->RUnlock();
    std::unordered_map<std::string, uint64_t> queriedVersions{ { objectKey, currentVersion - 1 } };

    auto result = dataClearImpl_->ClearObjectsWithSummary({ objectKey }, false, queriedVersions);

    EXPECT_EQ(result.clearedCount, 0U);
    EXPECT_EQ(result.unresolvedCount, 1U);
    EXPECT_THAT(result.unresolvedIds, UnorderedElementsAre(objectKey));
    EXPECT_TRUE(objectTable_->Get(objectKey, entry).IsOk());
}

TEST_F(WorkerOcServiceImplTest, AuthoritativeClearDeletesExactL2VersionBeforeRemovingLocalObject)
{
    metrics::ResetKvMetricsForTest();
    Raii restoreMetrics([] {
        metrics::ResetKvMetricsForTest();
        (void)metrics::InitKvMetrics();
    });
    ASSERT_TRUE(metrics::InitKvMetrics().IsOk());
    const std::string oldL2CacheType = FLAGS_l2_cache_type;
    FLAGS_l2_cache_type = "distributed_disk";
    Raii restoreL2CacheType([oldL2CacheType] { FLAGS_l2_cache_type = oldL2CacheType; });
    constexpr uint64_t objectVersion = 7;
    const std::string objectKey = "authoritative-l2-orphan";
    auto persistenceApi = std::make_shared<RecordingPersistenceApi>();
    auto deleteProc = CreateDeleteProc(persistenceApi);
    WorkerOcServiceClearDataFlow clearDataFlow(objectTable_, globalRefTable_, nullptr, gRefProc_, deleteProc, nullptr,
                                               metadataRoute_, *endpointPolicy_, localAddress_.ToString());
    AddObject(objectKey, objectVersion, 1024, WriteMode::WRITE_THROUGH_L2_CACHE);
    std::unordered_map<std::string, uint64_t> queriedVersions{ { objectKey, objectVersion } };

    auto result = clearDataFlow.ClearObjectsWithSummary({ objectKey }, false, queriedVersions);

    EXPECT_EQ(result.clearedCount, 1U);
    EXPECT_EQ(result.unresolvedCount, 0U);
    EXPECT_EQ(persistenceApi->deleteCount, 1U);
    EXPECT_EQ(persistenceApi->deletedObjectKey, objectKey);
    EXPECT_EQ(persistenceApi->deletedMaxVersion, objectVersion);
    EXPECT_EQ(persistenceApi->deletedObjectVersion, objectVersion);
    EXPECT_FALSE(persistenceApi->deleteAll);
    EXPECT_TRUE(persistenceApi->listIncomplete);
    std::string summary;
    for (const auto &part : metrics::DumpSummariesForTest()) {
        summary += part;
    }
    EXPECT_NE(summary.find("\"name\":\"worker_cleanup_batch_latency\""), std::string::npos);
    std::shared_ptr<SafeObjType> entry;
    EXPECT_EQ(objectTable_->Get(objectKey, entry).GetCode(), K_NOT_FOUND);
}

TEST_F(WorkerOcServiceImplTest, AuthoritativeObjectPersistenceKeepsExistingDeleteAllContract)
{
    const std::string oldL2CacheType = FLAGS_l2_cache_type;
    FLAGS_l2_cache_type = "sfs";
    Raii restoreL2CacheType([oldL2CacheType] { FLAGS_l2_cache_type = oldL2CacheType; });
    constexpr uint64_t objectVersion = 10;
    const std::string objectKey = "authoritative-object-persistence-orphan";
    auto persistenceApi = std::make_shared<RecordingPersistenceApi>();
    auto deleteProc = CreateDeleteProc(persistenceApi);
    WorkerOcServiceClearDataFlow clearDataFlow(objectTable_, globalRefTable_, nullptr, gRefProc_, deleteProc, nullptr,
                                               metadataRoute_, *endpointPolicy_, localAddress_.ToString());
    AddObject(objectKey, objectVersion, 1024, WriteMode::WRITE_THROUGH_L2_CACHE);
    std::unordered_map<std::string, uint64_t> queriedVersions{ { objectKey, objectVersion } };

    auto result = clearDataFlow.ClearObjectsWithSummary({ objectKey }, false, queriedVersions);

    EXPECT_EQ(result.clearedCount, 1U);
    EXPECT_EQ(result.unresolvedCount, 0U);
    EXPECT_EQ(persistenceApi->deleteCount, 1U);
    EXPECT_TRUE(persistenceApi->deleteAll);
}

TEST_F(WorkerOcServiceImplTest, AsyncL2CancellationWaitsForInFlightObjectBeforeAuthoritativeDelete)
{
    BlockingList queue;
    const std::string objectKey = "in-flight-write-back-orphan";
    auto element = std::make_shared<Element>();
    element->key = objectKey;
    DS_ASSERT_OK(queue.Offer(element));
    std::shared_ptr<Element> active;
    DS_ASSERT_OK(queue.Poll(active, 0, true));
    ASSERT_TRUE(queue.BeginPersistence(objectKey));
    std::atomic<bool> cancellationFinished{ false };

    std::thread cancellation([&] {
        EXPECT_TRUE(queue.CancelAndWait(objectKey, 1'000).IsOk());
        cancellationFinished = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(cancellationFinished.load());
    EXPECT_TRUE(queue.IsCancelled(objectKey));

    queue.FinishPersistence(objectKey);
    cancellation.join();
    queue.Finish(objectKey);
    EXPECT_TRUE(cancellationFinished.load());
}

TEST_F(WorkerOcServiceImplTest, AsyncL2CancellationRemovesQueuedObjectBeforeAuthoritativeDelete)
{
    BlockingList queue;
    const std::string objectKey = "queued-write-back-orphan";
    auto element = std::make_shared<Element>();
    element->key = objectKey;
    DS_ASSERT_OK(queue.Offer(element));

    DS_ASSERT_OK(queue.CancelAndWait(objectKey, 10));

    std::shared_ptr<Element> active;
    EXPECT_EQ(queue.Poll(active, 0).GetCode(), K_TRY_AGAIN);
}

TEST_F(WorkerOcServiceImplTest, QueueCapacityEvictionDoesNotReplaceActivePersistenceFence)
{
    BlockingList queue(1);
    const std::string activeKey = "active-write-back";
    auto activeElement = std::make_shared<Element>();
    activeElement->key = activeKey;
    DS_ASSERT_OK(queue.Offer(activeElement));
    std::shared_ptr<Element> active;
    DS_ASSERT_OK(queue.Poll(active, 0, true));
    ASSERT_TRUE(queue.BeginPersistence(activeKey));

    auto queuedElement = std::make_shared<Element>();
    queuedElement->key = "capacity-evicted";
    DS_ASSERT_OK(queue.Offer(queuedElement));
    auto replacement = std::make_shared<Element>();
    replacement->key = "capacity-replacement";
    DS_ASSERT_OK(queue.EnsureOffer(replacement));

    EXPECT_EQ(queue.CancelAndWait(activeKey, 1).GetCode(), K_TRY_AGAIN);
    queue.FinishPersistence(activeKey);
    queue.Finish(activeKey);
}

TEST_F(WorkerOcServiceImplTest, AsyncL2CancellationTimeoutKeepsInFlightObjectCancelled)
{
    BlockingList queue;
    const std::string objectKey = "timed-out-write-back-orphan";
    auto element = std::make_shared<Element>();
    element->key = objectKey;
    DS_ASSERT_OK(queue.Offer(element));
    std::shared_ptr<Element> active;
    DS_ASSERT_OK(queue.Poll(active, 0, true));
    ASSERT_TRUE(queue.BeginPersistence(objectKey));

    EXPECT_EQ(queue.CancelAndWait(objectKey, 1).GetCode(), K_TRY_AGAIN);
    EXPECT_TRUE(queue.IsCancelled(objectKey));
    queue.FinishPersistence(objectKey);
    queue.Finish(objectKey);
}

TEST_F(WorkerOcServiceImplTest, AuthoritativeClearRetainsLocalObjectWhenExactL2DeleteFails)
{
    const std::string oldL2CacheType = FLAGS_l2_cache_type;
    FLAGS_l2_cache_type = "distributed_disk";
    Raii restoreL2CacheType([oldL2CacheType] { FLAGS_l2_cache_type = oldL2CacheType; });
    constexpr uint64_t objectVersion = 8;
    const std::string objectKey = "unresolved-l2-orphan";
    auto persistenceApi = std::make_shared<RecordingPersistenceApi>();
    persistenceApi->deleteStatus = Status(K_RUNTIME_ERROR, "L2 delete failed");
    auto deleteProc = CreateDeleteProc(persistenceApi);
    WorkerOcServiceClearDataFlow clearDataFlow(objectTable_, globalRefTable_, nullptr, gRefProc_, deleteProc, nullptr,
                                               metadataRoute_, *endpointPolicy_, localAddress_.ToString());
    AddObject(objectKey, objectVersion, 1024, WriteMode::WRITE_THROUGH_L2_CACHE);
    std::unordered_map<std::string, uint64_t> queriedVersions{ { objectKey, objectVersion } };

    auto result = clearDataFlow.ClearObjectsWithSummary({ objectKey }, false, queriedVersions);

    EXPECT_EQ(result.clearedCount, 0U);
    EXPECT_EQ(result.unresolvedCount, 1U);
    EXPECT_THAT(result.unresolvedIds, UnorderedElementsAre(objectKey));
    EXPECT_EQ(persistenceApi->deleteCount, 1U);
    std::shared_ptr<SafeObjType> entry;
    EXPECT_TRUE(objectTable_->Get(objectKey, entry).IsOk());
}

TEST_F(WorkerOcServiceImplTest, OrdinaryClearKeepsExistingL2DeletionFlowUnchanged)
{
    const std::string oldL2CacheType = FLAGS_l2_cache_type;
    FLAGS_l2_cache_type = "distributed_disk";
    Raii restoreL2CacheType([oldL2CacheType] { FLAGS_l2_cache_type = oldL2CacheType; });
    constexpr uint64_t objectVersion = 9;
    const std::string objectKey = "ordinary-l2-clear";
    auto persistenceApi = std::make_shared<RecordingPersistenceApi>();
    auto deleteProc = CreateDeleteProc(persistenceApi);
    AddObject(objectKey, objectVersion, 1024, WriteMode::WRITE_THROUGH_L2_CACHE);
    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->GetAndLock(objectKey, entry));
    Raii unlock([&entry] { entry->WUnlock(); });
    ObjectKV objectKV(objectKey, *entry);

    DS_ASSERT_OK(deleteProc->ClearObject(objectKV));

    EXPECT_EQ(persistenceApi->deleteCount, 0U);
    EXPECT_EQ(objectTable_->Get(objectKey, entry).GetCode(), K_NOT_FOUND);
}

TEST_F(WorkerOcServiceImplTest, AuthoritativeClearFailureRemainsUnresolved)
{
    constexpr const char *kClearInjectPoint = "worker.clear_object_failure";
    const std::string objectKey = "authoritative-clear-failure";
    AddObject(objectKey);
    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
    DS_ASSERT_OK(entry->RLock());
    const auto currentVersion = entry->Get()->GetCreateTime();
    entry->RUnlock();
    std::unordered_map<std::string, uint64_t> queriedVersions{ { objectKey, currentVersion } };
    ASSERT_TRUE(inject::Set(kClearInjectPoint, "return(K_RUNTIME_ERROR)").IsOk());
    Raii clearInject([kClearInjectPoint] { (void)inject::Clear(kClearInjectPoint); });

    auto result = dataClearImpl_->ClearObjectsWithSummary({ objectKey }, false, queriedVersions);

    EXPECT_EQ(result.clearedCount, 0U);
    EXPECT_EQ(result.unresolvedCount, 1U);
    EXPECT_THAT(result.unresolvedIds, UnorderedElementsAre(objectKey));
    EXPECT_TRUE(objectTable_->Get(objectKey, entry).IsOk());
}

TEST_F(WorkerOcServiceImplTest, RestartRecoveryRetriesInitialFailureAndMarksMetadataEvidenceReadyWhenResolved)
{
    const HostPort restartedWorker("127.0.0.1", 18482);
    const std::string objectKey = "restart-retry-resolved";
    placement_.SetOwner(objectKey, restartedWorker);
    AddObject(objectKey);
    impl_->metadataRecoveryManager_ = std::make_unique<MetaDataRecoveryManager>(
        localAddress_, objectTable_, MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    impl_->clearDataFlow_ = std::make_unique<WorkerOcServiceClearDataFlow>(
        objectTable_, globalRefTable_, nullptr, gRefProc_, deleteProc_, impl_->metadataRecoveryManager_.get(),
        metadataRoute_, *endpointPolicy_, localAddress_.ToString());

    BINEXPECT_CALL((RecoverMetadataOfDataMethod)&WorkerOCServiceImpl::RecoverMetadataOfData, (_, _, _))
        .WillOnce(Invoke(
            [&objectKey](const std::vector<std::string> &objectKeys, std::vector<std::string> &failedIds, std::string) {
                EXPECT_THAT(objectKeys, ElementsAre(objectKey));
                failedIds = objectKeys;
                return Status(K_RUNTIME_ERROR, "initial recovery failed");
            }));
    BINEXPECT_CALL((RetryFailedMetadataRecoveryMethod)&WorkerOcServiceClearDataFlow::
                       RetryFailedMetadataRecoveryAndClearUnrecoverable,
                   (ElementsAre(objectKey)))
        .WillOnce(Invoke([](const std::vector<std::string> &) {
            WorkerOcServiceClearDataFlow::RetryMetadataRecoveryResult result;
            result.recoveredCount = 1;
            return result;
        }));

    auto rc = impl_->RecoverMetadataOfRestartedWorker(restartedWorker.ToString());

    EXPECT_TRUE(rc.IsOk());
    auto report = impl_->GetLastMetadataRecoveryEvidenceReport();
    EXPECT_TRUE(report.evidence.metadataReady);
    EXPECT_NE(report.detail.find("metadata_recovered=1/1"), std::string::npos);
    EXPECT_NE(report.detail.find("unresolved=0"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, ReconciliationNotificationWithoutMasterSourceDoesNotAdvanceProgress)
{
    const bool oldEnableReconciliation = FLAGS_enable_reconciliation;
    FLAGS_enable_reconciliation = true;
    Raii restoreFlag([oldEnableReconciliation] { FLAGS_enable_reconciliation = oldEnableReconciliation; });
    PushMetaToWorkerReqPb req;
    req.set_event_timestamp(1);
    req.set_is_restart(false);
    req.add_gref_object_keys("notification_without_master_source");

    auto rc = Reconcile(req);

    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_EQ(ReconciliationCount(), 0);

    req.set_event_timestamp(2);
    req.set_source_address("invalid-master-address");
    rc = Reconcile(req);

    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_EQ(ReconciliationCount(), 0);
}

TEST_F(WorkerOcServiceImplTest, ReconciliationRequiresEveryCurrentMetadataMaster)
{
    const std::set<std::string> expected{ "127.0.0.1:900", "127.0.0.1:901" };

    EXPECT_FALSE(HasCompleteReconciliationSet(expected, { "127.0.0.1:900", "127.0.0.1:902" }));
    EXPECT_TRUE(HasCompleteReconciliationSet(expected, { "127.0.0.1:900", "127.0.0.1:901" }));
    EXPECT_TRUE(HasCompleteReconciliationSet(expected, { "127.0.0.1:900", "127.0.0.1:901", "127.0.0.1:902" }));
}

TEST_F(WorkerOcServiceImplTest, RestartReconciliationMarksMetadataEvidenceReadyWhenComplete)
{
    worker::WorkerRuntimeFacade runtime;
    ASSERT_TRUE(MarkRuntimeRunning(runtime));
    impl_->SetRuntimeFacade(&runtime);
    bool handlerCalled = false;
    impl_->RegisterRecoveryEvidenceReadyHandler([&handlerCalled] { handlerCalled = true; });

    impl_->MarkRestartReconciliationEvidenceReady("restart_reconciliation metadata owners completed");

    const auto report = impl_->BuildObjectCacheRecoveryEvidenceReport();
    EXPECT_TRUE(handlerCalled);
    EXPECT_TRUE(report.evidence.metadataReady);
    EXPECT_TRUE(report.evidence.ownershipReady);
    EXPECT_NE(report.detail.find("restart_reconciliation"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, RuntimeAdmissionGuardRejectsWhenRuntimeIsNotRunning)
{
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(worker::WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local isolation");
    impl_->SetRuntimeFacade(&runtime);
    DS_ASSERT_OK(SetHealthProbe());
    SetTopologyServingAdmission(true);
    Raii reset([]() {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });

    std::optional<worker::WorkerRuntimeStateReadGuard> admissionGuard;
    auto rc =
        runtime.AcquireAdmissionGuard(worker::WorkerAdmissionKind::NORMAL_WRITE, "ObjectCacheService", admissionGuard);

    ASSERT_NE(rc.GetCode(), StatusCode::K_OK);
    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("LOCAL_ISOLATED"), std::string::npos);
    EXPECT_FALSE(admissionGuard.has_value());

    ReadLock noRecon;
    DS_ASSERT_OK(impl_->ValidateWorkerState(noRecon, 100));
}

TEST_F(WorkerOcServiceImplTest, ObjectCacheOutOfMemoryMarksRuntimeState)
{
    worker::WorkerRuntimeFacade runtime;
    ASSERT_TRUE(MarkRuntimeRunning(runtime));
    impl_->SetRuntimeFacade(&runtime);

    impl_->MarkOutOfMemoryIfNeeded(Status(StatusCode::K_OUT_OF_MEMORY, "allocation failed"), "Create");

    auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, worker::WorkerServiceMode::OUT_OF_MEMORY);
    EXPECT_EQ(snapshot.reason, worker::WorkerIsolationReason::OUT_OF_MEMORY);
    EXPECT_NE(snapshot.detail.find("Create"), std::string::npos);
    EXPECT_NE(snapshot.detail.find("allocation failed"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, DiskCreateOutOfMemoryRecordsDiskRecoveryRequirement)
{
    worker::WorkerRuntimeFacade runtime;
    ASSERT_TRUE(MarkRuntimeRunning(runtime));
    impl_->SetRuntimeFacade(&runtime);
    DS_ASSERT_OK(SetHealthProbe());
    SetTopologyServingAdmission(true);
    Raii reset([]() {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    BINEXPECT_CALL(&WorkerOcServiceCreateImpl::Create, (_, _))
        .Times(1)
        .WillOnce(Return(Status(StatusCode::K_OUT_OF_MEMORY, "disk full")));
    CreateReqPb req;
    req.set_cache_type(static_cast<int32_t>(memory::CacheType::DISK));
    CreateRspPb rsp;

    EXPECT_EQ(impl_->Create(req, rsp).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    auto recoverySnapshot = impl_->recoveryState_->GetResourceRecoverySnapshot();
    EXPECT_TRUE(recoverySnapshot.diskRequired);
    EXPECT_FALSE(recoverySnapshot.memoryRequired);
}

TEST_F(WorkerOcServiceImplTest, RestartReconciliationMarksRuntimeRecoveringBeforeFanout)
{
    auto restartService =
        std::make_shared<WorkerOCServiceImpl>(localAddress_, localAddress_, objectTable_, nullptr, evictionManager_,
                                              nullptr, nullptr, nullptr, topologyRuntime_.Engine(), metadataRoute_,
                                              topologyRuntime_.Engine()->Membership(), &exitRequested_, true, true);
    restartService->InitServiceImpl();
    worker::WorkerRuntimeFacade runtime;
    ASSERT_TRUE(MarkRuntimeRunning(runtime));

    restartService->SetRuntimeFacade(&runtime);

    const auto snapshotAfterAttach = runtime.GetSnapshot();
    EXPECT_EQ(snapshotAfterAttach.mode, worker::WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshotAfterAttach.recoveryPhase, worker::WorkerRecoveryPhase::METADATA);
    EXPECT_FALSE(restartService->BuildObjectCacheRecoveryEvidenceReport().evidence.metadataReady);
    EXPECT_FALSE(restartService->BuildObjectCacheRecoveryEvidenceReport().evidence.ownershipReady);

    ASSERT_TRUE(MarkRuntimeRunning(runtime));
    restartService->SetRuntimeFacade(&runtime);

    const auto rc = restartService->WhetherNonRestart();

    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, worker::WorkerServiceMode::RECOVERING);
    EXPECT_EQ(snapshot.recoveryPhase, worker::WorkerRecoveryPhase::METADATA);
}

TEST_F(WorkerOcServiceImplTest, MigrationOutOfMemoryHandlerUsesWorkerRuntimeState)
{
    worker::WorkerRuntimeFacade runtime;
    ASSERT_TRUE(MarkRuntimeRunning(runtime));
    impl_->SetRuntimeFacade(&runtime);
    BINEXPECT_CALL(&WorkerOcServiceMigrateImpl::CheckResource, (_, _))
        .Times(1)
        .WillOnce(Return(Status(StatusCode::K_OUT_OF_MEMORY, "migration allocation failed")));
    MigrateDataReqPb req;
    req.set_type(MigrateType::SPILL);
    req.add_objects()->set_object_key("object-a");
    MigrateDataRspPb rsp;

    EXPECT_EQ(impl_->gMigrateProc_->MigrateData(req, rsp, {}).GetCode(), StatusCode::K_OUT_OF_MEMORY);

    const auto snapshot = runtime.GetSnapshot();
    EXPECT_EQ(snapshot.mode, worker::WorkerServiceMode::OUT_OF_MEMORY);
    EXPECT_NE(snapshot.detail.find("MigrateData.CheckResource"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, HealthCheckRemainsAvailableWhenOutOfMemory)
{
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkOutOfMemory("oom");
    impl_->SetRuntimeFacade(&runtime);
    DS_ASSERT_OK(SetHealthProbe());
    SetTopologyServingAdmission(true);
    Raii reset([]() {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });

    HealthCheckRequestPb req;
    HealthCheckReplyPb resp;
    auto rc = impl_->HealthCheck(req, resp);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_OK);
    EXPECT_EQ(resp.worker_service_mode(), "OUT_OF_MEMORY");
    EXPECT_EQ(resp.worker_service_reason(), "OUT_OF_MEMORY");
    EXPECT_EQ(resp.worker_recovery_phase(), "RESOURCE");
    EXPECT_EQ(resp.recovery_evidence_mask(), 0U);
}

TEST_F(WorkerOcServiceImplTest, ReferenceCleanupRemainsAvailableDuringResourceRecovery)
{
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkOutOfMemory("oom");
    impl_->SetRuntimeFacade(&runtime);
    ReleaseGRefsReqPb releaseReq;
    ReleaseGRefsRspPb releaseRsp;
    GDecreaseReqPb decreaseReq;
    GDecreaseRspPb decreaseRsp;

    EXPECT_TRUE(impl_->DecreaseMemoryRef(ClientKey::Intern("client-id"), {}).IsOk());
    EXPECT_NE(impl_->ReleaseGRefs(releaseReq, releaseRsp).GetCode(), StatusCode::K_OUT_OF_MEMORY);
    EXPECT_NE(impl_->GDecreaseRef(decreaseReq, decreaseRsp).GetCode(), StatusCode::K_OUT_OF_MEMORY);

    runtime.MarkRecovering(worker::WorkerIsolationReason::OUT_OF_MEMORY, "resource recovery");
    EXPECT_TRUE(impl_->DecreaseMemoryRef(ClientKey::Intern("client-id"), {}).IsOk());
    EXPECT_NE(impl_->ReleaseGRefs(releaseReq, releaseRsp).GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(impl_->GDecreaseRef(decreaseReq, decreaseRsp).GetCode(), StatusCode::K_NOT_READY);
}

TEST_F(WorkerOcServiceImplTest, LocalExistRemainsAvailableWhenOutOfMemory)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    auto service = CreateWorkerOCService(akSkManager);
    service->InitServiceImpl();
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkOutOfMemory("oom");
    service->SetRuntimeFacade(&runtime);
    DS_ASSERT_OK(SetHealthProbe());
    SetTopologyServingAdmission(false);
    Raii reset([]() {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });

    auto clientId = ClientKey::Intern("exist-client-id");
    DS_ASSERT_OK(worker::ClientManager::Instance().AddClient(clientId, -1));
    Raii removeClient([&clientId]() { worker::ClientManager::Instance().RemoveClient(clientId); });

    ExistReqPb req;
    req.set_client_id("exist-client-id");
    req.set_is_local(true);
    req.add_object_keys("missing-object");
    ExistRspPb rsp;
    auto rc = service->Exist(req, rsp);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_OK);
    ASSERT_EQ(rsp.exists_size(), 1);
    EXPECT_FALSE(rsp.exists(0));
}

TEST_F(WorkerOcServiceImplTest, GetObjMetaInfoRemainsAvailableWhenOutOfMemory)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    auto service = CreateWorkerOCService(akSkManager);
    service->InitServiceImpl();
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkOutOfMemory("oom");
    service->SetRuntimeFacade(&runtime);
    DS_ASSERT_OK(SetHealthProbe());
    SetTopologyServingAdmission(false);
    Raii reset([]() {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });

    GetObjMetaInfoReqPb req;
    GetObjMetaInfoRspPb rsp;
    auto rc = service->GetObjMetaInfo(req, rsp);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_OK);
    EXPECT_EQ(rsp.objs_meta_info_size(), 0);
}

TEST_F(WorkerOcServiceImplTest, ClearObjectRemainsAvailableWhenOutOfMemory)
{
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkOutOfMemory("oom");
    impl_->SetRuntimeFacade(&runtime);
    impl_->clearDataFlow_ = std::make_unique<WorkerOcServiceClearDataFlow>(
        objectTable_, globalRefTable_, nullptr, gRefProc_, deleteProc_, nullptr, metadataRoute_, *endpointPolicy_,
        localAddress_.ToString());

    ClearDataReqPb req;
    auto rc = impl_->ClearObject(req);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_OK);
}

TEST_F(WorkerOcServiceImplTest, EvictionManagerRemainsUsableWhenOutOfMemory)
{
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkOutOfMemory("oom");
    impl_->SetRuntimeFacade(&runtime);
    auto akSkManager = std::make_shared<AkSkManager>();
    DS_ASSERT_OK(evictionManager_->Init(globalRefTable_, akSkManager));

    constexpr uint64_t dataSize = 1024;
    const std::string objectKey = "oom_eviction_candidate";
    AddObject(objectKey, 1, dataSize);

    evictionManager_->Add(objectKey);
    std::vector<EvictionList::Node> objects;
    EvictionList::Node oldest;
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(objects, oldest));
    ASSERT_EQ(objects.size(), size_t(1));
    EXPECT_EQ(oldest.objectKey, objectKey);

    evictionManager_->Erase(objectKey);
    objects.clear();
    DS_ASSERT_OK(evictionManager_->GetAllObjectsInfo(objects, oldest));
    EXPECT_TRUE(objects.empty());
}

TEST_F(WorkerOcServiceImplTest, WorkerWorkerMigrationTargetRejectsWhenRuntimeIsNotRunning)
{
    struct Case {
        worker::WorkerServiceMode mode;
        StatusCode expectedCode;
    };
    const std::vector<Case> cases = {
        { worker::WorkerServiceMode::LOCAL_ISOLATED, StatusCode::K_NOT_READY },
        { worker::WorkerServiceMode::RECOVERING, StatusCode::K_NOT_READY },
        { worker::WorkerServiceMode::OUT_OF_MEMORY, StatusCode::K_OUT_OF_MEMORY },
    };
    auto workerWorkerSvc = CreateWorkerWorkerOCService();
    for (const auto &item : cases) {
        worker::WorkerRuntimeFacade runtime;
        switch (item.mode) {
            case worker::WorkerServiceMode::LOCAL_ISOLATED:
                runtime.MarkLocalIsolated(worker::WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                                          "local isolation");
                break;
            case worker::WorkerServiceMode::RECOVERING:
                runtime.MarkRecovering(worker::WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "recovering");
                break;
            case worker::WorkerServiceMode::OUT_OF_MEMORY:
                runtime.MarkOutOfMemory("oom");
                break;
            default:
                break;
        }
        workerWorkerSvc.SetRuntimeFacade(&runtime);

        MigrateDataReqPb migrateReq;
        MigrateDataRspPb migrateRsp;
        auto migrateRc = workerWorkerSvc.MigrateData(migrateReq, migrateRsp, {});
        EXPECT_EQ(migrateRc.GetCode(), item.expectedCode) << worker::ToString(item.mode);
        EXPECT_NE(migrateRc.GetMsg().find("MIGRATION_TARGET"), std::string::npos);
        EXPECT_NE(migrateRc.GetMsg().find(worker::ToString(item.mode)), std::string::npos);

        MigrateDataDirectReqPb directReq;
        MigrateDataDirectRspPb directRsp;
        auto directRc = workerWorkerSvc.MigrateDataDirect(directReq, directRsp);
        EXPECT_EQ(directRc.GetCode(), item.expectedCode) << worker::ToString(item.mode);
        EXPECT_NE(directRc.GetMsg().find("MIGRATION_TARGET"), std::string::npos);
        EXPECT_NE(directRc.GetMsg().find(worker::ToString(item.mode)), std::string::npos);
    }
}

TEST_F(WorkerOcServiceImplTest, WorkerWorkerMigrationTargetRejectsDirectRequestsWithFailedObjects)
{
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkOutOfMemory("oom");
    auto workerWorkerSvc = CreateWorkerWorkerOCService();
    workerWorkerSvc.SetRuntimeFacade(&runtime);

    MigrateDataReqPb migrateReq;
    migrateReq.add_objects()->set_object_key("migrate-object");
    MigrateDataRspPb migrateRsp;
    auto migrateRc = workerWorkerSvc.MigrateData(migrateReq, migrateRsp, {});
    EXPECT_EQ(migrateRc.GetCode(), StatusCode::K_OUT_OF_MEMORY);
    EXPECT_THAT(migrateRsp.fail_ids(), ElementsAre("migrate-object"));

    MigrateDataDirectReqPb directReq;
    directReq.add_objects()->set_object_key("direct-object");
    MigrateDataDirectRspPb directRsp;
    auto directRc = workerWorkerSvc.MigrateDataDirect(directReq, directRsp);
    EXPECT_EQ(directRc.GetCode(), StatusCode::K_OUT_OF_MEMORY);
    EXPECT_THAT(directRsp.failed_object_keys(), ElementsAre("direct-object"));
}

TEST_F(WorkerOcServiceImplTest, WorkerWorkerClassifiesMigrationNotificationAndDiagnostics)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    constexpr const char *clientCred = "test-client-credential";
    constexpr const char *serverCred = "test-server-credential";
    akSkManager->SetClientAkSk(clientCred, serverCred);
    akSkManager->SetServerAkSk(AkSkType::SYSTEM, clientCred, serverCred);
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkStopping(worker::WorkerIsolationReason::PROCESS_STOPPING, "stopping");
    auto workerWorkerSvc = CreateWorkerWorkerOCService(akSkManager);
    workerWorkerSvc.SetRuntimeFacade(&runtime);

    NotifyRemoteGetReqPb migrationReq;
    NotifyRemoteGetRspPb migrationRsp;
    const auto migrationRc = workerWorkerSvc.NotifyRemoteGet(migrationReq, migrationRsp);
    EXPECT_EQ(migrationRc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(migrationRc.GetMsg().find("MIGRATION_TARGET"), std::string::npos);
    EXPECT_NE(migrationRc.GetMsg().find("NotifyRemoteGet"), std::string::npos);

    CheckCoordinatorStateReqPb diagnosticReq;
    CheckCoordinatorStateRspPb diagnosticRsp;
    const auto unsignedDiagnosticRc = workerWorkerSvc.CheckCoordinatorState(diagnosticReq, diagnosticRsp);
    EXPECT_EQ(unsignedDiagnosticRc.GetMsg().find("mode=STOPPING"), std::string::npos);
    DS_ASSERT_OK(akSkManager->GenerateSignature(diagnosticReq));
    EXPECT_TRUE(workerWorkerSvc.CheckCoordinatorState(diagnosticReq, diagnosticRsp).IsOk());

    GetClusterStateReqPb clusterReq;
    GetClusterStateRspPb clusterRsp;
    DS_ASSERT_OK(akSkManager->GenerateSignature(clusterReq));
    const auto clusterRc = workerWorkerSvc.GetClusterState(clusterReq, clusterRsp);
    EXPECT_EQ(clusterRc.GetMsg().find("mode=STOPPING"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, MasterWorkerClassifiesCleanupAndRecoveryRpcs)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    constexpr const char *clientCred = "test-client-credential";
    constexpr const char *serverCred = "test-server-credential";
    akSkManager->SetClientAkSk(clientCred, serverCred);
    akSkManager->SetServerAkSk(AkSkType::SYSTEM, clientCred, serverCred);
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkStopping(worker::WorkerIsolationReason::PROCESS_STOPPING, "stopping");
    MasterWorkerOCServiceImpl masterWorkerSvc(impl_, akSkManager);
    masterWorkerSvc.SetRuntimeFacade(&runtime);
    impl_->clearDataFlow_ = std::make_unique<WorkerOcServiceClearDataFlow>(
        objectTable_, globalRefTable_, nullptr, gRefProc_, deleteProc_, nullptr, metadataRoute_, *endpointPolicy_,
        localAddress_.ToString());

    ClearDataReqPb clearReq;
    ClearDataRspPb clearRsp;
    EXPECT_TRUE(masterWorkerSvc.ClearData(clearReq, clearRsp).IsOk());

    PushMetaToWorkerReqPb recoveryReq;
    PushMetaToWorkerRspPb recoveryRsp;
    const auto unsignedRecoveryRc = masterWorkerSvc.PushMetaToWorker(recoveryReq, recoveryRsp);
    EXPECT_EQ(unsignedRecoveryRc.GetMsg().find("mode=STOPPING"), std::string::npos);
    DS_ASSERT_OK(akSkManager->GenerateSignature(recoveryReq));
    const auto recoveryRc = masterWorkerSvc.PushMetaToWorker(recoveryReq, recoveryRsp);
    EXPECT_EQ(recoveryRc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(recoveryRc.GetMsg().find("RECOVERY_RPC"), std::string::npos);
    EXPECT_NE(recoveryRc.GetMsg().find("mode=STOPPING"), std::string::npos);

    RequestMetaFromWorkerReqPb requestMetaReq;
    RequestMetaFromWorkerRspPb requestMetaRsp;
    DS_ASSERT_OK(akSkManager->GenerateSignature(requestMetaReq));
    const auto requestMetaRc = masterWorkerSvc.RequestMetaFromWorker(requestMetaReq, requestMetaRsp);
    EXPECT_EQ(requestMetaRc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(requestMetaRc.GetMsg().find("RECOVERY_RPC"), std::string::npos);

    QueryGlobalRefNumReqPb queryRefReq;
    QueryGlobalRefNumRspPb queryRefRsp;
    DS_ASSERT_OK(akSkManager->GenerateSignature(queryRefReq));
    const auto queryRefRc = masterWorkerSvc.QueryGlobalRefNumOnWorker(queryRefReq, queryRefRsp);
    EXPECT_EQ(queryRefRc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(queryRefRc.GetMsg().find("NORMAL_READ"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, MasterWorkerRejectsDataAndOwnershipInputsOutsideRunning)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    constexpr const char *clientCred = "test-client-credential";
    constexpr const char *serverCred = "test-server-credential";
    akSkManager->SetClientAkSk(clientCred, serverCred);
    akSkManager->SetServerAkSk(AkSkType::SYSTEM, clientCred, serverCred);
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(worker::WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "isolated");
    MasterWorkerOCServiceImpl masterWorkerSvc(impl_, akSkManager);
    masterWorkerSvc.SetRuntimeFacade(&runtime);

    PublishMetaReqPb publishReq;
    PublishMetaRspPb publishRsp;
    DS_ASSERT_OK(akSkManager->GenerateSignature(publishReq));
    const auto publishRc = masterWorkerSvc.PublishMeta(publishReq, publishRsp);
    EXPECT_EQ(publishRc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(publishRc.GetMsg().find("MIGRATION_TARGET"), std::string::npos);
    EXPECT_NE(publishRc.GetMsg().find("PublishMeta"), std::string::npos);

    ChangePrimaryCopyReqPb primaryReq;
    ChangePrimaryCopyRspPb primaryRsp;
    DS_ASSERT_OK(akSkManager->GenerateSignature(primaryReq));
    const auto primaryRc = masterWorkerSvc.ChangePrimaryCopy(primaryReq, primaryRsp);
    EXPECT_EQ(primaryRc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(primaryRc.GetMsg().find("MIGRATION_TARGET"), std::string::npos);
    EXPECT_NE(primaryRc.GetMsg().find("ChangePrimaryCopy"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, PublishMetaRechecksAdmissionWhenQueuedWorkStarts)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    constexpr const char *clientCred = "test-client-credential";
    constexpr const char *serverCred = "test-server-credential";
    akSkManager->SetClientAkSk(clientCred, serverCred);
    akSkManager->SetServerAkSk(AkSkType::SYSTEM, clientCred, serverCred);
    worker::WorkerRuntimeFacade runtime;
    ASSERT_TRUE(MarkRuntimeRunning(runtime));
    MasterWorkerOCServiceImpl masterWorkerSvc(impl_, akSkManager);
    masterWorkerSvc.SetRuntimeFacade(&runtime);

    impl_->threadPool_ = std::make_shared<ThreadPool>(1, 1, "PublishMetaAdmissionTest");
    auto blockerStarted = std::make_shared<std::promise<void>>();
    auto releaseBlocker = std::make_shared<std::promise<void>>();
    auto releaseFuture = releaseBlocker->get_future().share();
    impl_->threadPool_->Execute([blockerStarted, releaseFuture] {
        blockerStarted->set_value();
        releaseFuture.wait();
    });
    blockerStarted->get_future().wait();

    BINEXPECT_CALL((GetObjectFromAnywhereMethod)&WorkerOCServiceImpl::GetObjectFromAnywhere, (_, _, _)).Times(0);
    PublishMetaReqPb req;
    req.mutable_meta()->set_object_key("queued-publish");
    PublishMetaRspPb rsp;
    DS_ASSERT_OK(akSkManager->GenerateSignature(req));
    DS_ASSERT_OK(masterWorkerSvc.PublishMeta(req, rsp));
    runtime.MarkLocalIsolated(worker::WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "isolated");
    releaseBlocker->set_value();
    impl_->threadPool_->Submit([] {}).wait();
}

TEST_F(WorkerOcServiceImplTest, MasterWorkerRejectsNestedRefIncreaseOutsideRunning)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    constexpr const char *clientCred = "test-client-credential";
    constexpr const char *serverCred = "test-server-credential";
    akSkManager->SetClientAkSk(clientCred, serverCred);
    akSkManager->SetServerAkSk(AkSkType::SYSTEM, clientCred, serverCred);
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkStopping(worker::WorkerIsolationReason::PROCESS_STOPPING, "stopping");
    MasterWorkerOCServiceImpl masterWorkerSvc(impl_, akSkManager);
    masterWorkerSvc.SetRuntimeFacade(&runtime);

    NotifyMasterIncNestedReqPb req;
    NotifyMasterIncNestedResPb rsp;
    DS_ASSERT_OK(akSkManager->GenerateSignature(req));
    const auto rc = masterWorkerSvc.NotifyMasterIncNestedRefs(req, rsp);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("NORMAL_WRITE"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("NotifyMasterIncNestedRefs"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, WorkerWorkerReadRejectsBeforeOwnershipEvidence)
{
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkRecovering(worker::WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE,
                           "ownership evidence is incomplete");
    auto workerWorkerSvc = CreateWorkerWorkerOCService();
    workerWorkerSvc.SetRuntimeFacade(&runtime);

    GetObjectRemoteReqPb req;
    GetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    auto rc = workerWorkerSvc.GetObjectRemote(req, rsp, payload, true);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("NORMAL_READ"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("RECOVERING"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, RecoverMetadataOfDataRecordsReadyEvidenceForEmptyBatch)
{
    impl_->metadataRecoveryManager_ = std::make_unique<MetaDataRecoveryManager>(
        localAddress_, objectTable_, MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    std::vector<std::string> failedIds;

    DS_ASSERT_OK(impl_->RecoverMetadataOfData({}, failedIds, ""));

    EXPECT_TRUE(failedIds.empty());
    auto report = impl_->GetLastMetadataRecoveryEvidenceReport();
    EXPECT_TRUE(report.evidence.metadataReady);
    EXPECT_NE(report.detail.find("metadata_recovered=0/0"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, RecoverMetadataOfDataRecordsMissingEvidenceWhenManagerUnavailable)
{
    impl_->metadataRecoveryManager_ = nullptr;
    std::vector<std::string> failedIds;

    auto rc = impl_->RecoverMetadataOfData({ "object-a" }, failedIds, "");

    EXPECT_EQ(rc.GetCode(), StatusCode::K_RUNTIME_ERROR);
    EXPECT_THAT(failedIds, ElementsAre("object-a"));
    auto report = impl_->GetLastMetadataRecoveryEvidenceReport();
    EXPECT_FALSE(report.evidence.metadataReady);
    EXPECT_NE(report.detail.find("metadata_recovered=0/1"), std::string::npos);
    EXPECT_NE(report.detail.find("metadata_failed=1"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, BuildObjectCacheRecoveryEvidenceRequiresMetadataAndSlotReadiness)
{
    impl_->metadataRecoveryManager_ = std::make_unique<MetaDataRecoveryManager>(
        localAddress_, objectTable_, MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(impl_->RecoverMetadataOfData({}, failedIds, ""));
    impl_->slotRecoveryManager_ = std::make_shared<TestSlotRecoveryManager>(std::make_shared<EmptySlotRecoveryStore>());

    auto report = impl_->BuildObjectCacheRecoveryEvidenceReport();

    EXPECT_TRUE(report.evidence.metadataReady);
    EXPECT_TRUE(report.evidence.slotReady);
    EXPECT_TRUE(report.evidence.ownershipReady);
    EXPECT_NE(report.detail.find("metadata_recovered=0/0"), std::string::npos);
    EXPECT_NE(report.detail.find("slot_recovery_disabled"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, BuildObjectCacheRecoveryEvidenceTreatsNoMetadataWorkAsReady)
{
    impl_->slotRecoveryManager_ = std::make_shared<TestSlotRecoveryManager>(std::make_shared<EmptySlotRecoveryStore>());

    auto report = impl_->BuildObjectCacheRecoveryEvidenceReport();

    EXPECT_TRUE(report.evidence.metadataReady);
    EXPECT_TRUE(report.evidence.slotReady);
    EXPECT_TRUE(report.evidence.ownershipReady);
    EXPECT_NE(report.detail.find("metadata_recovered=0/0"), std::string::npos);
    EXPECT_NE(report.detail.find("slot_recovery_disabled"), std::string::npos);
}

TEST_F(WorkerOcServiceImplTest, NewRecoveryGenerationInvalidatesOldCompleteEvidence)
{
    impl_->slotRecoveryManager_ = std::make_shared<TestSlotRecoveryManager>(std::make_shared<EmptySlotRecoveryStore>());
    auto oldGeneration = impl_->BeginRecoveryEvidenceGeneration("old");
    impl_->MarkRestartReconciliationEvidenceReady("old metadata reconciliation complete");
    auto oldReport = impl_->BuildObjectCacheRecoveryEvidenceReport(oldGeneration);
    EXPECT_TRUE(oldReport.evidence.metadataReady);
    EXPECT_TRUE(oldReport.evidence.slotReady);

    auto newGeneration = impl_->BeginRecoveryEvidenceGeneration("new");
    EXPECT_NE(oldGeneration, newGeneration);
    auto pendingReport = impl_->BuildObjectCacheRecoveryEvidenceReport();
    EXPECT_FALSE(pendingReport.evidence.metadataReady);
    EXPECT_FALSE(pendingReport.evidence.ownershipReady);

    auto staleReport = impl_->BuildObjectCacheRecoveryEvidenceReport(oldGeneration);
    EXPECT_FALSE(staleReport.evidence.membershipReady);
    EXPECT_FALSE(staleReport.evidence.metadataReady);

    auto currentReport = impl_->BuildObjectCacheRecoveryEvidenceReport(newGeneration);
    EXPECT_FALSE(currentReport.evidence.metadataReady);
    EXPECT_TRUE(currentReport.evidence.slotReady);
}

TEST_F(WorkerOcServiceImplTest, ResourceRecoveryRequirementRemainsStickyAcrossEvidenceChecks)
{
    worker::WorkerRuntimeFacade runtime;
    ASSERT_TRUE(MarkRuntimeRunning(runtime));
    impl_->SetRuntimeFacade(&runtime);
    impl_->metadataRecoveryManager_ = std::make_unique<MetaDataRecoveryManager>(
        localAddress_, objectTable_, MetaDataRecoveryManager::ClusterAccess{}, nullptr, metadataRoute_);
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(impl_->RecoverMetadataOfData({}, failedIds, ""));
    impl_->slotRecoveryManager_ = std::make_shared<TestSlotRecoveryManager>(std::make_shared<EmptySlotRecoveryStore>());
    impl_->evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, localAddress_,
                                                                        HostPort("127.0.0.1", 19091), metadataRoute_);
    BINEXPECT_CALL(&WorkerOcEvictionManager::IsResourceRecovered, (_))
        .Times(2)
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    impl_->MarkOutOfMemoryIfNeeded(Status(StatusCode::K_OUT_OF_MEMORY, "allocation failed"), "Create");

    EXPECT_TRUE(impl_->BuildObjectCacheRecoveryEvidenceReport().evidence.resourceReady);
    EXPECT_FALSE(impl_->BuildObjectCacheRecoveryEvidenceReport().evidence.resourceReady);
}

TEST_F(WorkerOcServiceImplTest, ResourceRecoveryRequirementClearsAfterMatchingGenerationCommit)
{
    worker::WorkerRuntimeFacade runtime;
    impl_->SetRuntimeFacade(&runtime);
    impl_->evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, localAddress_,
                                                                        HostPort("127.0.0.1", 19091), metadataRoute_);
    BINEXPECT_CALL(&WorkerOcEvictionManager::IsResourceRecovered, (_)).Times(1).WillOnce(Return(true));
    impl_->MarkOutOfMemoryIfNeeded(Status(StatusCode::K_OUT_OF_MEMORY, "allocation failed"), "Create");
    uint64_t generation = 0;

    EXPECT_TRUE(impl_->BuildObjectCacheRecoveryEvidenceReport(&generation).evidence.resourceReady);
    EXPECT_TRUE(impl_->PublishResourceRecoveryIfCurrent(generation, [] { return true; }));
    EXPECT_TRUE(impl_->BuildObjectCacheRecoveryEvidenceReport().evidence.resourceReady);
}

TEST_F(WorkerOcServiceImplTest, StaleResourceRecoveryCommitDoesNotClearNewIncident)
{
    worker::WorkerRuntimeFacade runtime;
    impl_->SetRuntimeFacade(&runtime);
    impl_->evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, localAddress_,
                                                                        HostPort("127.0.0.1", 19091), metadataRoute_);
    BINEXPECT_CALL(&WorkerOcEvictionManager::IsResourceRecovered, (_))
        .Times(2)
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    impl_->MarkOutOfMemoryIfNeeded(Status(StatusCode::K_OUT_OF_MEMORY, "first incident"), "Create");
    uint64_t staleGeneration = 0;
    EXPECT_TRUE(impl_->BuildObjectCacheRecoveryEvidenceReport(&staleGeneration).evidence.resourceReady);

    impl_->MarkOutOfMemoryIfNeeded(Status(StatusCode::K_OUT_OF_MEMORY, "second incident"), "Create");
    EXPECT_FALSE(impl_->PublishResourceRecoveryIfCurrent(staleGeneration, [] { return true; }));

    EXPECT_FALSE(impl_->BuildObjectCacheRecoveryEvidenceReport().evidence.resourceReady);
}

TEST_F(WorkerOcServiceImplTest, StaleResourceRecoveryDoesNotPublishRunningAdmission)
{
    worker::WorkerRuntimeFacade runtime;
    worker::WorkerRunningEvidence runningEvidence{ true, true, true, true, true, true };
    ASSERT_TRUE(runtime.TryCompleteRecovery(runningEvidence, "ready"));
    impl_->SetRuntimeFacade(&runtime);
    impl_->evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, localAddress_,
                                                                        HostPort("127.0.0.1", 19091), metadataRoute_);
    BINEXPECT_CALL(&WorkerOcEvictionManager::IsResourceRecovered, (_)).Times(1).WillOnce(Return(true));
    impl_->MarkOutOfMemoryIfNeeded(Status(StatusCode::K_OUT_OF_MEMORY, "first incident"), "Create");
    uint64_t staleGeneration = 0;
    EXPECT_TRUE(impl_->BuildObjectCacheRecoveryEvidenceReport(&staleGeneration).evidence.resourceReady);

    impl_->MarkOutOfMemoryIfNeeded(Status(StatusCode::K_OUT_OF_MEMORY, "second incident"), "Create");
    DS_ASSERT_OK(SetHealthProbe());
    SetTopologyServingAdmission(false);
    Raii reset([]() {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    bool published = false;

    const bool open = impl_->PublishResourceRecoveryIfCurrent(staleGeneration, [&] {
        published = true;
        const bool running = runtime.TryCompleteRecovery(runningEvidence, "stale recovery");
        SetTopologyServingAdmission(running);
        return running;
    });

    EXPECT_FALSE(open);
    EXPECT_FALSE(published);
    EXPECT_FALSE(IsHealthy());
    EXPECT_EQ(runtime.GetSnapshot().mode, worker::WorkerServiceMode::OUT_OF_MEMORY);
}

TEST_F(WorkerOcServiceImplTest, CollectDisconnectedClientRefIdsReturnsOnlyMissingClients)
{
    const auto liveClient = ClientKey::Intern("live-client");
    const auto staleClient = ClientKey::Intern("stale-client");
    DS_ASSERT_OK(worker::ClientManager::Instance().AddClient(liveClient, -1));
    Raii cleanup([&liveClient]() { worker::ClientManager::Instance().RemoveClient(liveClient); });

    std::vector<std::string> failIncIds;
    std::vector<std::string> firstIncIds;
    DS_ASSERT_OK(impl_->globalRefTable_->GIncreaseRef(liveClient, { "live-object" }, failIncIds, firstIncIds));
    failIncIds.clear();
    firstIncIds.clear();
    DS_ASSERT_OK(impl_->globalRefTable_->GIncreaseRef(staleClient, { "stale-object" }, failIncIds, firstIncIds));

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
    auto refFuture =
        std::async(std::launch::async, [&gRefProc, &req, &rsp] { return gRefProc.GIncreaseRef(req, rsp); });
    auto clearRetrySleepInject = Raii([]() { (void)inject::Clear(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT); });

    ASSERT_TRUE(api->WaitForFirstRefMovingCall(std::chrono::milliseconds(K_WAIT_FIRST_MOVING_CALL_TIMEOUT_MS)));
    ASSERT_TRUE(WaitForInjectPointExecuteCount(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT,
                                               K_FIRST_INJECT_EXECUTE_COUNT,
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
    auto clearRetrySleepInject = Raii([]() { (void)inject::Clear(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT); });

    ASSERT_TRUE(api->WaitForFirstRefMovingCall(std::chrono::milliseconds(K_WAIT_FIRST_MOVING_CALL_TIMEOUT_MS)));
    ASSERT_TRUE(WaitForInjectPointExecuteCount(K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT,
                                               K_FIRST_INJECT_EXECUTE_COUNT,
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

TEST_F(WorkerOcServiceImplTest, ClearMatchedObjectsRecoversBeforeCleanupWhenMetadataRecoveryDisabled)
{
    const bool oldEnableMetadataRecovery = FLAGS_enable_metadata_recovery;
    FLAGS_enable_metadata_recovery = false;
    Raii restoreFlag([oldEnableMetadataRecovery] { FLAGS_enable_metadata_recovery = oldEnableMetadataRecovery; });
    const std::string objectKey = "recover-before-cleanup";
    AddObject(objectKey);

    BINEXPECT_CALL((FilterObjectsNeedClearByMasterMethod)&WorkerOcServiceClearDataFlow::FilterObjectsNeedClearByMaster,
                   (ElementsAre(objectKey), _, _, _))
        .WillOnce(Invoke([&objectKey](const std::vector<std::string> &, std::vector<std::string> &needClear,
                                      std::unordered_set<std::string> &, std::unordered_map<std::string, uint64_t> &) {
            needClear.emplace_back(objectKey);
        }));
    BINEXPECT_CALL((RetryFailedMetadataRecoveryMethod)&WorkerOcServiceClearDataFlow::
                       RetryFailedMetadataRecoveryAndClearUnrecoverable,
                   (ElementsAre(objectKey)))
        .WillOnce(Invoke([](const std::vector<std::string> &) {
            WorkerOcServiceClearDataFlow::RetryMetadataRecoveryResult result;
            result.recoveredCount = 1;
            return result;
        }));

    ClearDataRetryIds retryIds;
    dataClearImpl_->ClearMatchedObjects({ objectKey }, retryIds);

    EXPECT_TRUE(retryIds.Empty());
    std::shared_ptr<SafeObjType> entry;
    EXPECT_TRUE(objectTable_->Get(objectKey, entry).IsOk());
}

TEST_F(WorkerOcServiceImplTest, DISABLED_ClearDataImplDispatchesMatchedObjectsToClearAndRebuild)
{
    using GetMatchObjectIdsMethod =
        Status (WorkerOcServiceClearDataFlow::*)(const ClearDataReqPb &, std::vector<std::string> &);
    using ClearMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);
    using RebuildRefForMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);

    std::vector<std::string> matchObjIds{ "obj1", "obj2" };
    std::vector<std::string> clearObjIds;
    std::vector<std::string> rebuildObjIds;
    BINEXPECT_CALL((GetMatchObjectIdsMethod)&WorkerOcServiceClearDataFlow::GetMatchObjectIds, (_, _))
        .WillOnce(Invoke([&matchObjIds](const ClearDataReqPb &, std::vector<std::string> &outObjIds) {
            outObjIds = matchObjIds;
            return Status::OK();
        }));
    BINEXPECT_CALL((ClearMatchedObjectsMethod)&WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _))
        .WillOnce(Invoke(
            [&clearObjIds](const std::vector<std::string> &objIds, ClearDataRetryIds &) { clearObjIds = objIds; }));
    BINEXPECT_CALL((RebuildRefForMatchedObjectsMethod)&WorkerOcServiceClearDataFlow::RebuildRefForMatchedObjects,
                   (_, _))
        .WillOnce(Invoke(
            [&rebuildObjIds](const std::vector<std::string> &objIds, ClearDataRetryIds &) { rebuildObjIds = objIds; }));

    ClearDataRetryIds retryIds;
    ClearDataReqPb req;
    DS_ASSERT_OK(dataClearImpl_->ClearDataImpl(req, retryIds));
    EXPECT_THAT(clearObjIds, ElementsAreArray(matchObjIds));
    EXPECT_THAT(rebuildObjIds, ElementsAreArray(matchObjIds));
    EXPECT_TRUE(retryIds.Empty());
}

TEST_F(WorkerOcServiceImplTest, ClearDataImplReturnsWhenSelectObjectsFailed)
{
    using GetMatchObjectIdsMethod =
        Status (WorkerOcServiceClearDataFlow::*)(const ClearDataReqPb &, std::vector<std::string> &);
    using ClearMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);
    using RebuildRefForMatchedObjectsMethod =
        void (WorkerOcServiceClearDataFlow::*)(const std::vector<std::string> &, ClearDataRetryIds &);

    Status selectFailed(StatusCode::K_RUNTIME_ERROR, "select failed");
    BINEXPECT_CALL((GetMatchObjectIdsMethod)&WorkerOcServiceClearDataFlow::GetMatchObjectIds, (_, _))
        .WillOnce(Return(selectFailed));
    BINEXPECT_CALL((ClearMatchedObjectsMethod)&WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _)).Times(0);
    BINEXPECT_CALL((RebuildRefForMatchedObjectsMethod)&WorkerOcServiceClearDataFlow::RebuildRefForMatchedObjects,
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
        kRecoverMasterAppRefSubscriber, [](std::function<bool(const std::string &)> matchFunc, const std::string &) {
            EXPECT_TRUE(matchFunc("obj1"));
            EXPECT_FALSE(matchFunc("obj2"));
            EXPECT_TRUE(matchFunc("obj3"));
            EXPECT_FALSE(matchFunc("obj4"));
            return Status(StatusCode::K_RUNTIME_ERROR, "recover failed");
        });
    BINEXPECT_CALL((IncreaseMasterRefMethod)&WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRefWithLock, (_, _))
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
    BINEXPECT_CALL((ClearMatchedObjectsMethod)&WorkerOcServiceClearDataFlow::ClearMatchedObjects, (_, _))
        .WillOnce(Invoke([&clearObjIds](const std::vector<std::string> &objIds, ClearDataRetryIds &retryIds) {
            clearObjIds = objIds;
            retryIds.clearFailedIds.emplace("clear-next");
        }));
    BINEXPECT_CALL((RetryIncreaseMasterRefMethod)&WorkerOcServiceClearDataFlow::RetryIncreaseMasterRef, (_, _))
        .WillOnce(Invoke([&increaseObjIds](const std::vector<std::string> &objIds, ClearDataRetryIds &retryIds) {
            increaseObjIds = objIds;
            retryIds.increaseFailedIds.emplace("increase-next");
        }));
    BINEXPECT_CALL((RetryRecoverMasterAppRefMethod)&WorkerOcServiceClearDataFlow::RetryRecoverMasterAppRef, (_, _))
        .WillOnce(Invoke([&recoverObjIds](const std::vector<std::string> &objIds, ClearDataRetryIds &retryIds) {
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

TEST_F(WorkerOcServiceImplTest, NotifyRemoteGetRejectsAfterLocalScaleInStarts)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    // Simulate voluntary scale-in by flipping the local exit flag that AcquireIncomingMigrationAdmission polls.
    exitRequested_.store(true, std::memory_order_relaxed);

    NotifyRemoteGetReqPb req;
    req.add_object_keys("late-notify-remote-get-object");
    NotifyRemoteGetRspPb rsp;
    EXPECT_EQ(impl_->NotifyRemoteGet(req, rsp).GetCode(), StatusCode::K_NOT_READY);
}

TEST_F(WorkerOcServiceImplTest, NotifyRemoteGetHoldsAdmissionUntilRequestReturns)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    constexpr std::chrono::seconds schedulingTimeout(1);
    constexpr std::chrono::seconds closeBudget(2);
    constexpr std::chrono::milliseconds observationWindow(50);
    const std::string injectPoint = "WorkerOCServiceImpl.NotifyRemoteGet.afterAdmission";

    // Block the RPC at the afterAdmission inject point so the test can assert
    // that CloseIncomingMigrationAdmissionAndWait waits while admission is held.
    DS_ASSERT_OK(inject::Set(injectPoint, "pause()"));

    NotifyRemoteGetReqPb req;
    req.add_object_keys("admission-hold-object");
    NotifyRemoteGetRspPb rsp;
    auto requestFuture =
        std::async(std::launch::async, [this, &req, &rsp] { return impl_->NotifyRemoteGet(req, rsp); });
    // Wait until the RPC hits the inject point - admission is acquired and held.
    const bool requestAdmitted = WaitForInjectPointExecuteCount(injectPoint, 1, schedulingTimeout);

    auto closeFuture = std::async(std::launch::async, [this, closeBudget] {
        return impl_->gMigrateProc_->CloseIncomingMigrationAdmissionAndWait(std::chrono::steady_clock::now()
                                                                            + closeBudget);
    });
    Status lateAdmission(K_RUNTIME_ERROR, "Migration admission gate did not close");
    const auto gateDeadline = std::chrono::steady_clock::now() + schedulingTimeout;
    do {
        lateAdmission = impl_->gMigrateProc_->AcquireIncomingMigrationAdmission();
        if (lateAdmission.IsOk()) {
            impl_->gMigrateProc_->ReleaseIncomingMigrationAdmission();
            std::this_thread::yield();
        }
    } while (lateAdmission.IsOk() && std::chrono::steady_clock::now() < gateDeadline);
    const auto closeStateWhileRequestPaused = closeFuture.wait_for(observationWindow);

    // Release the RPC by clearing the inject action.
    DS_ASSERT_OK(inject::Clear(injectPoint));
    (void)requestFuture.get();
    const auto closeStatus = closeFuture.get();

    EXPECT_TRUE(requestAdmitted);
    EXPECT_EQ(lateAdmission.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_EQ(closeStateWhileRequestPaused, std::future_status::timeout);
    DS_EXPECT_OK(closeStatus);
}

TEST_F(WorkerOcServiceImplTest, NotifyRemoteGetReturnsFailureWhenDrainTimesOut)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    constexpr std::chrono::seconds schedulingTimeout(1);
    constexpr std::chrono::milliseconds closeBudget(100);
    const std::string injectPoint = "WorkerOCServiceImpl.NotifyRemoteGet.afterAdmission";

    // Block the RPC at the afterAdmission inject point so drain will time out.
    DS_ASSERT_OK(inject::Set(injectPoint, "pause()"));

    NotifyRemoteGetReqPb req;
    req.add_object_keys("drain-timeout-object");
    NotifyRemoteGetRspPb rsp;
    auto requestFuture =
        std::async(std::launch::async, [this, &req, &rsp] { return impl_->NotifyRemoteGet(req, rsp); });
    // Wait until the RPC hits the inject point - admission is acquired and held.
    const bool requestAdmitted = WaitForInjectPointExecuteCount(injectPoint, 1, schedulingTimeout);

    // Drain with an already-expired deadline so it times out immediately.
    auto closeFuture = std::async(std::launch::async, [this, closeBudget] {
        return impl_->gMigrateProc_->CloseIncomingMigrationAdmissionAndWait(std::chrono::steady_clock::now()
                                                                            + closeBudget);
    });
    // Wait for drain to time out.
    const auto closeStatus = closeFuture.get();
    EXPECT_EQ(closeStatus.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
    EXPECT_TRUE(impl_->gMigrateProc_->IsIncomingMigrationDrainTimedOut());

    // Release the RPC. It should return K_NOT_READY (checkpoint A: admission closed).
    DS_ASSERT_OK(inject::Clear(injectPoint));
    const auto requestStatus = requestFuture.get();

    EXPECT_TRUE(requestAdmitted);
    EXPECT_EQ(requestStatus.GetCode(), StatusCode::K_NOT_READY);
}

}  // namespace ut
}  // namespace datasystem
