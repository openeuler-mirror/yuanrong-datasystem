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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gmock/gmock.h>

#include "../../../common/binmock/binmock.h"
#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/delayed_release_shm_manager.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/object_cache/device/worker_device_oc_manager.h"
#include "datasystem/worker/worker_health_check.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_multi_publish_impl.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_health_check.h"
#include "tests/ut/worker/object_cache/test_metadata_route.h"
#include "tests/ut/worker/object_cache/test_placement_facade.h"
#include "ut/common.h"
#define private public
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"
#undef private

using namespace ::testing;

using namespace datasystem::object_cache;

DS_DECLARE_string(health_check_path);
DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_bool(enable_leaving_intercept);
DS_DECLARE_bool(enable_reconciliation);
DS_DECLARE_bool(enable_transport_fallback);
DS_DECLARE_bool(enable_worker_worker_batch_get);
DS_DECLARE_uint32(arena_per_tenant);
DS_DECLARE_int32(oc_worker_worker_parallel_min);

namespace datasystem {
namespace ut {
namespace {
using WorkerTestPlacementFacade = TestPlacementFacade;
using ClearDataRetryIds = WorkerOcServiceClearDataFlow::ClearDataRetryIds;
constexpr int64_t K_META_MOVING_RETRY_TIMEOUT_MS = 1'000;
constexpr size_t K_EXPECTED_META_MOVING_RPC_CALLS = 2;
constexpr uint64_t K_META_MOVING_SUCCESS_VERSION = 7;
using WorkerMasterOCApiManager = worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>;

void AddDeviceRemoteGetMeta(master::QueryMetaRspPb &rsp, const std::string &objectKey, uint64_t version,
                            const HostPort &primaryAddress, DataFormat format)
{
    auto *queryMeta = rsp.add_query_metas();
    queryMeta->mutable_meta()->set_object_key(objectKey);
    queryMeta->mutable_meta()->set_version(version);
    queryMeta->mutable_meta()->set_data_size(1);
    queryMeta->mutable_meta()->set_primary_address(primaryAddress.ToString());
    queryMeta->mutable_meta()->mutable_config()->set_data_format(static_cast<uint32_t>(format));
}

constexpr const char *K_REF_MOVING_RETRY_BEFORE_SLEEP_INJECT_POINT =
    "WorkerOcServiceGlobalReferenceImpl.SleepForRefMovingRetry.beforeSleep";
constexpr int64_t K_INJECT_WAIT_POLL_MS = 1;
constexpr uint64_t K_FIRST_INJECT_EXECUTE_COUNT = 1;
constexpr int K_EXPECTED_REF_MOVING_GROUP_RPC_CALLS = 3;
constexpr const char *K_LOCAL_TEST_HOST = "127.0.0.1";
constexpr uint16_t K_PEER_MASTER_PORT = 18482;
constexpr uint64_t K_REMOTE_GET_TEST_MEMORY_SIZE = 32 * 1024UL * 1024UL;
constexpr int64_t K_WAIT_FIRST_MOVING_CALL_TIMEOUT_MS = 1000;
constexpr int64_t K_WAIT_RETRY_SLEEP_INJECT_TIMEOUT_MS = 1000;
constexpr int64_t K_LOCK_PROBE_TIMEOUT_MS = 1000;
constexpr int64_t K_REJOIN_RECONCILIATION_LOCK_UPPER_BOUND_MS = 1000;
constexpr uint64_t K_METADATA_FAILURE_TEST_DATA_SIZE = 8;
constexpr const char *K_METADATA_FAILURE_TEST_EXTRA = "metadata_transport_extra";

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

bool WaitForCondition(const std::function<bool()> &condition, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(K_INJECT_WAIT_POLL_MS));
    }
    return condition();
}

class FakeWorkerMasterOCApi final : public worker::WorkerLocalMasterOCApi {
public:
    using CreateMetaHandler = std::function<Status(master::CreateMetaReqPb &, master::CreateMetaRspPb &)>;
    using QueryMetaHandler = std::function<Status(int, master::QueryMetaRspPb &)>;
    using RemoveMetaHandler = std::function<Status(master::RemoveMetaReqPb &, master::RemoveMetaRspPb &)>;

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

    Status CreateMultiMeta(master::CreateMultiMetaReqPb &req, master::CreateMultiMetaRspPb &rsp,
                           bool retry = true) override
    {
        (void)retry;
        std::lock_guard<std::mutex> lock(mutex_);
        createMultiMetaRequests_.emplace_back(req);
        if (createMultiMetaHandler_) {
            return createMultiMetaHandler_(req, rsp);
        }
        return Status::OK();
    }

    Status CreateMeta(master::CreateMetaReqPb &req, master::CreateMetaRspPb &rsp) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++createMetaCallCount_;
        return createMetaHandler_ == nullptr ? Status::OK() : createMetaHandler_(req, rsp);
    }

    Status QueryMeta(master::QueryMetaReqPb &request, uint64_t subTimeout, master::QueryMetaRspPb &response,
                     std::vector<RpcMessage> &payloads) override
    {
        (void)request;
        (void)subTimeout;
        (void)payloads;
        std::lock_guard<std::mutex> lock(mutex_);
        ++queryMetaCallCount_;
        if (queryMetaHandler_) {
            return queryMetaHandler_(queryMetaCallCount_, response);
        }
        response = queryMetaResponse_;
        return queryMetaStatus_;
    }

    Status GetObjectLocations(master::GetObjectLocationsReqPb &, master::GetObjectLocationsRspPb &response) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++getObjectLocationsCallCount_;
        response = getObjectLocationsResponse_;
        return getObjectLocationsStatus_;
    }

    Status GetObjectLocations(master::GetObjectLocationsReqPb &request, master::GetObjectLocationsRspPb &response,
                              int64_t) override
    {
        return GetObjectLocations(request, response);
    }

    Status RemoveMeta(master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        removeMetaRequests_.emplace_back(request);
        return removeMetaHandler_ == nullptr ? Status::OK() : removeMetaHandler_(request, response);
    }

    Status PureQueryMeta(master::PureQueryMetaReqPb &, master::PureQueryMetaRspPb &response) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++pureQueryMetaCallCount_;
        response = pureQueryMetaResponse_;
        return pureQueryMetaStatus_;
    }

    void SetResponse(const master::GIncreaseRspPb &response)
    {
        response_ = response;
    }

    void SetQueryMetaStatus(const Status &status)
    {
        queryMetaStatus_ = status;
    }

    void SetQueryMetaResponse(const master::QueryMetaRspPb &response)
    {
        queryMetaResponse_ = response;
    }

    void SetQueryMetaHandler(QueryMetaHandler handler)
    {
        queryMetaHandler_ = std::move(handler);
    }

    void SetGetObjectLocationsStatus(const Status &status)
    {
        getObjectLocationsStatus_ = status;
    }

    void SetGetObjectLocationsResponse(const master::GetObjectLocationsRspPb &response)
    {
        getObjectLocationsResponse_ = response;
    }

    void SetPureQueryMetaStatus(const Status &status)
    {
        pureQueryMetaStatus_ = status;
    }

    void SetPureQueryMetaResponse(const master::PureQueryMetaRspPb &response)
    {
        pureQueryMetaResponse_ = response;
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

    void SetCreateMultiMetaHandler(
        std::function<Status(master::CreateMultiMetaReqPb &, master::CreateMultiMetaRspPb &)> handler)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        createMultiMetaHandler_ = std::move(handler);
    }

    void SetCreateMetaHandler(CreateMetaHandler handler)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        createMetaHandler_ = std::move(handler);
    }

    void SetRemoveMetaHandler(RemoveMetaHandler handler)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        removeMetaHandler_ = std::move(handler);
    }

    int CreateMetaCallCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return createMetaCallCount_;
    }

    std::vector<master::CreateMultiMetaReqPb> CreateMultiMetaRequests() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return createMultiMetaRequests_;
    }

    int QueryMetaCallCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queryMetaCallCount_;
    }

    int GetObjectLocationsCallCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return getObjectLocationsCallCount_;
    }

    std::vector<master::RemoveMetaReqPb> RemoveMetaRequests() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return removeMetaRequests_;
    }

    int PureQueryMetaCallCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return pureQueryMetaCallCount_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    master::GIncreaseRspPb response_;
    master::GDecreaseRspPb decreaseResponse_;
    master::QueryMetaRspPb queryMetaResponse_;
    master::GetObjectLocationsRspPb getObjectLocationsResponse_;
    master::PureQueryMetaRspPb pureQueryMetaResponse_;
    Status status_{ Status::OK() };
    Status queryMetaStatus_{ Status::OK() };
    Status getObjectLocationsStatus_{ Status::OK() };
    Status pureQueryMetaStatus_{ Status::OK() };
    std::vector<std::string> requestedObjectKeys_;
    bool returnRefMovingOnce_{ false };
    bool firstRefMovingCallSeen_{ false };
    int increaseCallCount_{ 0 };
    int decreaseCallCount_{ 0 };
    std::function<Status(master::CreateMultiMetaReqPb &, master::CreateMultiMetaRspPb &)> createMultiMetaHandler_;
    CreateMetaHandler createMetaHandler_;
    QueryMetaHandler queryMetaHandler_;
    RemoveMetaHandler removeMetaHandler_;
    std::vector<master::CreateMultiMetaReqPb> createMultiMetaRequests_;
    std::vector<master::RemoveMetaReqPb> removeMetaRequests_;
    int queryMetaCallCount_{ 0 };
    int getObjectLocationsCallCount_{ 0 };
    int pureQueryMetaCallCount_{ 0 };
    int createMetaCallCount_{ 0 };
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
        auto iter = apis_.find(masterAddress);
        return iter == apis_.end() ? api_ : iter->second;
    }

    Status GetWorkerMasterApi(const HostPort &masterAddress, std::shared_ptr<worker::WorkerMasterOCApi> &api) override
    {
        if (lookupStatus_.IsError()) {
            return lookupStatus_;
        }
        auto iter = apis_.find(masterAddress);
        api = iter == apis_.end() ? api_ : iter->second;
        return api == nullptr ? Status(K_RUNTIME_ERROR, "test master api is nullptr") : Status::OK();
    }

    void SetApi(std::shared_ptr<worker::WorkerMasterOCApi> api)
    {
        api_ = std::move(api);
    }

    void SetApi(const HostPort &address, std::shared_ptr<worker::WorkerMasterOCApi> api)
    {
        apis_[address] = std::move(api);
    }

    void SetLookupStatus(Status status)
    {
        lookupStatus_ = std::move(status);
    }

private:
    std::shared_ptr<worker::WorkerMasterOCApi> api_;
    std::unordered_map<HostPort, std::shared_ptr<worker::WorkerMasterOCApi>> apis_;
    Status lookupStatus_;
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
    Status Init(const HostPort &localAddress, const HostPort &peerAddress)
    {
        const auto makeTokens = [](const std::string &address) {
            std::vector<uint32_t> tokens;
            for (uint32_t index = 0; index < 4; ++index) {
                tokens.emplace_back(cluster::HashAlgorithm::MakeToken(address, index, 0));
            }
            return tokens;
        };
        cluster::TopologyState topology;
        topology.clusterHasInit = true;
        topology.version = TOPOLOGY_VERSION;
        topology.members = {
            cluster::Member{ { std::string(MEMBER_ID_SIZE, LOCAL_MEMBER_ID_FILL), localAddress.ToString() },
                             cluster::MemberState::ACTIVE, makeTokens(localAddress.ToString()) },
            cluster::Member{ { std::string(MEMBER_ID_SIZE, PEER_MEMBER_ID_FILL), peerAddress.ToString() },
                             cluster::MemberState::ACTIVE, makeTokens(peerAddress.ToString()) }
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

class TestGetObjectRemoteServerApi final
    : public ServerUnaryWriterReader<GetObjectRemoteRspPb, GetObjectRemoteReqPb> {
public:
    explicit TestGetObjectRemoteServerApi(GetObjectRemoteReqPb req) : req_(std::move(req))
    {
    }

    Status SendStatus(const Status &rc) override
    {
        (void)rc;
        return Status::OK();
    }

    Status Read(GetObjectRemoteReqPb &req) override
    {
        ++readCount_;
        req = req_;
        return Status::OK();
    }

    Status Write(const GetObjectRemoteRspPb &rsp) override
    {
        ++writeCount_;
        rsp_ = rsp;
        return Status::OK();
    }

    Status Finish() override
    {
        return Status::OK();
    }

    Status ReceivePayload(std::vector<RpcMessage> &payload) override
    {
        payload = std::move(payload_);
        return Status::OK();
    }

    Status SendAndTagPayload(std::vector<RpcMessage> &payload, bool tagPayloadFrame) override
    {
        (void)tagPayloadFrame;
        ++payloadSendCount_;
        payload_ = std::move(payload);
        return Status::OK();
    }

    Status SendPayload(std::vector<RpcMessage> &payload) override
    {
        ++payloadSendCount_;
        payload_ = std::move(payload);
        return Status::OK();
    }

    Status SendAndTagPayload(const std::vector<MemView> &payload, bool tagPayloadFrame) override
    {
        (void)payload;
        (void)tagPayloadFrame;
        return Status::OK();
    }

    Status SendPayload(const std::vector<MemView> &payload) override
    {
        (void)payload;
        return Status::OK();
    }

    Status GetOutMsg(RpcMsgFrames &outMsg) override
    {
        (void)outMsg;
        return Status::OK();
    }

    bool EnableMsgQ() override
    {
        return false;
    }

    void SetRequestInProgress() override
    {
    }

    void SetRequestComplete() override
    {
    }

    const GetObjectRemoteRspPb &Response() const
    {
        return rsp_;
    }

    const std::vector<RpcMessage> &Payload() const
    {
        return payload_;
    }

    size_t ReadCount() const
    {
        return readCount_;
    }

    size_t WriteCount() const
    {
        return writeCount_;
    }

    size_t PayloadSendCount() const
    {
        return payloadSendCount_;
    }

private:
    GetObjectRemoteReqPb req_;
    GetObjectRemoteRspPb rsp_;
    std::vector<RpcMessage> payload_;
    size_t readCount_{ 0 };
    size_t writeCount_{ 0 };
    size_t payloadSendCount_{ 0 };
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

    void AddReadableObject(const std::string &objectKey, uint64_t version = 1, uint64_t dataSize = 1024)
    {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        obj->SetDataSize(dataSize);
        obj->SetCreateTime(version);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE_EVICT);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->stateInfo.SetPrimaryCopy(true);
        obj->SetShmUnit(std::make_shared<ShmUnit>());
        DS_ASSERT_OK(objectTable_->Insert(objectKey, std::move(obj)));
    }

    void AddTransferableObject(const std::string &objectKey, uint64_t dataSize)
    {
        auto obj = std::make_unique<ObjCacheShmUnit>();
        auto shmUnit = std::make_shared<ShmUnit>();
        DS_ASSERT_OK(shmUnit->AllocateMemory("", dataSize, false));
        obj->SetShmUnit(std::move(shmUnit));
        obj->SetDataSize(dataSize);
        obj->SetCreateTime(1);
        obj->SetLifeState(ObjectLifeState::OBJECT_SEALED);
        obj->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
        obj->modeInfo.SetCacheType(CacheType::MEMORY);
        obj->stateInfo.SetDataFormat(DataFormat::BINARY);
        obj->stateInfo.SetPrimaryCopy(true);
        DS_ASSERT_OK(objectTable_->Insert(objectKey, std::move(obj)));
    }

    static void SetUnavailableSummary(PeerUbAdmission *admission, const HostPort &peer)
    {
        ASSERT_NE(admission, nullptr);
        UbHealthSummary summary;
        summary.worker = peer;
        summary.incarnation = "remote-get-requester";
        summary.epoch = 1;
        summary.writable = false;
        summary.state = UbAdmissionState::UNAVAILABLE;
        summary.reason = UbFailureClass::PORT_UNAVAILABLE_ERROR4;
        summary.lastStatusCode = StatusCode::K_URMA_ERROR;
        admission->ReplaceGlobalSummaries({ summary });
    }

    static GetObjectRemoteReqPb MakeUrmaRemoteGetRequest(const std::string &objectKey, uint64_t dataSize,
                                                         const HostPort &peer, uint64_t remoteOffset = 0)
    {
        GetObjectRemoteReqPb req;
        req.set_object_key(objectKey);
        req.set_data_size(dataSize);
        req.set_read_size(dataSize);
        auto *urmaInfo = req.mutable_urma_info();
        urmaInfo->set_seg_va(0x1000);
        urmaInfo->set_seg_data_offset(remoteOffset);
        urmaInfo->mutable_request_address()->set_host(peer.Host());
        urmaInfo->mutable_request_address()->set_port(peer.Port());
        return req;
    }

    void AddWorkerRef(const std::string &objectKey, const std::string &clientId = "client-id")
    {
        std::vector<std::string> objectKeys{ objectKey };
        std::vector<std::string> failIncIds;
        std::vector<std::string> firstIncIds;
        DS_ASSERT_OK(globalRefTable_->GIncreaseRef(ClientKey::Intern(clientId), objectKeys, failIncIds, firstIncIds));
        ASSERT_TRUE(failIncIds.empty());
    }

    void InitImplClearDataFlow()
    {
        impl_->clearDataFlow_ = std::make_unique<WorkerOcServiceClearDataFlow>(
            objectTable_, globalRefTable_, nullptr, impl_->gRefProc_, impl_->deleteProc_, nullptr, metadataRoute_,
            *endpointPolicy_, localAddress_.ToString());
    }

    WorkerOcServiceCrudParam MakeCrudParam(std::shared_ptr<WorkerMasterOCApiManager> apiManager = nullptr,
                                           const worker::MetadataRouteResolver *metadataRoute = nullptr,
                                           const ObjectEndpointPolicy *endpointPolicy = nullptr,
                                           std::function<void(const HostPort &, const Status &)> observer = nullptr,
                                           std::function<bool(const HostPort &)> failureReported = nullptr)
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
            .metadataRpcObserver = std::move(observer),
            .allowDirectoryLag = false,
            .metadataRpcFailureReported = std::move(failureReported),
        };
    }

    std::unique_ptr<SafeObjType> MakeMetadataFailureObject(ObjectLifeState lifeState)
    {
        auto object = std::make_unique<ObjCacheShmUnit>();
        object->SetDataSize(K_METADATA_FAILURE_TEST_DATA_SIZE);
        object->SetLifeState(lifeState);
        object->modeInfo.SetWriteMode(WriteMode::NONE_L2_CACHE);
        object->stateInfo.SetDataFormat(DataFormat::BINARY);
        return std::make_unique<SafeObjType>(std::move(object));
    }

    std::unique_ptr<WorkerOcServicePublishImpl> MakePublishProcessor(
        const std::shared_ptr<WorkerMasterOCApiManager> &apiManager)
    {
        auto param = MakeCrudParam(apiManager);
        return std::make_unique<WorkerOcServicePublishImpl>(
            param, std::make_shared<ThreadPool>(1), std::make_shared<AkSkManager>(0), localAddress_);
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

class WorkerOcRemoteGetAdmissionTest : public WorkerOcServiceImplTest {
public:
    void SetUp() override
    {
        WorkerOcServiceImplTest::SetUp();
        savedArenaPerTenant_ = FLAGS_arena_per_tenant;
        FLAGS_arena_per_tenant = 1;
        allocator_ = memory::Allocator::Instance();
        allocator_->ResetForTest();
        DS_ASSERT_OK(allocator_->Init(K_REMOTE_GET_TEST_MEMORY_SIZE));
    }

    void TearDown() override
    {
        WorkerOcServiceImplTest::TearDown();
        objectTable_.reset();
        allocator_->ResetForTest();
        allocator_ = nullptr;
        FLAGS_arena_per_tenant = savedArenaPerTenant_;
    }

private:
    memory::Allocator *allocator_{ nullptr };
    uint32_t savedArenaPerTenant_{ 0 };
};

TEST_F(WorkerOcServiceImplTest, DecreaseMemoryRefDelaysOnlyMarkedShmUnit)
{
    SetUnhealthy();
    Raii restoreHealth([] { SetUnhealthy(); });
    placement_.SetOwner("decrease-memory-ref", localAddress_);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    impl_->healthPublicationEnabled_.store(true, std::memory_order_release);
    impl_->reconciliationReady_.store(true, std::memory_order_release);
    DS_ASSERT_OK(impl_->RefreshStartupHealth());

    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const auto clientId = ClientKey::Intern("delay-release-client");
    const auto remainingClientId = ClientKey::Intern("remaining-reference-client");

    auto delayedShmUnit = std::make_shared<ShmUnit>();
    delayedShmUnit->id = ShmKey::Intern("delay-release-shm");
    const std::weak_ptr<ShmUnit> delayedWeak = delayedShmUnit;
    impl_->memoryRefTable_->AddShmUnit(clientId, delayedShmUnit);
    impl_->memoryRefTable_->AddShmUnit(remainingClientId, delayedShmUnit);
    DS_ASSERT_OK(impl_->DecreaseMemoryRef(clientId, { delayedShmUnit->GetId() }, true));
    DS_ASSERT_OK(impl_->DecreaseMemoryRef(remainingClientId, { delayedShmUnit->GetId() }, false));
    delayedShmUnit.reset();
    EXPECT_FALSE(delayedWeak.expired());

    auto immediateShmUnit = std::make_shared<ShmUnit>();
    immediateShmUnit->id = ShmKey::Intern("immediate-release-shm");
    const std::weak_ptr<ShmUnit> immediateWeak = immediateShmUnit;
    impl_->memoryRefTable_->AddShmUnit(clientId, immediateShmUnit);
    DS_ASSERT_OK(impl_->DecreaseMemoryRef(clientId, { immediateShmUnit->GetId() }, false));
    immediateShmUnit.reset();
    EXPECT_TRUE(immediateWeak.expired());

    EXPECT_TRUE(WaitForCondition(
        [&delayedWeak] { return delayedWeak.expired(); },
        std::chrono::milliseconds(DEFAULT_SHM_DELAY_RELEASE_MS * 10)));
}

TEST_F(WorkerOcServiceImplTest, RemoteGetTryLockMissDoesNotLeaveEmptyEntry)
{
    constexpr size_t kAttemptCount = 2;
    const std::string objectKey = "remote-get-try-lock-missing-key";
    ScopedRequestContext requestContext;
    WorkerWorkerOCServiceImpl remoteService(
        impl_, nullptr, topologyRuntime_.Engine()->Membership(), []() { return true; },
        []() { return cluster::ControlBackendObservation{}; });

    for (size_t i = 0; i < kAttemptCount; ++i) {
        std::shared_ptr<SafeObjType> safeEntry;
        auto rc = remoteService.GetSafeObjectEntry(objectKey, true, 0, safeEntry);

        ASSERT_EQ(rc.GetCode(), K_NOT_FOUND);
        EXPECT_THAT(rc.GetMsg(), HasSubstr("Object not found"));
        EXPECT_THAT(rc.GetMsg(), Not(HasSubstr("realObject is null")));
        EXPECT_EQ(objectTable_->Contains(objectKey).GetCode(), K_NOT_FOUND);
    }
}

TEST_F(WorkerOcServiceImplTest, RemoteGetL2MissDoesNotLeaveEmptyEntry)
{
    const std::string objectKey = "remote-get-l2-missing-key";
    ScopedRequestContext requestContext;
    WorkerWorkerOCServiceImpl remoteService(
        impl_, nullptr, topologyRuntime_.Engine()->Membership(), []() { return true; },
        []() { return cluster::ControlBackendObservation{}; });
    std::shared_ptr<SafeObjType> safeEntry;

    auto rc = remoteService.GetSafeObjectEntry(objectKey, false, 0, safeEntry);

    ASSERT_EQ(rc.GetCode(), K_NOT_FOUND);
    EXPECT_EQ(objectTable_->Contains(objectKey).GetCode(), K_NOT_FOUND);
}

TEST_F(WorkerOcRemoteGetAdmissionTest, BlockingRemoteGetAdmissionFailureFallsBackToPayloadWithoutUrmaPost)
{
    constexpr uint64_t dataSize = 16;
    const std::string objectKey = "blocked-remote-get-fallback";
    const HostPort requester("192.0.2.30", 18481);
    AddTransferableObject(objectKey, dataSize);
    SetUnavailableSummary(impl_->GetUbAdmission(), requester);
    const bool savedFallback = FLAGS_enable_transport_fallback;
    Raii restoreFallback([savedFallback] { FLAGS_enable_transport_fallback = savedFallback; });
    FLAGS_enable_transport_fallback = true;
    BINEXPECT_CALL(&datasystem::IsUrmaEnabled, ()).WillRepeatedly(Return(true));
    BINEXPECT_CALL(&datasystem::CheckTransportConnectionStable, (_, _)).Times(0);
    BINEXPECT_CALL(&datasystem::UrmaWritePayload, (_, _, _, _, _, _, _, _, _, _, _, _, _, _)).Times(0);
    auto akSkManager = std::make_shared<AkSkManager>();
    WorkerWorkerOCServiceImpl remoteService(
        impl_, akSkManager, topologyRuntime_.Engine()->Membership(), []() { return true; },
        []() { return cluster::ControlBackendObservation{}; });
    auto req = MakeUrmaRemoteGetRequest(objectKey, dataSize, requester);
    auto serverApi = std::make_shared<TestGetObjectRemoteServerApi>(std::move(req));

    auto rc = remoteService.GetObjectRemote(serverApi);

    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(serverApi->ReadCount(), 1);
    EXPECT_EQ(serverApi->WriteCount(), 1);
    EXPECT_EQ(serverApi->PayloadSendCount(), 1);
    EXPECT_EQ(serverApi->Response().error().error_code(), K_OK);
    EXPECT_EQ(serverApi->Response().data_source(), DataTransferSource::DATA_IN_PAYLOAD);
    ASSERT_EQ(serverApi->Payload().size(), 1);
    EXPECT_EQ(serverApi->Payload().front().Size(), dataSize);
}

TEST_F(WorkerOcRemoteGetAdmissionTest, BlockingRemoteGetAdmissionFailureReturnsUnavailableWhenFallbackDisabled)
{
    constexpr uint64_t dataSize = 16;
    const std::string objectKey = "blocked-remote-get-no-fallback";
    const HostPort requester("192.0.2.31", 18481);
    AddTransferableObject(objectKey, dataSize);
    SetUnavailableSummary(impl_->GetUbAdmission(), requester);
    const bool savedFallback = FLAGS_enable_transport_fallback;
    Raii restoreFallback([savedFallback] { FLAGS_enable_transport_fallback = savedFallback; });
    FLAGS_enable_transport_fallback = false;
    BINEXPECT_CALL(&datasystem::IsUrmaEnabled, ()).WillRepeatedly(Return(true));
    BINEXPECT_CALL(&datasystem::CheckTransportConnectionStable, (_, _)).Times(0);
    BINEXPECT_CALL(&datasystem::UrmaWritePayload, (_, _, _, _, _, _, _, _, _, _, _, _, _, _)).Times(0);
    auto akSkManager = std::make_shared<AkSkManager>();
    WorkerWorkerOCServiceImpl remoteService(
        impl_, akSkManager, topologyRuntime_.Engine()->Membership(), []() { return true; },
        []() { return cluster::ControlBackendObservation{}; });
    auto req = MakeUrmaRemoteGetRequest(objectKey, dataSize, requester);
    auto serverApi = std::make_shared<TestGetObjectRemoteServerApi>(std::move(req));

    auto rc = remoteService.GetObjectRemote(serverApi);

    EXPECT_EQ(rc.GetCode(), K_URMA_WORKER_UNAVAILABLE);
    EXPECT_EQ(serverApi->ReadCount(), 1);
    EXPECT_EQ(serverApi->WriteCount(), 0);
    EXPECT_EQ(serverApi->PayloadSendCount(), 0);
    EXPECT_TRUE(serverApi->Payload().empty());
}

TEST_F(WorkerOcRemoteGetAdmissionTest, BatchAdmissionPinsRequestToTcpBeforeLaneAndGather)
{
    constexpr uint64_t dataSize = 16;
    constexpr size_t objectCount = 3;
    const HostPort requester("192.0.2.32", 18481);
    SetUnavailableSummary(impl_->GetUbAdmission(), requester);
    BatchGetObjectRemoteReqPb req;
    req.set_allow_aggregate_gather(true);
    req.set_aggregate_gather_metadata_size(impl_->GetMetadataSize());
    for (size_t i = 0; i < objectCount; ++i) {
        const auto objectKey = "blocked-batch-remote-get-" + std::to_string(i);
        AddTransferableObject(objectKey, dataSize);
        req.add_requests()->CopyFrom(
            MakeUrmaRemoteGetRequest(objectKey, dataSize, requester,
                                     impl_->GetMetadataSize() + i * dataSize));
    }
    const bool savedFallback = FLAGS_enable_transport_fallback;
    const int32_t savedParallelMin = FLAGS_oc_worker_worker_parallel_min;
    Raii restoreFlags([savedFallback, savedParallelMin] {
        FLAGS_enable_transport_fallback = savedFallback;
        FLAGS_oc_worker_worker_parallel_min = savedParallelMin;
    });
    FLAGS_enable_transport_fallback = true;
    FLAGS_oc_worker_worker_parallel_min = 0;
    BINEXPECT_CALL(&datasystem::IsUrmaEnabled, ()).WillRepeatedly(Return(true));
    BINEXPECT_CALL(&datasystem::AcquireUrmaSendLane, (_, _)).Times(0);
    BINEXPECT_CALL(&datasystem::UrmaWritePayload, (_, _, _, _, _, _, _, _, _, _, _, _, _, _)).Times(0);
    BINEXPECT_CALL(&datasystem::UrmaWritePayloadWithLane, (_, _, _, _, _, _, _, _, _, _, _, _, _, _, _)).Times(0);
    BINEXPECT_CALL(&datasystem::UrmaGatherWrite, (_, _, _, _, _)).Times(0);
    BINEXPECT_CALL(&datasystem::UrmaGatherWriteWithLane, (_, _, _, _, _, _)).Times(0);
    WorkerWorkerOCServiceImpl remoteService(
        impl_, nullptr, topologyRuntime_.Engine()->Membership(), []() { return true; },
        []() { return cluster::ControlBackendObservation{}; });
    WorkerWorkerOCServiceImpl::BatchRh2dContext batchTransportContext;
    BatchGetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    ScopedRequestContext requestContext;

    DS_ASSERT_OK(remoteService.PrepareBatchGetObjectRemoteReq(req, batchTransportContext));
    ASSERT_TRUE(batchTransportContext.IsUrmaTcpFallback());
    DS_ASSERT_OK(remoteService.BatchGetObjectRemoteImpl(req, rsp, payload, batchTransportContext));

    ASSERT_EQ(rsp.responses_size(), static_cast<int>(objectCount));
    EXPECT_EQ(payload.size(), objectCount);
    for (const auto &response : rsp.responses()) {
        EXPECT_EQ(response.error().error_code(), K_OK);
        EXPECT_EQ(response.data_source(), DataTransferSource::DATA_IN_PAYLOAD);
    }
}

TEST_F(WorkerOcServiceImplTest, BatchAdmissionFailsBeforeLaneWhenFallbackDisabled)
{
    const HostPort requester("192.0.2.33", 18481);
    SetUnavailableSummary(impl_->GetUbAdmission(), requester);
    BatchGetObjectRemoteReqPb req;
    req.add_requests()->CopyFrom(MakeUrmaRemoteGetRequest("blocked-batch-no-fallback", 16, requester));
    const bool savedFallback = FLAGS_enable_transport_fallback;
    Raii restoreFallback([savedFallback] { FLAGS_enable_transport_fallback = savedFallback; });
    FLAGS_enable_transport_fallback = false;
    BINEXPECT_CALL(&datasystem::IsUrmaEnabled, ()).WillRepeatedly(Return(true));
    BINEXPECT_CALL(&datasystem::AcquireUrmaSendLane, (_, _)).Times(0);
    WorkerWorkerOCServiceImpl remoteService(
        impl_, nullptr, topologyRuntime_.Engine()->Membership(), []() { return true; },
        []() { return cluster::ControlBackendObservation{}; });
    WorkerWorkerOCServiceImpl::BatchRh2dContext batchTransportContext;

    auto rc = remoteService.PrepareBatchGetObjectRemoteReq(req, batchTransportContext);

    EXPECT_EQ(rc.GetCode(), K_URMA_WORKER_UNAVAILABLE);
}

class TestWorkerOcServiceCrudCommonApi : public WorkerOcServiceCrudCommonApi {
public:
    using WorkerOcServiceCrudCommonApi::TranslateQualifiedMetadataDeadline;
    using WorkerOcServiceCrudCommonApi::ValidateRollbackUnackResponse;
    using WorkerOcServiceCrudCommonApi::WorkerOcServiceCrudCommonApi;
};

TEST_F(WorkerOcServiceImplTest, EmptyRollbackResponseIsOnlyUnsupportedCompatibilityCase)
{
    master::RemoveMetaRspPb emptyResponse;
    master::RemoveMetaRspPb explicitResponse;
    explicitResponse.add_failed_ids("object-key");

    EXPECT_EQ(TestWorkerOcServiceCrudCommonApi::ValidateRollbackUnackResponse(
                  master::RemoveMetaReqPb::ROLLBACK_UNACK, emptyResponse)
                  .GetCode(),
              K_NOT_SUPPORTED);
    DS_EXPECT_OK(TestWorkerOcServiceCrudCommonApi::ValidateRollbackUnackResponse(
        master::RemoveMetaReqPb::NORMAL, emptyResponse));
    DS_EXPECT_OK(TestWorkerOcServiceCrudCommonApi::ValidateRollbackUnackResponse(
        master::RemoveMetaReqPb::ROLLBACK_UNACK, explicitResponse));
}

TEST_F(WorkerOcServiceImplTest, MetadataDeadlineTriggersRefreshStatusOnlyAfterFailureQualification)
{
    const HostPort masterAddress("127.0.0.1", 18482);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    bool qualified = false;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        nullptr, nullptr, nullptr, nullptr,
        [&qualified, &masterAddress](const HostPort &target) { return qualified && target == masterAddress; });
    TestWorkerOcServiceCrudCommonApi common(param);
    const Status deadline(K_RPC_DEADLINE_EXCEEDED, "metadata timeout");

    EXPECT_EQ(common.TranslateQualifiedMetadataDeadline(api, deadline, true).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    qualified = true;
    EXPECT_EQ(common.TranslateQualifiedMetadataDeadline(api, deadline, false).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(common.TranslateQualifiedMetadataDeadline(api, deadline, true).GetCode(), K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_EQ(common.TranslateQualifiedMetadataDeadline(api, Status::OK(), true).GetCode(), K_OK);
}

TEST_F(WorkerOcServiceImplTest, CreateMetadataMapsDispatchedOwnerPeerDead)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const std::string objectKey = "create-metadata-peer-dead";
    placement_.SetOwner(objectKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetCreateMetaHandler([](master::CreateMetaReqPb &, master::CreateMetaRspPb &) {
        return Status(K_RPC_PEER_DEAD, "metadata owner peer dead").WithExtra(K_METADATA_FAILURE_TEST_EXTRA);
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    auto publish = MakePublishProcessor(apiManager);
    auto safeObj = MakeMetadataFailureObject(ObjectLifeState::OBJECT_INVALID);
    ObjectKV objectKV(objectKey, *safeObj);
    const std::vector<std::string> nestedKeys;
    const WorkerOcServicePublishImpl::PublishParams params{
        ObjectLifeState::OBJECT_PUBLISHED, nestedKeys, false, 0, ExistenceOptPb::NONE, CacheType::MEMORY
    };

    auto rc = publish->RequestingToMasterCore(objectKV, params);

    EXPECT_EQ(rc.GetCode(), K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_EQ(rc.GetExtra(), K_METADATA_FAILURE_TEST_EXTRA);
    EXPECT_EQ(api->CreateMetaCallCount(), 1);
}

TEST_F(WorkerOcServiceImplTest, CreateMetadataMapsUndispatchedOwnerPeerDead)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const std::string objectKey = "create-metadata-route-peer-dead";
    placement_.SetOwner(objectKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    apiManager->SetLookupStatus(
        Status(K_RPC_PEER_DEAD, "metadata route peer dead").WithExtra(K_METADATA_FAILURE_TEST_EXTRA));
    auto publish = MakePublishProcessor(apiManager);
    auto safeObj = MakeMetadataFailureObject(ObjectLifeState::OBJECT_INVALID);
    ObjectKV objectKV(objectKey, *safeObj);
    const std::vector<std::string> nestedKeys;
    const WorkerOcServicePublishImpl::PublishParams params{
        ObjectLifeState::OBJECT_PUBLISHED, nestedKeys, false, 0, ExistenceOptPb::NONE, CacheType::MEMORY
    };

    auto rc = publish->RequestingToMasterCore(objectKV, params);

    EXPECT_EQ(rc.GetCode(), K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_EQ(rc.GetExtra(), K_METADATA_FAILURE_TEST_EXTRA);
    EXPECT_EQ(api->CreateMetaCallCount(), 0);
}

TEST_F(WorkerOcServiceImplTest, MultiPublishCreateMultiMetaFollowsRedirectToTarget)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort targetAddress("127.0.0.1", 18483);
    const std::string objectKey = "scale-out-absent-key";
    auto sourceApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    sourceApi->SetCreateMultiMetaHandler([&targetAddress, &objectKey](master::CreateMultiMetaReqPb &request,
                                                                      master::CreateMultiMetaRspPb &response) {
        EXPECT_TRUE(request.redirect());
        auto *info = response.add_info();
        info->set_redirect_meta_address(targetAddress.ToString());
        info->add_change_meta_ids(objectKey);
        return Status::OK();
    });
    auto targetApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    targetApi->SetCreateMultiMetaHandler([&objectKey](master::CreateMultiMetaReqPb &request,
                                                      master::CreateMultiMetaRspPb &response) {
        EXPECT_FALSE(request.redirect());
        EXPECT_EQ(request.metas_size(), 1);
        EXPECT_EQ(request.metas(0).object_key(), objectKey);
        response.set_version(42);
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(targetAddress, targetApi);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    auto memCpyThreadPool = std::make_shared<ThreadPool>(1);
    auto notifyThreadPool = std::make_shared<ThreadPool>(1);
    WorkerOcServiceMultiPublishImpl multiPublish(param, memCpyThreadPool, notifyThreadPool,
                                                 std::make_shared<AkSkManager>(0), localAddress_);
    master::CreateMultiMetaReqPb request;
    request.set_redirect(true);
    request.add_metas()->set_object_key(objectKey);
    master::CreateMultiMetaRspPb response;
    std::unordered_map<std::string, uint64_t> versionsByKey;

    DS_ASSERT_OK(multiPublish.RetryCreateMultiMetaForTest(sourceApi, request, response, versionsByKey));

    EXPECT_TRUE(response.failed_object_keys().empty());
    EXPECT_TRUE(response.existing_object_keys().empty());
    EXPECT_EQ(versionsByKey.at(objectKey), 42U);
    EXPECT_EQ(sourceApi->CreateMultiMetaRequests().size(), 1U);
    EXPECT_EQ(targetApi->CreateMultiMetaRequests().size(), 1U);
}

TEST_F(WorkerOcServiceImplTest, MultiPublishQualifiedMetadataDeadlineRequestsRingRefresh)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    auto sourceApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    sourceApi->SetCreateMultiMetaHandler([](master::CreateMultiMetaReqPb &, master::CreateMultiMetaRspPb &) {
        return Status(K_RPC_DEADLINE_EXCEEDED, "metadata timeout");
    });
    size_t observedFailures = 0;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        nullptr, nullptr, nullptr,
        [&observedFailures](const HostPort &, const Status &status) {
            if (status.GetCode() == K_RPC_DEADLINE_EXCEEDED) {
                ++observedFailures;
            }
        },
        [](const HostPort &) { return true; });
    auto memCpyThreadPool = std::make_shared<ThreadPool>(1);
    auto notifyThreadPool = std::make_shared<ThreadPool>(1);
    WorkerOcServiceMultiPublishImpl multiPublish(param, memCpyThreadPool, notifyThreadPool,
                                                 std::make_shared<AkSkManager>(0), localAddress_);
    master::CreateMultiMetaReqPb request;
    request.set_redirect(true);
    request.add_metas()->set_object_key("qualified-deadline-key");
    master::CreateMultiMetaRspPb response;
    std::unordered_map<std::string, uint64_t> versionsByKey;

    const auto status = multiPublish.RetryCreateMultiMetaForTest(sourceApi, request, response, versionsByKey);

    EXPECT_EQ(observedFailures, 1U);
    EXPECT_EQ(status.GetCode(), K_METADATA_OWNER_UNAVAILABLE);
}

TEST_F(WorkerOcServiceImplTest, MultiPublishCreateMultiMetaPartitionsLocalAndMultipleRedirectTargets)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort targetAAddress("127.0.0.1", 18483);
    const HostPort targetBAddress("127.0.0.1", 18484);
    const std::string localKey = "scale-out-local-key";
    const std::string targetAKey = "scale-out-target-a-key";
    const std::string targetBKey = "scale-out-target-b-key";
    const std::string existingKey = "scale-out-target-b-existing-key";
    auto sourceApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    sourceApi->SetCreateMultiMetaHandler([&](master::CreateMultiMetaReqPb &request,
                                             master::CreateMultiMetaRspPb &response) {
        if (!request.redirect()) {
            EXPECT_EQ(request.metas_size(), 1);
            EXPECT_EQ(request.metas(0).object_key(), localKey);
            response.set_version(100);
            return Status::OK();
        }
        auto *targetAInfo = response.add_info();
        targetAInfo->set_redirect_meta_address(targetAAddress.ToString());
        targetAInfo->add_change_meta_ids(targetAKey);
        auto *targetBInfo = response.add_info();
        targetBInfo->set_redirect_meta_address(targetBAddress.ToString());
        targetBInfo->add_change_meta_ids(targetBKey);
        targetBInfo->add_change_meta_ids(existingKey);
        return Status::OK();
    });
    auto targetAApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    targetAApi->SetCreateMultiMetaHandler([&](master::CreateMultiMetaReqPb &request,
                                              master::CreateMultiMetaRspPb &response) {
        EXPECT_FALSE(request.redirect());
        EXPECT_EQ(request.metas_size(), 1);
        EXPECT_EQ(request.metas(0).object_key(), targetAKey);
        response.set_version(101);
        return Status::OK();
    });
    auto targetBApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    targetBApi->SetCreateMultiMetaHandler([&](master::CreateMultiMetaReqPb &request,
                                              master::CreateMultiMetaRspPb &response) {
        EXPECT_FALSE(request.redirect());
        EXPECT_EQ(request.metas_size(), 2);
        EXPECT_EQ(request.metas(0).object_key(), targetBKey);
        EXPECT_EQ(request.metas(1).object_key(), existingKey);
        response.set_version(102);
        response.add_existing_object_keys(existingKey);
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(targetAAddress, targetAApi);
    apiManager->SetApi(targetBAddress, targetBApi);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    auto memCpyThreadPool = std::make_shared<ThreadPool>(1);
    auto notifyThreadPool = std::make_shared<ThreadPool>(1);
    WorkerOcServiceMultiPublishImpl multiPublish(param, memCpyThreadPool, notifyThreadPool,
                                                 std::make_shared<AkSkManager>(0), localAddress_);
    master::CreateMultiMetaReqPb request;
    request.set_redirect(true);
    request.add_metas()->set_object_key(localKey);
    request.add_metas()->set_object_key(targetAKey);
    request.add_metas()->set_object_key(targetBKey);
    request.add_metas()->set_object_key(existingKey);
    master::CreateMultiMetaRspPb response;
    std::unordered_map<std::string, uint64_t> versionsByKey;

    DS_ASSERT_OK(multiPublish.RetryCreateMultiMetaForTest(sourceApi, request, response, versionsByKey));

    ASSERT_EQ(sourceApi->CreateMultiMetaRequests().size(), 2U);
    EXPECT_TRUE(sourceApi->CreateMultiMetaRequests()[0].redirect());
    EXPECT_FALSE(sourceApi->CreateMultiMetaRequests()[1].redirect());
    EXPECT_EQ(targetAApi->CreateMultiMetaRequests().size(), 1U);
    EXPECT_EQ(targetBApi->CreateMultiMetaRequests().size(), 1U);
    ASSERT_EQ(response.existing_object_keys_size(), 1);
    EXPECT_EQ(response.existing_object_keys(0), existingKey);
    EXPECT_EQ(versionsByKey.at(localKey), 100U);
    EXPECT_EQ(versionsByKey.at(targetAKey), 101U);
    EXPECT_EQ(versionsByKey.at(targetBKey), 102U);
    EXPECT_EQ(versionsByKey.count(existingKey), 0U);
}

TEST_F(WorkerOcServiceImplTest, MultiPublishCreateMultiMetaRetriesTargetWhenMoving)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort targetAddress("127.0.0.1", 18483);
    const std::string objectKey = "scale-out-target-moving-key";
    auto sourceApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    sourceApi->SetCreateMultiMetaHandler([&targetAddress, &objectKey](master::CreateMultiMetaReqPb &,
                                                                      master::CreateMultiMetaRspPb &response) {
        auto *info = response.add_info();
        info->set_redirect_meta_address(targetAddress.ToString());
        info->add_change_meta_ids(objectKey);
        return Status::OK();
    });
    auto targetApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    int targetCallCount = 0;
    targetApi->SetCreateMultiMetaHandler([&targetCallCount](master::CreateMultiMetaReqPb &request,
                                                            master::CreateMultiMetaRspPb &response) {
        EXPECT_FALSE(request.redirect());
        if (++targetCallCount == 1) {
            response.set_meta_is_moving(true);
            return Status::OK();
        }
        response.set_version(44);
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(targetAddress, targetApi);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    auto memCpyThreadPool = std::make_shared<ThreadPool>(1);
    auto notifyThreadPool = std::make_shared<ThreadPool>(1);
    WorkerOcServiceMultiPublishImpl multiPublish(param, memCpyThreadPool, notifyThreadPool,
                                                 std::make_shared<AkSkManager>(0), localAddress_);
    master::CreateMultiMetaReqPb request;
    request.set_redirect(true);
    request.add_metas()->set_object_key(objectKey);
    master::CreateMultiMetaRspPb response;
    std::unordered_map<std::string, uint64_t> versionsByKey;

    DS_ASSERT_OK(multiPublish.RetryCreateMultiMetaForTest(sourceApi, request, response, versionsByKey));

    EXPECT_EQ(sourceApi->CreateMultiMetaRequests().size(), 1U);
    EXPECT_EQ(targetApi->CreateMultiMetaRequests().size(), 2U);
    EXPECT_EQ(versionsByKey.at(objectKey), 44U);
}

TEST_F(WorkerOcServiceImplTest, MultiPublishCreateMultiMetaRetriesWholeBatchWhenMoving)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const std::string firstKey = "scale-out-moving-first";
    const std::string secondKey = "scale-out-moving-second";
    auto sourceApi = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    int callCount = 0;
    sourceApi->SetCreateMultiMetaHandler([&callCount](master::CreateMultiMetaReqPb &request,
                                                      master::CreateMultiMetaRspPb &response) {
        ++callCount;
        if (callCount == 1) {
            response.set_meta_is_moving(true);
            return Status::OK();
        }
        EXPECT_TRUE(request.redirect());
        response.set_version(43);
        return Status::OK();
    });
    WorkerOcServiceCrudParam param = MakeCrudParam();
    auto memCpyThreadPool = std::make_shared<ThreadPool>(1);
    auto notifyThreadPool = std::make_shared<ThreadPool>(1);
    WorkerOcServiceMultiPublishImpl multiPublish(param, memCpyThreadPool, notifyThreadPool,
                                                 std::make_shared<AkSkManager>(0), localAddress_);
    master::CreateMultiMetaReqPb request;
    request.set_redirect(true);
    request.add_metas()->set_object_key(firstKey);
    request.add_metas()->set_object_key(secondKey);
    master::CreateMultiMetaRspPb response;
    std::unordered_map<std::string, uint64_t> versionsByKey;

    DS_ASSERT_OK(multiPublish.RetryCreateMultiMetaForTest(sourceApi, request, response, versionsByKey));

    EXPECT_EQ(sourceApi->CreateMultiMetaRequests().size(), 2U);
    EXPECT_EQ(sourceApi->CreateMultiMetaRequests()[1].metas_size(), 2);
    EXPECT_TRUE(versionsByKey.empty());
}

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

TEST_F(WorkerOcServiceImplTest, SyncDeleteNotificationPreservesNewerObjectVersion)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(1'000);
    const std::string objectKey = "sync-delete-version-fence";
    constexpr uint64_t olderDeleteVersion = 100;
    constexpr uint64_t newerObjectVersion = 200;
    AddObject(objectKey, newerObjectVersion);

    DeleteObjectReqPb request;
    DeleteObjectRspPb response;
    request.add_object_keys(objectKey);
    request.add_versions(olderDeleteVersion);
    request.set_is_async(false);

    DS_ASSERT_OK(deleteProc_->DeleteCopyNotification(request, response));
    EXPECT_TRUE(response.failed_object_keys().empty());
    std::shared_ptr<SafeObjType> entry;
    DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
    EXPECT_EQ((*entry)->GetCreateTime(), newerObjectVersion);
}

TEST_F(WorkerOcServiceImplTest, DeleteNotificationRejectsOversizedBatch)
{
    DeleteObjectReqPb request;
    DeleteObjectRspPb response;
    constexpr size_t oversizedBatch = 10'001;
    for (size_t i = 0; i < oversizedBatch; ++i) {
        request.add_object_keys(std::to_string(i));
        request.add_versions(i);
    }

    EXPECT_EQ(deleteProc_->DeleteCopyNotification(request, response).GetCode(), StatusCode::K_INVALID);
}

TEST_F(WorkerOcServiceImplTest, CleanupLocalStateForRejoinClearsLocalObjects)
{
    InitImplClearDataFlow();
    AddObject("rejoin-object-1");
    AddObject("rejoin-object-2");

    DS_ASSERT_OK(impl_->CleanupLocalStateForRejoin(std::chrono::steady_clock::now() + std::chrono::seconds(1)));

    EXPECT_EQ(objectTable_->GetSize(), 0);
}

TEST_F(WorkerOcServiceImplTest, CleanupLocalStateForRejoinRespectsExpiredDeadline)
{
    InitImplClearDataFlow();
    AddObject("rejoin-deadline-object");

    const auto rc = impl_->CleanupLocalStateForRejoin(std::chrono::steady_clock::now() - std::chrono::milliseconds(1));

    EXPECT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(objectTable_->GetSize(), 1);
}

TEST_F(WorkerOcServiceImplTest, CleanupLocalStateForRejoinDoesNotRebuildRefs)
{
    InitImplClearDataFlow();
    AddObject("rejoin-ref-object");
    AddWorkerRef("rejoin-ref-object");
    bool recoverMasterAppRefCalled = false;
    RecoverMasterAppRefEvent::GetInstance().AddSubscriber(
        kRecoverMasterAppRefSubscriber,
        [&recoverMasterAppRefCalled](const std::function<bool(const std::string &)> &, const std::string &) {
            recoverMasterAppRefCalled = true;
            return Status::OK();
        });

    DS_ASSERT_OK(impl_->CleanupLocalStateForRejoin(std::chrono::steady_clock::now() + std::chrono::seconds(1)));

    EXPECT_FALSE(recoverMasterAppRefCalled);
}

TEST_F(WorkerOcServiceImplTest, CleanupLocalStateForRejoinStopsWhenMetadataCleanupFails)
{
    InitImplClearDataFlow();
    AddObject("rejoin-metadata-failure-object");
    bool metadataCleanupCalled = false;
    impl_->RegisterLocalMetadataCleanupForRejoin([&metadataCleanupCalled] {
        metadataCleanupCalled = true;
        return Status(K_RUNTIME_ERROR, "metadata cleanup failed");
    });

    const auto rc = impl_->CleanupLocalStateForRejoin(std::chrono::steady_clock::now() + std::chrono::seconds(1));

    EXPECT_TRUE(metadataCleanupCalled);
    EXPECT_EQ(rc.GetCode(), StatusCode::K_RUNTIME_ERROR);
    EXPECT_EQ(objectTable_->GetSize(), 1);
}

TEST_F(WorkerOcServiceImplTest, CleanupLocalStateForRejoinWaitsForOrdinaryRpcDrain)
{
    InitImplClearDataFlow();
    AddObject("rejoin-drain-object");
    BthreadReadGuard inFlightRequest(&impl_->reconFlag_);

    const auto rc = impl_->CleanupLocalStateForRejoin(std::chrono::steady_clock::now() + std::chrono::milliseconds(20));

    EXPECT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(objectTable_->GetSize(), 1);
}

TEST_F(WorkerOcServiceImplTest, ReconciliationReturnsNotReadyWhenRejoinCleanupHoldsReconFlag)
{
    SetUnhealthy();
    BthreadWriteGuard cleanup(&impl_->reconFlag_);
    PushMetaToWorkerReqPb req;
    req.set_event_timestamp(1);

    const auto start = std::chrono::steady_clock::now();
    const auto rc = impl_->Reconciliation(req);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_LT(elapsed.count(), K_REJOIN_RECONCILIATION_LOCK_UPPER_BOUND_MS);
}

TEST_F(WorkerOcServiceImplTest, RestartReconciliationWaitsForRejoinCleanupReconFlag)
{
    SetUnhealthy();
    const bool savedReconciliation = FLAGS_enable_reconciliation;
    FLAGS_enable_reconciliation = true;
    Raii restoreReconciliation([savedReconciliation] { FLAGS_enable_reconciliation = savedReconciliation; });
    BthreadWriteGuard cleanup(&impl_->reconFlag_);
    PushMetaToWorkerReqPb req;
    req.set_event_timestamp(1);
    req.set_is_restart(true);
    constexpr const char *skipRestartWait = "WorkerOCServiceImpl.Reconciliation.SkipWait";
    DS_ASSERT_OK(inject::Set(skipRestartWait, "1*call()"));
    Raii clearInject([&] { (void)inject::Clear(skipRestartWait); });

    auto releaseCleanup = std::async(std::launch::async, [&cleanup] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        cleanup.UnlockIfLocked();
    });
    const auto start = std::chrono::steady_clock::now();
    const auto rc = impl_->Reconciliation(req);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    releaseCleanup.wait();

    EXPECT_NE(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_GE(elapsed.count(), 40);
    EXPECT_LT(elapsed.count(), K_REJOIN_RECONCILIATION_LOCK_UPPER_BOUND_MS);
}

TEST_F(WorkerOcServiceImplTest, HealthRemainsClosedWhileTopologyIsPending)
{
    SetUnhealthy();
    Raii restoreHealth([] { SetUnhealthy(); });

    const auto start = std::chrono::steady_clock::now();
    DS_EXPECT_OK(impl_->WhetherNonRestart());
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    EXPECT_FALSE(IsHealthy());
    EXPECT_FALSE(impl_->setHealthFile_.load(std::memory_order_acquire));
    // This path used to poll for up to 60 seconds. Keep the assertion loose enough for loaded CI hosts while proving
    // that startup merely records a pending topology gate instead of sleeping in the service method.
    EXPECT_LT(elapsed.count(), 1'000);

    placement_.SetOwner("topology-readiness-probe", localAddress_);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    DS_EXPECT_OK(impl_->RefreshStartupHealth());
    EXPECT_TRUE(IsHealthy());
    EXPECT_TRUE(impl_->setHealthFile_.load(std::memory_order_acquire));
}

TEST_F(WorkerOcServiceImplTest, ReconciliationAndTopologyJointlyGateHealthPublication)
{
    SetUnhealthy();
    Raii restoreHealth([] { SetUnhealthy(); });
    placement_.SetOwner("topology-readiness-probe", localAddress_);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    impl_->reconciliationReady_.store(true, std::memory_order_release);

    DS_EXPECT_OK(impl_->RefreshStartupHealth());
    EXPECT_FALSE(IsHealthy());
    EXPECT_FALSE(impl_->setHealthFile_.load(std::memory_order_acquire));

    impl_->healthPublicationEnabled_.store(true, std::memory_order_release);
    impl_->reconciliationReady_.store(false, std::memory_order_release);

    DS_EXPECT_OK(impl_->RefreshStartupHealth());
    EXPECT_FALSE(IsHealthy());
    EXPECT_FALSE(impl_->setHealthFile_.load(std::memory_order_acquire));

    impl_->reconciliationReady_.store(true, std::memory_order_release);
    DS_EXPECT_OK(impl_->RefreshStartupHealth());
    EXPECT_TRUE(IsHealthy());
    EXPECT_TRUE(impl_->setHealthFile_.load(std::memory_order_acquire));
}

TEST_F(WorkerOcServiceImplTest, ConcurrentTopologyNotificationsPublishHealthOnce)
{
    SetUnhealthy();
    Raii restoreHealth([] { SetUnhealthy(); });
    impl_->healthPublicationEnabled_.store(true, std::memory_order_release);
    placement_.SetOwner("topology-readiness-probe", localAddress_);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    impl_->reconciliationReady_.store(true, std::memory_order_release);

    constexpr size_t notificationCount = 8;
    std::atomic<size_t> publicationCount{ 0 };
    std::promise<void> publicationEntered;
    auto publicationEnteredFuture = publicationEntered.get_future();
    std::promise<void> releasePublication;
    auto releasePublicationFuture = releasePublication.get_future().share();
    impl_->healthPublisher_ = [&] {
        if (publicationCount.fetch_add(1, std::memory_order_relaxed) == 0) {
            publicationEntered.set_value();
        }
        releasePublicationFuture.wait();
        return SetHealthProbe();
    };
    std::vector<std::future<Status>> notifications;
    notifications.reserve(notificationCount);
    for (size_t index = 0; index < notificationCount; ++index) {
        notifications.emplace_back(
            std::async(std::launch::async, [this] { return impl_->RefreshStartupHealth(); }));
    }
    constexpr auto publicationEntryTimeout = std::chrono::seconds(1);
    EXPECT_EQ(publicationEnteredFuture.wait_for(publicationEntryTimeout), std::future_status::ready);
    releasePublication.set_value();
    for (auto &notification : notifications) {
        DS_EXPECT_OK(notification.get());
    }

    EXPECT_EQ(publicationCount.load(std::memory_order_relaxed), 1UL);
    EXPECT_TRUE(IsHealthy());
    EXPECT_TRUE(impl_->setHealthFile_.load(std::memory_order_acquire));
}

TEST_F(WorkerOcServiceImplTest, FailedHealthPublicationRollsBackAndCanRetry)
{
    SetUnhealthy();
    const auto savedHealthCheckPath = FLAGS_health_check_path;
    const auto testHealthCheckPath = GetTestCaseDataDir() + "/issue873-health-probe";
    Raii restoreHealth([savedHealthCheckPath, testHealthCheckPath] {
        (void)DeleteFile(testHealthCheckPath);
        FLAGS_health_check_path = savedHealthCheckPath;
        SetUnhealthy();
    });
    impl_->healthPublicationEnabled_.store(true, std::memory_order_release);
    impl_->reconciliationReady_.store(true, std::memory_order_release);
    placement_.SetOwner("topology-readiness-probe", localAddress_);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));

    FLAGS_health_check_path = "/proc/datasystem-issue873-health-probe";
    EXPECT_TRUE(SetHealthProbe().IsError());
    EXPECT_FALSE(IsHealthy());
    EXPECT_FALSE(FileExist(FLAGS_health_check_path));

    FLAGS_health_check_path = testHealthCheckPath;
    (void)DeleteFile(FLAGS_health_check_path);
    impl_->healthPublisher_ = [] {
        RETURN_IF_NOT_OK(SetHealthProbe());
        RETURN_STATUS(K_IO_ERROR, "injected failure after health marker publication");
    };
    EXPECT_TRUE(impl_->RefreshStartupHealth().IsError());
    EXPECT_FALSE(IsHealthy());
    EXPECT_FALSE(FileExist(FLAGS_health_check_path));
    EXPECT_FALSE(impl_->setHealthFile_.load(std::memory_order_acquire));

    impl_->healthPublisher_ = [] { return SetHealthProbe(); };
    DS_EXPECT_OK(impl_->RefreshStartupHealth());
    EXPECT_TRUE(IsHealthy());
    EXPECT_TRUE(FileExist(FLAGS_health_check_path));
    EXPECT_TRUE(impl_->setHealthFile_.load(std::memory_order_acquire));
}

TEST_F(WorkerOcServiceImplTest, RevokeHealthProbeReportsUnlinkFailureAndRetries)
{
    SetUnhealthy();
    const auto savedHealthCheckPath = FLAGS_health_check_path;
    const auto testDir = GetTestCaseDataDir() + "/issue873-revoke-probe";
    const auto testHealthCheckPath = testDir + "/healthy";
    DS_ASSERT_OK(CreateDir(testDir, true, 0700));
    Raii restore([savedHealthCheckPath, testDir] {
        (void)chmod(testDir.c_str(), 0700);
        FLAGS_health_check_path = savedHealthCheckPath;
        SetUnhealthy();
    });
    FLAGS_health_check_path = testHealthCheckPath;
    DS_ASSERT_OK(SetHealthProbe());
    ASSERT_TRUE(FileExist(testHealthCheckPath));

    ASSERT_EQ(chmod(testDir.c_str(), 0200), 0);
    EXPECT_TRUE(RevokeHealthProbe().IsError());
    EXPECT_FALSE(IsHealthy());

    ASSERT_EQ(chmod(testDir.c_str(), 0700), 0);
    EXPECT_TRUE(FileExist(testHealthCheckPath));
    DS_EXPECT_OK(RevokeHealthProbe());
    EXPECT_FALSE(FileExist(testHealthCheckPath));
    DS_EXPECT_OK(RevokeHealthProbe());
}

TEST_F(WorkerOcServiceImplTest, TopologyWatchIsolationRevokesPublishedHealth)
{
    SetUnhealthy();
    const auto savedHealthCheckPath = FLAGS_health_check_path;
    const auto testHealthCheckPath = GetTestCaseDataDir() + "/issue873-runtime-health-probe";
    Raii restore([savedHealthCheckPath, testHealthCheckPath] {
        (void)DeleteFile(testHealthCheckPath);
        FLAGS_health_check_path = savedHealthCheckPath;
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    FLAGS_health_check_path = testHealthCheckPath;
    (void)DeleteFile(testHealthCheckPath);

    std::shared_ptr<WorkerOCServiceImpl> watchedImpl;
    ObjectTopologyTestRuntime watchedRuntime;
    DS_ASSERT_OK(watchedRuntime.Init(localAddress_, [&watchedImpl](cluster::TopologyAvailabilityLevel level) {
        if (watchedImpl == nullptr) {
            return;
        }
        const bool allowBusiness = level == cluster::TopologyAvailabilityLevel::NORMAL
                                   || level == cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED;
        watchedImpl->NotifyTopologyAvailability(allowBusiness);
    }));
    WorkerTestPlacementFacade watchedPlacement;
    worker::MetadataRouteResolver watchedRoute(&watchedPlacement, worker::MetadataRouteOptions{});
    auto watchedObjectTable = std::make_shared<object_cache::ObjectTable>();
    auto watchedEvictionManager = std::make_shared<WorkerOcEvictionManager>(
        watchedObjectTable, localAddress_, localAddress_, watchedRoute, nullptr);
    watchedImpl = std::make_shared<WorkerOCServiceImpl>(
        localAddress_, localAddress_, watchedObjectTable, nullptr, watchedEvictionManager, nullptr, nullptr, nullptr,
        watchedRuntime.Engine(), watchedRoute, watchedRuntime.Engine()->Membership(), &exitRequested_, false, false);
    watchedImpl->healthPublicationEnabled_.store(true, std::memory_order_release);
    watchedImpl->reconciliationReady_.store(true, std::memory_order_release);
    DS_ASSERT_OK(watchedImpl->StartTopologyHealthCoordinator());
    watchedPlacement.SetOwner("topology-readiness-probe", localAddress_);
    DS_ASSERT_OK(watchedRuntime.StartWithActiveLocalMember(localAddress_));

    constexpr auto healthTransitionTimeout = std::chrono::seconds(2);
    ASSERT_TRUE(WaitForCondition(
        [&] { return IsHealthy() && FileExist(testHealthCheckPath); }, healthTransitionTimeout));
    ASSERT_TRUE(watchedImpl->setHealthFile_.load(std::memory_order_acquire));

    // Only the real Coordinator watch drives isolation; no explicit health refresh is made by this test.
    DS_ASSERT_OK(watchedRuntime.TriggerAuthorityConflict(localAddress_));

    EXPECT_TRUE(WaitForCondition(
        [&] {
            return !IsHealthy() && !FileExist(testHealthCheckPath)
                   && !watchedImpl->setHealthFile_.load(std::memory_order_acquire);
        },
        healthTransitionTimeout));
}

TEST_F(WorkerOcServiceImplTest, TopologyHealthRecoveryRevalidatesBeforeRepublish)
{
    SetUnhealthy();
    const auto savedHealthCheckPath = FLAGS_health_check_path;
    const auto testHealthCheckPath = GetTestCaseDataDir() + "/issue873-recovery-health-probe";
    Raii restore([savedHealthCheckPath, testHealthCheckPath] {
        (void)DeleteFile(testHealthCheckPath);
        FLAGS_health_check_path = savedHealthCheckPath;
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    FLAGS_health_check_path = testHealthCheckPath;
    (void)DeleteFile(testHealthCheckPath);
    impl_->healthPublicationEnabled_.store(true, std::memory_order_release);
    impl_->reconciliationReady_.store(true, std::memory_order_release);
    DS_ASSERT_OK(impl_->StartTopologyHealthCoordinator());

    impl_->NotifyTopologyAvailability(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(IsHealthy());
    EXPECT_FALSE(FileExist(testHealthCheckPath));

    placement_.SetOwner("topology-readiness-probe", localAddress_);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    constexpr auto healthTransitionTimeout = std::chrono::seconds(2);
    ASSERT_TRUE(WaitForCondition(
        [&] { return IsHealthy() && FileExist(testHealthCheckPath); }, healthTransitionTimeout));

    impl_->NotifyTopologyAvailability(false);
    ASSERT_TRUE(WaitForCondition(
        [&] { return !IsHealthy() && !FileExist(testHealthCheckPath); }, healthTransitionTimeout));

    impl_->NotifyTopologyAvailability(true);
    EXPECT_TRUE(WaitForCondition(
        [&] { return IsHealthy() && FileExist(testHealthCheckPath); }, healthTransitionTimeout));
}

TEST_F(WorkerOcServiceImplTest, ServingNotificationDoesNotClosePublishedHealth)
{
    const auto savedHealthCheckPath = FLAGS_health_check_path;
    Raii restore([savedHealthCheckPath] {
        FLAGS_health_check_path = savedHealthCheckPath;
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    FLAGS_health_check_path.clear();
    SetTopologyServingAdmission(true);
    DS_ASSERT_OK(SetHealthProbe());
    ASSERT_TRUE(IsHealthy());
    DS_ASSERT_OK(impl_->StartTopologyHealthCoordinator());

    // A NORMAL callback can race with the startup thread immediately after it publishes health. It requests an
    // asynchronous revalidation, but must not create a transient K_NOT_READY window for an already serving Worker.
    impl_->NotifyTopologyAvailability(true);
    uint64_t expectedGeneration;
    {
        std::lock_guard<std::mutex> lock(impl_->topologyHealthMutex_);
        expectedGeneration = impl_->topologyHealthGeneration_;
    }

    ASSERT_TRUE(WaitForCondition(
        [this, expectedGeneration] {
            std::lock_guard<std::mutex> lock(impl_->topologyHealthMutex_);
            return impl_->topologyHealthProcessedGeneration_ >= expectedGeneration;
        },
        std::chrono::milliseconds(200)));
    EXPECT_TRUE(IsHealthy());
    impl_->StopTopologyHealthCoordinator();

    impl_->NotifyTopologyAvailability(false);
    ASSERT_FALSE(IsHealthy());
    impl_->NotifyTopologyAvailability(true);
    EXPECT_FALSE(IsHealthy());
}

TEST_F(WorkerOcServiceImplTest, GiveUpReconciliationDoesNotBypassTopologyHealthGate)
{
    SetUnhealthy();
    const bool savedDistributedMaster = FLAGS_enable_distributed_master;
    const bool savedReconciliation = FLAGS_enable_reconciliation;
    Raii restore([savedDistributedMaster, savedReconciliation] {
        FLAGS_enable_distributed_master = savedDistributedMaster;
        FLAGS_enable_reconciliation = savedReconciliation;
        SetUnhealthy();
    });
    FLAGS_enable_distributed_master = true;
    FLAGS_enable_reconciliation = true;
    placement_.SetOwner("topology-readiness-probe", localAddress_);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    ASSERT_TRUE(topologyRuntime_.Engine()->HasEstablishedMemberLease());
    auto restartImpl = std::make_shared<WorkerOCServiceImpl>(
        localAddress_, localAddress_, objectTable_, nullptr, evictionManager_, nullptr, nullptr, nullptr,
        topologyRuntime_.Engine(), metadataRoute_, topologyRuntime_.Engine()->Membership(), &exitRequested_, true,
        true);
    restartImpl->healthPublicationEnabled_.store(true, std::memory_order_release);

    DS_EXPECT_OK(restartImpl->GiveUpReconciliation());

    EXPECT_TRUE(restartImpl->reconciliationReady_.load(std::memory_order_acquire));
    EXPECT_FALSE(restartImpl->setHealthFile_.load(std::memory_order_acquire));
    EXPECT_FALSE(IsHealthy());
}

TEST_F(WorkerOcServiceImplTest, RejoinRequiredRejectsClientFacingRpc)
{
    SetTopologyServingAdmission(false);
    Raii restoreAdmission([] { SetTopologyServingAdmission(true); });

    CreateReqPb req;
    req.set_object_key("rejoin-reject-create");
    req.set_data_size(1);
    CreateRspPb rsp;

    EXPECT_EQ(impl_->Create(req, rsp).GetCode(), StatusCode::K_NOT_READY);
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

TEST_F(WorkerOcServiceImplTest, QueryMetaDataFromMasterReportsMetadataRpcFailure)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort masterAddress("127.0.0.1", 18482);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    api->SetQueryMetaStatus(Status(K_RPC_DEADLINE_EXCEEDED, "query meta timeout"));
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    std::vector<std::pair<std::string, StatusCode>> observations;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        apiManager, nullptr, nullptr,
        [&observations](const HostPort &target, const Status &status) {
            observations.emplace_back(target.ToString(), status.GetCode());
        });
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    master::QueryMetaRspPb rsp;
    std::vector<RpcMessage> payloads;

    auto rc = getImpl.QueryMetaDataFromMasterImpl(masterAddress, K_META_MOVING_RETRY_TIMEOUT_MS, { "obj" }, rsp,
                                                  payloads);

    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(api->QueryMetaCallCount(), 1);
    EXPECT_THAT(observations, ElementsAre(Pair(masterAddress.ToString(), K_RPC_DEADLINE_EXCEEDED)));
}

TEST_F(WorkerOcServiceImplTest, GetLocationCleanupUsesNormalCause)
{
    const std::string objectKey = "object";
    placement_.SetOwner(objectKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    DS_ASSERT_OK(getImpl.RemoveLocation(objectKey, UINT64_MAX));

    auto requests = api->RemoveMetaRequests();
    ASSERT_EQ(requests.size(), 1U);
    EXPECT_EQ(requests.front().cause(), master::RemoveMetaReqPb::NORMAL);
}

TEST_F(WorkerOcServiceImplTest, DeviceRemoteGetReservesCleanupBudgetBeforeRetry)
{
    constexpr uint64_t queryMetaVersion = 61;
    constexpr int64_t retryCleanupWindowMs = 50;
    constexpr int64_t initialTimeoutMs = 1'000;
    constexpr int secondQueryMetaCall = 2;
    constexpr char remainingTimeInject[] = "worker.device_remote_get.remaining_time_ms";
    const bool oldBatchRemoteGet = FLAGS_enable_worker_worker_batch_get;
    Raii restoreBatchRemoteGet([oldBatchRemoteGet]() { FLAGS_enable_worker_worker_batch_get = oldBatchRemoteGet; });
    FLAGS_enable_worker_worker_batch_get = false;
    const std::string finalRetryKey = "device-a-final-retry";
    const std::string secondAttemptFailureKey = "device-z-second-failure";
    const std::string firstAttemptFailureKey = "device-zz-first-failure";
    for (const auto &key : { finalRetryKey, secondAttemptFailureKey, firstAttemptFailureKey }) {
        placement_.SetOwner(key, localAddress_);
    }
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetQueryMetaHandler([&](int callCount, master::QueryMetaRspPb &rsp) {
        AddDeviceRemoteGetMeta(rsp, finalRetryKey, queryMetaVersion, localAddress_, DataFormat::BINARY);
        if (callCount <= secondQueryMetaCall) {
            AddDeviceRemoteGetMeta(rsp, secondAttemptFailureKey, queryMetaVersion, localAddress_,
                                   callCount == secondQueryMetaCall ? DataFormat::LIST : DataFormat::BINARY);
        }
        if (callCount == 1) {
            AddDeviceRemoteGetMeta(rsp, firstAttemptFailureKey, queryMetaVersion, localAddress_, DataFormat::LIST);
        }
        return Status::OK();
    });
    api->SetRemoveMetaHandler([](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
        *response.mutable_success_ids() = { request.ids().begin(), request.ids().end() };
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    impl_->getProc_ = std::make_shared<WorkerOcServiceGetImpl>(param, nullptr, nullptr, nullptr, nullptr, localAddress_,
                                                               nullptr);
    WorkerDeviceOcManager deviceManager(impl_.get());
    GetDeviceObjectReqPb req;
    std::vector<std::string> remoteObjectKeys{ finalRetryKey, secondAttemptFailureKey, firstAttemptFailureKey };
    auto request = std::make_shared<GetDeviceObjectRequest>(remoteObjectKeys, nullptr,
                                                            ClientKey::Intern("device-client"), 0, req);
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(initialTimeoutMs);
    DS_ASSERT_OK(inject::Set(remainingTimeInject,
                             FormatString("1*call(%ld)->1*call(%ld)->1*call(0)", initialTimeoutMs,
                                          retryCleanupWindowMs)));
    Raii clearRemainingTimeInject([&]() { (void)inject::Clear(remainingTimeInject); });

    DS_ASSERT_OK(deviceManager.TryGetDeviceObjectFromRemote(0, request, remoteObjectKeys));

    EXPECT_EQ(api->QueryMetaCallCount(), secondQueryMetaCall);
    auto requests = api->RemoveMetaRequests();
    ASSERT_EQ(requests.size(), 1U);
    EXPECT_EQ(requests.front().cause(), master::RemoveMetaReqPb::ROLLBACK_UNACK);
    EXPECT_THAT(requests.front().ids(),
                UnorderedElementsAre(finalRetryKey, secondAttemptFailureKey, firstAttemptFailureKey));
    ASSERT_EQ(requests.front().id_with_version_size(), static_cast<int>(remoteObjectKeys.size()));
    for (const auto &idWithVersion : requests.front().id_with_version()) {
        EXPECT_EQ(idWithVersion.version(), queryMetaVersion);
    }
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackEmptySingleResponseStopsCleanupRetry)
{
    constexpr uint64_t rollbackVersion = 7;
    const std::string objectKey = "rollback-unack-unsupported-single";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(objectKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked({ { objectKey, rollbackVersion } });

    auto requests = api->RemoveMetaRequests();
    ASSERT_EQ(requests.size(), 1U);
    EXPECT_EQ(requests.front().cause(), master::RemoveMetaReqPb::ROLLBACK_UNACK);
    EXPECT_THAT(requests.front().ids(), ElementsAre(objectKey));
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackEmptyBatchResponseStopsCleanupRetry)
{
    constexpr uint64_t rollbackVersion = 8;
    const std::string firstKey = "rollback-unack-unsupported-batch-first";
    const std::string secondKey = "rollback-unack-unsupported-batch-second";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(firstKey, localAddress_);
    placement_.SetOwner(secondKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked({ { firstKey, rollbackVersion }, { secondKey, rollbackVersion } });

    auto requests = api->RemoveMetaRequests();
    ASSERT_EQ(requests.size(), 1U);
    EXPECT_THAT(requests.front().ids(), UnorderedElementsAre(firstKey, secondKey));
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackRetriesBatchRpcNotSupportedFailure)
{
    constexpr uint64_t rollbackVersion = 16;
    const std::string firstKey = "rollback-unack-rpc-not-supported-batch-first";
    const std::string secondKey = "rollback-unack-rpc-not-supported-batch-second";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(firstKey, localAddress_);
    placement_.SetOwner(secondKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    size_t calls = 0;
    api->SetRemoveMetaHandler([&](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
        if (++calls == 1) {
            return Status(K_NOT_SUPPORTED, "remote batch remove operation is unavailable");
        }
        *response.mutable_success_ids() = { request.ids().begin(), request.ids().end() };
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    auto param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked({ { firstKey, rollbackVersion }, { secondKey, rollbackVersion } });

    EXPECT_EQ(api->RemoveMetaRequests().size(), 2U);
}

TEST_F(WorkerOcServiceImplTest, DeleteObjectsMetaUnackedUsesRollbackCause)
{
    constexpr uint64_t rollbackVersion = 9;
    const std::string objectKey = "delete-unacked-rollback-cause";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(objectKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    api->SetRemoveMetaHandler([](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
        *response.mutable_success_ids() = { request.ids().begin(), request.ids().end() };
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked({ { objectKey, rollbackVersion } });

    auto requests = api->RemoveMetaRequests();
    ASSERT_EQ(requests.size(), 1U);
    EXPECT_EQ(requests.front().cause(), master::RemoveMetaReqPb::ROLLBACK_UNACK);
    ASSERT_EQ(requests.front().id_with_version_size(), 1);
    EXPECT_EQ(requests.front().id_with_version(0).version(), rollbackVersion);
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackEmptyRedirectResponseStopsCleanupRetry)
{
    constexpr uint64_t rollbackVersion = 10;
    const HostPort redirectMaster("127.0.0.1", 18482);
    const std::string objectKey = "redirected-rollback-unack";
    TestDistributedTopology topology(localAddress_, redirectMaster);
    DS_ASSERT_OK(topology.InitStatus());
    auto redirectApi = std::make_shared<FakeWorkerMasterOCApi>(redirectMaster);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(redirectMaster, redirectApi);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    TestWorkerOcServiceCrudCommonApi common(param);
    master::RemoveMetaRspPb response;
    auto *redirect = response.add_info();
    redirect->set_redirect_meta_address(redirectMaster.ToString());
    redirect->add_change_meta_ids(objectKey);
    std::vector<std::string> failedIds;
    std::vector<std::string> needMigrateIds;
    std::vector<std::string> needWaitIds;
    std::vector<std::string> needL2CacheIds;

    DS_ASSERT_OK(common.RemoveMetadataFromRedirectMaster(
        response, master::RemoveMetaReqPb::ROLLBACK_UNACK, localAddress_.ToString(),
        { { objectKey, rollbackVersion } }, failedIds, needMigrateIds, needWaitIds, needL2CacheIds, ""));

    EXPECT_TRUE(failedIds.empty());
    auto requests = redirectApi->RemoveMetaRequests();
    ASSERT_EQ(requests.size(), 1U);
    EXPECT_EQ(requests.front().cause(), master::RemoveMetaReqPb::ROLLBACK_UNACK);
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackRedirectRpcNotSupportedRemainsRetryable)
{
    constexpr uint64_t rollbackVersion = 17;
    const HostPort redirectMaster("127.0.0.1", 18482);
    const std::string objectKey = "redirected-rollback-unack-rpc-not-supported";
    TestDistributedTopology topology(localAddress_, redirectMaster);
    DS_ASSERT_OK(topology.InitStatus());
    auto redirectApi = std::make_shared<FakeWorkerMasterOCApi>(redirectMaster);
    redirectApi->SetRemoveMetaHandler([](master::RemoveMetaReqPb &, master::RemoveMetaRspPb &) {
        return Status(K_NOT_SUPPORTED, "remote redirect remove operation is unavailable");
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(redirectMaster, redirectApi);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    TestWorkerOcServiceCrudCommonApi common(param);
    master::RemoveMetaRspPb response;
    auto *redirect = response.add_info();
    redirect->set_redirect_meta_address(redirectMaster.ToString());
    redirect->add_change_meta_ids(objectKey);
    std::vector<std::string> failedIds;
    std::vector<std::string> needMigrateIds;
    std::vector<std::string> needWaitIds;
    std::vector<std::string> needL2CacheIds;

    DS_ASSERT_OK(common.RemoveMetadataFromRedirectMaster(
        response, master::RemoveMetaReqPb::ROLLBACK_UNACK, localAddress_.ToString(),
        { { objectKey, rollbackVersion } }, failedIds, needMigrateIds, needWaitIds, needL2CacheIds, ""));

    EXPECT_THAT(failedIds, ElementsAre(objectKey));
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackRetriesTransportFailure)
{
    constexpr uint64_t rollbackVersion = 11;
    const std::string objectKey = "rollback-unack-transport-retry";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(objectKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    size_t calls = 0;
    api->SetRemoveMetaHandler([&](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
        if (++calls == 1) {
            return Status(K_RPC_UNAVAILABLE, "transient rollback failure");
        }
        *response.mutable_success_ids() = { request.ids().begin(), request.ids().end() };
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    auto param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked({ { objectKey, rollbackVersion } });

    EXPECT_EQ(api->RemoveMetaRequests().size(), 2U);
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackRetriesRpcNotSupportedFailure)
{
    constexpr uint64_t rollbackVersion = 15;
    const std::string objectKey = "rollback-unack-rpc-not-supported";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(objectKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    size_t calls = 0;
    api->SetRemoveMetaHandler([&](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
        if (++calls == 1) {
            return Status(K_NOT_SUPPORTED, "remote remove operation is unavailable");
        }
        *response.mutable_success_ids() = { request.ids().begin(), request.ids().end() };
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    auto param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked({ { objectKey, rollbackVersion } });

    EXPECT_EQ(api->RemoveMetaRequests().size(), 2U);
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackRetriesExplicitFailure)
{
    constexpr uint64_t rollbackVersion = 12;
    const std::string firstKey = "rollback-unack-explicit-failure";
    const std::string secondKey = "rollback-unack-missing-result";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(firstKey, localAddress_);
    placement_.SetOwner(secondKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    size_t calls = 0;
    api->SetRemoveMetaHandler([&](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
        if (++calls == 1) {
            response.add_failed_ids(firstKey);
            response.add_success_ids(secondKey);
        } else {
            *response.mutable_success_ids() = { request.ids().begin(), request.ids().end() };
        }
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    auto param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked({ { firstKey, rollbackVersion }, { secondKey, rollbackVersion } });

    auto requests = api->RemoveMetaRequests();
    ASSERT_EQ(requests.size(), 2U);
    EXPECT_THAT(requests.back().ids(), ElementsAre(firstKey));
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackRetriesMissingPartialResult)
{
    constexpr uint64_t rollbackVersion = 13;
    const std::string acknowledgedKey = "rollback-unack-acknowledged-result";
    const std::string missingKey = "rollback-unack-missing-result";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(acknowledgedKey, localAddress_);
    placement_.SetOwner(missingKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    size_t calls = 0;
    api->SetRemoveMetaHandler([&](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
        if (++calls == 1) {
            response.add_success_ids(acknowledgedKey);
        } else {
            *response.mutable_success_ids() = { request.ids().begin(), request.ids().end() };
        }
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    auto param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked(
        { { acknowledgedKey, rollbackVersion }, { missingKey, rollbackVersion } });

    auto requests = api->RemoveMetaRequests();
    ASSERT_EQ(requests.size(), 2U);
    EXPECT_THAT(requests.back().ids(), ElementsAre(missingKey));
}

TEST_F(WorkerOcServiceImplTest, RollbackUnackRetriesMetaMovingResponse)
{
    constexpr uint64_t rollbackVersion = 14;
    const std::string objectKey = "rollback-unack-meta-moving";
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    placement_.SetOwner(objectKey, localAddress_);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(localAddress_);
    size_t calls = 0;
    api->SetRemoveMetaHandler([&](master::RemoveMetaReqPb &request, master::RemoveMetaRspPb &response) {
        if (++calls == 1) {
            response.set_meta_is_moving(true);
        } else {
            *response.mutable_success_ids() = { request.ids().begin(), request.ids().end() };
        }
        return Status::OK();
    });
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    auto param = MakeCrudParam(apiManager);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);

    getImpl.DeleteObjectsMetaUnacked({ { objectKey, rollbackVersion } });

    EXPECT_EQ(api->RemoveMetaRequests().size(), 2U);
}

TEST_F(WorkerOcServiceImplTest, QueryMetaDataFromMasterReportsMetadataRpcSuccess)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort masterAddress("127.0.0.1", 18482);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    std::vector<std::pair<std::string, StatusCode>> observations;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        apiManager, nullptr, nullptr,
        [&observations](const HostPort &target, const Status &status) {
            observations.emplace_back(target.ToString(), status.GetCode());
        });
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    master::QueryMetaRspPb rsp;
    std::vector<RpcMessage> payloads;

    DS_ASSERT_OK(getImpl.QueryMetaDataFromMasterImpl(masterAddress, K_META_MOVING_RETRY_TIMEOUT_MS, { "obj" }, rsp,
                                                     payloads));

    EXPECT_EQ(api->QueryMetaCallCount(), 1);
    EXPECT_THAT(observations, ElementsAre(Pair(masterAddress.ToString(), K_OK)));
}

TEST_F(WorkerOcServiceImplTest, QueryMetaDataFromMasterDoesNotReportLocalRetryBudgetTimeoutAsPeerFailure)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(1);
    const HostPort masterAddress("127.0.0.1", 18482);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    master::QueryMetaRspPb movingRsp;
    movingRsp.set_meta_is_moving(true);
    api->SetQueryMetaResponse(movingRsp);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    std::vector<std::pair<std::string, StatusCode>> observations;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        apiManager, nullptr, nullptr,
        [&observations](const HostPort &target, const Status &status) {
            observations.emplace_back(target.ToString(), status.GetCode());
        });
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    master::QueryMetaRspPb rsp;
    std::vector<RpcMessage> payloads;

    auto rc = getImpl.QueryMetaDataFromMasterImpl(masterAddress, K_META_MOVING_RETRY_TIMEOUT_MS, { "obj" }, rsp,
                                                  payloads);

    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(api->QueryMetaCallCount(), 1);
    EXPECT_THAT(observations, ElementsAre(Pair(masterAddress.ToString(), K_OK)));
}

TEST_F(WorkerOcServiceImplTest, RedirectGetObjectLocationsMapsMetadataRpcFailure)
{
    const HostPort redirectAddress("127.0.0.1", 18482);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(redirectAddress);
    api->SetGetObjectLocationsStatus(Status(K_RPC_UNAVAILABLE, "redirect unavailable"));
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, metadataRoute_);
    apiManager->SetApi(api);
    std::vector<std::pair<std::string, StatusCode>> observations;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        apiManager, nullptr, nullptr,
        [&observations](const HostPort &target, const Status &status) {
            observations.emplace_back(target.ToString(), status.GetCode());
        });
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    google::protobuf::RepeatedPtrField<RedirectMetaInfo> infos;
    auto *info = infos.Add();
    info->set_redirect_meta_address(redirectAddress.ToString());
    info->add_change_meta_ids("obj");
    std::unordered_map<std::string, master::ObjectLocationInfoPb> result;
    Status lastRc;

    DS_ASSERT_OK(getImpl.QueryObjectLocationsFromRedirectMaster(infos, result, lastRc));

    EXPECT_EQ(lastRc.GetCode(), K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_EQ(api->GetObjectLocationsCallCount(), 1);
    EXPECT_THAT(observations, ElementsAre(Pair(redirectAddress.ToString(), K_RPC_UNAVAILABLE)));
}

TEST_F(WorkerOcServiceImplTest, GetMapOfObjectKeysDoesNotReportLocalRetryBudgetTimeoutAsPeerFailure)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(1);
    const HostPort masterAddress("127.0.0.1", 18482);
    const std::string objectKey = "obj";
    TestDistributedTopology topology(localAddress_, masterAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(objectKey, masterAddress);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    master::GetObjectLocationsRspPb movingRsp;
    movingRsp.set_meta_is_moving(true);
    api->SetGetObjectLocationsResponse(movingRsp);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    std::vector<std::pair<std::string, StatusCode>> observations;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        apiManager, topology.Route(), topology.EndpointPolicy(),
        [&observations](const HostPort &target, const Status &status) {
            observations.emplace_back(target.ToString(), status.GetCode());
        });
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    std::unordered_map<std::string, master::ObjectLocationInfoPb> result;
    Status lastRc;

    auto rc = getImpl.GetMapOfObjectKeys({ objectKey }, result, lastRc);

    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(lastRc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(api->GetObjectLocationsCallCount(), 1);
    EXPECT_THAT(observations, ElementsAre(Pair(masterAddress.ToString(), K_OK)));
}

TEST_F(WorkerOcServiceImplTest, GetMapOfObjectKeysMapsOwnerPeerDeadToMetadataOwnerUnavailable)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort masterAddress("127.0.0.1", 18482);
    const std::string objectKey = "metadata-owner-peer-dead-key";
    TestDistributedTopology topology(localAddress_, masterAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(objectKey, masterAddress);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    api->SetGetObjectLocationsStatus(Status(K_RPC_PEER_DEAD, "metadata owner refused"));
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    std::vector<std::pair<std::string, StatusCode>> observations;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        apiManager, topology.Route(), topology.EndpointPolicy(),
        [&observations](const HostPort &target, const Status &status) {
            observations.emplace_back(target.ToString(), status.GetCode());
        });
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    std::unordered_map<std::string, master::ObjectLocationInfoPb> result;
    Status lastRc;

    auto rc = getImpl.GetMapOfObjectKeys({ objectKey }, result, lastRc);

    EXPECT_EQ(rc.GetCode(), K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_EQ(lastRc.GetCode(), K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(api->GetObjectLocationsCallCount(), 1);
    EXPECT_THAT(observations, ElementsAre(Pair(masterAddress.ToString(), K_RPC_PEER_DEAD)));
}

TEST_F(WorkerOcServiceImplTest, GetMapOfObjectKeysKeepsNonConnectionOwnerError)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort masterAddress("127.0.0.1", 18482);
    const std::string objectKey = "metadata-owner-runtime-error-key";
    TestDistributedTopology topology(localAddress_, masterAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(objectKey, masterAddress);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    api->SetGetObjectLocationsStatus(Status(K_RUNTIME_ERROR, "metadata owner internal error"));
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    std::vector<std::pair<std::string, StatusCode>> observations;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        apiManager, topology.Route(), topology.EndpointPolicy(),
        [&observations](const HostPort &target, const Status &status) {
            observations.emplace_back(target.ToString(), status.GetCode());
        });
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    std::unordered_map<std::string, master::ObjectLocationInfoPb> result;
    Status lastRc;

    auto rc = getImpl.GetMapOfObjectKeys({ objectKey }, result, lastRc);

    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(lastRc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_THAT(rc.GetMsg(), Not(HasSubstr("Metadata owner RPC failure")));
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(api->GetObjectLocationsCallCount(), 1);
    EXPECT_THAT(observations, ElementsAre(Pair(masterAddress.ToString(), K_RUNTIME_ERROR)));
}

TEST_F(WorkerOcServiceImplTest, QueryObjectLocationsMapsPureMetadataOwnerPeerDead)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort masterAddress("127.0.0.1", 18482);
    const std::string objectKey = "pure-metadata-owner-peer-dead-key";
    TestDistributedTopology topology(localAddress_, masterAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(objectKey, masterAddress);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    api->SetPureQueryMetaStatus(Status(K_RPC_PEER_DEAD, "pure metadata owner refused"));
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    std::vector<std::pair<std::string, StatusCode>> observations;
    WorkerOcServiceCrudParam param = MakeCrudParam(
        apiManager, topology.Route(), topology.EndpointPolicy(),
        [&observations](const HostPort &target, const Status &status) {
            observations.emplace_back(target.ToString(), status.GetCode());
        });
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    std::unordered_map<std::string, master::ObjectLocationInfoPb> locations;

    auto rc = getImpl.QueryObjectLocations({ objectKey }, locations);

    EXPECT_EQ(rc.GetCode(), K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_TRUE(locations.empty());
    EXPECT_EQ(api->PureQueryMetaCallCount(), 1);
    EXPECT_THAT(observations, ElementsAre(Pair(masterAddress.ToString(), K_RPC_PEER_DEAD)));
}

TEST_F(WorkerOcServiceImplTest, QueryObjectLocationsCarriesServingMasterTopologyVersion)
{
    constexpr uint64_t servingMasterTopologyVersion = 11;
    constexpr uint64_t dataSize = 8 * 1024;
    const HostPort masterAddress("127.0.0.1", 18482);
    const HostPort dataWorkerAddress("127.0.0.1", 18483);
    const std::string objectKey = "versioned-location-key";
    TestDistributedTopology topology(localAddress_, masterAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(objectKey, masterAddress);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(masterAddress);
    master::PureQueryMetaRspPb response;
    auto *queryMeta = response.add_query_metas();
    queryMeta->mutable_meta()->set_object_key(objectKey);
    queryMeta->mutable_meta()->set_data_size(dataSize);
    queryMeta->mutable_meta()->set_primary_address(dataWorkerAddress.ToString());
    queryMeta->set_topology_version(servingMasterTopologyVersion);
    api->SetPureQueryMetaResponse(response);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    std::unordered_map<std::string, master::ObjectLocationInfoPb> locations;

    DS_ASSERT_OK(getImpl.QueryObjectLocations({ objectKey }, locations));
    ASSERT_EQ(locations.count(objectKey), 1u);
    EXPECT_EQ(locations.at(objectKey).topology_version(), servingMasterTopologyVersion);
}

TEST_F(WorkerOcServiceImplTest, QueryObjectLocationsCarriesPerResultServingMasterTopologyVersions)
{
    constexpr uint64_t redirectTopologyVersion = 7;
    constexpr uint64_t directMasterTopologyVersion = 11;
    constexpr uint64_t redirectMasterTopologyVersion = 13;
    const HostPort originalMaster("127.0.0.1", 18482);
    const HostPort redirectMaster("127.0.0.1", 18483);
    const HostPort dataWorkerAddress("127.0.0.1", 18484);
    const std::string directKey = "direct-versioned-location-key";
    const std::string redirectKey = "redirect-versioned-location-key";
    TestDistributedTopology topology(localAddress_, originalMaster);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(directKey, originalMaster);
    topology.SetOwner(redirectKey, originalMaster);
    auto originalApi = std::make_shared<FakeWorkerMasterOCApi>(originalMaster);
    master::PureQueryMetaRspPb originalResponse;
    auto *directMeta = originalResponse.add_query_metas();
    directMeta->mutable_meta()->set_object_key(directKey);
    directMeta->mutable_meta()->set_primary_address(dataWorkerAddress.ToString());
    directMeta->set_topology_version(directMasterTopologyVersion);
    auto *redirect = originalResponse.add_info();
    redirect->set_redirect_meta_address(redirectMaster.ToString());
    redirect->add_change_meta_ids(redirectKey);
    redirect->set_topology_version(redirectTopologyVersion);
    originalApi->SetPureQueryMetaResponse(originalResponse);
    auto redirectApi = std::make_shared<FakeWorkerMasterOCApi>(redirectMaster);
    master::PureQueryMetaRspPb redirectResponse;
    auto *queryMeta = redirectResponse.add_query_metas();
    queryMeta->mutable_meta()->set_object_key(redirectKey);
    queryMeta->mutable_meta()->set_data_size(8 * 1024);
    queryMeta->mutable_meta()->set_primary_address(dataWorkerAddress.ToString());
    queryMeta->set_topology_version(redirectMasterTopologyVersion);
    redirectApi->SetPureQueryMetaResponse(redirectResponse);
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(originalMaster, originalApi);
    apiManager->SetApi(redirectMaster, redirectApi);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, localAddress_, nullptr);
    std::unordered_map<std::string, master::ObjectLocationInfoPb> locations;

    DS_ASSERT_OK(getImpl.QueryObjectLocations({ directKey, redirectKey }, locations));
    ASSERT_EQ(locations.size(), 2u);
    EXPECT_EQ(locations.at(directKey).topology_version(), directMasterTopologyVersion);
    EXPECT_EQ(locations.at(redirectKey).topology_version(), redirectMasterTopologyVersion);
}

TEST_F(WorkerOcServiceImplTest, GetObjMetaInfoUsesLocalObjectBeforeStaleOwner)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort staleOwnerAddress("127.0.0.1", 18482);
    const std::string objectKey = "scale-in-local-object";
    constexpr uint64_t dataSize = 8192;
    AddReadableObject(objectKey, 7, dataSize);
    TestDistributedTopology topology(localAddress_, staleOwnerAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(objectKey, staleOwnerAddress);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(staleOwnerAddress);
    api->SetGetObjectLocationsStatus(Status(K_RPC_PEER_DEAD, "stale owner down"));
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, std::make_shared<AkSkManager>(0), localAddress_,
                                   nullptr);
    GetObjMetaInfoReqPb req;
    req.add_object_keys(objectKey);
    GetObjMetaInfoRspPb resp;

    DS_ASSERT_OK(getImpl.GetObjMetaInfo(req, resp));

    ASSERT_EQ(resp.objs_meta_info_size(), 1);
    EXPECT_EQ(resp.objs_meta_info(0).obj_size(), dataSize);
    ASSERT_EQ(resp.objs_meta_info(0).location_ids_size(), 1);
    EXPECT_EQ(resp.objs_meta_info(0).location_ids(0), localAddress_.ToString());
    EXPECT_EQ(api->GetObjectLocationsCallCount(), 1);
}

TEST_F(WorkerOcServiceImplTest, GetObjMetaInfoDoesNotHideRemoteOwnerFailureAfterLocalHit)
{
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    const HostPort staleOwnerAddress("127.0.0.1", 18482);
    const std::string localObjectKey = "scale-in-mixed-local-object";
    const std::string remoteObjectKey = "scale-in-mixed-remote-object";
    AddReadableObject(localObjectKey);
    TestDistributedTopology topology(localAddress_, staleOwnerAddress);
    DS_ASSERT_OK(topology.InitStatus());
    topology.SetOwner(localObjectKey, staleOwnerAddress);
    topology.SetOwner(remoteObjectKey, staleOwnerAddress);
    auto api = std::make_shared<FakeWorkerMasterOCApi>(staleOwnerAddress);
    api->SetGetObjectLocationsStatus(Status(K_RPC_PEER_DEAD, "stale owner down"));
    auto apiManager = std::make_shared<FakeWorkerMasterApiManager>(localAddress_, *topology.Route());
    apiManager->SetApi(api);
    WorkerOcServiceCrudParam param = MakeCrudParam(apiManager, topology.Route(), topology.EndpointPolicy());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, std::make_shared<AkSkManager>(0), localAddress_,
                                   nullptr);
    GetObjMetaInfoReqPb req;
    req.add_object_keys(localObjectKey);
    req.add_object_keys(remoteObjectKey);
    GetObjMetaInfoRspPb resp;

    auto rc = getImpl.GetObjMetaInfo(req, resp);

    EXPECT_EQ(rc.GetCode(), K_METADATA_OWNER_UNAVAILABLE);
    EXPECT_EQ(resp.objs_meta_info_size(), 0);
    EXPECT_EQ(api->GetObjectLocationsCallCount(), 1);
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

TEST_F(WorkerOcServiceImplTest, NotifyRemoteGetRejectsAfterIncomingMigrationAdmissionCloses)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    constexpr std::chrono::seconds closeBudget(1);
    DS_ASSERT_OK(impl_->gMigrateProc_->CloseIncomingMigrationAdmissionAndWait(
        std::chrono::steady_clock::now() + closeBudget));

    NotifyRemoteGetReqPb req;
    req.add_object_keys("late-notify-remote-get-object");
    NotifyRemoteGetRspPb rsp;
    EXPECT_EQ(impl_->NotifyRemoteGet(req, rsp).GetCode(), StatusCode::K_NOT_READY);
}

TEST_F(WorkerOcServiceImplTest, ExitIntentRejectsClientHealthBeforeTopologyDrainStarts)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    Raii restoreHealth([] {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    SetTopologyServingAdmission(true);
    DS_ASSERT_OK(SetHealthProbe());
    ASSERT_TRUE(IsHealthy());
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    exitRequested_.store(true, std::memory_order_relaxed);

    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    HealthCheckRequestPb req;
    HealthCheckReplyPb rsp;
    EXPECT_EQ(impl_->HealthCheck(req, rsp).GetCode(), K_SCALE_DOWN);
    EXPECT_FALSE(impl_->MigrateDataStarted());
}

TEST_F(WorkerOcServiceImplTest, ShutdownRequestRejectsClientHealthWithoutTopologyExitIntent)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    Raii restoreHealth([] {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    SetTopologyServingAdmission(true);
    DS_ASSERT_OK(SetHealthProbe());
    ASSERT_TRUE(IsHealthy());
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));

    impl_->RequestShutdown();

    EXPECT_FALSE(exitRequested_.load(std::memory_order_acquire));
    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    HealthCheckRequestPb req;
    HealthCheckReplyPb rsp;
    EXPECT_EQ(impl_->HealthCheck(req, rsp).GetCode(), K_SCALE_DOWN);
}

TEST_F(WorkerOcServiceImplTest, IncomingMigrationGateClosureDoesNotImpersonateTopologyScaleInDrain)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    const bool savedLeavingIntercept = FLAGS_enable_leaving_intercept;
    Raii restore([savedLeavingIntercept] { FLAGS_enable_leaving_intercept = savedLeavingIntercept; });
    FLAGS_enable_leaving_intercept = false;
    exitRequested_.store(true, std::memory_order_release);

    constexpr std::chrono::seconds closeBudget(1);
    DS_ASSERT_OK(impl_->gMigrateProc_->CloseIncomingMigrationAdmissionAndWait(
        std::chrono::steady_clock::now() + closeBudget));

    EXPECT_FALSE(impl_->MigrateDataStarted());
    DS_EXPECT_OK(impl_->VerifyClientWriteAdmission(false));
}

TEST_F(WorkerOcServiceImplTest, DrainRejectsClientWritesWhenLeavingInterceptIsDisabled)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    const bool savedLeavingIntercept = FLAGS_enable_leaving_intercept;
    Raii restore([savedLeavingIntercept] {
        FLAGS_enable_leaving_intercept = savedLeavingIntercept;
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    FLAGS_enable_leaving_intercept = false;
    SetTopologyServingAdmission(true);
    DS_ASSERT_OK(SetHealthProbe());
    ASSERT_TRUE(IsHealthy());
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    exitRequested_.store(true, std::memory_order_release);

    cluster::TopologyPhaseAction action;
    action.taskId = "client-write-fence-task";
    cluster::CancellationToken cancellation;
    constexpr std::chrono::seconds drainBudget(1);
    DS_ASSERT_OK(impl_->DrainTopologyScaleInData(action, "client-write-fence-operation",
                                                 std::chrono::steady_clock::now() + drainBudget, cancellation));
    EXPECT_TRUE(impl_->MigrateDataStarted());
    ASSERT_TRUE(IsHealthy());

    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    CreateReqPb req;
    req.set_object_key("write-after-drain");
    req.set_data_size(1);
    CreateRspPb rsp;
    EXPECT_EQ(impl_->Create(req, rsp).GetCode(), K_SCALE_DOWN);
}

TEST_F(WorkerOcServiceImplTest, ExitIntentRejectsRoutedCreateBeforeDataDrain)
{
    const bool savedLeavingIntercept = FLAGS_enable_leaving_intercept;
    Raii restore([savedLeavingIntercept] {
        FLAGS_enable_leaving_intercept = savedLeavingIntercept;
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    FLAGS_enable_leaving_intercept = false;
    SetTopologyServingAdmission(true);
    DS_ASSERT_OK(SetHealthProbe());
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    exitRequested_.store(true, std::memory_order_release);

    ScopedRequestContext requestContext;
    GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
    CreateReqPb req;
    req.set_object_key("routed-write-after-exit-intent");
    req.set_data_size(1);
    req.set_is_routed(true);
    CreateRspPb rsp;

    EXPECT_EQ(impl_->Create(req, rsp).GetCode(), K_SCALE_DOWN);
    EXPECT_FALSE(impl_->MigrateDataStarted());
}

TEST_F(WorkerOcServiceImplTest, ScaleInDrainWaitsForLocalPreShutdownGateBeforeClosingWrites)
{
    impl_->RegisterAsyncTasksDoneChecker(
        [this](const std::string &, std::chrono::steady_clock::time_point,
               const cluster::CancellationToken &) {
            EXPECT_FALSE(impl_->MigrateDataStarted());
            return Status(K_NOT_READY, "local pre-shutdown gate remains closed");
        });
    cluster::TopologyPhaseAction action;
    action.taskId = "local-pre-shutdown-gate-task";
    cluster::CancellationToken cancellation;
    constexpr std::chrono::seconds drainBudget(1);

    EXPECT_EQ(impl_->DrainTopologyScaleInData(action, "local-pre-shutdown-gate-operation",
                                              std::chrono::steady_clock::now() + drainBudget, cancellation)
                  .GetCode(),
              K_NOT_READY);
    EXPECT_FALSE(impl_->MigrateDataStarted());
}

TEST_F(WorkerOcServiceImplTest, ScaleInDrainDoesNotWaitForAdmittedClientWrite)
{
    const bool savedLeavingIntercept = FLAGS_enable_leaving_intercept;
    const std::string injectPoint = "worker.Create.begin";
    Raii restore([savedLeavingIntercept, &injectPoint] {
        FLAGS_enable_leaving_intercept = savedLeavingIntercept;
        (void)inject::Clear(injectPoint);
        SetTopologyServingAdmission(true);
        SetUnhealthy();
    });
    FLAGS_enable_leaving_intercept = false;
    SetTopologyServingAdmission(true);
    DS_ASSERT_OK(SetHealthProbe());
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    DS_ASSERT_OK(inject::Set(injectPoint, "pause()"));

    CreateReqPb req;
    req.set_object_key("admitted-write-before-scale-in-snapshot");
    req.set_data_size(1);
    CreateRspPb rsp;
    auto requestFuture = std::async(std::launch::async, [this, &req, &rsp] {
        ScopedRequestContext requestContext;
        GetRequestContext()->reqTimeoutDuration.Init(K_META_MOVING_RETRY_TIMEOUT_MS);
        return impl_->Create(req, rsp);
    });
    constexpr std::chrono::seconds schedulingTimeout(1);
    ASSERT_TRUE(WaitForInjectPointExecuteCount(injectPoint, 1, schedulingTimeout));

    cluster::TopologyPhaseAction action;
    action.taskId = "admitted-write-drain-task";
    cluster::CancellationToken cancellation;
    constexpr std::chrono::seconds drainBudget(2);
    auto drainFuture = std::async(std::launch::async, [this, &action, &cancellation, drainBudget] {
        return impl_->DrainTopologyScaleInData(action, "admitted-write-drain-operation",
                                               std::chrono::steady_clock::now() + drainBudget, cancellation);
    });
    ASSERT_TRUE(WaitForCondition([this] { return impl_->MigrateDataStarted(); }, schedulingTimeout));
    EXPECT_EQ(drainFuture.wait_for(schedulingTimeout), std::future_status::ready);

    DS_ASSERT_OK(inject::Clear(injectPoint));
    (void)requestFuture.get();
    DS_EXPECT_OK(drainFuture.get());
}

TEST_F(WorkerOcServiceImplTest, NotifyRemoteGetHoldsAdmissionUntilRequestReturns)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
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
    auto requestFuture = std::async(std::launch::async, [this, &req, &rsp] {
        return impl_->NotifyRemoteGet(req, rsp);
    });
    // Wait until the RPC hits the inject point - admission is acquired and held.
    const bool requestAdmitted = WaitForInjectPointExecuteCount(injectPoint, 1, schedulingTimeout);

    auto closeFuture = std::async(std::launch::async, [this, closeBudget] {
        return impl_->gMigrateProc_->CloseIncomingMigrationAdmissionAndWait(
            std::chrono::steady_clock::now() + closeBudget);
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
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    constexpr std::chrono::seconds schedulingTimeout(1);
    constexpr std::chrono::milliseconds closeBudget(100);
    const std::string injectPoint = "WorkerOCServiceImpl.NotifyRemoteGet.afterAdmission";

    // Block the RPC at the afterAdmission inject point so drain will time out.
    DS_ASSERT_OK(inject::Set(injectPoint, "pause()"));

    NotifyRemoteGetReqPb req;
    req.add_object_keys("drain-timeout-object");
    NotifyRemoteGetRspPb rsp;
    auto requestFuture = std::async(std::launch::async, [this, &req, &rsp] {
        return impl_->NotifyRemoteGet(req, rsp);
    });
    // Wait until the RPC hits the inject point - admission is acquired and held.
    const bool requestAdmitted = WaitForInjectPointExecuteCount(injectPoint, 1, schedulingTimeout);

    // Drain with an already-expired deadline so it times out immediately.
    auto closeFuture = std::async(std::launch::async, [this, closeBudget] {
        return impl_->gMigrateProc_->CloseIncomingMigrationAdmissionAndWait(
            std::chrono::steady_clock::now() + closeBudget);
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

TEST_F(WorkerOcServiceImplTest, DrainWaitsForIncomingMigrationBeforeSnapshotCollection)
{
    ASSERT_NE(impl_->gMigrateProc_, nullptr);
    DS_ASSERT_OK(topologyRuntime_.StartWithActiveLocalMember(localAddress_));
    constexpr std::chrono::seconds schedulingTimeout(1);
    constexpr std::chrono::seconds drainBudget(2);
    constexpr std::chrono::milliseconds observationWindow(50);
    const std::string requestPoint = "WorkerOCServiceImpl.NotifyRemoteGet.afterAdmission";
    const std::string closePoint = "WorkerOcServiceMigrateImpl.CloseIncomingMigrationAdmissionAndWait.closed";
    const std::string snapshotPoint = "WorkerOCServiceImpl.DrainTopologyScaleInData.beforeSnapshot";
    Raii clearInjects([&] {
        (void)inject::Clear(requestPoint);
        (void)inject::Clear(closePoint);
        (void)inject::Clear(snapshotPoint);
    });
    DS_ASSERT_OK(inject::Set(requestPoint, "pause()"));
    DS_ASSERT_OK(inject::Set(closePoint, "call()"));
    DS_ASSERT_OK(inject::Set(snapshotPoint, "call()"));

    NotifyRemoteGetReqPb req;
    req.add_object_keys("snapshot-order-object");
    NotifyRemoteGetRspPb rsp;
    auto requestFuture = std::async(std::launch::async, [this, &req, &rsp] {
        return impl_->NotifyRemoteGet(req, rsp);
    });
    ASSERT_TRUE(WaitForInjectPointExecuteCount(requestPoint, 1, schedulingTimeout));

    cluster::TopologyPhaseAction action;
    action.taskId = "snapshot-order-task";
    cluster::CancellationToken cancellation;
    auto drainFuture = std::async(std::launch::async, [this, &action, &cancellation, drainBudget] {
        return impl_->DrainTopologyScaleInData(action, "snapshot-order-operation",
                                               std::chrono::steady_clock::now() + drainBudget, cancellation);
    });
    ASSERT_TRUE(WaitForInjectPointExecuteCount(closePoint, 1, schedulingTimeout));
    std::this_thread::sleep_for(observationWindow);
    EXPECT_EQ(inject::GetExecuteCount(snapshotPoint), 0U);

    DS_ASSERT_OK(inject::Clear(requestPoint));
    EXPECT_EQ(requestFuture.get().GetCode(), StatusCode::K_NOT_READY);
    DS_EXPECT_OK(drainFuture.get());
    EXPECT_EQ(inject::GetExecuteCount(snapshotPoint), 1U);
}

TEST_F(WorkerOcServiceImplTest, ValidateWorkerStateReturnsTryAgainDuringStartupReconciliation)
{
    // worker crash and restart, reconciliation
    DS_ASSERT_OK(ResetHealthProbe());
    ASSERT_FALSE(IsHealthy());
    FLAGS_enable_reconciliation = true;

    // controlBackendAvailableAtStartup=true: start reconciliation (reconciliationReady_=false)
    auto restartImpl = std::make_shared<WorkerOCServiceImpl>(
        localAddress_, localAddress_, objectTable_, nullptr, evictionManager_, nullptr, nullptr, nullptr,
        topologyRuntime_.Engine(), metadataRoute_, topologyRuntime_.Engine()->Membership(), &exitRequested_,
        /*isRestart=*/true, /*controlBackendAvailableAtStartup=*/true);
    restartImpl->InitServiceImpl();

    ASSERT_TRUE(restartImpl->isRestart_);
    ASSERT_FALSE(restartImpl->reconciliationReady_.load(std::memory_order_acquire));
    ASSERT_TRUE(restartImpl->IsStartupReconciling());

    BthreadReadGuard noRecon;
    Status rc = restartImpl->ValidateWorkerState(noRecon, 60000);
    EXPECT_EQ(rc.GetCode(), K_TRY_AGAIN);

    // reconciliation finish, reconciliationReady_=true, g_health=true -> K_OK
    DS_ASSERT_OK(SetHealthProbe());
    restartImpl->reconciliationReady_.store(true, std::memory_order_release);
    ASSERT_TRUE(IsHealthy());
    ASSERT_FALSE(restartImpl->IsStartupReconciling());
}

TEST_F(WorkerOcServiceImplTest, ValidateWorkerStateReturnsNotReadyWhenReconciliationSkippedAndUnhealthy)
{
    // skip reconciliation(controlBackend=false)
    DS_ASSERT_OK(ResetHealthProbe());
    ASSERT_FALSE(IsHealthy());
    FLAGS_enable_reconciliation = true;

    // controlBackendAvailableAtStartup=false: reconciliationReady_=true (skip reconciliation)
    auto skipImpl = std::make_shared<WorkerOCServiceImpl>(
        localAddress_, localAddress_, objectTable_, nullptr, evictionManager_, nullptr, nullptr, nullptr,
        topologyRuntime_.Engine(), metadataRoute_, topologyRuntime_.Engine()->Membership(), &exitRequested_,
        /*isRestart=*/true, /*controlBackendAvailableAtStartup=*/false);
    skipImpl->InitServiceImpl();

    ASSERT_TRUE(skipImpl->isRestart_);
    ASSERT_TRUE(skipImpl->reconciliationReady_.load(std::memory_order_acquire));
    ASSERT_FALSE(skipImpl->IsStartupReconciling());

    BthreadReadGuard noRecon;
    Status rc = skipImpl->ValidateWorkerState(noRecon, 60000);
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);

    DS_ASSERT_OK(SetHealthProbe());
}
}  // namespace ut
}  // namespace datasystem
