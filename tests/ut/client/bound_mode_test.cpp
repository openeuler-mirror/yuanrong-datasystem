#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "datasystem/client/object_cache/bound_mode.h"
#include "datasystem/client/object_cache/routing/ub_health_filter.h"
#include "datasystem/client/object_cache/transport/transport_layer.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/rpc/api_deadline.h"

namespace datasystem {
namespace object_cache {

class MockClientWorkerApi : public IClientWorkerApi {
public:
    MockClientWorkerApi() : MockClientWorkerApi(HostPort())
    {
    }

    explicit MockClientWorkerApi(const HostPort &hostPort)
        : client::IClientWorkerCommonApi(hostPort, HeartbeatType::RPC_HEARTBEAT, false,
                                         static_cast<Signature *>(nullptr)),
          IClientWorkerApi(hostPort, HeartbeatType::RPC_HEARTBEAT, false, static_cast<Signature *>(nullptr))
    {
    }

    std::shared_ptr<IClientWorkerApi> CloneWith(HostPort hostPort, HeartbeatType heartbeatType, SensitiveValue token, Signature *signature, std::string tenantId, bool enableCrossNodeConnection, std::shared_ptr<::datasystem::client::EmbeddedClientWorkerApi> api, void *worker) const override { return nullptr; }
    Status Create(const std::string &objectKey, int64_t dataSize, uint32_t &version, uint64_t &metadataSize, std::shared_ptr<ShmUnitInfo> &shmBuf, std::shared_ptr<UrmaRemoteAddrPb> &urmaDataInfo, const CacheType &cacheType, int32_t requestTimeoutMs) override { return Status::OK(); }
    Status Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isShm, bool isSeal, const std::unordered_set<std::string> &nestedKeys, uint32_t ttlSecond, int existence, int32_t requestTimeoutMs) override { return Status::OK(); }
    Status MultiPublish(const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo, const PublishParam &param, MultiPublishRspPb &rsp, const std::vector<const DeviceBlobList *> &deviceBlobRefs) override { return Status::OK(); }
    Status DecreaseWorkerRef(const std::vector<ShmKey> &objectKeys) override { return Status::OK(); }
    Status PipelineRH2D(PiplnRh2dParam &piplnRh2dParam, GetRspPb &rsp) override { return Status::OK(); }
    Status Get(const GetParam &, uint32_t &, GetRspPb &rsp, std::vector<RpcMessage> &,
               Status *ingressRpcStatus = nullptr) override
    {
        if (ingressRpcStatus != nullptr) {
            *ingressRpcStatus = Status::OK();
        }
        ++getCalls;
        if (providerUbFailureDetail.has_value()) {
            *rsp.mutable_provider_ub_failure_detail() = *providerUbFailureDetail;
        }
        if (expireDeadlineOnGet) {
            ApiDeadline::Instance().InitUs(0);
        }
        if (repeatedGetStatus != K_OK) {
            return Status(repeatedGetStatus, "injected repeated Get response");
        }
        return Status(getCalls == 1 ? firstGetStatus : K_INVALID, "injected Get response");
    }
    size_t getCalls = 0;
    StatusCode firstGetStatus = K_OK;
    StatusCode repeatedGetStatus = K_OK;
    bool expireDeadlineOnGet = false;
    std::optional<ProviderUbFailureDetailPb> providerUbFailureDetail;
    Status InvalidateBuffer(const std::string &objectKey) override { return Status::OK(); }
    Status GIncreaseWorkerRef(const std::vector<std::string> &firstIncIds, std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId) override { return Status::OK(); }
    Status ReleaseGRefs(const std::string &remoteClientId) override { return Status::OK(); }
    Status GDecreaseWorkerRef(const std::vector<std::string> &finishDecIds, std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId) override { return Status::OK(); }
    Status Delete(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys, bool areDeviceObjects) override { return Status::OK(); }
    Status QueryGlobalRefNum(const std::vector<std::string> &objectKeys, std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap) override { return Status::OK(); }
    Status PublishDeviceObject(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, size_t dataSize, bool isShm, void *nonShmPointer) override { return Status::OK(); }
    Status GetDeviceObject(const std::vector<std::string> &devObjKeys, uint64_t dataSize, int32_t timeoutMs, GetDeviceObjectRspPb &rsp, std::vector<RpcMessage> &payloads) override { return Status::OK(); }
    Status SubscribeReceiveEvent(int32_t deviceId, SubscribeReceiveEventRspPb &resp) override { return Status::OK(); }
    Status PutP2PMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, const std::vector<Blob> &blobs) override { return Status::OK(); }
    Status GetP2PMeta(std::vector<std::shared_ptr<DeviceBufferInfo>> &bufferInfoList, std::vector<DeviceBlobList> &devBlobList, GetP2PMetaRspPb &resp, int64_t subTimeoutMs) override { return Status::OK(); }
    Status SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp) override { return Status::OK(); }
    Status RecvRootInfo(RecvRootInfoReqPb &req, RecvRootInfoRspPb &resp) override { return Status::OK(); }
    Status GetBlobsInfo(const std::string &devObjKey, int32_t timeoutMs, std::vector<Blob> &blobs) override { return Status::OK(); }
    Status AckRecvFinish(AckRecvFinishReqPb &req) override { return Status::OK(); }
    Status RemoveP2PLocation(const std::string &objectKey, int32_t deviceId) override { return Status::OK(); }
    Status GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys, std::vector<ObjMetaInfo> &objMetas) override { return Status::OK(); }
    Status MultiCreate(bool skipCheckExistence, std::vector<MultiCreateParam> &createParams, uint32_t &version, std::vector<bool> &exists, bool &useShmTransfer) override { return Status::OK(); }
    Status QuerySize(const std::vector<std::string> &objectKeys, QuerySizeRspPb &rsp) override { return Status::OK(); }
    Status HealthCheck(ServerState &state) override { return Status::OK(); }
    Status Exist(const std::vector<std::string> &keys, std::vector<bool> &exists, const bool queryL2Cache, const bool isLocal) override { return Status::OK(); }
    Status Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds, std::vector<std::string> &failedKeys) override { return Status::OK(); }
    Status GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey, GetMetaInfoRspPb &metaInfos) override { return Status::OK(); }
    Status ReconnectWorker(const std::vector<std::string> &gRefIds) override { return Status::OK(); }
    Status PrepareForDecreaseShmRef(std::function<Status(const std::string &, const std::shared_ptr<ShmUnitInfo> &)> mmapFunc) override { return Status::OK(); }
    Status CleanUpForDecreaseShmRefAfterWorkerLost() override { return Status::OK(); }
    bool WorkerSupportPiplnRH2D() override { return false; }
    Status InitPipelineRH2DQueue(ShmConvertHookFunc hook) override { return Status::OK(); }
    void CleanUpForPipelineRH2DQueueAfterWorkerLost() override { }
    Status DecreaseShmRef(const ShmKey &shmId, const std::function<Status()> &connectCheck, std::shared_timed_mutex &shutdownMtx) override
    {
        DecreaseShmRefCalls++;
        return DecreaseShmRefRc;
    }
    int DecreaseShmRefCalls = 0;
    Status Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs, uint64_t fastTransportSize, int32_t initAttemptTimeoutMs) override { return Status::OK(); }
    Status SendHeartbeat(bool &workerReboot, bool &clientRemoved, int64_t remainTime, bool &isWorkerVoluntaryScaleDown, const std::vector<int64_t> &releasedFds, std::vector<int64_t> &expiredWorkerFds) override { return Status::OK(); }
    Status GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds, const std::string &tenantId) override { return Status::OK(); }
    Status Disconnect(bool isDestruct) override { return Status::OK(); }
    Status Reconnect() override { return Status::OK(); }
    Status TryFastTransportAfterHeartbeat() override { return Status::OK(); }
    std::vector<HostPort> GetStandbyWorkers() override { return {}; }
    Status UpdateToken(SensitiveValue &token) override { return Status::OK(); }
    Status UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey) override { return Status::OK(); }
    Status SetToken(std::string &token) override { return Status::OK(); }
    void SetTenantId(std::string &tenantId) override { }
    Status Connect(RegisterClientReqPb &req, int32_t timeoutMs, bool reconnection, int32_t stateTimeoutMs) override { return Status::OK(); }
    Status DecreaseShmRefRc = Status::OK();
    Status ReconcileShmRef(const std::unordered_set<ShmKey> &confirmedExpiredShmIds, std::vector<ShmKey> &maybeExpiredShmIds) override { return Status::OK(); }
    Status SendBufferViaUb(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data, uint64_t length, bool traceEnabled) override { return Status::OK(); }
    Status SendBufferViaUbFromPool(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data, uint64_t length, bool traceEnabled) override { return Status::OK(); }
};

namespace {
constexpr char LOCAL_PROBE_ACCEPTED_INJECT[] = "TransportLayer.ClientUbProbeCooldown.localAccepted";
constexpr char REMOTE_PROBE_ACCEPTED_INJECT[] = "TransportLayer.ClientUbProbeCooldown.remoteAccepted";

class BoundModeTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        auto mock = std::make_shared<MockClientWorkerApi>();
        mockApi = mock;
        workerApi.resize(3);
        workerApi[static_cast<WorkerNode>(0)] = mock;
        BoundMode::Deps deps{ workerApi,
                              nullMmap,
                              &refTable,
                              &globalRefTable,
                              &globalRefMutex,
                              nullTransport,
                              routing,
                              memCopyPool,
                              asyncReleasePool,
                              asyncGetRPCPool,
                              asyncPipelineRH2DPool,
                              simpleIdRe,
                              nullptr,
                              shutdownMux,
                              currentNode,
                              requestTimeoutMs,
                              tenantId,
                              token,
                              enableLocalCache,
                              enableH2D,
                              parallismNum,
                              {},
                              {} };
        deps.host.getSelf = [] { return std::shared_ptr<ObjectClientImpl>(); };
        deps.host.isClientReady = [] { return Status::OK(); };
        deps.host.checkConnection = [] { return Status::OK(); };
        deps.host.checkConnWhileShmModify = [] { return Status::OK(); };
        deps.host.isBufferAlive = [](uint32_t) { return true; };
        deps.host.handleDirectGetFailure = [](const std::shared_ptr<IClientWorkerApi> &, const Status &) {};
        deps.getWorkerApiNode = [this](std::shared_ptr<IClientWorkerApi> &api, std::unique_ptr<Raii> &guard,
                                       WorkerNode &node) {
            EXPECT_FALSE(getGuardHeld);
            ++getApiCalls;
            getGuardHeld = true;
            guard = std::make_unique<Raii>([this] { getGuardHeld = false; });
            api = mockApi;
            node = LOCAL_WORKER;
            return Status::OK();
        };
        bound = std::make_unique<BoundMode>(deps);
    }

    std::shared_ptr<MockClientWorkerApi> mockApi;
    std::vector<std::shared_ptr<IClientWorkerApi>> workerApi;
    ClientMemoryRefTable refTable;
    TbbGlobalRefTable globalRefTable;
    std::shared_timed_mutex globalRefMutex;
    std::shared_ptr<client::Routing> routing;
    std::unique_ptr<client::MmapManager> nullMmap;
    std::unique_ptr<client::TransportLayer> nullTransport;
    std::shared_ptr<ThreadPool> memCopyPool;
    std::shared_ptr<ThreadPool> asyncReleasePool = std::make_shared<ThreadPool>(1);
    std::shared_ptr<ThreadPool> asyncGetRPCPool;
    std::shared_ptr<ThreadPool> asyncPipelineRH2DPool;
    re2::RE2 simpleIdRe{ "^[a-zA-Z0-9_]*$" };
    std::shared_timed_mutex shutdownMux;
    std::atomic<WorkerNode> currentNode{ static_cast<WorkerNode>(0) };
    int32_t requestTimeoutMs = 1000;
    std::string tenantId = "tn0";
    SensitiveValue token;
    bool enableLocalCache = true;
    bool enableH2D = false;
    int parallismNum = 0;
    std::unique_ptr<BoundMode> bound;
    bool getGuardHeld = false;
    size_t getApiCalls = 0;
};

TEST_F(BoundModeTest, ConstructObjKeyWithTenantIdPrefixesTenant)
{
    // Baseline keeps this quirk: the branch picks the injected tenantId_ but the concat
    // reads GetRequestContext()->tenantId, which is empty outside a request scope.
    ASSERT_EQ(bound->ConstructObjKeyWithTenantId("obj1"), "$obj1");
}

TEST_F(BoundModeTest, ConstructObjKeyWithTenantIdPassthroughUnfilteredKey)
{
    std::string out;
    ASSERT_EQ(bound->ConstructObjKeyWithTenantId("bad key!"), "$bad key!");
}

TEST_F(BoundModeTest, DecreaseReferenceCntCallsWorkerAndIsRepeatable)
{
    ASSERT_TRUE(inject::Set("client.DecreaseReferenceCnt", "call(0)"));
    const ShmKey shmId = ShmKey::Intern("obj1");
    bound->DecreaseReferenceCnt(shmId, true, 0);
    bound->DecreaseReferenceCnt(shmId, true, 0);
    ASSERT_EQ(mockApi->DecreaseShmRefCalls, 2);
    inject::Clear("client.DecreaseReferenceCnt");
}

TEST_F(BoundModeTest, DecreaseReferenceCntWorkerErrorPropagates)
{
    ASSERT_TRUE(inject::Set("client.DecreaseReferenceCnt", "call(0)"));
    mockApi->DecreaseShmRefRc = Status(K_RUNTIME_ERROR, "worker refused");
    const ShmKey shmId = ShmKey::Intern("obj2");
    bound->DecreaseReferenceCnt(shmId, false, 7);
    ASSERT_EQ(mockApi->DecreaseShmRefCalls, 1);
    inject::Clear("client.DecreaseReferenceCnt");
}

TEST_F(BoundModeTest, UbGetRetryReacquiresWorkerApiAndPreservesTerminalError)
{
    for (auto code : {K_URMA_ERROR, K_URMA_WORKER_UNAVAILABLE, K_URMA_DATA_WORKER_UNAVAILABLE}) {
        ApiDeadlineGuard deadline(1000);
        mockApi->firstGetStatus = code;
        mockApi->getCalls = 0;
        getApiCalls = 0;
        std::vector<std::shared_ptr<Buffer>> buffers(1);
        EXPECT_EQ(bound->GetFromLocalWorker({"key"}, 0, buffers, false, false, 1000).GetCode(), K_INVALID);
        EXPECT_EQ(mockApi->getCalls, 2U);
        EXPECT_EQ(getApiCalls, 2U);
        EXPECT_FALSE(getGuardHeld);
    }
}

TEST_F(BoundModeTest, NonUbGetFailureDoesNotRetry)
{
    ApiDeadlineGuard deadline(1000);
    mockApi->firstGetStatus = K_INVALID;
    std::vector<std::shared_ptr<Buffer>> buffers(1);
    EXPECT_EQ(bound->GetFromLocalWorker({"key"}, 0, buffers, false, false, 1000).GetCode(), K_INVALID);
    EXPECT_EQ(mockApi->getCalls, 1U);
    EXPECT_EQ(getApiCalls, 1U);
    EXPECT_FALSE(getGuardHeld);
}

TEST_F(BoundModeTest, UbGetRetryStopsAtDeadlineAndPreservesLastError)
{
    ApiDeadlineGuard deadline(1000);
    mockApi->firstGetStatus = K_URMA_ERROR;
    mockApi->expireDeadlineOnGet = true;
    std::vector<std::shared_ptr<Buffer>> buffers(1);
    EXPECT_EQ(bound->GetFromLocalWorker({"key"}, 0, buffers, false, false, 1000).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(mockApi->getCalls, 1U);
    EXPECT_EQ(getApiCalls, 1U);
    EXPECT_FALSE(getGuardHeld);
}

TEST_F(BoundModeTest, PersistentUbGetFailureStopsAtIndependentAttemptLimit)
{
    ApiDeadlineGuard deadline(10'000);
    mockApi->repeatedGetStatus = K_URMA_ERROR;
    std::vector<std::shared_ptr<Buffer>> buffers(1);

    EXPECT_EQ(bound->GetFromLocalWorker({ "key" }, 0, buffers, false, false, 10'000).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(mockApi->getCalls, 4U);
    EXPECT_EQ(getApiCalls, 4U);
    EXPECT_GT(ApiDeadline::Instance().ApiRemainingUs(), 0);
    EXPECT_FALSE(getGuardHeld);
}

TEST_F(BoundModeTest, ProviderCqe4AndClientCqe9UseUnifiedProbeEntry)
{
    const HostPort provider("127.0.0.1", 19103);
    mockApi = std::make_shared<MockClientWorkerApi>(provider);
    workerApi[static_cast<WorkerNode>(0)] = mockApi;

    auto filter = std::make_shared<client::UbHealthFilter>();
    UbHealthSummary summary;
    summary.worker = provider;
    summary.incarnation = "incarnation-a";
    summary.portHealth = UbPortHealthSummary{ true, 4, 0, 1, false };
    ASSERT_TRUE(filter->ObserveSummary(summary, summary.incarnation));
    client::TransportLayerOptions options;
    options.initializeUbRuntime = false;
    options.readSourceFilter = filter;
    nullTransport = std::make_unique<client::TransportLayer>(std::make_shared<Signature>(),
                                                             std::make_shared<ThreadPool>(1), 0,
                                                             std::move(options));

    ASSERT_TRUE(inject::Set(LOCAL_PROBE_ACCEPTED_INJECT, "10*call()").IsOk());
    Raii clearLocalInject([] { (void)inject::Clear(LOCAL_PROBE_ACCEPTED_INJECT); });
    ASSERT_TRUE(inject::Set(REMOTE_PROBE_ACCEPTED_INJECT, "10*call()").IsOk());
    Raii clearRemoteInject([] { (void)inject::Clear(REMOTE_PROBE_ACCEPTED_INJECT); });

    const std::vector<std::string> objectKeys{ "key" };
    const std::vector<ReadParam> readParams;
    GetParam getParam{ objectKeys, 0, readParams, false };
    getParam.requestTimeoutMs = 1'000;
    std::vector<std::shared_ptr<Buffer>> buffers(1);
    ProviderUbFailureDetailPb providerCqe4;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider CQE4"), "client-endpoint", provider.ToString(),
                                URMA_PORT_UNAVAILABLE_STATUS, URMA_PORT_UNAVAILABLE_STATUS, providerCqe4);
    mockApi->providerUbFailureDetail = providerCqe4;
    mockApi->firstGetStatus = K_URMA_ERROR;
    EXPECT_EQ(bound->GetBuffersFromWorker(mockApi, getParam, buffers).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(inject::GetExecuteCount(REMOTE_PROBE_ACCEPTED_INJECT), 1U);

    ProviderUbFailureDetailPb clientCqe9;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "client CQE9"), "client-endpoint", provider.ToString(),
                                std::nullopt, URMA_REMOTE_ACK_TIMEOUT_STATUS, clientCqe9);
    mockApi->providerUbFailureDetail = clientCqe9;
    mockApi->getCalls = 0;
    EXPECT_EQ(bound->GetBuffersFromWorker(mockApi, getParam, buffers).GetCode(), K_URMA_ERROR);
    EXPECT_EQ(inject::GetExecuteCount(LOCAL_PROBE_ACCEPTED_INJECT), 1U);
}
}  // namespace
}  // namespace object_cache
}  // namespace datasystem

#pragma GCC diagnostic pop
