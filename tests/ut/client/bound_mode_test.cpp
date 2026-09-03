#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "datasystem/client/object_cache/bound_mode.h"
#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace object_cache {

class MockClientWorkerApi : public IClientWorkerApi {
public:
    MockClientWorkerApi()
        : client::IClientWorkerCommonApi(HostPort{}, HeartbeatType::RPC_HEARTBEAT, false,
                                         static_cast<Signature *>(nullptr)),
          IClientWorkerApi(HostPort{}, HeartbeatType::RPC_HEARTBEAT, false, static_cast<Signature *>(nullptr))
    {
    }

    std::shared_ptr<IClientWorkerApi> CloneWith(HostPort hostPort, HeartbeatType heartbeatType, SensitiveValue token, Signature *signature, std::string tenantId, bool enableCrossNodeConnection, std::shared_ptr<::datasystem::client::EmbeddedClientWorkerApi> api, void *worker) const override { return nullptr; }
    Status Create(const std::string &objectKey, int64_t dataSize, uint32_t &version, uint64_t &metadataSize, std::shared_ptr<ShmUnitInfo> &shmBuf, std::shared_ptr<UrmaRemoteAddrPb> &urmaDataInfo, const CacheType &cacheType, int32_t requestTimeoutMs) override { return Status::OK(); }
    Status Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isShm, bool isSeal, const std::unordered_set<std::string> &nestedKeys, uint32_t ttlSecond, int existence, int32_t requestTimeoutMs) override { return Status::OK(); }
    Status MultiPublish(const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo, const PublishParam &param, MultiPublishRspPb &rsp, const std::vector<const DeviceBlobList *> &deviceBlobRefs) override { return Status::OK(); }
    Status DecreaseWorkerRef(const std::vector<ShmKey> &objectKeys) override { return Status::OK(); }
    Status PipelineRH2D(PiplnRh2dParam &piplnRh2dParam, GetRspPb &rsp) override { return Status::OK(); }
    Status Get(const GetParam &getParam, uint32_t &version, GetRspPb &rsp, std::vector<RpcMessage> &payloads) override { return Status::OK(); }
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
class BoundModeTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        auto mock = std::make_shared<MockClientWorkerApi>();
        mockApi = mock;
        workerApi.resize(3);
        workerApi[static_cast<BoundMode::WorkerNode>(0)] = mock;
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
    std::atomic<BoundMode::WorkerNode> currentNode{ static_cast<BoundMode::WorkerNode>(0) };
    int32_t requestTimeoutMs = 1000;
    std::string tenantId = "tn0";
    SensitiveValue token;
    bool enableLocalCache = true;
    bool enableH2D = false;
    int parallismNum = 0;
    std::unique_ptr<BoundMode> bound;
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
}  // namespace
}  // namespace object_cache
}  // namespace datasystem

#pragma GCC diagnostic pop
