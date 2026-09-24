/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Unit tests for ClientWorkerRemoteApi OC-session recovery.
 *
 * Verifies:
 *   - Publish fast-fails with K_RPC_PEER_DEAD when the worker process is gone.
 *   - Publish recovers within the request budget after the worker restarts, without
 *     an explicit ReconnectWorker call (session rebuild + bounded retry).
 *   - ReconnectWorker rebuilds the OC session so subsequent Publish succeeds.
 *
 * The fake worker hosts the real generated brpc adapters (WorkerOCServiceBrpcAdapter /
 * WorkerServiceBrpcAdapter) on an in-process RpcServer, so the client runs the real
 * brpc channel + stub path. GetSocketPath returns an empty endpoint so the client
 * stays on pure TCP (no SHM fd handshake). AkSkManager has no keys configured, so
 * requests without signatures are accepted.
 */
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/client/object_cache/client_worker_api/client_worker_remote_api.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/object_posix.brpc.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/share_memory.brpc.pb.h"
#include "datasystem/protos/share_memory.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {

int GetFreeTcpPort()
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return 0;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = 0;
    if (bind(sock, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(sock);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (getsockname(sock, reinterpret_cast<struct sockaddr *>(&addr), &len) < 0) {
        close(sock);
        return 0;
    }
    int port = ntohs(addr.sin_port);
    close(sock);
    return port;
}

class FakeWorkerOcService : public IWorkerOCService {
public:
    Status HealthCheck(const HealthCheckRequestPb &req, HealthCheckReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Publish(const PublishReqPb &req, PublishRspPb &rsp, std::vector<RpcMessage> payload) override
    {
        (void)req;
        (void)payload;
        publishCount_.fetch_add(1, std::memory_order_relaxed);
        rsp.Clear();
        return Status::OK();
    }

    Status Create(const CreateReqPb &req, CreateRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Get(std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> writer) override
    {
        const auto status = getStatus_.load(std::memory_order_relaxed);
        if (status != K_OK) {
            return writer->SendStatus(Status(status, "injected worker Get status"));
        }
        GetRspPb rsp;
        return writer->Write(rsp);
    }

    Status QueryAndGet(std::shared_ptr<ServerUnaryWriterReader<QueryAndGetRspPb, QueryAndGetReqPb>> writer) override
    {
        (void)writer;
        return Status::OK();
    }

    Status DecreaseReference(const DecreaseReferenceRequest &req, DecreaseReferenceResponse &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status ReconcileShmRef(const ReconcileShmRefReqPb &req, ReconcileShmRefRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status DeleteAllCopy(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status InvalidateBuffer(const InvalidateBufferReqPb &req, InvalidateBufferRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status PublishDeviceObject(const PublishDeviceObjectReqPb &req, PublishDeviceObjectRspPb &rsp,
                               std::vector<RpcMessage> payload) override
    {
        (void)req;
        (void)rsp;
        (void)payload;
        return Status::OK();
    }

    Status GetDeviceObject(
        std::shared_ptr<ServerUnaryWriterReader<GetDeviceObjectRspPb, GetDeviceObjectReqPb>> writer) override
    {
        (void)writer;
        return Status::OK();
    }

    Status MultiPublish(const MultiPublishReqPb &req, MultiPublishRspPb &rsp,
                        std::vector<RpcMessage> payload) override
    {
        (void)req;
        (void)rsp;
        (void)payload;
        return Status::OK();
    }

    Status PutP2PMeta(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status SubscribeReceiveEvent(
        std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> writer)
        override
    {
        (void)writer;
        return Status::OK();
    }

    Status GetP2PMeta(std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> writer) override
    {
        (void)writer;
        return Status::OK();
    }

    Status SendRootInfo(const SendRootInfoReqPb &req, SendRootInfoRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status RecvRootInfo(
        std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> writer) override
    {
        (void)writer;
        return Status::OK();
    }

    Status GetDataInfo(std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> writer) override
    {
        (void)writer;
        return Status::OK();
    }

    Status AckRecvFinish(const AckRecvFinishReqPb &req, AckRecvFinishRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GetObjMetaInfo(const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status MultiCreate(const MultiCreateReqPb &req, MultiCreateRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status QuerySize(const QuerySizeReqPb &req, QuerySizeRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Exist(const ExistReqPb &req, ExistRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Expire(const ExpireReqPb &req, ExpireRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GetHashRing(const GetHashRingReqPb &req, GetHashRingRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status IncrSeqNo(const IncrSeqNoReqPb &req, IncrSeqNoRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GetSeqNo(const GetSeqNoReqPb &req, GetSeqNoRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status DelSeqNo(const DelSeqNoReqPb &req, DelSeqNoRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Lpush(const LpushRequestPb &req, LpushReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Lpop(const LpopRequestPb &req, LpopReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Lindex(const LindexRequestPb &req, LindexReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Llen(const LlenRequestPb &req, LlenReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Hget(const HgetRequestPb &req, HgetReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Hset(const HsetRequestPb &req, HsetReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Hdel(const HdelRequestPb &req, HdelReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Hgetall(const HgetallRequestPb &req, HgetallReplyPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    std::atomic<uint32_t> publishCount_{ 0 };
    std::atomic<StatusCode> getStatus_{ K_OK };
};

class FakeWorkerService : public IWorkerService {
public:
    explicit FakeWorkerService(std::string workerStartId) : workerStartId_(std::move(workerStartId)) {}

    Status GetClientFd(const GetClientFdReqPb &req, GetClientFdRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status GetSocketPath(const GetSocketPathReqPb &req, GetSocketPathRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        // Empty path + port 0: the client detects no SHM capability and stays on TCP.
        return Status::OK();
    }

    Status RegisterClient(const RegisterClientReqPb &req, RegisterClientRspPb &rsp) override
    {
        registerCount_.fetch_add(1, std::memory_order_relaxed);
        rsp.set_client_id(req.client_id().empty() ? "ut-client-id" : req.client_id());
        rsp.set_worker_start_id(workerStartId_);
        rsp.set_worker_uuid("ut-worker-uuid");
        rsp.set_lock_id(1);
        rsp.set_client_dead_timeout_s(60);
        return Status::OK();
    }

    Status DisconnectClient(const DisconnectClientReqPb &req, DisconnectClientRspPb &rsp) override
    {
        (void)req;
        (void)rsp;
        return Status::OK();
    }

    Status Heartbeat(const HeartbeatReqPb &req, HeartbeatRspPb &rsp) override
    {
        (void)req;
        rsp.set_worker_start_id(workerStartId_);
        return Status::OK();
    }

    std::atomic<uint32_t> registerCount_{ 0 };
    std::string workerStartId_;
};

class ClientWorkerRemoteApiReconnectTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        port_ = GetFreeTcpPort();
        ASSERT_GT(port_, 0);
        ASSERT_TRUE(StartFakeWorker().IsOk());
        signature_ = std::make_unique<Signature>();
        auto api = std::make_shared<object_cache::ClientWorkerRemoteApi>(
            HostPort("127.0.0.1", port_), HeartbeatType::NO_HEARTBEAT, "", signature_.get());
        ASSERT_TRUE(api->Init(kRequestTimeoutMs, kConnectTimeoutMs).IsOk());
        api_ = std::move(api);
    }

    void TearDown() override
    {
        api_.reset();
        StopFakeWorker();
    }

    Status StartFakeWorker()
    {
        ocService_ = std::make_unique<FakeWorkerOcService>();
        workerService_ = std::make_unique<FakeWorkerService>("ut-start-id-1");
        ocAdapter_ = std::make_unique<WorkerOCServiceBrpcAdapter>(*ocService_);
        workerAdapter_ = std::make_unique<WorkerServiceBrpcAdapter>(*workerService_);
        RpcServer::Builder builder;
        RETURN_IF_NOT_OK(builder.SetBrpcAddr("127.0.0.1", port_).Init(server_));
        RETURN_IF_NOT_OK(server_->AddBrpcService(ocAdapter_.get()));
        RETURN_IF_NOT_OK(server_->AddBrpcService(workerAdapter_.get()));
        RETURN_IF_NOT_OK(server_->StartBrpcServer("127.0.0.1", port_));
        return Status::OK();
    }

    void StopFakeWorker()
    {
        if (server_ != nullptr) {
            server_->StopBrpcServer();
            server_.reset();
        }
        workerAdapter_.reset();
        ocAdapter_.reset();
        workerService_.reset();
        ocService_.reset();
    }

    static std::shared_ptr<ObjectBufferInfo> MakeBufferInfo()
    {
        auto info = std::make_shared<ObjectBufferInfo>();
        info->objectKey = "issue1190-publish-key";
        info->dataSize = 1;
        return info;
    }

    Status PublishOnce(int32_t timeoutMs)
    {
        ScopedRequestContext requestCtx;
        ApiDeadlineGuard deadlineGuard(timeoutMs);
        GetRequestContext()->reqTimeoutDuration.Init(timeoutMs);
        return api_->Publish(MakeBufferInfo(), false, false, {}, 0, 0, timeoutMs);
    }

    Status GetOnce(int32_t timeoutMs, Status &ingressRpcStatus)
    {
        ScopedRequestContext requestCtx;
        ApiDeadlineGuard deadlineGuard(timeoutMs);
        GetRequestContext()->reqTimeoutDuration.Init(timeoutMs);
        const std::vector<std::string> objectKeys{ "get-key" };
        const std::vector<ReadParam> readParams;
        object_cache::GetParam getParam{ .objectKeys = objectKeys,
                                         .subTimeoutMs = timeoutMs,
                                         .readParams = readParams,
                                         .queryL2Cache = false,
                                         .requestTimeoutMs = timeoutMs };
        uint32_t version = 0;
        GetRspPb rsp;
        std::vector<RpcMessage> payloads;
        return api_->Get(getParam, version, rsp, payloads, &ingressRpcStatus);
    }

    static constexpr int32_t kRequestTimeoutMs = 5000;
    static constexpr int32_t kConnectTimeoutMs = 3000;

    std::unique_ptr<Signature> signature_;
    int port_ = 0;
    std::unique_ptr<FakeWorkerOcService> ocService_;
    std::unique_ptr<FakeWorkerService> workerService_;
    std::unique_ptr<WorkerOCServiceBrpcAdapter> ocAdapter_;
    std::unique_ptr<WorkerServiceBrpcAdapter> workerAdapter_;
    std::unique_ptr<RpcServer> server_;
    std::shared_ptr<object_cache::ClientWorkerRemoteApi> api_;
};

// With the worker process gone, Publish keeps the K_RPC_PEER_DEAD failure class
// (the caller must still see peer-dead, not a budget-expiry rewrite) and stays
// bounded by the request budget.
TEST_F(ClientWorkerRemoteApiReconnectTest, PublishFailsFastWhenWorkerDown)
{
    StopFakeWorker();

    Timer timer;
    const auto rc = PublishOnce(kRequestTimeoutMs);
    const auto elapsedMs = timer.ElapsedMilliSecond();

    ASSERT_EQ(rc.GetCode(), StatusCode::K_RPC_PEER_DEAD) << rc.ToString();
    ASSERT_LT(elapsedMs, kRequestTimeoutMs + 500) << rc.ToString();
}

TEST_F(ClientWorkerRemoteApiReconnectTest, GetSeparatesServerStatusFromIngressFailure)
{
    ocService_->getStatus_.store(K_RPC_PEER_DEAD, std::memory_order_relaxed);
    Status ingressRpcStatus(K_UNKNOWN_ERROR, "not initialized");

    const auto downstreamRc = GetOnce(1000, ingressRpcStatus);

    EXPECT_EQ(downstreamRc.GetCode(), K_RPC_PEER_DEAD) << downstreamRc.ToString();
    EXPECT_TRUE(ingressRpcStatus.IsOk()) << ingressRpcStatus.ToString();

    StopFakeWorker();
    const auto ingressRc = GetOnce(1000, ingressRpcStatus);
    EXPECT_EQ(ingressRc.GetCode(), K_RPC_PEER_DEAD) << ingressRc.ToString();
    EXPECT_EQ(ingressRpcStatus.GetCode(), K_RPC_PEER_DEAD) << ingressRpcStatus.ToString();
}

// After the worker process restarts, Publish recovers inside the same call's
// request budget — no explicit ReconnectWorker from the caller. The never-restarts
// tail also proves the bounded-retry budget is respected.
TEST_F(ClientWorkerRemoteApiReconnectTest, PublishRecoversAfterServerRestartWithinBudget)
{
    StopFakeWorker();

    Status restartRc;
    std::thread restarter([this, &restartRc] {
        constexpr int restartDelayMs = 800;
        std::this_thread::sleep_for(std::chrono::milliseconds(restartDelayMs));
        restartRc = StartFakeWorker();
    });

    const auto rc = PublishOnce(kRequestTimeoutMs);
    restarter.join();
    ASSERT_TRUE(restartRc.IsOk()) << restartRc.ToString();
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();

    // Bounded retry: a worker that never comes back must fail within the budget,
    // not hang past the deadline.
    StopFakeWorker();
    Timer timer;
    const auto deadRc = PublishOnce(1000);
    const auto elapsedMs = timer.ElapsedMilliSecond();
    ASSERT_EQ(deadRc.GetCode(), StatusCode::K_RPC_PEER_DEAD) << deadRc.ToString();
    ASSERT_LT(elapsedMs, 1500) << deadRc.ToString();
}

// ReconnectWorker rebuilds the OC session so Publish succeeds right after the
// worker restarts, without any Publish-level retry.
TEST_F(ClientWorkerRemoteApiReconnectTest, ReconnectWorkerRebuildsOcSession)
{
    StopFakeWorker();
    ASSERT_TRUE(StartFakeWorker().IsOk());

    ASSERT_TRUE(api_->ReconnectWorker({}).IsOk());
    const auto rc = PublishOnce(kRequestTimeoutMs);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_EQ(workerService_->registerCount_.load(), 1u);
    EXPECT_EQ(ocService_->publishCount_.load(), 1u);
}
}  // namespace
}  // namespace ut
}  // namespace datasystem
