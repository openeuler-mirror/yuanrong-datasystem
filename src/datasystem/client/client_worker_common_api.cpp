/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Implement the worker client class to communicate with the common worker service.
 */
#include "datasystem/client/client_worker_common_api.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/eventloop/timer_queue.h"
#ifdef WITH_TESTS
#include "datasystem/common/inject/inject_point.h"
#endif
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/spdlog/log_rate_limiter.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/rpc/zmq/exclusive_conn_mgr.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/fd_pass.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/version.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_worker_transport_api.h"

namespace datasystem {
namespace {
static constexpr int64_t MIN_WAIT_MS = 1;

int32_t CalculateSingleRpcTimeout(int32_t totalTimeoutMs)
{
    constexpr int32_t rpcMaxTimeout = 600000;    // 10min
    constexpr int32_t shorterSplitTime = 30000;  // 30s
    constexpr int32_t longerSplitTime = 90000;   // 90s
    constexpr int32_t retryTimes = 3;

    if (totalTimeoutMs <= shorterSplitTime) {
        return totalTimeoutMs;
    } else if (totalTimeoutMs <= longerSplitTime) {
        return shorterSplitTime;
    } else {
        return std::min(totalTimeoutMs / retryTimes, rpcMaxTimeout);
    }
}

const char *ShmEnableTypeName(ShmEnableType type)
{
    switch (type) {
        case ShmEnableType::UDS:
            return "Unix domain socket";
        case ShmEnableType::SCMTCP:
            return "SCMTCP";
        case ShmEnableType::NONE:
        default:
            return "TCP";
    }
}

}  // namespace

namespace client {
void ClientWorkerCommonApiAttribute::SetHeartbeatProperties(int32_t timeoutMs, const RegisterClientRspPb &rsp)
{
    std::vector<uint64_t> heartBeatTimeoutMsOptions = { static_cast<uint64_t>(timeoutMs), MAX_HEARTBEAT_TIMEOUT_MS };
    uint64_t clientDeadTimeoutMs = rsp.client_dead_timeout_s() * TO_MILLISECOND;
    // Compatible with old versions of worker
    clientDeadTimeoutMs_ = clientDeadTimeoutMs == 0ul ? MIN_HEARTBEAT_TIMEOUT_MS : clientDeadTimeoutMs;
    if (clientDeadTimeoutMs > 0) {
        const int32_t retryNum = 3;  // Make sure the heartbeat is retried twice before worker timeout.
        heartBeatTimeoutMsOptions.emplace_back(clientDeadTimeoutMs / retryNum);
    }
    heartBeatTimeoutMs_ = *std::min_element(heartBeatTimeoutMsOptions.begin(), heartBeatTimeoutMsOptions.end());
    uint64_t clientReconnectWaitMs = rsp.client_reconnect_wait_s() * TO_MILLISECOND;
    const int32_t reduceRatio = 5;
    heartBeatIntervalMs_ =
        std::min({ std::max(clientDeadTimeoutMs / reduceRatio, static_cast<uint64_t>(MIN_HEARTBEAT_INTERVAL_MS)),
                   static_cast<uint64_t>(MAX_HEARTBEAT_INTERVAL_MS),
                   clientReconnectWaitMs > 0 ? clientReconnectWaitMs / reduceRatio : UINT64_MAX });
}

// Static/global id generator init
std::atomic<int32_t> ClientWorkerRemoteCommonApi::exclusiveIdGen_ = 0;

IClientWorkerCommonApi::~IClientWorkerCommonApi() = default;
ClientWorkerCommonApiAttribute::~ClientWorkerCommonApiAttribute() = default;

ClientWorkerLocalCommonApi::ClientWorkerLocalCommonApi(
    HostPort hostPort, std::shared_ptr<::datasystem::client::EmbeddedClientWorkerApi> api, void *worker,
    HeartbeatType heartbeatType, bool enableCrossNodeConnection, Signature *signature, std::string deviceId)
    : IClientWorkerCommonApi(std::move(hostPort), heartbeatType, false, signature),
      api_(std::move(api)),
      worker_(worker),
      deviceId_(std::move(deviceId))
{
    (void)enableCrossNodeConnection;
}

Status ClientWorkerLocalCommonApi::Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs, uint64_t fastTransportSize)
{
    (void)fastTransportSize;
    requestTimeoutMs_ = requestTimeoutMs;
    connectTimeoutMs_ = connectTimeoutMs;
    workerService_ = api_->GetWorkerService(worker_);
    RegisterClientReqPb req;
    RETURN_IF_NOT_OK(Connect(req, connectTimeoutMs));
    return Status::OK();
}

Status ClientWorkerLocalCommonApi::SendHeartbeat(bool &workerReboot, bool &clientRemoved, int64_t remainTime,
                                                 bool &isWorkerVoluntaryScaleDown,
                                                 const std::vector<int64_t> &releasedFds,
                                                 std::vector<int64_t> &expiredWorkerFds)
{
    (void)remainTime;
    (void)releasedFds;
    (void)expiredWorkerFds;
    HeartbeatReqPb req;
    HeartbeatRspPb rsp;
    req.set_client_id(clientId_);
    req.set_removable(removable_.load(std::memory_order_relaxed));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    Status status = api_->WorkerHeartbeat(workerService_, req, rsp);
    workerReboot = false;
    clientRemoved = false;
    isWorkerVoluntaryScaleDown = false;
    if (status.IsOk()) {
        clientRemoved = rsp.client_removed();
        isWorkerVoluntaryScaleDown = rsp.is_voluntary_scale_down();
        SetHealthy(!rsp.unhealthy());
    }
    return status;
}

Status ClientWorkerLocalCommonApi::GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds,
                                               const std::string &tenantId)
{
    (void)workerFds;
    (void)clientFds;
    (void)tenantId;
    RETURN_STATUS(K_RUNTIME_ERROR, "Not support GetClientFd in embedded scenario");
}

Status ClientWorkerLocalCommonApi::UpdateToken(SensitiveValue &token)
{
    (void)token;
    RETURN_STATUS(K_RUNTIME_ERROR, "Not support UpdateToken in embedded scenario");
}

Status ClientWorkerLocalCommonApi::UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey)
{
    (void)accessKey;
    (void)secretKey;
    RETURN_STATUS(K_RUNTIME_ERROR, "Not support UpdateAkSk in embedded scenario");
}

std::vector<HostPort> ClientWorkerLocalCommonApi::GetStandbyWorkers()
{
    LOG(ERROR) << "Not support GetStandbyWorkers in embedded scenario";
    return {};
}

Status ClientWorkerLocalCommonApi::Disconnect(bool isDestruct)
{
    (void)isDestruct;
    LOG(INFO) << FormatString("Client %s sends exit notice to worker.", clientId_);
    DisconnectClientReqPb req;
    DisconnectClientRspPb rsp;
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    return api_->WorkerDisconnectClient(workerService_, req, rsp);
}

Status ClientWorkerLocalCommonApi::Reconnect()
{
    RETURN_STATUS(K_RUNTIME_ERROR, "Not support Reconnect in embedded scenario");
}

Status ClientWorkerLocalCommonApi::Connect(RegisterClientReqPb &req, int32_t timeoutMs, bool reconnection)
{
    (void)reconnection;
    shmEnableType_ = ShmEnableType::UDS;
    heartbeatType_ = HeartbeatType::RPC_HEARTBEAT;
    req.set_server_fd(INVALID_SOCKET_FD);
    req.set_version(DATASYSTEM_VERSION);
    req.set_git_hash(GetGitHash());
    req.set_heartbeat_enabled(heartbeatType_ != HeartbeatType::NO_HEARTBEAT);
    req.set_shm_enabled(IsShmEnable());
    req.set_tenant_id("");
    req.set_enable_cross_node(enableCrossNodeConnection_);
    req.set_enable_exclusive_connection(false);
    req.set_pod_name(Logging::PodName());
    req.set_device_id(deviceId_);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RegisterClientRspPb rsp;
    Status status;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(api_->WorkerRegisterClient(workerService_, req, rsp), "Register client failed");
    VLOG(1) << "Register response: " << rsp.DebugString();
    enableHugeTlb_ = rsp.enable_huge_tlb();
    clientId_ = rsp.client_id();
    lockId_ = rsp.lock_id();
    (void)workerVersion_.fetch_add(1, std::memory_order_relaxed);
    shmThreshold_ = rsp.shm_threshold();
    workerId_ = rsp.worker_uuid();
    workerEnableP2Ptransfer_ = rsp.enable_p2p_transfer();
    SetHealthy(!rsp.unhealthy());
    SetHeartbeatProperties(timeoutMs, rsp);
    LogRateLimiter::Instance().SetRate(rsp.log_rate_limit());
    LOG(INFO) << "Sync client log_rate_limit from worker register response: " << rsp.log_rate_limit();
    return Status::OK();
}

ClientWorkerRemoteCommonApi::ClientWorkerRemoteCommonApi(HostPort hostPort, RpcCredential cred,
                                                         HeartbeatType heartbeatType, SensitiveValue token,
                                                         Signature *signature, std::string tenantId,
                                                         bool enableCrossNodeConnection, bool enableExclusiveConnection,
                                                         std::string deviceId)
    : IClientWorkerCommonApi(std::move(hostPort), heartbeatType, enableCrossNodeConnection, signature),
      cred_(cred),
      tenantId_(std::move(tenantId)),
      enableExclusiveConnection_(enableExclusiveConnection),
      deviceId_(std::move(deviceId))
{
    recvPageThread_ = Thread(&ClientWorkerRemoteCommonApi::RecvPageFd, this);
    clientAccessToken_ = std::make_unique<ClientAccessToken>(std::move(token));
    urmaHandshakePool_ = std::make_unique<ThreadPool>(0, 1, "urma_handshake");
    urmaHandshakeRetryPool_ = std::make_unique<ThreadPool>(0, 1, "urma_handshake_retry");
}

ClientWorkerRemoteCommonApi::~ClientWorkerRemoteCommonApi()
{
    stopUrmaHandshakeRetry_ = true;
    recvClientFdState_.stopRecvPageFd = true;
    recvClientFdState_.recvPageWaitPost->Set();
    recvClientFdState_.recvPageNotify->Set();
    CloseSocketFd();
    urmaHandshakePool_.reset();
    urmaHandshakeRetryPool_.reset();
    if (recvPageThread_.joinable()) {
        recvPageThread_.join();
    }
}

void ClientWorkerRemoteCommonApi::SetRpcTimeout(int32_t timeout)
{
    rpcTimeoutMs_ = CalculateSingleRpcTimeout(timeout);
    LOG(INFO) << "The total timeout is " << timeout << " ms, single rpc timeout is " << rpcTimeoutMs_ << " ms";
}

Status ClientWorkerRemoteCommonApi::Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs, uint64_t fastTransportSize)
{
    CHECK_FAIL_RETURN_STATUS(
        connectTimeoutMs >= RPC_MINIMUM_TIMEOUT, StatusCode::K_INVALID,
        FormatString("The connectTimeoutMs(%d) should be greater than or equal to %d milliseconds.", connectTimeoutMs,
                     RPC_MINIMUM_TIMEOUT));
    CHECK_FAIL_RETURN_STATUS(
        requestTimeoutMs >= 0, StatusCode::K_INVALID,
        FormatString("The requestTimeoutMs(%d) should be greater than or equal to 0 milliseconds.", requestTimeoutMs));
    requestTimeoutMs_ = requestTimeoutMs;
    connectTimeoutMs_ = connectTimeoutMs;
    fastTransportMemSize_ = fastTransportSize;
    recvClientFdState_.getClientFdTimeoutMs = connectTimeoutMs;
    SetRpcTimeout(requestTimeoutMs);
    VLOG(1) << "Client start to connect worker";
    CHECK_FAIL_RETURN_STATUS(TimerQueue::GetInstance()->Initialize(), K_RUNTIME_ERROR, "TimerQueue init failed!");
    RegisterClientReqPb req;
    RETURN_IF_NOT_OK(Connect(req, connectTimeoutMs));
    VLOG(1) << "The new client id is: " << clientId_;
    return Status::OK();
}

Status ClientWorkerRemoteCommonApi::Connect(RegisterClientReqPb &req, int32_t timeoutMs, bool reconnection)
{
    auto channel = std::make_shared<RpcChannel>(hostPort_, cred_);
    commonWorkerSession_ = std::make_unique<WorkerService_Stub>(channel);

    bool isConnectSuccess = false;
    int32_t serverFd = INVALID_SOCKET_FD;
    int32_t socketFd = INVALID_SOCKET_FD;
    ShmEnableType shmEnableType = ShmEnableType::NONE;
    bool mustUds = heartbeatType_ == HeartbeatType::UDS_HEARTBEAT || (reconnection && IsShmEnable());
#ifdef WITH_TESTS
    INJECT_POINT_NO_RETURN("ClientWorkerCommonApi.Connect.MustUds", [&mustUds] { mustUds = true; });
#endif
    RETURN_IF_NOT_OK(CreateConnectionForTransferShmFd(timeoutMs, isConnectSuccess, serverFd, socketFd, shmEnableType));
    if (mustUds && !isConnectSuccess) {
        return { StatusCode::K_RPC_UNAVAILABLE,
                 "[SHM_FD_TRANSFER_FAILED] Can not create connection to worker for shm fd transfer." };
    }

    if (isConnectSuccess) {
        LOG(INFO) << "Client and worker support transfer data through shared memory and the fd send over "
                  << ShmEnableTypeName(shmEnableType) << ", socketFd: " << socketFd << ", serverFd: " << serverFd;
    }

    CloseSocketFd();

    shmEnableType_ = isUseStandbyWorker_ ? ShmEnableType::NONE : shmEnableType;
    socketFd_ = socketFd;
    if (isConnectSuccess) {
        heartbeatType_ = heartbeatType_ == HeartbeatType::NO_HEARTBEAT ? HeartbeatType::RPC_HEARTBEAT : heartbeatType_;
    }
    req.set_server_fd(serverFd);
    RETURN_IF_NOT_OK(RegisterClient(req, timeoutMs));
    LOG(INFO) << FormatString("Register client to worker through the %s successfully, client id: %s",
                              ShmEnableTypeName(shmEnableType_), clientId_);
    return Status::OK();
}

bool ClientWorkerRemoteCommonApi::PrepareShmTransferEndpoint(const GetSocketPathRspPb &reply, std::string &endpointStr,
                                                             ShmEnableType &shmEnableType)
{
    uint32_t shmWorkerPort = reply.shm_worker_port();
    if (shmWorkerPort > 0) {
        endpointStr = FormatString("tcp://%s:%d", hostPort_.Host(), shmWorkerPort);
        shmEnableType = ShmEnableType::SCMTCP;
        return true;
    }

    if (!reply.path().empty()) {
        endpointStr = FormatString("ipc://%s", reply.path());
        shmEnableType = ShmEnableType::UDS;
        return true;
    }

    shmEnableType = ShmEnableType::NONE;
    LOG(INFO) << "Both the uds socket path and shm_worker_port is empty, cannot transfer data through shm between "
                 "client and worker.";
    return false;
}

Status ClientWorkerRemoteCommonApi::CreateHandShakeFunc(UnixSockFd &fd, const std::string &endpoint, int32_t &serverFd)
{
    LOG(INFO) << FormatString("Try connect worker for shm fd transfer, endpoint: %s", endpoint);
    RETURN_IF_NOT_OK(fd.Connect(endpoint));
    uint32_t tmpServerFd;
    // Get the server side fd and add this to the RegisterClientReqPb
    RETURN_IF_NOT_OK(fd.SetTimeout(STUB_FRONTEND_TIMEOUT));
    RETURN_IF_NOT_OK(fd.Recv32(tmpServerFd, false));
#ifdef WITH_TESTS
    INJECT_POINT("ClientWorkerCommonApi.CreateHandShakeFunc");
#endif
    CHECK_FAIL_RETURN_STATUS(tmpServerFd <= INT32_MAX, K_RUNTIME_ERROR, "Server fd exceed range of int32_t");
    serverFd = static_cast<int32_t>(tmpServerFd);  // The FD sent by the worker is of the int type.
    if (shmWorkerPort_ > 0) {
        // check the client and worker in the same node.
        std::vector<int> clientFds;
        uint64_t requestId;
        RETURN_IF_NOT_OK_APPEND_MSG(SockRecvFd(fd.GetFd(), true, clientFds, requestId), "Recv fd failed");
        // after check connection, direct close the received fds.
        for (int fd : clientFds) {
            RETRY_ON_EINTR(close(fd));
        }
    }
    LOG(INFO) << FormatString("Connects to local server %s successfully. Client fd %d. Server fd %d.", endpoint,
                              fd.GetFd(), serverFd);
    // Change the timeout back to the default.
    return fd.SetTimeout(0);
}

Status ClientWorkerRemoteCommonApi::CreateConnectionForTransferShmFd(int32_t timeoutMs, bool &isConnectSuccess,
                                                                     int32_t &serverFd, int32_t &socketFd,
                                                                     ShmEnableType &shmEnableType)
{
    Timer timer(timeoutMs);
    GetSocketPathReqPb req;
    RETURN_IF_NOT_OK(SetToken(req));
    req.set_tenant_id(tenantId_);
    GetSocketPathRspPb reply;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    auto rc = RetryOnError(
        timer.GetRemainingTimeMs(),
        [this, &req, &reply](int32_t realRpcTimeout) {
            INJECT_POINT("client.get_sock.fail");
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            return commonWorkerSession_->GetSocketPath(opts, req, reply);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE },
        CalculateSingleRpcTimeout(timeoutMs));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Get socket path failed.");

    isConnectSuccess = false;
    shmEnableType = ShmEnableType::NONE;

    std::string endpointStr;
    // Determine how to reach the worker for shared memory transfer.
    if (!PrepareShmTransferEndpoint(reply, endpointStr, shmEnableType)) {
        return Status::OK();
    }

    shmWorkerPort_ = reply.shm_worker_port();
    UnixSockFd sock(RPC_NO_FILE_FD, shmWorkerPort_ > 0);
    auto func = [this, &sock, &endpointStr, &serverFd](int32_t) {
        Status status = CreateHandShakeFunc(sock, endpointStr, serverFd);
        if (status.IsError()) {
            LOG(WARNING) << FormatString(
                "Client can not connect to server for shm fd transfer within allowed time (%dms). Socket path: "
                "%s, Detail: %s",
                STUB_FRONTEND_TIMEOUT, endpointStr, status.GetMsg());
            sock.Close();
        }
        return status;
    };
    rc = RetryOnError(timer.GetRemainingTimeMs(), func, [] { return Status::OK(); }, { StatusCode::K_TRY_AGAIN });
    if (rc.IsOk()) {
        isConnectSuccess = true;
        socketFd = sock.GetFd();
    } else {
        LOG(INFO) << "Failed connect to local worker via " << ShmEnableTypeName(shmEnableType)
                  << ", client and worker maybe not in the same node, falling back to TCP";
        shmEnableType = ShmEnableType::NONE;
        shmWorkerPort_ = 0;
    }
    return Status::OK();
}

void ClientWorkerRemoteCommonApi::SaveStandbyWorker(
    const std::string standbyWorker, const ::google::protobuf::RepeatedPtrField<std::string> &availableWorkers)
{
    if (!enableCrossNodeConnection_) {
        return;
    }
    std::lock_guard<std::shared_timed_mutex> l(standbyWorkerMutex_);
    standbyWorkerAddrs_.clear();
    for (const auto &addr : availableWorkers) {
        HostPort workerAddr;
        if (workerAddr.ParseString(addr).IsError()) {
            VLOG(HEARTBEAT_LEVEL) << FormatString("Standby worker address %s is invalid.", addr);
            continue;
        }
        (void)standbyWorkerAddrs_.emplace(workerAddr);
    }
    if (!standbyWorker.empty()) {
        HostPort workerAddr;
        if (workerAddr.ParseString(standbyWorker).IsOk()) {
            (void)standbyWorkerAddrs_.emplace(workerAddr);
        } else {
            VLOG(HEARTBEAT_LEVEL) << FormatString("Standby worker address %s is invalid.", standbyWorker);
        }
    }
}

std::vector<HostPort> ClientWorkerRemoteCommonApi::GetStandbyWorkers()
{
    std::vector<HostPort> workers;
    {
        std::shared_lock<std::shared_timed_mutex> l(standbyWorkerMutex_);
        for (const auto &addr : standbyWorkerAddrs_) {
            workers.emplace_back(addr);
        }
    }
    static std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
    std::shuffle(workers.begin(), workers.end(), gen);
    return workers;
}

Status ClientWorkerRemoteCommonApi::SendHeartbeat(bool &workerReboot, bool &clientRemoved, int64_t remainTimeMs,
                                                  bool &isWorkerVoluntaryScaleDown,
                                                  const std::vector<int64_t> &releasedFds,
                                                  std::vector<int64_t> &expiredWorkerFds)
{
    HeartbeatReqPb req;
    HeartbeatRspPb rsp;
    req.set_client_id(clientId_);
    req.set_removable(removable_.load(std::memory_order_relaxed));
    RETURN_IF_NOT_OK(SetToken(req));
    *req.mutable_released_worker_fds() = { releasedFds.begin(), releasedFds.end() };

    RpcOptions opts;
    //  If the value is small, the worker restart scenario can be quickly detected.
    if (remainTimeMs < heartBeatTimeoutMs_ && remainTimeMs > 0) {
        opts.SetTimeout(remainTimeMs);
    } else {
        opts.SetTimeout(heartBeatTimeoutMs_);
    }
#ifdef WITH_TESTS
    INJECT_POINT("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", [&opts](int time) {
        opts.SetTimeout(time);
        return Status::OK();
    });
#endif
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    Status status = commonWorkerSession_->Heartbeat(opts, req, rsp);
    workerReboot = false;
    clientRemoved = false;
    isWorkerVoluntaryScaleDown = false;
    if (status.IsOk()) {
        workerReboot = !workerStartId_.empty() && rsp.worker_start_id() != workerStartId_;
        if (workerReboot) {
            storeNotifyReboot_ = true;
        }
        clientRemoved = rsp.client_removed();
        isWorkerVoluntaryScaleDown = rsp.is_voluntary_scale_down();
        SaveStandbyWorker(rsp.standby_worker(), rsp.available_workers());
        expiredWorkerFds = { rsp.expired_worker_fds().begin(), rsp.expired_worker_fds().end() };
        SetHealthy(!rsp.unhealthy());
    }
    return status;
}

void ClientWorkerRemoteCommonApi::ConstructDecShmUnit(const RegisterClientRspPb &rsp)
{
    if (IsShmEnable()) {
        decShmUnit_ = std::make_shared<ShmUnitInfo>();
        decShmUnit_->fd = rsp.store_fd();
        decShmUnit_->mmapSize = rsp.mmap_size();
        decShmUnit_->offset = static_cast<ptrdiff_t>(rsp.offset());
        decShmUnit_->id = ShmKey::Intern(rsp.shm_id());
    }
}

void ClientWorkerRemoteCommonApi::CloseSocketFd()
{
    if (socketFd_ == INVALID_SOCKET_FD) {
        return;
    }
    int fd = socketFd_;
    int ret = shutdown(fd, SHUT_RDWR);
    VLOG(1) << FormatString("shutdown socket fd:%d, ret: %d ", fd, ret);
    if (ret == -1) {
        LOG(ERROR) << "Close the old socket fd failed, fd = " << fd << " errno = " << errno;
    }
    RETRY_ON_EINTR(close(fd));
#ifdef WITH_TESTS
    INJECT_POINT("client.CloseSocketFd",
                 [](int delayMs) { std::this_thread::sleep_for(std::chrono::milliseconds(delayMs)); });
#endif
    socketFd_ = INVALID_SOCKET_FD;
}

Status ClientWorkerRemoteCommonApi::RegisterClient(RegisterClientReqPb &req, int32_t timeoutMs)
{
    RETURN_IF_NOT_OK(SetToken(req));
    req.set_version(DATASYSTEM_VERSION);
    req.set_git_hash(GetGitHash());
    req.set_heartbeat_enabled(heartbeatType_ != HeartbeatType::NO_HEARTBEAT);
    req.set_shm_enabled(IsShmEnable());
    req.set_tenant_id(tenantId_);
    req.set_enable_cross_node(enableCrossNodeConnection_);
    req.set_enable_exclusive_connection(enableExclusiveConnection_);
    req.set_pod_name(Logging::PodName());
    req.set_support_multi_shm_ref_count(true);
    req.set_device_id(deviceId_);
#ifdef WITH_TESTS
    INJECT_POINT("client.RegisterClient.multi_shm_ref_count", [&req](bool multiShmRefCount) {
        req.set_support_multi_shm_ref_count(multiShmRefCount);
        return Status::OK();
    });
#endif
    RegisterClientRspPb rsp;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    LOG(INFO) << "Start to send rpc to register client to worker, shm enable: " << IsShmEnable()
              << ", enable cross node: " << enableCrossNodeConnection_ << ", tenant id: " << tenantId_
              << ", auth: " << signature_->ToString()
              << ", client support multi shm ref count:" << req.support_multi_shm_ref_count();
    auto rc = RetryOnError(
        timeoutMs,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            INJECT_POINT("client.register.fail");
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            return commonWorkerSession_->RegisterClient(opts, req, rsp);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE },
        CalculateSingleRpcTimeout(timeoutMs));
    if (rc.GetCode() == K_SERVER_FD_CLOSED) {
        auto msg = rc.GetMsg();
        rc = Status(K_TRY_AGAIN, std::move(msg));
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Register client failed");
    VLOG(1) << "Register response: " << rsp.DebugString();
    PostRegisterClient(timeoutMs, rsp);
    return Status::OK();
}

Status ClientWorkerRemoteCommonApi::UpdateToken(SensitiveValue &token)
{
    LOG(INFO) << "update token for client, clientId: " << clientId_;
    CHECK_FAIL_RETURN_STATUS(!token.Empty(), K_INVALID, "token is empty");
    clientAccessToken_->UpdateToken(token);
    return Status::OK();
}

Status ClientWorkerRemoteCommonApi::UpdateAkSk(const std::string &accessKey, SensitiveValue &secretKey)
{
    std::hash<std::string> hasher;
    LOG(INFO) << FormatString("[%s] update ak/sk (ak hash: %s, sk hash: %s)", tenantId_, hasher(accessKey),
                              GetTruncatedStr(std::to_string(hasher(secretKey.GetData()))));
    CHECK_FAIL_RETURN_STATUS(!accessKey.empty(), K_INVALID, "accessKey is empty");
    CHECK_FAIL_RETURN_STATUS(!secretKey.Empty(), K_INVALID, "secretKey is empty");
    return signature_->SetClientAkSk(accessKey, secretKey);
}

Status ClientWorkerRemoteCommonApi::Disconnect(bool isDestruct)
{
    CHECK_FAIL_RETURN_STATUS(commonWorkerSession_ != nullptr, StatusCode::K_OK,
                             "No active connection. Do not send disconnect notice.");
    LOG(INFO) << FormatString("Client %s sends exit notice to worker.", clientId_);
    DisconnectClientReqPb req;
    DisconnectClientRspPb rsp;
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK(SetToken(req));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    // Millions of objects will cost worker dozens of seconds to process, set 10 min RPC timeout to prevent the client
    // from timeout error while the worker doesn't finish processing.
    int rpcTimeoutMs = 10 * 60 * 1000;
#ifdef WITH_TESTS
    INJECT_POINT("ClientWorkerCommonApi.Disconnect.ShutdownQuickily", [&rpcTimeoutMs](int time) {
        rpcTimeoutMs = time;
        return Status::OK();
    });
#endif
    RpcOptions opts;
    opts.SetTimeout(rpcTimeoutMs);
    RETURN_IF_NOT_OK(commonWorkerSession_->DisconnectClient(opts, req, rsp));
    if (!isDestruct && enableExclusiveConnection_ && exclusiveId_.has_value()) {
        LOG_IF_ERROR(gExclusiveConnMgr.CloseExclusiveConn(exclusiveId_.value()),
                     FormatString("Failed to close exclusive connection %d", exclusiveId_.value()));
    }
    return Status::OK();
}

void ClientWorkerRemoteCommonApi::RecvFdAfterNotify(const std::vector<int> &workerFds, uint64_t requestId,
                                                    const Timer &timer, std::vector<int> &clientFds)
{
    bool isRetry = false;
    do {
        if (isRetry) {
            recvClientFdState_.recvPageWaitPost->Set();
        }
        recvClientFdState_.recvPageNotify->WaitFor(timer.GetRemainingTimeMs());
        recvClientFdState_.recvPageNotify->Clear();
        {
            std::lock_guard<std::mutex> lck(recvClientFdState_.mutex);
            auto itr = recvClientFdState_.requestId2UnmmapedClientFds.find(requestId);
            if (itr == recvClientFdState_.requestId2UnmmapedClientFds.end()) {
                isRetry = true;
                continue;
            }
            if (itr->second.first.size() != workerFds.size()) {
                LOG(ERROR) << "Get client fd from worker failed";
                return;
            }
            clientFds = std::move(itr->second.first);
            recvClientFdState_.requestId2UnmmapedClientFds.erase(itr);
        }
    } while (clientFds.empty() && timer.GetRemainingTimeMs() > 0);
}

Status ClientWorkerRemoteCommonApi::GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds,
                                                const std::string &tenantId)
{
    if (!IsShmEnable() || socketFd_ == INVALID_SOCKET_FD) {
        return { K_RUNTIME_ERROR, "Current client can not support uds, so query client fd failed." };
    }
#ifdef WITH_TESTS
    PerfPoint point(PerfKey::RPC_WORKER_GET_CLIENT_FDS);
#endif
    GetClientFdReqPb req;
    GetClientFdRspPb rsp;
    req.set_client_id(clientId_);
    auto requestId = ++recvClientFdState_.requestId;  // 0 is used for compatibility with old versions.
    req.set_request_id(requestId);
    for (auto workerFd : workerFds) {
        req.add_worker_fds(static_cast<google::protobuf::int32>(workerFd));
    }
    recvClientFdState_.recvPageWaitPost->Set();

    VLOG(1) << "Start to query page fd, socket fd: " << socketFd_
            << ", worker fds: " << VectorToString(req.worker_fds());
    Timer time = Timer(recvClientFdState_.getClientFdTimeoutMs);
    RpcOptions opts;
    opts.SetTimeout(recvClientFdState_.getClientFdTimeoutMs);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(recvClientFdState_.getClientFdTimeoutMs));
    if (!tenantId.empty()) {
        req.set_tenant_id(tenantId);
        RETURN_IF_NOT_OK(SetToken(req));
    } else {
        RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    }
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));

    // Notice: GetClientFd should not be retried. Otherwise, Fd resources may remain or the process may be suspended.
    Status status = commonWorkerSession_->GetClientFd(opts, req, rsp);
#ifdef WITH_TESTS
    INJECT_POINT("ClientWorkerCommonApi.GetClientFd.preReceive");
#endif
    if (status.IsOk()) {
        RecvFdAfterNotify(workerFds, requestId, time, clientFds);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!clientFds.empty(), K_RUNTIME_ERROR,
                                         FormatString("Receive fd[%s] from %d failed, detail: %s",
                                                      VectorToString(workerFds), socketFd_, status.ToString()));
    VLOG(1) << FormatString("Receive fd[%s] from socket[%d] success, res: %s ", VectorToString(workerFds), socketFd_,
                            VectorToString(clientFds));
#ifdef WITH_TESTS
    point.Record();
#endif
    return Status::OK();
}

void ClientWorkerRemoteCommonApi::PostRecvPageFd(const Status &status, uint64_t requestId,
                                                 const std::vector<int> &pageFds)
{
    if (status.IsOk()) {
        {
            std::lock_guard<std::mutex> lck(recvClientFdState_.mutex);
            recvClientFdState_.requestId2UnmmapedClientFds.emplace(
                (requestId == 0 ? recvClientFdState_.requestId.load() : requestId),
                std::make_pair(pageFds, GetSteadyClockTimeStampMs()));
        }
        LOG_IF(WARNING, pageFds.empty()) << "The received fd list is empty, may exceed the max open fd limit.";
    } else {
        LOG(WARNING) << "recv page fd from socketFd_ " << socketFd_ << " failed, status:" << status.ToString();
    }

#ifdef WITH_TESTS
    INJECT_POINT("ClientWorkerCommonApi.RecvPageFd",
                 [](int32_t time) { std::this_thread::sleep_for(std::chrono::milliseconds(time)); });
#endif
    VLOG(1) << "Finish to receive page fds: " << VectorToString(pageFds);
}

void ClientWorkerRemoteCommonApi::CloseExpiredFd()
{
    std::lock_guard<std::mutex> lck(recvClientFdState_.mutex);
    for (auto itr = recvClientFdState_.requestId2UnmmapedClientFds.begin();
         itr != recvClientFdState_.requestId2UnmmapedClientFds.end();) {
        if (static_cast<uint64_t>(GetSteadyClockTimeStampMs()) - itr->second.second
            > static_cast<uint64_t>(recvClientFdState_.getClientFdTimeoutMs)) {
            LOG(INFO) << "Close expired fds: " << VectorToString(itr->second.first);
            for (auto fd : itr->second.first) {
                RETRY_ON_EINTR(close(fd));
            }
            itr = recvClientFdState_.requestId2UnmmapedClientFds.erase(itr);
            continue;
        }
        ++itr;
    }
}

void ClientWorkerRemoteCommonApi::RecvPageFd()
{
    while (!recvClientFdState_.stopRecvPageFd) {
        VLOG(1) << "Start to wait to receive page fd";
        recvClientFdState_.recvPageWaitPost->Wait();
        recvClientFdState_.recvPageWaitPost->Clear();
#ifdef WITH_TESTS
        INJECT_POINT("client.RecvPageFd", [this](int sleepMs) {
            if (recvClientFdState_.stopRecvPageFd) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
            }
        });
#endif
        if (recvClientFdState_.stopRecvPageFd) {
            break;
        }
        std::vector<int> pageFds;
        VLOG(1) << "Start to receive page fd by socketFd_:" << socketFd_;
        // Blocks and waits for worker to send fd unless the socket is disconnected.
        std::vector<int> serverFds;
        uint64_t requestId;
        auto status = SockRecvFd(socketFd_, shmWorkerPort_ > 0, pageFds, requestId);
        PostRecvPageFd(status, requestId, pageFds);
        CloseExpiredFd();
        recvClientFdState_.recvPageNotify->Set();
    }
}

Status ClientWorkerRemoteCommonApi::Reconnect()
{
    LOG(INFO) << "Reconnect starting...";
    // Avoid waiting too long to reconnect.
    const int32_t reconnectTimeout = 1 * 1000;  // 1s for reconnect.
    RegisterClientReqPb req;
    RETURN_IF_NOT_OK(Connect(req, reconnectTimeout, true));
    RETURN_IF_NOT_OK(TryFastTransportAfterHeartbeat());
    LOG(INFO) << "Reconnect success! New unix domain socket: " << socketFd_;
    return Status::OK();
}

void ClientWorkerRemoteCommonApi::PostRegisterClient(int32_t timeoutMs, const RegisterClientRspPb &rsp)
{
    enableHugeTlb_ = rsp.enable_huge_tlb();
    clientId_ = rsp.client_id();
    workerStartId_ = rsp.worker_start_id();
    lockId_ = rsp.lock_id();
    uint32_t workerVersion = workerVersion_.fetch_add(1, std::memory_order_relaxed) + 1;
    shmThreshold_ = rsp.shm_threshold();
    workerId_ = rsp.worker_uuid();
    workerEnableP2Ptransfer_ = rsp.enable_p2p_transfer();
    SetHealthy(!rsp.unhealthy());
    exclusiveConnSockPath_ = rsp.exclusive_conn_sockpath();
    if (enableExclusiveConnection_ && exclusiveConnSockPath_.empty()) {
        LOG(WARNING) << "Client requested exclusive connection, but the older worker did not support the feature.";
        enableExclusiveConnection_ = false;
    }
    workerSupportMultiShmRefCount_ = rsp.support_multi_shm_ref_count();
    SetHeartbeatProperties(timeoutMs, rsp);
    SaveStandbyWorker(rsp.standby_worker(), rsp.available_workers());
    ConstructDecShmUnit(rsp);
    LOG(INFO) << "[URMA_INIT] post_register addr=" << hostPort_.ToString() << " clientId=" << clientId_
              << " ver=" << workerVersion << " shm=" << IsShmEnable() << " ft=" << rsp.fast_transport_mode();
    pendingFtHandshake_ = FtHandshakeContext{ timeoutMs, workerVersion, rsp };
    LogRateLimiter::Instance().SetRate(rsp.log_rate_limit());
    LOG(INFO) << "Sync client log_rate_limit from worker register response: " << rsp.log_rate_limit();
}

Status ClientWorkerRemoteCommonApi::TryFastTransportAfterHeartbeat()
{
    if (!pendingFtHandshake_.has_value()) {
        return Status::OK();
    }
    auto ctx = std::move(*pendingFtHandshake_);
    pendingFtHandshake_.reset();
    LOG(INFO) << "[URMA_INIT] try_ft addr=" << hostPort_.ToString() << " clientId=" << clientId_
              << " ver=" << ctx.workerVersion << " shm=" << IsShmEnable() << " ft=" << ctx.rsp.fast_transport_mode();
    auto rc = FastTransportHandshake(ctx.timeoutMs, ctx.workerVersion, ctx.rsp);
    if (rc.IsError()) {
        FLAGS_enable_urma = false;
        LOG(ERROR) << "Fast transport handshake failed, fall back to TCP/IP communication. Detail: " << rc.ToString();
        return rc;
    }
    return Status::OK();
}

Status ClientWorkerRemoteCommonApi::FastTransportHandshake(int32_t timeoutMs, uint32_t workerVersion,
                                                           const RegisterClientRspPb &rsp)
{
    (void)timeoutMs;
    (void)workerVersion;
    // Initialize UrmaManager regardless of shmEnabled_ to avoid switch overhead standby worker.
    // This only warms up local URMA hardware resources and memory pools.
    // The actual remote connection is established later during the handshake.
    SetClientFastTransportMode(rsp.fast_transport_mode(), fastTransportMemSize_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitializeFastTransportManager(), "Fast transport init failed");

    // Enable UB fast transport if client cannot use share memory while worker supports UB.
    if (IsShmEnable()) {
        FLAGS_enable_urma = false;
        return Status::OK();
    }

#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        auto traceId = Trace::Instance().GetTraceID();
        auto future = urmaHandshakePool_->Submit([this, workerVersion, traceId]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            return AsyncFirstUrmaHandshake(workerVersion);
        });
        int64_t waitMs = std::max<int64_t>(timeoutMs, MIN_WAIT_MS);

        if (future.wait_for(std::chrono::milliseconds(waitMs)) == std::future_status::ready) {
            Status status = future.get();
            if (status.IsError()) {
                LOG(WARNING) << "URMA handshake failed in register path within wait time " << waitMs
                             << "ms, fall back to TCP/IP communication. Detail: " << status.ToString();
            }
        } else {
            LOG(WARNING) << "URMA handshake exceeded register wait time " << waitMs
                         << "ms and will continue in background. Heartbeat will start first for worker "
                         << hostPort_.ToString() << ", clientId: " << clientId_;
        }
    }
#endif
    return Status::OK();
}

Status ClientWorkerRemoteCommonApi::AsyncFirstUrmaHandshake(uint32_t workerVersion)
{
#ifdef WITH_TESTS
    INJECT_POINT("client.urma_first_handshake_delay");
#endif
    LOG(INFO) << "[URMA_INIT] first_hs addr=" << hostPort_.ToString() << " clientId=" << clientId_
              << " ver=" << workerVersion;
    reqTimeoutDuration.InitWithPositiveTime(ClientGetRequestTimeout(requestTimeoutMs_));
    Status status = TryUrmaHandshake();
    uint32_t currentWorkerVersion = workerVersion_.load(std::memory_order_relaxed);
    if (workerVersion != currentWorkerVersion) {
        LOG(INFO) << "Ignore stale URMA first handshake result for worker " << hostPort_.ToString()
                  << ", clientId: " << clientId_ << ", stale version: " << workerVersion
                  << ", current version: " << currentWorkerVersion;
        return Status::OK();
    }
    if (status.IsError()) {
        ScheduleUrmaHandshakeRetry(workerVersion);
    }
    return status;
}

Status ClientWorkerRemoteCommonApi::TryUrmaHandshake()
{
    PerfPoint perfPoint(PerfKey::CLIENT_URMA_HANDSHAKE);
    LOG(INFO) << "[URMA_INIT] try_hs addr=" << hostPort_.ToString() << " clientId=" << clientId_;
    // Perform handshake to set up jfr and segments.
    using TbbTransportStubTable =
        tbb::concurrent_hash_map<std::string, std::shared_ptr<datasystem::object_cache::WorkerRemoteWorkerTransApi>>;
    static TbbTransportStubTable tarnsportApiTable;
    std::string endPoint = hostPort_.ToString();
    TbbTransportStubTable::const_accessor constAccApi;
    while (!tarnsportApiTable.find(constAccApi, endPoint)) {
        TbbTransportStubTable::accessor acc;
        if (tarnsportApiTable.insert(acc, endPoint)) {
            std::shared_ptr<datasystem::object_cache::WorkerRemoteWorkerTransApi> transportApi =
                std::make_shared<datasystem::object_cache::WorkerRemoteWorkerTransApi>(hostPort_, clientId_);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(transportApi->Init(), "Create transport api faild.");
            acc->second = std::move(transportApi);
        }
    }
    UrmaHandshakeRspPb handshakeRsp;
    RETURN_IF_NOT_OK(constAccApi->second->ExecOnceParrallelExchange(handshakeRsp));
    return Status::OK();
}

void ClientWorkerRemoteCommonApi::ScheduleUrmaHandshakeRetry(uint32_t workerVersion)
{
    auto traceId = Trace::Instance().GetTraceID();
    urmaHandshakeRetryPool_->Execute([this, workerVersion, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        constexpr int maxRetryIntervalMs = 8000;
        constexpr int retryCheckIntervalMs = 100;
        constexpr int64_t rpcTimeout = 15000;
        int nextRetryTimeMs = retryCheckIntervalMs;
        Timer timer;
        while (!stopUrmaHandshakeRetry_.load(std::memory_order_relaxed)) {
            uint32_t currentWorkerVersion = workerVersion_.load(std::memory_order_relaxed);
            if (workerVersion != currentWorkerVersion) {
                LOG(INFO) << "Stop stale URMA handshake retry for worker " << hostPort_.ToString()
                          << ", clientId: " << clientId_ << ", stale version: " << workerVersion
                          << ", current version: " << currentWorkerVersion;
                break;
            }
            INJECT_POINT_NO_RETURN("client.urma_handshake_retry");
            if (timer.ElapsedMilliSecond() < nextRetryTimeMs) {
                std::this_thread::sleep_for(std::chrono::milliseconds(retryCheckIntervalMs));
                continue;
            }
            timer.Reset();
            reqTimeoutDuration.InitWithPositiveTime(rpcTimeout);
            Status status = TryUrmaHandshake();
            currentWorkerVersion = workerVersion_.load(std::memory_order_relaxed);
            if (workerVersion != currentWorkerVersion) {
                LOG(INFO) << "Ignore stale URMA handshake retry result for worker " << hostPort_.ToString()
                          << ", clientId: " << clientId_ << ", stale version: " << workerVersion
                          << ", current version: " << currentWorkerVersion;
                break;
            }
            if (IsRpcTimeoutOrTryAgain(status) || status.GetCode() == StatusCode::K_URMA_CONNECT_FAILED) {
                nextRetryTimeMs = std::min<int>(nextRetryTimeMs << 1, maxRetryIntervalMs);
                LOG(WARNING) << "URMA handshake async retry failed for worker " << hostPort_.ToString()
                             << ", clientId: " << clientId_ << ", retry after " << nextRetryTimeMs
                             << "ms. Detail: " << status.ToString();
                continue;
            }

            if (status.IsOk()) {
                LOG(INFO) << "URMA handshake retry succeeded for worker " << hostPort_.ToString()
                          << ", clientId: " << clientId_;
            } else {
                LOG(WARNING) << "URMA handshake retry failed for worker " << hostPort_.ToString()
                             << ", clientId: " << clientId_ << ". Detail: " << status.ToString();
            }
            break;
        }
    });
}
}  // namespace client
}  // namespace datasystem
