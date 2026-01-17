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
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <sys/types.h>
#include <nlohmann/json.hpp>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/rpc/zmq/exclusive_conn_mgr.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/fd_manager.h"
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

namespace datasystem {
namespace client {

// Static/global id generator init
std::atomic<int32_t> ClientWorkerRemoteCommonApi::exclusiveIdGen_ = 0;

IClientWorkerCommonApi::~IClientWorkerCommonApi() = default;
ClientWorkerCommonApiAttribute::~ClientWorkerCommonApiAttribute() = default;

ClientWorkerRemoteCommonApi::ClientWorkerRemoteCommonApi(HostPort hostPort, RpcCredential cred,
                                                         HeartbeatType heartbeatType, SensitiveValue token,
                                                         Signature *signature, std::string tenantId,
                                                         bool enableCrossNodeConnection, bool enableExclusiveConnection)
    : IClientWorkerCommonApi(std::move(hostPort), heartbeatType, enableCrossNodeConnection),
      cred_(cred),
      pageSize_(0),
      signature_(signature),
      tenantId_(std::move(tenantId)),
      enableExclusiveConnection_(enableExclusiveConnection)
{
    recvPageThread_ = Thread(&ClientWorkerRemoteCommonApi::RecvPageFd, this);
    clientAccessToken_ = std::make_unique<ClientAccessToken>(std::move(token));
}

ClientWorkerRemoteCommonApi::~ClientWorkerRemoteCommonApi()
{
    recvClientFdState_.stopRecvPageFd = true;
    recvClientFdState_.recvPageWaitPost->Set();
    recvClientFdState_.recvPageNotify->Set();
    CloseSocketFd();
    if (recvPageThread_.joinable()) {
        recvPageThread_.join();
    }
}

void ClientWorkerRemoteCommonApi::SetRpcTimeout(int32_t timeout)
{
    constexpr int32_t rpcMaxTimeout = 600000;  // 10min
    int32_t rpcTimeout = timeout / retryTimes_;

    int32_t shorterSplitTime = 30000;  // 30s
    int32_t longerSplitTime = 90000;   // 90s
    if (timeout <= shorterSplitTime) {
        rpcTimeoutMs_ = timeout;
    } else if (timeout <= longerSplitTime) {
        rpcTimeoutMs_ = shorterSplitTime;
    } else {
        rpcTimeoutMs_ = std::min(rpcTimeout, rpcMaxTimeout);
    }
    LOG(INFO) << "The total timeout is " << timeout << " ms, single rpc timeout is " << rpcTimeoutMs_ << " ms";
}

Status ClientWorkerRemoteCommonApi::Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs)
{
    CHECK_FAIL_RETURN_STATUS(
        connectTimeoutMs >= RPC_MINIMUM_TIMEOUT, StatusCode::K_INVALID,
        FormatString("The connect timeout should be greater than or equal to %d milliseconds.", RPC_MINIMUM_TIMEOUT));
    CHECK_FAIL_RETURN_STATUS(requestTimeoutMs >= 0, StatusCode::K_INVALID,
                             FormatString("The req timeout should be greater than or equal to 0 milliseconds."));
    requestTimeoutMs_ = requestTimeoutMs;
    connectTimeoutMs_ = connectTimeoutMs;
    recvClientFdState_.getClientFdTimeoutMs = connectTimeoutMs;
    SetRpcTimeout(requestTimeoutMs);
    VLOG(1) << "Client start to connect worker";
    CHECK_FAIL_RETURN_STATUS(TimerQueue::GetInstance()->Initialize(), K_RUNTIME_ERROR, "TimerQueue init failed!");
    RegisterClientReqPb req;
    RETURN_IF_NOT_OK(Connect(req, connectTimeoutMs));
    VLOG(1) << "The new client id is: " << clientId_ << ", Received pageSize= " << pageSize_ << " from worker.";
    return Status::OK();
}

Status ClientWorkerRemoteCommonApi::Connect(RegisterClientReqPb &req, int32_t timeoutMs, bool reconnection)
{
    auto channel = std::make_shared<RpcChannel>(hostPort_, cred_);
    commonWorkerSession_ = std::make_unique<WorkerService_Stub>(channel);

    bool isConnectUdsSuccess = false;
    int32_t serverFd = INVALID_SOCKET_FD;
    int32_t socketFd = INVALID_SOCKET_FD;
    bool mustUds = heartbeatType_ == HeartbeatType::UDS_HEARTBEAT || (reconnection && shmEnabled_);
    INJECT_POINT_NO_RETURN("ClientWorkerCommonApi.Connect.MustUds", [&mustUds] { mustUds = true; });
    RETURN_IF_NOT_OK(CreateUnixDomainSocket(timeoutMs, isConnectUdsSuccess, serverFd, socketFd));
    if (isConnectUdsSuccess) {
        VLOG(1) << "Create unix domain socket successfully, socketFd: " << socketFd << ", serverFd: " << serverFd;
    }
    if (mustUds && !isConnectUdsSuccess) {
        return { StatusCode::K_RPC_UNAVAILABLE, "Can not create a unix domain socket to connect worker." };
    }

    CloseSocketFd();

    shmEnabled_ = isUseStandbyWorker_ ? false : isConnectUdsSuccess;
    socketFd_ = socketFd;
    if (isConnectUdsSuccess) {
        heartbeatType_ = heartbeatType_ == HeartbeatType::NO_HEARTBEAT ? HeartbeatType::RPC_HEARTBEAT : heartbeatType_;
    }
    std::string type = isConnectUdsSuccess ? "Unix domain socket" : "TCP";
    VLOG(1) << "Start to register client to worker through the " << type;
    req.set_server_fd(serverFd);
    RETURN_IF_NOT_OK(RegisterClient(req, timeoutMs));
    LOG(INFO) << FormatString("Register client to worker through the %s successfully, client id: %s", type, clientId_);
    return Status::OK();
}

Status ClientWorkerRemoteCommonApi::CreateHandShakeFunc(UnixSockFd &fd, std::string &sockPath, int32_t &serverFd)
{
    LOG(INFO) << FormatString("Unix socket path %s", sockPath);
    RETURN_IF_NOT_OK(fd.Connect(FormatString("ipc://%s", sockPath)));
    uint32_t tmpServerFd;
    // Get the server side fd and add this to the RegisterClientReqPb
    RETURN_IF_NOT_OK(fd.SetTimeout(STUB_FRONTEND_TIMEOUT));
    RETURN_IF_NOT_OK(fd.Recv32(tmpServerFd, false));
    INJECT_POINT("ClientWorkerCommonApi.CreateHandShakeFunc");
    CHECK_FAIL_RETURN_STATUS(tmpServerFd <= INT32_MAX, K_RUNTIME_ERROR, "Server fd exceed range of int32_t");
    serverFd = static_cast<int32_t>(tmpServerFd);  // The FD sent by the worker is of the int type.
    LOG(INFO) << FormatString("Connects to local server %s successfully. Client fd %d. Server fd %d.", sockPath,
                              fd.GetFd(), serverFd);
    // Change the timeout back to the default.
    return fd.SetTimeout(0);
}

Status ClientWorkerRemoteCommonApi::CreateUnixDomainSocket(int32_t timeoutMs, bool &isConnectUdsSuccess,
                                                           int32_t &serverFd, int32_t &socketFd)
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
        rpcTimeoutMs_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Get socket path failed.");
    std::string sockPath = reply.path();
    isConnectUdsSuccess = false;
    if (sockPath.empty()) {
        LOG(INFO) << "The uds socket path is empty, current server can not support unix domain socket.";
        return Status::OK();
    }

    UnixSockFd sock;
    auto func = [this, &sock, &sockPath, &serverFd](int32_t) {
        Status status = CreateHandShakeFunc(sock, sockPath, serverFd);
        if (status.IsError()) {
            LOG(WARNING) << FormatString(
                "Client can not connect to server by unix domain socket within allowed time (%dms). Socket path: "
                "%s, Detail: %s",
                STUB_FRONTEND_TIMEOUT, sockPath, status.GetMsg());
            // Make sure we release the file descriptor.
            sock.Close();
        }
        return status;
    };
    rc = RetryOnError(timer.GetRemainingTimeMs(), func, [] { return Status::OK(); }, { StatusCode::K_TRY_AGAIN });
    if (rc.IsOk()) {
        isConnectUdsSuccess = true;
        socketFd = sock.GetFd();
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
    INJECT_POINT("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", [&opts](int time) {
        opts.SetTimeout(time);
        return Status::OK();
    });
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
    if (shmEnabled_) {
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
    INJECT_POINT("client.CloseSocketFd",
                 [](int delayMs) { std::this_thread::sleep_for(std::chrono::milliseconds(delayMs)); });
    socketFd_ = INVALID_SOCKET_FD;
}

void ClientWorkerRemoteCommonApi::SetHealthy(bool healthy)
{
    bool prev = healthy_.exchange(healthy, std::memory_order_relaxed);
    if (prev != healthy) {
        LOG(INFO) << FormatString("client %s health state: (%d) -> (%d)", clientId_, prev, healthy);
    }
}

Status ClientWorkerRemoteCommonApi::RegisterClient(RegisterClientReqPb &req, int32_t timeoutMs)
{
    RETURN_IF_NOT_OK(SetToken(req));
    req.set_version(DATASYSTEM_VERSION);
    req.set_git_hash(GetGitHash());
    req.set_heartbeat_enabled(heartbeatType_ != HeartbeatType::NO_HEARTBEAT);
    req.set_shm_enabled(shmEnabled_);
    req.set_tenant_id(tenantId_);
    req.set_enable_cross_node(enableCrossNodeConnection_);
    req.set_enable_exclusive_connection(enableExclusiveConnection_);
    req.set_pod_name(Logging::PodName());
    req.set_support_multi_shm_ref_count(true);
    INJECT_POINT("client.RegisterClient.multi_shm_ref_count", [&req](bool multiShmRefCount) {
        req.set_support_multi_shm_ref_count(multiShmRefCount);
        return Status::OK();
    });

    RegisterClientRspPb rsp;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    LOG(INFO) << "Start to send rpc to register client to worker, shm enable: " << shmEnabled_
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
        rpcTimeoutMs_);
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
    INJECT_POINT("ClientWorkerCommonApi.Disconnect.ShutdownQuickily", [&rpcTimeoutMs](int time) {
        rpcTimeoutMs = time;
        return Status::OK();
    });
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
    if (!shmEnabled_ || socketFd_ == INVALID_SOCKET_FD) {
        return { K_RUNTIME_ERROR, "Current client can not support uds, so query client fd failed." };
    }
    PerfPoint point(PerfKey::RPC_WORKER_GET_CLIENT_FDS);
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
    INJECT_POINT("ClientWorkerCommonApi.GetClientFd.preReceive");
    if (status.IsOk()) {
        RecvFdAfterNotify(workerFds, requestId, time, clientFds);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!clientFds.empty(), K_RUNTIME_ERROR,
                                         FormatString("Receive fd[%s] from %d failed, detail: %s",
                                                      VectorToString(workerFds), socketFd_, status.ToString()));
    VLOG(1) << FormatString("Receive fd[%s] from socket[%d] success, res: %s ", VectorToString(workerFds), socketFd_,
                            VectorToString(clientFds));
    point.Record();
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
    INJECT_POINT("ClientWorkerCommonApi.RecvPageFd",
                 [](int32_t time) { std::this_thread::sleep_for(std::chrono::milliseconds(time)); });
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
        INJECT_POINT("client.RecvPageFd", [this](int sleepMs) {
            if (recvClientFdState_.stopRecvPageFd) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
            }
        });
        if (recvClientFdState_.stopRecvPageFd) {
            break;
        }
        std::vector<int> pageFds;
        VLOG(1) << "Start to receive page fd by socketFd_:" << socketFd_;
        // Blocks and waits for worker to send fd unless the socket is disconnected.
        std::vector<int> serverFds;
        uint64_t requestId;
        auto status = SockRecvFd(socketFd_, pageFds, requestId);
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
    LOG(INFO) << "Reconnect success! New unix domain socket: " << socketFd_;
    return Status::OK();
}

void ClientWorkerRemoteCommonApi::SetHeatbeatProperties(int32_t timeoutMs, const RegisterClientRspPb &rsp)
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

void ClientWorkerRemoteCommonApi::PostRegisterClient(int32_t timeoutMs, const RegisterClientRspPb &rsp)
{
    enableHugeTlb_ = rsp.enable_huge_tlb();
    workerTimeoutMult_ = rsp.quorum_timeout_mult();
    clientId_ = rsp.client_id();
    workerStartId_ = rsp.worker_start_id();
    pageSize_ = static_cast<uint32_t>(rsp.page_size());
    lockId_ = rsp.lock_id();
    (void)workerVersion_.fetch_add(1, std::memory_order_relaxed);
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
    SetHeatbeatProperties(timeoutMs, rsp);
    SaveStandbyWorker(rsp.standby_worker(), rsp.available_workers());
    ConstructDecShmUnit(rsp);
}
}  // namespace client
}  // namespace datasystem
