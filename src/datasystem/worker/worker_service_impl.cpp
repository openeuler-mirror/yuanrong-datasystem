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
 * Description: Implement the worker common service processing main class.
 */
#include "datasystem/worker/worker_service_impl.h"

#include <cstdint>
#include <shared_mutex>
#include "datasystem/c_api/status_definition.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"
#ifdef __linux__
#include <linux/memfd.h>
#endif
#include <poll.h>
#include "re2/re2.h"

#include <utility>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/fd_manager.h"
#include "datasystem/common/util/fd_pass.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/version.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/worker/client_manager/client_manager.h"

DS_DECLARE_string(unix_domain_socket_dir);
DS_DECLARE_uint32(page_size);
DS_DEFINE_bool(authorization_enable, false, "Indicates whether to enable the tenant authentication, default is false.");
DS_DECLARE_bool(ipc_through_shared_memory);
DS_DEFINE_uint32(shared_memory_worker_port, 0, "The worker port for shared memory communication.");
DS_DEFINE_validator(shared_memory_worker_port, &Validator::ValidatePort);
DS_DEFINE_uint32(max_client_num, 200,
                 "Maximum number of clients that can be connected to a worker. Value range: [1, 10000]");
DS_DEFINE_validator(max_client_num, &Validator::ValidateClientNum);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_string(cluster_name);
DS_DECLARE_uint64(client_dead_timeout_s);
DS_DECLARE_bool(enable_huge_tlb);
DS_DEFINE_uint64(oc_shm_transfer_threshold_kb, 500u,
                 "The data threshold to transfer obj data between client and worker via shm, unit is KB");
DS_DECLARE_bool(enable_p2p_transfer);
DS_DECLARE_uint32(client_reconnect_wait_s);

namespace datasystem {
namespace worker {
const size_t UNBOUNDED_FD_MULTIPIER = 10;
WorkerServiceImpl::WorkerServiceImpl(HostPort serverAddr, HostPort masterAddr, double timeoutMult, CommonServer *worker,
                                     std::shared_ptr<AkSkManager> akSkManager, std::string workerUuid)
    : WorkerService(std::move(serverAddr)),
      masterAddr_(std::move(masterAddr)),
      worker_(worker),
      timeoutMultiplier_(timeoutMult),
      listenFd_(-1),
      shmWorkerPort_(0),
      akSkManager_(akSkManager),
      workerUuid_(std::move(workerUuid)),
      maxCacheUnboundedUnixSockFdsCount_(FLAGS_max_client_num * UNBOUNDED_FD_MULTIPIER)
{
}

WorkerServiceImpl::~WorkerServiceImpl()
{
    if (listenFd_ != -1) {
        int ret = shutdown(listenFd_, SHUT_RDWR);
        LOG(INFO) << FormatString("shutdown socket fd:%d, ret:%d ", listenFd_, ret);
        listenFd_ = -1;
    }
    if (!sockPath_.empty()) {
        unlink(sockPath_.data());
    }
}

Status WorkerServiceImpl::Init()
{
    RETURN_IF_NOT_OK(TenantAuthManager::Instance()->Init(FLAGS_authorization_enable, akSkManager_));
    workerStartId_ = GetStringUuid();
    if (FLAGS_ipc_through_shared_memory) {
        if (FLAGS_shared_memory_worker_port > 0) {
            // Try to create TCP socket with IPPROTO_SCMTCP for FD passing over TCP
            uint32_t tcpPort = FLAGS_shared_memory_worker_port;
            UnixSockFd scmSockFd(RPC_NO_FILE_FD, true);
            std::string tmp;
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                scmSockFd.Bind(FormatString("tcp://%s:%d", localAddress_.Host(), tcpPort), RPC_SOCK_MODE, tmp),
                FormatString("Create SCMTCP socket failed, errno %d", errno));
            listenFd_ = scmSockFd.GetFd();
            shmWorkerPort_ = tcpPort;
        } else {
            INJECT_POINT("worker.bind_unix_path");
            std::string sockDir = FLAGS_unix_domain_socket_dir;
            RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(sockDir, FLAGS_unix_domain_socket_dir, ""));
            const int defaultUDSDirMode = 0750;
            Status rc = CreateDir(sockDir, true, defaultUDSDirMode);
            if (!rc.IsOk() && !FileExist(sockDir)) {
                RETURN_STATUS(rc.GetCode(), rc.GetMsg());
            }
            std::string sockPath = FormatString("%s/%s", sockDir, GetStringUuid().substr(0, RPC_EIGHT));
            (void)unlink(sockPath.data());
            UnixSockFd udsSockFd;
            std::string tmp;
            RETURN_IF_NOT_OK(udsSockFd.Bind(FormatString("ipc://%s", sockPath), RPC_SOCK_MODE, tmp));
            sockPath_ = std::move(sockPath);
            listenFd_ = udsSockFd.GetFd();
        }
    }
#ifndef MFD_HUGETLB
    if (FLAGS_enable_huge_tlb) {
        LOG(WARNING) << "current linux version is not support MFD_HUGETLB flag, huge_tlb will not take effect.";
        FLAGS_enable_huge_tlb = false;
    }
#endif
    return Status::OK();
}

Status WorkerServiceImpl::GetClientFd(const GetClientFdReqPb &req, GetClientFdRspPb &rsp)
{
    INJECT_POINT("worker.before_GetClientFd");
    (void)rsp;
    int socketFd;
    LOG(INFO) << "Start to query client fd, localAddress: " << localAddress_.ToString();
    std::string tenantId;
    std::string authTenantId = req.tenant_id();
    auto clientId = ClientKey::Intern(req.client_id());
    if (authTenantId.empty()) {
        bool exist;
        authTenantId = worker::ClientManager::Instance().GetAuthTenantIdByClientId(clientId, exist);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AuthenticateRequest(akSkManager_, req, authTenantId, tenantId),
                                     "Authenticate failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ClientManager::Instance().GetClientSocketFd(clientId, socketFd),
                                     "worker get client socketfd failed");

    std::vector<int> workerFds = { req.worker_fds().cbegin(), req.worker_fds().cend() };
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(memory::Allocator::Instance()->CheckWorkerFdTenant(tenantId, workerFds),
                                     "Authenticate workerFd failed.");

    LOG(INFO) << "Start to send client fd. worker fds: " << VectorToString(workerFds) << ", socket fd: " << socketFd
              << ", request id: " << req.request_id();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SockSendFd(socketFd, shmWorkerPort_ > 0, workerFds, req.request_id()),
                                     "worker socketfd send failed");
    ClientManager::Instance().SetClientId2WorkerFdMap(clientId, std::move(workerFds));
    LOG(INFO) << "Finish to send client fd. worker fds: " << VectorToString(workerFds) << ", socket fd: " << socketFd;
    return Status::OK();
}

Status WorkerServiceImpl::DisconnectClient(const DisconnectClientReqPb &req, DisconnectClientRspPb &rsp)
{
    std::string tenantId;
    bool exist;
    auto clientId = ClientKey::Intern(req.client_id());
    std::string authTenantId = worker::ClientManager::Instance().GetAuthTenantIdByClientId(clientId, exist);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AuthenticateRequest(akSkManager_, req, authTenantId, tenantId),
                                     "Authenticate failed");
    LOG(INFO) << FormatString("disconnect client: %s", clientId);
    (void)rsp;
    worker_->AfterClientLostHandler(clientId);
    return Status::OK();
}

Status WorkerServiceImpl::CheckClientVersion(const std::string &clientVersion)
{
    const re2::RE2 versionRe("^([0-9]+)(\\.([0-9A-Z]+)){1,3}$");
    if (re2::RE2::FullMatch(clientVersion, versionRe)) {
        VLOG(1) << FormatString("Client version is : %s", clientVersion);
    } else {
        LOG(ERROR) << FormatString("Invalid client version : %s", clientVersion);
    }
    std::string workerVersion = DATASYSTEM_VERSION;
    INJECT_POINT("WorkerServiceImpl.CheckClientVersion.workerVersion", [&workerVersion](std::string version) {
        workerVersion = version;
        return Status::OK();
    });
    if (clientVersion != workerVersion) {
        LOG(WARNING) << "The versions of client and worker are inconsistent. Client Version: " << clientVersion
                     << ", worker version: " << workerVersion;
    }
    return Status::OK();
}

const std::string WorkerServiceImpl::GetStandbyWorker()
{
    std::string standbyWorker;
    Status rc = etcdCM_->GetStandbyWorker(standbyWorker);
    if (rc.IsError()) {
        VLOG(HEARTBEAT_LEVEL) << "Get standby worker failed: " << rc.ToString();
        return "";
    }
    return standbyWorker;
}

Status WorkerServiceImpl::RegisterClient(const RegisterClientReqPb &req, RegisterClientRspPb &rsp)
{
    bool remainClient = !req.client_id().empty();
    auto clientId = ClientKey::Intern(remainClient ? req.client_id() : GetStringUuid());
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AuthenticateRequest(akSkManager_, req, req.tenant_id(), tenantId),
                                     "Authenticate failed");
    if (!IsHealthy() && (etcdCM_ != nullptr && etcdCM_->CheckLocalNodeIsExiting())) {
        LOG(WARNING) << "Register client failed because worker is exiting and unhealthy now";
        RETURN_STATUS(StatusCode::K_NOT_READY, "Worker is exiting and unhealthy now!");
    }

    auto serverFd = req.server_fd();
    if (serverFd > 0) {
        std::lock_guard<std::shared_timed_mutex> lck(mutex_);
        INJECT_POINT("WorkerServiceImpl.RegisterClient.BlowAuth");
        CHECK_FAIL_RETURN_STATUS(unboundedUnixSockFds_.find(serverFd) != unboundedUnixSockFds_.end(),
                                 K_SERVER_FD_CLOSED, FormatString("Fd %d has been released", serverFd));
        (void)unboundedUnixSockFds_.erase(serverFd);
        if (!req.shm_enabled()) {
            // close scmtcp fd for remote client
            LOG(INFO) << "the connection not enable shm, close fd: " << serverFd;
            RETRY_ON_EINTR(close(serverFd));
        }
    }
    RaiiPlus raiiP([&req]() {
        if (req.shm_enabled() && req.server_fd() > 0) {
            RETRY_ON_EINTR(close(req.server_fd()));
        }
    });
    CheckClientGitHash(req.git_hash());
    const auto &version = req.version();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckClientVersion(version), "worker check client version failed");
    uint32_t lockId = 0;
    auto shmEnabled = req.shm_enabled();
    int32_t socketFd = shmEnabled ? req.server_fd() : INVALID_SOCKET_FD;
    bool supportMultiShmRefCount = req.support_multi_shm_ref_count();
    LOG(INFO) << "Register client: " << clientId << ", pod: " << req.pod_name() << ", version: " << version
              << ", socket fd: " << socketFd << ", shmEnabled: " << shmEnabled << ", tenantId: " << tenantId
              << ", enable cross node: " << req.enable_cross_node() << ", reconnect client: " << remainClient
              << ", heartbeat: " << req.heartbeat_enabled() << ", supportMultiShmRefCount: " << supportMultiShmRefCount;
    INJECT_POINT("WorkerServiceImpl.RegisterClient.AboveAddClient");
    INJECT_POINT("worker.RegisterClient.multi_shm_ref_count", [&supportMultiShmRefCount](bool multiShmRefCount) {
        supportMultiShmRefCount &= multiShmRefCount;
        return Status::OK();
    });
    if (req.heartbeat_enabled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            worker_->AddClient(clientId, shmEnabled, socketFd, tenantId, req.enable_cross_node(), req.pod_name(),
                               supportMultiShmRefCount, req.device_id(), lockId),
            "worker add client failed");
    }
    // After executing "AddClient", the server fd will be bound to the client and released when the client loses
    // connection, so there is no need to roll back.
    raiiP.ClearAllTask();

    if (remainClient) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker_->ProcessServerReboot(clientId, tenantId, req.token(), req.extend()),
                                         "worker process server reboot failed");
    }
    int fd;
    uint64_t mmapSize;
    ptrdiff_t offset;
    ShmKey id;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker_->GetShmQueueUnit(lockId, fd, mmapSize, offset, id),
                                     "worker process get ShmQ unit failed");

    std::string exclusiveConnSockPath;
    if (req.enable_exclusive_connection()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker_->GetExclConnSockPath(exclusiveConnSockPath),
                                         "worker process get exclusive connection socket path failed");
    }
    rsp.set_page_size(FLAGS_page_size);
    rsp.set_quorum_timeout_mult(timeoutMultiplier_);
    rsp.set_client_id(clientId);
    rsp.set_lock_id(lockId);
    rsp.set_worker_start_id(workerStartId_);
    rsp.set_shm_threshold(FLAGS_oc_shm_transfer_threshold_kb * KB);
    rsp.set_worker_uuid(workerUuid_);
    rsp.set_standby_worker(GetStandbyWorker());
    rsp.set_node_timeout_s(FLAGS_node_timeout_s);
    rsp.set_store_fd(fd);
    rsp.set_mmap_size(mmapSize);
    rsp.set_offset(offset);
    rsp.set_shm_id(id);
    rsp.set_enable_huge_tlb(FLAGS_enable_huge_tlb);
    rsp.set_unhealthy(!IsHealthy());
    SetAvailableWorkers(rsp);
    auto clientDeadTimeoutSec = std::min<uint64_t>(FLAGS_client_dead_timeout_s, FLAGS_node_timeout_s);
    rsp.set_client_dead_timeout_s(clientDeadTimeoutSec);
    rsp.set_enable_p2p_transfer(FLAGS_enable_p2p_transfer);
    rsp.set_client_reconnect_wait_s(FLAGS_client_reconnect_wait_s);
    rsp.set_exclusive_conn_sockpath(exclusiveConnSockPath);
    rsp.set_support_multi_shm_ref_count(supportMultiShmRefCount);
#ifdef USE_URMA
    if (IsUrmaEnabled() && GetUrmaMode() == UrmaMode::UB) {
        rsp.set_fast_transport_mode(FastTransportMode::UB);
    }
#endif

    INJECT_POINT("worker.RegisterClient.end", [&rsp](int fd) {
        rsp.set_store_fd(fd);
        return Status::OK();
    });
    LOG(INFO) << "Register client " << clientId << " done, healthy: " << !rsp.unhealthy()
              << ", worker support multi shm ref count:" << rsp.support_multi_shm_ref_count();
    return Status::OK();
}

Status WorkerServiceImpl::Heartbeat(const HeartbeatReqPb &req, HeartbeatRspPb &rsp)
{
    INJECT_POINT("worker.Heartbeat.begin");
    std::string tenantId;
    bool exist;
    auto clientId = ClientKey::Intern(req.client_id());
    std::string authTenantId = worker::ClientManager::Instance().GetAuthTenantIdByClientId(clientId, exist);
    // When exist is false, it indicates a restart scenario, skip iam and authenticate.
    if (exist) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AuthenticateRequest(akSkManager_, req, authTenantId, tenantId),
                                         "Authenticate failed");
    }
    VLOG(HEARTBEAT_LEVEL) << "Receive a heartbeat from client " << clientId;
    INJECT_POINT("Worker.Heartbeat.startToHeartbeat");
    rsp.set_worker_start_id(workerStartId_);
    Status status = ClientManager::Instance().UpdateLastHeartbeat(clientId, req.removable());
    std::vector<int> releasedFds;
    for (auto i : req.released_worker_fds()) {
        releasedFds.emplace_back(i);
    }
    LOG_IF(INFO, !releasedFds.empty()) << FormatString("[TENANT RELEASER]Receive cleint[%s] has released fds[%s]",
                                                       clientId, VectorToString(releasedFds));
    if (exist) {
        // We only allow authenticated requests to modify data.
        ClientManager::Instance().DelClientId2WorkerFdMap(clientId, releasedFds);
    }
    // If the client has been removed, UpdateLastHeartbeat will return RuntimeError. Otherwise, OK is returned.
    rsp.set_client_removed(status.IsError());
    rsp.set_standby_worker(GetStandbyWorker());
    rsp.set_unhealthy(!IsHealthy());
    SetAvailableWorkers(rsp);
    if (etcdCM_ != nullptr) {
        rsp.set_is_voluntary_scale_down(etcdCM_->CheckLocalNodeIsExiting());
    } else {
        const int logDuration = 30;
        LOG_EVERY_T(WARNING, logDuration) << "[Heartbeat] Etcd manager is null";
    }

    auto expiredFds = memory::Allocator::Instance()->GetAllExpiredFds();
    auto fdsMmapedByClient = ClientManager::Instance().GetWorkerFdByClientId(clientId);
    std::set<int> intersection;
    std::set_intersection(expiredFds.begin(), expiredFds.end(), fdsMmapedByClient.begin(), fdsMmapedByClient.end(),
                          std::inserter(intersection, intersection.begin()));
    for (auto fd : intersection) {
        rsp.add_expired_worker_fds(fd);
    }
    VLOG(HEARTBEAT_LEVEL) << "Response heartbeat message to client " << clientId << ", response: " << rsp.DebugString();
    return Status::OK();
}

Status WorkerServiceImpl::GetSocketPath(const GetSocketPathReqPb &req, GetSocketPathRspPb &rsp)
{
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AuthenticateRequest(akSkManager_, req, req.tenant_id(), tenantId),
                                     "Authenticate failed");
    if (FLAGS_ipc_through_shared_memory) {
        rsp.set_path(sockPath_);
        if (shmWorkerPort_ > 0) {
            rsp.set_shm_worker_port(shmWorkerPort_);
        }
    }
    return Status::OK();
}

void WorkerServiceImpl::CloseExpiredUnixSockFd()
{
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    for (auto itr = unboundedUnixSockFds_.begin(); itr != unboundedUnixSockFds_.end();) {
        auto now = std::chrono::steady_clock::now();
        auto storeTimeMs = std::chrono::steady_clock::time_point(std::chrono::milliseconds(itr->second));
        if (now - storeTimeMs > std::chrono::milliseconds(CACHE_UDS_SOCK_FD_TIMEOUT_MS)) {
            LOG(INFO) << FormatString(
                "Fd %d for uds connection has not been bound to the client for a long time and "
                "needs to be released",
                itr->first);
            RETRY_ON_EINTR(close(itr->first));
            itr = unboundedUnixSockFds_.erase(itr);
            continue;
        }
        ++itr;
    }
}

Status WorkerServiceImpl::operator()()
{
    UnixSockFd tmpSockFd;
    RETURN_IF_NOT_OK(tmpSockFd.CreateUnixSocket());
    Raii closeTmpSockFd([&tmpSockFd]() { tmpSockFd.Close(); });
    int tmpfd = tmpSockFd.GetFd();

    bool hasBeenLogged = false;  //  This variable is used to avoid excessive log printing.
    // Loop forever until the main() is interrupted.
    const int checkCacheFdIntervalMs = 3'000;
    Timer checkCacheFdTimer;
    while (!IsInterrupted()) {
        if (checkCacheFdTimer.ElapsedMilliSecond() > checkCacheFdIntervalMs) {
            CloseExpiredUnixSockFd();
            checkCacheFdTimer.Reset();
        }
        struct pollfd pfd{ .fd = listenFd_, .events = POLLIN, .revents = 0 };
        int n = poll(&pfd, 1, RPC_POLL_TIME);
        if (n <= 0) {
            continue;
        }
        int fd = accept(listenFd_, nullptr, 0);
        if (fd > 0) {
            {
                std::lock_guard<std::shared_timed_mutex> lck(mutex_);
                if (unboundedUnixSockFds_.size() >= maxCacheUnboundedUnixSockFdsCount_) {
                    RETRY_ON_EINTR(close(fd));
                    LOG(INFO) << "Up to the limit of caching fd, worker will close socket fd: " << fd;
                    continue;
                }
                unboundedUnixSockFds_.emplace(fd, GetSteadyClockTimeStampMs());
            }
            hasBeenLogged = false;
            // The following is for diagnostic only.
            // The return code from getsockopt is not important and
            // won't affect the rest of the run.
            ucred peer{};
            socklen_t len = sizeof(peer);
            (void)getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &peer, &len);
            LOG(INFO) << FormatString("Spawn uds connection %d for client (pid %d) service %s, socket fd %d", fd,
                                      peer.pid, this->ServiceName(), fd);
            // We will send back the fd for this connection and the peer will add this
            // information to the RegisterClientReqPb.
            UnixSockFd sock(fd);
            Status rc = sock.Send32(fd);
            INJECT_POINT("WorkerServiceImpl.AcceptFdLoop.AfterSend");
            if (rc.IsOk() && shmWorkerPort_ > 0) {
                rc = SockSendFd(fd, true, { tmpfd }, 1);
            }
            if (rc.IsError()) {
                RETRY_ON_EINTR(close(fd));
                {
                    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
                    unboundedUnixSockFds_.erase(fd);
                }
                LOG(ERROR) << "Send socket to client failed, worker will close socket fd " << fd
                           << ", err info:" << rc.ToString();
                continue;
            }
        } else {
            if (!hasBeenLogged) {
                hasBeenLogged = true;
                LOG(ERROR) << FormatString("Accept error for fd %d. Errno %d", listenFd_, errno);
            }
        }
    }
    return Status::OK();
}
}  // namespace worker
}  // namespace datasystem
