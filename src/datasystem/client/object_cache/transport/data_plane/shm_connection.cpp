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

/** Description: Implements endpoint-scoped client-to-worker shared-memory sessions. */

#include "datasystem/client/object_cache/transport/data_plane/shm_connection.h"

#include "datasystem/client/object_cache/transport/data_plane/shm_receive_buffer_owner.h"
#include "datasystem/client/object_cache/transport/data_plane/shm_send_buffer_owner.h"
#include "datasystem/client/object_cache/transport/object_read/object_read_types.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <exception>
#include <limits>
#include <new>
#include <utility>

#include <sys/socket.h>
#include <unistd.h>

#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/util/compatibility_manager.h"
#include "datasystem/common/util/fd_pass.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/version.h"

namespace datasystem {
namespace client {
namespace {

constexpr int64_t SHM_REFERENCE_RELEASE_TIMEOUT_MS = 1000;
constexpr uint64_t SHM_MAINTENANCE_MAX_INTERVAL_S = 5;
constexpr uint64_t SHM_MAINTENANCE_MIN_INTERVAL_S = 1;
constexpr uint64_t SHM_MAINTENANCE_INTERVAL_MS_PER_S = 1000;
constexpr int64_t SHM_CONNECT_FAILURE_BACKOFF_INITIAL_MS = 10;
constexpr int64_t SHM_CONNECT_FAILURE_BACKOFF_MAX_MS = 1000;
constexpr int64_t SHM_CONNECT_FAILURE_BACKOFF_MULTIPLIER = 2;

Status RemainingTimeoutMs(int64_t &timeoutMs)
{
    const int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    CHECK_FAIL_RETURN_STATUS(remainingUs > 0, K_RPC_DEADLINE_EXCEEDED, "API deadline exceeded");
    timeoutMs = TimeoutDuration::CeilUsToMs(remainingUs);
    return Status::OK();
}

void CloseFds(std::vector<int> &fds)
{
    for (int fd : fds) {
        if (fd >= 0) {
            RETRY_ON_EINTR(close(fd));
        }
    }
    fds.clear();
}

Status ConnectFdSocket(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient,
                       const TransportRequestContext &context, ShmFd &socketFd, bool &isScmTcp,
                       int32_t &serverFd)
{
    GetSocketPathReqPb pathReq;
    pathReq.set_token(context.token);
    pathReq.set_tenant_id(context.tenantId);
    GetSocketPathRspPb pathRsp;
    RETURN_IF_NOT_OK(rpcClient->InvokeGetSocketPath(pathReq, pathRsp));

    std::string endpoint;
    isScmTcp = pathRsp.shm_worker_port() > 0;
    if (isScmTcp) {
        endpoint = FormatString("tcp://%s:%d", workerAddr.Host(), pathRsp.shm_worker_port());
    } else {
        CHECK_FAIL_RETURN_STATUS(!pathRsp.path().empty(), K_NOT_SUPPORTED,
                                 "Worker did not provide an fd-passing endpoint");
        endpoint = FormatString("ipc://%s", pathRsp.path());
    }

    UnixSockFd socket(RPC_NO_FILE_FD, isScmTcp);
    Status connectRc = socket.Connect(endpoint);
    if (connectRc.IsError()) {
        // UnixSockFd does not close its fd in the destructor. Connect may leave an fd open
        // after a UDS connect failure or a TCP post-connect socket-option failure.
        socket.Close();
        return connectRc;
    }
    ShmFd socketOwner(socket.GetFd());
    int64_t remainingMs = 0;
    RETURN_IF_NOT_OK(RemainingTimeoutMs(remainingMs));
    RETURN_IF_NOT_OK(socket.SetTimeout(std::min<int64_t>(STUB_FRONTEND_TIMEOUT, remainingMs)));
    uint32_t rawServerFd = 0;
    RETURN_IF_NOT_OK(socket.Recv32(rawServerFd, false));
    CHECK_FAIL_RETURN_STATUS(rawServerFd <= static_cast<uint32_t>(std::numeric_limits<int32_t>::max()),
                             K_RUNTIME_ERROR, "Worker fd-passing server fd exceeds int32");
    serverFd = static_cast<int32_t>(rawServerFd);

    if (isScmTcp) {
        std::vector<int> probeFds;
        uint64_t requestId = 0;
        Status rc = SockRecvFd(socket.GetFd(), true, probeFds, requestId);
        CloseFds(probeFds);
        RETURN_IF_NOT_OK_APPEND_MSG(rc, "Receive SCMTCP locality probe failed");
    }
    RETURN_IF_NOT_OK(socket.SetTimeout(0));
    socketFd.Reset(socketOwner.Release());
    return Status::OK();
}

Status RegisterShmClient(const std::shared_ptr<WorkerRpcClient> &rpcClient,
                         const TransportRequestContext &context, int32_t serverFd, RegisterClientRspPb &response)
{
    RegisterClientReqPb request;
    request.set_token(context.token);
    request.set_version(DATASYSTEM_VERSION);
    request.set_git_hash(GetGitHash());
    request.set_heartbeat_enabled(true);
    request.set_socket_heartbeat(true);
    request.set_shm_enabled(true);
    request.set_server_fd(serverFd);
    request.set_tenant_id(context.tenantId);
    request.set_support_multi_shm_ref_count(true);
    request.set_compatibility_version(CompatibilityManager::Instance().GetCurrentCompatibilityVersion().ToString());
    return rpcClient->InvokeRegisterShmClient(request, response);
}

}  // namespace

ShmFdChannel::ShmFdChannel(std::shared_ptr<WorkerRpcClient> rpcClient, ShmFd socketFd, bool isScmTcp,
                           std::string clientId)
    : rpcClient_(std::move(rpcClient)),
      socketFd_(std::move(socketFd)),
      isScmTcp_(isScmTcp),
      clientId_(std::move(clientId))
{
    socketNumber_.store(socketFd_.Get(), std::memory_order_release);
}

ShmFdChannel::~ShmFdChannel()
{
    Close();
}

Status ShmFdChannel::GetClientFd(const std::vector<int> &workerFds, std::vector<int> &clientFds,
                                 const std::string &tenantId)
{
    clientFds.clear();
    CHECK_FAIL_RETURN_STATUS(!workerFds.empty(), K_INVALID, "Worker fd list is empty");
    std::lock_guard<bthread::Mutex> lock(mutex_);
    Raii closeOnExit([this]() {
        if (!IsAlive()) {
            socketFd_.Reset();
        }
    });
    const int socketNumber = socketNumber_.load(std::memory_order_acquire);
    CHECK_FAIL_RETURN_STATUS(IsAlive() && socketNumber != INVALID_SHM_FD, K_RPC_UNAVAILABLE,
                             "Shared-memory fd channel is closed");

    GetClientFdReqPb request;
    request.set_client_id(clientId_);
    request.set_request_id(++requestId_);
    for (int workerFd : workerFds) {
        request.add_worker_fds(workerFd);
    }
    request.set_token(auth_.token);
    request.set_tenant_id(tenantId.empty() ? auth_.tenantId : tenantId);
    GetClientFdRspPb response;
    Status rc = rpcClient_->InvokeGetClientFd(request, response);
    if (rc.IsError()) {
        return rc;
    }

    int64_t remainingMs = 0;
    RETURN_IF_NOT_OK(RemainingTimeoutMs(remainingMs));
    UnixSockFd socket(socketNumber, isScmTcp_);
    RETURN_IF_NOT_OK(socket.SetTimeout(remainingMs));
    uint64_t receivedRequestId = 0;
    rc = SockRecvFd(socketNumber, isScmTcp_, clientFds, receivedRequestId);
    LOG_IF_ERROR(socket.SetTimeout(0), "Restore shared-memory fd socket timeout failed");
    if (rc.IsError() || receivedRequestId != requestId_ || clientFds.size() != workerFds.size()) {
        CloseFds(clientFds);
        if (rc.IsError()) {
            return rc;
        }
        RETURN_STATUS(K_RUNTIME_ERROR, "Received shared-memory fds do not match GetClientFd request");
    }
    return Status::OK();
}

const std::string &ShmFdChannel::ClientId() const
{
    return clientId_;
}

void ShmFdChannel::UpdateAuth(const TransportRequestContext &context)
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    auth_ = context;
}

bool ShmFdChannel::IsAlive() const
{
    return alive_.load(std::memory_order_acquire);
}

void ShmFdChannel::Close()
{
    alive_.store(false, std::memory_order_release);
    const int socketNumber = socketNumber_.exchange(INVALID_SHM_FD, std::memory_order_acq_rel);
    if (socketNumber != INVALID_SHM_FD) {
        (void)shutdown(socketNumber, SHUT_RDWR);
    }
    // Do not wait for a GetClientFd BRPC while a DataPlaneManager endpoint lock may be held.
    // The in-flight transaction owns mutex_ and closes socketFd_ through closeOnExit.
    std::unique_lock<bthread::Mutex> lock(mutex_, std::try_to_lock);
    if (lock.owns_lock()) {
        socketFd_.Reset();
    }
}

ShmSession::ShmSession(HostPort workerAddr, std::shared_ptr<WorkerRpcClient> rpcClient,
                       std::shared_ptr<ShmFdChannel> fdChannel, std::shared_ptr<MmapManager> mmapManager,
                       std::string clientId, std::string workerStartId, uint32_t lockId,
                       std::weak_ptr<ThreadPool> releasePool, TransportRequestContext auth,
                       bool supportMultiRefCount, std::shared_ptr<std::atomic<bool>> scaleInDraining)
    : workerAddr_(std::move(workerAddr)),
      rpcClient_(std::move(rpcClient)),
      fdChannel_(std::move(fdChannel)),
      mmapManager_(std::move(mmapManager)),
      clientId_(std::move(clientId)),
      workerStartId_(std::move(workerStartId)),
      lockId_(lockId),
      releasePool_(std::move(releasePool)),
      auth_(std::move(auth)),
      supportMultiRefCount_(supportMultiRefCount),
      scaleInDraining_(std::move(scaleInDraining))
{
}

Status ShmSession::Create(const HostPort &workerAddr, const std::shared_ptr<WorkerRpcClient> &rpcClient,
                          const TransportRequestContext &context, std::weak_ptr<ThreadPool> releasePool,
                          std::shared_ptr<std::atomic<bool>> scaleInDraining,
                          std::shared_ptr<ShmSession> &session)
{
    session.reset();
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient);
    ShmFd socketFd;
    bool isScmTcp = false;
    int32_t serverFd = INVALID_SHM_FD;
    RETURN_IF_NOT_OK(ConnectFdSocket(workerAddr, rpcClient, context, socketFd, isScmTcp, serverFd));

    RegisterClientRspPb response;
    RETURN_IF_NOT_OK(RegisterShmClient(rpcClient, context, serverFd, response));
    CHECK_FAIL_RETURN_STATUS(!response.client_id().empty(), K_RUNTIME_ERROR,
                             "RegisterClient returned an empty client ID");
    CHECK_FAIL_RETURN_STATUS(!response.unhealthy(), K_NOT_READY,
                             "Target worker is unhealthy during shared-memory registration");
    auto fdChannel =
        std::make_shared<ShmFdChannel>(rpcClient, std::move(socketFd), isScmTcp, response.client_id());
    fdChannel->UpdateAuth(context);
    auto mmapManager = std::make_shared<MmapManager>(fdChannel, response.enable_huge_tlb());
    auto candidate = std::shared_ptr<ShmSession>(
        new ShmSession(workerAddr, rpcClient, std::move(fdChannel), std::move(mmapManager), response.client_id(),
                       response.worker_start_id(), response.lock_id(), std::move(releasePool), context,
                       response.support_multi_shm_ref_count(), std::move(scaleInDraining)));
    const uint64_t deadTimeoutSeconds =
        std::max<uint64_t>(response.client_dead_timeout_s(), SHM_MAINTENANCE_MIN_INTERVAL_S);
    candidate->maintenanceIntervalMs_ = std::min<uint64_t>(deadTimeoutSeconds, SHM_MAINTENANCE_MAX_INTERVAL_S)
                                        * SHM_MAINTENANCE_INTERVAL_MS_PER_S;
    Status maintenanceRc = candidate->StartMaintenance();
    if (maintenanceRc.IsError()) {
        candidate->Close(true);
        return maintenanceRc;
    }
    session = std::move(candidate);
    return Status::OK();
}

ShmSession::~ShmSession()
{
    Close(true);
}

Status ShmSession::Get(const DataGetBatchRequest &inputs, GetRspPb &response, std::vector<RpcMessage> &payloads)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Shared-memory session is closed");
    CHECK_FAIL_RETURN_STATUS(!inputs.empty(), K_INVALID, "WorkerOCService Get request is empty");
    CHECK_FAIL_RETURN_STATUS(inputs.front().context != nullptr, K_INVALID, "Transport read context is missing");
    const auto &readContext = *inputs.front().context;
    {
        std::lock_guard<bthread::Mutex> lock(authMutex_);
        auth_ = readContext.requestContext;
    }
    fdChannel_->UpdateAuth(readContext.requestContext);

    GetReqPb request;
    request.set_client_id(clientId_);
    request.set_token(readContext.requestContext.token);
    request.set_tenant_id(readContext.requestContext.tenantId);
    request.set_sub_timeout(readContext.subTimeoutMs);
    request.set_no_query_l2cache(!readContext.queryL2Cache);
    request.set_return_object_index(true);
    int64_t remainingMs = 0;
    RETURN_IF_NOT_OK(RemainingTimeoutMs(remainingMs));
    request.set_request_timeout(remainingMs);
    for (const auto &input : inputs) {
        CHECK_FAIL_RETURN_STATUS(input.context == inputs.front().context, K_INVALID,
                                 "Batch Get transport contexts do not match");
        request.add_object_keys(input.objectKey);
    }
    return rpcClient_->InvokeClientGet(request, response, payloads);
}

Status ShmSession::ValidateObjectInfo(const GetRspPb::ObjectInfoPb &info, uint64_t &offset, uint64_t &metadataSize,
                                      uint64_t &dataSize, uint64_t &mmapSize) const
{
    CHECK_FAIL_RETURN_STATUS(info.store_fd() > 0, K_NOT_FOUND,
                             "WorkerOCService Get did not return shared memory");
    CHECK_FAIL_RETURN_STATUS(info.store_fd() <= std::numeric_limits<int>::max(), K_RUNTIME_ERROR,
                             "WorkerOCService Get shared-memory fd exceeds local limits");
    CHECK_FAIL_RETURN_STATUS(info.offset() >= 0 && info.data_size() >= 0 && info.metadata_size() >= 0
                                 && info.mmap_size() > 0,
                             K_RUNTIME_ERROR, "WorkerOCService Get returned invalid shared-memory bounds");
    offset = static_cast<uint64_t>(info.offset());
    metadataSize = static_cast<uint64_t>(info.metadata_size());
    dataSize = static_cast<uint64_t>(info.data_size());
    mmapSize = static_cast<uint64_t>(info.mmap_size());
    CHECK_FAIL_RETURN_STATUS(offset <= mmapSize && metadataSize <= mmapSize - offset
                                 && dataSize <= mmapSize - offset - metadataSize,
                             K_RUNTIME_ERROR, "WorkerOCService Get shared-memory range exceeds mmap size");
    CHECK_FAIL_RETURN_STATUS(!info.shm_id().empty(), K_RUNTIME_ERROR,
                             "WorkerOCService Get returned an empty shm ID");
    CHECK_FAIL_RETURN_STATUS(offset <= static_cast<uint64_t>(std::numeric_limits<ptrdiff_t>::max())
                                 && mmapSize <= std::numeric_limits<size_t>::max()
                                 && metadataSize <= std::numeric_limits<uint32_t>::max(),
                             K_RUNTIME_ERROR, "WorkerOCService Get shared-memory metadata exceeds local limits");
    CHECK_FAIL_RETURN_STATUS(
        info.write_mode() <= static_cast<uint32_t>(WriteMode::WRITE_BACK_L2_CACHE_EVICT)
            && info.consistency_type() <= static_cast<uint32_t>(ConsistencyType::CAUSAL)
            && info.cache_type() <= static_cast<uint32_t>(CacheType::DISK),
        K_RUNTIME_ERROR, "WorkerOCService Get returned an invalid object mode");
    return Status::OK();
}

Status ShmSession::BuildResult(const GetRspPb::ObjectInfoPb &info, const DataGetRequest &input, DataGetResult &result)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Shared-memory session is closed");
    CHECK_FAIL_RETURN_STATUS(input.context != nullptr, K_INVALID, "Transport read context is missing");
    uint64_t offset = 0;
    uint64_t metadataSize = 0;
    uint64_t dataSize = 0;
    uint64_t mmapSize = 0;
    RETURN_IF_NOT_OK(ValidateObjectInfo(info, offset, metadataSize, dataSize, mmapSize));

    auto unit = std::make_shared<ShmUnitInfo>();
    unit->fd = static_cast<int>(info.store_fd());
    unit->mmapSize = mmapSize;
    unit->offset = static_cast<ptrdiff_t>(info.offset());
    unit->size = dataSize;
    unit->id = ShmKey::Intern(info.shm_id());
    RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd(input.context->requestContext.tenantId, unit));
    auto mmapEntry = mmapManager_->GetMmapEntryByFd(unit->fd);
    CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr && unit->pointer != nullptr, K_RUNTIME_ERROR,
                             "Shared-memory mmap entry is unavailable");
    CHECK_FAIL_RETURN_STATUS(mmapEntry->GetMmapSize() == mmapSize, K_RUNTIME_ERROR,
                             "WorkerOCService Get mmap size does not match the cached fd");

    DataGetResult built;
    built.response.mutable_error()->set_error_code(K_OK);
    built.response.set_data_size(info.data_size());
    built.response.set_data_source(DataTransferSource::DATA_ALREADY_TRANSFERRED);
    built.externalData = static_cast<const uint8_t *>(unit->pointer) + offset;
    built.externalSize = dataSize;
    ExternalBufferMeta meta;
    meta.metadataSize = metadataSize;
    meta.shmId = unit->id;
    meta.lockId = lockId_;
    meta.isSeal = info.is_seal();
    meta.mode.SetWriteMode(WriteMode(info.write_mode()));
    meta.mode.SetConsistencyType(ConsistencyType(info.consistency_type()));
    meta.mode.SetCacheType(CacheType(info.cache_type()));
    meta.workerAddr = workerAddr_;
    built.externalMeta = std::move(meta);
    RETURN_IF_NOT_OK(RegisterReference(unit->id));
    try {
        built.externalOwner = std::make_shared<ShmReceiveBufferOwner>(
            shared_from_this(), std::move(mmapEntry), unit->id, input.context, releasePool_);
    } catch (const std::bad_alloc &e) {
        RETURN_STATUS(K_RUNTIME_ERROR, e.what());
    }
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Shared-memory session closed while materializing the result");
    built.kind = AccessTransportKind::SHM;
    result = std::move(built);
    return Status::OK();
}

Status ShmSession::BuildQueryAndGetResult(const QueryAndGetShmInfoPb &info, const DataGetRequest &input,
                                          DataGetResult &result)
{
    GetRspPb::ObjectInfoPb objectInfo;
    objectInfo.set_store_fd(info.store_fd());
    objectInfo.set_offset(info.offset());
    objectInfo.set_data_size(info.data_size());
    objectInfo.set_metadata_size(info.metadata_size());
    objectInfo.set_mmap_size(info.mmap_size());
    objectInfo.set_shm_id(info.shm_id());
    objectInfo.set_is_seal(info.is_seal());
    objectInfo.set_write_mode(info.write_mode());
    objectInfo.set_consistency_type(info.consistency_type());
    objectInfo.set_cache_type(info.cache_type());
    return BuildResult(objectInfo, input, result);
}

Status ShmSession::MmapWriteRegion(const CreateRspPb &createRsp, const TransportRequestContext &context,
                                   uint64_t size, ObjectBufferInfo &info)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Shared-memory session is closed");
    CHECK_FAIL_RETURN_STATUS(createRsp.store_fd() > 0, K_RUNTIME_ERROR,
                             "WorkerOCService Create did not return shared memory");
    CHECK_FAIL_RETURN_STATUS(createRsp.store_fd() <= std::numeric_limits<int>::max(), K_RUNTIME_ERROR,
                             "WorkerOCService Create shared-memory fd exceeds local limits");
    // createRsp.offset() is uint64 (always >= 0); the offset <= mmapSize bounds check below covers it.
    CHECK_FAIL_RETURN_STATUS(createRsp.mmap_size() > 0, K_RUNTIME_ERROR,
                             "WorkerOCService Create returned invalid shared-memory bounds");
    const uint64_t offset = static_cast<uint64_t>(createRsp.offset());
    const uint64_t mmapSize = static_cast<uint64_t>(createRsp.mmap_size());
    const uint64_t metadataSize = static_cast<uint64_t>(createRsp.metadata_size());
    CHECK_FAIL_RETURN_STATUS(
        offset <= mmapSize && metadataSize <= mmapSize - offset && size <= mmapSize - offset - metadataSize,
        K_RUNTIME_ERROR, "WorkerOCService Create shared-memory range exceeds mmap size");
    // Defensive upper bounds (aligned with the read path ValidateObjectInfo): the subsequent
    // static_cast<ptrdiff_t>(offset) and unit->pointer + offset pointer arithmetic are UB on overflow.
    CHECK_FAIL_RETURN_STATUS(offset <= static_cast<uint64_t>(std::numeric_limits<ptrdiff_t>::max())
                                 && mmapSize <= std::numeric_limits<size_t>::max()
                                 && metadataSize <= std::numeric_limits<uint32_t>::max(),
                             K_RUNTIME_ERROR, "WorkerOCService Create shared-memory metadata exceeds local limits");
    CHECK_FAIL_RETURN_STATUS(!createRsp.shm_id().empty(), K_RUNTIME_ERROR,
                             "WorkerOCService Create returned an empty shm ID");

    auto unit = std::make_shared<ShmUnitInfo>();
    unit->fd = static_cast<int>(createRsp.store_fd());
    unit->mmapSize = mmapSize;
    unit->offset = static_cast<ptrdiff_t>(createRsp.offset());
    unit->size = size;
    unit->id = ShmKey::Intern(createRsp.shm_id());
    RETURN_IF_NOT_OK(mmapManager_->LookupUnitsAndMmapFd(context.tenantId, unit));
    auto mmapEntry = mmapManager_->GetMmapEntryByFd(unit->fd);
    CHECK_FAIL_RETURN_STATUS(mmapEntry != nullptr && unit->pointer != nullptr, K_RUNTIME_ERROR,
                             "Shared-memory mmap entry is unavailable");
    CHECK_FAIL_RETURN_STATUS(mmapEntry->GetMmapSize() == mmapSize, K_RUNTIME_ERROR,
                             "WorkerOCService Create mmap size does not match the cached fd");
    RETURN_IF_NOT_OK(RegisterReference(unit->id));
    info.pointer = static_cast<uint8_t *>(unit->pointer) + offset;
    info.metadataSize = metadataSize;
    info.dataSize = size;
    info.mmapEntry = mmapEntry;
    info.shmId = unit->id;
    info.sessionLockId = lockId_;
    info.useSessionLockId = true;
    try {
        // Owner holds its own mmapEntry ref so the mapping outlives until the queued DecreaseReference
        // completes (defense against unmap-before-worker-ack). CheckAlive gates Publish on both RPC
        // client liveness and SHM session/fd data-plane liveness (preserves the original
        // session->IsAlive() gate that covers alive_ + fdChannel_).
        auto sessionPtr = shared_from_this();
        info.receiveBufferOwner = std::make_shared<ShmSendBufferOwner>(
            rpcClient_, unit->id, context, releasePool_, mmapEntry,
            [sessionPtr]() { return sessionPtr->IsAlive(); });
    } catch (const std::bad_alloc &e) {
        // RegisterReference already incremented the worker ref; release it on OOM to avoid a leak.
        LOG_IF_ERROR(DecreaseReferenceByRequestClient(context, unit->id),
                     "DecreaseReference after ShmSendBufferOwner OOM");
        RETURN_STATUS(K_RUNTIME_ERROR, e.what());
    }
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE,
                             "Shared-memory session closed while mapping the write region");
    return Status::OK();
}

bool ShmSession::IsAlive() const
{
    return alive_.load(std::memory_order_acquire) && rpcClient_ != nullptr && rpcClient_->IsAlive()
           && fdChannel_ != nullptr && fdChannel_->IsAlive();
}

const std::string &ShmSession::ClientId() const
{
    return clientId_;
}

const std::string &ShmSession::WorkerStartId() const
{
    return workerStartId_;
}

void ShmSession::Close(bool notifyWorker)
{
    const bool wasAlive = alive_.exchange(false, std::memory_order_acq_rel);
    if (fdChannel_ != nullptr) {
        fdChannel_->Close();
    }
    if (mmapManager_ != nullptr) {
        // Repeat the clear on idempotent Close calls: an in-flight cold mmap that observed the old
        // generation may finish after the first clear, and its caller will then invalidate the session again.
        mmapManager_->Clear();
    }
    if (!wasAlive) {
        return;
    }
    if (notifyWorker) {
        ScheduleDisconnect();
    }
}

void ShmSession::CloseForScaleIn()
{
    scaleInDraining_->store(true, std::memory_order_release);
    Close(false);
    ScheduleDisconnect();
}

void ShmSession::ScheduleDisconnect()
{
    if (rpcClient_ == nullptr || !rpcClient_->IsAlive() || clientId_.empty()) {
        return;
    }
    if (disconnectScheduled_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    TransportRequestContext context;
    {
        std::lock_guard<bthread::Mutex> lock(authMutex_);
        context = auth_;
    }
    auto releasePool = releasePool_.lock();
    if (releasePool == nullptr) {
        // The fd channel is already closed. Worker socket-heartbeat/client-lost cleanup
        // remains the shutdown fallback when the async pool is unavailable.
        return;
    }
    auto rpcClient = rpcClient_;
    const std::string clientId = clientId_;
    try {
        releasePool->Execute([rpcClient = std::move(rpcClient), clientId, context = std::move(context)]() {
            DisconnectClientReqPb request;
            request.set_client_id(clientId);
            request.set_token(context.token);
            DisconnectClientRspPb response;
            LOG_IF_ERROR(rpcClient->InvokeDisconnectShmClient(request, response),
                         "Disconnect routed shared-memory client failed");
        });
    } catch (const std::exception &e) {
        LOG(WARNING) << "Submit routed SHM disconnect failed: " << e.what();
    }
}

Status ShmSession::DecreaseReference(const TransportRequestContext &context, const ShmKey &shmId)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Shared-memory session is closed");
    if (supportMultiRefCount_) {
        TransportRequestContext requestContext = context;
        requestContext.clientId = clientId_;
        return rpcClient_->InvokeDecreaseReference(requestContext, shmId);
    }

    bool callWorker = false;
    {
        std::lock_guard<bthread::Mutex> lock(refMutex_);
        auto iter = localRefCounts_.find(shmId);
        CHECK_FAIL_RETURN_STATUS(iter != localRefCounts_.end() && iter->second > 0, K_RUNTIME_ERROR,
                                 "Shared-memory reference is not owned by this session");
        if (--iter->second == 0) {
            localRefCounts_.erase(iter);
            callWorker = true;
        }
    }
    RETURN_OK_IF_TRUE(!callWorker);
    TransportRequestContext requestContext = context;
    requestContext.clientId = clientId_;
    return rpcClient_->InvokeDecreaseReference(requestContext, shmId);
}

Status ShmSession::DecreaseReferenceByRequestClient(const TransportRequestContext &context, const ShmKey &shmId)
{
    CHECK_FAIL_RETURN_STATUS(IsAlive(), K_RPC_UNAVAILABLE, "Shared-memory session is closed");
    // Use the request context's own clientId (the SDK global clientId that routed Create registered the
    // reference under). Do NOT override with the session UUID — the worker would not match the release.
    return rpcClient_->InvokeDecreaseReference(context, shmId);
}

Status ShmSession::RegisterReference(const ShmKey &shmId)
{
    // A modern Worker tracks every Get reference independently, so the Buffer owner's
    // one-shot Release is sufficient and the hot path does not need a session-wide map lock.
    if (supportMultiRefCount_) {
        return Status::OK();
    }
    try {
        std::lock_guard<bthread::Mutex> lock(refMutex_);
        ++localRefCounts_[shmId];
    } catch (const std::bad_alloc &e) {
        RETURN_STATUS(K_RUNTIME_ERROR, e.what());
    }
    return Status::OK();
}

Status ShmSession::StartMaintenance()
{
    CHECK_FAIL_RETURN_STATUS(TimerQueue::GetInstance()->Initialize(), K_RUNTIME_ERROR,
                             "Initialize shared-memory maintenance timer failed");
    return ScheduleMaintenance();
}

Status ShmSession::ScheduleMaintenance()
{
    if (!IsAlive()) {
        return Status::OK();
    }
    TimerQueue::TimerImpl timer;
    std::weak_ptr<ShmSession> weakSession = weak_from_this();
    return TimerQueue::GetInstance()->AddTimer(
        maintenanceIntervalMs_,
        [weakSession]() {
            auto session = weakSession.lock();
            if (session == nullptr || !session->IsAlive()) {
                return;
            }
            auto pool = session->releasePool_.lock();
            if (pool == nullptr) {
                session->Close(false);
                return;
            }
            try {
                pool->Execute([session]() { session->RunMaintenance(); });
            } catch (const std::exception &e) {
                LOG(WARNING) << "Submit routed SHM maintenance failed: " << e.what();
                session->Close(false);
            }
        },
        timer);
}

void ShmSession::RunMaintenance()
{
    if (!IsAlive()) {
        return;
    }
    TransportRequestContext context;
    {
        std::lock_guard<bthread::Mutex> lock(authMutex_);
        context = auth_;
    }
    HeartbeatReqPb request;
    request.set_client_id(clientId_);
    request.set_token(context.token);
    request.set_removable(false);
    *request.mutable_released_worker_fds() = { releasedWorkerFds_.begin(), releasedWorkerFds_.end() };
    HeartbeatRspPb response;
    Status rc = rpcClient_->InvokeShmHeartbeat(request, response);
    const bool voluntaryScaleDown = rc.IsOk() && response.is_voluntary_scale_down();
    if (voluntaryScaleDown) {
        rc = Status(K_NOT_READY, WORKER_DRAINING_FOR_SCALE_IN_MESSAGE);
    } else if (rc.IsOk() && !workerStartId_.empty() && response.worker_start_id() != workerStartId_) {
        rc = Status(K_RPC_UNAVAILABLE, "Target worker restarted during shared-memory session");
    } else if (rc.IsOk() && response.client_removed()) {
        rc = Status(K_RPC_UNAVAILABLE, "Target worker removed the shared-memory client");
    } else if (rc.IsOk() && response.unhealthy()) {
        rc = Status(K_NOT_READY, "Target worker is unavailable during shared-memory maintenance");
    }
    if (rc.IsError()) {
        LOG(WARNING) << "Routed SHM maintenance failed for worker " << workerAddr_.ToString() << ": "
                     << rc.ToString();
        voluntaryScaleDown ? CloseForScaleIn() : Close(false);
        return;
    }

    releasedWorkerFds_.assign(response.expired_worker_fds().begin(), response.expired_worker_fds().end());
    mmapManager_->ClearExpiredFds(releasedWorkerFds_);
    rc = ScheduleMaintenance();
    if (rc.IsError()) {
        LOG(WARNING) << "Reschedule routed SHM maintenance failed for worker " << workerAddr_.ToString() << ": "
                     << rc.ToString();
        Close(false);
    }
}

ShmConnection::ShmConnection(HostPort workerAddr, std::shared_ptr<WorkerRpcClient> rpcClient,
                             std::weak_ptr<ThreadPool> releasePool)
    : workerAddr_(std::move(workerAddr)), rpcClient_(std::move(rpcClient)), releasePool_(std::move(releasePool))
{
}

ShmConnection::~ShmConnection()
{
    Teardown();
}

Status ShmConnection::Establish(const HostPort &workerAddr)
{
    CHECK_FAIL_RETURN_STATUS(workerAddr == workerAddr_, K_INVALID,
                             "Shared-memory connection endpoint does not match transporter");
    return Status::OK();
}

Status ShmConnection::WaitForConnecting(std::unique_lock<bthread::Mutex> &lock)
{
    const int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    CHECK_FAIL_RETURN_STATUS(remainingUs > 0, K_RPC_DEADLINE_EXCEEDED, "API deadline exceeded");
    const long waitUs = remainingUs > std::numeric_limits<long>::max()
                            ? std::numeric_limits<long>::max()
                            : static_cast<long>(remainingUs);
    if (cv_.wait_for(lock, waitUs) == ETIMEDOUT) {
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "Timed out waiting for shared-memory connection");
    }
    return Status::OK();
}

Status ShmConnection::CompleteConnectionAttempt(uint64_t attemptId, const std::shared_ptr<ShmSession> &candidate,
                                                Status result, std::shared_ptr<ShmSession> &session)
{
    bool publish = false;
    {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        connecting_ = false;
        if (result.IsOk() && scaleInDraining_->load(std::memory_order_acquire)) {
            result = Status(K_NOT_READY, WORKER_DRAINING_FOR_SCALE_IN_MESSAGE);
        }
        publish = result.IsOk() && !closed_ && attemptId == attemptId_;
        if (publish) {
            session_ = candidate;
            session = candidate;
            failureBackoffMs_ = SHM_CONNECT_FAILURE_BACKOFF_INITIAL_MS;
            retryAfter_ = {};
        } else if (result.IsError() && !closed_) {
            lastConnectFailure_ = result;
            retryAfter_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(failureBackoffMs_);
            failureBackoffMs_ = std::min<int64_t>(failureBackoffMs_ * SHM_CONNECT_FAILURE_BACKOFF_MULTIPLIER,
                                                  SHM_CONNECT_FAILURE_BACKOFF_MAX_MS);
        }
        cv_.notify_all();
    }
    if (!publish && candidate != nullptr) {
        candidate->Close(true);
    }
    if (result.IsError()) {
        return result;
    }
    CHECK_FAIL_RETURN_STATUS(publish, K_SHUTTING_DOWN, "Shared-memory connection closed during establishment");
    return Status::OK();
}

Status ShmConnection::Acquire(const TransportRequestContext &context, std::shared_ptr<ShmSession> &session)
{
    session.reset();
    uint64_t attemptId = 0;
    {
        std::unique_lock<bthread::Mutex> lock(mutex_);
        bool keepWaiting = connecting_ && !closed_;
        while (keepWaiting) {
            // WaitForConnecting blocks on the cv until the in-flight connect publishes a result
            // (connecting_/closed_ flip under mutex_) or the API deadline expires (returns error).
            RETURN_IF_NOT_OK(WaitForConnecting(lock));
            // Re-read the control flags after the cv wait released and reacquired mutex_.
            keepWaiting = connecting_ && !closed_;
        }
        CHECK_FAIL_RETURN_STATUS(!closed_, K_SHUTTING_DOWN, "Shared-memory connection is closed");
        if (scaleInDraining_->load(std::memory_order_acquire)) {
            return Status(K_NOT_READY, WORKER_DRAINING_FOR_SCALE_IN_MESSAGE);
        }
        if (session_ != nullptr && session_->IsAlive()) {
            session = session_;
            return Status::OK();
        }
        if (std::chrono::steady_clock::now() < retryAfter_) {
            return lastConnectFailure_;
        }
        connecting_ = true;
        attemptId = ++attemptId_;
    }

    std::shared_ptr<ShmSession> candidate;
    Status result = ShmSession::Create(workerAddr_, rpcClient_, context, releasePool_, scaleInDraining_, candidate);
    return CompleteConnectionAttempt(attemptId, candidate, std::move(result), session);
}

bool ShmConnection::IsAlive() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return !closed_ && rpcClient_ != nullptr && rpcClient_->IsAlive();
}

AccessTransportKind ShmConnection::Kind() const
{
    return AccessTransportKind::SHM;
}

void ShmConnection::Teardown()
{
    std::shared_ptr<ShmSession> stale;
    {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        if (closed_) {
            return;
        }
        closed_ = true;
        ++attemptId_;
        stale = std::move(session_);
        cv_.notify_all();
    }
    if (stale != nullptr) {
        stale->Close(true);
    }
}

}  // namespace client
}  // namespace datasystem
