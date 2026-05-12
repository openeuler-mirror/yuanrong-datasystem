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
 * Description: Defines the worker client remote class to communicate with the worker service.
 */
#include "datasystem/client/object_cache/client_worker_api/client_worker_remote_api.h"

#include <cstdint>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"

using datasystem::client::ClientWorkerRemoteCommonApi;

namespace datasystem {
namespace object_cache {
static constexpr uint32_t BIT_NUM_OF_INT = 32;
static constexpr char URMA_TRANSPORT_FAILED_MSG[] = "URMA transport failed";
static constexpr char CLIENT_TO_WORKER_FALLBACK[] = "client->worker";
const std::unordered_set<StatusCode> RETRY_ERROR_CODE{ StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED,
                                                       StatusCode::K_RPC_DEADLINE_EXCEEDED,
                                                       StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_OUT_OF_MEMORY };
static constexpr uint64_t P2P_TIMEOUT_MS = 60000;
constexpr uint64_t P2P_SUBSCRIBE_TIMEOUT_MS = 20000;

namespace {
bool IsUrmaFallbackPayload(const std::shared_ptr<ObjectBufferInfo> &bufferInfo)
{
    return bufferInfo->ubUrmaDataInfo != nullptr && !bufferInfo->ubDataSentByMemoryCopy;
}

Status AppendPublishPayload(std::atomic<uint64_t> &pendingBytes, const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                            std::vector<MemView> &payloads, UrmaFallbackTcpLimiter::Ticket &ticket)
{
    if (IsUrmaFallbackPayload(bufferInfo)) {
        auto rc = UrmaFallbackTcpLimiter::TryAcquire(
            pendingBytes, bufferInfo->dataSize,
            Status(StatusCode::K_URMA_ERROR, URMA_TRANSPORT_FAILED_MSG), CLIENT_TO_WORKER_FALLBACK, ticket);
        if (rc.IsError()) {
            LOG(WARNING) << "Client-to-worker TCP fallback payload rejected: " << rc.ToString();
            return rc;
        }
    }
    payloads.emplace_back(bufferInfo->pointer, bufferInfo->dataSize);
    return Status::OK();
}

void FillMultiPublishObjectInfo(const std::shared_ptr<ObjectBufferInfo> &bufferInfo,
                                const std::vector<uint64_t> *blobSizes, MultiPublishReqPb &req)
{
    MultiPublishReqPb::ObjectInfoPb objectInfoPb;
    if (blobSizes != nullptr && !blobSizes->empty()) {
        objectInfoPb.mutable_blob_sizes()->Add(blobSizes->begin(), blobSizes->end());
    }
    objectInfoPb.set_object_key(bufferInfo->objectKey);
    objectInfoPb.set_data_size(bufferInfo->dataSize);
    objectInfoPb.set_shm_id(bufferInfo->shmId);
    req.mutable_object_info()->Add(std::move(objectInfoPb));
}

void InitMultiPublishReq(const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo, const PublishParam &param,
                         const std::string &clientId, MultiPublishReqPb &req)
{
    req.set_client_id(clientId);
    req.set_ttl_second(param.ttlSecond);
    req.set_write_mode(static_cast<uint32_t>(bufferInfo[0]->objectMode.GetWriteMode()));
    req.set_consistency_type(static_cast<uint32_t>(bufferInfo[0]->objectMode.GetConsistencyType()));
    req.set_cache_type(static_cast<uint32_t>(bufferInfo[0]->objectMode.GetCacheType()));
    req.set_istx(param.isTx);
    req.set_existence(static_cast<::datasystem::ExistenceOptPb>(param.existence));
    req.set_is_replica(param.isReplica);
    req.set_auto_release_memory_ref(!bufferInfo[0]->shmId.Empty());
}
}  // namespace

ClientWorkerRemoteApi::ClientWorkerRemoteApi(HostPort hostPort, RpcCredential cred, HeartbeatType heartbeatType,
                                             SensitiveValue token, Signature *signature, std::string tenantId,
                                             bool enableCrossNodeConnection, bool enableExclusiveConnection,
                                             std::string deviceId)
    : client::IClientWorkerCommonApi(hostPort, heartbeatType, enableCrossNodeConnection, signature),
      ClientWorkerBaseApi(hostPort, heartbeatType, enableCrossNodeConnection, signature),
      ClientWorkerRemoteCommonApi(hostPort, cred, heartbeatType, std::move(token), signature, std::move(tenantId),
                                  enableCrossNodeConnection, enableExclusiveConnection, std::move(deviceId))
{
    if (enableExclusiveConnection) {
        // Assign a value and then bump the counter. This id is a client-side-only identifier, a bit like a
        // client id but lighter weight for performance sensitive comparisons (existing client id is a large
        // string and costly for lookups and string compare)
        exclusiveId_ = exclusiveIdGen_++;
    }
}

Status ClientWorkerRemoteApi::Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs, uint64_t fastTransportSize)
{
    RETURN_IF_NOT_OK(ClientWorkerRemoteCommonApi::Init(requestTimeoutMs, connectTimeoutMs, fastTransportSize));
    std::shared_ptr<RpcChannel> channel;
    channel = std::make_shared<RpcChannel>(hostPort_, cred_);
    // We will enable uds after handshaking with the worker.
    if (IsShmEnableByUDS()) {
        channel->SetServiceUdsEnabled(WorkerOCService_Stub::FullServiceName(),
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
    }
    if (clientDeadTimeoutMs_ > 0) {
        connectTimeoutMs = std::min(clientDeadTimeoutMs_, static_cast<uint64_t>(requestTimeoutMs));
    }
    stub_ = std::make_unique<WorkerOCService_Stub>(channel, connectTimeoutMs);
    if (enableExclusiveConnection_ && exclusiveId_.has_value() && IsShmEnableByUDS()) {
        // Note: exclusiveConnSockPath_ will be initialized during client register call driven from base class Init()
        stub_->SetExclusiveConnInfo(exclusiveId_, exclusiveConnSockPath_);
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::InitDecreaseQueue()
{
    QueueInfo defaultMeta;
    // futexFlag + shmId(uuid) + clientId(uuid). The size of the queue must be the same as when the worker constructed
    // the queue.
    uint32_t elementSize = defaultMeta.elementFlagSize + defaultMeta.elementDataSize + defaultMeta.elementDataSize;
    uint32_t lockId = lockId_ % BIT_NUM_OF_INT;  // One queue can support 32 client writing into it.
    decreaseRPCQ_ = std::make_shared<ShmCircularQueue>(defaultMeta.capacity, elementSize, decShmUnit_, lockId, true);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(decreaseRPCQ_->Init(), "Init failed with shm circular queue.");
    decreaseRPCQ_->UpdateQueueMeta();
    return Status::OK();
}

Status ClientWorkerRemoteApi::ReconnectWorker(const std::vector<std::string> &gRefIds)
{
    LOG(INFO) << "Start to reconnect worker.";
    GRefRecoveryPb extendPb;
    for (const auto &id : gRefIds) {
        extendPb.add_object_keys(id);
    }
    RegisterClientReqPb req;
    req.add_extend()->PackFrom(extendPb);
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK(Connect(req, connectTimeoutMs_, true));
    RETURN_IF_NOT_OK(TryFastTransportAfterHeartbeat());
    if (enableExclusiveConnection_ && exclusiveId_.has_value() && IsShmEnableByUDS()) {
        // exclusiveConnSockPath_ needs to be updated after reconnecting to worker.
        stub_->SetExclusiveConnInfo(exclusiveId_, exclusiveConnSockPath_);
    }
    return Status::OK();
}

void ClientWorkerRemoteApi::RecreateOCStub()
{
    // Recreate the OC service stub after hostPort_ changes (e.g., worker IP change).
    // Connect() only updates commonWorkerSession_, not stub_.
    auto channel = std::make_shared<RpcChannel>(hostPort_, cred_);
    if (IsShmEnableByUDS()) {
        channel->SetServiceUdsEnabled(WorkerOCService_Stub::FullServiceName(),
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
    }
    int32_t stubTimeout = connectTimeoutMs_;
    if (clientDeadTimeoutMs_ > 0) {
        stubTimeout = std::min(clientDeadTimeoutMs_, static_cast<uint64_t>(requestTimeoutMs_));
    }
    stub_ = std::make_unique<WorkerOCService_Stub>(channel, stubTimeout);
    if (enableExclusiveConnection_ && exclusiveId_.has_value() && IsShmEnableByUDS()) {
        stub_->SetExclusiveConnInfo(exclusiveId_, exclusiveConnSockPath_);
    }
}

Status ClientWorkerRemoteApi::Create(const std::string &objectKey, int64_t dataSize, uint32_t &version,
                                     uint64_t &metadataSize, std::shared_ptr<ShmUnitInfo> &shmBuf,
                                     std::shared_ptr<UrmaRemoteAddrPb> &urmaDataInfo, const CacheType &cacheType)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_CREATE_LATENCY);
    (void)urmaDataInfo;
    VLOG(1) << AppendSrcDstForLog(
        FormatString("Begin to create object, client id: %s, object key: %s", clientId_, objectKey), "",
        hostPort_.ToString());
    CHECK_FAIL_RETURN_STATUS(dataSize > 0, StatusCode::K_INVALID,
                             FormatString("data size:%lld must be more than 0!", dataSize));
    CreateReqPb req;
    req.set_object_key(objectKey);
    req.set_client_id(clientId_);
    req.set_data_size(dataSize);
    req.set_cache_type(static_cast<uint32_t>(cacheType));
    req.set_request_timeout(requestTimeoutMs_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when create data.");

    CreateRspPb rsp;
    PerfPoint partPoint(PerfKey::RPC_CLIENT_CREATE_OBJECT);
    RETURN_IF_NOT_OK(RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                             "Fail to generate signature when create date.");
            VLOG(1) << "Start to send rpc to create object: " << req.object_key();
            return stub_->Create(opts, req, rsp);
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeoutMs_));
    INJECT_POINT("ClientWorkerApi.Create.MockTimeout");
    partPoint.Record();

    shmBuf->fd = rsp.store_fd();
    shmBuf->mmapSize = rsp.mmap_size();
    shmBuf->offset = static_cast<ptrdiff_t>(rsp.offset());
    shmBuf->id = ShmKey::Intern(rsp.shm_id());
    metadataSize = rsp.metadata_size();
    version = workerVersion_.load(std::memory_order_relaxed);
#ifdef USE_URMA
    // Extract URMA info from response when enabled (for Create+MemoryCopy+Publish path).
    if (IsUrmaEnabled() && rsp.has_urma_info()) {
        if (urmaDataInfo == nullptr) {
            urmaDataInfo = std::make_shared<UrmaRemoteAddrPb>();
        }
        urmaDataInfo->CopyFrom(rsp.urma_info());
    }
#endif
    return Status::OK();
}

Status ClientWorkerRemoteApi::MultiCreate(bool skipCheckExistence, std::vector<MultiCreateParam> &createParams,
                                          uint32_t &version, std::vector<bool> &exists, bool &useShmTransfer)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_CREATE_LATENCY);
    MultiCreateReqPb req;
    req.set_skip_check_existence(skipCheckExistence);
    req.set_client_id(clientId_);
    int sz = static_cast<int>(createParams.size());
    req.mutable_object_key()->Reserve(sz);
    req.mutable_data_size()->Reserve(sz);
    req.set_request_timeout(requestTimeoutMs_);
    for (auto &param : createParams) {
        req.add_object_key(param.objectKey);
        req.add_data_size(param.dataSize);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when create data.");

    PerfPoint point(PerfKey::CLIENT_MULTI_CREATE_IPC);
    MultiCreateRspPb rsp;
    RETURN_IF_NOT_OK(RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                             "Fail to generate signature when create date.");
            return stub_->MultiCreate(opts, req, rsp);
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeoutMs_));

    CHECK_FAIL_RETURN_STATUS(
        createParams.size() == static_cast<size_t>(rsp.results().size()), K_INVALID,
        FormatString("The length of objectKeyList (%zu) and dataSizeList (%zu) should be the same.",
                     createParams.size(), rsp.results().size()));
    if (!skipCheckExistence) {
        CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(rsp.exists_size()) == createParams.size(), K_INVALID,
                                 "The size of rspExists is not consistent with createParams");
        exists.assign(rsp.exists().begin(), rsp.exists().end());
    }
    PostMultiCreate(skipCheckExistence, rsp, createParams, useShmTransfer, point, version, exists);
    return Status::OK();
}

Status ClientWorkerRemoteApi::HealthCheck(ServerState &state)
{
    HealthCheckRequestPb req;
    HealthCheckReplyPb rsp;
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when create data.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RpcOptions opts;
    // HealthCheck must time out in very short time, now is 3s
    opts.SetTimeout(MIN_HEARTBEAT_INTERVAL_MS);
    state = ServerState::NORMAL;
    if (storeNotifyReboot_) {
        storeNotifyReboot_ = false;
        state = ServerState::REBOOT;
    }
    return stub_->HealthCheck(opts, req, rsp);
}

bool ClientWorkerRemoteApi::IsAllGetFailed(GetRspPb &rsp)
{
    for (const auto &obj : rsp.objects()) {
        if (obj.data_size() != -1) {
            return false;
        }
    }
    return true;
}

#ifdef USE_URMA
Status ClientWorkerRemoteApi::ResolveUBGetSize(const GetParam &getParam, const std::string &tenantId,
                                               uint64_t &totalRequiredSize, bool &fallbackToTcp)
{
    totalRequiredSize = getParam.ubTotalSize;
    fallbackToTcp = false;
    if (totalRequiredSize > 0) {
        return Status::OK();
    }
    if (getParam.ubMetaResolved) {
        fallbackToTcp = true;
        return Status::OK();
    }
    std::vector<ObjMetaInfo> objMetas;
    Status metaRc = GetObjMetaInfo(tenantId, getParam.objectKeys, objMetas);
    if (metaRc.IsError()) {
        return metaRc;
    }
    if (objMetas.size() != getParam.objectKeys.size()) {
        fallbackToTcp = true;
        LOG(WARNING) << "GetObjMetaInfo object count mismatch: expected " << getParam.objectKeys.size() << " but got "
                     << objMetas.size() << ", fallback to TCP/IP payload before get.";
        return Status::OK();
    }
    for (const auto &meta : objMetas) {
        totalRequiredSize += meta.objSize;
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::PrepareGetUrmaBuffer(const GetParam &getParam, GetReqPb &req,
                                                   std::shared_ptr<UrmaManager::BufferHandle> &ubBufferHandle,
                                                   uint8_t *&ubBufferPtr, uint64_t &ubBufferSize)
{
    if (!IsUrmaEnabled() || IsShmEnable()) {
        return Status::OK();
    }
    uint64_t totalRequiredSize = 0;
    bool fallbackToTcp = false;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ResolveUBGetSize(getParam, req.tenant_id(), totalRequiredSize, fallbackToTcp),
                                     "Resolve UB get size failed");
    if (totalRequiredSize > 0) {
        PrepareUrmaBuffer(req, ubBufferHandle, ubBufferPtr, ubBufferSize, totalRequiredSize);
    } else if (fallbackToTcp) {
        LOG(WARNING) << "UB meta unavailable, fallback to TCP/IP payload: " << VectorToString(getParam.objectKeys);
    }
    return Status::OK();
}
#endif

Status ClientWorkerRemoteApi::Get(const GetParam &getParam, uint32_t &version, GetRspPb &rsp,
                                  std::vector<RpcMessage> &payloads)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_GET_LATENCY);
    const int64_t &subTimeoutMs = getParam.subTimeoutMs;
    int64_t requestTimeoutMs = std::max<int64_t>(0, requestTimeoutMs_ - getParam.ubGetObjMetaElapsedMs);
    GetReqPb req;
    RETURN_IF_NOT_OK(PreGet(getParam, subTimeoutMs, req));
    req.set_request_timeout(std::max<int64_t>(subTimeoutMs, requestTimeoutMs));
    int64_t rpcTimeout = std::max<int64_t>(subTimeoutMs, rpcTimeoutMs_);
    INJECT_POINT("ClientWorkerApi.Get.retryTimeout", [this, &requestTimeoutMs, &rpcTimeout](int timeout) {
        requestTimeoutMs = timeout;
        rpcTimeout = timeout;
        requestTimeoutMs_ = timeout;
        return Status::OK();
    });
#ifdef USE_URMA
    std::shared_ptr<UrmaManager::BufferHandle> ubBufferHandle;
    uint8_t *ubBufferPtr = nullptr;
    uint64_t ubBufferSize = 0;
    RETURN_IF_NOT_OK(PrepareGetUrmaBuffer(getParam, req, ubBufferHandle, ubBufferPtr, ubBufferSize));
#endif
    Status getStatus;
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_GET_OBJECT);
    Status status = RetryOnError(
        std::max<int32_t>(requestTimeoutMs, subTimeoutMs),
        [this, &req, &rsp, &payloads, &getStatus](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(opts.GetTimeout()));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                             "Fail to generate signature when get date.");
            LOG(INFO) << AppendSrcDstForLog(
                FormatString("Start to send rpc to get object, rpc timeout: %d", realRpcTimeout), "",
                hostPort_.ToString());
            getStatus = stub_->Get(opts, req, rsp, payloads);
            INJECT_POINT("Get.RetryOnError.retry_on_error_after_func");
            RETURN_IF_NOT_OK(getStatus);
            Status recvStatus = Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
            if (IsRpcTimeoutOrTryAgain(recvStatus)
                || (recvStatus.GetCode() == StatusCode::K_OUT_OF_MEMORY && IsAllGetFailed(rsp))) {
                return recvStatus;
            }
            return Status::OK();
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeout);
    RETURN_IF_NOT_OK(getStatus);
#ifdef USE_URMA
    RETURN_IF_NOT_OK(FillUrmaBuffer(ubBufferHandle, rsp, payloads, ubBufferPtr, ubBufferSize));
#endif
    version = workerVersion_.load(std::memory_order_relaxed);
    perfPoint.Record();
    return Status::OK();
}

Status ClientWorkerRemoteApi::InvalidateBuffer(const std::string &objectKey)
{
    InvalidateBufferReqPb req;
    req.set_object_key(objectKey);
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    InvalidateBufferRspPb rsp;
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_INVALIDATE_BUFFER);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    return stub_->InvalidateBuffer(opts, req, rsp);
}

Status ClientWorkerRemoteApi::Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isShm, bool isSeal,
                                      const std::unordered_set<std::string> &nestedKeys, uint32_t ttlSecond,
                                      int existence)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_PUBLISH_LATENCY);
    PublishReqPb req;
    RETURN_IF_NOT_OK(PreparePublishReq(bufferInfo, isSeal, nestedKeys, ttlSecond, existence, req));
    std::vector<MemView> payloads;
    UrmaFallbackTcpLimiter::Ticket fallbackTicket;
    if (!isShm && !bufferInfo->ubDataSentByMemoryCopy) {
        RETURN_IF_NOT_OK(AppendPublishPayload(urmaFallbackTcpPendingBytes_, bufferInfo, payloads, fallbackTicket));
    }
    PublishRspPb rsp;
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_PUBLISH_OBJECT);
    bool isRetry = false;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RetryOnError(
            requestTimeoutMs_,
            [this, &req, &rsp, &payloads, &isRetry](int32_t realRpcTimeout) {
                req.set_is_retry(isRetry);
                RpcOptions opts;
                opts.SetTimeout(realRpcTimeout);
                reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
                RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
                VLOG(1) << "Start to send rpc to publish object: " << req.object_key();
                Status s = stub_->Publish(opts, req, rsp, payloads);
                if (req.is_retry() && req.is_seal() && s.GetCode() == K_OC_ALREADY_SEALED) {
                    VLOG(1) << FormatString(
                        "Object(%s) retry seal and returned K_OC_ALREADY_SEALED, success is also considered.",
                        req.object_key());
                    return Status::OK();
                }
                isRetry = true;
                return s;
            },
            []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeoutMs_),
        "Send Publish request error");

    if (!isShm && !bufferInfo->ubDataSentByMemoryCopy) {
        METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_TCP_WRITE_TOTAL_BYTES, bufferInfo->dataSize);
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::MultiPublish(const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo,
                                           const PublishParam &param, MultiPublishRspPb &rsp,
                                           const std::vector<std::vector<uint64_t>> &blobSizes)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_PUBLISH_LATENCY);
    PerfPoint point(PerfKey::CLIENT_MULTI_PUBLISH_CONSTRUCT);
    MultiPublishReqPb req;
    InitMultiPublishReq(bufferInfo, param, clientId_, req);
    std::vector<MemView> payloads;
    std::vector<UrmaFallbackTcpLimiter::Ticket> fallbackTickets;
    uint64_t payloadBytes = 0;
    fallbackTickets.reserve(bufferInfo.size());
    req.mutable_object_info()->Reserve(static_cast<int>(bufferInfo.size()));
    for (size_t i = 0; i < bufferInfo.size(); ++i) {
        if (bufferInfo[i]->shmId.Empty() || IsUrmaFallbackPayload(bufferInfo[i])) {
            if (IsUrmaFallbackPayload(bufferInfo[i])) {
                fallbackTickets.emplace_back();
                RETURN_IF_NOT_OK(AppendPublishPayload(urmaFallbackTcpPendingBytes_, bufferInfo[i], payloads,
                                                      fallbackTickets.back()));
            } else {
                payloads.emplace_back(bufferInfo[i]->pointer, bufferInfo[i]->dataSize);
            }
            payloadBytes += bufferInfo[i]->dataSize;
        }
        const auto *currentBlobSizes = blobSizes.empty() ? nullptr : &blobSizes[i];
        FillMultiPublishObjectInfo(bufferInfo[i], currentBlobSizes, req);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when multi publish.");
    point.RecordAndReset(PerfKey::RPC_CLIENT_MULTI_PUBLISH_OBJECT);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RetryOnError(
            requestTimeoutMs_,
            [this, &req, &rsp, &payloads](int32_t realRpcTimeout) {
                RpcOptions opts;
                opts.SetTimeout(realRpcTimeout);
                reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
                RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
                return stub_->MultiPublish(opts, req, rsp, payloads);
            },
            []() { return Status::OK(); },
            { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
              StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_OUT_OF_MEMORY, StatusCode::K_SCALING },
            rpcTimeoutMs_),
        "Send multi publish request error");
    METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_TCP_WRITE_TOTAL_BYTES, payloadBytes);
    return Status::OK();
}

Status ClientWorkerRemoteApi::CleanUpForDecreaseShmRefAfterWorkerLost()
{
    if (decreaseRPCQ_ == nullptr) {
        return Status::OK();
    }
    // wake up element wait.
    decreaseRPCQ_->WakeUpClientProcessAndFinish();
    // need to clear RPC_Q, avoid someone is dealing.
    std::lock_guard<std::mutex> lock(mtx_);
    for (auto &flagMeta : waitRespMap_) {
        uint32_t *shmPtr = (uint32_t *)(flagMeta.second);
        *shmPtr = 0;  // clear shm data
        Lock::FutexWake(shmPtr);
    }
    decreaseRPCQ_ = nullptr;
    return Status::OK();
}

Status ClientWorkerRemoteApi::AddShmLockForClient(const struct timespec &timeoutStruct, int64_t &retryCount)
{
    auto futexRc = decreaseRPCQ_->WaitForQueueFull(timeoutStruct);
    CHECK_FAIL_RETURN_STATUS(!decreaseRPCQ_->CheckQueueDestroyed(), K_RUNTIME_ERROR, "The shm rpc is in destroy.");
    if (futexRc.IsError() && errno != ETIMEDOUT) {
        return futexRc;
    } else if (futexRc.IsError() && errno == ETIMEDOUT) {
        retryCount++;
        RETURN_STATUS(K_TRY_AGAIN, "WaitForQueueFull timeout");
    }
    futexRc = decreaseRPCQ_->SharedLock(timeoutStruct.tv_sec);
    CHECK_FAIL_RETURN_STATUS(!decreaseRPCQ_->CheckQueueDestroyed(), K_RUNTIME_ERROR, "The shm rpc is in destroy.");
    if (futexRc.IsError() && errno != ETIMEDOUT) {
        return futexRc;
    } else if (futexRc.IsError() && errno == ETIMEDOUT) {
        retryCount++;
        RETURN_STATUS(K_TRY_AGAIN, "Add SharedLock timeout");
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::CheckShmFutexResult(uint32_t *waitFlag, uint32_t waitNum, struct timespec &timeoutStruct)
{
    long result;
    FUTEX_RETRY_ON_EINTR(result, Lock::FutexWait((uint32_t *)waitFlag, waitNum, &timeoutStruct));
    return ShmCircularQueue::CheckFutexErrno(result);
}

Status ClientWorkerRemoteApi::DecreaseShmRef(const ShmKey &shmId, const std::function<Status()> &connectCheck,
                                             std::shared_timed_mutex &shutdownMtx)
{
    if (!EnableDecreaseShmRefByShmQueue()) {
        return DecreaseWorkerRef({ shmId });
    }
    std::shared_lock<std::shared_timed_mutex> shutdownLock(shutdownMtx);
    RETURN_RUNTIME_ERROR_IF_NULL(decreaseRPCQ_);
    std::string decElement;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(StringUuidToBytes(shmId, decElement), "Serialization shmId failed");
    std::string clientIdBytes;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(StringUuidToBytes(clientId_, clientIdBytes), "Serialization clientId failed");
    uint32_t waitNum = 1;
    std::string futexFlag(reinterpret_cast<const char *>(&waitNum), sizeof(waitNum));
    decElement = futexFlag + decElement + clientIdBytes;
    uint8_t *waitFlag = nullptr;

    // Interval for futex wait is 3 second.
    constexpr int intervalSec = 3;
    // calculate retry count.
    int64_t waitTimes = static_cast<int64_t>(retryTimes_) * requestTimeoutMs_ / ONE_THOUSAND / intervalSec + 1;

    struct timespec timeoutStruct = { .tv_sec = static_cast<long int>(intervalSec), .tv_nsec = 0 };
    uint32_t slotIndex;
    Status lastStatus;
    int64_t retryCount = 0;
    do {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(retryCount < waitTimes, K_RUNTIME_ERROR, lastStatus.ToString());
        std::lock_guard<std::mutex> lock(mtx_);  // protect the circular queue.
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(decreaseRPCQ_ != nullptr, K_RUNTIME_ERROR, "Shared mem is not init.");
        lastStatus = AddShmLockForClient(timeoutStruct, retryCount);
        if (lastStatus.IsError()) {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(lastStatus.GetCode() == K_TRY_AGAIN, lastStatus.GetCode(),
                                                 "Failed with add futex");
            RETURN_IF_NOT_OK(connectCheck());
            continue;
        }
        INJECT_POINT("ClientWorkerApi.DecreaseWorkerRefByShm.ClientDeadlock");
        {  // Auto release queue shared lock.
            Raii unlockAll([this]() { decreaseRPCQ_->SharedUnlock(); });
            decreaseRPCQ_->UpdateQueueMeta();
            if (!decreaseRPCQ_->GetSlotUntilSuccess(slotIndex)) {
                continue;
            }
            RETURN_IF_NOT_OK(decreaseRPCQ_->PushBySlot(slotIndex, decElement.data(), decElement.length(), &waitFlag));
            waitRespMap_[slotIndex] = waitFlag;
        }
        decreaseRPCQ_->NotifyNotEmpty();
        break;
    } while (true);

    // Time for wait rsp , it can wake up by worker rsp or disconnect.
    // requestTimeoutMs_ is milliseconds; timespec expects seconds + nanoseconds.
    // Avoid intermediate overflow by taking modulo before scaling.
    timeoutStruct.tv_sec = static_cast<time_t>(requestTimeoutMs_ / ONE_THOUSAND);
    timeoutStruct.tv_nsec =
        static_cast<long>((static_cast<int64_t>(requestTimeoutMs_ % ONE_THOUSAND) * ONE_THOUSAND) * ONE_THOUSAND);

    auto rc = CheckShmFutexResult((uint32_t *)waitFlag, waitNum, timeoutStruct);
    std::lock_guard<std::mutex> lock(mtx_);  // protect the circular queue.
    waitRespMap_.erase(slotIndex);
    auto respCheck = rc.IsOk() || rc.GetMsg().find("Time out") != std::string::npos;  // ignore timeout
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(respCheck, rc.GetCode(), rc.GetMsg());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(decreaseRPCQ_ != nullptr, K_RUNTIME_ERROR, "ShmQueue is destroyed.");
    return Status::OK();
}

Status ClientWorkerRemoteApi::DecreaseWorkerRef(const std::vector<ShmKey> &objectKeys)
{
    DecreaseReferenceRequest req;
    req.set_client_id(clientId_);
    for (const auto &objectKey : objectKeys) {
        req.add_object_keys(objectKey);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when decreaseWorkerRef data.");
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    DecreaseReferenceResponse resp;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK(stub_->DecreaseReference(opts, req, resp));
    RETURN_STATUS(static_cast<StatusCode>(resp.error().error_code()), resp.error().error_msg());
}

Status ClientWorkerRemoteApi::ReconcileShmRef(const std::unordered_set<ShmKey> &confirmedExpiredShmIds,
                                              std::vector<ShmKey> &maybeExpiredShmIds)
{
    ReconcileShmRefReqPb req;
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK(SetToken(req));
    for (const auto &shmId : confirmedExpiredShmIds) {
        req.add_confirmed_expired_shm_ids(shmId);
    }
    RpcOptions opts;
    // using rpc timeout 60s for reconcile shm ref
    opts.SetTimeout(RPC_TIMEOUT);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(RPC_TIMEOUT));
    ReconcileShmRefRspPb resp;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK(stub_->ReconcileShmRef(opts, req, resp));
    maybeExpiredShmIds.reserve(resp.maybe_expired_shm_ids_size());
    std::transform(resp.maybe_expired_shm_ids().begin(), resp.maybe_expired_shm_ids().end(),
                   std::back_inserter(maybeExpiredShmIds), [](const auto &shmId) { return ShmKey::Intern(shmId); });
    return Status::OK();
}

Status ClientWorkerRemoteApi::PipelineRH2D(H2DParam &h2DParam, GetRspPb &rsp)
{
#ifdef BUILD_PIPLN_H2D
    reqTimeoutDuration.Init(connectTimeoutMs_);
    GetReqPb req;

    H2DChunkManager chunkManager{ true /* isClient */ };
    RETURN_IF_NOT_OK(PreparePipelineRH2DReq(h2DParam, chunkManager, req));

    // send and wait
    int64_t rpcTimeout = std::max<int64_t>(h2DParam.subTimeoutMs, rpcTimeoutMs_);
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_PIPELINE_H2D);
    Status status = RetryOnError(
        std::max<int32_t>(requestTimeoutMs_, h2DParam.subTimeoutMs),
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(opts.GetTimeout()));
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                             "Fail to generate signature when sending H2D request.");
            VLOG(1) << "Start to send rpc to do H2D, rpc timeout: " << realRpcTimeout;
            std::vector<RpcMessage> payloads;
            RETURN_IF_NOT_OK(stub_->Get(opts, req, rsp, payloads));
            return Status::OK();
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeout);
    perfPoint.Record();

    return status;
#else
    (void)h2DParam;
    (void)rsp;
    return Status(K_NOT_SUPPORTED, "not build with BUILD_PIPLN_H2D");
#endif
}

Status ClientWorkerRemoteApi::GIncreaseWorkerRef(const std::vector<std::string> &firstIncIds,
                                                 std::vector<std::string> &failedObjectKeys,
                                                 const std::string &remoteClientId)
{
    GIncreaseReqPb req;
    GIncreaseRspPb rsp;
    req.set_address(clientId_);
    *req.mutable_object_keys() = { firstIncIds.begin(), firstIncIds.end() };
    req.set_client_id(clientId_);
    req.set_remote_client_id(remoteClientId);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when gincreaseWorkerRef data.");

    Status rc = RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            return stub_->GIncreaseRef(opts, req, rsp);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE },
        rpcTimeoutMs_);
    if (rc.IsError()) {
        LOG(ERROR) << "[Ref] GIncreaseWorkerRef failed with " << rc.ToString();
        failedObjectKeys = firstIncIds;
        return rc;
    }
    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(ERROR) << "[Ref] GIncreaseWorkerRef response " << LogHelper::IgnoreSensitive(rsp);
        failedObjectKeys = { rsp.failed_object_keys().begin(), rsp.failed_object_keys().end() };
        return recvRc;
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::GDecreaseWorkerRef(const std::vector<std::string> &finishDecIds,
                                                 std::vector<std::string> &failedObjectKeys,
                                                 const std::string &remoteClientId)
{
    GDecreaseReqPb req;
    GDecreaseRspPb rsp;
    req.set_address(clientId_);
    *req.mutable_object_keys() = { finishDecIds.begin(), finishDecIds.end() };
    req.set_client_id(clientId_);
    req.set_remote_client_id(remoteClientId);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when GDecreaseWorkerRef data.");
    return RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp, &finishDecIds, &failedObjectKeys](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            Status rc = stub_->GDecreaseRef(opts, req, rsp);
            if (rc.IsError()) {
                LOG(ERROR) << "[Ref] GDecreaseWorkerRef failed with " << rc.ToString();
                failedObjectKeys = failedObjectKeys.empty() ? finishDecIds : failedObjectKeys;
                return rc;
            }
            Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
            failedObjectKeys = { rsp.failed_object_keys().begin(), rsp.failed_object_keys().end() };
            if (recvRc.IsError()) {
                LOG(ERROR) << "[Ref] GDecreaseWorkerRef response failed with " << LogHelper::IgnoreSensitive(rsp);
                return recvRc;
            }
            return rc;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE },
        rpcTimeoutMs_);
}

Status ClientWorkerRemoteApi::ReleaseGRefs(const std::string &remoteClientId)
{
    ReleaseGRefsReqPb req;
    ReleaseGRefsRspPb rsp;
    req.set_client_id(clientId_);
    req.set_remote_client_id(remoteClientId);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when ReleaseGRefs data.");
    auto rc = RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            return stub_->ReleaseGRefs(opts, req, rsp);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE },
        rpcTimeoutMs_);
    if (rc.IsError()) {
        LOG(ERROR) << "ReleaseGRefs failed with " << rc.ToString();
        return rc;
    }
    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(ERROR) << "ReleaseGRefs response " << LogHelper::IgnoreSensitive(rsp);
        return recvRc;
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::Delete(const std::vector<std::string> &objectKeys,
                                     std::vector<std::string> &failedObjectKeys, bool areDeviceObjects)
{
    LOG(INFO) << FormatString("Begin to delete object, client id: %s, worker address: %s, object key: %s", clientId_,
                              hostPort_.ToString(), VectorToString(objectKeys));
    DeleteAllCopyReqPb req;
    DeleteAllCopyRspPb rsp;
    req.set_client_id(clientId_);
    for (const auto &id : objectKeys) {
        req.add_object_keys(id);
    }
    req.set_are_device_objects(areDeviceObjects);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_DEL_OBJECT);
    auto rc = RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp, &failedObjectKeys, &objectKeys](int32_t realRpcTimeout) {
            failedObjectKeys.clear();
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(opts.GetTimeout()));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            VLOG(1) << "Start to send rpc to delete object: " << VectorToString(objectKeys);
            Status status = stub_->DeleteAllCopy(opts, req, rsp);
            if (status.IsError()) {
                LOG(ERROR) << "DeleteAllCopy failed with " << status.ToString();
                failedObjectKeys = objectKeys;
                return status;
            }
            status = Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
            if (status.IsError()) {
                LOG(ERROR) << "DeleteAllCopy response " << LogHelper::IgnoreSensitive(rsp);
                failedObjectKeys = { rsp.fail_object_keys().begin(), rsp.fail_object_keys().end() };
            }
            return status;
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE },
        rpcTimeoutMs_);
    return rc;
}

Status ClientWorkerRemoteApi::QueryGlobalRefNum(
    const std::vector<std::string> &objectKeys,
    std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap)
{
    QueryGlobalRefNumReqPb req;
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    req.set_client_id(clientId_);
    QueryGlobalRefNumRspCollectionPb rsp;
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    LOG(INFO) << "[GRef] Client Send Rpc QueryGlobalRefNum to worker";
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK(stub_->QueryGlobalRefNum(opts, req, rsp));
    LOG(INFO) << "[GRef] Client Recv Rpc QueryGlobalRefNum Response From worker";
    ParseGlbRefPb(rsp, gRefMap);
    LOG(INFO) << "[GRef] Client Parsed QueryGlobalRefNum Response Successfully";
    return Status::OK();
}

Status ClientWorkerRemoteApi::PublishDeviceObject(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, size_t dataSize,
                                                  bool isShm, void *nonShmPointer)
{
    PublishDeviceObjectReqPb req;
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    req.set_client_id(clientId_);
    req.set_dev_object_key(bufferInfo->devObjKey);
    req.set_data_size(dataSize);
    req.set_shm_id(bufferInfo->shmId);
    std::vector<MemView> payloads;
    if (!isShm) {
        payloads.emplace_back(nonShmPointer, dataSize);
    }
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    PublishDeviceObjectRspPb rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when PublishDeviceObject data.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    return stub_->PublishDeviceObject(opts, req, rsp, payloads);
}

Status ClientWorkerRemoteApi::GetDeviceObject(const std::vector<std::string> &devObjKeys, uint64_t dataSize,
                                              int32_t timeoutMs, GetDeviceObjectRspPb &rsp,
                                              std::vector<RpcMessage> &payloads)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsInNonNegativeInt32(timeoutMs), K_INVALID,
        FormatString("timeoutMs %d is out of range., which should be between [%d, %d]", timeoutMs, 0, INT32_MAX));
    GetDeviceObjectReqPb req;

    req.set_client_id(clientId_);
    req.set_data_size(dataSize);
    *req.mutable_device_object_keys() = { devObjKeys.begin(), devObjKeys.end() };

    int64_t subTimeout = ClientGetRequestTimeout(timeoutMs);
    req.set_sub_timeout(subTimeout);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RpcOptions opts;
    auto rpcTimeout = std::max<int32_t>(timeoutMs, rpcTimeoutMs_);
    opts.SetTimeout(rpcTimeout);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(rpcTimeout));
    return stub_->GetDeviceObject(opts, req, rsp, payloads);
}

Status ClientWorkerRemoteApi::SubscribeReceiveEvent(int32_t deviceId, SubscribeReceiveEventRspPb &resp)
{
    int32_t timeoutMs = P2P_SUBSCRIBE_TIMEOUT_MS;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsInNonNegativeInt32(timeoutMs), K_INVALID,
        FormatString("timeoutMs %d is out of range., which should be between [%d, %d]", timeoutMs, 0, INT32_MAX));
    SubscribeReceiveEventReqPb req;
    req.set_src_client_id(clientId_);
    req.set_src_device_id(deviceId);
    RpcOptions opts;
    auto rpcTimeout = std::max<int32_t>(timeoutMs, rpcTimeoutMs_);
    INJECT_POINT("SubscribeReceiveEvent.quicklyTimeout", [&rpcTimeout](long qTimeout) {
        rpcTimeout = qTimeout;
        return Status::OK();
    });
    INJECT_POINT("SubscribeReceiveEvent.slowlyTimeout", [&rpcTimeout](long timeout) {
        rpcTimeout = timeout;
        return Status::OK();
    });
    opts.SetTimeout(rpcTimeout);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(rpcTimeout));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when SubscribeReceiveEvent data.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when create date.");
    return stub_->SubscribeReceiveEvent(opts, req, resp);
}

Status ClientWorkerRemoteApi::PutP2PMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo,
                                         const std::vector<Blob> &blobs)
{
    PutP2PMetaReqPb req;
    PutP2PMetaRspPb resp;
    auto subReq = req.add_dev_obj_meta();
    FillDevObjMeta(bufferInfo, blobs, subReq);
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    INJECT_POINT("ClientWorkerApi.PutP2PMeta.timeoutDuration", [](int time) {
        reqTimeoutDuration.Init(time);
        return Status::OK();
    });
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when FillDevObjMeta data.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when create date.");
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_PUT_P2PMETA);
    return stub_->PutP2PMeta(opts, req, resp);
}

Status ClientWorkerRemoteApi::GetP2PMeta(std::vector<std::shared_ptr<DeviceBufferInfo>> &bufferInfoList,
                                         std::vector<DeviceBlobList> &devBlobList, GetP2PMetaRspPb &resp,
                                         int64_t subTimeoutMs)
{
    INJECT_POINT("GETP2PMeta.subTimeoutMs", [&subTimeoutMs](int64_t t) {
        subTimeoutMs = t;
        return Status::OK();
    });
    int64_t timeoutMs = P2P_TIMEOUT_MS;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsInNonNegativeInt32(timeoutMs), K_INVALID,
        FormatString("timeoutMs %d is out of range., which should be between [%d, %d]", timeoutMs, 0, INT32_MAX));
    GetP2PMetaReqPb req;
    if (bufferInfoList.size() != devBlobList.size()) {
        LOG(ERROR) << "buffer info list size not matching data info list size";
        return Status(K_INVALID, "buffer info list size not matching data info list size");
    }
    for (size_t i = 0; i < bufferInfoList.size(); i++) {
        auto subReq = req.add_dev_obj_meta();
        FillDevObjMeta(bufferInfoList[i], devBlobList[i].blobs, subReq);
    }
    req.set_sub_timeout(subTimeoutMs);
    RpcOptions opts;
    auto rpcTimeout = std::max<int64_t>(timeoutMs, rpcTimeoutMs_);
    opts.SetTimeout(rpcTimeout);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(rpcTimeout));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when GetP2PMeta data.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when create date.");
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_GET_P2PMETA);
    return stub_->GetP2PMeta(opts, req, resp);
}

Status ClientWorkerRemoteApi::SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when SendRootInfo data.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when creating data");
    PerfPoint perfPoint(PerfKey::RPC_HETERO_CLIENT_SEND_ROOT_INFO);
    return stub_->SendRootInfo(opts, req, resp);
}

Status ClientWorkerRemoteApi::RecvRootInfo(RecvRootInfoReqPb &req, RecvRootInfoRspPb &resp)
{
    int64_t timeoutMs = P2P_TIMEOUT_MS;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::IsInNonNegativeInt32(timeoutMs), K_INVALID,
        FormatString("timeoutMs %d is out of range, which should be between [%d, %d]", timeoutMs, 0, INT32_MAX));
    RpcOptions opts;
    auto rpcTimeout = std::max<int64_t>(timeoutMs, rpcTimeoutMs_);
    opts.SetTimeout(rpcTimeout);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(rpcTimeout));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when RecvRootInfo data.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when creating data");
    PerfPoint perfPoint(PerfKey::RPC_HETERO_CLIENT_RECV_ROOT_INFO);
    return stub_->RecvRootInfo(opts, req, resp);
}

Status ClientWorkerRemoteApi::AckRecvFinish(AckRecvFinishReqPb &req)
{
    AckRecvFinishRspPb resp;
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when AckRecvFinish.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when creating data");
    PerfPoint perfPoint(PerfKey::RPC_HETERO_CLIENT_ACK_RECV_FINISH);
    return stub_->AckRecvFinish(opts, req, resp);
}

Status ClientWorkerRemoteApi::GetBlobsInfo(const std::string &devObjKey, int32_t timeoutMs, std::vector<Blob> &blobs)
{
    RpcOptions opts;
    auto rpcTimeout = std::max<int64_t>(timeoutMs, rpcTimeoutMs_);
    opts.SetTimeout(rpcTimeout);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(rpcTimeout));

    GetDataInfoReqPb req;
    int64_t subTimeout = ClientGetRequestTimeout(timeoutMs);
    req.set_object_key(devObjKey);
    req.set_sub_timeout(subTimeout);
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when GetDataInfo.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when create date.");
    GetDataInfoRspPb resp;
    PerfPoint perfPoint(PerfKey::RPC_HETERO_CLIENT_GET_DATA_INFO);
    RETURN_IF_NOT_OK(stub_->GetDataInfo(opts, req, resp));
    // Obtains the blobs from resp
    std::vector<DataInfoPb> dataInfoPbs = { resp.data_infos().begin(), resp.data_infos().end() };
    blobs.reserve(dataInfoPbs.size());
    for (const auto &dataInfoPb : dataInfoPbs) {
        blobs.emplace_back(Blob{ nullptr, dataInfoPb.count() });
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::RemoveP2PLocation(const std::string &objectKey, int32_t deviceId)
{
    RemoveP2PLocationReqPb req;
    req.set_object_key(objectKey);
    req.set_client_id(clientId_);
    req.set_device_id(deviceId);
    RemoveP2PLocationRspPb resp;
    RpcOptions opts;
    opts.SetTimeout(requestTimeoutMs_);
    reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when RemoveP2PLocation.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when creating data.");
    PerfPoint perfPoint(PerfKey::RPC_HETERO_CLIENT_LOCAL_DELETE);
    return stub_->RemoveP2PLocation(opts, req, resp);
}

Status ClientWorkerRemoteApi::GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                                             std::vector<ObjMetaInfo> &objMetas)
{
    GetObjMetaInfoReqPb req;
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    req.set_tenantid(tenantId);
    GetObjMetaInfoRspPb rsp;
    auto status = RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp, tenantId](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            VLOG(1) << "Start to send rpc to get obj meta: " << VectorToString(req.object_keys()) << " of tenant "
                    << tenantId;
            return stub_->GetObjMetaInfo(opts, req, rsp);
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeoutMs_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(status, "Send GetObjMetaInfo failed.");
    // fill outpara
    LOG(INFO) << "Finish GetObjMetaInfo success.";
    objMetas.reserve(objectKeys.size());
    for (auto &i : rsp.objs_meta_info()) {
        objMetas.emplace_back(
            ObjMetaInfo{ i.obj_size(), std::vector<std::string>{ i.location_ids().begin(), i.location_ids().end() } });
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::QuerySize(const std::vector<std::string> &objectKeys, QuerySizeRspPb &rsp)
{
    QuerySizeReqPb req;
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when QuerySize.");
    auto status = RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            VLOG(1) << "Start to send rpc to get obj meta: " << VectorToString(req.object_keys());
            RETURN_IF_NOT_OK(stub_->QuerySize(opts, req, rsp));
            Status recvStatus = Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
            if (IsRpcTimeoutOrTryAgain(recvStatus)) {
                return recvStatus;
            }
            return Status::OK();
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeoutMs_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(status, "Send QuerySize failed.");
    LOG(INFO) << "Finish QuerySize success.";
    return Status::OK();
}

Status ClientWorkerRemoteApi::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists,
                                    const bool queryL2Cache, const bool isLocal)
{
    ExistReqPb req;
    *req.mutable_object_keys() = { keys.begin(), keys.end() };
    req.set_client_id(clientId_);
    req.set_query_l2cache(queryL2Cache);
    req.set_is_local(isLocal);
    INJECT_POINT("Exist.QueryLocalMem", [&req]() {
        req.set_query_l2cache(false);
        req.set_is_local(true);
        return Status::OK();
    });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token to ExistReqPb.");
    ExistRspPb rsp;
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_EXIST);
    auto status = RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            VLOG(1) << "Start to send rpc to check existence";
            return stub_->Exist(opts, req, rsp);
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeoutMs_);
    if (status.IsError()) {
        LOG(ERROR) << "Exist resp error, msg:" << status.ToString();
        return status;
    }
    if (keys.size() != static_cast<size_t>(rsp.exists().size())) {
        LOG(ERROR) << "Exist response size " << rsp.exists().size() << " is not equal to key size " << keys.size();
        exists.assign(keys.size(), false);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Exist response size mismatch.");
    }
    exists.assign(rsp.exists().begin(), rsp.exists().end());
    LOG(INFO) << "Check existence success.";
    return Status::OK();
}

Status ClientWorkerRemoteApi::Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds,
                                     std::vector<std::string> &failedKeys)
{
    ExpireReqPb req;
    *req.mutable_object_keys() = { keys.begin(), keys.end() };
    req.set_client_id(clientId_);
    req.set_ttl_second(ttlSeconds);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token to ExpireReqPb.");
    ExpireRspPb rsp;
    auto status = RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            VLOG(1) << "Start to send rpc to set expire ttl time";
            return stub_->Expire(opts, req, rsp);
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeoutMs_);
    if (status.IsError()) {
        LOG(ERROR) << "Expire resp error, msg:" << status.ToString();
        return status;
    }
    failedKeys.assign(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
    if (keys.size() == static_cast<size_t>(failedKeys.size())) {
        return Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    }
    LOG(INFO) << FormatString("Expire objects like %s with ttl time %d success.", keys[0], ttlSeconds);
    return Status::OK();
}

Status ClientWorkerRemoteApi::GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey,
                                          GetMetaInfoRspPb &rsp)
{
    GetMetaInfoReqPb req;
    req.set_client_id(clientId_);
    req.set_is_dev_key(isDevKey);
    *req.mutable_object_keys() = { keys.begin(), keys.end() };
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token to GetMetaInfoReqPb.");
    auto status = RetryOnError(
        requestTimeoutMs_,
        [this, &req, &rsp](int32_t realRpcTimeout) {
            RpcOptions opts;
            opts.SetTimeout(realRpcTimeout);
            reqTimeoutDuration.Init(ClientGetRequestTimeout(realRpcTimeout));
            RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
            return stub_->GetMetaInfo(opts, req, rsp);
        },
        []() { return Status::OK(); }, RETRY_ERROR_CODE, rpcTimeoutMs_);
    if (status.IsError()) {
        LOG(ERROR) << "GetMetaInfo resp error, msg:" << status.ToString();
        return status;
    }
    return Status::OK();
}

Status ClientWorkerRemoteApi::PrepairForDecreaseShmRef(
    std::function<Status(const std::string &, const std::shared_ptr<ShmUnitInfo> &)> mmapFunc)
{
    if (!EnableDecreaseShmRefByShmQueue()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(mmapFunc("", decShmUnit_));
    return InitDecreaseQueue();
}
}  // namespace object_cache
}  // namespace datasystem
