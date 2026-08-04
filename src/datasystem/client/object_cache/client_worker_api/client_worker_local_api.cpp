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
 * Description: Defines the worker client local class to communicate with the worker service.
 */
#include "datasystem/client/object_cache/client_worker_api/client_worker_local_api.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_common_api.h"


using datasystem::client::ClientWorkerRemoteCommonApi;

namespace datasystem {
namespace object_cache {

namespace {
struct MultiPublishRequestContext {
    const std::string &clientId;
    const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo;
    const PublishParam &param;
    const std::vector<const DeviceBlobList *> &deviceBlobRefs;
    MultiPublishReqPb &req;
    std::vector<MemView> &mvs;
    uint64_t &payloadBytes;
    uint64_t &shmBytes;
};

Status ValidateDeviceBlobRefs(const std::vector<const DeviceBlobList *> &deviceBlobRefs, size_t bufferCount)
{
    if (deviceBlobRefs.empty()) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(deviceBlobRefs.size() == bufferCount, K_INVALID,
                             FormatString("deviceBlobRefs size %zu does not match bufferInfo size %zu",
                                          deviceBlobRefs.size(), bufferCount));
    for (size_t i = 0; i < deviceBlobRefs.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(deviceBlobRefs[i] != nullptr, K_INVALID,
                                 FormatString("deviceBlobRefs[%zu] is null", i));
        CHECK_FAIL_RETURN_STATUS(
            deviceBlobRefs[i]->blobs.size() <= static_cast<size_t>(std::numeric_limits<int>::max()), K_INVALID,
            FormatString("blob count %zu exceeds int limit, index %zu", deviceBlobRefs[i]->blobs.size(), i));
    }
    return Status::OK();
}

Status AppendMultiPublishObjects(MultiPublishRequestContext &context)
{
    for (size_t i = 0; i < context.bufferInfo.size(); ++i) {
        if (context.bufferInfo[i]->shmId.Empty()) {
            context.mvs.emplace_back(context.bufferInfo[i]->pointer, context.bufferInfo[i]->dataSize);
            context.payloadBytes += context.bufferInfo[i]->dataSize;
        } else {
            context.shmBytes += context.bufferInfo[i]->dataSize;
        }
        MultiPublishReqPb::ObjectInfoPb objectInfoPb;
        if (!context.deviceBlobRefs.empty()) {
            const auto &blobs = context.deviceBlobRefs[i]->blobs;
            auto mutableBlobSizes = objectInfoPb.mutable_blob_sizes();
            mutableBlobSizes->Reserve(static_cast<int>(blobs.size()));
            for (const auto &blob : blobs) {
                mutableBlobSizes->Add(blob.size);
            }
        }
        objectInfoPb.set_object_key(context.bufferInfo[i]->objectKey);
        objectInfoPb.set_data_size(context.bufferInfo[i]->dataSize);
        objectInfoPb.set_shm_id(context.bufferInfo[i]->shmId);
        context.req.mutable_object_info()->Add(std::move(objectInfoPb));
    }
    return Status::OK();
}

void SetMultiPublishRequestOptions(MultiPublishRequestContext &context)
{
    context.req.set_client_id(context.clientId);
    context.req.set_ttl_second(context.param.ttlSecond);
    context.req.set_write_mode(static_cast<uint32_t>(context.bufferInfo[0]->objectMode.GetWriteMode()));
    context.req.set_consistency_type(static_cast<uint32_t>(context.bufferInfo[0]->objectMode.GetConsistencyType()));
    context.req.set_cache_type(static_cast<uint32_t>(context.bufferInfo[0]->objectMode.GetCacheType()));
    context.req.set_existence(static_cast<::datasystem::ExistenceOptPb>(context.param.existence));
    context.req.set_is_replica(context.param.isReplica);
    context.req.set_auto_release_memory_ref(!context.bufferInfo[0]->shmId.Empty());
    context.req.set_return_local_published_indexes(context.param.returnLocalPublishedIndexes);
}

Status BuildMultiPublishRequest(MultiPublishRequestContext &context)
{
    SetMultiPublishRequestOptions(context);
    CHECK_FAIL_RETURN_STATUS(
        context.bufferInfo.size() <= static_cast<size_t>(std::numeric_limits<int>::max()), K_INVALID,
        FormatString("bufferInfo count %zu exceeds protobuf int limit", context.bufferInfo.size()));
    RETURN_IF_NOT_OK(ValidateDeviceBlobRefs(context.deviceBlobRefs, context.bufferInfo.size()));
    context.req.mutable_object_info()->Reserve(static_cast<int>(context.bufferInfo.size()));
    std::optional<PerfPoint> serializePoint;
    if (!context.deviceBlobRefs.empty()) {
        serializePoint.emplace(PerfKey::CLIENT_D2H_SERIALIZE_BLOB_SIZES);
    }
    RETURN_IF_NOT_OK(AppendMultiPublishObjects(context));
    if (serializePoint.has_value()) {
        serializePoint->Record();
    }
    return Status::OK();
}
}  // namespace

ClientWorkerLocalApi::ClientWorkerLocalApi(HostPort hostPort,
                                           std::shared_ptr<::datasystem::client::EmbeddedClientWorkerApi> api,
                                           void *worker, HeartbeatType heartbeatType, Signature *signature,
                                           bool enableCrossNodeConnection, std::string deviceId)
    : client::IClientWorkerCommonApi(hostPort, heartbeatType, enableCrossNodeConnection, signature),
      ClientWorkerBaseApi(hostPort, heartbeatType, enableCrossNodeConnection, signature),
      ClientWorkerLocalCommonApi(hostPort, std::move(api), worker, heartbeatType, enableCrossNodeConnection, signature,
                                 std::move(deviceId))
{
}

Status ClientWorkerLocalApi::Init(int32_t requestTimeoutMs, int32_t connectTimeoutMs, uint64_t fastTransportSize,
                                  int32_t initAttemptTimeoutMs)
{
    connectTimeoutMs_ = connectTimeoutMs;
    RETURN_IF_NOT_OK(
        ClientWorkerLocalCommonApi::Init(requestTimeoutMs, connectTimeoutMs, fastTransportSize, initAttemptTimeoutMs));
    workerOCService_ = api_->GetWorkerOCService();
    return Status::OK();
}

Status ClientWorkerLocalApi::Create(const std::string &objectKey, int64_t dataSize, uint32_t &version,
                                    uint64_t &metadataSize, std::shared_ptr<ShmUnitInfo> &shmBuf,
                                    std::shared_ptr<UrmaRemoteAddrPb> &urmaDataInfo, const CacheType &cacheType)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_CREATE_LATENCY);
    (void)urmaDataInfo;
    VLOG(1) << FormatString("Begin to create object, client id: %s, worker address: %s, object key: %s", clientId_,
                            hostPort_.ToString(), objectKey);
    CHECK_FAIL_RETURN_STATUS(dataSize > 0, StatusCode::K_INVALID,
                             FormatString("data size:%lld must be more than 0!", dataSize));
    int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    GetRequestContext()->reqTimeoutDuration.InitUs(remainingUs);
    CreateReqPb req;
    req.set_object_key(objectKey);
    req.set_client_id(clientId_);
    req.set_data_size(dataSize);
    req.set_cache_type(static_cast<uint32_t>(cacheType));
    req.set_request_timeout(TimeoutDuration::CeilUsToMs(remainingUs));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when create date.");
    CreateRspPb rsp;
    RETURN_IF_NOT_OK(api_->WorkerOCCreate(workerOCService_, req, rsp));
    shmBuf->fd = rsp.store_fd();
    shmBuf->mmapSize = rsp.mmap_size();
    shmBuf->offset = static_cast<ptrdiff_t>(rsp.offset());
    shmBuf->id = ShmKey::Intern(rsp.shm_id());
    metadataSize = rsp.metadata_size();
    version = workerVersion_.load(std::memory_order_relaxed);
    return Status::OK();
}

Status ClientWorkerLocalApi::Publish(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isShm, bool isSeal,
                                     const std::unordered_set<std::string> &nestedKeys, uint32_t ttlSecond,
                                     int existence)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_PUBLISH_LATENCY);
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    PublishReqPb req;
    RETURN_IF_NOT_OK(PreparePublishReq(bufferInfo, isSeal, nestedKeys, ttlSecond, existence, req));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    std::vector<RpcMessage> rms;
    if (!isShm) {
        std::vector<MemView> mvs;
        mvs.emplace_back(bufferInfo->pointer, bufferInfo->dataSize);
        RETURN_IF_NOT_OK(MemView2RpcMessage(mvs, rms));
    }
    PublishRspPb rsp;
    RETURN_IF_NOT_OK(api_->WorkerOCPublish(workerOCService_, req, rsp, std::move(rms)));
    if (isShm) {
        METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_SHM_WRITE_TOTAL_BYTES, bufferInfo->dataSize);
    } else {
        METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_LOCAL_WRITE_TOTAL_BYTES, bufferInfo->dataSize);
    }
    return Status::OK();
}

Status ClientWorkerLocalApi::MultiPublish(const std::vector<std::shared_ptr<ObjectBufferInfo>> &bufferInfo,
                                          const PublishParam &param, MultiPublishRspPb &rsp,
                                          const std::vector<const DeviceBlobList *> &deviceBlobRefs)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_PUBLISH_LATENCY);
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    MultiPublishReqPb req;
    std::vector<MemView> mvs;
    uint64_t payloadBytes = 0;
    uint64_t shmBytes = 0;
    MultiPublishRequestContext context{ clientId_, bufferInfo, param, deviceBlobRefs, req, mvs, payloadBytes,
                                        shmBytes };
    RETURN_IF_NOT_OK(BuildMultiPublishRequest(context));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when multi publish.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    std::vector<RpcMessage> rms;
    RETURN_IF_NOT_OK(MemView2RpcMessage(mvs, rms));
    RETURN_IF_NOT_OK(api_->WorkerOCMultiPublish(workerOCService_, req, rsp, std::move(rms)));
    METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_LOCAL_WRITE_TOTAL_BYTES, payloadBytes);
    METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_SHM_WRITE_TOTAL_BYTES, shmBytes);
    return Status::OK();
}

Status ClientWorkerLocalApi::DecreaseWorkerRef(const std::vector<ShmKey> &objectKeys)
{
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    DecreaseReferenceRequest req;
    req.set_client_id(clientId_);
    for (const auto &objectKey : objectKeys) {
        req.add_object_keys(objectKey);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when decreaseWorkerRef data.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    DecreaseReferenceResponse resp;
    RETURN_IF_NOT_OK(api_->WorkerOCDecreaseReference(workerOCService_, req, resp));
    RETURN_STATUS(static_cast<StatusCode>(resp.error().error_code()), resp.error().error_msg());
}

Status ClientWorkerLocalApi::PipelineRH2D(PiplnRh2dParam &piplnRh2dParam, GetRspPb &rsp)
{
    (void)piplnRh2dParam;
    (void)rsp;
    return Status(K_NOT_SUPPORTED, "not support local pipeline rh2d now");
}

Status ClientWorkerLocalApi::Get(const GetParam &getParam, uint32_t &version, GetRspPb &rsp,
                                 std::vector<RpcMessage> &payloads)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_GET_LATENCY);
    const int64_t &subTimeoutMs = getParam.subTimeoutMs;
    GetReqPb req;
    RETURN_IF_NOT_OK(PreGet(getParam, subTimeoutMs, req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req), "Fail to generate signature when get date.");
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    std::promise<std::pair<GetRspPb, Status>> promise;
    auto future = promise.get_future();
    std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> serverApi =
        std::make_shared<LocalServerUnaryWriterReader<GetRspPb, GetReqPb>>(req, std::move(promise));
    RETURN_IF_NOT_OK(api_->WorkerOCGet(workerOCService_, serverApi));
    std::pair<GetRspPb, Status> result;
    int64_t waitMs = TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs());
    if (future.wait_for(std::chrono::milliseconds(waitMs)) == std::future_status::ready) {
        try {
            result = future.get();
            serverApi->ReceivePayload(payloads);
            rsp = std::move(result.first);
        } catch (const std::exception &e) {
            result.second = { K_RUNTIME_ERROR, FormatString("Exception when calling future.get(): %s ", e.what()) };
        }
    } else {
        return Status(K_RPC_DEADLINE_EXCEEDED,
                      FormatString("Local get deadline exceeded, remaining %ld us",
                                   ApiDeadline::Instance().ApiRemainingUs()));
    }
    RETURN_IF_NOT_OK(result.second);
    version = workerVersion_.load(std::memory_order_relaxed);
    return Status::OK();
}

Status ClientWorkerLocalApi::InvalidateBuffer(const std::string &objectKey)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    InvalidateBufferReqPb req;
    req.set_object_key(objectKey);
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    InvalidateBufferRspPb rsp;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    return api_->WorkerOCInvalidateBuffer(workerOCService_, req, rsp);
}

Status ClientWorkerLocalApi::GIncreaseWorkerRef(const std::vector<std::string> &firstIncIds,
                                                std::vector<std::string> &failedObjectKeys,
                                                const std::string &remoteClientId)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    GIncreaseReqPb req;
    GIncreaseRspPb rsp;
    req.set_address(clientId_);
    *req.mutable_object_keys() = { firstIncIds.begin(), firstIncIds.end() };
    req.set_client_id(clientId_);
    req.set_remote_client_id(remoteClientId);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when gincreaseWorkerRef data.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    Status rc = api_->WorkerOCGIncreaseRef(workerOCService_, req, rsp);
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

Status ClientWorkerLocalApi::ReleaseGRefs(const std::string &remoteClientId)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    ReleaseGRefsReqPb req;
    ReleaseGRefsRspPb rsp;
    req.set_client_id(clientId_);
    req.set_remote_client_id(remoteClientId);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when ReleaseGRefs data.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(api_->WorkerOCReleaseGRefs(workerOCService_, req, rsp), "ReleaseGRefs failed");
    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(ERROR) << "ReleaseGRefs response " << LogHelper::IgnoreSensitive(rsp);
        return recvRc;
    }
    return Status::OK();
}

Status ClientWorkerLocalApi::GDecreaseWorkerRef(const std::vector<std::string> &finishDecIds,
                                                std::vector<std::string> &failedObjectKeys,
                                                const std::string &remoteClientId)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    GDecreaseReqPb req;
    GDecreaseRspPb rsp;
    req.set_address(clientId_);
    *req.mutable_object_keys() = { finishDecIds.begin(), finishDecIds.end() };
    req.set_client_id(clientId_);
    req.set_remote_client_id(remoteClientId);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when GDecreaseWorkerRef data.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    Status rc = api_->WorkerOCGDecreaseRef(workerOCService_, req, rsp);
    if (rc.IsError()) {
        LOG(ERROR) << "[Ref] GDecreaseWorkerRef failed with " << rc.ToString();
        failedObjectKeys = finishDecIds;
        return rc;
    }
    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    failedObjectKeys = { rsp.failed_object_keys().begin(), rsp.failed_object_keys().end() };
    if (recvRc.IsError()) {
        LOG(ERROR) << "[Ref] GDecreaseWorkerRef response failed with " << LogHelper::IgnoreSensitive(rsp);
        return recvRc;
    }
    return Status::OK();
}

Status ClientWorkerLocalApi::Delete(const std::vector<std::string> &objectKeys,
                                    std::vector<std::string> &failedObjectKeys, bool areDeviceObjects)
{
    LOG(INFO) << FormatString("Begin to delete object, client id: %s, worker address: %s, object key: %s", clientId_,
                              hostPort_.ToString(), VectorToString(objectKeys));
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    DeleteAllCopyReqPb req;
    DeleteAllCopyRspPb rsp;
    req.set_client_id(clientId_);
    for (const auto &id : objectKeys) {
        req.add_object_keys(id);
    }
    req.set_are_device_objects(areDeviceObjects);
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    Status rc = api_->WorkerOCDeleteAllCopy(workerOCService_, req, rsp);
    if (rc.IsError()) {
        LOG(ERROR) << "DeleteAllCopy failed with " << rc.ToString();
        failedObjectKeys = objectKeys;
        return rc;
    }
    rc = Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (rc.IsError()) {
        LOG(ERROR) << "DeleteAllCopy response " << LogHelper::IgnoreSensitive(rsp);
        failedObjectKeys = { rsp.fail_object_keys().begin(), rsp.fail_object_keys().end() };
    }
    return rc;
}

Status ClientWorkerLocalApi::QueryGlobalRefNum(
    const std::vector<std::string> &objectKeys,
    std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap)
{
    QueryGlobalRefNumReqPb req;
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    QueryGlobalRefNumRspCollectionPb rsp;
    LOG(INFO) << "[GRef] Client Send Rpc QueryGlobalRefNum to worker";
    RETURN_IF_NOT_OK(api_->WorkerOCQueryGlobalRefNum(workerOCService_, req, rsp));
    LOG(INFO) << "[GRef] Client Recv Rpc QueryGlobalRefNum Response From worker";
    ParseGlbRefPb(rsp, gRefMap);
    LOG(INFO) << "[GRef] Client Parsed QueryGlobalRefNum Response Successfully";
    return Status::OK();
}

Status ClientWorkerLocalApi::PublishDeviceObject(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, size_t dataSize,
                                                 bool isShm, void *nonShmPointer)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    PublishDeviceObjectReqPb req;
    RETURN_IF_NOT_OK(SetTokenAndTenantId(req));
    req.set_client_id(clientId_);
    req.set_dev_object_key(bufferInfo->devObjKey);
    req.set_data_size(dataSize);
    req.set_shm_id(bufferInfo->shmId);
    std::vector<RpcMessage> rms;
    if (!isShm) {
        std::vector<MemView> mvs;
        mvs.emplace_back(nonShmPointer, dataSize);
        RETURN_IF_NOT_OK(MemView2RpcMessage(mvs, rms));
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when PublishDeviceObject data.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    PublishDeviceObjectRspPb rsp;
    return api_->WorkerOCPublishDeviceObject(workerOCService_, req, rsp, std::move(rms));
}

Status ClientWorkerLocalApi::GetDeviceObject(const std::vector<std::string> &devObjKeys, uint64_t dataSize,
                                             int32_t timeoutMs, GetDeviceObjectRspPb &rsp,
                                             std::vector<RpcMessage> &payloads)
{
    (void)devObjKeys;
    (void)dataSize;
    (void)timeoutMs;
    (void)rsp;
    (void)payloads;
    return Status(K_RUNTIME_ERROR, "Not support GetDeviceObject in embedded scenario");
}

Status ClientWorkerLocalApi::SubscribeReceiveEvent(int32_t deviceId, SubscribeReceiveEventRspPb &resp)
{
    (void)deviceId;
    (void)resp;
    return Status(K_RUNTIME_ERROR, "Not support SubscribeReceiveEvent in embedded scenario");
}

Status ClientWorkerLocalApi::PutP2PMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo,
                                        const std::vector<Blob> &blobs)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    PutP2PMetaReqPb req;
    PutP2PMetaRspPb resp;
    auto subReq = req.add_dev_obj_meta();
    FillDevObjMeta(bufferInfo, blobs, subReq);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when FillDevObjMeta data.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when create date.");
    return api_->WorkerOCPutP2PMeta(workerOCService_, req, resp);
}

Status ClientWorkerLocalApi::GetP2PMeta(std::vector<std::shared_ptr<DeviceBufferInfo>> &bufferInfoList,
                                        std::vector<DeviceBlobList> &devBlobList, GetP2PMetaRspPb &resp,
                                        int64_t subTimeoutMs)
{
    (void)bufferInfoList;
    (void)devBlobList;
    (void)resp;
    (void)subTimeoutMs;
    return Status(K_RUNTIME_ERROR, "Not support GetP2PMeta in embedded scenario");
}

Status ClientWorkerLocalApi::SendRootInfo(SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when SendRootInfo data.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when creating data");
    return api_->WorkerOCSendRootInfo(workerOCService_, req, resp);
}

Status ClientWorkerLocalApi::RecvRootInfo(RecvRootInfoReqPb &req, RecvRootInfoRspPb &resp)
{
    (void)req;
    (void)resp;
    return Status(K_RUNTIME_ERROR, "Not support RecvRootInfo in embedded scenario");
}

Status ClientWorkerLocalApi::GetBlobsInfo(const std::string &devObjKey, int32_t timeoutMs, std::vector<Blob> &blobs)
{
    (void)devObjKey;
    (void)timeoutMs;
    (void)blobs;
    return Status(K_RUNTIME_ERROR, "Not support GetBlobsInfo in embedded scenario");
}

Status ClientWorkerLocalApi::AckRecvFinish(AckRecvFinishReqPb &req)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    AckRecvFinishRspPb resp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when AckRecvFinish.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when creating data");
    return api_->WorkerOCAckRecvFinish(workerOCService_, req, resp);
}

Status ClientWorkerLocalApi::RemoveP2PLocation(const std::string &objectKey, int32_t deviceId)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    RemoveP2PLocationReqPb req;
    req.set_object_key(objectKey);
    req.set_client_id(clientId_);
    req.set_device_id(deviceId);
    RemoveP2PLocationRspPb resp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when RemoveP2PLocation.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when creating data.");
    return api_->WorkerOCRemoveP2PLocation(workerOCService_, req, resp);
}

Status ClientWorkerLocalApi::GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                                            std::vector<ObjMetaInfo> &objMetas)
{
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    GetObjMetaInfoReqPb req;
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    req.set_tenantid(tenantId);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    GetObjMetaInfoRspPb rsp;
    RETURN_IF_NOT_OK(api_->WorkerOCGetObjMetaInfo(workerOCService_, req, rsp));
    // fill outpara
    LOG(INFO) << "Finish GetObjMetaInfo success.";
    objMetas.reserve(objectKeys.size());
    for (auto &i : rsp.objs_meta_info()) {
        objMetas.emplace_back(
            ObjMetaInfo{ i.obj_size(), std::vector<std::string>{ i.location_ids().begin(), i.location_ids().end() } });
    }
    return Status::OK();
}

Status ClientWorkerLocalApi::MultiCreate(bool skipCheckExistence, std::vector<MultiCreateParam> &createParams,
                                         uint32_t &version, std::vector<bool> &exists, bool &useShmTransfer)
{
    METRIC_TIMER(metrics::KvMetricId::CLIENT_RPC_CREATE_LATENCY);
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    MultiCreateReqPb req;
    req.set_skip_check_existence(skipCheckExistence);
    req.set_client_id(clientId_);
    int sz = static_cast<int>(createParams.size());
    req.mutable_object_key()->Reserve(sz);
    req.mutable_data_size()->Reserve(sz);
    req.set_request_timeout(TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs()));
    for (auto &param : createParams) {
        req.add_object_key(param.objectKey);
        req.add_data_size(param.dataSize);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when create data.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(signature_->GenerateSignature(req),
                                     "Fail to generate signature when MultiCreate.");
    MultiCreateRspPb rsp;
    RETURN_IF_NOT_OK(api_->WorkerOCMultiCreate(workerOCService_, req, rsp));
    CHECK_FAIL_RETURN_STATUS(
        createParams.size() == static_cast<size_t>(rsp.results().size()), K_INVALID,
        FormatString("The length of objectKeyList (%zu) and dataSizeList (%zu) should be the same.",
                     createParams.size(), rsp.results().size()));
    if (!skipCheckExistence) {
        CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(rsp.exists_size()) == createParams.size(), K_INVALID,
                                 "The size of rspExists is not consistent with createParams");
        exists.assign(rsp.exists().begin(), rsp.exists().end());
    }
    PerfPoint point(PerfKey::CLIENT_MULTI_CREATE_FILL_PARAM);
    PostMultiCreate(skipCheckExistence, rsp, createParams, useShmTransfer, point, version, exists);
    return Status::OK();
}

Status ClientWorkerLocalApi::ReconcileShmRef(const std::unordered_set<ShmKey> &confirmedExpiredShmIds,
                                             std::vector<ShmKey> &maybeExpiredShmIds)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(RPC_TIMEOUT));
    ReconcileShmRefReqPb req;
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetToken(*req.mutable_token()), "Fail to set token when ReconcileShmRef");
    for (const auto &shmId : confirmedExpiredShmIds) {
        req.add_confirmed_expired_shm_ids(shmId);
    }
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    ReconcileShmRefRspPb rsp;
    RETURN_IF_NOT_OK(api_->WorkerOCReconcileShmRef(workerOCService_, req, rsp));
    maybeExpiredShmIds.reserve(rsp.maybe_expired_shm_ids_size());
    std::transform(rsp.maybe_expired_shm_ids().begin(), rsp.maybe_expired_shm_ids().end(),
                   std::back_inserter(maybeExpiredShmIds), [](const auto &shmId) { return ShmKey::Intern(shmId); });
    return Status::OK();
}

Status ClientWorkerLocalApi::QuerySize(const std::vector<std::string> &objectKeys, QuerySizeRspPb &rsp)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    QuerySizeReqPb req;
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when QuerySize.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    (void)rsp;
    RETURN_IF_NOT_OK(api_->WorkerOCQuerySize(workerOCService_, req, rsp));
    LOG(INFO) << "Finish QuerySize success.";
    return Status::OK();
}

Status ClientWorkerLocalApi::HealthCheck(ServerState &state)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(MIN_HEARTBEAT_INTERVAL_MS));
    HealthCheckRequestPb req;
    HealthCheckReplyPb rsp;
    req.set_client_id(clientId_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token when create data.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    state = ServerState::NORMAL;
    return api_->WorkerOCHealthCheck(workerOCService_, req, rsp);
}

Status ClientWorkerLocalApi::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists,
                                   const bool queryL2Cache, const bool isLocal)
{
    PerfPoint point(PerfKey::CLIENT_EXIST_LOCAL);
    GetRequestContext()->reqTimeoutDuration.InitUs(ApiDeadline::Instance().ApiRemainingUs());
    ExistReqPb req;
    *req.mutable_object_keys() = { keys.begin(), keys.end() };
    req.set_client_id(clientId_);
    req.set_query_l2cache(queryL2Cache);
    req.set_is_local(isLocal);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token to ExistReqPb.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    ExistRspPb rsp;
    RETURN_IF_NOT_OK(api_->WorkerOCExist(workerOCService_, req, rsp));
    if (keys.size() != static_cast<size_t>(rsp.exists().size())) {
        LOG(ERROR) << "Exist response size " << rsp.exists().size() << " is not equal to key size " << keys.size();
        exists.assign(keys.size(), false);
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Exist response size mismatch.");
    }
    exists.assign(rsp.exists().begin(), rsp.exists().end());
    LOG(INFO) << "Check existence success.";
    return Status::OK();
}

Status ClientWorkerLocalApi::Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds,
                                    std::vector<std::string> &failedKeys)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    ExpireReqPb req;
    *req.mutable_object_keys() = { keys.begin(), keys.end() };
    req.set_client_id(clientId_);
    req.set_ttl_second(ttlSeconds);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token to ExpireReqPb.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    ExpireRspPb rsp;
    RETURN_IF_NOT_OK(api_->WorkerOCExpire(workerOCService_, req, rsp));
    failedKeys.assign(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
    if (keys.size() == static_cast<size_t>(failedKeys.size())) {
        return Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    }
    LOG(INFO) << FormatString("Expire objects like %s with ttl time %d success.", keys[0], ttlSeconds);
    return Status::OK();
}

Status ClientWorkerLocalApi::GetMetaInfo(const std::vector<std::string> &keys, const bool isDevKey,
                                         GetMetaInfoRspPb &rsp)
{
    GetRequestContext()->reqTimeoutDuration.Init(ClientGetRequestTimeout(requestTimeoutMs_));
    GetMetaInfoReqPb req;
    req.set_client_id(clientId_);
    req.set_is_dev_key(isDevKey);
    *req.mutable_object_keys() = { keys.begin(), keys.end() };
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetTokenAndTenantId(req), "Fail to set token to GetMetaInfoReqPb.");
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    return api_->WorkerOCGetMetaInfo(workerOCService_, req, rsp);
}

Status ClientWorkerLocalApi::ReconnectWorker(const std::vector<std::string> &gRefIds)
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
    return Status::OK();
}

Status ClientWorkerLocalApi::PrepairForDecreaseShmRef(
    std::function<Status(const std::string &, const std::shared_ptr<ShmUnitInfo> &)> mmapFunc)
{
    (void)mmapFunc;
    return Status::OK();
}

Status ClientWorkerLocalApi::InitPipelineRH2DQueue(ShmConvertHookFunc hook)
{
    (void)hook;
    return Status::OK();
}

Status ClientWorkerLocalApi::CleanUpForDecreaseShmRefAfterWorkerLost()
{
    return Status::OK();
}

void ClientWorkerLocalApi::CleanUpForPipelineRH2DQueueAfterWorkerLost()
{
}

bool ClientWorkerLocalApi::WorkerSupportPiplnRH2D()
{
    return false;
}

Status ClientWorkerLocalApi::DecreaseShmRef(const ShmKey &shmId, const std::function<Status()> &connectCheck,
                                            std::shared_timed_mutex &shutdownMtx)
{
    (void)connectCheck;
    (void)shutdownMtx;
    return DecreaseWorkerRef({ shmId });
}

Status ClientWorkerLocalApi::MemView2RpcMessage(const std::vector<MemView> &mvs, std::vector<RpcMessage> &rms)
{
    rms.clear();
    for (const auto &mv : mvs) {
        rms.emplace_back();
        auto &msg = rms.back();
        RETURN_IF_NOT_OK(msg.ZeroCopyBuffer(const_cast<void *>(mv.Data()), mv.Size()));
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
