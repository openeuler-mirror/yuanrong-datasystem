/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description:
 */

#include "datasystem/client/embedded_client_worker_api.h"
#include <utility>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
 
namespace datasystem {
namespace client {

Status EmbeddedClientWorkerApi::LoadPlugin()
{
    void *handle =
        dlopen("libdatasystem_worker.so", RTLD_LAZY);
    if (!handle) {
        RETURN_STATUS(K_INVALID, "open so failed: " + GetDlErrorMsg());
    }
    // init service
    DLSYM_FUNC_OBJ(CreateWorker, handle);
    DLSYM_FUNC_OBJ(WorkerDestroy, handle);
    DLSYM_FUNC_OBJ(InitEmbeddedWorker, handle);
    DLSYM_FUNC_OBJ(GetWorkerService, handle);
    DLSYM_FUNC_OBJ(GetWorkerOCService, handle);
    // common service
    DLSYM_FUNC_OBJ(WorkerRegisterClient, handle);
    DLSYM_FUNC_OBJ(WorkerHeartbeat, handle);
    DLSYM_FUNC_OBJ(WorkerDisconnectClient, handle);
    // object cache service
    DLSYM_FUNC_OBJ(WorkerOCCreate, handle);
    DLSYM_FUNC_OBJ(WorkerOCPublish, handle);
    DLSYM_FUNC_OBJ(WorkerOCGet, handle);
    DLSYM_FUNC_OBJ(WorkerOCMultiPublish, handle);
    DLSYM_FUNC_OBJ(WorkerOCDecreaseReference, handle);
    DLSYM_FUNC_OBJ(WorkerOCInvalidateBuffer, handle);
    DLSYM_FUNC_OBJ(WorkerOCGIncreaseRef, handle);
    DLSYM_FUNC_OBJ(WorkerOCReleaseGRefs, handle);
    DLSYM_FUNC_OBJ(WorkerOCGDecreaseRef, handle);
    DLSYM_FUNC_OBJ(WorkerOCDeleteAllCopy, handle);
    DLSYM_FUNC_OBJ(WorkerOCQueryGlobalRefNum, handle);
    DLSYM_FUNC_OBJ(WorkerOCPublishDeviceObject, handle);
    DLSYM_FUNC_OBJ(WorkerOCPutP2PMeta, handle);
    DLSYM_FUNC_OBJ(WorkerOCSendRootInfo, handle);
    DLSYM_FUNC_OBJ(WorkerOCAckRecvFinish, handle);
    DLSYM_FUNC_OBJ(WorkerOCRemoveP2PLocation, handle);
    DLSYM_FUNC_OBJ(WorkerOCGetObjMetaInfo, handle);
    DLSYM_FUNC_OBJ(WorkerOCMultiCreate, handle);
    DLSYM_FUNC_OBJ(WorkerOCQuerySize, handle);
    DLSYM_FUNC_OBJ(WorkerOCHealthCheck, handle);
    DLSYM_FUNC_OBJ(WorkerOCExist, handle);
    DLSYM_FUNC_OBJ(WorkerOCExpire, handle);
    DLSYM_FUNC_OBJ(WorkerOCGetMetaInfo, handle);
    return Status::OK();
}

void *EmbeddedClientWorkerApi::CreateWorker()
{
    if (CreateWorkerFunc_ == nullptr) {
        return nullptr;
    };
    return CreateWorkerFunc_();
}

void EmbeddedClientWorkerApi::WorkerDestroy(void *worker)
{
    if (WorkerDestroyFunc_ == nullptr) {
        return;
    }
    return WorkerDestroyFunc_(worker);
}

Status EmbeddedClientWorkerApi::InitEmbeddedWorker(const EmbeddedConfig &config, void *worker)
{
    RETURN_RUNTIME_ERROR_IF_NULL(InitEmbeddedWorkerFunc_);
    return InitEmbeddedWorkerFunc_(config, worker);
}

void *EmbeddedClientWorkerApi::GetWorkerService(void *worker)
{
    if (GetWorkerServiceFunc_ == nullptr) {
        return nullptr;
    }
    return GetWorkerServiceFunc_(worker);
}

void *EmbeddedClientWorkerApi::GetWorkerOCService(void *worker)
{
    if (GetWorkerOCServiceFunc_ == nullptr) {
        return nullptr;
    }
    return GetWorkerOCServiceFunc_(worker);
}

Status EmbeddedClientWorkerApi::WorkerOCCreate(void *obj, const CreateReqPb &req, CreateRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCCreateFunc_);
    return WorkerOCCreateFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCGet(void *obj,
                                            std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> &serverApi)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCGetFunc_);
    return WorkerOCGetFunc_(obj, serverApi);
}

Status EmbeddedClientWorkerApi::WorkerHeartbeat(void *obj, const HeartbeatReqPb &req, HeartbeatRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerHeartbeatFunc_);
    return WorkerHeartbeatFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerRegisterClient(void *obj, const RegisterClientReqPb &req,
                                                     RegisterClientRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerRegisterClientFunc_);
    return WorkerRegisterClientFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerDisconnectClient(void *obj, const DisconnectClientReqPb &req,
                                                       DisconnectClientRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerDisconnectClientFunc_);
    return WorkerDisconnectClientFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCPublish(void *obj, const PublishReqPb &req, PublishRspPb &resp,
                                                std::vector<RpcMessage> payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCPublishFunc_);
    return WorkerOCPublishFunc_(obj, req, resp, std::move(payloads));
}

Status EmbeddedClientWorkerApi::WorkerOCMultiPublish(void *obj, const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                                                     std::vector<RpcMessage> payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCMultiPublishFunc_);
    return WorkerOCMultiPublishFunc_(obj, req, resp, std::move(payloads));
}

Status EmbeddedClientWorkerApi::WorkerOCDecreaseReference(void *obj, const DecreaseReferenceRequest &req,
                                                          DecreaseReferenceResponse &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCDecreaseReferenceFunc_);
    return WorkerOCDecreaseReferenceFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCInvalidateBuffer(void *obj, const InvalidateBufferReqPb &req,
                                                         InvalidateBufferRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCInvalidateBufferFunc_);
    return WorkerOCInvalidateBufferFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCGIncreaseRef(void *obj, const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCGIncreaseRefFunc_);
    return WorkerOCGIncreaseRefFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCReleaseGRefs(void *obj, const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCReleaseGRefsFunc_);
    return WorkerOCReleaseGRefsFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCGDecreaseRef(void *obj, const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCGDecreaseRefFunc_);
    return WorkerOCGDecreaseRefFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCDeleteAllCopy(void *obj, const DeleteAllCopyReqPb &req,
                                                      DeleteAllCopyRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCDeleteAllCopyFunc_);
    return WorkerOCDeleteAllCopyFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCQueryGlobalRefNum(void *obj, const QueryGlobalRefNumReqPb &req,
                                                          QueryGlobalRefNumRspCollectionPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCQueryGlobalRefNumFunc_);
    return WorkerOCQueryGlobalRefNumFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCPublishDeviceObject(void *obj, const PublishDeviceObjectReqPb &req,
                                                            PublishDeviceObjectRspPb &resp,
                                                            std::vector<RpcMessage> payloads)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCPublishDeviceObjectFunc_);
    return WorkerOCPublishDeviceObjectFunc_(obj, req, resp, std::move(payloads));
}

Status EmbeddedClientWorkerApi::WorkerOCPutP2PMeta(void *obj, const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCPutP2PMetaFunc_);
    return WorkerOCPutP2PMetaFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCSendRootInfo(void *obj, const SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCSendRootInfoFunc_);
    return WorkerOCSendRootInfoFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCAckRecvFinish(void *obj, const AckRecvFinishReqPb &req,
                                                      AckRecvFinishRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCAckRecvFinishFunc_);
    return WorkerOCAckRecvFinishFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCRemoveP2PLocation(void *obj, const RemoveP2PLocationReqPb &req,
                                                          RemoveP2PLocationRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCRemoveP2PLocationFunc_);
    return WorkerOCRemoveP2PLocationFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCGetObjMetaInfo(void *obj, const GetObjMetaInfoReqPb &req,
                                                       GetObjMetaInfoRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCGetObjMetaInfoFunc_);
    return WorkerOCGetObjMetaInfoFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCMultiCreate(void *obj, const MultiCreateReqPb &req, MultiCreateRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCMultiCreateFunc_);
    return WorkerOCMultiCreateFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCQuerySize(void *obj, const QuerySizeReqPb &req, QuerySizeRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCQuerySizeFunc_);
    return WorkerOCQuerySizeFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCHealthCheck(void *obj, const HealthCheckRequestPb &req,
                                                    HealthCheckReplyPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCHealthCheckFunc_);
    return WorkerOCHealthCheckFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCExist(void *obj, const ExistReqPb &req, ExistRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCExistFunc_);
    return WorkerOCExistFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCExpire(void *obj, const ExpireReqPb &req, ExpireRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCExpireFunc_);
    return WorkerOCExpireFunc_(obj, req, resp);
}

Status EmbeddedClientWorkerApi::WorkerOCGetMetaInfo(void *obj, const GetMetaInfoReqPb &req, GetMetaInfoRspPb &resp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(WorkerOCGetMetaInfoFunc_);
    return WorkerOCGetMetaInfoFunc_(obj, req, resp);
}
}  // namespace client
}  // namespace datasystem