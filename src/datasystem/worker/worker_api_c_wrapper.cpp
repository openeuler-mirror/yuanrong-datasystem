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
 * Description: Provide functions for dlopen to call.
 */

#include "datasystem/worker/worker_api_c_wrapper.h"

void *GetWorkerService(void *obj)
{
    return static_cast<datasystem::worker::Worker *>(obj)->GetWorkerService();
}

void *GetWorkerOCService(void *obj)
{
    return static_cast<datasystem::worker::Worker *>(obj)->GetWorkerOCService();
}

Status WorkerRegisterClient(void *obj, const RegisterClientReqPb &req, RegisterClientRspPb &resp)
{
    return static_cast<datasystem::worker::WorkerServiceImpl *>(obj)->RegisterClient(req, resp);
}

Status WorkerHeartbeat(void *obj, const HeartbeatReqPb &req, HeartbeatRspPb &resp)
{
    return static_cast<datasystem::worker::WorkerServiceImpl *>(obj)->Heartbeat(req, resp);
}

Status WorkerDisconnectClient(void *obj, const DisconnectClientReqPb &req, DisconnectClientRspPb &resp)
{
    return static_cast<datasystem::worker::WorkerServiceImpl *>(obj)->DisconnectClient(req, resp);
}

Status WorkerOCCreate(void *obj, const CreateReqPb &req, CreateRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->Create(req, resp);
}

Status WorkerOCPublish(void *obj, const PublishReqPb &req, PublishRspPb &resp, std::vector<RpcMessage> payloads)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->Publish(req, resp, std::move(payloads));
}

Status WorkerOCGet(void *obj, std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> &serverApi)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->Get(serverApi);
}

Status WorkerOCMultiPublish(void *obj, const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                            std::vector<RpcMessage> payloads)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->MultiPublish(req, resp,
                                                                                           std::move(payloads));
}

Status WorkerOCDecreaseReference(void *obj, const DecreaseReferenceRequest &req, DecreaseReferenceResponse &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->DecreaseReference(req, resp);
}

Status WorkerOCInvalidateBuffer(void *obj, const InvalidateBufferReqPb &req, InvalidateBufferRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->InvalidateBuffer(req, resp);
}

Status WorkerOCGIncreaseRef(void *obj, const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->GIncreaseRef(req, resp);
}

Status WorkerOCReleaseGRefs(void *obj, const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->ReleaseGRefs(req, resp);
}

Status WorkerOCGDecreaseRef(void *obj, const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->GDecreaseRef(req, resp);
}

Status WorkerOCDeleteAllCopy(void *obj, const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->DeleteAllCopy(req, resp);
}

Status WorkerOCQueryGlobalRefNum(void *obj, const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->QueryGlobalRefNum(req, resp);
}

Status WorkerOCPublishDeviceObject(void *obj, const PublishDeviceObjectReqPb &req, PublishDeviceObjectRspPb &resp,
                                   std::vector<RpcMessage> payloads)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->PublishDeviceObject(req, resp,
                                                                                                  std::move(payloads));
}

Status WorkerOCPutP2PMeta(void *obj, const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->PutP2PMeta(req, resp);
}

Status WorkerOCSendRootInfo(void *obj, const SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->SendRootInfo(req, resp);
}

Status WorkerOCAckRecvFinish(void *obj, const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->AckRecvFinish(req, resp);
}

Status WorkerOCRemoveP2PLocation(void *obj, const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->RemoveP2PLocation(req, resp);
}

Status WorkerOCGetObjMetaInfo(void *obj, const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->GetObjMetaInfo(req, resp);
}

Status WorkerOCMultiCreate(void *obj, const MultiCreateReqPb &req, MultiCreateRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->MultiCreate(req, resp);
}

Status WorkerOCQuerySize(void *obj, const QuerySizeReqPb &req, QuerySizeRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->QuerySize(req, resp);
}

Status WorkerOCHealthCheck(void *obj, const HealthCheckRequestPb &req, HealthCheckReplyPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->HealthCheck(req, resp);
}

Status WorkerOCExist(void *obj, const ExistReqPb &req, ExistRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->Exist(req, resp);
}

Status WorkerOCExpire(void *obj, const ExpireReqPb &req, ExpireRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->Expire(req, resp);
}

Status WorkerOCGetMetaInfo(void *obj, const GetMetaInfoReqPb &req, GetMetaInfoRspPb &resp)
{
    return static_cast<datasystem::object_cache::WorkerOCServiceImpl *>(obj)->GetMetaInfo(req, resp);
}