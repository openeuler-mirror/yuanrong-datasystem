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
 
#ifndef DATASYSTEM_CLIENT_EMBEDDED_CLIENT_WORKER_API_H
#define DATASYSTEM_CLIENT_EMBEDDED_CLIENT_WORKER_API_H
 
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/common/util/dlutils.h"
#include "datasystem/utils/embedded_config.h"
#include "datasystem/utils/status.h"
 
namespace datasystem {
namespace client {
class EmbeddedClientWorkerApi {
public:
    EmbeddedClientWorkerApi() = default;

    Status LoadPlugin();
 
    // init service
    void *CreateWorker();
    void WorkerDestroy(void *worker);
    Status InitEmbeddedWorker(const EmbeddedConfig &config, void *worker);
    void *GetWorkerService(void *worker);
    void *GetWorkerOCService(void *worker);
    // common service
    Status WorkerRegisterClient(void *obj, const RegisterClientReqPb &req, RegisterClientRspPb &resp);
    Status WorkerHeartbeat(void *obj, const HeartbeatReqPb &req, HeartbeatRspPb &resp);
    Status WorkerDisconnectClient(void *obj, const DisconnectClientReqPb &req, DisconnectClientRspPb &resp);
    // object cache service
    Status WorkerOCCreate(void *obj, const CreateReqPb &req, CreateRspPb &resp);
    Status WorkerOCPublish(void *obj, const PublishReqPb &req, PublishRspPb &resp, std::vector<RpcMessage> payloads);
    Status WorkerOCGet(void *obj, std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> &serverApi);
    Status WorkerOCMultiPublish(void *obj, const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                                std::vector<RpcMessage> payloads);
    Status WorkerOCDecreaseReference(void *obj, const DecreaseReferenceRequest &req, DecreaseReferenceResponse &resp);
    Status WorkerOCInvalidateBuffer(void *obj, const InvalidateBufferReqPb &req, InvalidateBufferRspPb &resp);
    Status WorkerOCGIncreaseRef(void *obj, const GIncreaseReqPb &req, GIncreaseRspPb &resp);
    Status WorkerOCReleaseGRefs(void *obj, const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp);
    Status WorkerOCGDecreaseRef(void *obj, const GDecreaseReqPb &req, GDecreaseRspPb &resp);
    Status WorkerOCDeleteAllCopy(void *obj, const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp);
    Status WorkerOCQueryGlobalRefNum(void *obj, const QueryGlobalRefNumReqPb &req,
                                     QueryGlobalRefNumRspCollectionPb &resp);
    Status WorkerOCPublishDeviceObject(void *obj, const PublishDeviceObjectReqPb &req, PublishDeviceObjectRspPb &resp,
                                       std::vector<RpcMessage> payloads);
    Status WorkerOCPutP2PMeta(void *obj, const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp);
    Status WorkerOCSendRootInfo(void *obj, const SendRootInfoReqPb &req, SendRootInfoRspPb &resp);
    Status WorkerOCAckRecvFinish(void *obj, const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp);
    Status WorkerOCRemoveP2PLocation(void *obj, const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp);
    Status WorkerOCGetObjMetaInfo(void *obj, const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &resp);
    Status WorkerOCMultiCreate(void *obj, const MultiCreateReqPb &req, MultiCreateRspPb &resp);
    Status WorkerOCQuerySize(void *obj, const QuerySizeReqPb &req, QuerySizeRspPb &resp);
    Status WorkerOCHealthCheck(void *obj, const HealthCheckRequestPb &req, HealthCheckReplyPb &resp);
    Status WorkerOCExist(void *obj, const ExistReqPb &req, ExistRspPb &resp);
    Status WorkerOCExpire(void *obj, const ExpireReqPb &req, ExpireRspPb &resp);
    Status WorkerOCGetMetaInfo(void *obj, const GetMetaInfoReqPb &req, GetMetaInfoRspPb &resp);

private:
    // init service
    REG_METHOD(CreateWorker, void *);
    REG_METHOD(WorkerDestroy, void, void *);
    REG_METHOD(InitEmbeddedWorker, Status, const EmbeddedConfig &, void *);
    REG_METHOD(GetWorkerService, void *, void *);
    REG_METHOD(GetWorkerOCService, void *, void *);
    // common service
    REG_METHOD(WorkerRegisterClient, Status, void *, const RegisterClientReqPb &, RegisterClientRspPb &);
    REG_METHOD(WorkerHeartbeat, Status, void *, const HeartbeatReqPb &, HeartbeatRspPb &);
    REG_METHOD(WorkerDisconnectClient, Status, void *, const DisconnectClientReqPb &, DisconnectClientRspPb &);
    // object cache service
    REG_METHOD(WorkerOCCreate, Status, void *, const CreateReqPb &, CreateRspPb &);
    REG_METHOD(WorkerOCPublish, Status, void *, const PublishReqPb &, PublishRspPb &, std::vector<RpcMessage>);
    REG_METHOD(WorkerOCGet, Status, void *, std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> &);
    REG_METHOD(WorkerOCMultiPublish, Status, void *, const MultiPublishReqPb &, MultiPublishRspPb &,
               std::vector<RpcMessage>);
    REG_METHOD(WorkerOCDecreaseReference, Status, void *, const DecreaseReferenceRequest &,
               DecreaseReferenceResponse &);
    REG_METHOD(WorkerOCInvalidateBuffer, Status, void *, const InvalidateBufferReqPb &, InvalidateBufferRspPb &);
    REG_METHOD(WorkerOCGIncreaseRef, Status, void *, const GIncreaseReqPb &, GIncreaseRspPb &);
    REG_METHOD(WorkerOCReleaseGRefs, Status, void *, const ReleaseGRefsReqPb &, ReleaseGRefsRspPb &);
    REG_METHOD(WorkerOCGDecreaseRef, Status, void *, const GDecreaseReqPb &, GDecreaseRspPb &);
    REG_METHOD(WorkerOCDeleteAllCopy, Status, void *, const DeleteAllCopyReqPb &, DeleteAllCopyRspPb &);
    REG_METHOD(WorkerOCQueryGlobalRefNum, Status, void *, const QueryGlobalRefNumReqPb &,
               QueryGlobalRefNumRspCollectionPb &);
    REG_METHOD(WorkerOCPublishDeviceObject, Status, void *, const PublishDeviceObjectReqPb &,
               PublishDeviceObjectRspPb &, std::vector<RpcMessage>);
    REG_METHOD(WorkerOCPutP2PMeta, Status, void *, const PutP2PMetaReqPb &, PutP2PMetaRspPb &);
    REG_METHOD(WorkerOCSendRootInfo, Status, void *, const SendRootInfoReqPb &, SendRootInfoRspPb &);
    REG_METHOD(WorkerOCAckRecvFinish, Status, void *, const AckRecvFinishReqPb &, AckRecvFinishRspPb &);
    REG_METHOD(WorkerOCRemoveP2PLocation, Status, void *, const RemoveP2PLocationReqPb &, RemoveP2PLocationRspPb &);
    REG_METHOD(WorkerOCGetObjMetaInfo, Status, void *, const GetObjMetaInfoReqPb &, GetObjMetaInfoRspPb &);
    REG_METHOD(WorkerOCMultiCreate, Status, void *, const MultiCreateReqPb &, MultiCreateRspPb &);
    REG_METHOD(WorkerOCQuerySize, Status, void *, const QuerySizeReqPb &, QuerySizeRspPb &);
    REG_METHOD(WorkerOCHealthCheck, Status, void *, const HealthCheckRequestPb &, HealthCheckReplyPb &);
    REG_METHOD(WorkerOCExist, Status, void *, const ExistReqPb &, ExistRspPb &);
    REG_METHOD(WorkerOCExpire, Status, void *, const ExpireReqPb &, ExpireRspPb &);
    REG_METHOD(WorkerOCGetMetaInfo, Status, void *, const GetMetaInfoReqPb &, GetMetaInfoRspPb &);
};
}  // namespace client
}  // namespace datasystem
#endif