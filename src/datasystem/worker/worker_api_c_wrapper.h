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
 * Description: The embedded client plugin.
 */

#ifndef DATASYSTEM_WORKER_WORKER_API_C_WRAPPER_H
#define DATASYSTEM_WORKER_WORKER_API_C_WRAPPER_H

#include <cstddef>
#include <cstdint>

#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/worker.h"
#include "datasystem/worker/worker_service_impl.h"

#ifdef __cplusplus
extern "C" {
#endif
/**
 * @brief Get workerservice ptr.
 * @param[in] obj worker ptr.
 */
void *GetWorkerService(void *obj);

/**
 * @brief Get workerocservice ptr.
 * @param[in] obj worker ptr.
 */
void *GetWorkerOCService(void *obj);

/**
 * @brief RegisterClient.
 * @param[in] obj WorkerServiceImpl ptr.
 * @param[in] req RegisterClientReqPb.
 * @param[in] resp RegisterClientRspPb.
 */
Status WorkerRegisterClient(void *obj, const RegisterClientReqPb &req, RegisterClientRspPb &resp);

/**
 * @brief Heartbeat.
 * @param[in] obj WorkerServiceImpl ptr.
 * @param[in] req HeartbeatReqPb.
 * @param[in] resp HeartbeatRspPb.
 */
Status WorkerHeartbeat(void *obj, const HeartbeatReqPb &req, HeartbeatRspPb &resp);

/**
 * @brief WorkerDisconnectClient.
 * @param[in] obj WorkerServiceImpl ptr.
 * @param[in] req DisconnectClientReqPb.
 * @param[in] resp DisconnectClientRspPb.
 */
Status WorkerDisconnectClient(void *obj, const DisconnectClientReqPb &req, DisconnectClientRspPb &resp);

/**
 * @brief WorkerOCCreate.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req CreateReqPb.
 * @param[in] resp CreateRspPb.
 */
Status WorkerOCCreate(void *obj, const CreateReqPb &req, CreateRspPb &resp);

/**
 * @brief WorkerOCPublish.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req PublishReqPb.
 * @param[in] resp PublishRspPb.
 * @param[in] payloads payloads to send data.
 */
Status WorkerOCPublish(void *obj, const PublishReqPb &req, PublishRspPb &resp, std::vector<RpcMessage> payloads);

/**
 * @brief WorkerOCGet.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] serverApi ptr for read req and return rsp.
 */
Status WorkerOCGet(void *obj, std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> &serverApi);

/**
 * @brief WorkerOCMultiPublish.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req MultiPublishReqPb.
 * @param[in] resp MultiPublishRspPb.
 * @param[in] payloads payloads to send data.
 */
Status WorkerOCMultiPublish(void *obj, const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                            std::vector<RpcMessage> payloads);

/**
 * @brief WorkerOCDecreaseReference.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req DecreaseReferenceRequest.
 * @param[in] resp DecreaseReferenceResponse.
 */
Status WorkerOCDecreaseReference(void *obj, const DecreaseReferenceRequest &req, DecreaseReferenceResponse &resp);

/**
 * @brief WorkerOCReconcileShmRef.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req ReconcileShmRefReqPb.
 * @param[in] resp ReconcileShmRefRspPb.
 */
Status WorkerOCReconcileShmRef(void *obj, const ReconcileShmRefReqPb &req, ReconcileShmRefRspPb &resp);

/**
 * @brief WorkerOCInvalidateBuffer.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req InvalidateBufferReqPb.
 * @param[in] resp InvalidateBufferRspPb.
 */
Status WorkerOCInvalidateBuffer(void *obj, const InvalidateBufferReqPb &req, InvalidateBufferRspPb &resp);

/**
 * @brief WorkerOCGIncreaseRef.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req GIncreaseReqPb.
 * @param[in] resp GIncreaseRspPb.
 */
Status WorkerOCGIncreaseRef(void *obj, const GIncreaseReqPb &req, GIncreaseRspPb &resp);

/**
 * @brief WorkerOCReleaseGRefs.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req ReleaseGRefsReqPb.
 * @param[in] resp ReleaseGRefsRspPb.
 */
Status WorkerOCReleaseGRefs(void *obj, const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp);

/**
 * @brief WorkerOCGDecreaseRef.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req GDecreaseReqPb.
 * @param[in] resp GDecreaseRspPb.
 */
Status WorkerOCGDecreaseRef(void *obj, const GDecreaseReqPb &req, GDecreaseRspPb &resp);

/**
 * @brief WorkerOCDeleteAllCopy.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req DeleteAllCopyReqPb.
 * @param[in] resp DeleteAllCopyRspPb.
 */
Status WorkerOCDeleteAllCopy(void *obj, const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp);

/**
 * @brief WorkerOCQueryGlobalRefNum.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req QueryGlobalRefNumReqPb.
 * @param[in] resp QueryGlobalRefNumRspCollectionPb.
 */
Status WorkerOCQueryGlobalRefNum(void *obj, const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &resp);

/**
 * @brief WorkerOCPublishDeviceObject.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req PublishDeviceObjectRspPb.
 * @param[in] resp DisconnectClientRspPb.
 * @param[out] payloads payloads to receive.
 */
Status WorkerOCPublishDeviceObject(void *obj, const PublishDeviceObjectReqPb &req, PublishDeviceObjectRspPb &resp,
                                   std::vector<RpcMessage> payloads);

/**
 * @brief PutP2PMeta.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req PutP2PMetaReqPb.
 * @param[in] resp PutP2PMetaRspPb.
 */
Status WorkerOCPutP2PMeta(void *obj, const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp);

/**
 * @brief SendRootInfo.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req SendRootInfoReqPb.
 * @param[in] resp SendRootInfoRspPb.
 */
Status WorkerOCSendRootInfo(void *obj, const SendRootInfoReqPb &req, SendRootInfoRspPb &resp);

/**
 * @brief AckRecvFinish.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req AckRecvFinishReqPb.
 * @param[in] resp AckRecvFinishRspPb.
 */
Status WorkerOCAckRecvFinish(void *obj, const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp);

/**
 * @brief RemoveP2PLocation.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req RemoveP2PLocationReqPb.
 * @param[in] resp RemoveP2PLocationRspPb.
 */
Status WorkerOCRemoveP2PLocation(void *obj, const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp);

/**
 * @brief GetObjMetaInfo.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req GetObjMetaInfoReqPb.
 * @param[in] resp GetObjMetaInfoRspPb.
 */
Status WorkerOCGetObjMetaInfo(void *obj, const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &resp);

/**
 * @brief MultiCreate.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req MultiCreateReqPb.
 * @param[in] resp MultiCreateRspPb.
 */
Status WorkerOCMultiCreate(void *obj, const MultiCreateReqPb &req, MultiCreateRspPb &resp);

/**
 * @brief QuerySize.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req QuerySizeReqPb.
 * @param[in] resp QuerySizeRspPb.
 */
Status WorkerOCQuerySize(void *obj, const QuerySizeReqPb &req, QuerySizeRspPb &resp);

/**
 * @brief HealthCheck.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req HealthCheckRequestPb.
 * @param[in] resp HealthCheckReplyPb.
 */
Status WorkerOCHealthCheck(void *obj, const HealthCheckRequestPb &req, HealthCheckReplyPb &resp);

/**
 * @brief Exist.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req ExistReqPb.
 * @param[in] resp ExistRspPb.
 */
Status WorkerOCExist(void *obj, const ExistReqPb &req, ExistRspPb &resp);

/**
 * @brief Expire.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req ExpireReqPb.
 * @param[in] resp ExpireRspPb.
 */
Status WorkerOCExpire(void *obj, const ExpireReqPb &req, ExpireRspPb &resp);

/**
 * @brief GetMetaInfo.
 * @param[in] obj WorkerOCServiceImpl.
 * @param[in] req GetMetaInfoReqPb.
 * @param[in] resp GetMetaInfoRspPb.
 */
Status WorkerOCGetMetaInfo(void *obj, const GetMetaInfoReqPb &req, GetMetaInfoRspPb &resp);

#ifdef __cplusplus
};
#endif

#endif
