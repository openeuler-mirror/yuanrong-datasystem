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
 * Description: pipeline h2d interface implement for client and worker
 */

#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/gflag/common_gflags.h"

namespace OsXprtPipln {

using namespace datasystem;

Status ParsePiplnH2DRequest(const GetReqPb &req, H2DChunkManager &mgr, const std::string &objectKey, int infoIdx,
                            std::string deviceId)
{
    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    const auto &info = req.h2d_infos(static_cast<int>(infoIdx));

    int32_t devId = -1;
    try {
        devId = std::stoi(deviceId);
    } catch (...) {
        return Status(K_INVALID, "invalid device id " + deviceId);
    }
    DevShmInfo devShmInfo{ .devType = TargetDeviceType::CUDA,
                           .devId = static_cast<uint32_t>(devId), /* now only support int device id */
                           .ptr = nullptr,                        /* pointer is inited in AddKey */
                           .size = size_t(info.target_size()) };
    uint32_t workerReqId = (uint32_t)GenerateReqId();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(mgr.AddKey(objectKey, workerReqId, devShmInfo, info.target_device_handle()),
                                     " objectKey is " + objectKey);
    return Status::OK();
}

Status ConstructPipelineRH2DResponse(GetRspPb &resp, H2DChunkManager &mgr, std::vector<std::string> rawObjectKeys)
{
    Status lastRc = Status::OK();
    if (!mgr.KeyNum())
        return lastRc;

    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    LOG_IF_ERROR(mgr.WaitAll(), "failed to wait all");

    for (size_t objectIndex = 0; objectIndex < rawObjectKeys.size(); objectIndex++) {
        auto &key = rawObjectKeys[objectIndex];
        uint32_t reqId;
        Status rc = mgr.GetReqId(key, reqId);
        if (rc.IsError()) {
            lastRc = Status(K_RUNTIME_ERROR, "find failed reqId in chunkmanager for key " + key);
            resp.add_objects()->set_object_key(key);
            continue;
        }
        auto reqInfo = mgr.GetReqInfo(reqId);
        if (!reqInfo) {
            lastRc = Status(K_RUNTIME_ERROR, "find failed reqInfo in chunkmanager for key " + key);
            resp.add_objects()->set_object_key(key);
            continue;
        }
        if ((reqInfo->receivedChunks <= 0) || (reqInfo->failedChunkId)) {
            lastRc = Status(K_RUNTIME_ERROR, "find failed chunk or unreceived chunk for key " + key);
            resp.add_objects()->set_object_key(key);
        }
    }

    return lastRc;
}
#undef ADD_FAILED_KEY
#undef UPDATE_LAST_STATUS

Status TriggerLocalPipelineRH2D(H2DChunkManager &mgr, const std::string &objectKey, void *pointer, size_t metaSize,
                                size_t dataSize)
{
    if (!mgr.KeyNum())
        return Status::OK();

    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    uint32_t reqId;
    RETURN_IF_NOT_OK(mgr.GetReqId(objectKey, reqId));
    size_t srcData = reinterpret_cast<size_t>(pointer) + metaSize;
    RETURN_IF_NOT_OK(mgr.ReceiveLocalChunk(reqId, (void *)srcData, dataSize));

    return Status::OK();
}

Status TriggerRemotePipelineRH2D(H2DChunkManager &mgr, const std::string &key, uint64_t offset, uint64_t size,
                                 std::shared_ptr<ShmUnit> shmUnit, const std::string &remoteAddress,
                                 GetObjectRemoteReqPb &subReq)
{
    if (!mgr.KeyNum())
        return Status::OK();

    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();
    uint32_t reqId;
    RETURN_IF_NOT_OK(mgr.GetReqId(key, reqId));

    // get seg
    uint64_t segAddress;
    uint64_t segSize;
    urma_target_seg_t *targetSeg;
    urma_jfr_t *targetJfr;
    uint64_t pointer = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
    uint64_t src = pointer + offset;
    GetSegmentInfoFromShmUnit(shmUnit, pointer, segAddress, segSize);
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetTargetSeg(segAddress, segSize, remoteAddress, &targetSeg, &targetJfr));

    // start Receiver
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(mgr.StartReceiver(reqId, src, size, targetSeg, targetJfr),
                                     "failed to start receiver");

    // set field to trigger sender
    subReq.mutable_urma_info()->set_client_req_id(reqId);

    return Status::OK();
}

Status InitOsPiplnH2DEnv(void *ctx, void *jfc, void *jfce)
{
    if (!SupportPipelineRH2D())
        return Status::OK();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        H2DChunkManager::InitOsPiplnH2DEnv((urma_context_t *)ctx, (urma_jfc_t *)jfc, (urma_jfce_t *)jfce,
                                           FLAGS_pipeline_h2d_thread_num),
        "failed to init os pipeline env");
    return Status::OK();
}

int PiplnH2DRecvEventHook(void *cr)
{
    if (!SupportPipelineRH2D())
        return 0;
    return ChunkManager::ReceiveEventHook((urma_cr_t *)cr);
}

Status StartPipelineSender(PiplnSndArgs &args)
{
    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    ChunkManager mgr{ false /* isClient */ };
    DevShmInfo dev{
        .devType = TargetDeviceType::CUDA,
        .devId = 0,
        .ptr = nullptr,
        .size = 0,
    };
    mgr.AddKey("", args.clientKey, dev, "");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(mgr.StartSender(args), "failed to start sender");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(mgr.WaitAll(), "failed to wait send done");
    return Status::OK();
}

bool IsPiplnH2DRequest(const GetReqPb &req)
{
    return req.h2d_infos_size() != 0;
}

bool IsPiplnH2DRequest(const UrmaRemoteAddrPb &urmaInfo)
{
    return urmaInfo.has_client_req_id();
}

bool IsPiplnH2DRequest(const BatchGetObjectRemoteReqPb &req)
{
    return req.requests_size() && req.requests(0).urma_info().has_client_req_id();
}

bool IsPiplnH2DRequest(const H2DChunkManager &mgr)
{
    return (mgr.KeyNum() != 0);
}

void UnInitOsPiplnH2DEnv()
{
    ChunkManager::UnInitOsPiplnH2DEnv();
}

Status MaybeTriggerLocalPipelineRH2D(H2DChunkManager &mgr, const std::string &key, uint64_t offset, uint64_t size,
                                     std::shared_ptr<ShmUnit> shmUnit)
{
    if (!mgr.KeyNum())
        return Status::OK();

    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();
    uint32_t reqId;
    RETURN_IF_NOT_OK(mgr.GetReqId(key, reqId));
    ReqInfo *reqInfo = mgr.GetReqInfo(reqId);

    // maybe already get from remote
    if (reqInfo->receivedChunks != 0)
        return Status::OK();

    // switch remote pipeline to local pipeline
    mgr.Cancel(reqId);
    uint64_t src = reinterpret_cast<uint64_t>(shmUnit->GetPointer()) + offset;
    RETURN_IF_NOT_OK(mgr.ReceiveLocalChunk(reqId, reinterpret_cast<void *>(src), size));
    return Status::OK();
}

}  // namespace OsXprtPipln