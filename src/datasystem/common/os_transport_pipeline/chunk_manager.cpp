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
 * Description: manage the splited segment chunks, use driver to implement device share memory IPC.
 */

#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include <ub/umdk/urma/urma_api.h>

// maybe not need
#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
// not need

#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/os_transport_pipeline/dlopen_util.h"

#include "datasystem/common/os_transport_pipeline/cuda_ipc.h"
#include "datasystem/common/os_transport_pipeline/mock_ipc.h"
#include "os-transport/os_transport.h"

namespace OsXprtPipln {

void *IpcDriver::GetEncodeHandle(size_t handleSize)
{
    targetHandle.resize(handleSize + ReservedHandleSize());
    uint32_t *handle = reinterpret_cast<uint32_t *>(targetHandle.data());
    *handle = devId;
    return (void *)(handle + 1);
}

void *IpcDriver::GetDecodeHandle(size_t handleSize)
{
    if (targetHandle.size() != handleSize + ReservedHandleSize()) {
        return nullptr;
    }
    uint32_t *handle = reinterpret_cast<uint32_t *>(targetHandle.data());
    devId = *handle;
    return (void *)(handle + 1);
}

uint32_t ChunkManager::GenerateReqId()
{
    ++reqId_;
    if (reqId_ == 0) {
        reqId_ = 1;
    }
    return reqId_;
}

std::atomic<uint32_t> ChunkManager::reqId_ = std::atomic<uint32_t>([]() {
    std::random_device rd;
    return rd();
}());

void *ChunkManager::osPiplnH2DHandle_ = nullptr;

Status ChunkManager::AddKey(const std::string &key, uint32_t reqId, const DevShmInfo &devInfo,
                            const std::string &targetHandle)
{
    auto it = reqInfos.find(reqId);
    if (it != reqInfos.end()) {
        return Status(StatusCode::K_DUPLICATED, "key " + key
                                                    + " will duplicate, old seqId: " + std::to_string(it->first)
                                                    + " new reqId: " + std::to_string(reqId));
    }

    auto &reqInfo = reqInfos[reqId];
    RETURN_IF_NOT_OK(IpcDriver::GetDriver(devInfo, targetHandle, isClient_, reqInfo.driver));

    keyToReqIdMap[key] = reqId;

    return Status::OK();
}

Status ChunkManager::ReceiveRemoteChunk(ChunkTag tag, void *srcData)
{
    auto chunkId = tag.chunkId;
    size_t chunkSize = tag.chunkSize;
    uint32_t reqId = tag.reqId;
    auto it = reqInfos.find(reqId);
    if (it == reqInfos.end()) {
        return Status(StatusCode::K_NOT_FOUND, "[ReceiveRemoteChunk] no requestid " + std::to_string(reqId));
    }
    auto &info = it->second;
    size_t destOffset;

    if (ChunkTag::IsLastChunk(tag)) {
        destOffset = info.driver->targetSize - chunkSize;
    } else {
        destOffset = chunkSize * chunkId;
    }
    info.receivedChunks++;
    Status ret = SubmitIO(info, srcData, chunkSize, destOffset);
    if (ret.IsError() && !info.failedChunkId) {
        info.failedChunkId = chunkId;
    }
    return ret;
}

Status ChunkManager::GetReqId(const std::string &key, uint32_t &reqId)
{
    auto it = keyToReqIdMap.find(key);
    if (it == keyToReqIdMap.end()) {
        return Status(StatusCode::K_NOT_FOUND, "request key " + key + " is not found in ChunkManager");
    } else {
        reqId = it->second;
        return Status::OK();
    }
}

Status ChunkManager::StartReceiver(uint32_t reqId, uint64_t src, uint64_t size, urma_target_seg_t *targetSeg,
                                   urma_jfr_t *targetJfr)
{
    ReqInfo *info = GetReqInfo(reqId);
    if (!info) {
        return Status(StatusCode::K_NOT_FOUND, "reqId " + std::to_string(reqId) + " is not registered in ChunkManager");
    }

    ost_buffer_info_t hostSrc;
    ost_device_info_t deviceInfo;

    hostSrc.addr = src;
    hostSrc.tseg = targetSeg;

    if (info->driver->targetType == TargetDeviceType::CUDA) {
        deviceInfo.dst = info->driver->targetAddr;
        deviceInfo.jfr = targetJfr;
        dynamic_cast<CudaIPC *>(info->driver.get())->ExportHandles(deviceInfo.stream, deviceInfo.event);
    }

    int ret;
    CALL_OS_XPRT_FUNC(ret, DoRecv, osPiplnH2DHandle_, &hostSrc, &deviceInfo, size, reqId,
                      (task_sync **)&info->syncHandle);
    VLOG(1) << "os_transport_recv ret: " << ret << " reqId: " << reqId << " src " << src << " targetSeg.seg.ubva.va "
            << targetSeg->seg.ubva.va << " len " << size << " segoff " << (src - targetSeg->seg.ubva.va);
    if (ret != 0) {
        return Status(StatusCode::K_RUNTIME_ERROR, "os_transport_recv start failed for " + std::to_string(reqId));
    }
    return Status::OK();
}

ReqInfo *ChunkManager::GetReqInfo(uint32_t reqId)
{
    auto it = reqInfos.find(reqId);
    if (it == reqInfos.end()) {
        return nullptr;
    }
    return &it->second;
}

static void OsLogCallback(int level, const char* msg)
{
    datasystem::LogMessage((datasystem::LogSeverity)level, __FILE__, __LINE__).Stream() << msg;
}

Status ChunkManager::InitOsPiplnH2DEnv(urma_context_t *ctx, urma_jfc_t *jfc, urma_jfce_t *jfce, int threadNum)
{
    os_transport_cfg_t cfg;
    cfg.worker_thread_num = threadNum;
    cfg.urma_event_mode = true;
    cfg.jfce = jfce;
    cfg.jfc = jfc;

    DO_LOAD_DYNLIB(OsTransportLibLoader);
    if (!IS_VALID_DYNFUNC(OsTransportLibLoader, DoInit))
        return Status(StatusCode::K_NOT_FOUND, "failed to load OsTransportLibLoader");

    DO_LOAD_DYNLIB(CudaRTLibLoader);
    if (!IS_VALID_DYNFUNC(CudaRTLibLoader, cudaSetDevice)) {
        DO_UNLOAD_DYNLIB(OsTransportLibLoader);
        return Status(StatusCode::K_NOT_FOUND, "failed to load CudaRTLibLoader");
    }

    int ret;
    CALL_OS_XPRT_FUNC(ret, DoLogReg, FLAGS_minloglevel, OsLogCallback);
    CALL_OS_XPRT_FUNC(ret, DoInit, ctx, &cfg, &osPiplnH2DHandle_);
    if (ret == 0) {
        return Status::OK();
    } else {
        osPiplnH2DHandle_ = nullptr;
        DO_UNLOAD_DYNLIB(OsTransportLibLoader);
        DO_UNLOAD_DYNLIB(CudaRTLibLoader);
        LOG(ERROR) << "os_transport_init ret: " << ret;
        return Status(StatusCode::K_INVALID, "Failed to init os pipeline h2d environments");
    }
}

void ChunkManager::UnInitOsPiplnH2DEnv()
{
    if (!osPiplnH2DHandle_)
        return;
    int ret;
    CALL_OS_XPRT_FUNC(ret, DoDestroy, osPiplnH2DHandle_);
    osPiplnH2DHandle_ = nullptr;
    DO_UNLOAD_DYNLIB(OsTransportLibLoader);
    DO_UNLOAD_DYNLIB(CudaRTLibLoader);
}

Status ChunkManager::StartSender(PiplnSndArgs &args)
{
    uint32_t reqId = args.clientKey;

    ReqInfo *info = GetReqInfo(reqId);
    if (!info) {
        return Status(StatusCode::K_NOT_FOUND, "reqId " + std::to_string(reqId) + " is not registered in ChunkManager");
    }

    urma_jetty_info jettyInfo;
    jettyInfo.jfs = nullptr;
    jettyInfo.tjetty = args.tjetty;
    jettyInfo.jetty_mode = JETTY_MODE_SIMPLEX;
    jettyInfo.jetty = args.jetty;

    ost_buffer_info_t src;
    src.addr = args.localAddr;
    src.tseg = args.localSeg;

    ost_buffer_info_t dst;
    dst.addr = args.remoteAddr;
    dst.tseg = args.remoteSeg;

    int ret;
    CALL_OS_XPRT_FUNC(ret, DoSend, osPiplnH2DHandle_, &jettyInfo, &src, &dst, args.len, args.serverKey, args.clientKey,
                      (task_sync **)&info->syncHandle);
    VLOG(1) << "os_transport_send ret: " << ret << " reqId: " << reqId << " remote src " << args.remoteAddr
            << " targetSeg.seg.ubva.va " << args.remoteSeg->seg.ubva.va << " len " << args.len << " segoff "
            << (args.remoteAddr - args.remoteSeg->seg.ubva.va);

    if (ret) {
        return Status(StatusCode::K_RUNTIME_ERROR, "os_transport_send " + std::to_string(reqId) + " failed");
    }
    return Status::OK();
}

Status ChunkManager::WaitPipelineDone()
{
    Status lastError = Status::OK();
    for (auto &info : reqInfos) {
        if (info.second.isCanceled) {
            continue;
        }
        task_sync_t *syncHandle = (task_sync_t *)info.second.syncHandle;
        if (syncHandle) {
            info.second.syncHandle = nullptr;
            int ret;
            CALL_OS_XPRT_FUNC(ret, DoWait, osPiplnH2DHandle_, syncHandle);
            info.second.receivedChunks++;
            if (ret) {
                info.second.failedChunkId++;
                lastError = Status(StatusCode::K_IO_ERROR, "failed to wait pipeline for " + std::to_string(info.first));
                LOG(ERROR) << lastError.GetMsg();
            }
        }
    }
    return lastError;
}

Status ChunkManager::ReceiveLocalChunk(uint32_t reqId, void *srcData, size_t srcSize)
{
    auto it = reqInfos.find(reqId);
    if (it == reqInfos.end()) {
        return Status(StatusCode::K_NOT_FOUND, "[ReceiveLocalChunk] no requestid " + std::to_string(reqId));
    }
    Status ret = SubmitIO(it->second, srcData, srcSize, 0);
    it->second.receivedChunks++;
    if (ret.IsError()) {
        it->second.failedChunkId = 1;
    }
    return ret;
}

Status ChunkManager::SubmitIO(ReqInfo &info, void *srcData, size_t srcSize, size_t destOffset)
{
    if (info.driver->targetSize < destOffset + srcSize) {
        return Status(StatusCode::K_INVALID, "client size is " + std::to_string(info.driver->targetSize) + " but need "
                                                 + std::to_string(((size_t)destOffset + srcSize)));
    }

    return info.driver->SubmitIO(srcData, srcSize, destOffset);
}

Status ChunkManager::WaitAll()
{
    // wait worker2 copy to worker and device
    Status firstError = WaitPipelineDone();
    // wait worker1 write to device
    for (auto &req : reqInfos) {
        Status ret = req.second.driver->WaitIO();
        if (ret.IsError() && firstError.IsOk()) {
            LOG(ERROR) << ret.GetMsg();
            firstError = ret;
        }
    }
    return firstError;
}

Status ChunkManager::ReleaseAll()
{
    Status firstError = WaitAll();
    for (auto &it : reqInfos) {
        Status ret = it.second.driver->Release();
        if (ret.IsError() && firstError.IsOk()) {
            firstError = ret;
        }
    }
    return firstError;
}

void ChunkManager::CancelAll()
{
    for (auto &info : reqInfos) {
        info.second.isCanceled = true;
    }
    canceled = true;
}

void ChunkManager::Cancel(uint32_t reqId)
{
    ReqInfo *info = GetReqInfo(reqId);
    info->isCanceled = true;
}

int ChunkManager::ReceiveEventHook(urma_cr_t *cr)
{
    if (!osPiplnH2DHandle_)
        return 0;
    // return 0 when wake up task successfully
    int ret;
    CALL_OS_XPRT_FUNC(ret, DoNotify, osPiplnH2DHandle_, cr);
    uint32_t low = (uint32_t)cr->user_ctx;
    uint32_t high = (uint32_t)(cr->user_ctx >> 32);
    VLOG(1) << "RH2D:ReceiveEventHook " << ret << " cr.user_ctx high:" << high << " low:" << low;
    if (ret == -1)
        return 0;
    return 1;
}

Status IpcDriver::GetDriver(const DevShmInfo &devInfo, const std::string &targetHandle, bool isClient,
                            std::shared_ptr<IpcDriver> &driver)
{
    switch (devInfo.devType) {
        case TargetDeviceType::MOCK:
            driver = std::make_shared<MockIPC>(devInfo, targetHandle, isClient);
            break;
        case TargetDeviceType::CUDA:
            driver = std::make_shared<CudaIPC>(devInfo, targetHandle, isClient);
            break;
        default:
            return Status(StatusCode::K_NOT_SUPPORTED,
                          "not support H2D device type " + std::to_string(devInfo.devType));
    }

    if (isClient) {
        if (devInfo.ptr)
            return driver->EncodeDriver();
    } else {
        if (!targetHandle.empty())
            return driver->DecodeDriver();
    }
    return Status::OK();
}

Status IpcDriver::Release()
{
    return Status::OK();
}

}  // namespace OsXprtPipln