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

#include "datasystem/common/os_transport_pipeline/cuda_rh2d_driver.h"
#include "datasystem/common/os_transport_pipeline/dlopen_util.h"
#include "datasystem/common/os_transport_pipeline/mock_rh2d_driver.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/util/thread_local.h"
#include "os-transport/os_transport.h"

#define LOG_AND_SET_FIRST_ERROR(code, msg)              \
    do {                                                \
        LOG(ERROR) << (msg);                            \
        if (firstError.IsOk())                          \
            firstError = Status(StatusCode::code, msg); \
    } while (0)

namespace OsXprtPipln {

void BaseRH2DDriver::SetShmFd(int32_t shmId)
{
    devId = static_cast<uint32_t>(shmId);
}
int32_t BaseRH2DDriver::GetShmFd()
{
    return static_cast<int32_t>(devId);
}
void BaseRH2DDriver::SetShmOffset(uint64_t shmOffset)
{
    targetAddr = reinterpret_cast<void *>(shmOffset);
}
uint64_t BaseRH2DDriver::GetShmOffset()
{
    return reinterpret_cast<uint64_t>(targetAddr);
}
void BaseRH2DDriver::SetShmSize(uint64_t shmSize)
{
    targetSize = static_cast<size_t>(shmSize);
}
uint64_t BaseRH2DDriver::GetShmSize()
{
    return static_cast<uint64_t>(targetSize);
}

uint32_t ChunkManager::GenerateReqId()
{
    static constexpr uint32_t kMaxPipelineReqId = (1U << CHUNKTAG_REQID_LEN) - 1;
    uint32_t ret = (++reqId_) & kMaxPipelineReqId;
    if (ret == 0) {
        ret = (++reqId_) & kMaxPipelineReqId;
    }
    return ret;
}

std::atomic<uint32_t> ChunkManager::reqId_ = std::atomic<uint32_t>([]() {
    std::random_device rd;
    return rd() & ((1U << CHUNKTAG_REQID_LEN) - 1);
}());

void *ChunkManager::osPiplnH2DHandle_ = nullptr;
std::mutex ChunkManager::reqIdToChkMgrMapMutex_;
std::map<uint32_t, ChunkManager *> ChunkManager::reqIdToChkMgrMap_;

Status ChunkManager::AddKey(const std::string &key, uint32_t reqId, const DevShmInfo &devInfo, int index)
{
    auto it = reqInfos_.find(reqId);
    if (it != reqInfos_.end()) {
        return Status(StatusCode::K_DUPLICATED, "key " + key
                                                    + " will duplicate, old seqId: " + std::to_string(it->first)
                                                    + " new reqId: " + std::to_string(reqId));
    }

    auto &reqInfo = reqInfos_[reqId];
    VLOG(2) << "Add Rh2d Key " << key << " reqId " << reqId;
    RETURN_IF_NOT_OK(BaseRH2DDriver::GetDriver(reqId, devInfo, isClient_, reqInfo.driver));
    reqInfo.key = key;

    keyToReqIdMap_[key] = reqId;
    if (index != -1)
        indexToReqIdMap_[index] = &reqInfo;

    return Status::OK();
}

void ChunkManager::AddReqIdMap(uint32_t serverReqId, uint32_t clientReqId)
{
    if (reqInfos_.find(serverReqId) == reqInfos_.end()) {
        LOG(WARNING) << "map server " << serverReqId << " -> client " << clientReqId
                     << " failed, worker reqId is not found";
    }
    reqIdMap_[serverReqId] = clientReqId;
}

void ChunkManager::RegisterPipelineConsumer(std::shared_ptr<PipelineRH2DQueueConsumer> &pipelineConsumer)
{
    pipelineConsumer_ = pipelineConsumer;
    auto callback = std::make_shared<PipelineMsgHandler>(
        [this](uint32_t reqId, uint64_t dataSrc, ChunkTag chunkTag, uint32_t chunkSize) {
            std::shared_lock<std::shared_mutex> l(this->releaseMutex, std::try_to_lock);
            if (!l.owns_lock()) {
                LOG(WARNING) << "chunk manager is released, ignore consume chunk ("
                             << ChunkTag::DebugString(chunkTag, chunkSize) << ")";
                return;
            }
            this->DoPiplnStep2_ChunkConsume(reqId, dataSrc, chunkTag, chunkSize);
        });
    for (auto &it : reqInfos_) {
        pipelineConsumer_->AddCallback(it.first, callback);
    }
}

void ChunkManager::DoPiplnStep2_ChunkConsume(uint32_t reqId, uint64_t dataSrc, ChunkTag chunkTag, uint32_t chunkSize)
{
    auto it = reqInfos_.find(reqId);
    if (it == reqInfos_.end()) {
        LOG(WARNING) << "reqId is not registered, ignore consume chunk(" << ChunkTag::DebugString(chunkTag, chunkSize)
                     << ")";
        return;
    }
    auto &info = it->second;
    if (info.IsCanceledOrDone()) {
        LOG(WARNING) << "request is canceled, ignore consume chunk(" << ChunkTag::DebugString(chunkTag, chunkSize)
                     << ")";
        return;
    }

    uint64_t dataOffset;
    if (ChunkTag::IsLastChunk(chunkTag)) {
        info.totalChunks = chunkTag.chunkId + 1;
        dataOffset = info.driver->targetSize - chunkSize;
    } else {
        dataOffset = chunkSize * chunkTag.chunkId;
    }
    info.receivedChunks++;

    dataSrc += dataOffset;
    PIPLN_DEBUG_LOG_DATA_RAW("Before SubmitIO", info.key, reqId, dataSrc, chunkSize);
    Status rc = DoPiplnStep3_SubmitIO(info, reinterpret_cast<void *>(dataSrc), chunkSize, dataOffset);
    LOG_IF_ERROR(rc, "submit io for chunk failed (" + ChunkTag::DebugString(chunkTag, chunkSize) + ")");
    if (rc.IsError()) {
        info.failedChunkId = chunkTag.chunkId;
        MarkCancelOrDone(reqId, false /* isDone */);  // prevent following chunks
    } else if (info.totalChunks == info.receivedChunks) {
        MarkCancelOrDone(reqId, true /* isDone */);  // is done
    } else if (ChunkTag::IsLastChunk(chunkTag) && chunkTag.chunkId == 0) {
        VLOG(1) << "received " << (info.receivedChunks - 1)
                << " chunks from worker2 and worker1 triggered sending one total chunk again";
        /**
         * if some chunk from remote worker2 failed, worker1 may fetch data
         * from other node and trigger local sending total chunk again.
         * hack here for this condition
         */
        info.receivedChunks = 1;
        MarkCancelOrDone(reqId, true /* isDone */);  // is done
    }
}

void ChunkManager::RemoveConsumerCallback(uint32_t reqId)
{
    if (pipelineConsumer_)
        pipelineConsumer_->RemoveCallback(reqId);
}

void ChunkManager::RegisterPipelineProducer(std::shared_ptr<PipelineRH2DQueueProducer> &pipelineProducer,
                                            uint32_t queueId)
{
    pipelineProducer_ = pipelineProducer;
    queueId_ = queueId;
}

Status ChunkManager::DoPiplnStep2_ChunkProduce(const ChunkTag &chunkTag)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pipelineProducer_ != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "pipelineProducer_ is null");

    VLOG(2) << "DoPiplnStep2_ChunkProduce: " << ChunkTag::DebugString(chunkTag);
    if (reqIdMap_.find(chunkTag.reqId) == reqIdMap_.end()) {
        return Status(StatusCode::K_RUNTIME_ERROR,
                      "no client reqId for server reqId " + std::to_string(chunkTag.reqId));
    }

    auto it = reqInfos_.find(chunkTag.reqId);
    if (it == reqInfos_.end()) {
        return Status(StatusCode::K_RUNTIME_ERROR,
                      "reqId for " + std::to_string(chunkTag.reqId) + " is not registered");
    }
    auto &info = it->second;

    if (ChunkTag::IsLastChunk(chunkTag)) {
        info.totalChunks = chunkTag.chunkId + 1;
    }
    info.receivedChunks++;
    const uint64_t objectSize = info.objectSize != 0 ? info.objectSize : info.driver->targetSize;
    const uint32_t actualChunkSize = ChunkTag::ResolveChunkSize(chunkTag, objectSize);

    // change request id from server id to client id
    ChunkTag newTag = chunkTag;
    newTag.reqId = reqIdMap_[chunkTag.reqId];
    // shmfd and shmoffset is set in DoPiplnStep1_StartReceiver
    PipelineRH2DMsg msg{ (int32_t)actualChunkSize, info.driver->GetShmFd(), info.driver->GetShmSize(),
                         info.driver->GetShmOffset(), newTag };
    VLOG(2) << "before ProduceOne serverReqId " << chunkTag.reqId << " clientReqId " << newTag.reqId << " key "
            << info.key;
    Status rc = pipelineProducer_->ProduceOne(queueId_, msg);
    if (rc.IsError()) {
        if (info.failedChunkId == -1) {
            LOG(WARNING) << "produce chunk failed queueId " << queueId_ << " chunk(" << ChunkTag::DebugString(chunkTag)
                         << ") detail:" << rc.GetMsg();
            info.failedChunkId = chunkTag.chunkId;
        }
        MarkCancelOrDone(chunkTag.reqId, false /* isDone */);
    } else if (info.totalChunks == info.receivedChunks) {
        info.doneStep = PIPLN_DONE_TWO_STEP;
        MarkCancelOrDone(chunkTag.reqId, true /* isDone */);
    }
    return rc;
}

Status ChunkManager::GetReqId(const std::string &key, uint32_t &reqId)
{
    auto it = keyToReqIdMap_.find(key);
    if (it == keyToReqIdMap_.end()) {
        return Status(StatusCode::K_NOT_FOUND, "request key " + key + " is not found in ChunkManager");
    } else {
        reqId = it->second;
        return Status::OK();
    }
}

bool ChunkManager::CheckIsRequestSuccess(uint32_t reqId)
{
    auto info = GetReqInfo(reqId);
    bool success = (info->totalChunks != 0 && info->totalChunks == info->receivedChunks && info->failedChunkId == -1);
    if (!success) {
        LOG(WARNING) << "unsuccess request key: " << info->key << " totalChunk:" << info->totalChunks
                     << " receivedChunks:" << info->receivedChunks << " failedChunkId:" << info->failedChunkId;
    }
    return success;
}

int ChunkManager::DoPiplnStep1_ReceiveCallback(void *arg)
{
    ChunkTag tag = ChunkTag::FromUint64(*(uint64_t *)arg);

    // step1: find chunk manager
    reqIdToChkMgrMapMutex_.lock();
    ChunkManager *mgr = nullptr;
    auto it = reqIdToChkMgrMap_.find(tag.reqId);
    if (it == reqIdToChkMgrMap_.end()) {
        LOG(WARNING) << "no reqId to ChunkManager map, ignore produce chunk (" << ChunkTag::DebugString(tag) << ")";
        reqIdToChkMgrMapMutex_.unlock();
        return -1;
    }
    mgr = it->second;

    // step2: produce one with release lock, ignore it if released
    {
        std::shared_lock<std::shared_mutex> l(mgr->releaseMutex, std::try_to_lock);
        reqIdToChkMgrMapMutex_.unlock();
        if (!l.owns_lock()) {
            LOG(WARNING) << "chunk manager maybe released, ignore produce chunk (" << ChunkTag::DebugString(tag) << ")";
            return -1;
        }
        Status rc = mgr->DoPiplnStep2_ChunkProduce(tag);
        if (rc.IsError()) {
            LOG_IF_ERROR(rc, "produce chunk failed for " + ChunkTag::DebugString(tag));
            return -1;
        }
    }

    return 0;
}

Status ChunkManager::DoPiplnStep1_StartReceiver(uint32_t reqId, uint64_t dataSrc, uint64_t size,
                                                urma_target_seg_t *targetSeg, urma_jfr_t *targetJfr,
                                                urma_jetty_t *targetJetty, int32_t shmFd, uint64_t shmSize,
                                                uint64_t shmOffset)
{
    ReqInfo *info = GetReqInfo(reqId);
    if (!info) {
        return Status(StatusCode::K_NOT_FOUND, "reqId " + std::to_string(reqId) + " is not registered in ChunkManager");
    }

    ost_buffer_info_t hostSrc;
    hostSrc.addr = dataSrc;
    hostSrc.tseg = targetSeg;

    ost_device_info_t deviceInfo = {};
    deviceInfo.jfr = targetJfr;
    deviceInfo.jetty = targetJetty;

    info->objectSize = size;
    info->driver->SetShmFd(shmFd);
    info->driver->SetShmSize(shmSize);
    info->driver->SetShmOffset(shmOffset);
    int ret;
    {
        std::lock_guard<std::mutex> l(reqIdToChkMgrMapMutex_);
        reqIdToChkMgrMap_[reqId] = this;
    }
    CALL_OS_XPRT_FUNC(ret, DoRecv, osPiplnH2DHandle_, &hostSrc, &deviceInfo, size, reqId,
                      (task_sync **)&info->syncHandle, DoPiplnStep1_ReceiveCallback);
    VLOG(2) << "os_transport_recv ret: " << ret << " reqId: " << reqId << " dataSrc " << dataSrc
            << " targetSeg.seg.ubva.va " << targetSeg->seg.ubva.va << " len " << size << " segoff "
            << (dataSrc - targetSeg->seg.ubva.va);
    if (ret != 0) {
        // skip pipeline step 1, wait for worker2 urma write done and start pipeline step 2
        return Status(StatusCode::K_RUNTIME_ERROR, "os_transport_recv start failed for " + std::to_string(reqId));
    }

    return Status::OK();
}

ReqInfo *ChunkManager::GetReqInfo(uint32_t reqId)
{
    auto it = reqInfos_.find(reqId);
    if (it == reqInfos_.end()) {
        return nullptr;
    }
    return &it->second;
}

ReqInfo *ChunkManager::GetReqInfoByIndex(int32_t keyIndex)
{
    return indexToReqIdMap_[keyIndex];
}

static void OsLogCallback(int level, const char *msg)
{
    datasystem::LogMessage((datasystem::LogSeverity)level, __FILE__, __LINE__).Stream() << msg;
}

Status ChunkManager::InitOsPiplnRH2DEnv(urma_context_t *ctx, urma_jfc_t *jfc, urma_jfce_t *jfce, uint32_t jettySize,
                                        int threadNum, bool needCuda)
{
    os_transport_cfg_t cfg;
    memset_s(&cfg, sizeof(cfg), 0, sizeof(cfg));
    cfg.worker_thread_num = threadNum;
    cfg.urma_event_mode = true;
    cfg.jfce = jfce;
    cfg.jfc = jfc;
    cfg.recv_queue_capacity = jettySize;

    DO_LOAD_DYNLIB(OsTransportLibLoader);
    if (!IS_VALID_DYNFUNC(OsTransportLibLoader, DoInit))
        return Status(StatusCode::K_NOT_FOUND, "failed to load OsTransportLibLoader");

    if (needCuda) {
#ifndef PIPLN_USE_MOCK
        DO_LOAD_DYNLIB(CudaRTLibLoader);
        if (!IS_VALID_DYNFUNC(CudaRTLibLoader, cudaSetDevice)) {
            DO_UNLOAD_DYNLIB(OsTransportLibLoader);
            return Status(StatusCode::K_NOT_FOUND, "failed to load CudaRTLibLoader");
        }

        Status loadRet = CudaRH2DDriver::LoadGpuIds();
        if (loadRet.IsError()) {
            LOG(ERROR) << "failed to LoadGpuIds:" + loadRet.GetMsg();
        } else {
            for (auto &it : CudaRH2DDriver::GetDevIdMap()) {
                LOG(INFO) << "find gpu id " << it.second << " uuid: " << it.first;
            }
        }
#endif
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

    return Status::OK();
}

void ChunkManager::UnInitOsPiplnRH2DEnv()
{
    if (!osPiplnH2DHandle_)
        return;
    int ret;
    CALL_OS_XPRT_FUNC(ret, DoDestroy, osPiplnH2DHandle_);
    osPiplnH2DHandle_ = nullptr;
    DO_UNLOAD_DYNLIB(OsTransportLibLoader);
    DO_UNLOAD_DYNLIB(CudaRTLibLoader);
}

Status ChunkManager::DoPiplnStep1_StartSender(PiplnSndArgs &args)
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

Status ChunkManager::WaitPiplnStep12Done()
{
    Status firstError = Status::OK();
    for (auto &info : reqInfos_) {
        int64_t remainingTimeMs = datasystem::reqTimeoutDuration.CalcRealRemainingTime();
        if (isClient_) {
            // client wait step 2 cancel or done
            bool done = info.second.WaitCancelOrDone(remainingTimeMs);
            if (!done && !info.second.IsCanceled()) {
                MarkCancelOrDone(info.first, false /* isDone */);
                LOG_AND_SET_FIRST_ERROR(K_RPC_DEADLINE_EXCEEDED,
                                        "pipeline rh2d timeout while waiting client notify queue for request "
                                            + std::to_string(info.first) + ", key=" + info.second.key);
            }
        } else {
            // worker wait step 1,2 done
            task_sync_t *syncHandle = (task_sync_t *)info.second.syncHandle;
            if (syncHandle) {
                info.second.syncHandle = nullptr;
                int ret;
                if (IS_VALID_DYNFUNC(OsTransportLibLoader, DoWaitTimeout)) {
                    CALL_OS_XPRT_FUNC(ret, DoWaitTimeout, osPiplnH2DHandle_, syncHandle, remainingTimeMs);
                } else {
                    LOG(WARNING)
                        << "wait_and_free_sync_timeout is not found, fallback to wait_and_free_sync without timeout";
                    CALL_OS_XPRT_FUNC(ret, DoWait, osPiplnH2DHandle_, syncHandle);
                }
                if (ret == 0) {
                    MarkCancelOrDone(info.first, true /* isDone */);
                } else {
                    MarkCancelOrDone(info.first, false /* isDone */);
                    LOG_AND_SET_FIRST_ERROR(
                        K_RPC_DEADLINE_EXCEEDED,
                        "pipeline rh2d timeout or failed while waiting os_transport tasks for request "
                            + std::to_string(info.first) + ", key=" + info.second.key + ", ret=" + std::to_string(ret));
                }
            }
        }
    }
    return firstError;
}

Status ChunkManager::DoPiplnStep2_ProduceLocalChunk(uint32_t reqId, int32_t shmFd, uint64_t shmSize, uint64_t shmOffset,
                                                    size_t srcSize)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(pipelineProducer_ != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "pipelineProducer_ is null");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        reqIdMap_.find(reqId) != reqIdMap_.end(), StatusCode::K_RUNTIME_ERROR,
        "server reqId " + std::to_string(reqId) + " has no client reqId map, ignore this chunk.");
    auto clientReqId = reqIdMap_[reqId];

    auto it = reqInfos_.find(reqId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(it != reqInfos_.end(), StatusCode::K_NOT_FOUND,
                                         "no requestid " + std::to_string(reqId));
    auto &info = it->second;

    // local chunk produce maybe triggered before
    if (info.doneStep == PIPLN_DONE_TWO_STEP)
        return Status::OK();

    info.totalChunks = 1;
    info.receivedChunks = 1;

    ChunkTag tag;
    tag.reqId = clientReqId;
    tag.chunkId = 0;
    ChunkTag::SetIsLastChunk(tag);
    info.driver->SetShmFd(shmFd);
    info.driver->SetShmSize(shmSize);
    info.driver->SetShmOffset(shmOffset);
    VLOG(1) << "DoPiplnStep2_ProduceLocalChunk: " << ChunkTag::DebugString(tag, info.driver->GetShmSize())
            << " serverReqId " << reqId << " key " << info.key;
    PipelineRH2DMsg msg{ (int32_t)srcSize, info.driver->GetShmFd(), info.driver->GetShmSize(),
                         info.driver->GetShmOffset(), tag };
    Status ret = pipelineProducer_->ProduceOne(queueId_, msg);
    if (ret.IsError()) {
        info.failedChunkId = 0;
        LOG_IF_ERROR(ret, "failed to produce local chunk for " + std::to_string(reqId) + " client reqId "
                              + std::to_string(clientReqId));
        MarkCancelOrDone(reqId, false /* isDone */);
    } else {
        info.doneStep = PIPLN_DONE_TWO_STEP;
        MarkCancelOrDone(reqId, true /* isDone */);
    }
    return ret;
}

Status ChunkManager::DoPiplnStep3_SubmitIO(ReqInfo &info, void *srcData, size_t srcSize, size_t destOffset)
{
    if (info.driver->targetSize < destOffset + srcSize) {
        return Status(StatusCode::K_INVALID, "client size is " + std::to_string(info.driver->targetSize) + " but need "
                                                 + std::to_string(((size_t)destOffset + srcSize)));
    }

    return info.driver->SubmitIO(srcData, srcSize, destOffset);
}

Status ChunkManager::WaitAll()
{
    if (allDone_)
        return Status::OK();
    // wait step1: worker2 write to worker1
    // wait step2: worker1 write to pipeline queue
    Status firstError = WaitPiplnStep12Done();
    // wait step3: client write to device
    for (auto &req : reqInfos_) {
        Status ret = req.second.driver->WaitIO();
        if (ret.IsError()) {
            LOG_AND_SET_FIRST_ERROR(K_IO_ERROR, req.second.key + " IO failed:" + ret.GetMsg());
        } else {
            (void)CheckIsRequestSuccess(req.first);
        }
    }
    allDone_ = true;
    return firstError;
}

void ChunkManager::DebugPrintAllPipelineStatus()
{
    for (auto &it : reqInfos_) {
        LOG(ERROR) << "reqId " << it.first << " " << it.second.DebugString();
    }
}

Status ChunkManager::ReleaseAll()
{
    {
        std::lock_guard<std::mutex> l(reqIdToChkMgrMapMutex_);
        for (auto &it : reqInfos_) {
            reqIdToChkMgrMap_.erase(it.first);
        }
    }
    std::unique_lock<std::shared_mutex> l(releaseMutex);
    Status firstError = WaitAll();
    for (auto &it : reqInfos_) {
        RemoveConsumerCallback(it.first);
        Status ret = it.second.driver->Release();
        if (ret.IsError() && firstError.IsOk()) {
            firstError = ret;
        }
    }
    indexToReqIdMap_.clear();
    return firstError;
}

void ChunkManager::CancelAll()
{
    for (auto &info : reqInfos_) {
        MarkCancelOrDone(info.first, false /* isDone */);
    }
}

void ChunkManager::MarkCancelOrDone(const std::string &key, bool isDone)
{
    uint32_t reqId;
    Status rc = GetReqId(key, reqId);
    if (rc.IsOk())
        MarkCancelOrDone(reqId, isDone);
}

void ChunkManager::MarkCancelOrDone(uint32_t reqId, bool isDone)
{
    ReqInfo *info = GetReqInfo(reqId);
    if (reqIdMap_.find(reqId) == reqIdMap_.end()) {
        VLOG(1) << (isDone ? "done " : "cancel") << " key " << info->key << " reqId " << reqId << " receive Chunk "
                << info->receivedChunks;
    } else {
        VLOG(1) << (isDone ? "done " : "cancel") << " key " << info->key << " reqId " << reqId << " clientReqId "
                << reqIdMap_[reqId] << " receive Chunk " << info->receivedChunks;
    }
    {
        std::lock_guard<std::mutex> lock(info->promiseMutex);
        if (info->IsCanceledOrDone())
            return;
        if (isDone) {
            info->Done();
        } else {
            uint32_t ret;
            CALL_OS_XPRT_FUNC(ret, DoCancel, osPiplnH2DHandle_, &info->syncHandle, reqId);
            info->Cancel();
        }
    }
}

bool ChunkManager::DoPiplnStep1_ReceiveUrmaEventHook(urma_cr_t *cr)
{
    if (!osPiplnH2DHandle_)
        return false;
    // return 0 when wake up task successfully
    int ret;
    CALL_OS_XPRT_FUNC(ret, DoNotify, osPiplnH2DHandle_, cr);
    uint32_t low = (uint32_t)cr->user_ctx;
    uint32_t high = (uint32_t)(cr->user_ctx >> 32);
    VLOG(1) << "RH2D:DoPiplnStep1_ReceiveUrmaEventHook " << ret << " cr.user_ctx high:" << high << " low:" << low;
    if (ret == -1)
        return false;
    return true;
}

Status BaseRH2DDriver::GetDriver(const uint32_t reqId, const DevShmInfo &devInfo, bool isClient,
                                 std::shared_ptr<BaseRH2DDriver> &driver)
{
#ifdef PIPLN_USE_MOCK
    const_cast<DevShmInfo &>(devInfo).devType = TargetDeviceType::MOCK;
#endif
    switch (devInfo.devType) {
        case TargetDeviceType::MOCK:
            driver = std::make_shared<MockRH2DDriver>(devInfo, isClient);
            std::static_pointer_cast<MockRH2DDriver>(driver)->SetReqId(reqId);
            break;
        case TargetDeviceType::CUDA:
            driver = std::make_shared<CudaRH2DDriver>(devInfo, isClient);
            break;
        default:
            return Status(StatusCode::K_NOT_SUPPORTED,
                          "not support H2D device type " + std::to_string(devInfo.devType));
    }

    return driver->Init();
}

void ReqInfo::Cancel()
{
    canceledOrDonePromise.set_value(false);
}

void ReqInfo::Done()
{
    canceledOrDonePromise.set_value(true);
}

bool ReqInfo::WaitCancelOrDone()
{
    return canceledOrDoneFuture.get();
}

bool ReqInfo::WaitCancelOrDone(int64_t timeoutMs)
{
    if (timeoutMs < 0) {
        return WaitCancelOrDone();
    }
    auto waitStatus = canceledOrDoneFuture.wait_for(std::chrono::milliseconds(timeoutMs));
    if (waitStatus != std::future_status::ready) {
        return false;
    }
    return canceledOrDoneFuture.get();
}

bool ReqInfo::IsCanceledOrDone()
{
    return (canceledOrDoneFuture.wait_for(std::chrono::seconds(0)) == std::future_status::ready);
}

bool ReqInfo::IsCanceled()
{
    return IsCanceledOrDone() && (WaitCancelOrDone() == false);
}

std::string ReqInfo::DebugString()
{
    std::stringstream ss;
    ss << key << " receive " << receivedChunks << "/" << totalChunks << " failedChunk " << failedChunkId
       << " objectSize: " << objectSize << " syncHandle: " << syncHandle << " IsCanceledOrDone: " << IsCanceledOrDone();
    return ss.str();
}

Status BaseRH2DDriver::Release()
{
    return Status::OK();
}

}  // namespace OsXprtPipln
