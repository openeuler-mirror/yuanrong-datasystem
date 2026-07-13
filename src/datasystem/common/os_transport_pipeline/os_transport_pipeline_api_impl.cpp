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
#include <set>
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/os_transport_pipeline/cuda_rh2d_driver.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"

namespace OsXprtPipln {

static constexpr int MIN_PIPLN_THREAD_NUM = 8;
static constexpr int MAX_PIPLN_THREAD_NUM = 128;
static constexpr int DEFAULT_PIPLN_THREAD_NUM = 64;

using namespace datasystem;

static std::shared_ptr<PipelineRH2DQueueProducer> gQueueProducer = std::make_shared<PipelineRH2DQueueProducer>();
static bool g_isClientMode = false;
Status SetIsClientMode(bool clientMode)
{
    g_isClientMode = clientMode;
    return Status::OK();
}

Status ParsePiplnH2DRequest(const GetReqPb &req, H2DChunkManager &mgr, const std::string &objectKey, int infoIdx,
                            int32_t pipelineQueueId)
{
    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    uint32_t clntReqId = req.pipeline_rh2d_reqids(static_cast<int>(infoIdx));
    DevShmInfo devShmInfo{ .devType = TargetDeviceType::CUDA,
                           .devId = (uint32_t)-1,
                           .ptr = nullptr, /* pointer is inited in AddKey */
                           .size = 0 };
    uint32_t workerReqId = (uint32_t)(GenerateReqId() & URMA_REQID_MASK);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(mgr.AddKey(objectKey, workerReqId, devShmInfo, infoIdx),
                                     PIPLN_LOG_PREFIX " objectKey is " + objectKey);
    mgr.AddReqIdMap(workerReqId, clntReqId);
    mgr.RegisterPipelineProducer(gQueueProducer, pipelineQueueId);
    return Status::OK();
}

void StopPipelineRH2D(H2DChunkManager &mgr, GetRspPb::ObjectInfoPb &object, const std::string &key, bool isOk)
{
    if (!mgr.KeyNum()) {
        return;
    }

    uint32_t reqId;
    mgr.GetReqId(key, reqId);
    ReqInfo *info = mgr.GetReqInfo(reqId);
    if (isOk) {
        info->WaitCancelOrDone(datasystem::reqTimeoutDuration.CalcRealRemainingTime());
    } else {
        mgr.MarkCancelOrDone(reqId, false /* isDone */);
    }

    object.set_pipeline_done_step(info->doneStep);
}

void StopPipelineRH2D(H2DChunkManager &mgr, GetRspPb::ObjectInfoPb &object, size_t index, bool isOk)
{
    if (!mgr.KeyNum()) {
        return;
    }

    ReqInfo *info = mgr.GetReqInfoByIndex(index);
    uint32_t reqId;
    mgr.GetReqId(info->key, reqId);
    if (isOk) {
        info->WaitCancelOrDone(datasystem::reqTimeoutDuration.CalcRealRemainingTime());
    } else {
        mgr.MarkCancelOrDone(reqId, false /* isDone */);
    }

    object.set_pipeline_done_step(info->doneStep);
}

void StopPipelineRH2D(H2DChunkManager &mgr, const std::string &key)
{
    if (!mgr.KeyNum()) {
        return;
    }

    uint32_t reqId;
    mgr.GetReqId(key, reqId);
    mgr.MarkCancelOrDone(reqId, false /* isDone */);
}

void StopPipelineRH2D(H2DChunkManager &mgr, size_t index)
{
    if (!mgr.KeyNum()) {
        return;
    }

    uint32_t reqId;
    ReqInfo *info = mgr.GetReqInfoByIndex(index);
    mgr.GetReqId(info->key, reqId);
    mgr.MarkCancelOrDone(reqId, false /* isDone */);
}

Status WaitPipelineRH2DDone(H2DChunkManager &mgr)
{
    if (!mgr.KeyNum()) {
        return Status::OK();
    }

    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    PerfPoint point(PerfKey::PIPLN_RH2D_WORKER_WAIT_DONE);
    Timer timer;
    Status rc = mgr.WaitAll();
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    auto rpcThresholdUs = GetServerLatencyTraceConfig().rpcSlowerThanUs;
    SLOW_LOG_IF_OR_VLOG(INFO, rpcThresholdUs > 0 && elapsedUs >= rpcThresholdUs, 1,
                        "[PIPLN RH2D] worker wait done, keyCount: " << mgr.KeyNum() << ", costUs: " << elapsedUs
                                                                    << ", status: " << rc.ToString());
    return rc;
}
#undef ADD_FAILED_KEY
#undef UPDATE_LAST_STATUS

Status TriggerLocalPipelineRH2D(H2DChunkManager &mgr, const std::string &objectKey, std::shared_ptr<ShmUnit> shmUnit,
                                uint64_t dataOffset, uint64_t dataSize)
{
    if (!mgr.KeyNum()) {
        return Status::OK();
    }

    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    PerfPoint point(PerfKey::PIPLN_RH2D_WORKER_TRIGGER_LOCAL);
    Timer timer;
    uint32_t reqId;
    RETURN_IF_NOT_OK(mgr.GetReqId(objectKey, reqId));
    PIPLN_DEBUG_LOG_DATA("TriggerLocalPipelineRH2D", objectKey, reqId, shmUnit, dataOffset, dataSize);
    Status rc = mgr.DoPiplnStep2_ProduceLocalChunk(reqId, shmUnit->GetFd(), shmUnit->GetMmapSize(),
                                                   shmUnit->GetOffset() + dataOffset, dataSize);
    VLOG(2) << PIPLN_LOG_PREFIX " Local data path: key=" << objectKey << ", reqId=" << reqId << ", size=" << dataSize;
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    auto processThresholdUs = GetServerLatencyTraceConfig().processSlowerThanUs;
    SLOW_LOG_IF_OR_VLOG(INFO, processThresholdUs > 0 && elapsedUs >= processThresholdUs, 1,
                        "[PIPLN RH2D] worker trigger local done, reqId: " << reqId << ", dataSize: " << dataSize
                                                                          << ", costUs: " << elapsedUs
                                                                          << ", status: " << rc.ToString());
    RETURN_IF_NOT_OK(rc);

    return Status::OK();
}

Status TriggerRemotePipelineRH2D(H2DChunkManager &mgr, const std::string &key, uint64_t dataOffset, uint64_t size,
                                 std::shared_ptr<ShmUnit> shmUnit, const std::string &remoteAddress,
                                 GetObjectRemoteReqPb &subReq)
{
    if (!mgr.KeyNum()) {
        return Status::OK();
    }

    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();
    PerfPoint point(PerfKey::PIPLN_RH2D_WORKER_TRIGGER_REMOTE);
    uint32_t reqId;
    RETURN_IF_NOT_OK(mgr.GetReqId(key, reqId));

    // get seg
    uint64_t segAddress;
    uint64_t segSize;
    urma_target_seg_t *targetSeg = nullptr;
    urma_jfr_t *targetJfr = nullptr;
    urma_jetty_t *targetJetty = nullptr;
    uint64_t pointer = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
    uint64_t dataSrc = pointer + dataOffset;  // dataOffset = MetaSize + readOffset( always 0 )
    uint64_t totalOffset = shmUnit->GetOffset() + dataOffset;
    GetSegmentInfoFromShmUnit(shmUnit, pointer, segAddress, segSize);
    RETURN_IF_NOT_OK(
        UrmaManager::Instance().GetTargetSeg(segAddress, segSize, remoteAddress, &targetSeg, &targetJfr, &targetJetty));

    PIPLN_DEBUG_LOG_DATA("Before StartReceiver", key, reqId, shmUnit, dataOffset, size);
    // start Receiver
    Timer receiverTimer;
    Status receiverRc = Status::OK();
    {
        PerfPoint receiverPoint(PerfKey::PIPLN_RH2D_START_RECEIVER);
        receiverRc = mgr.DoPiplnStep1_StartReceiver(reqId, dataSrc, size, targetSeg, targetJfr, targetJetty,
                                                    shmUnit->GetFd(), shmUnit->GetMmapSize(), totalOffset);
    }
    const auto receiverUs = static_cast<uint64_t>(receiverTimer.ElapsedMicroSecond());
    auto processThresholdUs = GetServerLatencyTraceConfig().processSlowerThanUs;
    SLOW_LOG_IF_OR_VLOG(INFO, processThresholdUs > 0 && receiverUs >= processThresholdUs, 1,
                        "[PIPLN RH2D] worker start receiver done, reqId: "
                            << reqId << ", dataSize: " << size << ", remoteAddress: " << remoteAddress
                            << ", costUs: " << receiverUs << ", status: " << receiverRc.ToString());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(receiverRc, PIPLN_LOG_PREFIX "failed to start receiver");
    VLOG(2) << PIPLN_LOG_PREFIX " Remote data path: key=" << key << ", reqId=" << reqId << ", size=" << size
            << ", remote=" << remoteAddress;

    // set field to trigger sender
    subReq.mutable_urma_info()->set_pipeline_rh2d_req_id(reqId);

    return Status::OK();
}

Status InitOsPiplnRH2DEnv(void *ctx, void *jfc, void *jfce, uint32_t jettySize)
{
    int actualThreadNum;

    if (!SupportPipelineRH2D()) {
        return Status::OK();
    }
    if (g_isClientMode) {
        actualThreadNum = 0;  // client should not have pipeline worker thread, so init with 0 thread
    } else if (FLAGS_pipeline_h2d_thread_num < MIN_PIPLN_THREAD_NUM
               || FLAGS_pipeline_h2d_thread_num > MAX_PIPLN_THREAD_NUM) {
        actualThreadNum = DEFAULT_PIPLN_THREAD_NUM;
        LOG(WARNING) << PIPLN_LOG_PREFIX " Invalid thread num: pipeline_h2d_thread_num="
                     << FLAGS_pipeline_h2d_thread_num << ", use default=" << actualThreadNum;
    } else {
        actualThreadNum = FLAGS_pipeline_h2d_thread_num;
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        H2DChunkManager::InitOsPiplnRH2DEnv((urma_context_t *)ctx, (urma_jfc_t *)jfc, (urma_jfce_t *)jfce, jettySize,
                                            actualThreadNum, g_isClientMode),
        PIPLN_LOG_PREFIX " failed to init os pipeline env");
    return Status::OK();
}

bool PiplnH2DRecvEventHook(void *cr)
{
    if (!SupportPipelineRH2D()) {
        return false;
    }
    return ChunkManager::DoPiplnStep1_ReceiveUrmaEventHook((urma_cr_t *)cr);
}

Status DoPiplnStep1_StartSender(PiplnSndArgs &args)
{
    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    PerfPoint point(PerfKey::PIPLN_RH2D_START_SENDER);
    ChunkManager mgr{ false /* isClient */ };
    DevShmInfo dev{
        .devType = TargetDeviceType::CUDA,
        .devId = 0,
        .ptr = nullptr,
        .size = 0,
    };
    mgr.AddKey("", args.clientKey, dev);
    PIPLN_DEBUG_LOG_DATA_RAW("Before StartSender", "unkwon", args.clientKey, args.localAddr, args.len);
    VLOG(2) << PIPLN_LOG_PREFIX " Worker2 sending: key=" << args.clientKey << ", server key=" << args.serverKey
            << ", size=" << args.len;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(mgr.DoPiplnStep1_StartSender(args), "failed to start sender");
    point.RecordAndReset(PerfKey::PIPLN_RH2D_SENDER_WAIT_DONE);
    Timer waitTimer;
    Status waitRc = mgr.WaitAll();
    const auto waitUs = static_cast<uint64_t>(waitTimer.ElapsedMicroSecond());
    auto rpcThresholdUs = GetServerLatencyTraceConfig().rpcSlowerThanUs;
    SLOW_LOG_IF_OR_VLOG(INFO, rpcThresholdUs > 0 && waitUs >= rpcThresholdUs, 1,
                        "[PIPLN RH2D] sender wait done, clientReqId: " << args.clientKey << ", dataSize: " << args.len
                                                                       << ", costUs: " << waitUs
                                                                       << ", status: " << waitRc.ToString());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(waitRc, "failed to wait send done");
    return Status::OK();
}

bool IsPiplnH2DRequest(const GetReqPb &req)
{
    return req.pipeline_rh2d_reqids_size() != 0;
}

bool IsPiplnH2DRequest(const UrmaRemoteAddrPb &urmaInfo)
{
    return urmaInfo.has_pipeline_rh2d_req_id();
}

bool IsPiplnH2DRequest(const BatchGetObjectRemoteReqPb &req)
{
    for (int i = 0; i < req.requests_size(); ++i) {
        const auto &subReq = req.requests(i);
        if (subReq.has_urma_info() && subReq.urma_info().has_pipeline_rh2d_req_id()) {
            return true;
        }
    }
    return false;
}

bool IsPiplnH2DRequest(const H2DChunkManager &mgr)
{
    return (mgr.KeyNum() != 0);
}

void UnInitOsPiplnRH2DEnv()
{
    ChunkManager::UnInitOsPiplnRH2DEnv();
}

Status MaybeTriggerLocalPipelineRH2D(H2DChunkManager &mgr, const std::string &key, std::shared_ptr<ShmUnit> shmUnit,
                                     uint64_t dataOffset, uint64_t dataSize)
{
    if (!mgr.KeyNum()) {
        return Status::OK();
    }

    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();
    uint32_t reqId;
    RETURN_IF_NOT_OK(mgr.GetReqId(key, reqId));
    ReqInfo *reqInfo = mgr.GetReqInfo(reqId);

    /**
     * PIPLN_DONE_ONE_STEP: worker2 data has been successfully send to worker1 by pipeline
     * PIPLN_DONE_TWO_STEP: worker1 has received all chunks from worker2, or worker triggered
     *                      sending local chunk.
     */
    // maybe already get from remote
    if (reqInfo->doneStep >= PIPLN_DONE_ONE_STEP)
        return Status::OK();

    // switch remote pipeline to local pipeline
    PerfPoint point(PerfKey::PIPLN_RH2D_WORKER_MAYBE_TRIGGER_LOCAL);
    Timer timer;
    mgr.MarkCancelOrDone(reqId, false /* isDone */);
    PIPLN_DEBUG_LOG_DATA("MaybeTriggerLocalPipelineRH2D", key, reqId, shmUnit, dataOffset, dataSize);
    Status rc = mgr.DoPiplnStep2_ProduceLocalChunk(reqId, shmUnit->GetFd(), shmUnit->GetMmapSize(),
                                                   shmUnit->GetOffset() + dataOffset, dataSize);
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    auto processThresholdUs = GetServerLatencyTraceConfig().processSlowerThanUs;
    SLOW_LOG_IF_OR_VLOG(INFO, processThresholdUs > 0 && elapsedUs >= processThresholdUs, 1,
                        "[PIPLN RH2D] worker maybe trigger local done, reqId: " << reqId << ", dataSize: " << dataSize
                                                                                << ", costUs: " << elapsedUs
                                                                                << ", status: " << rc.ToString());
    RETURN_IF_NOT_OK(rc);

    return Status::OK();
}

Status HoldOnePiplnRH2DQueue(uint32_t &queueId)
{
    if (!SupportPipelineRH2D()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(gQueueProducer->HoldAvailableQueue(queueId),
                                     "[PIPLN RH2D] hold one pipeline queue failed");
    VLOG(1) << "[PIPLN RH2D] HoldAvailableQueue: queueId=" << queueId;
    return Status::OK();
}

Status ReleaseAvailableQueue(uint32_t queueId)
{
    if (!SupportPipelineRH2D()) {
        return Status::OK();
    }
    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();
    VLOG(1) << PIPLN_LOG_PREFIX " ReleaseAvailableQueue: queueId=" << queueId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(gQueueProducer->ReleaseAvailableQueue(queueId),
                                     PIPLN_LOG_PREFIX " release pipeline queue failed");
    return Status::OK();
}

static Status SetPiplnDataShmInfo(RegisterClientRspPb &resp, const std::string &tenantId)
{
    auto allocator = memory::Allocator::Instance();
    auto arenaManager = allocator->GetArenaManager();
    if (arenaManager == nullptr) {
        return Status::OK();
    }

    std::set<int> addedFds;
    std::vector<memory::ArenaGroupKey> keys{ { DEFAULT_TENANT_ID, memory::CacheType::MEMORY } };
    if (tenantId != DEFAULT_TENANT_ID) {
        keys.emplace_back(memory::ArenaGroupKey{ tenantId, memory::CacheType::MEMORY });
    }

    for (const auto &key : keys) {
        std::shared_ptr<memory::ArenaGroup> arenaGroup;
        Status rc = arenaManager->GetArenaGroup(key, arenaGroup);
        if (rc.IsError() || arenaGroup == nullptr) {
            LOG(WARNING) << PIPLN_LOG_PREFIX " Skip shm query: tenant=" << key.tenantId << ", error=" << rc.ToString();
            continue;
        }

        auto fds = arenaGroup->GetAllFds();
        for (int fd : fds) {
            if (fd <= 0 || addedFds.count(fd) != 0) {
                continue;
            }
            std::pair<void *, uint64_t> ptrMmapSz;
            Status rc = allocator->FdToPointer(key, fd, ptrMmapSz);
            if (rc.IsError()) {
                LOG(WARNING) << PIPLN_LOG_PREFIX " Query data shm failed: fd=" << fd << ", tenant=" << key.tenantId
                             << ", error=" << rc.ToString();
                continue;
            }
            if (ptrMmapSz.second == 0) {
                continue;
            }
            auto *info = resp.add_pipeline_data_shm_infos();
            info->set_shm_fd(fd);
            info->set_mmap_size(ptrMmapSz.second);
            addedFds.emplace(fd);
        }
    }

    LOG(INFO) << PIPLN_LOG_PREFIX " RegisterClient returns " << addedFds.size() << " shm fds for cudaHostRegister";
    return Status::OK();
}

Status SetPiplnQueueShmInfo(RegisterClientRspPb &resp, uint32_t queueId, const std::string &tenantId)
{
    auto info = resp.mutable_pipeline_queue_info();
    if (queueId == INVALID_PIPLN_QUEUE_ID || !SupportPipelineRH2D()) {
        info->set_shm_fd(-1);
        return Status::OK();
    }

    int shmFd;
    ptrdiff_t offset;
    size_t mmapSize;
    std::string shmId;

    Status ret = gQueueProducer->GetQueueShmInfo(queueId, shmFd, offset, mmapSize, shmId);
    if (ret.IsOk()) {
        info->set_shm_fd(shmFd);
        info->set_offset(offset);
        info->set_mmap_size(mmapSize);
        info->set_shm_id(shmId);
        LOG(INFO) << PIPLN_LOG_PREFIX " RegisterClient assigns queueId=" << queueId;
        SetPiplnDataShmInfo(resp, tenantId);
    } else {
        LOG(ERROR) << PIPLN_LOG_PREFIX " SetPiplnQueueShmInfo failed: " << ret.GetMsg();
    }
    return Status::OK();
}

Status MarkPipelineStep1Ok(H2DChunkManager &mgr, const std::string &key)
{
    if (!SupportPipelineRH2D()) {
        return Status::OK();
    }
    RETURN_IF_NOT_SUPPORT_PIPLN_H2D();

    uint32_t reqId;
    RETURN_IF_NOT_OK(mgr.GetReqId(key, reqId));
    ReqInfo *reqInfo = mgr.GetReqInfo(reqId);
    // for three step pipeline, reqInfo->syncHandle is not nullptr
    if (reqInfo->doneStep == PIPLN_DONE_NO_STEP && reqInfo->syncHandle)
        reqInfo->doneStep = PIPLN_DONE_ONE_STEP;
    VLOG(1) << key << PIPLN_LOG_PREFIX " MarkPipelineStep1Ok reqInfo->doneStep " << reqInfo->doneStep;

    return Status::OK();
}

}  // namespace OsXprtPipln
