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

#ifndef OS_XPRT_PIPLN_CHUNK_MANAGER
#define OS_XPRT_PIPLN_CHUNK_MANAGER

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <ub/umdk/urma/urma_api.h>

#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_types.h"
#include "datasystem/common/os_transport_pipeline/pipeline_notify_queue.h"
#include "datasystem/common/util/dyn_bitmap.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
namespace OsXprtPipln {

using datasystem::DummyRWLock;
using datasystem::DynBitmap;
using datasystem::Status;
using datasystem::StatusCode;

class BaseRH2DDriver {
public:
    BaseRH2DDriver(const DevShmInfo &devInfo, bool isClient)
        : devId(devInfo.devId),
          targetAddr(devInfo.ptr),
          targetSize(devInfo.size),
          targetType(devInfo.devType),
          isClient(isClient)
    {
    }
    virtual ~BaseRH2DDriver()
    {
    }
    virtual Status Init() = 0;
    virtual Status SubmitIO(void *srcData, size_t srcSize, size_t destOffset) = 0;
    virtual Status WaitIO() = 0;
    virtual Status Release() = 0;
    virtual Status Cancel()
    {
        return Status::OK();
    };
    static Status GetDriver(const uint32_t reqId, const DevShmInfo &devInfo, bool isClient,
                            std::shared_ptr<BaseRH2DDriver> &driver);

    void SetShmFd(int32_t shmId);
    int32_t GetShmFd();
    void SetShmOffset(uint64_t shmOffset);
    uint64_t GetShmOffset();
    void SetShmSize(uint64_t shmSize);
    uint64_t GetShmSize();

public:
    uint32_t devId;     // reuse as shmFd
    void *targetAddr;   // reuse as shmOffset
    size_t targetSize;  // reuse as shmSize
    TargetDeviceType targetType;
    bool isClient;
};

struct ReqInfo {
    ReqInfo()
    {
        canceledOrDoneFuture = canceledOrDonePromise.get_future().share();
    }
    /**
     * @return true: done
     * @return false: canceled
     */
    bool WaitCancelOrDone();
    bool WaitCancelOrDone(int64_t timeoutMs);
    bool WaitPipelineDone(int64_t timeoutMs);
    bool IsCanceledOrDone();
    bool IsCanceled();
    void Cancel();
    void Done();
    void RecordFirstChunkElapse();

    std::string DebugString();

    int32_t receivedChunks = 0;
    int32_t totalChunks = 0;
    int32_t failedChunkId = -1;
    DynBitmap<DummyRWLock> receivedChunksDetail;
    void *syncHandle = nullptr;
    std::string key;
    std::shared_ptr<BaseRH2DDriver> driver;
    // For performance tracking: request start time (in milliseconds)
    int64_t startTimeMs = 0;
    // For worker-side remote pipeline receive, this is the real object payload size for the current request.
    // BaseRH2DDriver::targetSize is reused as shmSize on worker side after SetShmSize(), so it must not be
    // used to recover the last chunk's real size.
    uint64_t objectSize = 0;
    int shmFd;
    int shmOffset;
    uint32_t reqId;
    /**
     * @brief step before doneStep is all done canceled
     * step1: worker2 -> worker1
     * step2: worker1 -> kvclient
     * step3: kvclient -> cuda
     */
    PiplnDoneStep doneStep = PIPLN_DONE_NO_STEP;  // prevent get from local worker again
    std::promise<bool> canceledOrDonePromise;     // if canceled, ignore following chunks
    std::shared_future<bool> canceledOrDoneFuture;
    std::mutex promiseMutex;
};

class ChunkManager {
public:
    ChunkManager(bool isClient) : isClient_(isClient)
    {
    }
    ~ChunkManager()
    {
        ReleaseAll();
    }

    // common
    Status AddKey(const std::string &key, uint32_t reqId, const DevShmInfo &devInfo, int index = -1);
    void AddReqIdMap(uint32_t serverReqId, uint32_t clientReqId);
    ReqInfo *GetReqInfo(uint32_t reqId);
    ReqInfo *GetReqInfoByIndex(int32_t keyIndex);
    Status ReleaseAll();
    Status GetReqId(const std::string &key, uint32_t &reqId);
    static uint32_t GenerateReqId();
    int32_t KeyNum() const
    {
        return reqInfos_.size();
    };
    static Status InitOsPiplnRH2DEnv(urma_context_t *ctx, urma_jfc_t *jfc, urma_jfce_t *jfce, uint32_t jettySize,
                                     int threadNum, bool needCuda);
    static void UnInitOsPiplnRH2DEnv();
    Status WaitAll();
    void MarkCancelOrDone(uint32_t reqId, bool isDone);
    void MarkCancelOrDone(const std::string &key, bool isDone);
    void CancelAll();

    // dataSrc = shmPointer + shmOffset + metaSize
    // step 1
    Status DoPiplnStep1_StartReceiver(uint32_t reqId, uint64_t dataSrc, uint64_t size, urma_target_seg_t *targetSeg,
                                      urma_jfr_t *targetJfr, urma_jetty_t *targetJetty, int32_t shmFd, uint64_t shmSize,
                                      uint64_t shmOffset);
    Status DoPiplnStep1_StartSender(PiplnSndArgs &args);
    static int DoPiplnStep1_ReceiveCallback(void *arg);
    Status WaitPiplnStep12Done();
    // @return: is my event ?
    static bool DoPiplnStep1_ReceiveUrmaEventHook(urma_cr_t *cr);

    // step 2
    Status DoPiplnStep2_ProduceLocalChunk(uint32_t reqId, int32_t shmFd, uint64_t shmSize, uint64_t shmOffset,
                                          size_t srcSize);

    void RegisterPipelineConsumer(std::shared_ptr<PipelineRH2DQueueConsumer> &pipelineConsumer);
    void DoPiplnStep2_ChunkConsume(uint32_t reqId, uint64_t dataSrc, ChunkTag chunkTag, uint32_t chunkSize);
    void RemoveConsumerCallback(uint32_t reqId);

    void RegisterPipelineProducer(std::shared_ptr<PipelineRH2DQueueProducer> &pipelineProducer, uint32_t queueId);
    Status DoPiplnStep2_ChunkProduce(const ChunkTag &chunkTag);

    // step 3
    bool CheckIsRequestSuccess(uint32_t reqId);
    // copy srcData -> destData + destOffset
    Status DoPiplnStep3_SubmitIO(ReqInfo &info, void *srcData, size_t srcSize, size_t destOffset);

    void DebugPrintAllPipelineStatus();

public:
    std::shared_mutex releaseMutex;

private:
    static std::atomic<uint32_t> reqId_;
    bool isClient_;
    static void *osPiplnH2DHandle_;
    std::map<uint32_t, ReqInfo> reqInfos_;
    std::unordered_map<std::string, uint32_t> keyToReqIdMap_;
    std::map<uint32_t, ReqInfo *> indexToReqIdMap_;
    bool allDone_ = false;

    static std::mutex reqIdToChkMgrMapMutex_;
    static std::map<uint32_t, ChunkManager *> reqIdToChkMgrMap_;

    uint32_t queueId_;
    std::shared_ptr<PipelineRH2DQueueConsumer> pipelineConsumer_;
    std::shared_ptr<PipelineRH2DQueueProducer> pipelineProducer_;
    std::map<uint32_t, uint32_t> reqIdMap_;
};

}  // namespace OsXprtPipln

#endif