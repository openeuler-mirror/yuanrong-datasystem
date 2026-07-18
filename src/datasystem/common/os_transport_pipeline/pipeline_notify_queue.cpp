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
 * Description: pipeline rh2d message queue between kvclient and worker
 */

#include "datasystem/common/os_transport_pipeline/pipeline_notify_queue.h"
#ifdef BUILD_PIPLN_H2D
#include "datasystem/common/os_transport_pipeline/cuda_rh2d_driver.h"
#endif

#include <algorithm>
#include <iterator>
#include <cstring>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

using namespace datasystem;

static constexpr int MAX_RH2D_QUEUE = 200;

namespace OsXprtPipln {

std::string PipelineRH2DMsg::DebugString() const
{
    std::stringstream ss;
    ss << "fd:" << shmFd << " offset:" << shmOffset << " chunk(" << ChunkTag::DebugString(chunkTag) << ")";
    return ss.str();
}

PipelineRH2DQueueProducer::PipelineRH2DQueueProducer()
{
    for (uint32_t i = 0; i < MAX_RH2D_QUEUE; i++) {
        freeQueueIds_.insert(i);
        queues_.emplace_back(nullptr);
    }
}

PipelineRH2DQueueProducer::~PipelineRH2DQueueProducer()
{
    for (auto q : queues_) {
        if (q)
            q->WakeUpClientProcessAndFinish();
    }
}

Status PipelineRH2DQueueProducer::HoldAvailableQueue(uint32_t &queueId)
{
    queueId = INVALID_PIPLN_QUEUE_ID;
    uint32_t targetQueueId;
    {
        std::lock_guard<std::mutex> l(mutex_);
        auto it = freeQueueIds_.begin();
        if (it == freeQueueIds_.end()) {
            RETURN_STATUS(StatusCode::K_NO_SPACE, "Client number upper to the limit");
        }
        targetQueueId = *freeQueueIds_.begin();
        (void)freeQueueIds_.erase(targetQueueId);
    }

    auto &q = queues_[targetQueueId];
    if (!q)
        RETURN_IF_NOT_OK_APPEND_MSG(CreateQueue(targetQueueId, q), "failed to create pipeline queue");

    q->Clear();
    queueId = targetQueueId;
    return Status::OK();
}

Status PipelineRH2DQueueProducer::ReleaseAvailableQueue(uint32_t queueId)
{
    if (queueId == INVALID_PIPLN_QUEUE_ID)
        return Status::OK();

    std::lock_guard<std::mutex> l(mutex_);
    queues_[queueId]->WakeUpClientProcessAndFinish();
    if (freeQueueIds_.find(queueId) != freeQueueIds_.end()) {
        return Status(K_RUNTIME_ERROR, "queue " + std::to_string(queueId) + " is not held");
    }
    freeQueueIds_.insert(queueId);
    return Status::OK();
}

Status PipelineRH2DQueueProducer::CreateQueue(uint32_t id, std::shared_ptr<PiplnMsgShmQueue> &q)
{
    (void)id;
    uint32_t elementSize = sizeof(PipelineRH2DMsg);
    uint32_t memorySize = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) + QUEUE_CAPACITY * elementSize;
    auto shmUnit = std::make_shared<ShmUnit>();
    shmUnit->id = ShmKey::Intern(GetStringUuid());
    RETURN_IF_NOT_OK(shmUnit->AllocateMemory(DEFAULT_TENANT_ID, memorySize, true));
    auto result = memset_s(shmUnit->pointer, memorySize, 0, memorySize);
    CHECK_FAIL_RETURN_STATUS(result == 0, K_RUNTIME_ERROR,
                             FormatString("Memory set failed, the memset_s return: %d: ", result));
    auto tmp = std::make_shared<PiplnMsgShmQueue>(QUEUE_CAPACITY, elementSize, shmUnit);
    RETURN_IF_NOT_OK(tmp->Init());
    q = std::move(tmp);
    return Status::OK();
}

Status PipelineRH2DQueueProducer::ProduceOne(uint32_t queueId, const PipelineRH2DMsg &msg, uint32_t timeoutSec)
{
    VLOG(2) << PIPLN_LOG_PREFIX "start ProduceOne queueId " << queueId << " " << msg.DebugString() << " chunkSize "
            << msg.chunkSize;
    Timer timer(timeoutSec * 1000L);
    std::shared_ptr<PiplnMsgShmQueue> queue;
    {
        std::lock_guard<std::mutex> l(mutex_);
        auto it = freeQueueIds_.find(queueId);
        if (it != freeQueueIds_.end()) {
            return Status(K_NOT_FOUND, "failed to find queue for " + std::to_string(queueId)
                                           + ", stop produce message for requeust "
                                           + std::to_string(msg.chunkTag.reqId));
        }
    }
    queue = queues_[queueId];
    if (!queue)
        return Status(K_NOT_FOUND, "queue " + std::to_string(queueId) + " is already released");

    // wait if full
    uint32_t slotIndex;
    bool done = false;
    while (!timer.IsTimeout()) {
        CHECK_FAIL_RETURN_STATUS(!queue->CheckQueueDestroyed(), K_RUNTIME_ERROR,
                                 "pipeline rh2d queue is released when produce.");
        queue->UpdateQueueMeta();
        while (queue->IsFull()) {
            CHECK_FAIL_RETURN_STATUS(!timer.IsTimeout(), K_RUNTIME_ERROR,
                                     "failed to add lock in " + std::to_string(timeoutSec) + " seconds");
            uint64_t remainingNs = static_cast<uint64_t>(timer.GetRemainingTimeMs() * 1'000'000.0);
            struct timespec lastTime = NsToTs(remainingNs);
            RETURN_IF_NOT_OK(queue->WaitForQueueFull(lastTime));
            CHECK_FAIL_RETURN_STATUS(!queue->CheckQueueDestroyed(), K_RUNTIME_ERROR,
                                     "pipeline rh2d queue is released when produce.");
        }
        uint64_t lastSecond = static_cast<uint64_t>(timer.GetRemainingTimeMs() / 1000);
        auto ret = queue->SharedLock(lastSecond);
        if (ret.IsError()) {
            continue;
        }
        {
            Raii unlockAll([&queue]() { queue->SharedUnlock(); });
            queue->UpdateQueueMeta();
            if (!queue->GetSlotUntilSuccess(slotIndex)) {
                continue;
            }
            uint8_t *waitFlag;
            RETURN_IF_NOT_OK(queue->PushBySlot(slotIndex, (const char *)&msg, (uint32_t)sizeof(msg), &waitFlag));
        }
        queue->NotifyNotEmpty();
        done = true;
        break;
    }

    CHECK_FAIL_RETURN_STATUS(done, K_RUNTIME_ERROR, "pipeline rh2d timeout, ignore producing chunk");
    VLOG(2) << PIPLN_LOG_PREFIX "end ProduceOne queueId " << queueId << " " << msg.DebugString();
    return Status::OK();
}

Status PipelineRH2DQueueProducer::GetQueueShmInfo(uint32_t queueId, int &shmFd, ptrdiff_t &offset, size_t &mmapSize,
                                                  std::string &shmId)
{
    shmFd = -1;
    if (queues_[queueId]) {
        ShmKey shmKey;
        auto ret = queues_[queueId]->GetQueueShmUnit(shmFd, mmapSize, offset, shmKey);
        shmId = shmKey.ToString();
        return ret;
    } else {
        return Status(K_RUNTIME_ERROR, "pipeline queue " + std::to_string(queueId) + " is null");
    }
    return Status::OK();
}

Status PipelineRH2DQueueConsumer::InitQueue(std::shared_ptr<ShmUnitInfo> shmUnitInfo,
                                            std::shared_ptr<ShmConvertHookFunc> converter)
{
    converter_ = converter;
    RETURN_IF_NOT_OK(converter_->operator()(shmUnitInfo));
    queue_ =
        std::make_shared<PiplnMsgShmQueue>(QUEUE_CAPACITY, sizeof(PipelineRH2DMsg), shmUnitInfo, true /* isClient */);
    RETURN_IF_NOT_OK(queue_->Init());
    queue_->SetGetDataHandler([this](uint8_t *element) { this->ConsumeOne(element); });  // called in GetAndPopAll
    worker_ = std::make_shared<std::thread>([this]() { this->ConsumerLoop(); });
    return Status::OK();
}

PipelineRH2DQueueConsumer::~PipelineRH2DQueueConsumer()
{
    if (!queue_)
        return;
    {
        std::lock_guard<std::mutex> l(mutex_);
        msgHandlers_.clear();
    }
    workerStop_ = true;
    if (worker_) {
        queue_->WriteLock();
        queue_->UpdateQueueMeta();
        if (queue_->Length() == 0) {
            queue_->WakeUpQueueProcessAndFinish();
        }
        queue_->WriteUnlock();
        worker_->join();
    }
    {
        std::lock_guard<std::mutex> l(pinnedHostMemoryMutex_);
        for (auto &it : pinnedHostMemories_) {
            CudaRH2DDriver::UnRegisterHostMemory(it.second.ptr);
        }
    }
}

void PipelineRH2DQueueConsumer::AddCallback(uint32_t requestId, std::shared_ptr<PipelineMsgHandler> callback)
{
    std::lock_guard<std::mutex> l(mutex_);
    msgHandlers_[requestId] = callback;
}

void PipelineRH2DQueueConsumer::RemoveCallback(uint32_t requestId)
{
    std::lock_guard<std::mutex> l(mutex_);
    msgHandlers_.erase(requestId);
}

Status PipelineRH2DQueueConsumer::RegisterHostMemory(int workerFd, void *ptr, size_t size)
{
    CHECK_FAIL_RETURN_STATUS(workerFd >= 0, StatusCode::K_INVALID, "invalid worker shm fd for cudaHostRegister");
    CHECK_FAIL_RETURN_STATUS(ptr != nullptr, StatusCode::K_INVALID, "nullptr host pointer for cudaHostRegister");
    CHECK_FAIL_RETURN_STATUS(size > 0, StatusCode::K_INVALID, "zero host memory size for cudaHostRegister");
    std::lock_guard<std::mutex> l(pinnedHostMemoryMutex_);
    if (workerStop_)
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "connection lost");
    auto it = pinnedHostMemories_.find(workerFd);
    if (it != pinnedHostMemories_.end()) {
        if (it->second.ptr == ptr && it->second.size == size) {
            return Status::OK();
        }
        std::stringstream ss;
        ss << "worker shm fd has already been cudaHostRegister'ed with another address or size old: " << it->second.ptr
           << "(size:" << it->second.size << ")"
           << " new " << ptr << "(size:" << size << ")";
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, ss.str());
    }

    auto ret = CudaRH2DDriver::RegisterHostMemory(ptr, size);
    if (ret.IsError()) {
        LOG(WARNING) << PIPLN_LOG_PREFIX "cudaHostRegister failed: fd=" << workerFd << ", size=" << size
                     << ", error=" << ret.GetMsg();
        return ret;
    } else {
        VLOG(1) << "Pipeline RH2D cudaHostRegister success, worker fd: " << workerFd << " ptr " << ptr
                << "(size:" << size << ")";
        pinnedHostMemories_.emplace(workerFd, PinnedHostMemoryInfo{ ptr, size });
        return Status::OK();
    }
}

void PipelineRH2DQueueConsumer::ConsumeOne(uint8_t *element)
{
    PipelineRH2DMsg *msg = (PipelineRH2DMsg *)(element);
    VLOG(2) << "start ConsumeOne " << msg->DebugString() << " chunkSize " << msg->chunkSize;
    if (msg->shmFd <= 0 || msg->shmSize == 0) {
        LOG(WARNING) << PIPLN_LOG_PREFIX "Invalid shm msg, skip shmfd " << msg->shmFd << " shmSize " << msg->shmSize;
        return;
    }
    auto shmUnit = std::make_shared<ShmUnitInfo>(msg->shmFd, msg->shmSize);
    // convert fd to local pointer, see LookupUnitsAndMmapFd
    Status rc = converter_->operator()(shmUnit);
    if (rc.IsError()) {
        LOG(ERROR) << PIPLN_LOG_PREFIX "mmap shm failed: " << rc.ToString() << ", msg=" << msg->DebugString();
        return;
    }

    uint64_t dataSrc = (uint64_t)shmUnit->GetPointer() + msg->shmOffset;
    std::shared_ptr<PipelineMsgHandler> handler;
    {
        std::lock_guard<std::mutex> l(mutex_);
        auto it = msgHandlers_.find(msg->chunkTag.reqId);
        if (it == msgHandlers_.end()) {
            LOG(WARNING) << PIPLN_LOG_PREFIX "No callback for reqId=" << msg->chunkTag.reqId << ", ignore msg";
            return;
        }
        handler = it->second;
    }
    handler->operator()(msg->chunkTag.reqId, dataSrc, msg->chunkTag, msg->chunkSize);
    VLOG(2) << "end ConsumeOne " << msg->DebugString() << " chunkSize " << msg->chunkSize;
}

void PipelineRH2DQueueConsumer::ConsumerLoop()
{
    queue_->UpdateQueueMeta();
    constexpr int timeKilo = 1000;
    const struct timespec timeoutStruct = { .tv_sec = static_cast<long int>(RPC_TIMEOUT / timeKilo), .tv_nsec = 0 };
    while (!workerStop_) {
        auto futexRc = queue_->WaitForQueueEmpty(timeoutStruct);
        if (workerStop_) {
            return;
        }
        if (futexRc.IsError()) {
            if (futexRc.GetCode() == K_UNKNOWN_ERROR) {
                LOG(ERROR) << PIPLN_LOG_PREFIX "Wait queue error: " << futexRc.ToString();
            } else {
                LOG(WARNING) << PIPLN_LOG_PREFIX "Wait msg timeout: no msg in " << timeoutStruct.tv_nsec << " seconds";
            }
            continue;
        }
        auto ret = queue_->WriteLock();
        if (ret.IsError()) {
            LOG_IF_ERROR(ret, PIPLN_LOG_PREFIX "PipelineRH2DQueueConsumer Failed to add write lock");
            continue;
        }
        {
            Raii unlockAll([this]() { queue_->WriteUnlock(); });
            if (workerStop_) {
                return;
            }
            queue_->UpdateQueueMeta();
            int popSize = queue_->GetAndPopAll();
            if (popSize <= 0) {
                int fd;
                uint64_t mmapSize;
                ptrdiff_t offset;
                ShmKey id;
                queue_->GetQueueShmUnit(fd, mmapSize, offset, id);
                LOG(WARNING) << PIPLN_LOG_PREFIX "poped no queue msg: fd=" << fd << ", offset=" << offset
                             << ", shmId:" << id;
                continue;
            } else {
                VLOG(2) << "popped " << popSize << " pipeline rh2d msg.";
                queue_->NotifyQueueNotFull();
            }
        }
    }
}

}  // namespace OsXprtPipln
