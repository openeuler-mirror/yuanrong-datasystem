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

#ifndef OS_XPRT_PIPLN_NOTIFY_QUEUE_H
#define OS_XPRT_PIPLN_NOTIFY_QUEUE_H

#include <cstdint>
#include <functional>
#include <memory>
#include <map>
#include <set>
#include <mutex>
#include <atomic>
#include <thread>
#include <string>

#include "datasystem/common/constants.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_types.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/util/queue/shm_circular_queue.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"

// converter: shm fd -> shm pointer
using datasystem::ShmCircularQueue;
using datasystem::ShmUnitInfo;
using datasystem::Status;
using ShmConvertHookFunc = std::function<Status(std::shared_ptr<ShmUnitInfo> &)>;

namespace OsXprtPipln {

constexpr uint32_t QUEUE_CAPACITY = 200;
constexpr uint32_t INVALID_PIPLN_QUEUE_ID = (uint32_t)-1;

struct PipelineRH2DMsg {
    int32_t chunkSize;
    int32_t shmFd;
    uint64_t shmSize;
    uint64_t shmOffset;  // shmOffset = shmUnitOffset + readOffset + MetaSize
    ChunkTag chunkTag;
    std::string DebugString() const;
};

using PipelineMsgHandler =
    std::function<void(uint32_t /* requestId */, uint64_t /* dataSrc */, ChunkTag, uint32_t /* chunkSize */)>;

struct PiplnMsgShmQueue : public ShmCircularQueue {
    PiplnMsgShmQueue(size_t capacity, uint32_t elementSize, std::shared_ptr<ShmUnitInfo> shmUnit, bool isClient = false)
        : ShmCircularQueue(capacity, elementSize, shmUnit, 0, isClient)
    {
    }
    Status SharedLock(uint64_t timeoutSec)
    {
        return queueLock_->CommonLock::RLatch(timeoutSec);
    }
    Status SharedUnlock()
    {
        queueLock_->CommonLock::UnRLatch();
        return Status::OK();
    }
    Status TrySharedUnlockByLockId()
    {
        queueLock_->CommonLock::UnRLatch();
        return Status::OK();
    }
};

/**
 * @brief Producer side for RH2D Pipeline Queue
 */
class PipelineRH2DQueueProducer {
public:
    PipelineRH2DQueueProducer();
    ~PipelineRH2DQueueProducer();

    Status HoldAvailableQueue(uint32_t &queueId);
    Status ReleaseAvailableQueue(uint32_t queueId);
    Status ProduceOne(uint32_t queueId, const PipelineRH2DMsg &msg, uint32_t timeoutSec = 60);
    Status GetQueueShmInfo(uint32_t queueId, int &shmFd, ptrdiff_t &offset, size_t &mmap_size, std::string &shmId);

private:
    Status CreateQueue(uint32_t id, std::shared_ptr<PiplnMsgShmQueue> &q);

    std::set<uint32_t> freeQueueIds_;
    std::mutex mutex_;
    std::vector<std::shared_ptr<PiplnMsgShmQueue>> queues_;
};

/**
 * @brief Consumer side for RH2D Pipeline Queue
 */
class PipelineRH2DQueueConsumer {
public:
    PipelineRH2DQueueConsumer(){};
    ~PipelineRH2DQueueConsumer();

    Status InitQueue(std::shared_ptr<ShmUnitInfo> shmUnitInfo, std::shared_ptr<ShmConvertHookFunc> converter);
    void AddCallback(uint32_t requestId, std::shared_ptr<PipelineMsgHandler> callback);
    void RemoveCallback(uint32_t requestId);
    Status RegisterHostMemory(int workerFd, void *ptr, size_t size);

private:
    void ConsumeOne(uint8_t *element);
    void ConsumerLoop();

    std::shared_ptr<ShmConvertHookFunc> converter_;
    std::shared_ptr<PiplnMsgShmQueue> queue_;
    std::shared_ptr<std::thread> worker_;
    bool workerStop_ = false;

    std::mutex mutex_;
    std::map<uint32_t, std::shared_ptr<PipelineMsgHandler>> msgHandlers_;

    struct PinnedHostMemoryInfo {
        void *ptr = nullptr;
        size_t size = 0;
    };
    std::mutex pinnedHostMemoryMutex_;
    std::unordered_map<int, PinnedHostMemoryInfo> pinnedHostMemories_;
};

}  // namespace OsXprtPipln

#endif