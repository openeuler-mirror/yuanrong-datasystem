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
 * Description: SharedPageQueue
 */

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_SHARED_PAGE_QUEUE_BASE_H
#define DATASYSTEM_WORKER_STREAM_CACHE_SHARED_PAGE_QUEUE_BASE_H

#include "datasystem/worker/stream_cache/page_queue/page_queue_base.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class ClientWorkerSCServiceImpl;
class RemoteWorkerManager;
class SharedPageQueue : public std::enable_shared_from_this<SharedPageQueue>, public PageQueueBase {
public:
    SharedPageQueue(std::string tenantId, HostPort remoteWorker, int partId,
                    std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager, ClientWorkerSCServiceImpl *scSvc);
    virtual ~SharedPageQueue() = default;
    Status RemoteAck();
    uint64_t UpdateLastAckCursorUnlocked(uint64_t minSubsAckCursor);

    const std::string &GetPageQueueId() const;

    Status GetOrCreateLastPageRef(ShmView &lastPageRefShmView,
                                  std::shared_ptr<SharedMemViewImpl> &lastPageRefShmViewImpl);

public:
    std::string LogPrefix() const override;
    size_t GetPageSize() const override;
    Status AllocateMemoryImpl(size_t memSizeNeeded, ShmUnit &shmUnit, bool retryOnOOM) override;
    Status CheckHadEnoughMem(uint64_t memSize) override;
    Status UpdateLocalCursorLastDataPage(const ShmView &shmView) override;
    Status AfterAck() override;
    bool IsEncryptStream(const std::string &streamName) const override;
    std::string GetStreamName() const override;
    RemoteWorkerManager *GetRemoteWorkerManager() const override;

private:
    std::shared_ptr<PageQueueBase> SharedFromThis() override;
    const std::string tenantId_;
    const HostPort remoteWorker_;
    std::string pageQueueId_;
    std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager_;
    std::atomic<uint64_t> lastAckCursor_;
    ClientWorkerSCServiceImpl *scSvc_;

    mutable std::shared_timed_mutex lastPageRefMutex_;
    std::unique_ptr<ShmUnit> lastPageRefShmUnit_;
    std::shared_ptr<SharedMemViewImpl> lastPageRefShmViewImpl_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif
