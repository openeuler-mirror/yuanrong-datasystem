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
 * Description: SharedPageQueueGroup
 */

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_SHARED_PAGE_QUEUE_GROUP_H
#define DATASYSTEM_WORKER_STREAM_CACHE_SHARED_PAGE_QUEUE_GROUP_H

#include <memory>
#include <shared_mutex>

#include "datasystem/worker/stream_cache/page_queue/shared_page_queue.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class SharedPageQueueGroup final {
public:
    SharedPageQueueGroup(HostPort remoteWorker, std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager,
                         ClientWorkerSCServiceImpl *scSvc);
    ~SharedPageQueueGroup();

    std::vector<std::string> GetAllSharedPageName();

    void GetOrCreateSharedPageQueue(const std::string &namespaceUri, std::shared_ptr<SharedPageQueue> &pageQueue);

    Status GetSharedPageQueue(const std::string &namespaceUri, std::shared_ptr<SharedPageQueue> &pageQueue);

    Status RemoveSharedPageQueueForTenant(const std::string &tenantId);

private:
    size_t GetPartId(const std::string &streamName) const;

    const size_t partCount_;
    const HostPort remoteWorker_;
    std::shared_timed_mutex mutex_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<SharedPageQueue>>> tenantPageQueues_;
    std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager_;
    ClientWorkerSCServiceImpl *scSvc_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif
