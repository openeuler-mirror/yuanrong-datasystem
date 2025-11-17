/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the worker sc allocate memory manager class.
 */

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_WORKER_SC_ALLOCATE_MEMORY_H
#define DATASYSTEM_WORKER_STREAM_CACHE_WORKER_SC_ALLOCATE_MEMORY_H

#include <memory>

#include "datasystem/common/shared_memory/shm_unit.h"
namespace datasystem {
namespace object_cache {
class WorkerOcEvictionManager;
}
namespace worker {
namespace stream_cache {
class WorkerSCAllocateMemory {
public:
    /**
     * @brief Construct a new Worker S C Evict Object object
     * @param manager
     */
    WorkerSCAllocateMemory(std::shared_ptr<object_cache::WorkerOcEvictionManager> manager);

    /**
     * @brief
     * @param tenantId
     * @param streamId
     * @param needSize
     * @param populate
     * @param shmUnit
     * @param retryOnOOM
     * @return Status
     */
    Status AllocateMemoryForStream(const std::string &tenantId, const std::string &streamId,
                                   const uint64_t needSize, bool populate, ShmUnit &shmUnit, bool retryOnOOM);

    /**
     * @brief Gets Total SHM memory allocated to the Stream Cache
     * @return Memory Size
     */
    uint64_t GetTotalMaxStreamSHMSize()
    {
        return streamMaxSize_;
    }

private:
    std::shared_ptr<object_cache::WorkerOcEvictionManager> ocEvictManager_;
    uint64_t streamMaxSize_ = 0;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_STREAM_CACHE_WORKER_SC_ALLOCATE_MEMORY_H