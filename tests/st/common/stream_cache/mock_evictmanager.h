/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Test StreamPage StreamPageOwner classes.
 */
#ifndef DATASYSTEM_TEST_ST_WORKER_STREAM_CACHE_MOCK_EVICTMANAGER_H
#define DATASYSTEM_TEST_ST_WORKER_STREAM_CACHE_MOCK_EVICTMANAGER_H

#include <cstdint>

#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"

namespace datasystem {
namespace st {
class MockEvictManager : public datasystem::worker::stream_cache::WorkerSCAllocateMemory {
public:
    MockEvictManager() : WorkerSCAllocateMemory(nullptr){};

    Status AllocateMemoryForStream(const std::string &tenantId, const std::string &streamId,
                                   const uint64_t needSize, bool populate, ShmUnit &shmUnit, bool retryOnOOM)
    {
        (void)streamId;
        (void)retryOnOOM;
        return shmUnit.AllocateMemory(tenantId, needSize, populate);
    }
};
}  // namespace st
}  // namespace datasystem
#endif