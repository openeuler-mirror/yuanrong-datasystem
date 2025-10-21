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
 * Description: Define shared memory ShmMemStat struct.
 */
#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_MEMSTAT_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_MEMSTAT_H

#include <cstdint>

namespace datasystem {
namespace memory {
struct ShmMemStat {
    // Allocate size user require.
    uint64_t memoryUsage = 0;

    // Shared memory max size limit.
    uint64_t maxMemoryLimit = 0;

    // Object shared memory usage.
    uint64_t objectMemoryUsage = 0;

    // Allocate size user require but internal fragments are included.
    uint64_t realMemoryUsage = 0;

    // Allocate size already binding to physical memory.
    uint64_t physicalMemoryUsage = 0;

    // Number of the memory block allocated by allocator.
    uint64_t numOfAllocated = 0;

    // Number of the fd that arena handle.
    uint64_t numOfFds = 0;

    // Reference page count.
    uint64_t refPageCount = 0;

    // No reference page count.
    uint64_t noRefPageCount = 0;
};
}  // namespace memory
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SHARED_MEMORY_MEMSTAT_H