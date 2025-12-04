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
 * Description: A proper memory copy method is provided based on the object size.
 */

#ifndef DATASYSTEM_COMMON_UTIL_MEMORY_H
#define DATASYSTEM_COMMON_UTIL_MEMORY_H

#include <future>
#include <memory>
#include <thread>
#include <vector>

#include <securec.h>

#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
static constexpr uintptr_t MEMCOPY_BLOCK_SIZE = 64;             // The default block size of memory alignment.
static constexpr int MEMCOPY_THREAD_NUM = 8;                    // The default number of memcpy threads.
static constexpr int MEMCOPY_PARALLEL_THRESHOLD = 256 * 1024;   // The default data size by parallel memcopy.
static constexpr int MEMCOPY_CHUNK_SIZE = 32 * 1024 * 1024;     // The default chunk size of memory split.
static constexpr uint64_t SHM_THRESHOLD = 500 * 1024;           // The default threshold of choosing shm is 500KB.
static constexpr uint64_t MEMCOPY_SIZE_LIMIT = 0x7fffffffUL;    // 0x7fffffffUL bytes = 2GB.
static constexpr int MEMCOPY_CHUNK_THRESHOLD =
    MEMCOPY_THREAD_NUM * MEMCOPY_CHUNK_SIZE;  // The default threshold whether memory split by chunk is 256MB.

struct MemoryCopyInfo {
    uint8_t *dst;
    uint64_t dstSize;
    const uint8_t *src;
    uint64_t srcSize;

    MemoryCopyInfo(uint8_t *dst, uint64_t dstSize, const uint8_t *src, uint64_t srcSize)
        : dst(dst), dstSize(dstSize), src(src), srcSize(srcSize)
    {
    }
};

/**
 * @brief This function is used for doing memcpy with multiple threads. Memory alignment is considered to ensure that
 * the CPU reads data from the memory most efficiently.
 * @param[out] dst The destination address.
 * @param[in] dstMaxSize The maximum length of destination buffer.
 * @param[in] src The source address.
 * @param[in] srcSize The length of source buffer.
 * @param[in] threadPool The thread pool.
 * @return Status of the call.
 */
Status ParallelMemoryCopy(uint8_t *dst, uint64_t dstMaxSize, const uint8_t *src, uint64_t srcSize,
                          const std::shared_ptr<ThreadPool> &threadPool);

/**
 * @brief This is a common function which choose a better performance copy function based on the input data size.
 * If the data size is greater than MEMCOPY_PARALLEL_THRESHOLD(1 MB), the parallel copy function will be used.
 * Otherwise, the native memory copy function will be used.
 * @param[out] dst The destination address.
 * @param[in] dstMaxSize The maximum length of destination buffer.
 * @param[in] src The source address.
 * @param[in] srcSize The length of source buffer.
 * @param[in] threadPool The thread pool.
 * @param[in] threshold The threshold of parallel memory copy.
 * @return Status of the call.
 */
Status MemoryCopy(uint8_t *dst, uint64_t dstMaxSize, const uint8_t *src, uint64_t srcSize,
                  const std::shared_ptr<ThreadPool> &threadPool, uint64_t threshold = MEMCOPY_PARALLEL_THRESHOLD);

/**
 * @brief This is a common function which is used to copy huge memory blocks by chunks.
 * If srcSize > MEM_COPY_SIZE_LIMIT, it will cut the memory block into smaller chunks and
 * copy then in sequence, hence eliminating the 2GB limit of the secure memcpy_s function.
 * @param[out] dest The destination address.
 * @param[in] destMax The maximum length of destination buffer.
 * @param[in] src The source address.
 * @param[in] srcSize The length of source buffer.
 * @return Status of the call.
 */
Status HugeMemoryCopy(uint8_t *dest, uint64_t destMax, const uint8_t *src, uint64_t srcSize);

/**
 * @brief for destSize is large.
 * @param[in] dest dest address
 * @param[in] destSize dest size
 * @param[in] value set val
 * @param[in] count set size
 */
Status HugeMemset(uint8_t *dest, size_t destSize, int value, size_t count);

/**
 * @brief Get the recommended MemoryCopy threadPool size.
 * If we use dynamic thread pool, set this size as maxThreadNum.
 * Setting a thread pool size larger than recommended does not result in better throughput
 * @return The size of MemoryCopy threadPool.
 */
size_t GetRecommendedMemoryCopyThreadsNum();
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_MEMORY_H
