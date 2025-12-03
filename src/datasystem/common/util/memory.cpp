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
 * Description: This file is used to encapsulate all memory copy facilities.
 */

#include "datasystem/common/util/memory.h"

#include <cstddef>
#include <system_error>
#include <thread>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/status.h"

namespace datasystem {
uint8_t *MemoryPointerAlignment(const uint8_t *address, uintptr_t bits)
{
    return reinterpret_cast<uint8_t *>(reinterpret_cast<uintptr_t>(address) & bits);
}

void AlignSrcMemoryPointer(const uint8_t *src, uint64_t srcSize, uint8_t *&left, uint8_t *&right)
{
    // Align left and right pointer by 64bit to improve memory copy performance.
    uintptr_t blockSize = MEMCOPY_BLOCK_SIZE;
    left = MemoryPointerAlignment(src + blockSize - 1, ~(blockSize - 1));
    right = MemoryPointerAlignment(src + srcSize, ~(blockSize - 1));
}

void SplitMemoryByThreads(uint8_t *dst, uint64_t dstMaxSize, const uint8_t *src, uint64_t srcSize,
                          std::vector<MemoryCopyInfo> &chunks)
{
    uint8_t *left = nullptr;
    uint8_t *right = nullptr;
    AlignSrcMemoryPointer(src, srcSize, left, right);

    uintptr_t blockSize = MEMCOPY_BLOCK_SIZE;
    auto threadNum = MEMCOPY_THREAD_NUM;
    int64_t numBlocks = (right - left) / blockSize;

    // Now we update right address that these blocks between left and right are suited to the copy threads,
    // and the remainder is handled on the main thread.
    // The data layout | prefix | k * threadNum * blockSize | suffix |.
    //                 ^        ^                           ^        ^
    //                 |        |                           |        |
    //                src      left                       right    src+srcSize
    right = right - (numBlocks % threadNum) * static_cast<int>(blockSize);

    int64_t chunkSize = static_cast<int64_t>((right - left) / threadNum);
    int64_t prefix = static_cast<int64_t>(left - src);
    int64_t suffix = static_cast<int64_t>(src + srcSize - right);

    auto parallelNum = threadNum + 2;
    chunks.reserve(parallelNum);
    int index = 0;
    while (index < threadNum) {
        int64_t offset = static_cast<int64_t>(prefix + index * chunkSize);
        chunks.emplace_back(dst + offset, chunkSize, left + index * chunkSize, chunkSize);
        index++;
    }
    if (prefix != 0) {
        chunks.emplace_back(dst, prefix, src, prefix);
    }
    if (suffix != 0) {
        chunks.emplace_back(dst + (right - src), dstMaxSize - (right - src), right, suffix);
    }
}

void SplitMemoryByFixedChunk(uint8_t *dst, uint64_t dstMaxSize, const uint8_t *src, uint64_t srcSize,
                             std::vector<MemoryCopyInfo> &chunks)
{
    uint8_t *left = nullptr;
    uint8_t *right = nullptr;
    AlignSrcMemoryPointer(src, srcSize, left, right);

    int64_t chunkSize = MEMCOPY_CHUNK_SIZE;
    int64_t chunkNum = std::max<int64_t>(0, (right - left) / chunkSize - 1);

    // Now we update right address that these blocks between left and right are suited to the copy threads,
    // and the remainder is handled on the main thread.
    // The data layout | prefix | chunkNum * chunkSize | suffix1 | suffix2 |.
    //                 ^        ^                      ^                   ^
    //                 |        |                      |                   |
    //                src      left                  right             src+srcSize
    right = left + chunkNum * chunkSize;
    int64_t prefix = left - src;
    int64_t suffix = src + srcSize - right;
    uint32_t parallelNum = chunkNum + 3;

    chunks.reserve(parallelNum);
    int32_t index = 0;
    while (index < chunkNum) {
        int64_t offset = prefix + index * chunkSize;
        chunks.emplace_back(dst + offset, chunkSize, left + index * chunkSize, chunkSize);
        index++;
    }
    if (prefix != 0) {
        chunks.emplace_back(dst, prefix, src, prefix);
    }
    if (suffix != 0) {
        // Split suffix to two copy task, improve the performance when memory size < 2 * chunkSize.
        // For example, if chunkSize is 32M, memory size is 32.1M, split to 2 * 16.05M is better than 32M + 0.1M
        uint64_t suffix1 = suffix / 2;
        chunks.emplace_back(dst + (right - src), suffix1, right, suffix1);
        chunks.emplace_back(dst + (right + suffix1 - src), dstMaxSize - (right + suffix1 - src), right + suffix1,
                            suffix - suffix1);
    }
}

Status ParallelMemoryCopy(uint8_t *dst, uint64_t dstMaxSize, const uint8_t *src, uint64_t srcSize,
                          const std::shared_ptr<ThreadPool> &threadPool)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(dst != nullptr && src != nullptr, K_INVALID,
                                         "dst or src pointers cannot be null.");
    if (threadPool == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR, "Thread pool is null");
    }

    // Assmue copying MEMCOPY_CHUNK_SIZE memory in a thread need a fixed time x.
    // The memory copy directly will spent time: (srcSize / MEMCOPY_CHUNK_SIZE + 1) * x
    // If the memory copy task submit to threadPool, the task need to wait until the tasks submitted before finish.
    // So new task need to wait: (threadsNum + WaitingTasksNum) / threadsNum * x
    // If copy task submit to threadpool need wait more time than copy directly, copy directly.
    if (threadPool->GetWaitingTasksNum() > threadPool->GetThreadsNum() * (srcSize / MEMCOPY_CHUNK_SIZE)) {
        return HugeMemoryCopy(dst, dstMaxSize, src, srcSize);
    }

    std::vector<MemoryCopyInfo> chunks;
    if (srcSize > MEMCOPY_CHUNK_THRESHOLD) {
        SplitMemoryByFixedChunk(dst, dstMaxSize, src, srcSize, chunks);
    } else {
        SplitMemoryByThreads(dst, dstMaxSize, src, srcSize, chunks);
    }
    std::vector<std::future<Status>> futures;
    futures.reserve(chunks.size());
    for (auto &memCopyInfo : chunks) {
        futures.push_back(threadPool->Submit(HugeMemoryCopy, memCopyInfo.dst, memCopyInfo.dstSize, memCopyInfo.src,
                                             memCopyInfo.srcSize));
    }
    for (auto &future : futures) {
        Status rc = future.get();
        CHECK_FAIL_RETURN_STATUS(rc.IsOk(), StatusCode::K_RUNTIME_ERROR, "Parallel memory copy failed");
    }
    return Status::OK();
}

Status MemoryCopy(uint8_t *dst, uint64_t dstMaxSize, const uint8_t *src, uint64_t srcSize,
                  const std::shared_ptr<ThreadPool> &threadPool, uint64_t threshold)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(dst != nullptr && src != nullptr, K_INVALID,
                                         "dst or src pointer cannot be null.");
    PerfPoint point(PerfKey::COMMON_UTIL_MEMORY_COPY);
    if (dstMaxSize < srcSize) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      FormatString("dst size: %d smaller than src size: %d", dstMaxSize, srcSize));
    }
    if (threadPool != nullptr && srcSize > threshold) {
        Status rc = Status::OK();
        try {
            rc = ParallelMemoryCopy(dst, dstMaxSize, src, srcSize, threadPool);
        } catch (std::system_error &sysErr) {
            LOG(ERROR) << "ParallelMemoryCopy is failed because system_error happened when creating new thread in "
                          "submit tasks, and thread pool have not remaining threads to run tasks.";
            // If ParallelMemoryCopy throw system_error, try to use HugeMemoryCopy in working thread.
            rc = HugeMemoryCopy(dst, dstMaxSize, src, srcSize);
        }
        return rc;
    }
    int ret = memcpy_s(dst, std::min(dstMaxSize, srcSize), src, srcSize);
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Memory copy failed, the memcpy_s return: %d: ", ret));
    return Status::OK();
}

Status HugeMemset(uint8_t *dest, size_t destSize, int value, size_t count)
{
    size_t chunk_size = SECUREC_MEM_MAX_LEN;
    size_t remaining = count;
    if (count > destSize) {
        return Status(K_RUNTIME_ERROR, "memset count > destSize");
    }

    while (remaining > 0) {
        size_t current_size = (remaining > chunk_size) ? chunk_size : remaining;
        auto ret = memset_s(dest, current_size, value, current_size);
        if (ret != 0) {
            return Status(K_RUNTIME_ERROR, FormatString("memset failed, err: %d", ret));
        }
        dest += current_size;
        remaining -= current_size;
    }
    return Status::OK();
}

Status HugeMemoryCopy(uint8_t *dest, uint64_t destMax, const uint8_t *src, uint64_t srcSize)
{
    INJECT_POINT("HugeMemoryCopy");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(dest != nullptr && src != nullptr, K_INVALID,
                                         "dest and src pointers cannot be  null.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(srcSize > 0 && srcSize <= destMax, K_INVALID,
                                         "src data length must be in (0, destMax].");
    auto dstPtr = dest;
    auto srcPtr = src;
    auto dstLen = destMax;
    auto srcLen = srcSize;

    uint64_t memChunkLimit = MEMCOPY_SIZE_LIMIT;

    // To reduce running time of the UT: ObjectClientTest.HugeMemoryCopyTest.
    INJECT_POINT("memcopy.GetMemChunkLimit", [&memChunkLimit](int sizeLimit) {
        LOG(INFO) << "set memChunkLimit to " << sizeLimit;
        memChunkLimit = sizeLimit;
        return Status::OK();
    });
    const int DEBUG_MIDDLE_LEVEL = 2;
    VLOG(DEBUG_MIDDLE_LEVEL) << "memChunkLimit = " << memChunkLimit / MEMCOPY_PARALLEL_THRESHOLD << "MB";
    VLOG(DEBUG_MIDDLE_LEVEL) << "srcLen = " << srcLen / MEMCOPY_PARALLEL_THRESHOLD << "MB";

    while (srcLen > memChunkLimit) {
        int ret = memcpy_s(dstPtr, memChunkLimit, srcPtr, memChunkLimit);
        CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("Memory copy failed, the memcpy_s return: %d: ", ret));
        srcPtr += memChunkLimit;
        dstPtr += memChunkLimit;
        dstLen -= memChunkLimit;
        srcLen -= memChunkLimit;
    }

    if (srcLen > 0) {
        int ret = memcpy_s(dstPtr, std::min(srcLen, dstLen), srcPtr, srcLen);
        CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                                 FormatString("Memory copy failed, the memcpy_s return: %d: ", ret));
    }
    return Status::OK();
}

size_t GetRecommendedMemoryCopyThreadsNum()
{
    // When threads number set to the number of cores, we will get the best parallel performance for MemoryCopy.
    // But std::thread::hardware_concurrency() may return 0, or if the number of cores is less than 8,
    // the threads is not enough for parallel copy tasks to schedule.
    // After experiments, we found that 32 is a balancing threads num for performance and concurrency.
    size_t defaultThreadPoolNum = 32;
    return std::max(defaultThreadPoolNum, static_cast<size_t>(std::thread::hardware_concurrency()));
}
}  // namespace datasystem