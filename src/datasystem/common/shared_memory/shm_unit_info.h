/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Define the basic unit information of the shared memory in client side.
 * Don't support allocate or free shared memory.
 */
#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_SHM_UNIT_INFO_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_SHM_UNIT_INFO_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/immutable_string/immutable_string.h"

namespace datasystem {
struct ShmView {
    int fd = -1;
    uint64_t mmapSz = 0;
    ptrdiff_t off = 0;
    uint64_t sz = 0;

    std::string ToStr() const
    {
        std::stringstream ss;
        ss << "fd: " << fd << ", mmapSz: " << mmapSz << ", off: " << off << ", sz: " << sz;
        return ss.str();
    }

    bool operator==(const ShmView &other) const
    {
        return (this->fd == other.fd && this->sz == other.sz && this->off == other.off && this->mmapSz == other.mmapSz);
    }

    bool operator!=(const ShmView &other) const
    {
        return !operator==(other);
    }
};

// The pages in the shared memory file(fd, the size is mmapSize)
// Notice: Pointer refers to the pointer from the original mmap.
// 0                |-----------------| <- pointer
//                  |-----------------|
//                  |-----------------|
// offset1          |---Start Page1---| <- pointer + offset1
//                  |-----------------|
// offset1 + size1  |---End Page1-----|
//                  |-----------------|
// offset2          |---Start Page2---| <- pointer + offset2
//                  |-----------------|
//                  |-----------------|
//                  |-----------------|
// offset2 + size2  |---End Page2-----|
// mmapSize - 1     |-----------------|
class ShmUnitInfo {
public:
    /**
     * @brief Constructor 1 (default).
     * Initializes all fields to their defaults.
     * ShmUnitInfo is mainly used in the client side (Client don't need to allocate or free shared memory).
     * If you need to allocate or free shared memory, use its subclass ShmUnit in the server side.
     */
    ShmUnitInfo() = default;

    /**
     * @brief Constructor 2.
     * Similar to constructor 1, however this one is explicitly only used in the client case of a memory mapped region
     * where the memory is owned externally. Therefore it is assumed that it is taking a shallow reference to memory
     * and is not responsible to free it
     * @param[in] fd The file descriptor for a memory mapped region.
     * @param[in] mmapSze The size of the memory mapped region.
     */
    ShmUnitInfo(int fd, uint64_t mmapSz);

    /**
     * @brief Constructor 3.
     * A constructor that builds a ShmUnit based on the inputs from a ShmView.
     * @param[in] id The id for the ShmUnitInfo.
     * @param[in] shmView The shmViewInfo to use as the source of some fields for the ShmUnitInfo.
     * @param[in] pointer The pointer to allocated data for the ShmUnitInfo (This ShmUnitInfo will not free it during
     * it's destructor.
     */
    ShmUnitInfo(ShmKey id, ShmView shmView, void *pointer);

    /**
     * @brief Destructor. Client-side ShmUnitInfo's do not own memory and will not free memory.
     */
    virtual ~ShmUnitInfo() = default;

    /**
     * @brief Returns shm id.
     * @return id of shared memory.
     */
    ShmKey GetId() const
    {
        return id;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return The reference count (if implemented by derived child. returns 0 if not implemented.
     */
    int32_t GetRefCount() const
    {
        return refCount;
    }

    /**
     * @brief A update function that a derived class can optionally implement.
     * (reference count will be increased by 1 if implemented by derived child)
     */
    void IncrementRefCount()
    {
        refCount++;
    }

    /**
     * @brief A update function that a derived class can optionally implement.
     * (reference count will be decreased by 1 if implemented by derived child)
     */
    void DecrementRefCount()
    {
        refCount--;
    }

    /**
     * @brief Get the fd.
     * @return The fd.
     */
    int GetFd() const
    {
        return fd;
    }

    /**
     * @brief Get the mmapSize.
     * @return The mmapSize.
     */
    uint64_t GetMmapSize() const
    {
        return mmapSize;
    }

    uint64_t GetSize() const
    {
        return size;
    }

    /**
     * @brief Get the offset.
     * @return The offset.
     */
    uint64_t GetOffset() const
    {
        return offset;
    }

    /**
     * @brief Get the pointer.
     * @return the pointer.
     */
    void *GetPointer() const
    {
        return pointer;
    }

    /**
     * @brief Set the offset.
     * @param[in] newOffset The new offset.
     */
    void SetOffset(const uint64_t newOffset)
    {
        offset = (ptrdiff_t)newOffset;
    }

    /**
     * @brief Copy data into this ShmUnitInfo. Contains sanity check to prevent memory overwrite if the source data
     * length exceeds the size of the memory in this ShmUnitInfo.
     * @param[in] src The source address to copy from.
     * @param[in] srcSize The amount of bytes to copy into this ShmUnitInfo.
     * @param[in] threadPool the thread pool that might be used for larger memcopies.
     */
    Status MemoryCopy(const uint8_t *src, uint64_t srcSize, const std::shared_ptr<ThreadPool> &threadPool) const;

    /**
     * @brief Copy data into this ShmUnitInfo. Contains sanity check to prevent memory overwrite if the source data
     * length exceeds the size of the memory in this ShmUnitInfo.
     * @param[in] payloads Vector of payload data to copy into the ShmUnitInfo (ptr,size pair).
     * @param[in] threadPool the thread pool that might be used for larger memcopies.
     * @param[in] off Relative off.
     */
    Status MemoryCopy(const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads,
                      const std::shared_ptr<ThreadPool> &threadPool, uint64_t off);

    // Shared memory mapped file contains the buffer.
    int fd = -1;

    // Shared memory mapped file mmap size.
    uint64_t mmapSize = 0;

    // Pointer to the buffer data.
    // Notice: The pointer position starts from the end of the previous buffer.
    // Example:
    //    |              buffer1      |    buffer2      |
    //    ^                           ^
    // ShmUnitInfo1.pointer             ShmUnitInfo2.pointer
    void *pointer = nullptr;

    // Offset from the base of the mmap.
    ptrdiff_t offset = 0;

    // Buffer size.
    uint64_t size = 0;

    // Number of clients currently use this buffer.
    std::atomic<int32_t> refCount = { 0 };

    // uuid
    ShmKey id;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SHARED_MEMORY_SHM_UNIT_H
