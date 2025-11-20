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
 * Description: Define the basic unit of the shared memory in the server side.
 * Support allocate and free shared memory.
 */

#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_SHM_UNIT_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_SHM_UNIT_H

#include "datasystem/common/shared_memory/arena.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/shared_memory/shm_unit_info.h"

namespace datasystem {
class ShmOwner;

class ShmUnit : public ShmUnitInfo {
public:
    /**
     * @brief Constructor 1 (default).
     * Initializes all fields to their defaults.
     * ShmUnit is mainly used in the server side which need to allocate or free shared memory.
     * If you don't need to allocate or free shared memory, use ShmUnitInfo directly.
     */
    ShmUnit() = default;

    /**
     * @brief Constructor 2.
     * Similar to constructor 1, specify the fd and mmapSize.
     * @param[in] fd The file descriptor for a memory mapped region.
     * @param[in] mmapSz The size of the memory mapped region.
     */
    ShmUnit(int fd, uint64_t mmapSz);

    /**
     * @brief Constructor 3.
     * A constructor that builds a ShmUnit based on the inputs from a ShmView.
     * @param[in] id The id for the ShmUnit.
     * @param[in] shmView The shmView to use as the source of some fields for the ShmUnit.
     * @param[in] pointer The pointer to allocated data for the ShmUnit (This ShmUnit shall be responsible to free it
     * during it's destructor.
     */
    ShmUnit(ShmKey id, ShmView shmView, void *pointer);

    /**
     * @brief Destructor. ShmUnit own memory and will clean themself up and free the memory that they own.
     */
    ~ShmUnit();

    /**
     * @brief An explicit call to release the ShmUnit's memory resources.
     * @return status of the call.
     */
    Status FreeMemory();

    /**
     * @brief Allocates the requested size of bytes into the ShmUnit.
     * @param[in] tenantId The Id of the tenant owns the shm unit.
     * @param[in] needSize The requested size in bytes to allocate.
     * @param[in] populate Indicate need populate or not.
     * @param[in] serviceType The type of datasystem service for this allocation request.
     * @param[in] cacheType The cache type.
     * @return Status of the call.
     */
    Status AllocateMemory(const std::string &tenantId, uint64_t needSize, bool populate,
                          ServiceType serviceType = ServiceType::OBJECT,
                          memory::CacheType cacheType = memory::CacheType::MEMORY);

    /**
     * @brief Get shared memory info.
     * @return Shared memory info view.
     */
    ShmView GetShmView();

    /**
     * @brief Get the Tenant Id object.
     * @return tenantId.
     */
    std::string GetTenantId();

    /**
     * @brief Set a hard free memory flag to this shm unit. It usually occurs
     *        when non-populate memory is applied for and is not used.
     */
    void SetHardFreeMemory();

private:
    friend class ShmOwner;

    ServiceType serviceType_ = ServiceType::OBJECT;

    memory::CacheType cacheType_ = memory::CacheType::MEMORY;

    std::string tenantId_;

    bool needHardFree_ = false;

    std::shared_ptr<ShmOwner> shmOwner_{ nullptr };
};

class ShmOwner : public ShmUnit, public std::enable_shared_from_this<ShmOwner> {
public:
    /**
     * @brief Distribute allocated shared memory into the ShmUnit.
     * @param[in] shmSize The required shared memory size.
     * @param[out] shmUnit The shared memory unit.
     * @return Status of the call.
     */
    Status DistributeMemory(uint64_t shmSize, ShmUnit &shmUnit);

private:
    /**
     * @brief Move up the cursor in ShmOwner to indicate some memory is distributed.
     * @param[in] shmSize The required shared memory size.
     * @return cursor position before the increment.
     */
    uint64_t AllocatePosition(uint64_t shmSize);

    std::atomic<uint64_t> cursor_{ 0 };
};

/**
 * @brief Helper function to align size to 4 bits ceiling.
 * @param[in] size The shared memory size to align.
 * @return Aligned size.
 */
uint64_t Align4BitsCeiling(uint64_t size);
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SHARED_MEMORY_SHM_UNIT_H
