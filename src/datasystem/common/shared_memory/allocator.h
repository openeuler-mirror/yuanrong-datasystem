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
 * Description: Shared memory compatibility interface.
 */
#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_ALLOCATOR_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_ALLOCATOR_H

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/constants.h"
#include "datasystem/common/shared_memory/arena.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/shared_memory/resource_pool.h"
#include "datasystem/common/shared_memory/shared_disk_detecter.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace memory {
class Allocator {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Pointer of Allocator.
     */
    static Allocator *Instance();

    ~Allocator() noexcept;

    /**
     * @brief Pre allocate shared memory size. The method will create a temp
     *        file in /dev/shm directory, mmap it, truncate its size to what
     *        we want and then delete the temp file immediately (because we
     *        don't want to maintain the shared memory file).
     * @param[in] shmSize shared memory size in bytes.
     * @param[in] shdSize shared disk size in bytes.
     * @param[in] populate Shared memory need pre-populate or not.
     * @param[in] scaling Shared memory need scaling or not.
     * @param[in] decayMs Decay clean dirty pages milliseconds.
     * @param[in] objectThreshold A limit to restrict the memory usage of object cache / kv service.
     * @param[in] streamThreshold A limit to restrict the memory usage of stream cache service.
     * @return Status of the call.
     */
    Status Init(uint64_t shmSize, uint64_t shdSize = 0, bool populate = false, bool scaling = true,
                ssize_t decayMs = 5'000, int objectThreshold = 100, int streamThreshold = 100);

    /**
     * @brief Pre allocate device memory size. The method will create a devHost mem and devDevice mem.
     * @param[in] devSize Device size in bytes.
     * @param[in] memFuncRegister The allocate and destroy func for dev_allocator.
     * @param[in] populate Memory need pre-populate or not.
     * @param[in] scaling Memory need scaling or not.
     * @param[in] decayMs Decay clean dirty pages milliseconds.
     * @return Status of the call.
     */
    Status InitWithoutShm(uint64_t devDevSize, uint64_t devHostSize, DevMemFuncRegister memFuncRegister,
                          bool populate = false, bool scaling = true, ssize_t decayMs = 5'000);
    /**
     * @brief Shutdown allocator.
     */
    void Shutdown();

    /**
     * @brief Create an new arena group to allocate/deallocate shared memory.
     * @param[in] maxSize Shared memory max size in bytes.
     * @param[out] arenaGroup The arena group reference.
     * @param[in] cacheType The cache type.
     * @return K_OK if success, the error otherwise.
     *         K_RUNTIME_ERROR: Runtime error because of many things.
     */
    Status CreateArenaGroup(uint64_t maxSize, std::shared_ptr<ArenaGroup> &arenaGroup,
                            CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Create an new arena to allocate/deallocate shared memory for the specific tenant.
     * @param[in] tenantId The tenant that need to create an arena.
     * @param[in] maxSize Shared memory max size in bytes.
     * @param[out] arenaGroup The arena group reference.
     * @param[in]  cacheType The cache type.
     * @return K_OK if success, the error otherwise.
     *         K_RUNTIME_ERROR: Runtime error because of many things.
     */
    Status CreateArenaGroup(const std::string &tenantId, uint64_t maxSize, std::shared_ptr<ArenaGroup> &arenaGroup,
                            CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Destroy the arena group for the specific tenant.
     * @param[in] key The key of the arena group need to destroy its arena.
     * @return Status
     */
    Status DestroyArenaGroup(const ArenaGroupKey &key);

    /**
     * @brief Increase the memory usage for the given service type.
     * @param[in] needSize Memory size to be allocated in bytes.
     * @param[in] serviceType The type of datasystem service for which memory usage is increased.
     * @param[in] cacheType The cache type.
     * @return Status of the call.
     */
    Status IncrementMemoryUsage(uint64_t needSize, ServiceType serviceType, CacheType cacheType);

    /**
     * @brief Allocate memory from shared memory for the specific tenant.
     * @param[in] tenantId The tenant that need to allocate memory.
     * @param[in] needSize Memory size to be allocated in bytes.
     * @param[in] populate Indicate need populate or not.
     * @param[out] pointer Pointer to the allocated shared memory.
     * @param[out] fd File descriptor of the allocated shared memory segments.
     * @param[out] offset Offset from the base of the shared memory mmap.
     * @param[out] mmapSize Total size of shared memory segments.
     * @param[in] serviceType The type of datasystem service for this allocation request.
     * @param[in] cacheType The cache type, either MEMORY or DISK.
     * @return Status of the call.
     */
    Status AllocateMemory(const std::string &tenantId, uint64_t needSize, bool populate, void *&pointer, int &fd,
                          ptrdiff_t &offset, uint64_t &mmapSize, ServiceType serviceType = ServiceType::OBJECT,
                          CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Free memory from shared memory.
     * @param[in] pointer reference to the pointer to free. Sets pointer to nullptr after.
     * @param[in] type The service type for which memory is getting freed.
     * @return Status of the call.
     */
    Status FreeMemory(void *&pointer, ServiceType type = ServiceType::OBJECT);

    /**
     * @brief Free memory from shared memory for the specific tenant.
     * @param[in] tenantId The Id of the tenant which to free its memory.
     * @param[in] pointer reference to the pointer to free. Sets pointer to nullptr after.
     * @param[in] type The service type for which memory is getting freed.
     * @param[in] cacheType The cache type, either MEMORY or DISK.
     * @return Status of the call.
     */
    Status FreeMemory(const std::string &tenantId, void *&pointer, ServiceType serviceType = ServiceType::OBJECT,
                      CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Get max memory size for the requested service type.
     * @param[in] serviceType The service type for which the max memory size is requested.
     * @param[in] cacheType The cache type.
     * @return max memory size in bytes for the requested type.
     */
    uint64_t GetMaxMemorySize(ServiceType serviceType = ServiceType::OBJECT,
                              CacheType cacheType = CacheType::MEMORY) const;

    /**
     * @brief Get the Max Memory Limit size.
     * @param[in] cacheType The cache type.
     * @return uint64_t The max memory limit size.
     */
    uint64_t GetMaxMemoryLimit(CacheType cacheType = CacheType::MEMORY) const;

    /**
     * @brief Obtains the allocated memory size.
     * @param[in] tenantId The Id of the tenant which to get the allocated size.
     * @param[in] cacheType The cache type.
     * @return allocated memory size in bytes. Note that return 0 could also means there is no arena for this tenant.
     */
    uint64_t GetMemoryUsage(const std::string &tenantId = DEFAULT_TENANT_ID, CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Obtains the storeFd related pointer.
     * @param[in] tenantId The key of the arena group which to obtain the storeFd related pointer.
     * @param[in] fd Fd of related pooled memory.
     * @param[out] ptrMmapSz Pointer and mmapSz.
     * @return Status of the call.
     */
    Status FdToPointer(const ArenaGroupKey &key, int fd, std::pair<void *, uint64_t> &ptrMmapSz) const;

    /**
     * @brief Obtains the storeFd related pointer.
     * @param[in] fd Fd of related pooled memory.
     * @param[out] ptrMmapSz Pointer and mmapSz.
     * @return Status of the call.
     */
    Status FdToPointer(int fd, std::pair<void *, uint64_t> &ptrMmapSz) const;

    /**
     * @brief Obtains the storeFd related pointer.
     * @param[out] shmMemStat Shared memory statistics.
     * @return Status of the call.
     */
    Status GetMemStat(ShmMemStat &shmMemStat);

    /**
     * @brief Change the reference page count.
     * @param[in] num The amount to add by.
     */
    void ChangeRefPageCount(int64_t num);

    /**
     * @brief Change the no reference page count.
     * @param[in] num The amount to add by.
     */
    void ChangeNoRefPageCount(int64_t num);

    /**
     * @brief Get the total used physical memory.
     * @param[in] cacheType The cache type.
     * @return The total used physical memory.
     */
    uint64_t GetTotalPhysicalMemoryUsage(CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Get the total memory usage for the given service type.
     * @param[in] serviceType The service type for which total memory usage is requested.
     * @param[in] cacheType The cache type.
     * @return The total memory usage.
     */
    uint64_t GetTotalMemoryUsage(ServiceType serviceType = ServiceType::OBJECT, CacheType cacheType = CacheType::MEMORY)
    {
        return GetResourcePoolByType(serviceType, cacheType)->Usage();
    }

    /**
     * @brief Get the total real memory usage.
     * @param[in] serviceType The service type for which total real memory usage is requested.
     * @param[in] cacheType The cache type.
     * @return The total real memory usage.
     */
    uint64_t GetTotalRealMemoryUsage(ServiceType serviceType = ServiceType::OBJECT,
                                     CacheType cacheType = CacheType::MEMORY)
    {
        return GetResourcePoolByType(serviceType, cacheType)->RealUsage();
    }

    /**
     * @brief Get the total real memory limit.
     * @param[in] type The service type for which total real memory limit is requested.
     * @return The total real memory limit.
     */
    uint64_t GetTotalMemoryLimit(ServiceType type = ServiceType::OBJECT) const
    {
        if (type == ServiceType::OBJECT) {
            return std::min(objectMemoryStats_->FootprintLimit(),
                            physicalMemoryStats_->FootprintLimit() - streamMemoryStats_->RealUsage());
        }
        return std::min(streamMemoryStats_->FootprintLimit(),
                        physicalMemoryStats_->FootprintLimit() - objectMemoryStats_->RealUsage());
    }

    /**
     * @brief Get the total real memory free to be allocated.
     * @param[in] cacheType The cache type.
     * @return The total real free memory.
     */
    uint64_t GetTotalRealMemoryFree(CacheType cacheType = CacheType::MEMORY)
    {
        uint64_t limit;
        uint64_t realUsage;
        if (cacheType == CacheType::DISK) {
            limit = diskStats_->FootprintLimit();
            realUsage = diskStats_->RealUsage();
        } else {
            limit = physicalMemoryStats_->FootprintLimit();
            realUsage = objectMemoryStats_->RealUsage() + streamMemoryStats_->RealUsage();
        }
        return limit > realUsage ? limit - realUsage : 0;
    }

    /**
     * @brief Get the memory available to high water mark.
     * @return The memory available to high water mark.
     */
    uint64_t GetMemoryAvailToHighWater() const;

    /**
     * @brief Get the memory available ratio based on the cache type.
     * @param[in] cacheType The cache type, either MEMORY or DISK.
     * @return The memory available ratio as a percentage (0.0 to 100.0), representing the available memory.
     */
    double GetMemoryAvailableRatio(CacheType cacheType = CacheType::MEMORY)
    {
        uint64_t limit;
        uint64_t realUsage;

        if (cacheType == CacheType::DISK) {
            limit = diskStats_->FootprintLimit();
            realUsage = diskStats_->RealUsage();
        } else {
            limit = physicalMemoryStats_->FootprintLimit();
            realUsage = objectMemoryStats_->RealUsage() + streamMemoryStats_->RealUsage();
        }

        if (limit == 0) {
            return 0.0;
        }

        uint64_t available = limit - realUsage;
        double ratio = (static_cast<double>(available) / static_cast<double>(limit)) * 100.0;

        return ratio;
    }

    /**
     * @brief Get the arena manager pointer.
     * @return the arena manager pointer.
     */
    ArenaManager *GetArenaManager()
    {
        return arenaManager_.get();
    }

    /**
     * @brief Obtains the usage of shared memory.
     * @return Usage:
     * "memoryUsage/physicalMemoryUsage/totalLimit/workerShareMemoryUsage/streamMemoryUsage/streamMemoryLimit"
     */
    std::string GetMemoryStatistics()
    {
        if (physicalMemoryStats_->FootprintLimit() == 0) {
            return "0/0/0/0/0/0";
        }
        auto objectMemoryUsage = objectMemoryStats_->RealUsage();
        auto streamMemoryUsage = streamMemoryStats_->RealUsage();
        auto memoryUsage = objectMemoryUsage + streamMemoryUsage;
        auto workerShareMemoryUsage = memoryUsage / static_cast<float>(physicalMemoryStats_->FootprintLimit());
        auto streamMemoryLimit = GetTotalMemoryLimit(ServiceType::STREAM);
        return FormatString("%lu/%lu/%lu/%.3f/%lu/%lu", memoryUsage, physicalMemoryStats_->RealUsage(),
                            physicalMemoryStats_->FootprintLimit(), workerShareMemoryUsage, streamMemoryUsage,
                            streamMemoryLimit);
    }

    /**
     * @brief Get the shared disk statistics.
     * @return std::string format: "usage/physicalUsage/totalLimit/rate".
     */
    std::string GetSharedDiskStatistics()
    {
        if (diskStats_->FootprintLimit() == 0) {
            return "0/0/0/0";
        }
        return FormatString("%lu/%lu/%lu/%.3f", diskStats_->RealUsage(), physicalDiskStats_->RealUsage(),
                            diskStats_->FootprintLimit(),
                            diskStats_->RealUsage() / static_cast<float>(diskStats_->FootprintLimit()));
    }

    /**
     * @brief Get all expired fds.
     * @return All expired fds.
     */
    std::set<int> GetAllExpiredFds();

    /**
     * @brief Set a callback function for checking whether the fd has released by client.
     * @param[in] f The callback function.
     */
    void SetCheckIfAllFdReleasedHandler(std::function<bool(const std::vector<int> &)> f)
    {
        checkIfAllFdReleasedHandler_ = std::move(f);
    }

    bool CheckIfAllFdReleasedHandler(const std::vector<int> &fds)
    {
        return checkIfAllFdReleasedHandler_ == nullptr ? false : checkIfAllFdReleasedHandler_(fds);
    }

    /**
     * @brief Check if the tenantId can access input workerFds or not.
     * @param[in] tenantId The tenant ID from request.
     * @param[in] workerFds The workerFds from request.
     * @return K_OK if success, the error otherwise.
     *         K_NOT_AUTHORIZED: There is workerFd that cannot be accessed by this tenant.
     */
    Status CheckWorkerFdTenant(const std::string &tenantId, const std::vector<int> &workerFds);

    /**
     * @brief Check disk buffer is available.
     * @return True if disk is available.
     */
    bool IsDiskAvailable();

private:
    /**
     * @brief Increase the actual physical memory usage.
     * @param[in] type The cache type.
     * @param[in] size The memory size.
     * @return True indicates memory limit is not exceeded.
     */
    bool AddTotalPhysicalMemoryUsage(CacheType type, uint64_t size);

    /**
     * @brief Decrease the actual physical memory usage.
     * @param[in] type The cache type.
     * @param[in] size The memory size.
     */
    void SubTotalPhysicalMemoryUsage(CacheType type, uint64_t size);

    /**
     * @brief Get the ResourcePool by type.
     * @param[in] serviceType The service type.
     * @param[in] cacheType The cache type.
     * @return ResourcePool* The ResourcePool.
     */
    ResourcePool *GetResourcePoolByType(ServiceType serviceType, CacheType cacheType) const;

    /**
     * @brief Get the ResourcePool by type.
     * @param[in] serviceType The service type.
     * @param[in] cacheType The cache type.
     * @return ResourcePool* The ResourcePool.
     */
    ResourcePool *GetPhyResourcePoolByType(CacheType cacheType) const;

    /**
     * @brief Init shared memory.
     * @param[in] size The shared memory size.
     * @param[in] objectThreshold A limit to restrict the memory usage of object cache / kv service.
     * @param[in] streamThreshold A limit to restrict the memory usage of stream cache service.
     * @return Status K_OK if success, the error otherwise.
     */
    Status InitSharedMemory(uint64_t size, int objectThreshold, int streamThreshold);

    /**
     * @brief Init shared disk.
     * @param[in] size The shared disk size.
     * @return Status K_OK if success, the error otherwise.
     */
    Status InitSharedDisk(uint64_t size);

    /**
     * @brief Init device mem pool.
     * @param[in] size The shared disk size.
     * @return Status K_OK if success, the error otherwise.
     */
    Status InitDevMemory(uint64_t devDevSize, uint64_t devHostSize);

    friend Arena;

    Allocator() = default;

    // The arena manager.
    std::unique_ptr<ArenaManager> arenaManager_ = nullptr;

    // The count of reference page count.
    std::atomic<int64_t> refPageCount_{ 0 };

    // The count of reference page count.
    std::atomic<int64_t> noRefPageCount_{ 0 };

    // Record the shared memory already binding to physical memory.
    // physicalMemoryStats_.realUsage_ = realUsage_(object+stream) + Size of the cache memory that has not been released
    // after the memory is free.
    std::unique_ptr<ResourcePool> physicalMemoryStats_;

    // Record the shared disk already binding to physical disk.
    std::unique_ptr<ResourcePool> physicalDiskStats_;

    // Record the shared disk allocated in bytes.
    std::unique_ptr<ResourcePool> diskStats_;

    // Record the device allocated in bytes.
    std::unique_ptr<ResourcePool> devDeviceMemStats_;
    std::unique_ptr<ResourcePool> devHostMemStats_;

    // Number of the memory block allocated by allocator.
    std::atomic<uint64_t> totalNumOfAllocated_{ 0 };

    // Record the shared memory real allocated in bytes among all arenas for different service types.
    // realUsage = usage + Additional memory for jemalloc alignment
    std::unique_ptr<ResourcePool> objectMemoryStats_;
    std::unique_ptr<ResourcePool> streamMemoryStats_;

    std::function<bool(const std::vector<int> &)> checkIfAllFdReleasedHandler_;

    std::unique_ptr<SharedDiskDetecter> diskDetecter_;
};

void DeallocateForZmqFree(void *data, void *hint = nullptr);

}  // namespace memory
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SHARED_MEMORY_ALLOCATOR_H
