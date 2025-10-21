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

#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_ARENA_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_ARENA_H

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/shared_memory/memstat.h"
#include "datasystem/common/shared_memory/mmap/immap.h"
#include "datasystem/common/shared_memory/resource_pool.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
namespace memory {
constexpr uint32_t TENANT_RESOURCE_RELEASE_DELAY_MS = 600'000;  // 10min
class Arena;

class ArenaGroup {
public:
    ArenaGroup(std::vector<std::shared_ptr<Arena>> arenas, uint64_t maxSize, CacheType cacheType);
    ArenaGroup(const ArenaGroup &) = delete;
    ArenaGroup &operator=(const ArenaGroup &) = delete;
    ~ArenaGroup();

    /**
     * @brief Allocate memory from shared memory.
     * @param[in] size Memory size to be allocated in bytes.
     * @param[in] populate Indicate need populate or not.
     * @param[out] realSize Memory size real allocated in bytes.
     * @param[out] pointer Pointer to the allocated shared memory.
     * @param[out] fd File descriptor of the allocated shared memory segments.
     * @param[out] offset Offset from the base of the shared memory mmap.
     * @param[out] mmapSize Total size of shared memory segments.
     * @return K_OK if success, the error otherwise.
     *         K_OUT_OF_MEMORY: no enough memory can be allocated.
     *         K_RUNTIME_ERROR: arena has not been initialized, pointer is null, or
     *                          memory info can not be found, it should not happen.
     */
    Status AllocateMemory(uint64_t size, bool populate, uint64_t &realSize, void *&pointer, int &fd, ptrdiff_t &offset,
                          uint64_t &mmapSize);

    /**
     * @brief Free memory from shared memory.
     * @param[in] pointer Pointer to be free.
     * @param[out] bytesFree The free bytes.
     * @param[out] bytesRealFree The real free bytes.
     * @param[in] usedSize The size of memory used by the service type.
     * @return K_OK if success, the error otherwise.
     *         K_RUNTIME_ERROR: arena has not been initialized, pointer is null or pointer is not found.
     */
    Status FreeMemory(void *pointer, uint64_t &bytesFree, uint64_t &bytesRealFree, uint64_t usedSize);

    /**
     * @brief Free memory from shared memory.
     * @param[in] pointer Pointer to be free.
     * @return K_OK if success, the error otherwise.
     *         K_RUNTIME_ERROR: arena has not been initialized, pointer is null or pointer is not found.
     */
    Status FreeMemory(void *pointer);

    /**
     * @brief Get allocated size of arena.
     * @return Allocated size in bytes.
     */
    uint64_t GetMemoryUsage() const;

    /**
     * @brief Obtains the storeFd related pointer.
     * @param[in] fd Fd of related pooled memory.
     * @param[out] ptrMmapSz Pointer and mmapSz.
     * @return K_OK if success, the error otherwise.
     *         K_RUNTIME_ERROR: fd can not be found.
     */
    Status FdToPointer(int fd, std::pair<void *, uint64_t> &ptrMmapSz) const;

    /**
     * @brief Get Arena statistics view.
     * @param[out] shmMemStat Shared memory statistics.
     */
    void GetArenaGroupStat(ShmMemStat &shmMemStat);

    /**
     * @brief Get the arena id list.
     * @return The arena id list.
     */
    std::vector<uint32_t> GetArenaIds() const;

    /**
     * @brief Destroy all arena in this group.
     * @return Status of this call.
     */
    Status DestroyAll();

    /**
     * @brief Get all the fds of this arena group.
     * @return fds
     */
    std::vector<int> GetAllFds();

private:
    /**
     * @brief Allocate memory from the specified arena, and retry allocate from other arenas if OOM.
     * @param[in] retry Retry when OOM.
     * @param[in] populate Indicate need populate or not.
     * @param[in, out] size Read needed memory size in bytes and write actual allocated size.
     * @param[in, out] index The arena index.
     * @param[out] pointer Allocated memory pointer.
     * @return Status of this call.
     */
    Status AllocateMemoryImpl(bool retry, bool populate, uint64_t &size, size_t &index, void *&pointer);

    // The arena in this group. Not modified after creation, so no lock protection required.
    std::vector<std::shared_ptr<Arena>> arenas_;

    // The max memory size.
    const uint64_t maxSize_;

    // Arena group cache type.
    CacheType cacheType_;

    // The next arena index in arenas_ for call AllocateMemory.
    std::atomic<size_t> nextId_{ 0 };

    // Record the allocated size in bytes.
    std::atomic<uint64_t> memoryUsage_{ 0 };

    // Record the real allocated size in bytes.
    std::atomic<uint64_t> realMemoryUsage_{ 0 };

    // File handler - pointer record table. Not modified after creation, so no lock protection required
    std::unordered_map<int, std::pair<void *, uint64_t>> fdPointerTable_;

    // Protects 'allocatedTable_'.
    mutable std::shared_timed_mutex allocatedMutex_;

    struct AllocRecord {
        uint64_t size = 0;
        uint64_t realSize = 0;
        size_t index = 0;
    };

    // Record the mapping allocated pointer - AllocEntry.
    std::unordered_map<void *, AllocRecord> allocatedTable_;

    std::atomic<bool> destroyed_{ false };
};

class ArenaManager {
public:
    ArenaManager(bool populate, bool scaling, ssize_t decayMs);

    /**
     * @brief Init ArenaManager.
     * @param[in] devMemFuncRegister The register func for allocator.
     */
    void Init(DevMemFuncRegister devMemFuncRegister);

    /**
     * @brief Create an new arena group to allocate/deallocate shared memory.
     * @param[in] type The cache type.
     * @param[in] maxSize Shared memory max size in bytes.
     * @param[out] arenaGroup New created arena group.
     * @return K_OK if success, the error otherwise.
     *         K_RUNTIME_ERROR: Runtime error because of many things.
     */
    Status CreateArenaGroup(CacheType type, uint64_t maxSize, std::shared_ptr<ArenaGroup> &arenaGroup);

    /**
     * @brief Create an new arena group for the specific tenant to allocate/deallocate shared memory.
     * @param[in] tenantId The ID of the specific tenant.
     * @param[in] type The cache type.
     * @param[in] maxSize Shared memory max size in bytes.
     * @param[out] arenaGroup New created arena group.
     * @return K_OK if success, the error otherwise.
     *         K_RUNTIME_ERROR: Runtime error because of many things.
     */
    Status CreateArenaGroup(const std::string &tenantId, CacheType type, uint64_t maxSize,
                            std::shared_ptr<ArenaGroup> &arenaGroup);

    /**
     * @brief Get the Arena of the specific tenant.
     * @param[in] key The arena group key.
     * @param[out] arenaGroup The arena group reference.
     * @return Status
     */
    Status GetArenaGroup(const ArenaGroupKey &key, std::shared_ptr<ArenaGroup> &arenaGroup);

    /**
     * @brief Get the tenant arena or create an new arena group for the specific tenant to allocate/deallocate shared
     * memory.
     * @param[in] key The arena group key.
     * @param[in] maxSize Shared memory max size in bytes.
     * @param[out] arena New created arena.
     * @return K_OK if success, the error otherwise.
     *         K_RUNTIME_ERROR: Runtime error because of many things.
     */
    Status GetOrCreateArenaGroup(const ArenaGroupKey &type, uint64_t maxSize, std::shared_ptr<ArenaGroup> &arenaGroup);

    /**
     * @brief Destroy the arena group
     * @param[in] arenaGroup Arena group.
     * @return K_OK if success, the error otherwise.
     *         K_NOT_FOUND: arena can not be found.
     */
    Status DestroyArenaGroup(const std::shared_ptr<ArenaGroup> &arenaGroup);

    /**
     * @brief Destroy the arena group with a delay timer.
     * @param[in] key The arena group key.
     * @return Status of this call.
     */
    Status DestroyArenaGroup(const ArenaGroupKey &key);

    /**
     * @brief Destroy all arena group.
     * @return Status of this call.
     */
    Status DestroyAllArenaGroup();

    /**
     * @brief Get arena count.
     * @return Arena count.
     */
    uint32_t GetArenaCounts()
    {
        return numArenas_.load(std::memory_order_relaxed);
    }

    /**
     * @brief pre allocate virtual memory for each arena.
     * @param[in] arenaInds arena index
     * @param[in] maxSize maxSize of shared memory.
     */
    void FakeAllocate(CacheType type, const std::vector<uint64_t> &arenaInds, const uint64_t maxSize);

    ~ArenaManager();

    /**
     * @brief Set releaseable tenant.
     * @param[in] key The arena group key.
     */
    void SetReleaseableTenant(const ArenaGroupKey &key);

    /**
     * @brief Cancel expired tenant timer.
     * @param[in] key The arena group key.
     */
    void CancelExpiredTenantTimer(const ArenaGroupKey &key);

    /**
     * @brief Get all expired fds.
     * @return All expired fds.
     */
    std::set<int> GetAllExpiredFds();

    /**
     * @brief Check if the tenantId can access input workerFds or not.
     * @param[in] tenantId The tenant ID from request.
     * @param[in] workerFds The workerFds from request.
     * @return K_OK if success, the error otherwise.
     *         K_NOT_AUTHORIZED: There is workerFd that cannot be accessed by this tenant.
     */
    Status CheckWorkerFdTenant(const std::string &tenantId, const std::vector<int> &workerFds);

private:
    /**
     * @brief To align the size to an integer multiple of a page.
     * @param[in] size mmap size .
     * @return aligned size
     */
    uint64_t RoundUpToNextMultiple(uint64_t size);

    /**
     * @brief The alloc hook is invoked whenever the arena needs additional memory from the OS, e.g. when
     *        all local mapped memory are in use, and we need more memory to satisfy the current request
     *        (which is typical done through mmap).
     * @param[in] size Always a multiple of the page size in bytes.
     * @param[in] alignment Always a power of two at least as large as the page size.
     * @param[in] arenaInd Arena index.
     * @param[out] zero Indicate whether the extent is zeroed.
     * @param[out] commit Indicate whether the extent is committed.
     * @return A pointer to size bytes of mapped memory on behalf of arena.
     */
    static void *AllocHook(size_t size, size_t alignment, unsigned arenaInd, bool *zero, bool *commit);

    /**
     * @brief Destroys an extent at given addr and size with committed/decommited memory as indicated,
     *        on behalf of arena arenaInd. This function may be called to destroy retained extents
     *        during arena destruction (see arena.<i>.destroy).
     * @param[in] addr Deallocate address.
     * @param[in] size Deallocate size in bytes.
     * @param[in] committed Commit state.
     * @param[in] arenaInd Arena index.
     */
    static void DestroyHook(void *addr, size_t size, bool committed, unsigned arenaInd);

    /**
     * @brief Commits or decommits any physical memory to back pages within an extent at given addr and size at
     *        offset bytes, extending for length on behalf of arena arena_ind, returning false upon success.
     * @param[in] commit True indicates commit physical memory, False indicates decommits physical memory.
     * @param[in] addr The commit address.
     * @param[in] size Address size.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @param[in] arenaInd Arena index.
     * @return True indicates commit or decommit failed; false means success.
     */
    static bool CommitHook(bool commit, void *addr, size_t size, size_t offset, size_t length, unsigned arenaInd);

    template <typename F>
    Status DestroyArenas(const std::vector<uint32_t> &arenaIds, F &&f)
    {
        std::lock_guard<std::shared_timed_mutex> l(mutex_);
        LOG_IF_ERROR(f(), "Destroy Jemalloc arena failed");
        for (const auto arenaId : arenaIds) {
            if (arenas_[arenaId] == nullptr) {
                LOG(ERROR) << "Arena id not found: " << arenaId;
                continue;
            }
            arenas_[arenaId] = nullptr;
            (void)numArenas_.fetch_sub(1, std::memory_order_relaxed);
        }
        return Status::OK();
    }

    friend ArenaGroup;

    // Created arena count.
    std::atomic<uint32_t> numArenas_;

    // Arena list.
    std::vector<std::shared_ptr<Arena>> arenas_;

    struct PreReleaseTenantResourceInfo {
        bool IsExpired()
        {
            auto timeoutDurationMs = TENANT_RESOURCE_RELEASE_DELAY_MS;
            INJECT_POINT("PreReleaseTenantResourceInfo.IsExpired", [&timeoutDurationMs](int timeMs) {
                timeoutDurationMs = timeMs;
                return true;
            });
            return timer.ElapsedMilliSecond() > timeoutDurationMs;
        }

        Timer timer;
        ArenaGroupKey key;
        std::vector<int> fds;
        std::function<void()> releaseHandler = nullptr;
    };

    /**
     * @brief Get all fds by key.
     * @param[in] key The arena group key.
     */
    std::vector<int> GetAllFdsByKey(const ArenaGroupKey &key);

    /**
     * @brief Start check expired tenant resource thread.
     */
    void StartCheckExpiredTenantResource();

    /**
     * @brief Update expired tenant.
     * @param[in] expiredTenantsInfo The expired tenants' detail.
     */
    void UpdateExpiredTenant(const std::vector<PreReleaseTenantResourceInfo> &expiredTenantsInfo);

    std::shared_timed_mutex preReleaseTenantResourceInfoMapMutex_;
    std::unordered_map<ArenaGroupKey, PreReleaseTenantResourceInfo> preReleaseTenantResourceInfoMap_;

    std::unique_ptr<ThreadPool> handleExpiredTenantThread_;
    const int handleExpiredTenantThreadNum_ = 1;

    std::unordered_map<ArenaGroupKey, std::shared_ptr<ArenaGroup>> tenantArenas_;

    // Protects 'temamtArenas_' in create and destroy scenario.
    std::shared_timed_mutex tenantMutex_;

    // To ensure that the allocation of arenaId in Jemalloc and the management of our own tables is an atomic operation
    // and to avoid inconsistencies in the meta information managed.
    std::shared_timed_mutex mutex_;

    // Default arena list init size, the maximum number of arena that can be allocated by Jemalloc is 4096.
    const uint64_t ARENAS_INIT_SIZE = 4096;
    const static int HUGE_PAGE_SIZE = 2 * 1024 * 1024;
    // Record arena parameters for future arena init
    bool populate_{ false };

    bool scaling_{ false };

    ssize_t decayMs_;

    std::atomic<bool> destroyed_{ false };
    WaitPost destroyWaitPost_;
    uint64_t maxTenantSize_ = 0;

    DevMemFuncRegister devMemFuncRegister_;
};

class Arena {
public:
    Arena(uint32_t arenaId, void *handler, bool populate, bool scaling, uint64_t mmapSize,
          CacheType cacheType = CacheType::MEMORY);

    ~Arena() noexcept;

    /**
     * @brief Arena initialize.
     * @param[in] devMemFuncRegister The register func for allocator.
     * @return K_OK if success, the error otherwise.
     */
    Status Init(DevMemFuncRegister devMemFuncRegister);

    /**
     * @brief Get mmap entry info via pointer, it will traverses 'mmapEntryTable_' to find the
     *        MmapEntry which the pointer belongs (via pointer address), and return the info
     *        such as fd, offset and mmap size.
     * @param[in] pointer Pointer to search for the MmapEntry.
     * @param[out] fd Shared memory file fd.
     * @param[out] offset Offset from the base of the mmap.
     * @param[out] mmapSize Shared memory mmap size in bytes.
     * @return Status of the call.
     */
    Status GetAllocInfo(void *pointer, int &fd, ptrdiff_t &offset, uint64_t &mmapSize);

    /**
     * @brief Destroy Jemalloc arena.
     * @return Status of this call.
     */
    Status DestroyArena();

    /**
     * @brief Return arena id.
     * @return Arena ID.
     */
    uint32_t GetArenaId() const
    {
        return arenaId_;
    }

    /**
     * @brief Return the mmap fd.
     * @return Fd.
     */
    uint32_t GetFd() const
    {
        return mmap_->Fd();
    }

    /**
     * @brief Get the actual physical memory usage.
     * @return Physical memory usage in this arena.
     */
    uint64_t GetPhysicalMemoryUsage()
    {
        return physicalMemoryStats_.RealUsage();
    }

    /**
     * @brief Increase the real memory usage.
     * @param[in] realSize The memory size.
     */
    void AddRealMemoryUsage(uint64_t realSize);

    /**
     * @brief Decrease the real memory usage.
     * @param[in] realSize The memory size.
     */
    void SubRealMemoryUsage(uint64_t realSize);

    /**
     * @brief Get the real memory usage not include the jemalloc cache.
     * @return Real memory usage in this arena.
     */
    uint64_t GetRealMemoryUsage()
    {
        return realMemoryUsage_;
    }

    /**
     * @brief Get the mmap pointer.
     * @return The mmap pointer
     */
    void *GetPointer()
    {
        return mmap_->Pointer();
    }

    /**
     * @brief Get the mmap memory size.
     * @return The mmap memory size.
     */
    uint64_t GetMmapSize()
    {
        return mmapSize_;
    }

private:
    /**
     * @brief Jemalloc extent allocate hook, The alloc hook is invoked whenever the arena needs additional
     *        memory from the OS, e.g. when all local mapped memory are in use, and we need more memory to
     *        satisfy the current request (which is typical done through mmap).
     * @param[in] size Always a multiple of the page size in bytes.
     * @param[in] alignment Always a power of two at least as large as the page size.
     * @param[out] zero Indicate whether the extent is zeroed.
     * @param[out] commit Indicate whether the extent is committed.
     * @return A pointer to size bytes of mapped memory on behalf of arena.
     */
    void *AllocHook(size_t size, size_t alignment, bool *zero, bool *commit);

    /**
     * @brief Destroys an Jemalloc extent at given addr and size with committed/decommited memory as indicated,
     *        returning false upon success. If the function returns true, this indicates opt-out from deallocation;
     *        the memory mapping associated with the extent remains mapped, in the same commit state, and
     *        available for future use, in which case it will be automatically retained for later reuse.
     * @param[in] addr Deallocate address.
     * @param[in] size Deallocate size in bytes.
     * @param[in] committed Commit state.
     * @return False upon success; True means remains mapped, in the same commit state, and available for future use.
     */
    void DestroyHook(void *addr, size_t size, bool committed);

    /**
     * @brief Commits or decommits any physical memory to back pages within an extent at given addr and size at
     *        offset bytes, extending for length on behalf of arena arena_ind, returning false upon success.
     * @param[in] commit True indicates commit physical memory, False indicates decommits physical memory.
     * @param[in] addr The commit address.
     * @param[in] size Address size.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @return True indicates commit or decommit failed; false means success.
     */
    bool CommitHook(bool commit, void *addr, size_t size, size_t offset, size_t length);

    /**
     * @brief The Commit implement.
     * @param[in] addr The commit address.
     * @param[in] size Address size.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @return True indicates insufficient physical memory to satisfy the request; false means commit success.
     */
    bool CommitImpl(void *addr, size_t offset, size_t length);

    /**
     * @brief The Decommit implement.
     * @param[in] addr The decommit address.
     * @param[in] size Address size.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @return True indicates remains committed and available for future use; false means decommit success.
     */
    bool DecommitImpl(void *addr, size_t offset, size_t length);

    /**
     * @brief Increase the actual physical memory usage.
     * @param[in] size The memory size.
     * @return True indicates memory limit is not exceeded.
     */
    bool AddPhysicalMemeoryUsage(uint64_t size);

    /**
     * @brief Decrease the actual physical memory usage.
     * @param[in] size The memory size.
     */
    void SubPhysicalMemeoryUsage(uint64_t size);

    friend ArenaManager;

    // Page size.
    static uint64_t pageSize_;

    // Arena index assigned by Jemalloc.
    uint32_t arenaId_;

    // Jemalloc extent hooks handler.
    void *handler_;

    // Pre-populate page tables for shared memory, which avoids work when accessing the
    // pages later. But it may causes long pauses in shared memory initialization.
    bool populate_;

    // Enable scaling or not.
    bool scaling_;

    // Indicates if jemalloc arena has been destroyed.
    std::atomic<bool> destroyed_;

    // Arena mmap size.
    uint64_t mmapSize_;

    // Record the real allocated size in bytes.
    std::atomic<uint64_t> realMemoryUsage_{ 0 };

    // Allocated count.
    std::atomic<uint64_t> allocatedCount_{ 0 };

    // Record the shared memory already binding to physical memory.
    ResourcePool physicalMemoryStats_;

    std::atomic<bool> firstFakeHook_{ true };

    // Arena mmap's cache type.
    CacheType cacheType_;

    // Arena mmap to allocate and manage memory.
    std::unique_ptr<IMmap> mmap_;
};
}  // namespace memory
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SHARED_MEMORY_ARENA_H