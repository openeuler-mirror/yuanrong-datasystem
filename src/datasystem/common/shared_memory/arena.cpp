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

#include "datasystem/common/shared_memory/arena.h"

#include <algorithm>
#include <cstdint>
#include <limits>

#include "datasystem/common/shared_memory/arena_group_key.h"

#ifdef __linux__
#include <linux/memfd.h>
#endif
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/jemalloc.h"
#include "datasystem/common/shared_memory/mmap/flexible_mmap.h"
#include "datasystem/common/shared_memory/mmap/disk_mmap.h"
#include "datasystem/common/shared_memory/mmap/mem_mmap.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"

DS_DEFINE_uint32(
    arena_per_tenant, 16,
    "The arena count for each tenant. Multiple arenas can improve the performance of share memory allocation for "
    "the first time, but each arena will use one more fd. The valid range is 1 to 32.");
DS_DEFINE_validator(arena_per_tenant, &Validator::ValidateArenaPerTenant);
DS_DECLARE_bool(enable_huge_tlb);
DS_DEFINE_uint32(
    shared_disk_arena_per_tenant, 8,
    "The number of disk cache Arena for each tenant. Multiple arenas can improve the performance of shared "
    "disk allocation for "
    "the first time, but each arena will use one more fd. The valid range is 0 to 32.");
DS_DEFINE_validator(shared_disk_arena_per_tenant, &Validator::ValidateSharedDiskArenaPerTenant);
DS_DECLARE_string(shared_disk_directory);
DS_DECLARE_bool(enable_fallocate);

namespace datasystem {
namespace memory {
thread_local bool g_NeedFallocate = false;
thread_local uint64_t g_RequestSize = 0;
thread_local bool g_FakeAllocate = false;  // fake allocate only need allochook, no need to commit.

inline uintptr_t AlignCeiling(uintptr_t addr, uintptr_t alignment)
{
    return (addr + alignment - 1) & ~(alignment - 1);
}

ArenaGroup::ArenaGroup(std::vector<std::shared_ptr<Arena>> arenas, uint64_t maxSize, CacheType cacheType)
    : arenas_(std::move(arenas)), maxSize_(maxSize), cacheType_(cacheType)
{
    for (const auto &arena : arenas_) {
        fdPointerTable_.emplace(arena->GetFd(), std::make_pair(arena->GetPointer(), arena->GetMmapSize()));
    }
}

ArenaGroup::~ArenaGroup()
{
    LOG(INFO) << "ArenaGroup destructor.";
    LOG_IF_ERROR(DestroyAll(), "Destroy arenas failed");
}

Status ArenaGroup::AllocateMemory(uint64_t size, bool populate, uint64_t &realSize, void *&pointer, int &fd,
                                  ptrdiff_t &offset, uint64_t &mmapSize, ServiceType type)
{
    CHECK_FAIL_RETURN_STATUS(!destroyed_.load(), StatusCode::K_RUNTIME_ERROR, "ArenaGroup destroyed");
    CHECK_FAIL_RETURN_STATUS(!arenas_.empty(), StatusCode::K_RUNTIME_ERROR, "arenas_ is empty");

    if (size > maxSize_) {
        RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, "Upper to the size limit");
    }
    if (memoryUsage_.fetch_add(size, std::memory_order_relaxed) > (maxSize_ - size)) {
        (void)memoryUsage_.fetch_sub(size, std::memory_order_relaxed);
        RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, "Upper to the size limit");
    }

    auto index = nextId_.fetch_add(1) % arenas_.size();

    auto func = [&index, this](bool populate, uint64_t &size, void *&pointer, int &fd, ptrdiff_t &offset,
                               uint64_t &mmapSize) {
        RETURN_IF_NOT_OK(AllocateMemoryImpl(true, populate, size, index, pointer));
        return arenas_[index]->GetAllocInfo(pointer, fd, offset, mmapSize);
    };
    realSize = size;
    Status status = func(populate, realSize, pointer, fd, offset, mmapSize);
    if (status.IsError()) {
        (void)memoryUsage_.fetch_sub(size, std::memory_order_relaxed);
        const int logFreq = 100;
        LOG_EVERY_N(ERROR, logFreq) << "total size limit:" << Allocator::Instance()->GetMaxMemorySize(type, cacheType_)
                                    << ", total physical memory usage:"
                                    << Allocator::Instance()->GetTotalPhysicalMemoryUsage(cacheType_)
                                    << ", total real memory usage:"
                                    << Allocator::Instance()->GetTotalRealMemoryUsage(type, cacheType_)
                                    << ", total memory usage:"
                                    << Allocator::Instance()->GetTotalMemoryUsage(type, cacheType_)
                                    << ", try alloc size:" << size << ", cacheType:" << static_cast<int>(cacheType_);
        return status;
    }

    {
        std::lock_guard<std::shared_timed_mutex> l(allocatedMutex_);
        allocatedTable_.emplace(pointer, AllocRecord{ size, realSize, index });
    }
    auto arena = arenas_[index];
    arena->AddRealMemoryUsage(realSize);
    (void)realMemoryUsage_.fetch_add(realSize, std::memory_order_relaxed);
    VLOG(1) << "[Allocator] Arena " << arena->GetArenaId() << " allocate require size: " << size
            << ", real size: " << realSize << ", offset: " << offset;
    return Status::OK();
}

Status ArenaGroup::AllocateMemoryImpl(bool retry, bool populate, uint64_t &size, size_t &index, void *&pointer)
{
    CHECK_FAIL_RETURN_STATUS(!arenas_.empty(), StatusCode::K_RUNTIME_ERROR, "arenas_ is empty");
    CHECK_FAIL_RETURN_STATUS(arenas_.size() > index, StatusCode::K_RUNTIME_ERROR, "index is bigger than arenas_ size");
    using clock = std::chrono::steady_clock;
    g_NeedFallocate = populate;
    g_RequestSize = size;
    auto arenaCount = arenas_.size();
    auto arena = arenas_[index];
    auto arenaId = arena->GetArenaId();
    auto beginTime = clock::now();
    auto status = Jemalloc::Allocate(arenaId, size, pointer);
    if (status.GetCode() == StatusCode::K_OUT_OF_MEMORY) {
        auto it = CACHE_TYPE_STR.find(cacheType_);
        std::string errHint = "UnknowType";
        if (it != CACHE_TYPE_STR.end()) {
            errHint = it->second;
        }
        std::string errorMsg = FormatString("%s no space in arena: %d", errHint, arenaId);
        status = Status(StatusCode::K_OUT_OF_MEMORY, errorMsg);
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - beginTime).count();
    PerfPoint::RecordElapsed(status.IsOk() ? PerfKey::JEMALLOC_ALLOCATE_SUCCESS : PerfKey::JEMALLOC_ALLOCATE_FAIL,
                             elapsed);
    if (!retry || arenaCount == 1 || status.GetCode() != StatusCode::K_OUT_OF_MEMORY) {
        return status;
    }

    VLOG(1) << "alloc from arena " << arena->GetArenaId()
            << " failed, fallocate size:" << arena->GetPhysicalMemoryUsage()
            << ", real allocate size:" << arena->GetRealMemoryUsage();

    // try to allocate in other arena.
    for (auto idx = index + 1; idx < index + arenaCount; idx++) {
        auto nextIndex = idx % arenaCount;
        arena = arenas_[nextIndex];
        if (arena->GetPhysicalMemoryUsage() > arena->GetRealMemoryUsage()
            && arena->GetPhysicalMemoryUsage() - arena->GetRealMemoryUsage() > size) {
            VLOG(1) << "try alloc from arena " << arena->GetArenaId()
                    << ", fallocate size:" << arena->GetPhysicalMemoryUsage()
                    << ", real allocate size:" << arena->GetRealMemoryUsage();
            if (AllocateMemoryImpl(false, populate, size, nextIndex, pointer).IsOk()) {
                index = nextIndex;
                nextId_ = nextIndex + 1;
                return Status::OK();
            }
        }
    }

    return status;
}

Status ArenaGroup::FreeMemory(void *pointer, uint64_t &bytesFree, uint64_t &bytesRealFree, uint64_t usedSize)
{
    CHECK_FAIL_RETURN_STATUS(!destroyed_.load(), StatusCode::K_RUNTIME_ERROR, "ArenaGroup destroyed");
    CHECK_FAIL_RETURN_STATUS(!arenas_.empty(), StatusCode::K_RUNTIME_ERROR, "arenas_ is empty");

    RETURN_RUNTIME_ERROR_IF_NULL(pointer);
    PerfPoint point(PerfKey::ALLOCATE_FREE_WAIT_LOCK);
    std::lock_guard<std::shared_timed_mutex> l(allocatedMutex_);
    point.RecordAndReset(PerfKey::ALLOCATE_FREE_HOLD_LOCK);
    auto iter = allocatedTable_.find(pointer);
    if (iter == allocatedTable_.end()) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Allocated pointer not found.");
    }
    const auto &record = iter->second;

    CHECK_FAIL_RETURN_STATUS(record.index < arenas_.size(), StatusCode::K_RUNTIME_ERROR,
                             "invalid index " + std::to_string(record.index));
    const auto &arena = arenas_[record.index];
    CHECK_FAIL_RETURN_STATUS(record.size <= usedSize, K_RUNTIME_ERROR,
                             "The size of memory freed is larger than the memory used by the given service type");
    Jemalloc::Free(arena->GetArenaId(), pointer);
    bytesFree = record.size;
    bytesRealFree = record.realSize;
    arena->SubRealMemoryUsage(bytesRealFree);
    allocatedTable_.erase(pointer);
    (void)memoryUsage_.fetch_sub(bytesFree, std::memory_order_relaxed);
    (void)realMemoryUsage_.fetch_sub(bytesRealFree, std::memory_order_relaxed);

    VLOG(1) << "[Allocator] Arena " << arena->GetArenaId() << " free require size: " << bytesFree
            << ", real size: " << bytesRealFree;
    return Status::OK();
}

Status ArenaGroup::FreeMemory(void *pointer)
{
    uint64_t bytesFree;
    uint64_t bytesRealFree;
    return FreeMemory(pointer, bytesFree, bytesRealFree, UINT64_MAX);
}

uint64_t ArenaGroup::GetMemoryUsage() const
{
    std::shared_lock<std::shared_timed_mutex> l(allocatedMutex_);
    return memoryUsage_;
}

Status ArenaGroup::FdToPointer(int fd, std::pair<void *, uint64_t> &ptrMmapSz) const
{
    CHECK_FAIL_RETURN_STATUS(!destroyed_.load(), StatusCode::K_RUNTIME_ERROR, "ArenaGroup destroyed");
    auto iter = fdPointerTable_.find(fd);
    if (iter == fdPointerTable_.end()) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, FormatString("fd [%d] not found", fd));
    }
    ptrMmapSz = iter->second;
    return Status::OK();
}

void ArenaGroup::GetArenaGroupStat(ShmMemStat &stat)
{
    if (destroyed_) {
        return;
    }
    stat.maxMemoryLimit = maxSize_;
    stat.memoryUsage = memoryUsage_.load(std::memory_order_relaxed);
    stat.numOfFds = fdPointerTable_.size();
    stat.realMemoryUsage = 0;
    stat.physicalMemoryUsage = 0;
    for (const auto &arena : arenas_) {
        stat.realMemoryUsage += arena->GetRealMemoryUsage();
        stat.physicalMemoryUsage += arena->GetPhysicalMemoryUsage();
    }
    std::shared_lock<std::shared_timed_mutex> l(allocatedMutex_);
    stat.numOfAllocated = allocatedTable_.size();
}

Status ArenaGroup::DestroyAll()
{
    auto arenaIds = GetArenaIds();
    if (destroyed_.exchange(true)) {
        return Status::OK();
    }
    auto manager = Allocator::Instance()->GetArenaManager();
    RETURN_RUNTIME_ERROR_IF_NULL(manager);
    return manager->DestroyArenas(arenaIds, [this] {
        Status lastRc;
        for (const auto &arena : arenas_) {
            Status rc = arena->DestroyArena();
            lastRc = rc.IsError() ? rc : lastRc;
        }
        arenas_.clear();
        return lastRc;
    });
}

std::vector<uint32_t> ArenaGroup::GetArenaIds() const
{
    if (destroyed_) {
        return {};
    }
    std::vector<uint32_t> ids;
    for (const auto &arena : arenas_) {
        ids.emplace_back(arena->GetArenaId());
    }
    return ids;
}

std::vector<int> ArenaGroup::GetAllFds()
{
    std::vector<int> fds;
    for (const auto &arean : arenas_) {
        fds.emplace_back(arean->GetFd());
    }
    return fds;
}

ArenaManager::ArenaManager(bool populate, bool scaling, ssize_t decayMs)
    : numArenas_(0), populate_(populate), scaling_(scaling), decayMs_(decayMs), destroyed_(false)
{
    arenas_.resize(ARENAS_INIT_SIZE);
    Jemalloc::Init(&ArenaManager::AllocHook, &ArenaManager::DestroyHook, &ArenaManager::CommitHook);
    handleExpiredTenantThread_ = std::make_unique<ThreadPool>(handleExpiredTenantThreadNum_, 0, "TenantExpired");
    if (populate || FLAGS_enable_huge_tlb || NeedRegisterWholeArena()) {
        // Adding multiple arenas per tenant allows parallel physical memory binding operations across different arenas.
        // For scenarios requiring pre-allocation of physical memory, a single arena is sufficient.
        FLAGS_arena_per_tenant = 1;
        // Since physical memory has already been pre-allocated, there is no need to trigger fallocate during the commit
        // phase.
        FLAGS_enable_fallocate = false;
    }
    auto arenaNum = FLAGS_arena_per_tenant;
    if (!FLAGS_shared_disk_directory.empty()) {
        arenaNum += FLAGS_shared_disk_arena_per_tenant;
    }
    maxTenantSize_ = ARENAS_INIT_SIZE / arenaNum - 1;
}

void ArenaManager::Init()
{
    int runningNum = handleExpiredTenantThread_->GetRunningTasksNum();
    if (runningNum >= handleExpiredTenantThreadNum_) {
        return;
    }
    StartCheckExpiredTenantResource();
}

Status ArenaManager::Init(CacheType type, AllocatorFuncRegister funcRegister)
{
    std::lock_guard<std::shared_timed_mutex> lck(registerMutex_);
    auto rc = funcRegisterList_.try_emplace(type, funcRegister);
    if (!rc.second) {
        RETURN_STATUS(K_DUPLICATED, FormatString("Already register allocator func for type: %d", (int)type));
    }
    Init();
    return Status::OK();
}

uint64_t ArenaManager::RoundUpToNextMultiple(uint64_t size)
{
    if (size % HUGE_PAGE_SIZE == 0) {
        return size;
    } else {
        return size + (HUGE_PAGE_SIZE - (size % HUGE_PAGE_SIZE));
    }
}

Status ArenaManager::CreateArenaGroup(CacheType type, uint64_t maxSize, std::shared_ptr<ArenaGroup> &arenaGroup)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!destroyed_, K_RUNTIME_ERROR, "ArenaManager already destroyed");
    std::vector<uint64_t> arenaInds;
    auto rate = 0.78;  // class size is 0.8.
    CHECK_FAIL_RETURN_STATUS(
        static_cast<uint64_t>(static_cast<long double>(std::numeric_limits<uint64_t>::max()) * rate) > maxSize,
        K_RUNTIME_ERROR, "mmapSize overflow.");
    auto fakeAllocateSize = maxSize;
    if (populate_ || IsFastTransportEnabled() || IsRemoteH2DEnabled() || FLAGS_enable_huge_tlb) {
        // Here we ensure total allocated memory
        // does not exceed max requested by user
        rate = 1;
        auto overhead = 0.8;
        // fakeAllocate Size is decreased to
        // account for extra Jemalloc overhead
        fakeAllocateSize = static_cast<uint64_t>(overhead * maxSize);
    }
    uint64_t mmapSize = maxSize / rate;
    if (FLAGS_enable_huge_tlb) {
        mmapSize = RoundUpToNextMultiple(mmapSize);
    }
    {
        std::lock_guard<std::shared_timed_mutex> l(mutex_);
        std::vector<std::shared_ptr<Arena>> arenas;
        auto arenasNum = type == CacheType::MEMORY ? FLAGS_arena_per_tenant : FLAGS_shared_disk_arena_per_tenant;
        arenasNum = (type == CacheType::DEV_DEVICE || type == CacheType::DEV_HOST || type == CacheType::UB_TRANSPORT)
                        ? 1 : arenasNum;
        arenas.reserve(arenasNum);
        for (uint32_t i = 0; i < arenasNum; i++) {
            uint32_t arenaInd = 0;
            void *handler = nullptr;
            RETURN_IF_NOT_OK(Jemalloc::CreateArena(decayMs_, arenaInd, handler));
            if (ARENAS_INIT_SIZE <= arenaInd) {
                LOG_IF_ERROR(Jemalloc::DestroyArena(arenaInd), "Too many arena create, destroy arena failed");
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                              FormatString("Too many arena created! areneInd: %d, arena init size: %d", arenaInd,
                                           ARENAS_INIT_SIZE));
            }

            std::shared_ptr<Arena> arena;
            if (arenas_[arenaInd] == nullptr) {
                arena = std::make_shared<Arena>(arenaInd, handler, populate_, scaling_, mmapSize, type);
                arenas_[arenaInd] = arena;
            } else {
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                              FormatString("arena %d reuse, but exists in ArenaManager.", arenaInd));
            }

            AllocatorFuncRegister regFunc;
            {
                std::shared_lock<std::shared_timed_mutex> l(registerMutex_);
                auto it = funcRegisterList_.find(type);
                if (it != funcRegisterList_.end()) {
                    regFunc = it->second;
                }
            }
            Status rc = arena->Init(regFunc);
            if (rc.IsError()) {
                LOG(ERROR) << "Init arena " << arenaInd << " failed: " << rc;
                arenas_[arenaInd] = nullptr;
                return rc;
            }

            VLOG(1) << "Create arena:" << arenaInd << ", fd:" << arena->GetFd() << ", mmap size:" << mmapSize;
            arenaInds.emplace_back(arenaInd);
            (void)numArenas_.fetch_add(1, std::memory_order_relaxed);
            arenas.emplace_back(std::move(arena));
        }

        arenaGroup = std::make_shared<ArenaGroup>(std::move(arenas), maxSize, type);
    };

    FakeAllocate(type, arenaInds, fakeAllocateSize);
    return Status::OK();
}

void ArenaManager::FakeAllocate(CacheType type, const std::vector<uint64_t> &arenaInds, const uint64_t maxSize)
{
    g_FakeAllocate = true;
    for (const auto &arenaInd : arenaInds) {
        uint64_t hookSize = maxSize;
        void *pointer;
        auto status = Jemalloc::Allocate(arenaInd, hookSize, pointer);
        if (status.GetCode() == StatusCode::K_OUT_OF_MEMORY) {
            auto it = CACHE_TYPE_STR.find(type);
            std::string errHint = "UnknowType";
            if (it != CACHE_TYPE_STR.end()) {
                errHint = it->second;
            }
            std::string errorMsg = FormatString("%s no space in arena: %d", errHint, arenaInd);
            status = Status(StatusCode::K_OUT_OF_MEMORY, errorMsg);
        }
        if (status.IsOk()) {
            LOG(WARNING) << "fake allocate should not success, pointer is not nullptr";
            Jemalloc::Free(arenaInd, pointer);
        }
    }
    g_FakeAllocate = false;
}

Status ArenaManager::CreateArenaGroup(const std::string &tenantId, CacheType type, uint64_t maxSize,
                                      std::shared_ptr<ArenaGroup> &arenaGroup)
{
    RETURN_IF_NOT_OK(CreateArenaGroup(type, maxSize, arenaGroup));
    {
        std::lock_guard<std::shared_timed_mutex> l(tenantMutex_);
        tenantArenas_[{ tenantId, type }] = arenaGroup;
    }
    return Status::OK();
}

Status ArenaManager::GetArenaGroup(const ArenaGroupKey &key, std::shared_ptr<ArenaGroup> &arenaGroup)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!destroyed_, K_RUNTIME_ERROR, "ArenaManager already destroyed");
    std::shared_lock<std::shared_timed_mutex> l(tenantMutex_);
    auto iter = tenantArenas_.find(key);
    if (iter != tenantArenas_.end()) {
        RETURN_RUNTIME_ERROR_IF_NULL(iter->second);
        arenaGroup = iter->second;
        return Status::OK();
    }
    RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("No arena found for the tenant %s.", key.tenantId));
}

Status ArenaManager::GetOrCreateArenaGroup(const ArenaGroupKey &key, uint64_t maxSize,
                                           std::shared_ptr<ArenaGroup> &arenaGroup)
{
    // allow parallel get arena
    auto rc = GetArenaGroup(key, arenaGroup);
    if (rc.IsOk()) {
        return Status::OK();
    }
    {
        // lock to prevent another thread call Create Arena
        // Query the arena again which is a tradeoff of the
        // parallel get arena above
        std::lock_guard<std::shared_timed_mutex> l(tenantMutex_);
        auto iter = tenantArenas_.find(key);
        if (iter != tenantArenas_.end()) {
            arenaGroup = iter->second;
            return Status::OK();
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            tenantArenas_.size() < maxTenantSize_, K_RUNTIME_ERROR,
            FormatString("create tenant failed, the maximum tenant size %llu has been reached", maxTenantSize_));
        RETURN_IF_NOT_OK(CreateArenaGroup(key.type, maxSize, arenaGroup));
        tenantArenas_[key] = arenaGroup;
    }
    return Status::OK();
}

Status ArenaManager::DestroyArenaGroup(const std::shared_ptr<ArenaGroup> &arenaGroup)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!destroyed_, K_RUNTIME_ERROR, "ArenaManager already destroyed");
    return arenaGroup->DestroyAll();
}

Status ArenaManager::DestroyArenaGroup(const ArenaGroupKey &key)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!destroyed_, K_RUNTIME_ERROR, "ArenaManager already destroyed");
    std::shared_ptr<ArenaGroup> arenaGroup;
    RETURN_IF_NOT_OK(GetArenaGroup(key, arenaGroup));

    if (arenaGroup->GetMemoryUsage() == 0) {
        RETURN_IF_NOT_OK(DestroyArenaGroup(arenaGroup));
        {
            std::lock_guard<std::shared_timed_mutex> l(tenantMutex_);
            (void)tenantArenas_.erase(key);
        }
        return Status::OK();
    }
    RETURN_STATUS(
        StatusCode::K_NOT_READY,
        FormatString("The tenant %s's arena is unable to destroy as allocated spaces still exist.", key.tenantId));
}

Status ArenaManager::DestroyAllArenaGroup()
{
    if (destroyed_.exchange(true)) {
        return Status::OK();
    }
    Status lastRc;
    std::shared_lock<std::shared_timed_mutex> l(tenantMutex_);
    for (auto &kv : tenantArenas_) {
        auto &arenaGroup = kv.second;
        Status rc = arenaGroup->DestroyAll();
        lastRc = rc.IsError() ? rc : lastRc;
    }
    return lastRc;
}

void ArenaManager::SetReleaseableTenant(const ArenaGroupKey &key)
{
    auto releaseHandler = [key]() {
        LOG(INFO) << FormatString("[TENANT RELEASER]Destroy arena group for %s", key.tenantId);
        LOG_IF_ERROR(datasystem::memory::Allocator::Instance()->DestroyArenaGroup(key),
                     FormatString("Destroy arena group for %s failed", key.tenantId));
    };

    PreReleaseTenantResourceInfo preReleaseTenantResourceInfo;
    preReleaseTenantResourceInfo.key = key;
    preReleaseTenantResourceInfo.fds = GetAllFdsByKey(key);
    preReleaseTenantResourceInfo.releaseHandler = releaseHandler;

    std::lock_guard<std::shared_timed_mutex> lck(preReleaseTenantResourceInfoMapMutex_);
    VLOG(1) << FormatString("[TENANT RELEASER]Tenant[%s] is releaseable", key.tenantId);
    preReleaseTenantResourceInfoMap_.emplace(key, std::move(preReleaseTenantResourceInfo));
}

void ArenaManager::CancelExpiredTenantTimer(const ArenaGroupKey &key)
{
    {
        std::lock_guard<std::shared_timed_mutex> lck(preReleaseTenantResourceInfoMapMutex_);
        if (preReleaseTenantResourceInfoMap_.erase(key) != 0) {
            VLOG(1) << FormatString("[TENANT RELEASER]The release process for tenant[%s] has been canceled",
                                    key.tenantId);
        };
    }
}

ArenaManager::~ArenaManager()
{
    LOG(INFO) << "ArenaManager destructor.";
    LOG_IF_ERROR(DestroyAllArenaGroup(), "DestroyAllArenaGroup failed!");
    destroyWaitPost_.Set();
}

void *ArenaManager::AllocHook(size_t size, size_t alignment, unsigned arenaInd, bool *zero, bool *commit)
{
    auto manager = Allocator::Instance()->GetArenaManager();
    std::shared_lock<std::shared_timed_mutex> l(manager->mutex_);
    return manager->arenas_[arenaInd]->AllocHook(size, alignment, zero, commit);
}

void ArenaManager::DestroyHook(void *addr, size_t size, bool committed, unsigned arenaInd)
{
    auto manager = Allocator::Instance()->GetArenaManager();
    // No need add lock, would be invoke when destroy arena, and the lock has been added already.
    auto threadSafePtr = manager->arenas_[arenaInd];  // Avoid being destroyed while in use.
    threadSafePtr->DestroyHook(addr, size, committed);
}

bool ArenaManager::CommitHook(bool commit, void *addr, size_t size, size_t offset, size_t length, unsigned arenaInd)
{
    auto manager = Allocator::Instance()->GetArenaManager();
    auto threadSafePtr = manager->arenas_[arenaInd];  // Avoid being destroyed while in use.
    return threadSafePtr->CommitHook(commit, addr, size, offset, length);
}

std::vector<int> ArenaManager::GetAllFdsByKey(const ArenaGroupKey &key)
{
    std::shared_lock<std::shared_timed_mutex> l(tenantMutex_);
    auto iter = tenantArenas_.find(key);
    if (iter != tenantArenas_.end()) {
        auto fds = iter->second->GetAllFds();
        return fds;
    }
    return {};
}

void ArenaManager::StartCheckExpiredTenantResource()
{
    static int intervalMs = 5'000;  // 5s
    handleExpiredTenantThread_->Execute([this]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
        LOG(INFO) << "[TENANT RELEASER]Start check expired tenant resource thread";
        while (!destroyed_) {
            {
                std::lock_guard<std::shared_timed_mutex> lck(preReleaseTenantResourceInfoMapMutex_);
                std::vector<PreReleaseTenantResourceInfo> expiredTenantsInfo;
                for (auto &i : preReleaseTenantResourceInfoMap_) {
                    if (i.second.IsExpired()) {
                        VLOG(1) << FormatString("[TENANT RELEASER]Tenant[%s] is expired", i.first.tenantId);
                        expiredTenantsInfo.emplace_back(i.second);
                    }
                }
                UpdateExpiredTenant(expiredTenantsInfo);
            }
            destroyWaitPost_.WaitFor(intervalMs);
        }
    });
}

void ArenaManager::UpdateExpiredTenant(const std::vector<PreReleaseTenantResourceInfo> &expiredTenantsInfo)
{
    for (const auto &expiredTenantInfo : expiredTenantsInfo) {
        if (Allocator::Instance()->CheckIfAllFdReleasedHandler(expiredTenantInfo.fds)) {
            VLOG(1) << FormatString("[TENANT RELEASER]Tenant[%s] is ready to release", expiredTenantInfo.key.tenantId);
            if (expiredTenantInfo.releaseHandler != nullptr) {
                expiredTenantInfo.releaseHandler();
            }
            preReleaseTenantResourceInfoMap_.erase(expiredTenantInfo.key);
        }
    }
}

std::set<int> ArenaManager::GetAllExpiredFds()
{
    std::shared_lock<std::shared_timed_mutex> lck(preReleaseTenantResourceInfoMapMutex_);
    std::set<int> expiredFds;
    for (auto iter : preReleaseTenantResourceInfoMap_) {
        if (iter.second.IsExpired()) {
            expiredFds.insert(iter.second.fds.begin(), iter.second.fds.end());
        }
    }
    return expiredFds;
}

Status ArenaManager::CheckWorkerFdTenant(const std::string &tenantId, const std::vector<int> &workerFds)
{
    std::set<int> allFds;
    std::shared_ptr<ArenaGroup> arenaGroup;
    std::vector<ArenaGroupKey> keys = {
        // all tenant can access the worker fds of DEFAULT_TENANT_ID to access the object client lock area.
        { DEFAULT_TENANT_ID, CacheType::MEMORY },
        { DEFAULT_TENANT_ID, CacheType::DISK },
        { tenantId, CacheType::MEMORY },
        { tenantId, CacheType::DISK }
    };
    for (const auto &key : keys) {
        Status rc = GetArenaGroup(key, arenaGroup);
        if (rc.IsError()) {
            continue;
        }
        auto fds = arenaGroup->GetAllFds();
        allFds.insert(fds.begin(), fds.end());
    }

    for (auto fd : workerFds) {
        if (allFds.find(fd) == allFds.end()) {
            return Status(K_NOT_AUTHORIZED, FormatString("workerFd %d is not belong to tenant[%s]", fd, tenantId));
        }
    }
    return Status::OK();
}

uint64_t Arena::pageSize_ = getpagesize();

Arena::Arena(uint32_t arenaId, void *handler, bool populate, bool scaling, uint64_t mmapSize, CacheType cacheType)
    : arenaId_(arenaId),
      handler_(handler),
      populate_(populate),
      scaling_(scaling),
      destroyed_(false),
      mmapSize_(mmapSize),
      cacheType_(cacheType)
{
}

Status Arena::Init(AllocatorFuncRegister funcRegister)
{
    switch (cacheType_) {
        case CacheType::MEMORY:
            mmap_ = std::make_unique<MemMmap>();
            break;
        case CacheType::DISK:
            mmap_ = std::make_unique<DiskMmap>();
            break;
        case CacheType::DEV_DEVICE:
        case CacheType::DEV_HOST:
        case CacheType::UB_TRANSPORT:
            mmap_ = std::make_unique<FlexibleMmap>(funcRegister);
            break;
        default:
            return Status(StatusCode::K_INVALID,
                          FormatString("Unkowned cache type: %d", static_cast<int32_t>(cacheType_)));
    }
    return mmap_->Initialize(mmapSize_, populate_, FLAGS_enable_huge_tlb);
}

Status Arena::DestroyArena()
{
    if (!destroyed_.exchange(true)) {
        VLOG(1) << "destroy arena:" << arenaId_;
        return Jemalloc::DestroyArena(arenaId_);
    }
    return Status::OK();
}

Arena::~Arena() noexcept
{
    LOG_IF_ERROR(DestroyArena(), "DestroyArena failed");
    if (handler_ != nullptr) {
        Jemalloc::DestroyHandler(handler_);
        handler_ = nullptr;
    }
}

void *Arena::AllocHook(size_t size, size_t alignment, bool *zero, bool *commit)
{
    if (g_FakeAllocate) {
        *commit = false;
    }
    bool needCommit = *commit && !*zero;
    if (g_FakeAllocate && !firstFakeHook_.load()) {
        LOG(INFO) << "fake hook before, no need to hook again";
        return nullptr;
    }

    if (needCommit && !AddPhysicalMemeoryUsage(size)) {
        return nullptr;
    }
    void *addr = mmap_->Allocate(size, alignment, zero, commit);
    if (addr == nullptr) {
        if (needCommit) {
            (void)SubPhysicalMemeoryUsage(size);
        }
        return addr;
    }
    if (g_FakeAllocate) {
        firstFakeHook_ = false;
    }
    if (needCommit) {
        if (CommitImpl(addr, 0, size)) {
            (void)SubPhysicalMemeoryUsage(size);
        }
    }
    return addr;
}

void Arena::DestroyHook(void *addr, size_t size, bool committed)
{
    (void)size;
    (void)committed;
    int fd;
    ptrdiff_t offset;
    uint64_t mmapSize;
    Status rc = GetAllocInfo(addr, fd, offset, mmapSize);
    if (rc.IsError()) {
        mmap_->Destroy();
    }
}

bool Arena::CommitHook(bool commit, void *addr, size_t size, size_t offset, size_t length)
{
    VLOG(1) << "CommitHook arena " << arenaId_;
    (void)size;
    if (commit) {
        if (g_FakeAllocate) {
            LOG(INFO) << "fake allocate no need to commit";
            return true;
        }
        if (!AddPhysicalMemeoryUsage(length)) {
            return true;
        }
        bool ret = CommitImpl(addr, offset, length);
        if (ret) {
            (void)SubPhysicalMemeoryUsage(length);
        }
        return ret;
    }
    return DecommitImpl(addr, offset, length);
}

bool Arena::CommitImpl(void *addr, size_t offset, size_t length)
{
    PerfPoint point(PerfKey::JEMALLOC_COMMIT);
    if (!g_NeedFallocate && length > Arena::pageSize_) {
        if (length == g_RequestSize) {
            return false;
        } else if (length > g_RequestSize) {
            offset += g_RequestSize;
            length -= g_RequestSize;
        } else {
            LOG(WARNING) << "Request size is: " << g_RequestSize << " but length is " << length
                         << ", it should not happen";
        }
    }
    return mmap_->Commit(addr, offset, length);
}

bool Arena::DecommitImpl(void *addr, size_t offset, size_t length)
{
    PerfPoint point(PerfKey::JEMALLOC_DECOMMIT);
    // If not enable scaling, just return false to tell the jemalloc
    // that we have purge the memory, but we actually do nothing.
    if (!scaling_ || destroyed_.load()) {
        return false;
    }

    if (mmap_->Decommit(addr, offset, length)) {
        return true;
    }
    (void)SubPhysicalMemeoryUsage(length);
    return false;
}

Status Arena::GetAllocInfo(void *pointer, int &fd, ptrdiff_t &offset, uint64_t &mmapSize)
{
    PerfPoint point(PerfKey::ALLOCATE_GET_MAP);
    auto allocation = mmap_->GetAllocation(pointer);
    if (!allocation) {
        fd = -1;
        mmapSize = 0;
        offset = 0;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Pointer addr not Range");
    }
    fd = allocation->fd;
    offset = allocation->offset;
    mmapSize = allocation->mmapSize;
    return Status::OK();
}

bool Arena::AddPhysicalMemeoryUsage(uint64_t size)
{
    if (Allocator::Instance()->AddTotalPhysicalMemoryUsage(cacheType_, size)) {
        physicalMemoryStats_.AddRealUsageNoCheck(size);
        return true;
    }
    return false;
}

void Arena::SubPhysicalMemeoryUsage(uint64_t size)
{
    INJECT_POINT("arena.decommit", [&size](uint64_t num) { size += num; });
    Allocator::Instance()->SubTotalPhysicalMemoryUsage(cacheType_, size);
    if (!physicalMemoryStats_.SubRealUsageCAS(size)) {
        LOG(WARNING) << "Arena " << arenaId_ << " sub physical memory usage failed";
    }
}

void Arena::AddRealMemoryUsage(uint64_t realSize)
{
    (void)realMemoryUsage_.fetch_add(realSize, std::memory_order_relaxed);
    (void)allocatedCount_.fetch_add(1, std::memory_order_relaxed);
}

void Arena::SubRealMemoryUsage(uint64_t realSize)
{
    (void)realMemoryUsage_.fetch_sub(realSize, std::memory_order_relaxed);
    (void)allocatedCount_.fetch_sub(1, std::memory_order_relaxed);
}
}  // namespace memory
}  // namespace datasystem
