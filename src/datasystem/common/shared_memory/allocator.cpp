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
 * Description: Define shared memory allocator class.
 */
#include "datasystem/common/shared_memory/allocator.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <sys/mman.h>

#define JEMALLOC_NO_DEMANGLE
#include "jemalloc/jemalloc.h"
#undef JEMALLOC_NO_DEMANGLE

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#ifdef WITH_TESTS
#include "datasystem/common/inject/inject_point.h"
#endif
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/shared_memory/resource_pool.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/log/log.h"

DS_DECLARE_string(shared_disk_directory);
DS_DECLARE_uint32(eviction_reserve_mem_threshold_mb);
DS_DECLARE_uint32(arena_per_tenant);
DS_DECLARE_bool(enable_fallocate);

namespace datasystem {
namespace memory {
namespace {
Status ValidateSharedMemoryPopulateFlags(bool populate)
{
    if (!populate) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(FLAGS_arena_per_tenant <= 1, K_INVALID,
                             "If shared_memory_populate is true, arena_per_tenant must be 1");
    CHECK_FAIL_RETURN_STATUS(!FLAGS_enable_fallocate, K_INVALID,
                             "If shared_memory_populate is true, enable_fallocate must be false");
    return Status::OK();
}
}  // namespace

const int HUNDRED_PERCENT = 100;
const double HIGH_WATER_MARK_RATIO = 0.8;

void DeallocateForZmqFree(void *data, void *hint)
{
    (void)hint;
    (void)Allocator::Instance()->FreeMemory(data);
}

Allocator *Allocator::Instance()
{
    static Allocator instance;
    return &instance;
}

Allocator::~Allocator() noexcept
{
    LOG(INFO) << "Allocator destructor.";
}

Status Allocator::InitSharedMemory(uint64_t size, int objectThreshold, int streamThreshold)
{
    CHECK_FAIL_RETURN_STATUS((size > 0) && (size < UINT64_MAX / HUNDRED_PERCENT), K_INVALID,
                             "the memory size should be greater than 0 and less than UINT64_MAX/100");
    CHECK_FAIL_RETURN_STATUS(
        (objectThreshold > 0 && objectThreshold <= HUNDRED_PERCENT)
            && (streamThreshold > 0 && streamThreshold <= HUNDRED_PERCENT),
        K_INVALID, "the allocation threshold percentage should be greater than 0 and less than or equal to 100");
    physicalMemoryStats_ = std::make_unique<ResourcePool>(size);
    objectMemoryStats_ = std::make_unique<ResourcePool>((size * objectThreshold) / HUNDRED_PERCENT);
    streamMemoryStats_ = std::make_unique<ResourcePool>((size * streamThreshold) / HUNDRED_PERCENT);
    return Status::OK();
}

Status Allocator::InitSharedDisk(uint64_t size)
{
    physicalDiskStats_ = std::make_unique<ResourcePool>(size);
    diskStats_ = std::make_unique<ResourcePool>(size);
    RETURN_OK_IF_TRUE(size == 0 || FLAGS_shared_disk_directory.empty());
    RETURN_IF_NOT_OK(RemoveAll(FLAGS_shared_disk_directory));
    const int permission = 0700;
    RETURN_IF_NOT_OK(CreateDir(FLAGS_shared_disk_directory, true, permission));

    auto freeSpace = GetFreeSpaceBytes(FLAGS_shared_disk_directory);
    if (size > freeSpace) {
        LOG(WARNING) << FormatString(
            "The required disk space(%lluB) exceeds the available space(%lluB), which may lead to OOM", size,
            freeSpace);
    }
    diskDetecter_ = std::make_unique<SharedDiskDetecter>(FLAGS_shared_disk_directory);
    return Status::OK();
}

Status Allocator::InitDevHostMemory(uint64_t devHostSize)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(devHostSize > 0, K_INVALID, "Got invalid dev host memory init!");
    devHostMemStats_ = std::make_unique<ResourcePool>(devHostSize);
    return Status::OK();
}

Status Allocator::InitDevMemory(uint64_t devDevSize)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(devDevSize > 0, K_INVALID, "Got invalid dev device memory init!");
#ifdef WITH_TESTS
    INJECT_POINT_NO_RETURN("Allocator.InitDevMemory");
#endif
    devDeviceMemStats_ = std::make_unique<ResourcePool>(devDevSize);
    return Status::OK();
}

Status Allocator::InitUBTransportMemory(uint64_t size)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(size > 0, K_INVALID, "Got invalid dev device memory init!");
    ubTransportStats_ = std::make_unique<ResourcePool>(size);
    return Status::OK();
}

bool Allocator::IsDiskAvailable()
{
    return diskDetecter_ == nullptr ? true : diskDetecter_->IsAvailable();
}

Status Allocator::Init(uint64_t shmSize, uint64_t shdSize, bool populate, bool scaling, ssize_t decayMs,
                       int objectThreshold, int streamThreshold)
{
    if (arenaManager_) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(ValidateSharedMemoryPopulateFlags(populate));
    RETURN_IF_NOT_OK(InitSharedMemory(shmSize, objectThreshold, streamThreshold));
    RETURN_IF_NOT_OK(InitSharedDisk(shdSize));
    arenaManager_ = std::make_unique<ArenaManager>(populate, scaling, decayMs);
    arenaManager_->Init();
    return Status::OK();
}

Status Allocator::InitWithFlexibleRegister(CacheType cacheType, uint64_t size, AllocatorFuncRegister memFuncRegister,
                                           bool populate, bool scaling, ssize_t decayMs)
{
    if (!arenaManager_) {
        arenaManager_ = std::make_unique<ArenaManager>(populate, scaling, decayMs);
    }
    RETURN_IF_NOT_OK(arenaManager_->Init(cacheType, memFuncRegister));
    switch (cacheType) {
        case CacheType::DEV_DEVICE:
            RETURN_IF_NOT_OK(InitDevMemory(size));
            break;
        case CacheType::DEV_HOST:
            RETURN_IF_NOT_OK(InitDevHostMemory(size));
            break;
        case CacheType::UB_TRANSPORT:
            RETURN_IF_NOT_OK(InitUBTransportMemory(size));
            break;
        default:
            RETURN_STATUS_LOG_ERROR(K_INVALID, FormatString("Got unknow type: %d", (int)cacheType));
    }
    return Status::OK();
}

Status Allocator::CreateArenaGroup(uint64_t maxSize, std::shared_ptr<ArenaGroup> &arenaGroup, CacheType cacheType)
{
    RETURN_RUNTIME_ERROR_IF_NULL(arenaManager_);
    return arenaManager_->CreateArenaGroup(cacheType, maxSize, arenaGroup);
}

Status Allocator::CreateArenaGroup(const std::string &tenantId, uint64_t maxSize,
                                   std::shared_ptr<ArenaGroup> &arenaGroup, CacheType cacheType)
{
    RETURN_RUNTIME_ERROR_IF_NULL(arenaManager_);
    return arenaManager_->CreateArenaGroup(tenantId, cacheType, maxSize, arenaGroup);
}

Status Allocator::DestroyArenaGroup(const ArenaGroupKey &key)
{
    RETURN_RUNTIME_ERROR_IF_NULL(arenaManager_);
    auto rc = arenaManager_->DestroyArenaGroup(key);
    if (rc.IsOk() || rc.GetCode() == StatusCode::K_NOT_READY) {
        // Ignore the destroy request due to not met the requirement
        return Status::OK();
    }
    // Return other unexpected errors
    return rc;
}

uint64_t Allocator::GetMaxMemoryLimit(CacheType cacheType) const
{
    switch (cacheType) {
        case CacheType::MEMORY:
            return physicalMemoryStats_->FootprintLimit();
        case CacheType::DISK:
            return physicalDiskStats_->FootprintLimit();
        case CacheType::DEV_DEVICE:
            return devDeviceMemStats_->FootprintLimit();
        case CacheType::DEV_HOST:
            return devHostMemStats_->FootprintLimit();
        case CacheType::UB_TRANSPORT:
            return ubTransportStats_->FootprintLimit();
        default:
            LOG(ERROR) << FormatString("Got unknow type: %d", (int)cacheType);
            return 0;
    }
}

ResourcePool *Allocator::GetResourcePoolByType(ServiceType serviceType, CacheType cacheType) const
{
    if (serviceType == ServiceType::STREAM) {
        return streamMemoryStats_.get();
    }
    switch (cacheType) {
        case CacheType::DISK:
            return diskStats_.get();
        case CacheType::DEV_HOST:
            return devHostMemStats_.get();
        case CacheType::DEV_DEVICE:
            return devDeviceMemStats_.get();
        case CacheType::MEMORY:
            return objectMemoryStats_.get();
        case CacheType::UB_TRANSPORT:
            return ubTransportStats_.get();
        default:
            return objectMemoryStats_.get();
    }
}

ResourcePool *Allocator::GetPhyResourcePoolByType(CacheType cacheType) const
{
    if (cacheType == CacheType::DISK) {
        return physicalDiskStats_.get();
    }
    return physicalMemoryStats_.get();
}

uint64_t Allocator::GetMemoryAvailToHighWater() const
{
    uint64_t memoryLimit = GetTotalMemoryLimit();
    uint64_t highWater = std::max(static_cast<uint64_t>(memoryLimit * HIGH_WATER_MARK_RATIO),
                                  memoryLimit > FLAGS_eviction_reserve_mem_threshold_mb * MB_TO_BYTES
                                      ? memoryLimit - (FLAGS_eviction_reserve_mem_threshold_mb * MB_TO_BYTES)
                                      : 0);
    uint64_t realUsage = objectMemoryStats_->RealUsage();
    return highWater > realUsage ? highWater - realUsage : 0;
}

void Allocator::Shutdown()
{
    LOG(INFO) << "Allocator shutdown";
    if (arenaManager_ != nullptr) {
        LOG_IF_ERROR(arenaManager_->DestroyAllArenaGroup(), "destroy tenant arena group failed");
    }
}

Status Allocator::AllocateMemory(const std::string &tenantId, uint64_t needSize, bool populate, void *&pointer, int &fd,
                                 ptrdiff_t &offset, uint64_t &mmapSize, ServiceType serviceType, CacheType cacheType)
{
    RETURN_RUNTIME_ERROR_IF_NULL(arenaManager_);
#ifdef WITH_TESTS
    INJECT_POINT("worker.Allocator.AllocateMemory");
#endif
    if (cacheType == CacheType::DISK) {
        if (FLAGS_shared_disk_directory.empty()) {
            RETURN_STATUS(K_INVALID, "Allocate failed because shared disk is not enabled.");
        }
        if (!IsDiskAvailable()) {
            RETURN_STATUS(K_RUNTIME_ERROR, "Allocate failed because shared disk is not available.");
        }
    }

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(IncrementMemoryUsage(needSize, serviceType, cacheType), "ADD failed");
    std::shared_ptr<ArenaGroup> arenaGroup;
    Status rc = arenaManager_->GetOrCreateArenaGroup({ tenantId, cacheType }, GetMaxMemoryLimit(cacheType), arenaGroup);
    uint64_t realSize;
    if (rc.IsOk()) {
        RETURN_RUNTIME_ERROR_IF_NULL(arenaGroup);
        rc = arenaGroup->AllocateMemory(needSize, populate, realSize, pointer, fd, offset, mmapSize, serviceType);
    }
    auto stats = GetResourcePoolByType(serviceType, cacheType);
    if (rc.IsError()) {
        stats->SubUsage(needSize);
        return rc;
    }

    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(arenaGroup->GetMemoryUsage() != 0, K_RUNTIME_ERROR,
                                         "Memory is allocated, but statistics are not growing");
    arenaManager_->CancelExpiredTenantTimer({ tenantId, cacheType });

    // Still counting tenant's arena so we can know the total stats of memory usage
    (void)noRefPageCount_.fetch_add(1, std::memory_order_relaxed);
    (void)totalNumOfAllocated_.fetch_add(1, std::memory_order_relaxed);

    stats->AddRealUsageNoCheck(realSize);
    return Status::OK();
}

Status Allocator::IncrementMemoryUsage(uint64_t needSize, ServiceType serviceType, CacheType cacheType)
{
    if (cacheType == CacheType::DISK) {
        return diskStats_->AddUsageCAS(needSize);
    } else if (cacheType == CacheType::DEV_DEVICE) {
        return devDeviceMemStats_->AddUsageCAS(needSize);
    } else if (cacheType == CacheType::DEV_HOST) {
        return devHostMemStats_->AddUsageCAS(needSize);
    } else if (cacheType == CacheType::UB_TRANSPORT) {
        return ubTransportStats_->AddUsageCAS(needSize);
    }
#ifdef WITH_TESTS
    INJECT_POINT("worker.Allocator.MemoryAllocatedToStream", [this](int streamMemoryUsage) {
        streamMemoryStats_->SetUsage(streamMemoryUsage);
        streamMemoryStats_->SetRealUsage(streamMemoryUsage);
        return Status::OK();
    });
#endif
    if (serviceType == ServiceType::OBJECT) {
        return objectMemoryStats_->AddUsageCAS(
            needSize, physicalMemoryStats_->FootprintLimit() - streamMemoryStats_->RealUsage());
    } else {
        return streamMemoryStats_->AddUsageCAS(
            needSize, physicalMemoryStats_->FootprintLimit() - objectMemoryStats_->RealUsage());
    }
    return Status::OK();
}

Status Allocator::FreeMemory(void *&pointer, ServiceType type)
{
    return FreeMemory(DEFAULT_TENANT_ID, pointer, type);
}

Status Allocator::FreeMemory(const std::string &tenantId, void *&pointer, ServiceType serviceType, CacheType cacheType)
{
    RETURN_RUNTIME_ERROR_IF_NULL(arenaManager_);
    std::shared_ptr<ArenaGroup> arenaGroup;
    uint64_t bytesFree = 0;
    uint64_t bytesRealFree = 0;
    RETURN_IF_NOT_OK(arenaManager_->GetArenaGroup({ tenantId, cacheType }, arenaGroup));
    auto stats = GetResourcePoolByType(serviceType, cacheType);
    RETURN_IF_NOT_OK(arenaGroup->FreeMemory(pointer, bytesFree, bytesRealFree, stats->Usage()));
#ifdef WITH_TESTS
    INJECT_POINT("Allocator.FreeMemory.PostFreeMemoryPreSubUsage");
#endif
    if (arenaGroup->GetMemoryUsage() == 0) {
        arenaManager_->SetReleaseableTenant({ tenantId, cacheType });
    }

    (void)noRefPageCount_.fetch_sub(1, std::memory_order_relaxed);
    (void)totalNumOfAllocated_.fetch_sub(1, std::memory_order_relaxed);
    pointer = nullptr;  // Memory is freed, set the pointer to nullptr.
    stats->SubUsage(bytesFree);
    stats->SubRealUsage(bytesRealFree);
    return Status::OK();
}

uint64_t Allocator::GetMaxMemorySize(ServiceType serviceType, CacheType cacheType) const
{
    return GetResourcePoolByType(serviceType, cacheType)->FootprintLimit();
}

uint64_t Allocator::GetMemoryUsage(const std::string &tenantId, CacheType cacheType)
{
    if (arenaManager_ == nullptr) {
        return 0;
    }
    std::shared_ptr<ArenaGroup> arenaGroup;
    auto rc = arenaManager_->GetArenaGroup({ tenantId, cacheType }, arenaGroup);
    if (rc.IsOk()) {
        return arenaGroup->GetMemoryUsage();
    }
    // If arena is not existed, return 0 because no memory space is occupied.
    return 0;
}

Status Allocator::FdToPointer(int fd, std::pair<void *, uint64_t> &ptrMmapSz) const
{
    return FdToPointer({ DEFAULT_TENANT_ID, CacheType::MEMORY }, fd, ptrMmapSz);
}

Status Allocator::FdToPointer(const ArenaGroupKey &key, int fd, std::pair<void *, uint64_t> &ptrMmapSz) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(arenaManager_);
    std::shared_ptr<ArenaGroup> arenaGroup;
    RETURN_IF_NOT_OK(arenaManager_->GetArenaGroup(key, arenaGroup));
    return arenaGroup->FdToPointer(fd, ptrMmapSz);
}

Status Allocator::GetMemStat(ShmMemStat &shmMemStat)
{
    RETURN_RUNTIME_ERROR_IF_NULL(arenaManager_);
    shmMemStat.memoryUsage = objectMemoryStats_->Usage() + streamMemoryStats_->Usage();
    shmMemStat.realMemoryUsage = objectMemoryStats_->RealUsage() + streamMemoryStats_->RealUsage();
    shmMemStat.objectMemoryUsage = objectMemoryStats_->Usage();
    shmMemStat.physicalMemoryUsage = GetTotalPhysicalMemoryUsage();
    shmMemStat.numOfFds = arenaManager_->GetArenaCounts();
    shmMemStat.numOfAllocated = totalNumOfAllocated_;
    shmMemStat.refPageCount = refPageCount_.load(std::memory_order_relaxed);
    shmMemStat.noRefPageCount = noRefPageCount_.load(std::memory_order_relaxed);
    return Status::OK();
}

void Allocator::ChangeRefPageCount(int64_t num)
{
    (void)refPageCount_.fetch_add(num, std::memory_order_relaxed);
}

void Allocator::ChangeNoRefPageCount(int64_t num)
{
    (void)noRefPageCount_.fetch_add(num, std::memory_order_relaxed);
}

uint64_t Allocator::GetTotalPhysicalMemoryUsage(CacheType cacheType)
{
    if (cacheType == CacheType::DISK) {
        return physicalDiskStats_->GetOrUpdateRealUsage(diskStats_->RealUsage());
    }
    if (cacheType == CacheType::DEV_DEVICE) {
        return devDeviceMemStats_->RealUsage();
    }
    if (cacheType == CacheType::DEV_HOST) {
        return devDeviceMemStats_->RealUsage();
    }
    if (cacheType == CacheType::UB_TRANSPORT) {
        return ubTransportStats_->RealUsage();
    }
#ifdef WITH_TESTS
    INJECT_POINT("allocator.size", [this](int64_t usage) {
        physicalMemoryStats_->SetRealUsage(usage);
        return 0;
    });
#endif
    return physicalMemoryStats_->GetOrUpdateRealUsage(objectMemoryStats_->RealUsage()
                                                      + streamMemoryStats_->RealUsage());
}

bool Allocator::AddTotalPhysicalMemoryUsage(CacheType type, uint64_t size)
{
    if (type == CacheType::DEV_DEVICE || type == CacheType::DEV_HOST || type == CacheType::UB_TRANSPORT) {
        return true;
    }
    return GetPhyResourcePoolByType(type)->AddRealUsage(size);
}

void Allocator::SubTotalPhysicalMemoryUsage(CacheType type, uint64_t size)
{
    if (type == CacheType::DEV_DEVICE || type == CacheType::DEV_HOST || type == CacheType::UB_TRANSPORT) {
        return;
    }
    (void)GetPhyResourcePoolByType(type)->SubRealUsageCAS(size);
}

std::set<int> Allocator::GetAllExpiredFds()
{
    return arenaManager_->GetAllExpiredFds();
}

Status Allocator::CheckWorkerFdTenant(const std::string &tenantId, const std::vector<int> &workerFds)
{
    return arenaManager_->CheckWorkerFdTenant(tenantId, workerFds);
}
}  // namespace memory
}  // namespace datasystem
