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
 * Description: Some Object struct of worker
 */
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"

#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

#define RETRY_IF_OUT_MEMORY(rc_, statement_, maxRetryCnt_)                      \
    do {                                                                        \
        int currCnt_ = 0;                                                       \
        do {                                                                    \
            rc_ = (statement_);                                                 \
            currCnt_++;                                                         \
        } while (rc_.GetCode() == K_OUT_OF_MEMORY && currCnt_ <= maxRetryCnt_); \
        LOG(INFO) << "try get shm for payload " << currCnt_ << " times";        \
    } while (0)

DS_DECLARE_uint64(oc_worker_aggregate_merge_size);

namespace datasystem {
namespace object_cache {

ObjCacheShmUnit::ObjCacheShmUnit()
{
    if (IsRemoteH2DEnabled()) {
        remoteH2DHostInfoMap_ = std::make_shared<RemoteH2DHostInfoMap>();
    }
}

Status ObjCacheShmUnit::FreeResources()
{
    // Call super class to release our memory resources.
    shmUnit_.reset();
    return Status::OK();
}

bool ObjCacheShmUnit::IsInvalid() const
{
    return (lifeState_ == ObjectLifeState::OBJECT_INVALID);
}

bool ObjCacheShmUnit::IsSealed() const
{
    return (lifeState_ == ObjectLifeState::OBJECT_SEALED);
}

bool ObjCacheShmUnit::IsPublished() const
{
    return (lifeState_ == ObjectLifeState::OBJECT_PUBLISHED);
}

ObjectLifeState ObjCacheShmUnit::GetLifeState() const
{
    return lifeState_;
}

void ObjCacheShmUnit::SetLifeState(const ObjectLifeState newLifeState)
{
    lifeState_ = newLifeState;
}

uint64_t ObjCacheShmUnit::GetCreateTime() const
{
    return createTime_;
}

void ObjCacheShmUnit::SetCreateTime(const uint64_t newCreateTime)
{
    createTime_ = newCreateTime;
}

uint64_t ObjCacheShmUnit::GetDataSize() const
{
    return dataSize_;
}

void ObjCacheShmUnit::SetDataSize(const uint64_t size)
{
    dataSize_ = size;
}

uint64_t ObjCacheShmUnit::GetMetadataSize() const
{
    return metadataSize_;
}

void ObjCacheShmUnit::SetMetadataSize(const uint64_t size)
{
    metadataSize_ = size;
}

std::string ObjCacheShmUnit::GetAddress() const
{
    return address_;
}

void ObjCacheShmUnit::SetAddress(const std::string &newAddress)
{
    address_ = newAddress;
}

#ifndef DISABLE_RDMA
void ObjCacheShmUnit::SetRemoteHostInfo(const std::string &clientCommId,
                                        const std::shared_ptr<RemoteH2DHostInfoPb> &remoteH2DHostInfo)
{
    if (IsRemoteH2DEnabled()) {
        RemoteH2DHostInfoMap::accessor accessor;
        remoteH2DHostInfoMap_->insert(accessor, clientCommId);
        accessor->second = remoteH2DHostInfo;
    } else {
        // Handle the case when remote H2D is not enabled
        LOG(WARNING) << "Remote H2D is not enabled";
    }
}

std::shared_ptr<RemoteH2DHostInfoMap> ObjCacheShmUnit::GetRemoteHostInfo() const
{
    return remoteH2DHostInfoMap_;
}
#endif

Status CopyAndSplitBuffer(const std::string &tenantId, const void *data, size_t size, std::vector<RpcMessage> &messages)
{
    const size_t maxInt = std::numeric_limits<int32_t>::max();
    size_t remaining = size;
    auto ptr = static_cast<const uint8_t *>(data);
    int fd = -1;
    ptrdiff_t offset = 0;
    uint64_t mmapSize = 0;
    MsgFreeFn *ffn = memory::DeallocateForZmqFree;
    Status rc;
    while (remaining > 0) {
        size_t bufSize = std::min(remaining, maxInt);
        void *p = nullptr;
        RETRY_IF_OUT_MEMORY(
            rc, memory::Allocator::Instance()->AllocateMemory(tenantId, bufSize, false, p, fd, offset, mmapSize), 10);
        if (rc.IsError()) {
            return rc;
        }
        int r = memcpy_s(p, bufSize, ptr, bufSize);
        if (r != 0) {
            int ret = memset_s(p, bufSize, 0, bufSize);
            if (ret != EOK) {
                LOG(WARNING) << FormatString("memset failed, error code: %d.", ret);
            }
            RETURN_STATUS_LOG_ERROR(
                K_RUNTIME_ERROR, FormatString("Unable to copy %d bytes into shm. rc = %d errno = %d", size, r, errno));
        }

        messages.emplace_back();
        RETURN_IF_NOT_OK(messages.back().TransferOwnership(p, bufSize, ffn));
        remaining -= bufSize;
        ptr += bufSize;
    }
    return Status::OK();
}

static Status InitializeMetadataMemory(const std::string &objectKey, uint64_t metadataSize, bool populate,
                                       ShmUnit &shmUnit)
{
    if (metadataSize > 0) {
        auto ret = memset_s(shmUnit.GetPointer(), metadataSize, 0, metadataSize);
        if (ret != EOK) {
            if (!populate) {
                shmUnit.SetHardFreeMemory();
            }
            shmUnit.FreeMemory();
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                    FormatString("[ObjectKey %s] Memset failed, errno: %d", objectKey, ret));
        }
    }
    return Status::OK();
}

Status AllocateMemoryForObject(const std::string &objectKey, const uint64_t dataSize, uint64_t metadataSize,
                               bool populate, std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                               ShmUnit &shmUnit, CacheType cacheType, bool retryOnOOM)
{
    Timer timer;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        UINT64_MAX - metadataSize >= dataSize, K_RUNTIME_ERROR,
        FormatString("The size is overflow, size:%d + add:%d > UINT64_MAX:%d", dataSize, metadataSize, UINT64_MAX));
    uint64_t needSize = dataSize + metadataSize;
    PerfPoint point(PerfKey::WORKER_MEMORY_ALLOCATE);
    (void)EvictWhenMemoryExceedThrehold(objectKey, needSize, evictionManager, ServiceType::OBJECT, cacheType);
    // Allocate some memory into this shmUnit
    auto tenantId = TenantAuthManager::ExtractTenantId(objectKey);
    static const std::vector<int> WAIT_MSECOND = { 1, 10, 50, 100, 200, 400, 800, 1600, 3200 };
    Status rc = shmUnit.AllocateMemory(tenantId, needSize, populate, ServiceType::OBJECT,
                                       static_cast<memory::CacheType>(cacheType));
    if (rc.GetCode() == K_OUT_OF_MEMORY && retryOnOOM) {
        INJECT_POINT("worker.AllocateMemory.afterOOM");
        for (int t : WAIT_MSECOND) {
            auto remainingTime = reqTimeoutDuration.CalcRealRemainingTime();
            if (remainingTime <= 0) {
                break;
            }
            auto sleepTime = std::min<int64_t>(remainingTime, t);
            INJECT_POINT("worker.AllocateMemory.sleepTime", [&sleepTime](int time) {
                sleepTime = time;
                return Status::OK();
            });
            VLOG(1) << FormatString("OOM, sleep time: %ld, objectKey: %s, needSize %ld", sleepTime, objectKey,
                                    needSize);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
            rc = shmUnit.AllocateMemory(tenantId, needSize, populate, ServiceType::OBJECT,
                                        static_cast<memory::CacheType>(cacheType));
            if (rc.GetCode() != K_OUT_OF_MEMORY) {
                break;
            }
            (void)EvictWhenMemoryExceedThrehold(objectKey, needSize, evictionManager, ServiceType::OBJECT, cacheType);
        }
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, FormatString("[ObjectKey %s] Error while allocating memory.", objectKey));

    RETURN_IF_NOT_OK(InitializeMetadataMemory(objectKey, metadataSize, populate, shmUnit));

    point.Record();
    workerOperationTimeCost.Append("AllocateMemory", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status DistributeMemoryForObject(const std::string &objectKey, const uint64_t dataSize, uint64_t metadataSize,
                                 bool populate, std::shared_ptr<ShmOwner> shmOwner, ShmUnit &shmUnit)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        UINT64_MAX - metadataSize >= dataSize, K_RUNTIME_ERROR,
        FormatString("The size is overflow, size:%d + add:%d > UINT64_MAX:%d", dataSize, metadataSize, UINT64_MAX));
    uint64_t needSize = dataSize + metadataSize;
    PerfPoint point(PerfKey::WORKER_MEMORY_ALLOCATE);
    RETURN_IF_NOT_OK(shmOwner->DistributeMemory(needSize, shmUnit));
    RETURN_IF_NOT_OK(InitializeMetadataMemory(objectKey, metadataSize, populate, shmUnit));
    return Status::OK();
}

Status AggregateAllocate(
    const std::string &firstObjectKey,
    std::function<void(std::function<void(uint64_t, uint64_t, uint32_t)>, bool &)> &traversalHelper,
    std::shared_ptr<WorkerOcEvictionManager> evictionManager, std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
    std::vector<uint32_t> &shmIndexMapping, bool retryOnOOM, bool includeLargeObjects)
{
    // Pre-allocate aggregated chunks of shared memory as ShmOwner, to reduce the number of allocation calls.
    // By default only aggregate small objects (< 1MB), and batch up to 1024 keys and configured total size.
    // Slot migration can opt in to include large objects as well.
    const uint64_t batchLimitKeys = 1024;
    const uint64_t batchLimitSingleSize = 1024 * 1024;
    const uint64_t batchLimitTotalSize = FLAGS_oc_worker_aggregate_merge_size;

    bool needAggregate = false;
    std::vector<uint64_t> aggreatedSizes;
    uint64_t currentBatchSize = 0;
    uint64_t currentKeyCount = 0;

    std::function<void(uint64_t, uint64_t, uint32_t)> aggregateCollector = [&](uint64_t dataSz, uint64_t shmSize,
                                                                               uint32_t objectIndex) {
        // Skip any object that has size beyond 1MB.
        if (!includeLargeObjects && dataSz >= batchLimitSingleSize) {
            return;
        }

        // Seal the last batch and start the new batch.
        uint64_t ceilingSize = Align4BitsCeiling(shmSize);
        if (currentKeyCount >= batchLimitKeys
            || ((currentBatchSize + ceilingSize) > batchLimitTotalSize && currentBatchSize > 0)) {
            aggreatedSizes.emplace_back(currentBatchSize);
            currentBatchSize = 0;
            currentKeyCount = 0;
        }
        // Record the size and num, and also map from object key to ShmOwners index.
        currentBatchSize += ceilingSize;
        currentKeyCount++;
        shmIndexMapping[objectIndex] = aggreatedSizes.size();
    };

    traversalHelper(aggregateCollector, needAggregate);

    if (needAggregate && currentBatchSize > 0) {
        // Deal with the last batch.
        aggreatedSizes.emplace_back(currentBatchSize);
        // Allocate memory for each batch.
        for (const auto &aggregateSize : aggreatedSizes) {
            std::shared_ptr<ShmOwner> shmOwner = std::make_shared<ShmOwner>();
            // All keys in the batch request should belong to the same tenant.
            RETURN_IF_NOT_OK(AllocateMemoryForObject(firstObjectKey, aggregateSize, 0, false, evictionManager,
                                                     *shmOwner, CacheType::MEMORY, retryOnOOM));
            shmOwners.push_back(shmOwner);
        }
    } else {
        shmIndexMapping.clear();
    }

    return Status::OK();
}

Status AllocateNewShmUnit(const std::string &objectKey, uint64_t dataSize, uint64_t metadataSize, bool populate,
                          std::shared_ptr<WorkerOcEvictionManager> evictionManager, std::shared_ptr<ShmUnit> &shmUnit,
                          CacheType cacheType)
{
    shmUnit = std::make_shared<ShmUnit>();
    RETURN_IF_NOT_OK(
        AllocateMemoryForObject(objectKey, dataSize, metadataSize, populate, evictionManager, *shmUnit, cacheType));
    shmUnit->id = ShmKey::Intern(GetStringUuid());
    return Status::OK();
}

Status LoadSpilledObjectToMemory(ReadObjectKV &objectKV, std::shared_ptr<WorkerOcEvictionManager> evictionManager)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &entry = objectKV.GetObjEntry();
    // We do not expect that there is already a pointer assigned for a spilled object
    if (entry->IsShmUnitExistsAndComplete()) {
        return Status::OK();
    }
    const uint64_t dataSize = entry->GetDataSize();
    const uint64_t metaSize = entry->GetMetadataSize();
    const uint64_t needSize = dataSize + metaSize;
    uint64_t readOffset = objectKV.GetReadOffset();
    uint64_t readSize = objectKV.GetReadSize();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectKV.CheckReadOffset(), "Read offset verify failed.");
    // reuse the incomplete shm unit.
    std::shared_ptr<ShmUnit> newShmUnit;
    if (entry->GetShmUnit() == nullptr) {
        auto objShmUnit = SafeObjType::GetDerived<ObjCacheShmUnit>(entry);
        RETURN_RUNTIME_ERROR_IF_NULL(objShmUnit);
        newShmUnit = std::make_shared<ShmUnit>();
        RETURN_IF_NOT_OK(AllocateMemoryForObject(objectKey, entry->GetDataSize(), entry->GetMetadataSize(), false,
                                                 evictionManager, *newShmUnit));
        INJECT_POINT("LoadSpilledObjectToMemory", [&entry]() {
            entry->stateInfo.SetIncompleted(true);
            return Status(K_OUT_OF_MEMORY, "out of memory");
        });
        newShmUnit->id = ShmKey::Intern(GetStringUuid());
        objShmUnit->SetShmUnit(newShmUnit);
    }
    bool isOffsetRead = objectKV.IsOffsetRead();
    entry->stateInfo.SetIncompleted(isOffsetRead);

    LOG(INFO) << FormatString(
        "Object %s spilled to disk, metaSize: %zu, dataSize: %zu, %sprepare to get from disk, isOffsetRead: %zu, "
        "readOffset: %zu, readSize: %zu.",
        objectKey, metaSize, dataSize,
        (newShmUnit == nullptr ? std::string() : FormatString("allocate memory size %zu, ", needSize)), isOffsetRead,
        readOffset, readSize);
    // Load data.
    auto pointer = static_cast<uint8_t *>(entry->GetShmUnit()->GetPointer()) + entry->GetMetadataSize() + readOffset;
    Status status = WorkerOcSpill::Instance()->Get(objectKey, pointer, readSize, readOffset);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("Get object %s from disk failed, %s", objectKey, status.GetMsg());
        if (newShmUnit != nullptr) {
            // Get from disk failed, free pointer
            newShmUnit->SetHardFreeMemory();
            LOG_IF_ERROR(entry->FreeResources(), "SafeObj free failed");
        }
        return status;
    }
    evictionManager->Add(objectKey);
    return Status::OK();
}

/*
 * There are 3 scenarios will call this function:
 * 1. Publish no-shm object(firstly or subsequently): the entry will not have data if object already spilled.
 * 2. Remote get from other worker if object not exist locally: the entry will contain data, no allocate needed.
 * 3. Update expired object from remote worker: the entry will not have data if object already spilled.
 */
Status SaveBinaryObjectToMemory(ObjectKV &objectKV, const std::vector<RpcMessage> &payloads,
                                std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                                const std::shared_ptr<ThreadPool> &threadPool)
{
    INJECT_POINT("SaveBinaryObjectToMemory.error");
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &entry = objectKV.GetObjEntry();
    RETURN_RUNTIME_ERROR_IF_NULL(entry.Get());
    VLOG(1) << "Begin to dump object locally";

    std::vector<std::pair<const uint8_t *, uint64_t>> payloadData;
    uint64_t payloadSz = 0;
    for (const auto &msg : payloads) {
        payloadData.emplace_back(reinterpret_cast<const uint8_t *>(msg.Data()), msg.Size());
        payloadSz += msg.Size();
    }
    // Only create new shm if size changed or not exist.
    auto metaSz = entry->GetMetadataSize();
    uint64_t cap = payloadSz + metaSz;
    bool szChanged = (entry->GetShmUnit() == nullptr) || (entry->GetShmUnit()->size != cap);
    if (szChanged) {
        auto shmUnit = std::make_shared<ShmUnit>();
        RETURN_IF_NOT_OK(AllocateMemoryForObject(objectKey, payloadSz, metaSz, false, evictionManager, *shmUnit,
                                                 entry->modeInfo.GetCacheType()));
        shmUnit->id = ShmKey::Intern(GetStringUuid());
        entry->SetShmUnit(shmUnit);
    }
    // There is no need to latch buffer because client can't access the buffer this moment.
    PerfPoint copyPoint(PerfKey::WORKER_MEMORY_COPY);
    Status status = entry->GetShmUnit()->MemoryCopy(payloadData, threadPool, metaSz);
    if (status.IsError()) {
        entry->GetShmUnit()->SetHardFreeMemory();
        entry->GetShmUnit()->FreeMemory();
        LOG(ERROR) << "Fail to operate entry memory copy because of " << status.ToString();
        return status;
    }
    copyPoint.Record();
    entry->SetDataSize(payloadSz);
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
