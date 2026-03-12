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
 * Description: Implementation of EvictionList and EvictionManager.
 */
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

DS_DECLARE_uint32(eviction_reserve_mem_threshold_mb);
DS_DEFINE_uint32(eviction_thread_num, 1, "Thread number of eviction for object cache.");
DS_DEFINE_validator(eviction_thread_num, &Validator::ValidateThreadNum);
DS_DECLARE_uint32(spill_thread_num);
DS_DECLARE_bool(spill_to_remote_worker);

#ifdef WITH_TESTS
constexpr uint32_t MASTER_TASK_THREAD_NUM = 4;
#else
constexpr uint32_t MASTER_TASK_THREAD_NUM = 8;
#endif

constexpr uint32_t SPILL_EVICT_THREAD_NUM = 1;

namespace datasystem {
namespace object_cache {
static constexpr int DEBUG_LOG_LEVEL = 1;
static constexpr int BATCH_SPILL_THRESHOLD = 512;
thread_local std::string evictSpillTaskId;

WorkerOcEvictionManager::WorkerOcEvictionManager(std::shared_ptr<ObjectTable> objectTable, HostPort localAddress,
                                                 HostPort masterAddress, master::MasterOCServiceImpl *masterOc)
    : objectTable_(std::move(objectTable)),
      localAddress_(std::move(localAddress)),
      masterAddress_(std::move(masterAddress)),
      isDone_(true),
      masterOc_(masterOc)
{
}

Status WorkerOcEvictionManager::Init(const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &gRefTable,
                                     std::shared_ptr<AkSkManager> akSkManager)
{
    RETURN_IF_EXCEPTION_OCCURS(memEvictTaskThreadPool_ =
                                   std::make_unique<ThreadPool>(FLAGS_eviction_thread_num, 0, "MemEvictionThread"));
    RETURN_IF_EXCEPTION_OCCURS(spillEvictTaskThreadPool_ =
                                   std::make_unique<ThreadPool>(SPILL_EVICT_THREAD_NUM, 0, "SpillEvictionThread"));
    RETURN_IF_EXCEPTION_OCCURS(masterTaskThreadPool_ =
                                   std::make_unique<ThreadPool>(MASTER_TASK_THREAD_NUM, 0, "MasterTaskThread"));
    RETURN_IF_EXCEPTION_OCCURS(spillTaskThreadPool_ =
                                   std::make_unique<ThreadPool>(FLAGS_spill_thread_num, 0, "SpillThread"));
    RETURN_IF_EXCEPTION_OCCURS(scheduleEvictThreadPool_ = std::make_unique<ThreadPool>(1, 0, "scheduleEvictThread"));
    // reduce warn log output when thread pool is almost full
    spillTaskThreadPool_->SetWarnLevel(ThreadPool::WarnLevel::LOW);
    RETURN_IF_NOT_OK(WorkerOcSpill::Instance()->Init());
    gRefTable_ = gRefTable;
    akSkManager_ = std::move(akSkManager);
    scheduleEvictThreadPool_->Submit([this]() {
        Timer timer;
        while (!IsTermSignalReceived()) {
            auto evictInterval = 10;
            if (timer.ElapsedSecond() > evictInterval) {
                EvictWhenMemoryExceedThrehold("", 0, shared_from_this(), ServiceType::OBJECT);
                EvictWhenMemoryExceedThrehold("", 0, shared_from_this(), ServiceType::STREAM);
                timer.Reset();
            }
            auto checkIntervalMs = 10;
            std::this_thread::sleep_for(std::chrono::milliseconds(checkIntervalMs));
        }
    });
    return Status::OK();
}

std::string WorkerOcEvictionManager::GetSpillTaskId()
{
    return evictSpillTaskId;
}

void WorkerOcEvictionManager::Add(const std::string &objectKey)
{
    VLOG(DEBUG_LOG_LEVEL) << FormatString("[ObjectKey %s] EvictionManager add start.", objectKey);
    uint8_t counter;
    // Mutable or immutable object
    const uint32_t workerRefCnt = gRefTable_->GetRefWorkerCount(objectKey);
    if (workerRefCnt == 0) {
        counter = Q1;
    } else {
        counter = Q2;
    }
    memEvictionList_.Add(objectKey, counter);
}

void WorkerOcEvictionManager::Erase(const std::string &objectKey)
{
    VLOG(DEBUG_LOG_LEVEL) << FormatString("[ObjectKey %s] EvictionManager erase start.", objectKey);
    // If the data does not exist, it means that the deletion was successful, so there is no need to return a Status.
    (void)memEvictionList_.Erase(objectKey);
}

Status WorkerOcEvictionManager::RemoveMetaFromMasterForEviction(const std::string &objectKey, uint64_t version)
{
    VLOG(DEBUG_LOG_LEVEL) << "RemoveMetaFromMasterForEviction start. ObjectKey: " << objectKey;
    // Get Master address from objectKey
    if (etcdCM_ == nullptr) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "ETCD cluster manager is not provided");
    }
    MetaAddrInfo metaAddrInfo;
    RETURN_IF_NOT_OK(etcdCM_->GetMetaAddress(objectKey, metaAddrInfo));

    auto workerMasterApi = worker::WorkerMasterOCApi::CreateWorkerMasterOCApi(metaAddrInfo.GetAddressAndSaveDbName(),
                                                                              localAddress_, akSkManager_, masterOc_);
    RETURN_IF_NOT_OK(workerMasterApi->Init());
    master::RemoveMetaReqPb req;
    master::RemoveMetaRspPb rsp;
    req.add_ids(objectKey);
    req.set_address(localAddress_.ToString());
    req.set_cause(master::RemoveMetaReqPb::EVICTION);
    req.set_version(version);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApi->RemoveMeta(req, rsp),
                                     FormatString("RemoveMeta failed, objectKey %s.", objectKey));
    VLOG(DEBUG_LOG_LEVEL) << "RemoveMetaFromMasterForEviction end. ObjectKey: " << objectKey;
    return Status::OK();
}

void WorkerOcEvictionManager::GetObjectNextAction(SafeObjType &entry, std::unique_ptr<EvictionTrace> &trace,
                                                  size_t pendingSpillSize)
{
    Action nextAction;
    std::string info;
    bool hasL2Cache = IsObjectExistInL2Cache(entry);
    size_t needSpillSize = pendingSpillSize + entry->GetDataSize();
    if (!entry->stateInfo.IsPrimaryCopy()) {
        info = "not primary copy";
        nextAction = Action::DELETE;
    } else if (entry->modeInfo.GetCacheType() == CacheType::DISK) {
        if (hasL2Cache) {
            info = "object has L2 cache, which cache type is DISK";
            nextAction = Action::DELETE;
        } else if (entry->IsNoneL2CacheEvictMode()) {
            info = "object could be evict, which cache type is DISK";
            nextAction = Action::END_LIFE;
        } else {
            info = "object don't have L2 cache, which cache type is DISK";
            nextAction = Action::RETAIN;
        }
    } else if (entry->IsSpilled()) {
        info = "already spilled";
        nextAction = Action::FREE_MEMORY;
    } else if (FLAGS_spill_to_remote_worker && NodeSelector::Instance().HasEnoughAvailableMemory(needSpillSize)) {
        info = "object could be migrated";
        nextAction = Action::MIGRATE;
    } else if (WorkerOcSpill::Instance()->IsEnabled()) {
        const double ratio = 0.95;
        bool spaceFull = WorkerOcSpill::Instance()->IsSpaceExceed(ratio, needSpillSize);
        info =
            FormatString("space is %sfull and object has %sL2 cache", spaceFull ? "" : "not ", hasL2Cache ? "" : "no ");
        if (spaceFull && hasL2Cache) {
            nextAction = Action::DELETE;
        } else {
            nextAction = Action::SPILL;
        }
    } else if (entry->IsWriteBackL2CacheEvictMode()) {
        info = "object is WRITE_BACK_L2_CACHE_EVICT mode";
        nextAction = Action::END_LIFE;
    } else if (hasL2Cache) {
        info = "object has L2 cache but no spill directory";
        nextAction = Action::DELETE;
    } else if (entry->IsNoneL2CacheEvictMode()) {
        info = "object could be evict";
        nextAction = Action::END_LIFE;
    } else {
        info = "no spill directories are configured";
        nextAction = Action::RETAIN;
    }
    INJECT_POINT("evictAction.setDelete", [&nextAction]() { nextAction = Action::DELETE; });
    trace->info = info;
    trace->action = nextAction;
}

Status WorkerOcEvictionManager::EvictObject(ObjectKV &objectKV, Action nextAction)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &entry = objectKV.GetObjEntry();
    (void)memEvictionList_.Erase(objectKey);
    if (nextAction == Action::DELETE) {
        PerfPoint point(PerfKey::WORKER_EVICT_DELETE);
        uint64_t version = entry.Get()->GetCreateTime();
        auto traceID = Trace::Instance().GetTraceID();
        masterTaskThreadPool_->Execute([this, objectKey, version, traceID] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            AsyncMasterTask(objectKey, version);
        });
        // No need to call FreeResources as destructor will free the resources.
        RETURN_IF_NOT_OK(objectTable_->Erase(objectKey, entry));
        point.Record();
        VLOG(1) << FormatString("[ObjectKey %s] Object delete success", objectKey);
    } else if (nextAction == Action::FREE_MEMORY) {
        PerfPoint point(PerfKey::WORKER_EVICT_FREE);
        RETURN_IF_NOT_OK(entry->FreeResources());
        point.Record();
        VLOG(1) << FormatString("[ObjectKey %s] Object free success", objectKey);
    } else if (nextAction == Action::MIGRATE) {
        VLOG(1) << FormatString("[ObjectKey %s] Object will be migrated", objectKey);
    } else if (nextAction == Action::SPILL) {
        VLOG(1) << FormatString("[ObjectKey %s] Object will be spill", objectKey);
    } else if (nextAction == Action::END_LIFE) {
        if (entry->IsWriteBackL2CacheEvictMode()) {
            if (auto sp = asyncSendManager_.lock()) {
                sp->Remove(objectKey);
            }
        }
        VLOG(1) << FormatString("[ObjectKey %s] Object will be end of life", objectKey);
        RETURN_IF_NOT_OK(DeleteNoneL2CacheEvictableObject(objectKV));
    } else {
        RETURN_STATUS(K_NOT_READY, "No space in EvictObject");
    }
    return Status::OK();
}

Status WorkerOcEvictionManager::EvictClearObject(ObjectKV &objectKV)
{
    return EvictObject(objectKV, Action::DELETE);
}

uint64_t WorkerOcEvictionManager::GetLowWaterMark(CacheType cacheType)
{
    memory::CacheType memCacheType = static_cast<memory::CacheType>(cacheType);
    auto maxMemorySize = datasystem::memory::Allocator::Instance()->GetMaxMemorySize(ServiceType::OBJECT, memCacheType);
    auto usedMemorySize =
        datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::OBJECT, memCacheType);
    auto lowWater = static_cast<std::uint64_t>(
        std::min(datasystem::memory::Allocator::Instance()->GetTotalRealMemoryFree(memCacheType) + usedMemorySize,
                 maxMemorySize)
        * LOW_WATER_FACTOR);
    return lowWater;
}

bool WorkerOcEvictionManager::IsAboveLowWaterMark(uint64_t needSize, size_t pendingSpillSize, CacheType cacheType)
{
    uint64_t max = std::numeric_limits<uint64_t>::max();
    auto realMemoryUsage = datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(
        ServiceType::OBJECT, static_cast<memory::CacheType>(cacheType));
    realMemoryUsage = (realMemoryUsage > max - needSize) ? max : realMemoryUsage + needSize;
    auto lowWater = GetLowWaterMark(cacheType);
    lowWater = (lowWater > max - pendingSpillSize) ? max : lowWater + pendingSpillSize;
    return realMemoryUsage > lowWater;
}

void WorkerOcEvictionManager::EvictionTask(uint64_t needSize, CacheType cacheType)
{
    EvictFailedList evictFailedIds;
    std::unordered_map<std::string, SpillTask> spillTasks;
    LOG(INFO) << "EvictionList size before evict: " << memEvictionList_.Size();
    size_t pendingSpillSize = 0;
    // The size of low water mark memory usage is not fixed. It varies on the size of shared memory available.
    // Share memory release is delayed due to asynchronous spill, so the pending spill data size needs to be counted to
    // prevent all objects from being spilled.
    while (IsAboveLowWaterMark(needSize, pendingSpillSize, cacheType) && memEvictionList_.Size() != 0) {
        std::string candidateId;
        if (memEvictionList_.FindEvictCandidate(candidateId).IsError()) {
            LOG(ERROR) << "FindEvictCandidate failed, EvictionList is empty.";
            continue;
        }
        auto trace = std::make_unique<EvictionTrace>(candidateId);
        std::shared_ptr<SafeObjType> entry;
        Status rc = GetAndLockEntry(candidateId, entry, evictFailedIds);
        ObjectKV objectKV(candidateId, *entry);
        if (rc.IsError()) {
            trace->rc = Status(rc.GetCode(), FormatString("GetAndLockEntry failed %s.", rc.GetMsg()));
            continue;
        }
        bool locked = true;
        Raii unLockRaii([entry, &locked]() {
            if (locked) {
                entry->WUnlock();
            }
        });
        trace->AddObjectKeySize(candidateId, (*entry)->GetDataSize());
        // This moment object key may not in EvictionList.
        // It may be erased in other place after we got candidateId.
        // So we need to check it before do evict.
        if (!IsObjectEvictable(objectKV)) {
            trace->rc = Status(K_RUNTIME_ERROR, "IsObjectEvictable return false");
            continue;
        }
        GetObjectNextAction(*entry, trace, pendingSpillSize);
        rc = TryEvictObject(entry, std::move(trace), pendingSpillSize, spillTasks, locked);
        if (rc.IsError()) {
            evictFailedIds.emplace_back(candidateId, READD_COUNTER);
        }
        auto spilledSize = ReleaseSpillFutures(spillTasks, evictFailedIds, false);
        pendingSpillSize -= std::min(pendingSpillSize, spilledSize);
        INJECT_POINT("worker.Evict", [&pendingSpillSize](size_t size) { pendingSpillSize = size; });
    }
    auto it = spillTasks.find(GetSpillTaskId());
    if (it != spillTasks.end() && !it->second.future.valid() && !it->second.trace->objectKeySizeMap.empty()) {
        it->second.future = SubmitBatchSpillTask(GetSpillTaskId(), it->second.trace->objectKeySizeMap);
    }
    (void)ReleaseSpillFutures(spillTasks, evictFailedIds, true);

    for (const auto &objKeyCounter : evictFailedIds) {
        memEvictionList_.Add(objKeyCounter.first, objKeyCounter.second);
    }
    isDone_ = true;
    LOG(INFO) << "EvictionList size after evict:" << memEvictionList_.Size()
              << ", failed size:" << evictFailedIds.size();
}

Status WorkerOcEvictionManager::MigrateData(const std::string &taskId,
                                            const std::unordered_map<std::string, size_t> &migrateObjects,
                                            std::vector<std::string> &failedMigrateObjectKeys)
{
    RETURN_OK_IF_TRUE(migrateObjects.empty());
    std::vector<std::string> objectKeys;
    objectKeys.reserve(migrateObjects.size());
    std::transform(migrateObjects.begin(), migrateObjects.end(), std::back_inserter(objectKeys),
                   [](const auto &pair) { return pair.first; });
    int maxRetryCount = 5;
    INJECT_POINT("worker.MigrateData.setMaxRetryCount", [&maxRetryCount](int cnt) {
        maxRetryCount = cnt;
        return Status::OK();
    });
    DataMigrator migrator(MigrateType::SPILL, etcdCM_, localAddress_, akSkManager_, objectTable_, taskId,
                          maxRetryCount);
    migrator.Init();
    Status rc = migrator.Migrate(objectKeys, migrateObjects);
    migrator.GetFailedKeys(failedMigrateObjectKeys);
    return rc;
}

Status WorkerOcEvictionManager::TryEvictObject(std::shared_ptr<SafeObjType> &entry,
                                               std::unique_ptr<EvictionTrace> trace, size_t &pendingSpillSize,
                                               std::unordered_map<std::string, SpillTask> &spillTasks, bool &locked)
{
    const auto &objectKey = trace->taskId;
    ObjectKV objectKV(objectKey, *entry);
    PerfPoint point(PerfKey::WORKER_EVICT_ONE_OBJECT);
    Status rc = EvictObject(objectKV, trace->action);
    if (rc.IsError()) {
        trace->rc = rc;
        if (rc.GetCode() != K_NOT_READY) {
            trace->rc.AppendMsg("EvictObject failed");
        } else if ((*entry)->modeInfo.GetCacheType() == CacheType::DISK) {
            return Status::OK();
        }
        return rc;
    }
    if (trace->action == Action::MIGRATE) {
        if (GetSpillTaskId().empty()) {
            evictSpillTaskId = GetStringUuid();
        }
        auto it = spillTasks.find(GetSpillTaskId());
        if (it == spillTasks.end()) {
            spillTasks.emplace(GetSpillTaskId(),
                               SpillTask{ TaskType::BATCH, std::future<SpillResult>{}, std::move(trace) });
        } else {
            it->second.trace->AddObjectKeySize(objectKey, (*entry)->GetDataSize());
            if (it->second.trace->objectKeySizeMap.size() >= BATCH_SPILL_THRESHOLD) {
                it->second.future = SubmitBatchSpillTask(GetSpillTaskId(), it->second.trace->objectKeySizeMap);
                evictSpillTaskId.clear();
            }
        }
        pendingSpillSize += (*entry)->GetDataSize();
    } else if (trace->action == Action::SPILL) {
        auto objectSize = (*entry)->GetDataSize();
        auto version = (*entry)->GetCreateTime();
        // Ensure the spill task for the same object are not concurrent
        if (spillTasks.count(objectKey) > 0) {
            RETURN_STATUS(K_TRY_AGAIN, "Spill task is running.");
        }
        entry->WUnlock();
        locked = false;
        pendingSpillSize += objectSize;
        spillTasks.emplace(objectKey,
                           SpillTask{ TaskType::SINGLE, SubmitSpillTask(objectKey, version), std::move(trace) });
    }
    return Status::OK();
}

void WorkerOcEvictionManager::Evict(uint64_t needSize, CacheType cacheType)
{
    LOG(INFO) << "Eviction start.";
    bool expected = true;
    if (isDone_.compare_exchange_strong(expected, false)) {
        std::unique_lock<std::mutex> lk(cvMutex_);
        auto traceID = Trace::Instance().GetTraceID();
        memEvictTaskThreadPool_->Execute([this, traceID, needSize, cacheType] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            EvictionTask(needSize, cacheType);
        });
    } else {
        LOG(INFO) << "Evict is going on...";
    }
}

Status WorkerOcEvictionManager::GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest)
{
    return memEvictionList_.GetAllObjectsInfo(res, oldest);
}

void WorkerOcEvictionManager::AsyncMasterTask(const std::string &objectKey, uint64_t version)
{
    LOG(INFO) << FormatString("[ObjectKey %s] Start AsyncMasterTask. [version: %zu]", objectKey, version);
    Status rc;
    int retryCount = 0;
    const int maxRetryNum = 3;
    do {
        rc = RemoveMetaFromMasterForEviction(objectKey, version);
    } while (rc.IsError() && retryCount++ < maxRetryNum);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("[ObjectKey %s] RemoveMetaFromMasterForEviction failed, %s", objectKey,
                                   rc.ToString());
    }
}

Status WorkerOcEvictionManager::SpillImpl(const std::string &objectKey, uint64_t version)
{
    INJECT_POINT("worker.SubmitSpillTask");
    // Retry case: 1. try lock failed; 2. Spill failed;
    // Ignore case: 1. object not exists; 2. Shm released; 3. version changed.
    std::shared_ptr<SafeObjType> entryPtr;
    {
        Status rc = GetAndLockEntry(objectKey, version, false, entryPtr);
        if (rc.IsError()) {
            return rc.GetCode() == K_TRY_AGAIN ? rc : Status::OK();
        }

        bool locked = true;
        Raii rUnlockRaii([entryPtr, &locked] {
            if (locked) {
                entryPtr->RUnlock();
            }
        });
        SafeObjType &entry = *entryPtr;

        auto dataSize = entry->GetDataSize();
        auto metaSize = entry->GetMetadataSize();
        TryEvictSpilledObjects(dataSize);
        ShmGuard shmGuard(entry->GetShmUnit(), dataSize, metaSize);
        if (WorkerOcServiceCrudCommonApi::ShmEnable() && !shmGuard.TryRLatch(false)) {
            return Status(K_TRY_AGAIN, "TryRLatch failed");
        }
        // ShmGuard will hold the shm unit.
        auto shmUnit = entry->GetShmUnit();
        RETURN_RUNTIME_ERROR_IF_NULL(shmUnit);
        const void *buffer = static_cast<uint8_t *>(shmUnit->GetPointer()) + metaSize;
        bool isNoneL2EvictType = entry->IsNoneL2CacheEvictMode();
        bool canEvict = entry->HasL2Cache() || isNoneL2EvictType;
        entryPtr->RUnlock();
        locked = false;
        (void)locked;
        rc = WorkerOcSpill::Instance()->Spill(objectKey, buffer, dataSize, canEvict);
        if (isNoneL2EvictType && rc.GetCode() == StatusCode::K_NO_SPACE) {
            // If we lock failed, we can do nothing but retry next time.
            Status s = GetAndLockEntry(objectKey, version, true, entryPtr);
            if (s.IsError()) {
                return rc;
            }
            Raii wUnlockRaii([entryPtr] { entryPtr->WUnlock(); });
            rc = DeleteNoneL2CacheEvictableObject({ objectKey, entry });
        }
        RETURN_IF_NOT_OK(rc);
    }

    Status rc = GetAndLockEntry(objectKey, version, true, entryPtr);
    if (rc.IsError()) {
        // Rollback if failed.
        LOG_IF_ERROR(WorkerOcSpill::Instance()->Delete(objectKey), "Delete failed");
        return rc.GetCode() == K_TRY_AGAIN ? rc : Status::OK();
    }

    Raii wUnlockRaii([entryPtr] { entryPtr->WUnlock(); });
    LOG_IF_ERROR((*entryPtr)->FreeResources(), "SafeObj free failed");
    (*entryPtr)->stateInfo.SetSpillState(true);

    return Status::OK();
}

std::future<WorkerOcEvictionManager::SpillResult> WorkerOcEvictionManager::SubmitSpillTask(const std::string &objectKey,
                                                                                           uint64_t version)
{
    auto traceId = Trace::Instance().GetTraceID();
    return spillTaskThreadPool_->Submit([this, objectKey, version, traceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Timer timer;
        auto rc = SpillImpl(objectKey, version);
        return SpillResult{ .rc = rc, .elapsed = timer.ElapsedMilliSecond() };
    });
}

Status WorkerOcEvictionManager::BatchSpillImpl(const std::string &taskId,
                                               const std::unordered_map<std::string, uint64_t> &objectKeySizeMap,
                                               std::vector<std::string> failedKeys)
{
    if (FLAGS_spill_to_remote_worker) {
        return MigrateData(taskId, objectKeySizeMap, failedKeys);
    }
    RETURN_STATUS(K_RUNTIME_ERROR, "Current only support migrate data when spill to remote worker is set true");
}

std::future<WorkerOcEvictionManager::SpillResult> WorkerOcEvictionManager::SubmitBatchSpillTask(
    const std::string &taskId, const std::unordered_map<std::string, uint64_t> &objectKeySizeMap)
{
    auto traceId = Trace::Instance().GetTraceID();
    return spillTaskThreadPool_->Submit([this, taskId, objectKeySizeMap, traceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Timer timer;
        std::vector<std::string> failedKeys;
        auto rc = BatchSpillImpl(taskId, objectKeySizeMap, failedKeys);
        return SpillResult{ .rc = rc, .elapsed = timer.ElapsedMilliSecond(), .failedKeys = std::move(failedKeys) };
    });
}

size_t WorkerOcEvictionManager::ReleaseSpillFutures(std::unordered_map<std::string, SpillTask> &spillTasks,
                                                    std::vector<std::pair<std::string, uint8_t>> &evictFailedIds,
                                                    bool last)
{
    size_t spilledSize = 0;
    for (auto iter = spillTasks.begin(); iter != spillTasks.end();) {
        const auto &objectKey = iter->first;
        auto &task = iter->second;
        auto &future = iter->second.future;
        if (!future.valid()) {
            ++iter;
            continue;
        }
        auto &trace = iter->second.trace;
        if (!last) {
            std::future_status taskStatus = future.wait_for(std::chrono::microseconds(0));
            if (taskStatus != std::future_status::ready) {
                ++iter;
                continue;
            }
        } else {
            future.wait();
        }
        auto result = future.get();
        Status spillRc = result.rc;
        spilledSize += trace->objectSize;
        if (task.taskType == TaskType::SINGLE) {
            if (spillRc.IsError()) {
                auto counter = spillRc.GetCode() == StatusCode::K_TRY_AGAIN ? Q1 : READD_COUNTER;
                evictFailedIds.emplace_back(objectKey, counter);
            }
        }
        if (task.taskType == TaskType::BATCH) {
            evictFailedIds.reserve(evictFailedIds.size() + result.failedKeys.size());
            for (const auto &key : result.failedKeys) {
                evictFailedIds.emplace_back(key, READD_COUNTER);
            }
        }
        trace->rc = spillRc;
        trace->spillCost = result.elapsed;
        spillTasks.erase(iter++);
    }
    return spilledSize;
}

void WorkerOcEvictionManager::TryEvictSpilledObjects(uint64_t objectSize)
{
    if (!WorkerOcSpill::Instance()->IsSpaceExceedHWM(objectSize)) {
        return;
    }
    if (spillEvictTaskThreadPool_->GetRunningTasksNum() == 0) {
        spillEvictTaskThreadPool_->Execute(&WorkerOcEvictionManager::EvictSpilledObjects, this, objectSize);
    } else {
        LOG(INFO) << "Spill evict task running...";
    }
}

void WorkerOcEvictionManager::EvictSpilledObjects(uint64_t objectSize)
{
    EvictFailedList evictFailedIds;
    std::unordered_map<std::string, SpillTask> spillTasks;
    auto &spillEvictionList = WorkerOcSpill::Instance()->GetEvictionList();
    LOG(INFO) << "Spill eviction list size before evict: " << spillEvictionList.Size();

    size_t needSkipCount = 0;
    bool forceCompact = false;
    while (spillEvictionList.Size() != 0 && needSkipCount <= spillEvictionList.Size()
           && WorkerOcSpill::Instance()->IsActiveSpillSizeExceedLWM(objectSize)) {
        std::string candidateId;
        if (spillEvictionList.FindEvictCandidate(candidateId).IsError()) {
            LOG(ERROR) << "FindEvictCandidate failed, EvictionList is empty.";
            continue;
        }

        std::shared_ptr<SafeObjType> entry;
        Status rc = GetAndLockEntry(candidateId, entry, evictFailedIds);
        if (rc.IsError()) {
            needSkipCount++;
            continue;
        }
        Raii unLockRaii([entry]() { entry->WUnlock(); });

        if (!IsSpilledObjectEvictable(entry)) {
            needSkipCount++;
            continue;
        }

        needSkipCount = 0;
        if (entry->Get()->IsNoneL2CacheEvictMode()) {
            rc = DeleteNoneL2CacheEvictableObject(ObjectKV(candidateId, *entry));
        } else {
            rc = DeleteL2CacheEvictableObject(ObjectKV(candidateId, *entry));
        }

        if (rc.IsError()) {
            evictFailedIds.emplace_back(candidateId, READD_COUNTER);
        } else {
            forceCompact = true;
            (void)spillEvictionList.Erase(candidateId);
        }
    }

    for (const auto &objKeyCounter : evictFailedIds) {
        spillEvictionList.Add(objKeyCounter.first, objKeyCounter.second);
    }

    double ratio = (WorkerOcSpill::Instance()->LowWaterFactor() + WorkerOcSpill::Instance()->HighWaterFactor()) / 2.0;
    forceCompact &= WorkerOcSpill::Instance()->IsSpaceExceed(ratio, objectSize);
    if (forceCompact) {
        WorkerOcSpill::Instance()->ForceCompact();
    }

    LOG(INFO) << "Spill eviction list size after evict:" << spillEvictionList.Size()
              << ", failed size:" << evictFailedIds.size() << ", force compact: " << forceCompact;
}

bool WorkerOcEvictionManager::IsSpilledObjectEvictable(const std::shared_ptr<SafeObjType> &entry)
{
    auto entryPtr = entry->Get();
    return entryPtr->IsWriteThroughMode() || entryPtr->IsNoneL2CacheEvictMode()
           || (entryPtr->IsWriteBackMode() && entryPtr->stateInfo.IsWriteBackDone());
}

Status WorkerOcEvictionManager::DeleteNoneL2CacheEvictableObject(const ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    VLOG(DEBUG_LOG_LEVEL) << "DeleteNoneL2CacheEvictableObject start. ObjectKey: " << objectKey;
    // Get Master address from objectKey
    if (etcdCM_ == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_NOT_FOUND, "ETCD cluster manager is not provided");
    }
    MetaAddrInfo metaAddrInfo;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCM_->GetMetaAddress(objectKey, metaAddrInfo), "Get metadata address failed.");

    auto workerMasterApi = worker::WorkerMasterOCApi::CreateWorkerMasterOCApi(metaAddrInfo.GetAddressAndSaveDbName(),
                                                                              localAddress_, akSkManager_, masterOc_);
    RETURN_IF_NOT_OK(workerMasterApi->Init());
    master::DeleteAllCopyMetaReqPb req;
    req.add_object_keys(objectKey);
    req.set_address(localAddress_.ToString());
    req.set_redirect(true);
    master::DeleteAllCopyMetaRspPb rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApi->DeleteAllCopyMeta(req, rsp),
                                     FormatString("DeleteAllCopyMeta failed, objectKey %s.", objectKey));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg()),
        "Delete from master failed.");

    if (objectKV.GetObjEntry()->IsSpilled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(WorkerOcSpill::Instance()->Delete(objectKey),
                                         FormatString("[ObjectKey %s] Delete from disk failed", objectKey));
    }
    objectKV.GetObjEntry()->stateInfo.SetSpillState(false);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectTable_->Erase(objectKey, objectKV.GetObjEntry()),
                                     FormatString("Failed to erase object %s from object table", objectKey));
    VLOG(DEBUG_LOG_LEVEL) << "DeleteNoneL2CacheEvictableObject end. ObjectKey: " << objectKey;
    return Status::OK();
}

Status WorkerOcEvictionManager::DeleteL2CacheEvictableObject(const ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    auto &entry = objectKV.GetObjEntry();
    RETURN_IF_NOT_OK_EXCEPT(WorkerOcSpill::Instance()->Delete(objectKey), StatusCode::K_NOT_FOUND);
    entry->stateInfo.SetSpillState(false);
    return Status::OK();
}

Status WorkerOcEvictionManager::GetAndLockEntry(const std::string &objectKey, std::shared_ptr<SafeObjType> &entry,
                                                EvictFailedList &evictFailedIds)
{
    Status rc = objectTable_->Get(objectKey, entry);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object not in ObjectTable, %s.", objectKey, rc.ToString());
        (void)memEvictionList_.Erase(objectKey);
        return rc;
    }
    rc = entry->TryWLock();
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object TryWLock failed, %s.", objectKey, rc.ToString());
        Status eraseRc = memEvictionList_.Erase(objectKey);
        if (rc.GetCode() == K_TRY_AGAIN && eraseRc.IsOk()) {
            // If other thread are using this object, skip it and re-add into EvictionList later.
            uint8_t counter = READD_COUNTER;
            evictFailedIds.emplace_back(objectKey, counter);
        }
    }
    return rc;
}

Status WorkerOcEvictionManager::GetAndLockEntry(const std::string &objectKey, uint64_t version, bool isWrite,
                                                std::shared_ptr<SafeObjType> &entryPtr)
{
    Status rc = objectTable_->Get(objectKey, entryPtr);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object not in ObjectTable, %s.", objectKey, rc.ToString());
        return rc;
    }
    if (isWrite) {
        rc = entryPtr->TryWLock();
    } else {
        rc = entryPtr->TryRLock();
    }
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] %s failed, %s.", objectKey, isWrite ? "TryWLock" : "TryRLock",
                                     rc.ToString());
        return rc;
    }

    bool success = false;
    Raii raii([entryPtr, isWrite, &success] {
        if (!success) {
            isWrite ? entryPtr->WUnlock() : entryPtr->RUnlock();
        }
    });
    SafeObjType &entry = *entryPtr;
    if (entry->GetShmUnit() == nullptr) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object's shm has been free.", objectKey);
        RETURN_STATUS(K_RUNTIME_ERROR, "ShmUnit is null");
    }

    if (entry->GetCreateTime() != version) {
        LOG(WARNING) << FormatString("[ObjectKey %s] version changed, expected:%zu, current:%zu.", objectKey, version,
                                     entry->GetCreateTime());
        RETURN_STATUS(K_RUNTIME_ERROR, "version changed");
    }
    success = true;
    (void)success;
    return Status::OK();
}

bool WorkerOcEvictionManager::IsObjectEvictable(const ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    const SafeObjType &entry = objectKV.GetObjEntry();
    if (!memEvictionList_.Exist(objectKey)) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object not in EvictionList.", objectKey);
        return false;
    }
    bool isBinary = entry->IsBinary();
    if (!isBinary && !entry->HasL2Cache()) {
        LOG(ERROR) << FormatString("[ObjectId %s] Object doesn't have L2 cache, it's wrong status.", objectKey);
        (void)memEvictionList_.Erase(objectKey);
        return false;
    }
    if (isBinary && entry->GetShmUnit() == nullptr) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Object's shm has been free.", objectKey);
        (void)memEvictionList_.Erase(objectKey);
        return false;
    }
    return true;
}

bool WorkerOcEvictionManager::IsObjectExistInL2Cache(const SafeObjType &entry)
{
    return entry->IsWriteThroughMode() || (entry->IsWriteBackMode() && entry->stateInfo.IsWriteBackDone());
}

std::string WorkerOcEvictionManager::GetActionName(Action action)
{
    switch (action) {
        case Action::FREE_MEMORY:
            return "free memory";
        case Action::DELETE:
            return "delete";
        case Action::SPILL:
            return "spill";
        case Action::RETAIN:
            return "retain";
        case Action::END_LIFE:
            return "life end";
        default:
            return "unknown";
    }
}

bool EvictWhenMemoryExceedThrehold(const std::string &keyInfo, uint64_t needSize,
                                   const std::shared_ptr<WorkerOcEvictionManager> &evictionManager, ServiceType type,
                                   CacheType cacheType)
{
    uint64_t realMemoryUsed = 0;
    uint64_t memOccupied = 0;
    uint64_t maxAvailableMemorySize = 0;
    memory::CacheType memCacheType = static_cast<memory::CacheType>(cacheType);
    uint64_t memThreshold = 0;
    auto realObjMemoryUsed =
        datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::OBJECT, memCacheType);
    auto getMemThresInitVal = [](uint64_t maxAvailableMemorySize, uint64_t evictionThresholdMB) {
        return std::max(static_cast<uint64_t>(maxAvailableMemorySize * HIGH_WATER_FACTOR),
                        maxAvailableMemorySize > evictionThresholdMB * MB_TO_BYTES
                            ? maxAvailableMemorySize - evictionThresholdMB * MB_TO_BYTES
                            : 0);
    };
    if (UINT64_MAX - realMemoryUsed < needSize) {
        // If needSize + realMemoryUsed > UINT64_MAX, it means that the needSize is very large,
        // it could never be success, so skip evict.
        return false;
    }
    if (type == ServiceType::OBJECT) {
        realMemoryUsed = realObjMemoryUsed;
        memOccupied = realMemoryUsed + needSize;
        maxAvailableMemorySize = std::min(
            datasystem::memory::Allocator::Instance()->GetMaxMemorySize(type, memCacheType),
            (datasystem::memory::Allocator::Instance()->GetTotalRealMemoryFree(memCacheType) + realMemoryUsed));
        static uint64_t memThresInitVal =
            getMemThresInitVal(maxAvailableMemorySize, FLAGS_eviction_reserve_mem_threshold_mb);
        memThreshold = memThresInitVal;
    } else if (type == ServiceType::STREAM) {
        realMemoryUsed =
            datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::STREAM) + realObjMemoryUsed;
        memOccupied = realMemoryUsed + needSize;
        maxAvailableMemorySize = datasystem::memory::Allocator::Instance()->GetMaxMemoryLimit();
        static uint64_t memThresInitVal =
            getMemThresInitVal(maxAvailableMemorySize, FLAGS_eviction_reserve_mem_threshold_mb);
        memThreshold = memThresInitVal;
    }
    VLOG(1) << FormatString("Allocate memory for %s, size = %lu, memOccupied = %lu, memThreshold = %lu", keyInfo,
                            needSize, memOccupied, memThreshold);
    if (memOccupied >= memThreshold && realObjMemoryUsed > 0) {
        PerfPoint evictPoint(PerfKey::WORKER_EVICT_TASK);
        evictionManager->Evict(needSize, cacheType);
        evictPoint.Record();
        return true;
    }
    return false;
}

}  // namespace object_cache
}  // namespace datasystem
