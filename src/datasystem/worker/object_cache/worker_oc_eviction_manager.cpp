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

#include "datasystem/common/util/gflag/eviction_watermark.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

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
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/master/object_cache/master_oc_service_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

DS_DECLARE_uint32(eviction_reserve_mem_threshold_mb);
DS_DECLARE_uint32(spill_thread_num);
DS_DECLARE_bool(spill_to_remote_worker);

#ifdef WITH_TESTS
constexpr uint32_t MASTER_TASK_THREAD_NUM = 4;
#else
constexpr uint32_t MASTER_TASK_THREAD_NUM = 8;
#endif

constexpr uint32_t SPILL_EVICT_THREAD_NUM = 1;
constexpr uint32_t MEM_EVICT_THREAD_NUM = 1;
constexpr uint32_t PRIMARY_END_LIFE_THREAD_NUM = 1;

namespace datasystem {
namespace object_cache {
static constexpr int DEBUG_LOG_LEVEL = 1;
static constexpr int BATCH_SPILL_THRESHOLD = 512;
static constexpr int BATCH_DELETE_META_THRESHOLD = 300;
static constexpr int BATCH_DELETE_META_MAX_DELAY_MS = 10;
static constexpr size_t PRIMARY_END_LIFE_PENDING_LIMIT = 64;
static constexpr size_t PRIMARY_END_LIFE_BATCH_LIMIT = PRIMARY_END_LIFE_PENDING_LIMIT;
static constexpr int PRIMARY_END_LIFE_BATCH_MAX_DELAY_MS = 10;
static constexpr int64_t PRIMARY_END_LIFE_DELETE_ALL_COPY_TIMEOUT_MS = 5 * SECS_TO_MS;
static constexpr uint32_t PRIMARY_END_LIFE_LOCK_RETRY_TIMES = 3;
static constexpr int PRIMARY_END_LIFE_LOCK_RETRY_INTERVAL_MS = 1;
thread_local std::string evictSpillTaskId;

constexpr uint64_t EVICTION_TRACE_SUMMARY_INTERVAL_MS = 60 * SECS_TO_MS;
constexpr size_t EVICTION_TRACE_SUMMARY_THRESHOLD = 32;
namespace {
std::string JoinKeys(const std::vector<std::string> &keys)
{
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        ss << keys[i];
    }
    ss << "]";
    return ss.str();
}
}  // namespace

WorkerOcEvictionManager::EvictionTrace::~EvictionTrace()
{
    static thread_local EvictionTraceAggregator aggregator;
    aggregator.Add(*this);
}

WorkerOcEvictionManager::EvictionTraceAggregator::~EvictionTraceAggregator()
{
    for (auto &item : summaries_) {
        Flush(item.first, item.second);
    }
}

void WorkerOcEvictionManager::EvictionTraceAggregator::Add(const EvictionTrace &trace)
{
    if (trace.rc.GetCode() == K_TRY_AGAIN || trace.rc.GetCode() == K_NOT_READY) {
        return;
    }
    auto elapsed = trace.timer.ElapsedMilliSecond();
    if (elapsed > 1 || trace.rc.IsError()) {
        auto actionName = GetActionName(trace.action);
        std::stringstream ss;
        ss << "[TaskId " << trace.taskId << "] ";
        if (!trace.info.empty()) {
            ss << trace.info << ", ";
        }
        ss << "evict action " << actionName << ", total cost " << elapsed << " ms, "
           << "obj size: " << trace.objectSize;
        if (trace.action == Action::SPILL) {
            ss << "spill cost " << trace.spillCost << " ms, ";
        }
        ss << "status:" << (trace.rc.IsOk() ? "OK" : trace.rc.GetMsg());
        LOG(INFO) << ss.str();
    }
    auto nowMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
    auto &summary = summaries_[trace.action];
    if (summary.lastLogTimeMs == 0) {
        summary.lastLogTimeMs = nowMs;
    }
    if (!trace.objectKeySizeMap.empty()) {
        auto &keys = trace.rc.IsOk() ? summary.successKeys : summary.failedKeys;
        keys.reserve(keys.size() + trace.objectKeySizeMap.size());
        for (const auto &item : trace.objectKeySizeMap) {
            keys.emplace_back(item.first);
        }
    } else {
        if (trace.rc.IsOk()) {
            summary.successKeys.emplace_back(trace.taskId);
        } else {
            summary.failedKeys.emplace_back(trace.taskId);
        }
    }
    FlushIfNeeded(trace.action, summary, nowMs);
}

void WorkerOcEvictionManager::EvictionTraceAggregator::FlushIfNeeded(Action action, ActionSummary &summary,
                                                                     uint64_t nowMs)
{
    auto keyCount = summary.successKeys.size() + summary.failedKeys.size();
    if (keyCount >= EVICTION_TRACE_SUMMARY_THRESHOLD
        || nowMs - summary.lastLogTimeMs >= EVICTION_TRACE_SUMMARY_INTERVAL_MS) {
        Flush(action, summary);
    }
}

void WorkerOcEvictionManager::EvictionTraceAggregator::Flush(Action action, ActionSummary &summary)
{
    if (summary.successKeys.empty() && summary.failedKeys.empty()) {
        return;
    }
    LOG(INFO) << "Evict summary, action " << GetActionName(action)
              << ", success key count: " << summary.successKeys.size()
              << ", success keys: " << JoinKeys(summary.successKeys)
              << ", failed key count: " << summary.failedKeys.size()
              << ", failed keys: " << JoinKeys(summary.failedKeys);
    summary.successKeys.clear();
    summary.failedKeys.clear();
    summary.lastLogTimeMs = static_cast<uint64_t>(GetSteadyClockTimeStampMs());
}

const std::unordered_map<WorkerOcEvictionManager::Action, WorkerOcEvictionManager::ActionSummary> &
WorkerOcEvictionManager::EvictionTraceAggregator::GetSummaries() const
{
    return summaries_;
}

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
                                   std::make_unique<ThreadPool>(MEM_EVICT_THREAD_NUM, 0, "MemEvictionThread"));
    RETURN_IF_EXCEPTION_OCCURS(primaryEndLifeThreadPool_ = std::make_unique<ThreadPool>(PRIMARY_END_LIFE_THREAD_NUM, 0,
                                                                                        "PrimaryEndLifeThread"));
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

Status WorkerOcEvictionManager::RemoveMetaFromMasterForEviction(EvictDeletedObjects &objectKeyVersions)
{
    RETURN_OK_IF_TRUE(objectKeyVersions.empty());
    VLOG(DEBUG_LOG_LEVEL) << "RemoveMetaFromMasterForEviction start. Object count: " << objectKeyVersions.size();
    if (clusterManager_ == nullptr) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "ETCD cluster manager is not provided");
    }
    EvictDeletedObjects failedObjects;
    Status lastRc;
    auto addFailedObjects = [&objectKeyVersions, &failedObjects, &lastRc](const std::vector<std::string> &objectKeys,
                                                                          const Status &rc) {
        lastRc = rc;
        for (const auto &objectKey : objectKeys) {
            failedObjects[objectKey] = objectKeyVersions.at(objectKey);
        }
    };
    std::vector<std::string> objectKeys;
    objectKeys.reserve(objectKeyVersions.size());
    for (const auto &item : objectKeyVersions) {
        objectKeys.emplace_back(item.first);
    }
    auto objKeysGrpByMaster = clusterManager_->GroupObjKeysByMasterHostPort(objectKeys);
    INJECT_POINT_NO_RETURN("WorkerOcEvictionManager.RemoveMetaFromMasterForEviction.moveToEmptyMaster",
                           [&objKeysGrpByMaster](const std::string &objectKey) {
                               for (auto &item : objKeysGrpByMaster) {
                                   auto &objectKeys = item.second;
                                   objectKeys.erase(std::remove(objectKeys.begin(), objectKeys.end(), objectKey),
                                                    objectKeys.end());
                               }
                               objKeysGrpByMaster[MetaAddrInfo()].emplace_back(objectKey);
                           });
    for (const auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        const auto &currentObjectKeys = item.second;
        if (currentObjectKeys.empty()) {
            continue;
        }
        if (masterAddr.Empty()) {
            addFailedObjects(currentObjectKeys, { K_NOT_FOUND, "Cannot find master for eviction remove-meta." });
            continue;
        }
        auto workerMasterApi =
            worker::WorkerMasterOCApi::CreateWorkerMasterOCApi(masterAddr, localAddress_, akSkManager_, masterOc_);
        auto rc = workerMasterApi->Init();
        if (rc.IsError()) {
            addFailedObjects(currentObjectKeys, rc);
            continue;
        }
        master::RemoveMetaReqPb req;
        master::RemoveMetaRspPb rsp;
        req.set_address(localAddress_.ToString());
        req.set_cause(master::RemoveMetaReqPb::EVICTION);
        req.set_version(UINT64_MAX);
        *req.mutable_ids() = { currentObjectKeys.begin(), currentObjectKeys.end() };
        for (const auto &objectKey : currentObjectKeys) {
            auto *objKeyVersionPb = req.add_id_with_version();
            objKeyVersionPb->set_id(objectKey);
            objKeyVersionPb->set_version(objectKeyVersions.at(objectKey));
        }
        rc = workerMasterApi->RemoveMeta(req, rsp);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("RemoveMeta failed, object count %zu, status: %s.", currentObjectKeys.size(),
                                       rc.ToString());
            addFailedObjects(currentObjectKeys, rc);
            continue;
        }
        if (rsp.meta_is_moving()) {
            addFailedObjects(currentObjectKeys, { K_TRY_AGAIN, "Meta is moving." });
            continue;
        }
        if (!rsp.failed_ids().empty()) {
            std::vector<std::string> failedIds(rsp.failed_ids().begin(), rsp.failed_ids().end());
            addFailedObjects(failedIds, { K_TRY_AGAIN, "RemoveMeta returned failed ids." });
        }
    }
    objectKeyVersions = std::move(failedObjects);
    return objectKeyVersions.empty() ? Status::OK() : lastRc;
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

Status WorkerOcEvictionManager::EvictObject(ObjectKV &objectKV, Action nextAction, EvictDeletedObjects *deletedObjects,
                                            CacheType cacheType, uint64_t needSize)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &entry = objectKV.GetObjEntry();
    if (nextAction == Action::END_LIFE) {
        bool accepted = false;
        Status rc = SubmitPrimaryEndLifeTask(objectKV, cacheType, needSize, accepted);
        (void)memEvictionList_.Erase(objectKey);
        RETURN_IF_NOT_OK(rc);
        VLOG(1) << FormatString("[ObjectKey %s] Object will be end of life, accepted: %d", objectKey, accepted);
        return Status::OK();
    }
    (void)memEvictionList_.Erase(objectKey);
    if (nextAction == Action::DELETE) {
        PerfPoint point(PerfKey::WORKER_EVICT_DELETE);
        uint64_t version = entry.Get()->GetCreateTime();
        // No need to call FreeResources as destructor will free the resources.
        RETURN_IF_NOT_OK(objectTable_->Erase(objectKey, entry));
        if (deletedObjects == nullptr) {
            EvictDeletedObjects objectKeyVersions = { { objectKey, version } };
            SubmitAsyncMasterTask(objectKeyVersions);
        } else {
            (*deletedObjects)[objectKey] = version;
        }
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
        * GetEvictionLowWaterFactor());
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
    EvictDeletedObjects deletedObjects;
    Timer deletedObjectsFlushTimer(BATCH_DELETE_META_MAX_DELAY_MS);
    auto flushDeletedObjects = [this, &deletedObjects, &deletedObjectsFlushTimer]() {
        if (deletedObjects.empty()) {
            return;
        }
        SubmitAsyncMasterTask(deletedObjects);
        deletedObjects.clear();
        deletedObjectsFlushTimer.Reset();
    };
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
        if (rc.IsError()) {
            trace->rc = Status(rc.GetCode(), FormatString("GetAndLockEntry failed %s.", rc.GetMsg()));
            continue;
        }
        ObjectKV objectKV(candidateId, *entry);
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
        bool wasDeletedObjectsEmpty = deletedObjects.empty();
        rc = TryEvictObject(entry, std::move(trace), pendingSpillSize, spillTasks, locked, cacheType, &deletedObjects,
                            needSize);
        if (wasDeletedObjectsEmpty && !deletedObjects.empty()) {
            deletedObjectsFlushTimer.Reset();
        }
        if (deletedObjects.size() >= BATCH_DELETE_META_THRESHOLD
            || (!deletedObjects.empty() && deletedObjectsFlushTimer.IsTimeout())) {
            flushDeletedObjects();
        }
        if (rc.IsError()) {
            evictFailedIds.emplace_back(candidateId, READD_COUNTER);
        }
        auto spilledSize = ReleaseSpillFutures(spillTasks, evictFailedIds, false);
        pendingSpillSize -= std::min(pendingSpillSize, spilledSize);
        INJECT_POINT("worker.Evict", [&pendingSpillSize](size_t size) { pendingSpillSize = size; });
    }
    flushDeletedObjects();
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
    DataMigrator migrator(MigrateType::SPILL, clusterManager_, localAddress_, akSkManager_, objectTable_, taskId,
                          maxRetryCount);
    migrator.Init();
    Status rc = migrator.Migrate(objectKeys, migrateObjects);
    migrator.GetFailedKeys(failedMigrateObjectKeys);
    return rc;
}

Status WorkerOcEvictionManager::TryEvictObject(std::shared_ptr<SafeObjType> &entry,
                                               std::unique_ptr<EvictionTrace> trace, size_t &pendingSpillSize,
                                               std::unordered_map<std::string, SpillTask> &spillTasks, bool &locked,
                                               CacheType cacheType, EvictDeletedObjects *deletedObjects,
                                               uint64_t needSize)
{
    const auto &objectKey = trace->taskId;
    ObjectKV objectKV(objectKey, *entry);
    PerfPoint point(PerfKey::WORKER_EVICT_ONE_OBJECT);
    Status rc = EvictObject(objectKV, trace->action, deletedObjects, cacheType, needSize);
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
    LOG_EVERY_T(INFO, LOG_TIME_LIMIT_LEVEL3) << "Eviction start.";
    bool expected = true;
    if (isDone_.compare_exchange_strong(expected, false)) {
        std::unique_lock<std::mutex> lk(cvMutex_);
        auto traceID = Trace::Instance().GetTraceID();
        memEvictTaskThreadPool_->Execute([this, traceID, needSize, cacheType] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            EvictionTask(needSize, cacheType);
        });
    } else {
        LOG_EVERY_T(INFO, LOG_TIME_LIMIT_LEVEL3) << "Evict is going on...";
    }
}

Status WorkerOcEvictionManager::GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest)
{
    return memEvictionList_.GetAllObjectsInfo(res, oldest);
}

void WorkerOcEvictionManager::SubmitAsyncMasterTask(const EvictDeletedObjects &objectKeyVersions)
{
    if (objectKeyVersions.empty()) {
        return;
    }
    auto traceID = Trace::Instance().GetTraceID();
    masterTaskThreadPool_->Execute([this, objectKeyVersions, traceID] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        AsyncMasterTask(objectKeyVersions);
    });
}

void WorkerOcEvictionManager::AsyncMasterTask(const EvictDeletedObjects &objectKeyVersions)
{
    Status rc;
    int retryCount = 0;
    const int maxRetryNum = 3;
    Timer timer;
    EvictDeletedObjects failedObjects = objectKeyVersions;
    do {
        rc = RemoveMetaFromMasterForEviction(failedObjects);
    } while (rc.IsError() && !failedObjects.empty() && retryCount++ < maxRetryNum);
    if (rc.IsError()) {
        LOG_EVERY_T(ERROR, LOG_TIME_LIMIT_LEVEL2) << FormatString(
            "[Object count %zu] RemoveMetaFromMasterForEviction failed, %s", failedObjects.size(), rc.ToString());
    } else {
        auto elapsedMs = timer.ElapsedMilliSecond();
        auto logLevel = elapsedMs > 1 ? 0 : 1;
        VLOG(logLevel) << FormatString("[Object count %zu] RemoveMetaFromMasterForEviction took %f ms",
                                       objectKeyVersions.size(), elapsedMs);
    }
}

Status WorkerOcEvictionManager::SubmitPrimaryEndLifeTask(const ObjectKV &objectKV, CacheType cacheType,
                                                         uint64_t needSize, bool &accepted)
{
    const auto &objectKey = objectKV.GetObjKey();
    PrimaryEndLifeTask task{ objectKey, objectKV.GetObjEntry()->GetCreateTime(), cacheType, needSize };
    RETURN_IF_NOT_OK(ReservePrimaryEndLifeTask(task, accepted));
    if (!accepted) {
        VLOG(DEBUG_LOG_LEVEL) << FormatString("[ObjectKey %s] Primary end-life task already pending.", objectKey);
        return Status::OK();
    }
    Status rc = EnqueuePrimaryEndLifeTask(task);
    if (rc.IsError()) {
        LOG(WARNING) << "[ObjectKey " << objectKey << "] Enqueue primary end-life task failed, " << rc.ToString()
                     << ".";
        ClearPrimaryEndLifePending(task);
    }
    return rc;
}

Status WorkerOcEvictionManager::ReservePrimaryEndLifeTask(PrimaryEndLifeTask &task, bool &accepted)
{
    std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
    task.metaDeleted = false;
    auto metaIter = metaDeletedPrimaryEndLifeObjects_.find(task.objectKey);
    if (metaIter != metaDeletedPrimaryEndLifeObjects_.end()) {
        if (metaIter->second == task.version) {
            task.metaDeleted = true;
        } else {
            metaDeletedPrimaryEndLifeObjects_.erase(metaIter);
        }
    }
    accepted = false;
    auto [iter, inserted] = pendingPrimaryEndLifeObjects_.emplace(task.objectKey, task.version);
    if (!inserted) {
        return Status::OK();
    }
    if (pendingPrimaryEndLifeObjects_.size() > PRIMARY_END_LIFE_PENDING_LIMIT) {
        pendingPrimaryEndLifeObjects_.erase(iter);
        RETURN_STATUS(K_TRY_AGAIN, "Primary end-life pending queue is full.");
    }
    accepted = true;
    return Status::OK();
}

Status WorkerOcEvictionManager::EnqueuePrimaryEndLifeTask(const PrimaryEndLifeTask &task)
{
    std::unique_lock<std::mutex> lock(primaryEndLifeMutex_);
    try {
        primaryEndLifeQueue_.emplace_back(task);
    } catch (const std::exception &e) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Enqueue primary end-life task failed: %s", e.what()));
    }
    if (!primaryEndLifeDrainRunning_) {
        try {
            primaryEndLifeThreadPool_->Execute(&WorkerOcEvictionManager::DrainPrimaryEndLifeTasks, this);
            primaryEndLifeDrainRunning_ = true;
        } catch (const std::exception &e) {
            primaryEndLifeQueue_.pop_back();
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Submit primary end-life task failed: %s", e.what()));
        }
    }
    return Status::OK();
}

void WorkerOcEvictionManager::ClearPrimaryEndLifePending(const PrimaryEndLifeTask &task)
{
    std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
    auto iter = pendingPrimaryEndLifeObjects_.find(task.objectKey);
    if (iter != pendingPrimaryEndLifeObjects_.end() && iter->second == task.version) {
        pendingPrimaryEndLifeObjects_.erase(iter);
    }
}

void WorkerOcEvictionManager::FinishPrimaryEndLifeTask(const PrimaryEndLifeTask &task, bool readd)
{
    {
        std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
        auto pendingIter = pendingPrimaryEndLifeObjects_.find(task.objectKey);
        if (pendingIter != pendingPrimaryEndLifeObjects_.end() && pendingIter->second == task.version) {
            pendingPrimaryEndLifeObjects_.erase(pendingIter);
        }
        auto metaIter = metaDeletedPrimaryEndLifeObjects_.find(task.objectKey);
        if (readd && task.metaDeleted) {
            metaDeletedPrimaryEndLifeObjects_[task.objectKey] = task.version;
        } else if (metaIter != metaDeletedPrimaryEndLifeObjects_.end() && metaIter->second == task.version) {
            metaDeletedPrimaryEndLifeObjects_.erase(metaIter);
        }
    }
    if (readd) {
        memEvictionList_.Add(task.objectKey, READD_COUNTER);
    }
}

void WorkerOcEvictionManager::ReaddPrimaryEndLifeTasks(const std::vector<PrimaryEndLifeTask> &tasks)
{
    for (const auto &task : tasks) {
        FinishPrimaryEndLifeTask(task, true);
    }
}

void WorkerOcEvictionManager::DrainPrimaryEndLifeTasks()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(PRIMARY_END_LIFE_BATCH_MAX_DELAY_MS));
    while (true) {
        auto tasks = PopPrimaryEndLifeTasks();
        if (tasks.empty()) {
            return;
        }
        ProcessPrimaryEndLifeTasks(std::move(tasks));
    }
}

std::vector<WorkerOcEvictionManager::PrimaryEndLifeTask> WorkerOcEvictionManager::PopPrimaryEndLifeTasks()
{
    std::lock_guard<std::mutex> lock(primaryEndLifeMutex_);
    if (primaryEndLifeQueue_.empty()) {
        primaryEndLifeDrainRunning_ = false;
        return {};
    }
    auto batchSize = std::min(primaryEndLifeQueue_.size(), PRIMARY_END_LIFE_BATCH_LIMIT);
    std::vector<PrimaryEndLifeTask> tasks;
    tasks.reserve(batchSize);
    for (size_t i = 0; i < batchSize; ++i) {
        tasks.emplace_back(std::move(primaryEndLifeQueue_.front()));
        primaryEndLifeQueue_.pop_front();
    }
    return tasks;
}

void WorkerOcEvictionManager::ProcessPrimaryEndLifeTasks(std::vector<PrimaryEndLifeTask> tasks)
{
    if (clusterManager_ == nullptr) {
        LOG(ERROR) << "ETCD cluster manager is not provided for primary end-life eviction.";
        ReaddPrimaryEndLifeTasks(tasks);
        return;
    }
    std::vector<std::string> objectKeys;
    std::unordered_map<std::string, PrimaryEndLifeTask> taskByKey;
    objectKeys.reserve(tasks.size());
    taskByKey.reserve(tasks.size());
    for (const auto &task : tasks) {
        objectKeys.emplace_back(task.objectKey);
        taskByKey.emplace(task.objectKey, task);
    }
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> groupedKeys;
    std::optional<std::unordered_map<std::string, Status>> errInfos(std::in_place);
    clusterManager_->GroupObjKeysByMasterHostPortWithStatus(objectKeys, groupedKeys, errInfos);

    std::unordered_set<std::string> routeFailedKeys;
    for (const auto &item : *errInfos) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Skip primary end-life, master unavailable: %s.", item.first,
                                     item.second.ToString());
        routeFailedKeys.emplace(item.first);
    }
    std::vector<PrimaryEndLifeTask> failedTasks;
    failedTasks.reserve(routeFailedKeys.size());
    for (const auto &key : routeFailedKeys) {
        auto iter = taskByKey.find(key);
        if (iter != taskByKey.end()) {
            failedTasks.emplace_back(iter->second);
        }
    }
    ReaddPrimaryEndLifeTasks(failedTasks);

    for (const auto &item : groupedKeys) {
        HostPort masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<PrimaryEndLifeTask> masterTasks;
        masterTasks.reserve(item.second.size());
        for (const auto &objectKey : item.second) {
            if (routeFailedKeys.count(objectKey) == 0 && taskByKey.count(objectKey) > 0) {
                masterTasks.emplace_back(taskByKey.at(objectKey));
            }
        }
        if (masterTasks.empty()) {
            continue;
        }
        if (masterAddr.Empty()) {
            ReaddPrimaryEndLifeTasks(masterTasks);
            continue;
        }
        ProcessPrimaryEndLifeMasterBatch(masterAddr, std::move(masterTasks));
    }
}

void WorkerOcEvictionManager::ProcessPrimaryEndLifeMasterBatch(const HostPort &masterAddr,
                                                               std::vector<PrimaryEndLifeTask> tasks)
{
    std::sort(tasks.begin(), tasks.end(),
              [](const auto &lhs, const auto &rhs) { return lhs.objectKey < rhs.objectKey; });
    std::vector<PrimaryEndLifeCandidate> candidates;
    std::vector<PrimaryEndLifeTask> skippedTasks;
    LOG_IF_ERROR(PreparePrimaryEndLifeCandidates(tasks, candidates, skippedTasks),
                 "Prepare primary end-life candidates failed.");
    ReaddPrimaryEndLifeTasks(skippedTasks);
    if (candidates.empty()) {
        return;
    }
    Raii unlockRaii([&candidates] { UnlockPrimaryEndLifeCandidates(candidates); });
    std::vector<PrimaryEndLifeCandidate> needDeleteMetaCandidates;
    needDeleteMetaCandidates.reserve(candidates.size());
    for (const auto &candidate : candidates) {
        if (!candidate.task.metaDeleted) {
            needDeleteMetaCandidates.emplace_back(candidate);
        }
    }
    std::unordered_set<std::string> failedKeys;
    Status rc;
    if (!needDeleteMetaCandidates.empty()) {
        rc = DeleteAllCopyMetaForPrimaryEndLife(masterAddr, needDeleteMetaCandidates, failedKeys);
    }
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("Primary end-life DeleteAllCopyMeta failed, count %zu, status: %s.",
                                     needDeleteMetaCandidates.size(), rc.ToString());
    }
    for (const auto &candidate : candidates) {
        PrimaryEndLifeTask finishTask = candidate.task;
        bool failed = !candidate.task.metaDeleted && (rc.IsError() || failedKeys.count(candidate.task.objectKey) > 0);
        if (!failed) {
            bool removeAsyncSend = candidate.entry != nullptr && (*candidate.entry)->IsWriteBackL2CacheEvictMode();
            Status deleteRc = DeletePrimaryEndLifeLocal(candidate);
            failed = deleteRc.IsError();
            finishTask.metaDeleted = failed;
            if (!failed && removeAsyncSend) {
                RemovePrimaryEndLifeAsyncSend(candidate.task.objectKey);
            }
        }
        FinishPrimaryEndLifeTask(finishTask, failed);
    }
}

Status WorkerOcEvictionManager::PreparePrimaryEndLifeCandidates(const std::vector<PrimaryEndLifeTask> &tasks,
                                                                std::vector<PrimaryEndLifeCandidate> &candidates,
                                                                std::vector<PrimaryEndLifeTask> &skippedTasks)
{
    std::unordered_map<int, uint64_t> selectedSizeByCache;
    const size_t candidateBegin = candidates.size();
    bool handoffCandidates = false;
    Raii unlockOnError([&candidates, &handoffCandidates, candidateBegin] {
        if (handoffCandidates) {
            return;
        }
        for (size_t i = candidateBegin; i < candidates.size(); ++i) {
            if (candidates[i].entry != nullptr) {
                candidates[i].entry->WUnlock();
            }
        }
        candidates.resize(candidateBegin);
    });
    for (const auto &task : tasks) {
        if (!IsAboveLowWaterMark(task.needSize, 0, task.cacheType)) {
            skippedTasks.emplace_back(task);
            continue;
        }
        std::shared_ptr<SafeObjType> entry;
        Status rc = TryLockPrimaryEndLifeTask(task, entry);
        if (rc.IsError()) {
            skippedTasks.emplace_back(task);
            continue;
        }
        bool entryLocked = true;
        Raii unlockEntry([&entry, &entryLocked] {
            if (entryLocked) {
                entry->WUnlock();
            }
        });
        if (!IsPrimaryEndLifeTaskStillEvictable(task, *entry)) {
            skippedTasks.emplace_back(task);
            continue;
        }
        auto cacheKey = static_cast<int>(task.cacheType);
        auto releaseSize = GetPrimaryEndLifeReleaseSize(*entry);
        auto budget = GetPrimaryEndLifeReleaseBudget(task.cacheType, task.needSize);
        auto selectedSize = selectedSizeByCache[cacheKey];
        // The first candidate is allowed so a single large object can still relieve pressure.
        if (selectedSize != 0 && releaseSize > budget - std::min(budget, selectedSize)) {
            skippedTasks.emplace_back(task);
            continue;
        }
        auto maxSize = std::numeric_limits<uint64_t>::max();
        selectedSizeByCache[cacheKey] = releaseSize > maxSize - selectedSize ? maxSize : selectedSize + releaseSize;
        candidates.emplace_back(PrimaryEndLifeCandidate{ task, entry });
        entryLocked = false;
    }
    handoffCandidates = true;
    return Status::OK();
}

Status WorkerOcEvictionManager::TryLockPrimaryEndLifeTask(const PrimaryEndLifeTask &task,
                                                          std::shared_ptr<SafeObjType> &entry)
{
    RETURN_IF_NOT_OK(objectTable_->Get(task.objectKey, entry));
    Status rc;
    for (uint32_t i = 0; i < PRIMARY_END_LIFE_LOCK_RETRY_TIMES; ++i) {
        rc = entry->TryWLock();
        if (rc.IsOk() || rc.GetCode() != K_TRY_AGAIN) {
            return rc;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(PRIMARY_END_LIFE_LOCK_RETRY_INTERVAL_MS));
    }
    return rc;
}

bool WorkerOcEvictionManager::IsPrimaryEndLifeTaskStillEvictable(const PrimaryEndLifeTask &task,
                                                                 const SafeObjType &entry)
{
    if (entry->GetCreateTime() != task.version) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey << "] Skip primary end-life, version changed, "
                              << "expected: " << task.version << ", current: " << entry->GetCreateTime();
        return false;
    }
    if (!entry->stateInfo.IsPrimaryCopy()) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey << "] Skip primary end-life, not primary copy.";
        return false;
    }
    bool isBinary = entry->IsBinary();
    if (!isBinary && !entry->HasL2Cache()) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey
                              << "] Skip primary end-life, non-binary object has no L2 cache.";
        return false;
    }
    if (isBinary && entry->GetShmUnit() == nullptr) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey
                              << "] Skip primary end-life, binary object has no shm.";
        return false;
    }
    bool hasL2Cache = IsObjectExistInL2Cache(entry);
    if (entry->modeInfo.GetCacheType() == CacheType::DISK) {
        bool evictable = !hasL2Cache && entry->IsNoneL2CacheEvictMode();
        if (!evictable) {
            VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey
                                  << "] Skip primary end-life, disk object is not none-L2 evictable.";
        }
        return evictable;
    }
    if (entry->IsWriteBackL2CacheEvictMode()) {
        return true;
    }
    bool evictable = !hasL2Cache && entry->IsNoneL2CacheEvictMode();
    if (!evictable) {
        VLOG(DEBUG_LOG_LEVEL) << "[ObjectKey " << task.objectKey
                              << "] Skip primary end-life, memory object is not evictable.";
    }
    return evictable;
}

uint64_t WorkerOcEvictionManager::GetPrimaryEndLifeReleaseBudget(CacheType cacheType, uint64_t needSize)
{
    auto realUsage = datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(
        ServiceType::OBJECT, static_cast<memory::CacheType>(cacheType));
    auto max = std::numeric_limits<uint64_t>::max();
    realUsage = realUsage > max - needSize ? max : realUsage + needSize;
    auto lowWater = GetLowWaterMark(cacheType);
    return realUsage > lowWater ? realUsage - lowWater : 0;
}

Status WorkerOcEvictionManager::DeleteAllCopyMetaForPrimaryEndLife(
    const HostPort &masterAddr, const std::vector<PrimaryEndLifeCandidate> &candidates,
    std::unordered_set<std::string> &failedKeys)
{
    auto workerMasterApi =
        worker::WorkerMasterOCApi::CreateWorkerMasterOCApi(masterAddr, localAddress_, akSkManager_, masterOc_);
    RETURN_IF_NOT_OK(workerMasterApi->Init());
    master::DeleteAllCopyMetaReqPb req;
    req.set_address(localAddress_.ToString());
    req.set_redirect(true);
    for (const auto &candidate : candidates) {
        auto *objKeyVersionPb = req.add_ids_with_version();
        objKeyVersionPb->set_id(candidate.task.objectKey);
        objKeyVersionPb->set_version(candidate.task.version);
    }
    master::DeleteAllCopyMetaRspPb rsp;
    reqTimeoutDuration.Init(PRIMARY_END_LIFE_DELETE_ALL_COPY_TIMEOUT_MS);
    Raii resetTimeout([] { reqTimeoutDuration.Reset(); });
    RETURN_IF_NOT_OK(workerMasterApi->DeleteAllCopyMeta(req, rsp));
    return CollectDeleteAllCopyMetaResult(rsp, failedKeys);
}

Status WorkerOcEvictionManager::CollectDeleteAllCopyMetaResult(const master::DeleteAllCopyMetaRspPb &rsp,
                                                               std::unordered_set<std::string> &failedKeys)
{
    Status lastRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    failedKeys.insert(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
    failedKeys.insert(rsp.outdated_objs().begin(), rsp.outdated_objs().end());
    failedKeys.insert(rsp.objs_without_meta().begin(), rsp.objs_without_meta().end());
    for (const auto &redirectInfo : rsp.info()) {
        failedKeys.insert(redirectInfo.change_meta_ids().begin(), redirectInfo.change_meta_ids().end());
    }
    if (rsp.meta_is_moving()) {
        RETURN_STATUS(K_TRY_AGAIN, "DeleteAllCopyMeta meta is moving.");
    }
    RETURN_IF_NOT_OK(lastRc);
    return Status::OK();
}

void WorkerOcEvictionManager::RemovePrimaryEndLifeAsyncSend(const std::string &objectKey)
{
    if (auto sp = asyncSendManager_.lock()) {
        sp->Remove(objectKey);
    }
}

Status WorkerOcEvictionManager::DeletePrimaryEndLifeLocal(const PrimaryEndLifeCandidate &candidate)
{
    const auto &objectKey = candidate.task.objectKey;
    auto &entry = *candidate.entry;
    if (entry->IsSpilled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(WorkerOcSpill::Instance()->Delete(objectKey),
                                         FormatString("[ObjectKey %s] Delete from disk failed", objectKey));
    }
    entry->stateInfo.SetSpillState(false);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectTable_->Erase(objectKey, entry),
                                     FormatString("Failed to erase object %s from object table", objectKey));
    return Status::OK();
}

uint64_t WorkerOcEvictionManager::GetPrimaryEndLifeReleaseSize(const SafeObjType &entry)
{
    auto dataSize = entry->GetDataSize();
    auto metaSize = entry->GetMetadataSize();
    auto max = std::numeric_limits<uint64_t>::max();
    return dataSize > max - metaSize ? max : dataSize + metaSize;
}

void WorkerOcEvictionManager::UnlockPrimaryEndLifeCandidates(const std::vector<PrimaryEndLifeCandidate> &candidates)
{
    for (const auto &candidate : candidates) {
        candidate.entry->WUnlock();
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
                                               std::vector<std::string> &failedKeys)
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
    if (clusterManager_ == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_NOT_FOUND, "ETCD cluster manager is not provided");
    }
    MetaAddrInfo metaAddrInfo;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clusterManager_->GetMetaAddress(objectKey, metaAddrInfo),
                                     "Get metadata address failed.");

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
    std::unordered_set<std::string> failedKeys;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CollectDeleteAllCopyMetaResult(rsp, failedKeys), "Delete from master failed.");
    if (!failedKeys.empty()) {
        RETURN_STATUS_LOG_ERROR(K_TRY_AGAIN, FormatString("DeleteAllCopyMeta needs retry, objectKey %s.", objectKey));
    }

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
        case Action::MIGRATE:
            return "migrate";
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
        return std::max(static_cast<uint64_t>(maxAvailableMemorySize * GetEvictionHighWaterFactor()),
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
