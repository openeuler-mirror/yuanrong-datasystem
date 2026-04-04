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
 * Description: Implementation of AsyncSendManager.
 */
#include "datasystem/worker/object_cache/async_send_manager.h"

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_counter.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

DS_DECLARE_uint64(spill_size_limit);
DS_DEFINE_uint64(
    l2_cache_async_write_queue_size, 10000,
    "The size of the per-thread asynchronous queue for writing to the secondary cache. With 8 threads, the "
    "system supports a maximum total capacity of 8 * l2_cache_async_write_queue_size key-value pairs pending write.");
DS_DEFINE_uint32(l2_cache_async_write_rate_limit_mb, 200, "Data write to l2cache rate limit for every node");
DS_DEFINE_validator(l2_cache_async_write_rate_limit_mb, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});

DS_DEFINE_validator(l2_cache_async_write_queue_size, [](const char *flagName, uint64_t value) {
    (void)flagName;
    return value > 0;
});

namespace datasystem {
namespace object_cache {

AsyncSendManager::AsyncSendManager(std::shared_ptr<PersistenceApi> api,
                                   std::shared_ptr<WorkerOcEvictionManager> evictionManager)
    : persistenceApi_(api),
      evictionManager_(std::move(evictionManager)),
      limiter_(FLAGS_l2_cache_async_write_rate_limit_mb * 1024ul * 1024ul,
               FLAGS_l2_cache_async_write_rate_limit_mb * 1024ul * 1024ul)
{
}

AsyncSendManager::~AsyncSendManager()
{
    Stop();
}

Status AsyncSendManager::Init()
{
    running_ = true;
    for (int i = 0; i < QUEUE_NUM; i++) {
        queues_.emplace_back(std::make_shared<BlockingList>(FLAGS_l2_cache_async_write_queue_size));
    }
    for (int i = 0; i < QUEUE_NUM; i++) {
        threadPool_.emplace_back(Thread(&AsyncSendManager::Sender, this, i, queues_[i]));
    }
    return Status::OK();
}

void AsyncSendManager::Stop()
{
    if (running_.exchange(false)) {
        LOG(INFO) << "AsyncSendManager exit";
        running_ = false;
        for (auto &thread : threadPool_) {
            thread.join();
        }
    }
}

Status AsyncSendManager::Sender(int threadNum, const std::shared_ptr<BlockingList> &list)
{
    INJECT_POINT("worker.before_pop_from_queue");
    while (running_) {
        // 1. pop key
        std::shared_ptr<Element> element;
        static const int timeoutMs = 1000;
        Status rc = list->Poll(element, timeoutMs);
        if (rc.GetCode() == K_TRY_AGAIN || element == nullptr) {
            continue;
        }
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(element->traceID);
        std::string objectKey = element->key;
        VLOG(1) << FormatString("Sender %d get key: %s", threadNum, objectKey);

        INJECT_POINT("worker.async_send.before_send");

        (void)sendingCount_.fetch_add(1);
        // 2. get entry and lock
        std::shared_ptr<SafeObjType> entry = element->entry;
        while (running_) {
            INJECT_POINT("AsyncSendManager.Sender.verify", [&rc]() {
                static const int sleepTime = 10;
                if (rc.IsError()) {
                    LOG(ERROR) << "Async upload got message: " << rc.GetMsg();
                    std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
                }
                return Status::OK();
            });
            rc = LockAndSendToRemote(objectKey, entry, element->beginAsync);
            if (rc.IsOk() || rc.GetCode() == StatusCode::K_NOT_FOUND) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (!running_ && rc.IsError() && rc.GetCode() != StatusCode::K_NOT_FOUND) {
            AddToFailedObjects(objectKey);
        }
        element->promise.set_value(rc);
        (void)sendingCount_.fetch_sub(1);
    }
    return Status::OK();
}

Status AsyncSendManager::RLockAndSendToRemote(const std::string &objectKey, std::shared_ptr<SafeObjType> entryPtr,
                                              bool &objIsValidInMem,
                                              std::chrono::time_point<std::chrono::steady_clock> &beginTime)
{
    auto &entry = *entryPtr;
    uint64_t createTime;
    WriteMode writeMode = WriteMode::NONE_L2_CACHE;
    auto buf = std::make_shared<std::stringstream>();
    uint64_t dataSize = 0;
    {
        RETURN_IF_NOT_OK(entryPtr->TryRLock());
        Raii readUnlock([&entryPtr]() { entryPtr->RUnlock(); });
        if (entry->IsSpilled()
            && entry->GetShmUnit() == nullptr) {  // At this step, the local memory may already exist.
            objIsValidInMem = false;
            RETURN_STATUS_LOG_ERROR(K_NOT_FOUND, FormatString("Object %s not in memory.", objectKey));
        }
        if (entry->GetShmUnit() == nullptr) {
            LOG(INFO) << FormatString("Object %s is empty, async send abort.", objectKey);
            RETURN_STATUS(StatusCode::K_NOT_FOUND, FormatString("Object %s is empty", objectKey));
        }

        INJECT_POINT("worker.async_send_hold_rLock_timeMs", [](int sleepMs) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
            return Status::OK();
        });
        buf->write(static_cast<char *>(entry->GetShmUnit()->GetPointer()) + entry->GetMetadataSize(),
                   entry->GetDataSize());
        createTime = entry->GetCreateTime();
        writeMode = entry->modeInfo.GetWriteMode();
        dataSize = entry->GetDataSize();
    }
    RETURN_IF_NOT_OK(SendToRemoteOnLock(objectKey, std::move(buf), createTime, dataSize, writeMode, beginTime));
    return AfterSendToRemote(objectKey, entry, createTime);
}

Status AsyncSendManager::SendToRemoteOnLock(const std::string &objectKey, std::shared_ptr<std::stringstream> buf,
                                            uint64_t createTime, uint64_t &dataSize, WriteMode writeMode,
                                            std::chrono::time_point<std::chrono::steady_clock> &beginTime)
{
    static const int timeout = 60000;
    uint64_t elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - beginTime).count());
    LOG(INFO) << FormatString("The elapsed time of async l2cache is %llu.", elapsed);
    limiter_.WaitAllow(dataSize);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(persistenceApi_->Save(objectKey, createTime, timeout, buf, elapsed, writeMode),
                                     FormatString("Call save to l2cache failed. objectKey:%s", objectKey));
    VLOG(1) << FormatString("[ObjectKey %s] Send to l2cache success.", objectKey);
    uint64_t oldVersionMax = createTime == 0 ? 0 : createTime - 1;
    LOG_IF_ERROR(persistenceApi_->Del(objectKey, oldVersionMax, false, elapsed),
                 FormatString("async send, worker delete object's old version failed, objectKey:%s", objectKey));
    return Status::OK();
}

Status AsyncSendManager::LockAndSendToRemote(const std::string &objectKey, std::shared_ptr<SafeObjType> entryPtr,
                                             std::chrono::time_point<std::chrono::steady_clock> &beginTime)
{
    INJECT_POINT("worker.before_send_to_remote");
    // first, try the RLock instead of WLock to allow read the data parallel, not block by other reader.
    bool objIsValidInMem = true;
    Status memGetRes = RLockAndSendToRemote(objectKey, entryPtr, objIsValidInMem, beginTime);
    if (objIsValidInMem) {
        return memGetRes;
    }

    // use TryWLock load object from disk for the first step failed because of object not in memory,
    auto &entry = *entryPtr;
    uint64_t createTime;
    WriteMode writeMode = WriteMode::NONE_L2_CACHE;
    std::unique_ptr<char[]> data;
    auto buf = std::make_shared<std::stringstream>();
    uint64_t dataSize = 0;
    {
        RETURN_IF_NOT_OK(entryPtr->TryWLock());
        Raii wUnlock([&entryPtr]() { entryPtr->WUnlock(); });
        dataSize = entry->GetDataSize();
        if (entry->IsSpilled()) {
            if (entry->GetShmUnit() == nullptr) {
                try {
                    data = std::make_unique<char[]>(dataSize);
                } catch (const std::bad_alloc &e) {
                    RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, e.what());
                }
                LOG(INFO) << FormatString("Object %s spilled to disk, prepare to get from disk.", objectKey);
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(WorkerOcSpill::Instance()->Get(objectKey, data.get(), dataSize, 0ul),
                                                 FormatString("Read spilled object failed. objectKey:%s", objectKey));
                // zero copy.
                buf->rdbuf()->pubsetbuf(data.get(), dataSize);
            }
        } else {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                entry->GetShmUnit() != nullptr, StatusCode::K_NOT_FOUND,
                FormatString("Object %s is not found, ignore save to l2cache.", objectKey));
        }

        // Copy from shared memory.
        if (entry->GetShmUnit() != nullptr) {
            buf->write(static_cast<char *>(entry->GetShmUnit()->GetPointer()) + entry->GetMetadataSize(),
                       entry->GetDataSize());
        }
        createTime = entry->GetCreateTime();
        writeMode = entry->modeInfo.GetWriteMode();
    }
    RETURN_IF_NOT_OK(SendToRemoteOnLock(objectKey, std::move(buf), createTime, dataSize, writeMode, beginTime));
    return AfterSendToRemote(objectKey, entry, createTime);
}

Status AsyncSendManager::AfterSendToRemote(const std::string &objectKey, SafeObjType &entry, uint64_t createTime)
{
    UpdateLastSuccessTimestamp();
    Status rc = entry.WLock();
    if (rc.IsError()) {
        LOG(INFO) << FormatString("The object %s may not exists, will not set writeBackDone to true.", objectKey);
        return rc;
    }
    Raii writeUnlock([&entry]() { entry.WUnlock(); });
    if (createTime != entry->GetCreateTime()) {
        return Status::OK();
    }
    entry->stateInfo.SetWriteBackDone(true);

    // Try delete the spilled object from disk.
    if (WorkerOcSpill::Instance()->IsEnabled()) {
        if (WorkerOcSpill::Instance()->IsSpaceExceedLWM() && entry->IsSpilled()) {
            auto spilledSize = WorkerOcSpill::Instance()->GetSpilledSize();
            LOG(INFO) << "try delete spilled object " << objectKey << ", spilled size:" << spilledSize;
            rc = WorkerOcSpill::Instance()->Delete(objectKey);
            if (rc.IsError() && rc.GetCode() != K_NOT_FOUND) {
                LOG(ERROR) << "Delete spilled object failed with status:" << rc.ToString();
                return Status::OK();
            }
            ObjectKV objectKV(objectKey, entry);
            LOG_IF_ERROR(evictionManager_->EvictClearObject(objectKV), "EvictClearObject failed!");
        }
    }
    return Status::OK();
}

void AsyncSendManager::TryEvict(const std::string &objectKey, uint64_t needSize)
{
    auto memOccupied =
        static_cast<double>(datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage() + needSize);
    auto memThreshold =
        static_cast<double>(datasystem::memory::Allocator::Instance()->GetMaxMemorySize()) * HIGH_WATER_FACTOR;
    LOG(INFO) << FormatString("Allocate memory for %s, size = %ld, memOccupied = %ld, memThreshold = %ld", objectKey,
                              needSize, memOccupied, memThreshold);
    if (memOccupied >= memThreshold) {
        evictionManager_->Evict();
    }
}

Status AsyncSendManager::AllocateMemory(bool populate, ObjectKV &objectKV, datasystem::ShmUnit &shmUnit)
{
    const std::string &objectKey = objectKV.GetObjKey();
    const uint64_t dataSize = objectKV.GetObjEntry()->GetDataSize();
    uint64_t metadataSize = objectKV.GetObjEntry()->GetMetadataSize();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(UINT64_MAX - metadataSize >= dataSize, K_RUNTIME_ERROR,
                                         FormatString("The size is overflow, size:%llu + add:%llu > UINT64_MAX:%llu",
                                                      dataSize, metadataSize, UINT64_MAX));
    uint64_t needSize = dataSize + metadataSize;
    TryEvict(objectKey, needSize);
    static const std::vector<int> WAIT_MSECOND = { 1, 10, 50, 100, 200, 400, 800, 1600, 3200 };
    auto tenantId = TenantAuthManager::ExtractTenantId(objectKey);
    Status rc = shmUnit.AllocateMemory(tenantId, needSize, populate);
    if (rc.GetCode() == K_OUT_OF_MEMORY) {
        for (int t : WAIT_MSECOND) {
            auto sleepTime = t;
            VLOG(1) << FormatString("OOM, sleep time: %ld, objectKey: %s, needSize %ld", sleepTime, objectKey,
                                    needSize);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
            rc = shmUnit.AllocateMemory(tenantId, needSize, populate);
            if (rc.GetCode() != K_OUT_OF_MEMORY) {
                break;
            }
            TryEvict(objectKey, needSize);
        }
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, FormatString("[ObjectKey %s] Error while allocating memory.", objectKey));

    if (metadataSize > 0) {
        auto ret = memset_s(shmUnit.GetPointer(), metadataSize, 0, metadataSize);
        if (ret != EOK) {
            if (!populate) {
                shmUnit.SetHardFreeMemory();
            }
            LOG_IF_ERROR(shmUnit.FreeMemory(), "shmUnit FreeMemory failed");
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                    FormatString("[ObjectKey %s] Memset failed, errno: %d", objectKey, ret));
        }
    }
    return Status::OK();
}

Status AsyncSendManager::Add(const std::string &objectKey, std::shared_ptr<SafeObjType> entry,
                             std::future<Status> &future)
{
    size_t index = ObjectKey2QueueIndex(objectKey);
    VLOG(1) << FormatString("AsyncSendManager add key %s to list %zu", objectKey, index);
    std::promise<Status> promise;
    future = promise.get_future();
    auto element = std::make_shared<Element>();
    element->key = objectKey;
    element->entry = std::move(entry);
    element->promise = std::move(promise);
    element->beginAsync = std::chrono::steady_clock::now();
    element->traceID = Trace::Instance().GetTraceID();
    if (queues_[index]->Remove(objectKey).IsOk()) {
        LOG(INFO) << FormatString("Replace object %s in the l2cache queue.", objectKey);
    }
    RETURN_IF_NOT_OK(queues_[index]->EnsureOffer(element));
    RequestCounter::GetInstance().ResetLastArrivalTime("AsyncSendManager::Add");
    return Status::OK();
}

void AsyncSendManager::Remove(const std::string &objectKey)
{
    size_t index = ObjectKey2QueueIndex(objectKey);
    VLOG(1) << FormatString("AsyncSendManager remove key %s from list %zu", objectKey, index);
    (void)queues_[index]->Remove(objectKey);
}

size_t AsyncSendManager::ObjectKey2QueueIndex(const std::string &objectKey) const
{
    std::hash<std::string> hash;
    return hash(objectKey) % QUEUE_NUM;
}

bool AsyncSendManager::IsAsyncTasksQueueEmpty() const
{
    if (sendingCount_ > 0) {
        return false;
    }
    for (const auto &list : queues_) {
        if (list->Size() != 0) {
            return false;
        }
    }
    return true;
}

std::string AsyncSendManager::GetL2CacheAsyncTasksQueueUsage()
{
    uint64_t totalLimit = 0;
    uint64_t currentSize = 0;
    for (const auto &list : queues_) {
        totalLimit += list->Capacity();
        currentSize += list->Size();
    }
    if (totalLimit == 0) {
        return "0/0/0";
    }
    auto workerL2CacheQueueUsage = currentSize / static_cast<float>(totalLimit);
    return FormatString("%llu/%llu/%.3f", currentSize, totalLimit, workerL2CacheQueueUsage);
}

bool AsyncSendManager::CheckHealth() const
{
    INJECT_POINT("AsyncSendManager.CheckHealth", []() { return false; });
    uint64_t successTimoutSeconds = 120;
    INJECT_POINT("AsyncSendManager.CheckHealth.modify_timout", [&successTimoutSeconds]() {
        successTimoutSeconds = 1; // reduce to 1s for test.
        return true;
    });

    uint64_t asyncElapse = 0;
    {
        std::shared_lock<std::shared_timed_mutex> l(timestampMutex_);
        asyncElapse = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - lastSunccessTimestamp_)
                .count());
    }
    return IsAsyncTasksQueueEmpty() || asyncElapse <= successTimoutSeconds;
}

std::vector<std::string> AsyncSendManager::GetAllUnfinishedObjects()
{
    std::vector<std::string> ret;
    for (const auto &list : queues_) {
        list->GetAllKeys(ret);
    }
    {
        std::shared_lock<std::shared_timed_mutex> l(failedObjectsMutex_);
        for (const auto &objectKey : failedObjects_) {
            ret.emplace_back(objectKey);
        }
    }
    return ret;
}

void AsyncSendManager::UpdateLastSuccessTimestamp()
{
    std::lock_guard<std::shared_timed_mutex> l(timestampMutex_);
    lastSunccessTimestamp_ = std::chrono::steady_clock::now();
}

void AsyncSendManager::AddToFailedObjects(const std::string &objectKey)
{
    std::lock_guard<std::shared_timed_mutex> l(failedObjectsMutex_);
    (void)failedObjects_.emplace(objectKey);
}
}  // namespace object_cache
}  // namespace datasystem
