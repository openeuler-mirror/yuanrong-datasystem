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
 * Description: Manager the expired state cache object.
 */

#include "expired_object_manager.h"

#include <cstdint>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/log/log.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"

DS_DECLARE_uint32(node_timeout_s);

namespace datasystem {
namespace master {
static constexpr int THREAD_NUM = 1;
static constexpr int RETRY_WAIT_TIME = 1;
static constexpr int NOTIFY_DELETE_TIMEOUT = 10 * 1000;
void ExpiredStatisticsInfo::IncreaseObj()
{
    (void)totalNum_.fetch_add(1);
}

void ExpiredStatisticsInfo::IncreaseDelayDeleteObj(const uint64_t delaySecond)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor accessor;
    if (delayMap_.insert(accessor, delaySecond)) {
        accessor->second = 1;
    } else {
        accessor->second += 1;
    }
    (void)delNum_.fetch_add(1);
}

void ExpiredStatisticsInfo::IncreaseDelayGetObj(const uint64_t delaySecond)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    tbb::concurrent_hash_map<uint64_t, uint64_t>::accessor accessor;
    if (delayGetMap_.insert(accessor, delaySecond)) {
        accessor->second = 1;
    } else {
        accessor->second += 1;
    }
}

void ExpiredStatisticsInfo::IncreaseFailedDelObj(const uint64_t num)
{
    (void)failedNum_.fetch_add(num);
}

std::string ExpiredStatisticsInfo::ToString() const
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto getMapStr = [](const tbb::concurrent_hash_map<uint64_t, uint64_t> &map) {
        std::stringstream ss;
        ss << "[ ";
        uint64_t count = 0;
        for (const auto &iter : map) {
            ss << " ( " << iter.first << " , " << iter.second << " ) ";
            count++;
            if (count % 10 == 0) {
                ss << std::endl;
            }
        }
        ss << " ]";
        return ss.str();
    };
    return FormatString(
        "Total object num: %llu, succeed delete num: %llu, failed delete num: %llu, delay delete info:\n %s \n, delay "
        "get info:\n %s \n",
        totalNum_.load(), delNum_.load(), failedNum_.load(), getMapStr(delayMap_), getMapStr(delayGetMap_));
}

ExpiredObjectManager::~ExpiredObjectManager()
{
    Shutdown();
}

void ExpiredObjectManager::Init()
{
    threadPool_ = std::make_unique<ThreadPool>(THREAD_NUM, 0, "ExpiredObject");
    threadPool_->Execute(&ExpiredObjectManager::Run, this);
}

void ExpiredObjectManager::Shutdown()
{
    if (!interruptFlag_) {
        interruptFlag_ = true;
        cvLock_.Set();
        threadPool_.reset();
        LOG(INFO) << "Shutdown ExpiredObjectManager and the statistics info: " << statisticsInfo_.ToString();
    }
}

uint64_t ExpiredObjectManager::CalcExpireTime(const uint64_t version, const uint64_t ttlSecond) const
{
    if (UINT64_MAX / (ttlSecond * TIME_UNIT_CONVERSION * TIME_UNIT_CONVERSION) < 1) {
        return UINT64_MAX;
    }
    uint64_t ttlUs = ttlSecond * TIME_UNIT_CONVERSION * TIME_UNIT_CONVERSION;
    return (UINT64_MAX - ttlUs < version) ? UINT64_MAX : (version + ttlUs);
}

void ExpiredObjectManager::ReloadExpireObjects(const std::vector<std::tuple<std::string, uint64_t, uint32_t>> &objects)
{
    if (objects.empty()) {
        return;
    }
    LOG(INFO) << FormatString("There had %zu expire objects will reload to ExpiredObjectManager", objects.size());
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &iter : objects) {
        (void)InsertObjectUnlock(std::get<0>(iter), std::get<1>(iter), std::get<2>(iter));
    }
}

Status ExpiredObjectManager::InsertObject(const std::string &objectKey, const uint64_t version,
                                          const uint32_t ttlSecond, bool acceptZero)
{
    // If objectKey in timeObj_ and we insert the same objectKey again, it means the object is being updated.
    // If ttl is not zero, we should remove the object key and old expire time, and insert with new expire time again.
    if (!acceptZero && ttlSecond == 0) {
        return Status::OK();
    }
    Timer timer;
    std::lock_guard<std::mutex> lock(mutex_);
    masterOperationTimeCost.Append("InsertObject", timer.ElapsedMilliSecond());
    // If objectKey in readyExpiredObjects_, it means object is being deleted, can't update meta otherwise the new
    // updated data will be deleted soon.
    RETURN_IF_NOT_OK(CheckObjectInAsyncDelete(objectKey, K_RUNTIME_ERROR));
    RemoveObjectIfExistUnlock(objectKey);
    return InsertObjectUnlock(objectKey, version, ttlSecond);
}

Status ExpiredObjectManager::InsertObjectUnlock(const std::string &objectKey, const uint64_t version,
                                                const uint32_t ttlSecond)
{
    uint64_t expiredTime = CalcExpireTime(version, ttlSecond);
    auto iter = timedObj_.insert({ expiredTime, objectKey });
    obj2Timed_[objectKey] = iter;
    VLOG(1) << FormatString("Insert the object %s with version %llu, ttl second %u, expireTime %llu, remain time %llu",
                            objectKey, version, ttlSecond, expiredTime, expiredTime - GetSystemClockTimeStampUs());
    statisticsInfo_.IncreaseObj();
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE)).Inc();
    return Status::OK();
}

void ExpiredObjectManager::RemoveObjectIfExistUnlock(const std::string &objectKey)
{
    if (obj2Timed_.find(objectKey) != obj2Timed_.end()) {
        VLOG(1) << "Remove object: " << objectKey << "from ttl queue.";
        (void)timedObj_.erase(obj2Timed_[objectKey]);
        (void)obj2Timed_.erase(objectKey);
        metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE)).Dec();
    }
    // MASTER_TTL_PENDING_SIZE tracks |timedObj_| only (InsertObjectUnlock / GetExpiredObject / AddFailedObject).
    // When the key is no longer in timedObj_, the gauge was already decremented on dequeue; erasing failedObjects_
    // here only clears per-key retry bookkeeping, so do not Dec the gauge again (would double-count).
    if (failedObjects_.count(objectKey)) {
        (void)failedObjects_.erase(objectKey);
    }
}

Status ExpiredObjectManager::RemoveObjectIfExist(const std::string &objectKey)
{
    Timer timer;
    std::lock_guard<std::mutex> lock(mutex_);
    masterOperationTimeCost.Append("RemoveObjectIfExist", timer.ElapsedMilliSecond());
    // If objectKey in readyExpiredObjects_, it means object is being deleted, can't update meta otherwise the new
    // updated data will be deleted soon.
    RETURN_IF_NOT_OK(CheckObjectInAsyncDelete(objectKey, K_TRY_AGAIN));
    RemoveObjectIfExistUnlock(objectKey);
    return Status::OK();
}

void ExpiredObjectManager::AddSucceedObject(const std::unordered_map<std::string, uint64_t> &objectKeys)
{
    if (objectKeys.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &item : objectKeys) {
        auto &objectKey = item.first;
        auto &expiredTime = item.second;
        uint64_t delayTimeSecond =
            (GetSystemClockTimeStampUs() - expiredTime) / TIME_UNIT_CONVERSION / TIME_UNIT_CONVERSION;
        statisticsInfo_.IncreaseDelayDeleteObj(delayTimeSecond);
        if (failedObjects_.count(objectKey)) {
            VLOG(1) << FormatString("The expired object: %s had been deleted after %llu times retry.", objectKey,
                                    failedObjects_[objectKey]);
            (void)failedObjects_.erase(objectKey);
        }
        VLOG(1) << FormatString("The object %s had been deleted, expireTime %llu, delay delete time %llu second",
                                objectKey, expiredTime, delayTimeSecond);
    }
}

void ExpiredObjectManager::AddFailedObject(const std::set<std::string> &objectKeys)
{
    if (objectKeys.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &objectKey : objectKeys) {
        failedObjects_[objectKey]++;
        uint64_t newTtlSecond = (UINT64_MAX - 1) / failedObjects_[objectKey] < RETRY_WAIT_TIME
                                    ? UINT64_MAX
                                    : static_cast<uint64_t>(RETRY_WAIT_TIME) * failedObjects_[objectKey] + 1;
        uint64_t expiredTime = CalcExpireTime(GetSystemClockTimeStampUs(), newTtlSecond);
        auto iter = timedObj_.insert({ expiredTime, objectKey });
        obj2Timed_[objectKey] = iter;
        LOG(INFO) << FormatString(
            "The expired object: %s had been deleted failed with %llu times, will retry again after %u seconds later.",
            objectKey, failedObjects_[objectKey], newTtlSecond);
    }
    statisticsInfo_.IncreaseFailedDelObj(objectKeys.size());
    METRIC_ADD(metrics::KvMetricId::MASTER_TTL_RETRY_TOTAL, objectKeys.size());
    // One Inc per key: failed deletes are re-queued into timedObj_ above (same +1 as InsertObjectUnlock).
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE))
        .Inc(static_cast<int64_t>(objectKeys.size()));
}

std::unordered_map<std::string, uint64_t> ExpiredObjectManager::GetExpiredObject()
{
    std::unordered_map<std::string, uint64_t> expiredObject;
    uint64_t currentTime = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto iter = timedObj_.begin();
         iter != timedObj_.end() && iter->first <= currentTime && expiredObject.size() < MAX_DEL_BATCH_NUM;) {
        VLOG(1) << FormatString("Object %s, expire time: %llu, current time: %llu", iter->second, iter->first,
                                currentTime);
        expiredObject[iter->second] = iter->first;
        uint64_t delayTimeSecond =
            (GetSystemClockTimeStampUs() - iter->first) / TIME_UNIT_CONVERSION / TIME_UNIT_CONVERSION;
        statisticsInfo_.IncreaseDelayGetObj(delayTimeSecond);
        (void)readyExpiredObjects_.emplace(iter->second);
        (void)obj2Timed_.erase(iter->second);
        (void)timedObj_.erase(iter++);
    }
    if (!expiredObject.empty()) {
        METRIC_ADD(metrics::KvMetricId::MASTER_TTL_FIRE_TOTAL, expiredObject.size());
        metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE))
            .Dec(static_cast<int64_t>(expiredObject.size()));
    }
    return expiredObject;
}

Status ExpiredObjectManager::AsyncDelete(std::unordered_map<std::string, uint64_t> expiredObjMap)
{
    INJECT_POINT("master.ExpiredObjectManager.AsyncDelete", [](int sleepTime) {
        std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
        return Status::OK();
    });
    auto startTimeUs = GetSystemClockTimeStampUs();
    LOG(INFO) << "Expire objects size is: " << expiredObjMap.size();
    std::unordered_map<std::string, bool> requestObjectKeyMap;
    std::transform(expiredObjMap.begin(), expiredObjMap.end(),
                   std::inserter(requestObjectKeyMap, requestObjectKeyMap.end()),
                   [](const auto &kv) { return std::make_pair(kv.first, true); });

    // The address in request should be worker address and should not be empty. However, we are sending this request
    // from master. If we fill in with master address, the metadata manager will skip this address when deleting some
    // copies, even if the worker in this address has a copy (as we know the worker and master are merged). To cheat
    // the sanity check in metadata manager with an invalid address, let's use loopback IP without port number.
    timeoutDuration.Init(NOTIFY_DELETE_TIMEOUT);
    DeleteObjectMediator mediator("127.0.0.1", requestObjectKeyMap);
    mediator.SetObjKey2Version(std::move(expiredObjMap));
    ocMetadataManager_->FindNeedDeleteIds(mediator);

    std::unordered_set<std::string> hashObjsWithoutMeta = mediator.GetHashObjsWithoutMeta();
    ocMetadataManager_->ForwardDeleteAllCopyMeta2OtherAz(std::move(hashObjsWithoutMeta), mediator);

    ocMetadataManager_->NotifyDeleteAndClearMeta(mediator, true);
    if (mediator.GetStatus().IsError()) {
        LOG(ERROR) << FormatString("ExpiredObjectManager failed with status:%s", mediator.GetStatus().ToString());
    }
    std::unordered_map<std::string, uint64_t> succeedIds;
    std::set<std::string> failedIds = { mediator.GetFailedObjs().begin(), mediator.GetFailedObjs().end() };
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto &kv : requestObjectKeyMap) {
            if (failedIds.count(kv.first) == 0) {
                succeedIds.emplace(kv.first, expiredObjMap[kv.first]);
            }
            (void)readyExpiredObjects_.erase(kv.first);
        }
    }
    AddSucceedObject(succeedIds);
    AddFailedObject(failedIds);
    if (!succeedIds.empty()) {
        METRIC_ADD(metrics::KvMetricId::MASTER_TTL_DELETE_SUCCESS_TOTAL, succeedIds.size());
    }
    if (!failedIds.empty()) {
        METRIC_ADD(metrics::KvMetricId::MASTER_TTL_DELETE_FAILED_TOTAL, failedIds.size());
    }
    LOG(INFO) << FormatString("It cost %llu ms to delete expire object, succeed num:%lzu, failed num:%zu",
                              (GetSystemClockTimeStampUs() - startTimeUs) / TIME_UNIT_CONVERSION, succeedIds.size(),
                              failedIds.size());
    return Status::OK();
}

Status ExpiredObjectManager::GetObjectRemainTimeAndRemove(const std::string &objectKey, uint32_t &remainTimeSecond)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (obj2Timed_.find(objectKey) == obj2Timed_.end()) {
        RETURN_STATUS(StatusCode::K_INVALID, FormatString("The object[%s] not set ttl", objectKey));
    }
    uint64_t currentUs = GetSystemClockTimeStampUs();
    uint64_t remainUs = obj2Timed_[objectKey]->first > currentUs ? obj2Timed_[objectKey]->first - currentUs : 0;
    remainTimeSecond = remainUs / TIME_UNIT_CONVERSION / TIME_UNIT_CONVERSION;
    RemoveObjectIfExistUnlock(objectKey);
    return Status::OK();
}

void ExpiredObjectManager::Run()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    auto traceId = Trace::Instance().GetTraceID();
    int intervalMs = SCAN_INTERVAL_MS;
    constexpr int kMetaTableSizeTickMs = 1000;
    int sinceLastMetaTick = 0;
    while (!interruptFlag_) {
        auto expiredObjects = GetExpiredObject();
        INJECT_POINT("master.ExpiredObjectManager.Run", [this, &expiredObjects, &intervalMs] {
            for (const auto &kv : expiredObjects) {
                std::lock_guard<std::mutex> lock(mutex_);
                (void)readyExpiredObjects_.erase(kv.first);
            }
            expiredObjects.clear();
            intervalMs = 1;
        });
        if (!expiredObjects.empty()) {
            ocMetadataManager_->ExecuteAsyncTask([this, expiredObjectMap = std::move(expiredObjects), traceId] {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                LOG_IF_ERROR(AsyncDelete(expiredObjectMap), "Async delete expired object failed");
            });
        }
        sinceLastMetaTick += intervalMs;
        if (sinceLastMetaTick >= kMetaTableSizeTickMs && ocMetadataManager_ != nullptr) {
            metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_OBJECT_META_TABLE_SIZE))
                .Set(static_cast<int64_t>(ocMetadataManager_->GetMetaTableSize()));
            sinceLastMetaTick = 0;
        }
        cvLock_.WaitFor(intervalMs);
    }
}

Status ExpiredObjectManager::CheckObjectInAsyncDelete(const std::string &objectKey, StatusCode code)
{
    if (readyExpiredObjects_.count(objectKey) > 0) {
        RETURN_STATUS(code, FormatString("[ObjectKey]: %s is being deleted, please try again.", objectKey));
    }
    return Status::OK();
}

bool ExpiredObjectManager::CheckObjectInAsyncDeleteWithLock(const std::string &objectKey)
{
    std::lock_guard<std::mutex> lock(mutex_);
    return readyExpiredObjects_.count(objectKey) > 0;
}
}  // namespace master
}  // namespace datasystem
