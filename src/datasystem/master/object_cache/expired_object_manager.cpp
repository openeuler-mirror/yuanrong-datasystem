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
    for (const auto &[objectKey, version, ttlSecond] : objects) {
        auto &shard = shards_[GetShardIndex(objectKey)];
        std::lock_guard<std::mutex> lock(shard.mutex);
        (void)InsertObjectUnlock(shard, objectKey, version, ttlSecond);
    }
}

Status ExpiredObjectManager::InsertObject(const std::string &objectKey, const uint64_t version,
                                          const uint32_t ttlSecond, bool acceptZero)
{
    if (!acceptZero && ttlSecond == 0) {
        return Status::OK();
    }
    Timer timer;
    auto &shard = shards_[GetShardIndex(objectKey)];
    std::lock_guard<std::mutex> lock(shard.mutex);
    masterOperationTimeCost.Append("InsertObject", timer.ElapsedMilliSecond());
    RETURN_IF_NOT_OK(CheckObjectInAsyncDelete(shard, objectKey, K_RUNTIME_ERROR));
    RemoveObjectIfExistUnlock(shard, objectKey);
    return InsertObjectUnlock(shard, objectKey, version, ttlSecond);
}

Status ExpiredObjectManager::InsertObjectUnlock(ExpiredShard &shard, const std::string &objectKey,
                                                const uint64_t version, const uint32_t ttlSecond)
{
    uint64_t expiredTime = CalcExpireTime(version, ttlSecond);
    auto iter = shard.timedObj.insert({ expiredTime, objectKey });
    shard.obj2Timed[objectKey] = iter;
    VLOG(1) << FormatString("Insert the object %s with version %llu, ttl second %u, expireTime %llu, remain time %llu",
                            objectKey, version, ttlSecond, expiredTime, expiredTime - GetSystemClockTimeStampUs());
    statisticsInfo_.IncreaseObj();
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE)).Inc();
    return Status::OK();
}

void ExpiredObjectManager::RemoveObjectIfExistUnlock(ExpiredShard &shard, const std::string &objectKey)
{
    if (shard.obj2Timed.find(objectKey) != shard.obj2Timed.end()) {
        VLOG(1) << "Remove object: " << objectKey << "from ttl queue.";
        (void)shard.timedObj.erase(shard.obj2Timed[objectKey]);
        (void)shard.obj2Timed.erase(objectKey);
        metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE)).Dec();
    }
    if (shard.failedObjects.count(objectKey)) {
        (void)shard.failedObjects.erase(objectKey);
    }
}

Status ExpiredObjectManager::RemoveObjectIfExist(const std::string &objectKey)
{
    Timer timer;
    auto &shard = shards_[GetShardIndex(objectKey)];
    std::lock_guard<std::mutex> lock(shard.mutex);
    masterOperationTimeCost.Append("RemoveObjectIfExist", timer.ElapsedMilliSecond());
    RETURN_IF_NOT_OK(CheckObjectInAsyncDelete(shard, objectKey, K_TRY_AGAIN));
    RemoveObjectIfExistUnlock(shard, objectKey);
    return Status::OK();
}

void ExpiredObjectManager::AddSucceedObject(const std::unordered_map<std::string, uint64_t> &objectKeys)
{
    if (objectKeys.empty()) {
        return;
    }
    // Group keys by shard to minimize lock acquisitions.
    std::unordered_map<size_t, std::vector<const std::pair<const std::string, uint64_t>*>> byShard;
    for (const auto &item : objectKeys) {
        byShard[GetShardIndex(item.first)].push_back(&item);
    }
    for (auto &[shardIdx, items] : byShard) {
        auto &shard = shards_[shardIdx];
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (const auto *item : items) {
            auto &objectKey = item->first;
            auto &expiredTime = item->second;
            uint64_t delayTimeSecond =
                (GetSystemClockTimeStampUs() - expiredTime) / TIME_UNIT_CONVERSION / TIME_UNIT_CONVERSION;
            statisticsInfo_.IncreaseDelayDeleteObj(delayTimeSecond);
            if (shard.failedObjects.count(objectKey)) {
                VLOG(1) << FormatString("The expired object: %s had been deleted after %llu times retry.", objectKey,
                                        shard.failedObjects[objectKey]);
                (void)shard.failedObjects.erase(objectKey);
            }
            VLOG(1) << FormatString("The object %s had been deleted, expireTime %llu, delay delete time %llu second",
                                    objectKey, expiredTime, delayTimeSecond);
        }
    }
}

void ExpiredObjectManager::AddFailedObject(const std::set<std::string> &objectKeys)
{
    if (objectKeys.empty()) {
        return;
    }
    std::unordered_map<size_t, std::vector<std::string>> byShard;
    for (const auto &objectKey : objectKeys) {
        byShard[GetShardIndex(objectKey)].push_back(objectKey);
    }
    for (auto &[shardIdx, keys] : byShard) {
        auto &shard = shards_[shardIdx];
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (const auto &objectKey : keys) {
            shard.failedObjects[objectKey]++;
            uint64_t newTtlSecond = (UINT64_MAX - 1) / shard.failedObjects[objectKey] < RETRY_WAIT_TIME
                                        ? UINT64_MAX
                                        : static_cast<uint64_t>(RETRY_WAIT_TIME) * shard.failedObjects[objectKey] + 1;
            uint64_t expiredTime = CalcExpireTime(GetSystemClockTimeStampUs(), newTtlSecond);
            auto iter = shard.timedObj.insert({ expiredTime, objectKey });
            shard.obj2Timed[objectKey] = iter;
            LOG(INFO) << FormatString(
                "The expired object: %s had been deleted failed with %llu times, "
                "will retry again after %u seconds later.",
                objectKey, shard.failedObjects[objectKey], newTtlSecond);
        }
    }
    statisticsInfo_.IncreaseFailedDelObj(objectKeys.size());
    METRIC_ADD(metrics::KvMetricId::MASTER_TTL_RETRY_TOTAL, objectKeys.size());
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE))
        .Inc(static_cast<int64_t>(objectKeys.size()));
}

std::unordered_map<std::string, uint64_t> ExpiredObjectManager::GetExpiredObject()
{
    std::unordered_map<std::string, uint64_t> expiredObject;
    uint64_t currentTime = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    static constexpr size_t kChunkSize = 64;

    while (expiredObject.size() < MAX_DEL_BATCH_NUM) {
        // k-way merge: find the shard whose head has the smallest expiration time.
        size_t bestShard = kExpiredShardCount;
        uint64_t bestTime = UINT64_MAX;
        for (size_t i = 0; i < kExpiredShardCount; ++i) {
            std::lock_guard<std::mutex> lock(shards_[i].mutex);
            if (shards_[i].timedObj.empty()) {
                continue;
            }
            uint64_t headTime = shards_[i].timedObj.begin()->first;
            if (headTime <= currentTime && headTime < bestTime) {
                bestTime = headTime;
                bestShard = i;
            }
        }
        if (bestShard == kExpiredShardCount) {
            break;
        }

        // Pop a bounded chunk from the winning shard, then re-evaluate.
        auto &shard = shards_[bestShard];
        std::lock_guard<std::mutex> lock(shard.mutex);
        size_t remaining = MAX_DEL_BATCH_NUM - expiredObject.size();
        size_t popped = 0;
        for (auto iter = shard.timedObj.begin();
             iter != shard.timedObj.end() && iter->first <= currentTime && popped < remaining && popped < kChunkSize;) {
            VLOG(1) << FormatString("Object %s, expire time: %llu, current time: %llu", iter->second, iter->first,
                                    currentTime);
            expiredObject[iter->second] = iter->first;
            uint64_t delayTimeSecond =
                (GetSystemClockTimeStampUs() - iter->first) / TIME_UNIT_CONVERSION / TIME_UNIT_CONVERSION;
            statisticsInfo_.IncreaseDelayGetObj(delayTimeSecond);
            (void)shard.readyExpiredObjects.emplace(iter->second);
            (void)shard.obj2Timed.erase(iter->second);
            (void)shard.timedObj.erase(iter++);
            ++popped;
        }
    }

    if (!expiredObject.empty()) {
        METRIC_ADD(metrics::KvMetricId::MASTER_TTL_FIRE_TOTAL, expiredObject.size());
        metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE))
            .Dec(static_cast<int64_t>(expiredObject.size()));
    }
    return expiredObject;
}

std::unordered_map<std::string, uint64_t> ExpiredObjectManager::CleanupShardsAfterDelete(
    const std::unordered_map<std::string, bool>& requestObjectKeyMap,
    const std::set<std::string>& failedIds,
    const std::unordered_map<std::string, uint64_t>& expiredObjMap)
{
    std::unordered_map<std::string, uint64_t> succeedIds;
    // Group by shard to minimize lock acquisitions.
    std::unordered_map<size_t, std::vector<std::string>> byShard;
    for (const auto &kv : requestObjectKeyMap) {
        byShard[GetShardIndex(kv.first)].push_back(kv.first);
    }
    for (auto &[shardIdx, keys] : byShard) {
        auto &shard = shards_[shardIdx];
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (const auto &key : keys) {
            if (failedIds.count(key) == 0) {
                auto it = expiredObjMap.find(key);
                if (it != expiredObjMap.end()) {
                    succeedIds.emplace(key, it->second);
                }
            }
            (void)shard.readyExpiredObjects.erase(key);
        }
    }
    return succeedIds;
}

Status ExpiredObjectManager::AsyncDelete(std::unordered_map<std::string, uint64_t> expiredObjMap)
{
    INJECT_POINT("master.ExpiredObjectManager.AsyncDelete", [](int sleepTime) {
        std::this_thread::sleep_for(std::chrono::seconds(sleepTime));
        return Status::OK();
    });
    Timer timer;
    VLOG(1) << "Expire objects size is: " << expiredObjMap.size();
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

    ocMetadataManager_->NotifyDeleteAndClearMeta(mediator, true);
    if (mediator.GetStatus().IsError()) {
        LOG(ERROR) << FormatString("ExpiredObjectManager failed with status:%s", mediator.GetStatus().ToString());
    }
    std::set<std::string> failedIds = { mediator.GetFailedObjs().begin(), mediator.GetFailedObjs().end() };
    auto succeedIds = CleanupShardsAfterDelete(requestObjectKeyMap, failedIds, expiredObjMap);
    AddSucceedObject(succeedIds);
    AddFailedObject(failedIds);
    if (!succeedIds.empty()) {
        METRIC_ADD(metrics::KvMetricId::MASTER_TTL_DELETE_SUCCESS_TOTAL, succeedIds.size());
    }
    if (!failedIds.empty()) {
        METRIC_ADD(metrics::KvMetricId::MASTER_TTL_DELETE_FAILED_TOTAL, failedIds.size());
    }
    auto elapsedMs = timer.ElapsedMilliSecond();
    const int logLimitMs = 5;
    auto vlogLevel = elapsedMs > logLimitMs ? 0 : 1;
    VLOG(vlogLevel) << FormatString("It cost %.3fms to delete expire object, succeed num:%zu, failed num:%zu",
                                    elapsedMs, succeedIds.size(), failedIds.size());
    return Status::OK();
}

Status ExpiredObjectManager::GetObjectRemainTimeAndRemove(const std::string &objectKey, uint32_t &remainTimeSecond)
{
    auto &shard = shards_[GetShardIndex(objectKey)];
    std::lock_guard<std::mutex> lock(shard.mutex);
    if (shard.obj2Timed.find(objectKey) == shard.obj2Timed.end()) {
        RETURN_STATUS(StatusCode::K_INVALID, FormatString("The object[%s] not set ttl", objectKey));
    }
    uint64_t currentUs = GetSystemClockTimeStampUs();
    uint64_t expireTime = shard.obj2Timed[objectKey]->first;
    uint64_t remainUs = expireTime > currentUs ? expireTime - currentUs : 0;
    remainTimeSecond = remainUs / TIME_UNIT_CONVERSION / TIME_UNIT_CONVERSION;
    RemoveObjectIfExistUnlock(shard, objectKey);
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
                auto &shard = shards_[GetShardIndex(kv.first)];
                std::lock_guard<std::mutex> lock(shard.mutex);
                (void)shard.readyExpiredObjects.erase(kv.first);
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

Status ExpiredObjectManager::CheckObjectInAsyncDelete(const ExpiredShard &shard, const std::string &objectKey,
                                                      StatusCode code)
{
    if (shard.readyExpiredObjects.count(objectKey) > 0) {
        RETURN_STATUS(code, FormatString("[ObjectKey]: %s is being deleted, please try again.", objectKey));
    }
    return Status::OK();
}

bool ExpiredObjectManager::CheckObjectInAsyncDeleteWithLock(const std::string &objectKey)
{
    auto &shard = shards_[GetShardIndex(objectKey)];
    std::lock_guard<std::mutex> lock(shard.mutex);
    return shard.readyExpiredObjects.count(objectKey) > 0;
}
}  // namespace master
}  // namespace datasystem
