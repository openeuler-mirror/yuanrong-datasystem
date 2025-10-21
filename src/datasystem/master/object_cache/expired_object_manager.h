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

#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_EXPIRED_OBJECT_MANAGER_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_EXPIRED_OBJECT_MANAGER_H

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
namespace master {
class OCMetadataManager;
class ExpiredStatisticsInfo {
public:
    /**
     * @brief The ExpiredStatisticsInfo is used to record statistics info of expired objects.
     */
    ExpiredStatisticsInfo() = default;
    ~ExpiredStatisticsInfo() = default;

    /**
     * @brief Used to record the total expired object num.
     */
    void IncreaseObj();

    /**
     * @brief Used to record how many seconds the object is deleted later than expected.
     * @param[in] delaySecond  Number of seconds later than expected to delete the object.
     */
    void IncreaseDelayDeleteObj(uint64_t delaySecond);

    /**
     * @brief Used to record how many seconds the object is start to delete later than expected.
     * @param[in] delaySecond  Number of seconds later than expected to start delete the object.
     */
    void IncreaseDelayGetObj(uint64_t delaySecond);

    /**
     * @brief Used to record the failed delete object num.
     * @param[in] failedNum The failed object num
     */
    void IncreaseFailedDelObj(uint64_t failedNum);

    /**
     * @brief Get the statistics info.
     */
    std::string ToString() const;

private:
    std::atomic<uint64_t> totalNum_{ 0 };
    std::atomic<uint64_t> delNum_{ 0 };
    std::atomic<uint64_t> failedNum_{ 0 };
    mutable std::shared_timed_mutex mutex_;  // protect the following map.
    tbb::concurrent_hash_map<uint64_t, uint64_t> delayMap_;
    tbb::concurrent_hash_map<uint64_t, uint64_t> delayGetMap_;
};

class ExpiredObjectManager {
public:
    /**
     * @brief The ExpiredObjectManager is used to delete expired object.
     * @param[in] masterAddress The master ip address.
     */
    explicit ExpiredObjectManager(const std::string &masterAddress, OCMetadataManager *ocMetadataManager)
        : masterAddress_(masterAddress), ocMetadataManager_(ocMetadataManager)
    {
    }

    ~ExpiredObjectManager();

    /**
     * @brief Do some init work for ExpiredObjectManager.
     */
    void Init();

    /**
     * @brief Shutdown the ExpiredObjectManager.
     */
    void Shutdown();

    /**
     * @brief When master restart, need reload expired object
     * @param[in] objects The expired object info
     */
    void ReloadExpireObjects(const std::vector<std::tuple<std::string, uint64_t, uint32_t>> &objects);

    /**
     * @brief Insert object that will be expired after ttl seconds. Don't insert object if ttlSecond is 0.
     * @param[in] objectKey The object key.
     * @param[in] version The version of the object.
     * @param[in] ttlSecond The expired ttl second.
     * @param[in] accpetZero Whether insert object if ttlSecond is 0.
     */
    Status InsertObject(const std::string &objectKey, uint64_t version, uint32_t ttlSecond, bool acceptZero = false);

    /**
     * @brief Remove object that will be expired.
     * @param[in] objectKey The object key.
     */
    void RemoveObjectIfExistUnlock(const std::string &objectKey);

    /**
     * @brief Locking and remove object that will be expired.
     * @param[in] objectKey The object key.
     */
    Status RemoveObjectIfExist(const std::string &objectKey);

    /**
     * @brief After succeed delete the expired objects, will use this function do some clean and statistics work.
     * @param[in] objectKeys The expired object keys.
     */
    void AddSucceedObject(const std::unordered_map<std::string, uint64_t> &objectKeys);

    /**
     * @brief If master failed to delete the object, update the ttl second and retry again.
     * @param[in] objectKeys The expired object keys.
     */
    void AddFailedObject(const std::set<std::string> &objectKeys);

    /**
     * @brief Get the expired objects.
     * @return Return the expired objects.
     */
    std::unordered_map<std::string, uint64_t> GetExpiredObject();

    /**
     * @brief Scan the expired objects and submit the delete request to thread pool.
     */
    void Run();

    /**
     * @brief Delete the expired objects.
     * @param[in] expiredObjMap The expired object map.
     */
    Status AsyncDelete(std::unordered_map<std::string, uint64_t> expiredObjMap);

    /**
     * @brief check object is deleting
     * @param[in] objectKey object key
     * @return true object is deleting, false object is not deleting
     */
    bool CheckObjectInAsyncDeleteWithLock(const std::string &objectKey);

    /**
     * @brief Get the remaining time of the object and deletes it.
     * @param[in] objectKey The expired object key.
     * @param[out] remainTimeSecond Remaining time of the object. The unit is second.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetObjectRemainTimeAndRemove(const std::string &objectKey, uint32_t &remainTimeSecond);

private:
    uint64_t CalcExpireTime(uint64_t version, uint64_t ttlSecond) const;
    Status InsertObjectUnlock(const std::string &objectKey, uint64_t version, uint32_t ttlSecond);

    /**
     * @brief Delete the expired objects.
     * @param[in] objectKey The object key.
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckObjectInAsyncDelete(const std::string &objectKey, StatusCode code);

    const std::string masterAddress_;
    std::atomic<bool> interruptFlag_{ false };
    WaitPost cvLock_;

    ExpiredStatisticsInfo statisticsInfo_;
    const uint64_t MAX_DEL_BATCH_NUM = 3000;
    const uint32_t MAX_DEL_THREAD_NUM = 5;
    const uint32_t TIME_UNIT_CONVERSION = 1000;
    const uint32_t SCAN_INTERVAL_MS = 100;

    mutable std::mutex mutex_;  // protect the following variables.
    std::multimap<uint64_t, ImmutableString> timedObj_;
    std::unordered_map<ImmutableString, std::multimap<uint64_t, ImmutableString>::iterator> obj2Timed_;
    std::unordered_map<ImmutableString, uint64_t> failedObjects_;
    std::unique_ptr<ThreadPool> threadPool_;
    // Store the object key that have been erased from timedOjb_ and waiting for delete.
    std::set<ImmutableString> readyExpiredObjects_;
    OCMetadataManager *ocMetadataManager_;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_EXPIRED_OBJECT_MANAGER_H
