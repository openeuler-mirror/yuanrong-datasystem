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
 * Description: Defines AsyncSendManager Interface.
 */
#ifndef DATASYSTEM_ASYNC_SEND_MANAGER_H
#define DATASYSTEM_ASYNC_SEND_MANAGER_H

#include <future>
#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_set>
#include <utility>
#include <chrono>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/worker/object_cache/limiter/data_limiter.h"
#include "datasystem/worker/object_cache/limiter/data_limiter.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
// Forward declaration to avoid circular dependency
namespace datasystem {
namespace object_cache {
class WorkerOcEvictionManager;
}
}

namespace datasystem {
namespace object_cache {


const int QUEUE_NUM = 8;
const int QUEUE_CAPACITY = 10000;

struct Element {
    std::string key;
    std::shared_ptr<SafeObjType> entry;
    std::promise<Status> promise;
    std::chrono::time_point<std::chrono::steady_clock> beginAsync;
    std::string traceID;
};

class BlockingList {
public:
    explicit BlockingList(size_t capacity = 1000) : capacity_(capacity)
    {
    }

    ~BlockingList() = default;

    /**
     * @brief Push an element to the end of the list.
     * @param[in] element Element to be pushed.
     * @return Status K_OK or K_DUPLICATED (if the key is duplicated) or K_WRITE_BACK_QUEUE_FULL (if the queue is full).
     */
    Status Offer(std::shared_ptr<Element> element)
    {
        std::unique_lock<std::mutex> lock(mux_);
        if (!IsNotFull()) {
            RETURN_STATUS(StatusCode::K_WRITE_BACK_QUEUE_FULL, "The queue of async send is full");
        }
        if (indexTable_.count(element->key) != 0) {
            RETURN_STATUS(StatusCode::K_DUPLICATED, "Object " + element->key + " already exist in list");
        }
        auto newone = list_.insert(list_.end(), element);
        (void)indexTable_.emplace(element->key, newone);
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the list, if the list is full, remove the front element to ensure push.
     * @param[in] element Element to be pushed.
     * @return Status K_OK or K_DUPLICATED (if the key is duplicated).
     */
    Status EnsureOffer(std::shared_ptr<Element> element)
    {
        std::unique_lock<std::mutex> lock(mux_);
        if (indexTable_.count(element->key) != 0) {
            RETURN_STATUS(StatusCode::K_DUPLICATED, "Object " + element->key + " already exist in list");
        }
        if (!IsNotFull()) {
            auto removedElement = std::make_shared<Element>();
            lock.unlock();
            RETURN_IF_NOT_OK(Poll(removedElement, 0));
            lock.lock();
            LOG(WARNING) << FormatString(
                "The queue of 'Async op to cloud storage' is full, remove an operation for %s",
                removedElement->key);
        }
        auto newone = list_.insert(list_.end(), element);
        (void)indexTable_.emplace(element->key, newone);
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Get an element from the front of the list. Return error if timeout expires.
     * @param[out] out The pointer to take in popped element.
     * @param[in] timeoutMs in milliseconds.
     * @return Status K_OK or K_TRY_AGAIN (if timeout expires).
     */
    Status Front(std::shared_ptr<Element> &out, uint64_t timeoutMs)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvEmpty_.wait_for(lock, std::chrono::milliseconds(timeoutMs), IsNotEmpty);
        if (!IsNotEmpty()) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN,
                          "The list is empty within allowed time: " + std::to_string(timeoutMs) + " ms");
        }
        out = list_.front();
        return Status::OK();
    }

    /**
     * @brief Pop an element from the front of the list. Return error if timeout expires.
     * @param[in] timeoutMs in milliseconds.
     * @return Status K_OK or K_TRY_AGAIN (if timeout expires).
     */
    Status Pop(uint64_t timeoutMs)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvEmpty_.wait_for(lock, std::chrono::milliseconds(timeoutMs), IsNotEmpty);
        if (!IsNotEmpty()) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN,
                          "The list is empty within allowed time: " + std::to_string(timeoutMs) + " ms");
        }
        std::shared_ptr<Element> out = std::move(list_.front());
        list_.pop_front();
        (void)indexTable_.erase(out->key);
        return Status::OK();
    }

    /**
     * @brief Poll an element from the front of the list. Return error if timeout expires.
     * @param[out] out The pointer to take in popped element.
     * @param[in] timeoutMs in milliseconds.
     * @return Status K_OK or K_TRY_AGAIN (if timeout expires).
     */
    Status Poll(std::shared_ptr<Element> &out, uint64_t timeoutMs)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvEmpty_.wait_for(lock, std::chrono::milliseconds(timeoutMs), IsNotEmpty);
        if (!IsNotEmpty()) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN,
                          "The list is empty within allowed time: " + std::to_string(timeoutMs) + " ms");
        }
        out = list_.front();
        list_.pop_front();
        (void)indexTable_.erase(out->key);
        return Status::OK();
    }

    /**
     * @brief Erase a object from EvictionList.
     * @param[in] objectKey The ID of the object to erase.
     * @return Status of the call.
     */
    Status Remove(const std::string &objectKey)
    {
        std::unique_lock<std::mutex> lock(mux_);
        auto iter = indexTable_.find(objectKey);
        if (iter == indexTable_.end()) {
            RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object " + objectKey + " does not exist in list");
        }
        (void)list_.erase(iter->second);
        (void)indexTable_.erase(objectKey);
        return Status::OK();
    }

    /**
     * @brief Get the size of BlockingList.
     * @return The size of BlockingList.
     */
    size_t Size()
    {
        std::unique_lock<std::mutex> lock(mux_);
        return list_.size();
    }

    /**
     * @brief Get the capacity of BlockingList.
     * @return The capacity of BlockingList.
     */
    size_t Capacity()
    {
        std::unique_lock<std::mutex> lock(mux_);
        return capacity_;
    }

    /**
     * @brief Get all keys.
     * @param[out] keys keys in list.
     */
    void GetAllKeys(std::vector<std::string> &keys)
    {
        std::unique_lock<std::mutex> lock(mux_);
        for (const auto &entry : indexTable_) {
            keys.emplace_back(entry.first);
        }
    }

private:
    size_t capacity_;
    std::condition_variable cvEmpty_;
    std::mutex mux_;
    std::list<std::shared_ptr<Element>> list_;
    std::unordered_map<std::string, std::list<std::shared_ptr<Element>>::iterator> indexTable_;
    std::function<bool()> IsNotFull = [this]() -> bool { return list_.size() < capacity_; };
    std::function<bool()> IsNotEmpty = [this]() -> bool { return !list_.empty(); };
};

class AsyncSendManager {
public:
    /**
     * @brief Construct AsyncSendManager.
     */
    AsyncSendManager(std::shared_ptr<PersistenceApi> api, std::shared_ptr<WorkerOcEvictionManager> evictionManager);

    /**
     * @brief Deconstruct AsyncSendManager.
     */
    ~AsyncSendManager();

    /**
     * @brief Initialize the AsyncSendManager.
     * @return Status of the call.
     */
    Status Init();

    void Stop();

    /**
     * @brief Add a object to AsyncSendManager.
     * @param[in] objectKey The ID of the object need to send asynchronously.
     * @param[in] entry The object need to send asynchronously.
     * @param[out] future Async send future if write back to L2 cache, can't get during entry locked.
     * @return Status of the call.
     */
    Status Add(const std::string &objectKey, std::shared_ptr<SafeObjType> entry, std::future<Status> &future);

    /**
     * @brief Remove object from AsyncSendManager.
     * @param[in] objectKey The ID of the object need to remove.
     */
    void Remove(const std::string &objectKey);

    /**
     * @brief Check whether some async L2 cache tasks in the list.
     * @return True if all of async list is empty.
     */
    bool IsAsyncTasksQueueEmpty() const;

    /**
     * @brief Get the usage of the asynchronous task queue of L2Cache.
     * @note currentSize: the number of tasks in the current queue.
     *       totalLimit:  the maximum queue capacity
     * @return Usage: "currentSize/totalLimit/workerL2CacheQueueUsag"
     */
    std::string GetL2CacheAsyncTasksQueueUsage();

    /**
     * @brief Check async sender health.
     * @return True if health.
     */
    bool CheckHealth() const;

    /**
     * @brief Get all not send to l2 objects.
     * @return Unfininsed objects.
     */
    std::vector<std::string> GetAllUnfinishedObjects();

private:
    /**
     * @brief Sender thread responsible for sending data to L2 cache asynchronously.
     * @param[in] threadNum The thread ID.
     * @param[in] list List corresponding to sender, it save asynchronous task.
     * @return Status of the call.
     */
    Status Sender(int threadNum, const std::shared_ptr<BlockingList> &list);

    /**
     * @brief Lock object and send to L2 cache.
     * @param[in] objectKey The ID of the object need to send asynchronously.
     * @param[in] entryPtr The object need to send.
     * @param[in] beginTime The time this object get in to the async queue.
     * @return Status of the call.
     */
    Status LockAndSendToRemote(const std::string &objectKey, std::shared_ptr<SafeObjType> entryPtr,
                               std::chrono::time_point<std::chrono::steady_clock> &beginTime);

    /**
     * @brief RLock object and send to L2 cache, for the purpose read the obj data not blocking by other reader.
     * @param[in] objectKey The ID of the object need to send asynchronously.
     * @param[in] entryPtr The object need to send.
     * @param[out] objIsValidInMem whether the object is valid in memory.
     * @param[in] beginTime The time this object get in to the async queue.
     * @return Status of the call.
     */
    Status RLockAndSendToRemote(const std::string &objectKey, std::shared_ptr<SafeObjType> entryPtr,
                                bool &objIsValidInMem,
                                std::chrono::time_point<std::chrono::steady_clock> &beginTime);

    /**
     * @brief After send to remote, try modify property 'writeBackDone' and delete spilled object.
     * @param[in] objectKey The ID of the object
     * @param[in] entry The object entry
     * @param[in] createTime The object version.
     * @return Status of the call.
     */
    Status AfterSendToRemote(const std::string &objectKey, SafeObjType &entry, uint64_t createTime);

    /**
     * @brief send data to Remote under lock, the caller is responsible for locking
     * @param[in] objectKey the object key
     * @param[in] buf the data of object
     * @param[in] createTime the create time of object
     * @param[in] beginTime The time this object get in to the async queue.
     * @return Status of the call.
     */
    Status SendToRemoteOnLock(const std::string &objectKey, std::shared_ptr<std::stringstream> buf, uint64_t createTime,
                              uint64_t &dataSize, WriteMode writeMode, uint32_t ttlSecond,
                              std::chrono::time_point<std::chrono::steady_clock> &beginTime);

    /**
     * @brief Try to evict when memory size reach high water maker.
     * @param[in] objectKey The ID of the object need to allocate.
     * @param[in] needSize The size need to allocate.
     */
    void TryEvict(const std::string &objectKey, uint64_t needSize);

    /**
     * @brief Allocate memory for object to share.
     * @param[in] populate Indicate need populate or not.
     * @param[in] objectKV The entry that need to allocate memory and its corresponding objectKey.
     * @param[out] shmUnit The share memory info of object.
     * @return Status of the call.
     */
    Status AllocateMemory(bool populate, ObjectKV &objectKV, datasystem::ShmUnit &shmUnit);

    /**
     * @brief Calculate a index of list according to objectKey.
     * @param[in] objectKey The Id of the object need to be calculated.
     * @return Index of list.
     */
    size_t ObjectKey2QueueIndex(const std::string &objectKey) const;

    /**
     * @brief Update last success timestamp.
     */
    void UpdateLastSuccessTimestamp();

    /**
     * @brief Add unfininsed obejcts to failed objects.
     * @param[in] objectKey Object key.
     */
    void AddToFailedObjects(const std::string &objectKey);

    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<PersistenceApi> persistenceApi_{ nullptr };
    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    std::vector<std::shared_ptr<BlockingList>> queues_;
    std::vector<Thread> threadPool_;
    std::atomic<bool> running_{ false };
    std::atomic<uint32_t> sendingCount_{ 0 };

    // Protect 'lastSunccessTimestamp_'
    mutable std::shared_timed_mutex timestampMutex_;
    std::chrono::time_point<std::chrono::steady_clock> lastSunccessTimestamp_;

    // Protect 'failedObjects_'
    mutable std::shared_timed_mutex failedObjectsMutex_;
    std::unordered_set<std::string> failedObjects_;
    DataLimiter limiter_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_ASYNC_SEND_MANAGER_H
