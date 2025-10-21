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
 * Description: LRU cache.
 */
#ifndef DATASYSTEM_COMMON_LRU_LRU_CACHE_H
#define DATASYSTEM_COMMON_LRU_LRU_CACHE_H

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/lru/lru_policy.h"  // Added for convenience.  Caller just need to include lru_cache.h
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/template_util.h"
#include "datasystem/common/util/thread.h"

namespace datasystem {
/**
 * Base class for the keys used within the Lru cache.
 *
 * This class is using the CRTP design pattern to define the interface that derived classes must follow.
 * In this case, the derived key class must implement the *Impl functions for use with the LruCache.
 * @tparam Derived CRTP pattern technique, the derived class is the type for the base class.
 */
template <typename Derived>
class HashKey {
public:
    /**
     * @brief Default destructor.
     */
    ~HashKey() = default;

    /**
     * @brief Equality operator. Derived class must implement EqualImpl function.
     * @param[in] cmp The hash key to compare equality.
     * @return True if this instance is equal to input.
     */
    bool operator==(const HashKey &cmp) const
    {
        return static_cast<Derived const *>(this)->EqualImpl(static_cast<Derived const *>(&cmp));
    }

    /**
     * @brief Inequality operator.
     * @param[in] cmp the hash key to compare inequality
     * @return True if this instance is not equal to input.
     */
    bool operator!=(const HashKey &cmp) const
    {
        return !(static_cast<Derived const *>(this)->EqualImpl(static_cast<Derived const *>(&cmp)));
    }

    /**
     * @brief Optional. Invokes the ToString method to display debug info about the policy. Derived class has the
     * option to implement the ToStringImpl function.
     * @return The object in a string format.
     **/
    std::string ToString() const
    {
        return static_cast<Derived const *>(this)->ToStringImpl();
    };

    /**
     * @brief If the derived class does not implement ToStringImpl, this function exists to display <NA>.
     * @return The object in a string format.
     **/
    std::string ToStringImpl() const
    {
        return "<NA>";
    };

    /**
     * @brief Small nested class to provide the hash function. Derived class must implement static function HashImpl.
     * @return The computed hash code for hashing to hash buckets in a hash table implementation.
     */
    struct HashFunction {
        std::size_t operator()(const HashKey &k) const
        {
            return Derived::HashImpl(static_cast<Derived const *>(&k));
        };
    };

private:
    friend Derived;  // constructor

    /**
     * A CRTP pattern technique, private constructor prevents the wrong class from being passed to the CRTP template.
     */
    HashKey() = default;
};

/**
 * A customizable LRU (least recently used) cache template.  This is the main front-end class for users of the cache.
 *
 * This class provides an LRU cache.  Reading objects or writing new objects will promote the accessed object to the
 * most recent cache object age.
 * This cache manages the amount of entries within it by executing methods against a cache eviction strategy (policy)
 * class that the user must provide and implement (See lru_policy.h).
 * Note that this cache does not actually cache the objects themselves, but rather it only tracks the keys of the data
 * and a pointer to the objects.
 * Use of this LRU cache requires that the user must also implement some required classes via template parameters
 * described here (see lru_policy.h for further details on the interfaces for the policy classes).
 * @tparam LruKey A class to describe the key and it's hashing methods (base class interface is HashKey class).
 * @tparam LruObjPtr The object type (must be a pointer type) that will be cached.
 * @tparam LruEvictionPolicy The eviction policy that manages cache objects and decides which objects should be evicted.
 */
template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
class LruCache final {
public:
    static_assert(std::is_pointer<LruObjPtr>::value || is_shared_ptr<LruObjPtr>::value,
                  "The lru object template parameter must be a pointer type or shared_ptr type");
    /**
     * @brief A simple builder class for constructing the LruCache.
     * This enforces that the LruCache must be instantiated as a unique ptr, and it provides an initialization path
     * during the objects construction.
     */
    class LruBuilder {
    public:
        /**
         * @brief Builder constructor.
         */
        LruBuilder() : builderPolicy_(nullptr), builderNumBuckets_(DFT_NUM_BUCKETS)
        {
            lruCache_.reset(new LruCache<LruKey, LruObjPtr, LruEvictionPolicy>);
        };

        /**
         * @brief Move constructor.
         */
        LruBuilder(LruBuilder &&other) noexcept
        {
            lruCache_ = std::move(other.lruCache_);
            builderPolicy_ = std::move(other.builderPolicy_);
            builderNumBuckets_ = other.builderNumBuckets_;
        }

        /**
         * @brief Default destructor.
         */
        ~LruBuilder() = default;

        /**
         * @brief Setter for policy.
         */
        LruBuilder &SetPolicy(std::unique_ptr<LruEvictionPolicy> policy)
        {
            builderPolicy_ = std::move(policy);
            builderNumBuckets_ = DFT_NUM_BUCKETS;
            return *this;
        };

        /**
         * @brief Setter for number of buckets.
         */
        LruBuilder &SetNumBuckets(uint32_t numBuckets)
        {
            builderNumBuckets_ = numBuckets;
            return *this;
        };

        /**
         * @brief Setter for turn on/off async evict thread.
         * @param[in] async Indicate whether to use async evict.
         */
        LruBuilder &SetAsyncEvict(bool async)
        {
            lruCache_->allowAsyncEvict_ = async;
            return *this;
        };

        /**
         * @brief Setter for number of partitions.
         * @param[in] numPartitions The number of partitions.
         */
        LruBuilder &SetNumPartitions(int numPartitions)
        {
            numPartitions_ = numPartitions;
            return *this;
        };

        /**
         * @brief Final build method to provide the newly constructed class back to the user, as well as drive init
         * actions and error checks.
         * @param[out] newLru The constructed LruCache is output into this unique_ptr.
         * @returns Status of the call.
         */
        Status Build(std::unique_ptr<LruCache<LruKey, LruObjPtr, LruEvictionPolicy>> *newLru)
        {
            // Perform some initial error checks before continuing with construction of the object.
            if (newLru == nullptr || *newLru != nullptr) {
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "LruCache Build requires a null unique_ptr for output.");
            }

            if (builderPolicy_ == nullptr) {
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "LruCache must be created with a policy.");
            }

            if (builderNumBuckets_ < 1 || builderNumBuckets_ > MAX_NUM_BUCKETS) {
                RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                              "Buckets for lru cache must be in the range of 1 to " + std::to_string(MAX_NUM_BUCKETS));
            }

            RETURN_IF_NOT_OK(builderPolicy_->Init());
            lruCache_->lruKeyLookup_ = std::make_unique<KeyLookupMap>(builderNumBuckets_);
            lruCache_->evictPolicy_ = std::move(builderPolicy_);
            lruCache_->InitPartitions(numPartitions_);
            lruCache_->Init();
            *newLru = std::move(lruCache_);  // Return output.
            return Status::OK();
        };

    private:
        std::unique_ptr<LruCache<LruKey, LruObjPtr, LruEvictionPolicy>> lruCache_;  // Cache returned by builder.
        std::unique_ptr<LruEvictionPolicy> builderPolicy_;                          // Builder staging of the policy.
        uint32_t builderNumBuckets_;  // Builder staging for number of buckets.
        int numPartitions_ = 1;
    };

    /**
     * @brief Quick static method to give the builder to the caller.
     */
    static LruBuilder Builder()
    {
        return LruBuilder();
    };

    /**
     * @brief Default destructor.
     */
    ~LruCache() = default;

    /**
     * @brief This function makes a reference to the object in the LruCache
     * - If this object is already in the cache, it is promoted to the most recently used object.
     * - If the object is not in the cache, then the object will be inserted into the cache, and it becomes the most
     *   recently used object.
     * Depending on the algorithms provided by the LruEvictionPolicy class, it is possible that older entries in the
     * cache could be evicted to make room for the new entry.
     * @param[in] k  The key used to uniquely describe the object being cached.
     * @param[in] lruObjPtr The pointer to the object being cached.
     * @param[in/out] stats Optional statistics collection.  Pass in nullptr to bypass stats collection.
     * @return Status of the call.
     */
    Status Access(const LruKey &k, LruObjPtr lruObjPtr, std::vector<uint32_t> *stats = nullptr);

    /**
     * @brief This function executes a key lookup for the requested key.
     * - If the key is found, the pointer to the object is returned and the object is promoted to the most recently used
     * - If the key is not found, no action is taken and lruObjPtr is a nullptr.
     * @param[in] k The key used for the lookup.
     * @param[out] lruObjPtr The pointer to the object that was found.  nullptr if not found in the cache.
     * @return Status of the call.
     */
    Status Lookup(const LruKey &k, LruObjPtr *lruObjPtr);

    /**
     * @brief This function will remove the requested key from lru cache if the key is found. This is different from an
     * an eviction. It does not call the eviction methods of the policy which may change the state of the object.
     * Instead, it just silently removes the entry from the lru, but it does update the policy tracking to account for
     * the removed object.
     * @param[in] k The key to find the object to remove.
     * @return Status of the call.
     */
    Status Remove(const LruKey &k);

    /**
     * @brief For debug/testing purposes, this functions writes cache information to a string.
     * @param[in] getLock Indicates if the lru locking should be taken to ensure consistent output. If called from
     * with internal code that is already holding the locks, then this should be false.
     * @param[in] getKey True if you want to get the entire cache's list of keys and their data. Do not use this for
     * large caches because every single entry will be captured. Its intended use-case is for small unit tests to view
     * what is inside a small cache. If the value is false, only summary info is captured.
     * @return The object in a string format.
     */
    std::string ToString(bool getLock, bool getKey = true);

    /**
     * @brief Gets the current number of items inside the lru cache.
     * @return The number of objects in the lru.
     *
     */
    uint32_t Size() const;

    /**
     * @brief Checks a number of conditions within the policy and then decides overall if throttling may be needed to
     * slow down incoming workload.
     * @param[in] numPending The number of pending lru updates that have not completed yet.
     * @param[in] avgSize The average size of an object in the lru.
     * @return True if the code recommends that congestion control is turned on.
     */
    bool IsThrottlingNeeded(uint32_t numPending, uint64_t avgSize);

    /**
     * @brief Get the Lru Eviction Policy object.
     * @return The reference of lru eviction policy object.
     */
    const std::unique_ptr<LruEvictionPolicy> &GetLruEvictionPolicy() const
    {
        return evictPolicy_;
    }

private:
    using LruKeyNode = std::pair<LruKey, LruObjPtr>;  // Each item in the cache is the key and a pointer to the obj.
    using KeyLookupMap =
        std::unordered_map<LruKey, typename std::list<LruKeyNode>::iterator, typename LruKey::HashFunction>;

    /**
     * @brief Private constructor forces the user to create the object through the use of the builder.
     */
    LruCache() : softReset_(true), numObjects_(0), allowAsyncEvict_(false)
    {
        asyncEvictThread_ = std::make_unique<EvictThread>(this);
    };

    void Init()
    {
        if (allowAsyncEvict_) {
            asyncEvictThread_->Start();
        }
    }

    void InitPartitions(int numPartitions)
    {
        numPartitions_ = numPartitions;
        partitionStarts_.assign(numPartitions, lruKeyList_.end());
    }

    /**
     * @brief A private helper function to drive the logic of making room in the cache for a new entry.
     * @param[in] lruObjPtr The object we want to promote in the lru.
     * @param[in] cacheMiss An indicator if we are working on behalf of a cache miss scenario (T) or cache hit (F).
     * @param[in] stats A pointer to a vector of integers that represent statistics to collect (if enabled).
     * @return Status of the call.
     */
    Status MakeRoomInCache(LruObjPtr lruObjPtr, bool cacheMiss, std::vector<uint32_t> *stats);

    /**
     * @brief A private helper function to execute a hard eviction.
     * @param[in] lruObjPtr The object we want to promote in the lru.
     * @param[in] stats A pointer to a vector of integers that represent statistics to collect (if enabled).
     * @return Status of the call.
     */
    Status HardEviction(LruObjPtr lruObjPtr, std::vector<uint32_t> *stats);

    /**
     * @brief A private helper function to execute a soft eviction.
     * @param[in] lruObjPtr The object we want to promote in the lru.
     * @param[in] stats A pointer to a vector of integers that represent statistics to collect (if enabled).
     * @return Status of the call.
     */
    Status SoftEviction(LruObjPtr lruObjPtr, std::vector<uint32_t> *stats);

    /**
     * @brief Try to async evict cache entry when AboveHighWatermark return true.
     * @return true if the lru cache is above the high watermark, false otherwise.
     */
    bool AboveHighWatermark();

    /**
     * @brief We will evict the cache objects when AboveHighWatermark return true,
     * and stop when BelowLowWatermark return true.
     * @return Return the status of the call.
     */
    Status EvictToLowWatermarks();

    /**
     * @brief Searches the lru for a good starting point to begin soft evictions from.
     * @param[in] lruObjPtr The object we want to promote in the lru.
     * @param[in] doOpt Set to true to perform optimized position scan.
     * @return Return the status of the call.
     */
    Status InitSoftEviction(LruObjPtr lruObjPtr, bool doOpt);

    /**
     * @brief A small function to correct/check for a scenario when the lru is removing an object, but that object
     * happens to be pointed to by the lastSoft iterator.  In that case, we want to reverse the last soft iterator
     * to the next position so that the iterator will not become invalidated when the object is removed.
     * @param[in] key The Lru key.
     */
    void RepositionLastSoft(const LruKey &key);

    /**
     * @brief Reposition for PartitionStarts.
     * @param[in] key The Lru key.
     * @param[in] partitionId The partitionId.
     */
    void RepositionPartitionStarts(const LruKey &key, int partitionId);

    /**
     * @brief Get the insert position for partitionId.
     * @param[in] partitionId The partitionId.
     * @param[in] insertPos The insert position.
     */
    void GetInsertPos(int partitionId, typename std::list<LruKeyNode>::iterator &insertPos)
    {
        if (partitionId == 0) {
            insertPos = lruKeyList_.begin();
        } else if (partitionStarts_[partitionId] != lruKeyList_.end()) {
            insertPos = partitionStarts_[partitionId];
        } else {
            insertPos = lruKeyList_.end();
            auto nextPartitionId = partitionId + 1;
            while (nextPartitionId < numPartitions_) {
                if (partitionStarts_[nextPartitionId] != lruKeyList_.end()) {
                    insertPos = partitionStarts_[nextPartitionId];
                    break;
                }
                nextPartitionId++;
            }
        }
    }

    static const uint32_t DFT_NUM_BUCKETS = 128;
    static const uint32_t MAX_NUM_BUCKETS = 16384;

    std::list<LruKeyNode> lruKeyList_;                   // The cached objects.
    std::unique_ptr<KeyLookupMap> lruKeyLookup_;         // Lookup table.
    int numPartitions_ = 1;
    std::vector<typename std::list<LruKeyNode>::iterator> partitionStarts_;

    std::unique_ptr<LruEvictionPolicy> evictPolicy_;     // The eviction policy.
    typename std::list<LruKeyNode>::iterator lastSoft_;  // Position of the previous soft eviction position.

    class EvictThread;                               // The declaration of async EvictThread.
    std::unique_ptr<EvictThread> asyncEvictThread_;  // The thread pool to evict cache entry asynchronously.
                                // Due to C++ reverse class member destruction order, asyncEvictThread_ should
                                // be placed after evictPolicy_ to avoid accessing it after it got destructed.

    bool softReset_;                    // True if we need to reset the soft eviction position.
    std::atomic<uint32_t> numObjects_;  // The number of objects in this cache.
    bool allowAsyncEvict_;              // If set false will not allow async evict.
    std::mutex lruMtx_;                 // Locks all actions on this cache.
};

/**
 * @brief A simple thread class used to evict cache object asynchronously.
 */
template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
class LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::EvictThread {
public:
    /**
     * @brief EvictThread constructor.
     * @param[in] lruCache The LruCache that need to be evicted asynchronously.
     */
    explicit EvictThread(LruCache<LruKey, LruObjPtr, LruEvictionPolicy> *lruCache)
        : lruCache_(lruCache), isSleep_(false), exitFlag_(false)
    {
    }

    /**
     * @brief EvictThread destructor.
     */
    ~EvictThread()
    {
        Stop();
    }

    /**
     * @brief Start evict cache object asynchronously.
     */
    void Start()
    {
        LOG(INFO) << "Start LruCache async thread.";
        thread_ = std::make_unique<Thread>(&EvictThread::Run, this);
        thread_->set_name("LruEvict");
    }

    /**
     * @brief Stop evict thread.
     */
    void Stop()
    {
        if (!thread_) {
            return;
        }
        LOG(INFO) << "Stop LruCache async thread.";
        {
            std::lock_guard<std::mutex> lock(mutex_);
            exitFlag_ = true;
        }
        cv_.notify_one();
        thread_->join();
        thread_.reset();
    }

    /**
     * @brief The main logic of evict cache object asynchronously.
     */
    void Run()
    {
        TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
        LOG(INFO) << "Ready to do async evict.";
        while (!exitFlag_) {
            isSleep_ = false;
            if (lruCache_->AboveHighWatermark()) {
                LOG(INFO) << "Before evict:" << lruCache_->ToString(true, false);
                Status status = lruCache_->EvictToLowWatermarks();
                if (status.IsError()) {
                    LOG(ERROR) << status.ToString();
                }
                LOG(INFO) << "After evict:" << lruCache_->ToString(true, false);
            }
            std::unique_lock<std::mutex> lock(mutex_);
            isSleep_ = true;
            cv_.wait_for(lock, std::chrono::milliseconds(DFT_SLEEP_TIME_MS));
        }
    }

    /**
     * @brief Check whether it is need to evict, and try to wake up the thread if it is sleeping.
     */
    void TryToWakeUp()
    {
        if (isSleep_ && lruCache_->AboveHighWatermark()) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.notify_one();
        }
    }

private:
    static const uint32_t DFT_SLEEP_TIME_MS;
    LruCache<LruKey, LruObjPtr, LruEvictionPolicy> *lruCache_;
    std::atomic<bool> isSleep_;
    bool exitFlag_;
    std::condition_variable cv_;  // wait: DFT_SLEEP_TIME_MS; notify: isSleep_ && lruCache is above the high watermark.
    std::mutex mutex_;  // protect cv_ and exitFlag
    std::unique_ptr<Thread> thread_;
};

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
const uint32_t LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::EvictThread::DFT_SLEEP_TIME_MS = 1000;

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
uint32_t LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::Size() const
{
    return numObjects_;  // Atomic counter, does not require the mutex.
}

// For debug/testing purposes, this functions writes cache information to a string.
template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
std::string LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::ToString(bool getLock, bool getKey)
{
    std::string out_str("\n");

    // The unlocked version of this function is for when you want a debug print during a time where you are already
    // holding the mutex.
    if (getLock) {
        lruMtx_.lock();
    }
    out_str += "Lru cache size: " + std::to_string(numObjects_) + "\n";
    out_str += "  Lru policy info:\n  " + evictPolicy_->ToString() + "\n    Current soft key: ";
    if (softReset_) {
        out_str += "end";
    } else {
        out_str += lastSoft_->first.ToString();
    }
    if (getKey) {
        out_str += "\n    lru_key_list (most recently used to least recently used):";
        for (auto it : lruKeyList_) {
            out_str += "\n      Key: " + it.first.ToString() + ": " + it.second->ToString();
        }
        out_str += "\n    lru_key_Map:";
        for (auto it : *lruKeyLookup_) {
            out_str += "\n      Key: " + it.first.ToString() + ": " + it.second->second->ToString();
        }
        out_str += "\n    lru_partition_map:";
        for (auto it : partitionStarts_) {
            if (it != lruKeyList_.end()) {
                out_str += "\n      Key: " + it->first.ToString() + ": " + it->second->ToString();
            } else {
                out_str += "\n      end";
            }
        }
    }

    if (getLock) {
        lruMtx_.unlock();
    }

    return out_str;
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
Status LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::InitSoftEviction(LruObjPtr lruObjPtr, bool doOpt)
{
    static const float optMultiplier = 1.05;
    Status rc;
    uint64_t numIterated = 0;

    // The goal is to position the iterator on the oldest eviction candidate. It is a list ordered by age from newest
    // to oldest.  There are 2 ways to find the candidate.
    // For example:
    // head,n1,n2,n3,n4,candidate,n6,tail ==> Faster to search from the tail
    // head,n1,candidate,n3,n4,n5,n6,tail ==> Faster to search from the head
    //
    // The problem is that we do not know where the candidate is, so we have to scan for it.
    //
    // Method 1) Start from the tail and iterate backwards until we find the first candidate.  This method is 100%
    //           effective, but it can be costly if there are many trailing objects that are not eviction candidates.
    // Method 2) Start from the head and iterate forwards until we find the last candidate.  This method would require a
    //           full scan from start to finish (not optimal), however we can make an estimate that will work for almost
    //           all cases.
    //           The estimate will position the iterator at some placement a bit farther along than the soft eviction
    //           limit, followed by the reverse iteration.
    //           Example:  head,n1,candidate,n3,n4,n5,n6,nx,tail
    //           Here, we quickly scan forwards to n4, then reverse to the candidate.
    uint32_t softLimitPos = evictPolicy_->GetMaxSoftLimit() * optMultiplier;
    // If the number of leading entries is less than the number of trailing entries then seek forward that many slots.
    if (doOpt && softLimitPos != 0 && numObjects_ > softLimitPos && softLimitPos < (numObjects_ - softLimitPos)) {
        lastSoft_ = lruKeyList_.begin();
        std::advance(lastSoft_, softLimitPos);
        numIterated += softLimitPos;
        LOG(INFO) << "Soft eviction scan optimization moved forward " << numIterated << " objects.";
    } else {
        // The trailing entries are smaller, init to search from the tail.
        lastSoft_ = lruKeyList_.end();
        --(lastSoft_);
    }

    // Even if we did an initial forward iteration, we still need to validate/position our candidate (which might be
    // the one we are positioned on already).
    softReset_ = false;

    do {
        // Skip over the object that is being promoted if we are pointing to that one.
        if (lastSoft_->second == lruObjPtr) {
            --(lastSoft_);  // Reverse to the next least used.
            rc = Status(StatusCode::K_TRY_AGAIN, "retry");
            continue;
        }

        // Do a lockless reverse walk to find the first soft eviction candidate. Using EVICT_SOFT_POS flag means that we
        // are only testing if this object is a candidate. It will not actually execute a soft eviction yet.
        rc = evictPolicy_->Evict(lastSoft_->second, EvictionCtrl::EVICT_SOFT_POS);
        if (rc.GetCode() == StatusCode::K_TRY_AGAIN) {
            --(lastSoft_);  // reverse to the next least used.
        }
        ++numIterated;
    } while (rc.GetCode() == StatusCode::K_TRY_AGAIN && lastSoft_ != lruKeyList_.begin());

    // This message can help assess the performance cost of this setup code.
    LOG(INFO) << "Soft eviction position initialized.  Objects scanned: " << numIterated;
    return rc;
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
Status LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::SoftEviction(LruObjPtr lruObjPtr, std::vector<uint32_t> *stats)
{
    Status rc;
    uint32_t attempts = 0;

    // Attempt to evict the least recently used object.  We use a forward iterator here to walk the list backwards using
    // -- operator from least used to most used (lastSoft_).
    // Use the eviction policy to attempt to evict.  The eviction code may return K_TRY_AGAIN if the object disallows
    // the eviction.  In the case when an eviction is disallowed, we iterate backwards to the next least used and try
    // to evict the next one instead.

    // Here in the soft eviction version of the code, we never remove objects from the lru.
    // The start position of our reverse walk begins at the end of the list (least used), but if there were previous
    // soft evictions, we can start the reverse walk from where we left off last time.
    if (softReset_) {
        RETURN_IF_NOT_OK(InitSoftEviction(lruObjPtr, true));
    } else if (lastSoft_ == lruKeyList_.begin()) {
        RETURN_STATUS_LOG_ERROR(K_UNKNOWN_ERROR, "Invalid last eviction position");
    }

    do {
        // In a soft eviction, it's quite possible that we encounter ourself (the object we are promoting) during the
        // chain walk to find an object to softly evict (for example, it was a cache hit case).
        // In this scenario, do not grab the lock, just continue to the next one
        if (lastSoft_->second == lruObjPtr) {
            --(lastSoft_);  // reverse to the next least used.
            rc = Status(StatusCode::K_TRY_AGAIN, "retry");
            continue;
        }

        // Currently, we are holding the locks for the object we are making room for.
        // However, to evict an object, we also need to get locks on the object to evict.
        evictPolicy_->LockObject(lastSoft_->second);

        // Capture stats on this eviction attempt if enabled
        if (stats != nullptr) {
            evictPolicy_->UpdateStats(stats, lastSoft_->second);
        }

        rc = evictPolicy_->Evict(lastSoft_->second, EvictionCtrl::EVICT_SOFT);
        if (rc.GetCode() == StatusCode::K_TRY_AGAIN) {
            evictPolicy_->UnlockObject(lastSoft_->second);
            --(lastSoft_);  // Reverse to the next least used.
        } else if (rc.IsError()) {
            // Some other error. Bail out.
            evictPolicy_->UnlockObject(lastSoft_->second);
            return rc;
        } else {
            // Successfully evicted the object, release the write locking.
            evictPolicy_->UnlockObject(lastSoft_->second);
        }

        // There is a chance that we can reach the front of the chain without finding an eviction candidate. This can
        // happen if the optimization in InitSoftEviction() did not find the best starting place for soft evictions.
        // Circle back all the way to the tail of the chain and try again.
        if (attempts == 0 && lastSoft_ == lruKeyList_.begin()) {
            LOG(INFO) << "Soft eviction could not find an eviction candidate. Restart at the tail.";
            RETURN_IF_NOT_OK(InitSoftEviction(lruObjPtr, false));  // Updates lastSoft_.
            ++attempts;
            rc = Status(StatusCode::K_TRY_AGAIN, "retry");
        }
    } while (rc.IsError() && lastSoft_ != lruKeyList_.begin());

    // If no objects could be evicted, then we cannot proceed. This should be impossible to occur since the reason we
    // are doing the soft eviction is because there's entries to evict!
    if (attempts == 1 && lastSoft_ == lruKeyList_.begin()) {
        RETURN_STATUS_LOG_ERROR(K_UNKNOWN_ERROR, "Soft eviction parsed the entire chain without success.");
    }

    // We do not actually remove the object from the lru in the soft eviction. However, position the iterator to
    // go one more older so that the next run will start from there.
    --(lastSoft_);

    return Status::OK();
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
Status LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::HardEviction(LruObjPtr lruObjPtr, std::vector<uint32_t> *stats)
{
    Status rc;

    // If we have evicted the entire cache and we still don't have room for it, then bail out with error because the
    // single object exceeds the entire cache policy allowance.
    if (lruKeyList_.empty()) {
        std::string err("Object cannot fit in the LRU cache.");
        LOG(ERROR) << err;
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, err);
    }

    // Attempt to evict the least recently used object. We use a reverse iterator here to walk the list backwards from
    // least used to most used.  Tail of the list (rbegin() is the least used and most often we'll simply be able to
    // evict that one.
    // Use the eviction policy to attempt to evict. The eviction code may return K_TRY_AGAIN if the object disallows
    // the eviction. In the case when an eviction is disallowed, we iterate backwards to the next least used and try
    // to evict the next one instead.
    typename std::list<LruKeyNode>::reverse_iterator revIter = lruKeyList_.rbegin();
    do {
        // In a cache hit case, we may encounter ourself when reversing through the list to find an object to evict.
        // This isn't an error, just keep scrolling back.
        if (revIter->second == lruObjPtr) {
            ++revIter;
            rc = Status(StatusCode::K_TRY_AGAIN, "retry");
            continue;
        }

        // Currently, we are holding the locks for the object we are making room for.
        // However, to evict an object, we also need to get locks on the object to evict.
        evictPolicy_->LockObject(revIter->second);

        // Capture stats on this eviction attempt if enabled.
        if (stats != nullptr) {
            evictPolicy_->UpdateStats(stats, revIter->second);
        }

        rc = evictPolicy_->Evict(revIter->second, EvictionCtrl::NONE);
        if (rc.GetCode() == StatusCode::K_TRY_AGAIN) {
            evictPolicy_->UnlockObject(revIter->second);
            ++revIter;  // Reverse to the next least used.
        } else if (rc.IsError()) {
            // Some other error. Bail out.
            evictPolicy_->UnlockObject(revIter->second);
            return rc;
        } else {
            // Successfully evicted the object, release the write locking.
            evictPolicy_->UnlockObject(revIter->second);
        }
    } while (rc.IsError() && revIter != lruKeyList_.rend());

    // If no objects could be evicted, then we cannot proceed. This can only happen if all objects were disallowed from
    // being evicted. In this case, the cache is considered full and cannot contain the new entry that is trying to be
    // added or promoted. Rather than fail with severe error, give the caller a chance to try again.
    if (revIter == lruKeyList_.rend()) {
        std::string err("Could not evict enough entries to make room for object in the LRU cache, last rc: "
                        + rc.ToString());
        LOG(WARNING) << err;
        RETURN_STATUS(StatusCode::K_TRY_AGAIN, err);
    }

    // Now that we have called the objects eviction code, we can remove it from our key list and the map.
    (void)lruKeyLookup_->erase(revIter->first);

    for (auto i = 0; i < numPartitions_; i++) {
        if (partitionStarts_[i] == std::next(revIter).base()) {
            partitionStarts_[i] = lruKeyList_.end();
        }
    }

    --(numObjects_);  // Decrement atomic count of objects.

    // Check and resolve the last soft position if we might be removing at the same spot.
    RepositionLastSoft(revIter->first);
    (void)lruKeyList_.erase(std::next(revIter).base());

    return Status::OK();
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
Status LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::MakeRoomInCache(LruObjPtr lruObjPtr, bool cacheMiss,
                                                                       std::vector<uint32_t> *stats)
{
    Status rc;

    // If there is no room in the cache based on the policy, evict entries until there is room for it
    // A policy may implement an optional soft eviction. Call the appropriate eviction handler based
    // on the eviction type.
    rc = evictPolicy_->RoomInCache(lruObjPtr, cacheMiss);
    while (rc.IsError()) {
        if (rc.GetCode() == StatusCode::K_FC_HARD_LIMIT) {
            RETURN_IF_NOT_OK(HardEviction(lruObjPtr, stats));
        } else if (rc.GetCode() == StatusCode::K_FC_SOFT_LIMIT) {
            RETURN_IF_NOT_OK(SoftEviction(lruObjPtr, stats));
        } else {
            LOG(WARNING) << "Failed when trying to make room in cache.";
            return rc;
        }

        // Check the space again and loop.
        rc = evictPolicy_->RoomInCache(lruObjPtr, cacheMiss);
    }
    return Status::OK();
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
bool LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::AboveHighWatermark()
{
    return evictPolicy_->AboveHighWatermark();
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
Status LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::EvictToLowWatermarks()
{
    if (!evictPolicy_->AboveHighWatermark()) {
        LOG(INFO) << "EvictToLowWatermarks - does not need evict";
    } else {
        LOG(INFO) << "EvictToLowWatermarks - needs evict";
        std::unique_lock<std::mutex> uLock(lruMtx_);
        // Evict until the capacity is less than or equal to low watermark.
        while (!evictPolicy_->BelowLowWatermarkImpl()) {
            RETURN_IF_NOT_OK(HardEviction(nullptr, nullptr));
        }
    }
    return Status::OK();
}

// This function makes a reference to the object lruObj in the lruCache.
template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
Status LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::Access(const LruKey &key, LruObjPtr lruObjPtr,
                                                              std::vector<uint32_t> *stats)
{
    Status rc;
    bool keyFound = true;

    RETURN_RUNTIME_ERROR_IF_NULL(lruObjPtr);
    int partitionId = lruObjPtr->PartitionId();
    CHECK_FAIL_RETURN_STATUS(
        partitionId >= 0 && partitionId < numPartitions_, K_INVALID,
        FormatString("Unsupported partition ID: %d. Total number of partitions: %d.", partitionId, numPartitions_));

    {
        // Any access will either promote an existing obj or insert the object if it's new.
        // Thus we are always a "write" operation and need to lock exclusive lock to the lru structures.
        std::lock_guard<std::mutex> uLock(lruMtx_);

        // In addition to the lock over the lru structures, we also need to obtain locking against the object that we
        // are trying to update into the lru.
        // Note that this locking is optional. The implementation of the eviction policy can choose not to implement
        // these and leave them as no-op's.
        evictPolicy_->LockObject(lruObjPtr);
        Raii unlockObj([this, lruObjPtr]() { evictPolicy_->UnlockObject(lruObjPtr); });

        // Checks the object for any conditions that may allow it to quit early without executing any lru access.
        // This is done under the protection of the above lock so anything checks in the object are safe.
        if (evictPolicy_->CheckBypassLru(lruObjPtr)) {
            return Status::OK();
        }

        // If the key does not exist in the cache, we want to insert it into the cache.
        // We might need to free up space in the cache to make room for the object.
        auto mapIt = lruKeyLookup_->find(key);
        if (mapIt == lruKeyLookup_->end()) {
            // Cache miss
            keyFound = false;
            RETURN_IF_NOT_OK(MakeRoomInCache(lruObjPtr, true, stats));
        } else {
            // Cache hit
            typename std::list<LruKeyNode>::iterator listIt = mapIt->second;

            // Even if it was a cache hit, we still need to ensure that we are not violating the eviction policies
            // when the object gets promoted. Example: we may drive a soft eviction (non-removal). It is also possible
            // that the attributes of the object we hit have changed such that the policy is now in violation and needs
            // to evict.
            RETURN_IF_NOT_OK(MakeRoomInCache(lruObjPtr, false, stats));

            // Check and resolve the last soft position if we might be removing at the same spot.
            RepositionLastSoft(listIt->first);
            RepositionPartitionStarts(listIt->first, partitionId);
            listIt = lruKeyList_.erase(listIt);

            // Do not count the removal for numObjects_ as this is a cache hit case of removing it first and then
            // adding it back in to the front right away, the size should not change.
        }

        typename std::list<LruKeyNode>::iterator insertPos;
        GetInsertPos(partitionId, insertPos);
        auto newIt = lruKeyList_.insert(insertPos, std::make_pair(key, lruObjPtr));
        partitionStarts_[partitionId] = newIt;

        if (keyFound) {
            mapIt->second = newIt;
            evictPolicy_->ObjHit(lruObjPtr);  // Update policy tracking for the cache hit.
        } else {
            (*lruKeyLookup_)[key] = newIt;
            evictPolicy_->ObjAdd(lruObjPtr);  // Update policy tracking for new data added.
            ++(numObjects_);                  // Update atomic counter for number of objects.
        }
    }
    if (allowAsyncEvict_) {
        asyncEvictThread_->TryToWakeUp();
    }
    return Status::OK();
}

// This function executes a key lookup for the requested key.
template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
Status LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::Lookup(const LruKey &key, LruObjPtr *lruObjPtr)
{
    if (lruObjPtr == nullptr || *lruObjPtr != nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "LRU lookup expects empty/null output lru obj ptr.");
    }

    {
        // Although this is only a lookup, it is not a read-only operation. The lookup promotes the object in the cache
        // and a full lock is needed.
        std::lock_guard<std::mutex> uLock(lruMtx_);

        // If the key does not exist in the cache then we're done.
        auto keyLookupPos = lruKeyLookup_->find(key);
        if (keyLookupPos == lruKeyLookup_->end()) {
            RETURN_STATUS(K_NOT_FOUND, "LRU Key lookup cache miss");
        }

        // Lock the object we're operating on.
        evictPolicy_->LockObject(lruObjPtr);

        typename std::list<LruKeyNode>::iterator it = (*lruKeyLookup_)[key];
        *lruObjPtr = it->second;  // Save a copy of the ptr for output.

        // Promote the key within the key list and update it's location in the map.
        // Check and resolve the last soft position if we might be removing at the same spot.
        RepositionLastSoft(it->first);
        int partitionId = (*lruObjPtr)->PartitionId();
        CHECK_FAIL_RETURN_STATUS(partitionId >= 0 && partitionId < numPartitions_, K_INVALID, "");
        RepositionPartitionStarts(it->first, partitionId);
        lruKeyList_.erase(it);
        typename std::list<LruKeyNode>::iterator insertPos;
        GetInsertPos(partitionId, insertPos);
        auto newIt = lruKeyList_.insert(insertPos, std::make_pair(key, *lruObjPtr));
        partitionStarts_[partitionId] = newIt;
        keyLookupPos->second = newIt;
        evictPolicy_->UnlockObject(lruObjPtr);
    }
    if (allowAsyncEvict_) {
        asyncEvictThread_->TryToWakeUp();
    }
    return Status::OK();
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
Status LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::Remove(const LruKey &key)
{
    Status rc;

    {
        // Removing something from the lru needs to protect against concurrent access.
        std::lock_guard<std::mutex> uLock(lruMtx_);

        auto it = lruKeyLookup_->find(key);
        // If the key is not found, return key not found to the caller.
        if (it == lruKeyLookup_->end()) {
            RETURN_STATUS(K_NOT_FOUND, "LRU Key lookup cache miss");
        }

        VLOG(2) << "Lru object removal for key: " << key.ToString();

        // Iterator points to a pair<key, list iterator>. The list iterator points to a pair<key, objptr>.
        // To get the object ptr, dereference the parent iterator and then the nested iterator.
        LruObjPtr lruObjPtr = it->second->second;
        evictPolicy_->LockObject(lruObjPtr);

        // Perform a remove eviction. It is up to the policy to decide if it behaves differently from a regular
        // eviction.
        RETURN_IF_NOT_OK(evictPolicy_->Evict(lruObjPtr, EvictionCtrl::EVICT_REMOVE));

        // Check and resolve the last soft position if we might be removing at the same spot.
        this->RepositionLastSoft(it->first);
        for (int i = 0; i < numPartitions_; i++) {
            RepositionPartitionStarts(it->first, i);
        }
        // Erase the reference in the map, and erase the reference from the lru key list.
        (void)lruKeyList_.erase(it->second);
        (void)lruKeyLookup_->erase(key);
        --(numObjects_);  // Decrement the atomic counter for total objects.

        evictPolicy_->UnlockObject(lruObjPtr);
    }
    if (allowAsyncEvict_) {
        asyncEvictThread_->TryToWakeUp();
    }
    return Status::OK();
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
void LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::RepositionLastSoft(const LruKey &key)
{
    if (!softReset_ && (lastSoft_->first == key)) {
        LOG(INFO) << "Repositioning lastSoft due list removal collision.";
        --(lastSoft_);
        // It should be extremely unlikely that lastSoft position has iterated to the front of the chain.
        if (lastSoft_ == lruKeyList_.begin()) {
            LOG(INFO) << "Repositioning lastSoft reached the front of the chain.  Reset.";
            softReset_ = true;
        }
    }
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
bool LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::IsThrottlingNeeded(uint32_t numPending, uint64_t avgSize)
{
    return (evictPolicy_->IsThrottlingNeeded(numPending, avgSize));
}

template <typename LruKey, typename LruObjPtr, typename LruEvictionPolicy>
void LruCache<LruKey, LruObjPtr, LruEvictionPolicy>::RepositionPartitionStarts(const LruKey &key, int partitionId)
{
    if (partitionStarts_[partitionId]->first == key) {
        partitionStarts_[partitionId]++;
    }
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LRU_LRUCACHE_H
