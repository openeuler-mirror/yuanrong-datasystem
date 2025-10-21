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
 * Description: LRU cache eviction policy class hierarchy.
 */
#ifndef DATASYSTEM_COMMON_LRU_LRU_POLICY_H
#define DATASYSTEM_COMMON_LRU_LRU_POLICY_H

#include <atomic>
#include <iostream>
#include <memory>

#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
/**
 * Flag class for defining different behaviours for the eviction methods. Lru cache logic may pass different options
 * into the eviction methods to control the different behaviours (if the derived implementations choose to implement it.
 */
enum class EvictionCtrl : uint8_t { NONE = 0, EVICT_SOFT = 1u, EVICT_REMOVE = 1u << 1, EVICT_SOFT_POS = 1u << 2 };
ENABLE_BITMASK_ENUM_OPS(EvictionCtrl);

class LruObjBase {
public:
    LruObjBase(int partitionId) : partitionId_(partitionId)
    {
    }

    virtual ~LruObjBase() = default;

    virtual int PartitionId() const final
    {
        return partitionId_;
    }

private:
    int partitionId_ = 0;
};

/**
 * Base class for the cache eviction policy.
 *
 * This base class is using the CRTP design pattern to define the interface that derived classes must follow.
 * The policy base defines the functions that the lru cache executes during the logic of deciding how to track cache
 * usage, and how/when to evict entries from the cache when the policy rules have been met.
 * Derived classes must implement the *Impl functions...
 * @tparam Derived CRTP pattern technique, the derived class is the type for the base class.
 */
template <typename Derived>
class LruPolicyBase {
public:
    /**
     * @brief Default destructor.
     */
    ~LruPolicyBase() = default;

    /**
     * @brief Creates a cloned copy of this object as a unique ptr.
     * @return the unique ptr to the cloned object.
     */
    std::unique_ptr<Derived> clone() const
    {
        return static_cast<Derived const *>(this)->cloneImpl();
    }

    /**
     * @brief Optional. Initializes the policy.
     * @return Status of the call.
     */
    Status Init()
    {
        return static_cast<Derived *>(this)->InitImpl();
    };

    /**
     * @brief If the derived class does not implement an init method, this one is a no-op in the base.
     * @return Status of the call.
     */
    Status InitImpl()
    {
        return Status::OK();
    };

    /**
     * @brief The policy to async evict cache entry. We will keep the cache the ratio of cache in [lowWatermark,
     * highWatermark]. When the cache capacity exceeds the high watermark, the evict thread will evict the cache
     * asynchronously from  highWatermark to lowWatermark.
     * @param[in] lowWatermark The low watermark ratio of the cache, which should be less than highWatermark, and the
     * range should be (0, 100).
     * @param[in] highWatermark The high watermark ratio of the cache, which should be greater than lowWatermark, and
     * the range should be (0, 100).
     * @return Status of the call.
     */
    Status SetWatermark(uint32_t lowWatermark, uint32_t highWatermark)
    {
        CHECK_FAIL_RETURN_STATUS((lowWatermark > MIN_WATERMARK && highWatermark < MAX_WATERMARK), StatusCode::K_INVALID,
                                 "The range of lowWatermark_ to highWatermark_ should be (0, 100)");
        CHECK_FAIL_RETURN_STATUS((lowWatermark < highWatermark), StatusCode::K_INVALID,
                                 "The range of lowWatermark_ to highWatermark_ should be (0, 100) and "
                                 "lowWatermark_ is letter than highWatermark_");
        lowWatermark_ = lowWatermark;
        highWatermark_ = highWatermark;
        return Status::OK();
    }

    /**
     * @brief Used by async evict thread in LruCache. When this function return true, the evict thread will evict the
     * cache asynchronously from  highWatermark to lowWatermark.
     * @returns Return true if the cache need be evicted.
     */
    bool AboveHighWatermark() const
    {
        return static_cast<Derived const *>(this)->AboveHighWatermarkImpl();
    };

    /**
     * @brief If the derived class does not implement AboveHighWatermarkImpl, do not allow evict asynchronously.
     * @returns Return false, and do not allow evict asynchronously.
     */
    bool AboveHighWatermarkImpl() const
    {
        return false;
    };

    /**
     * @brief Used by async evict thread in LruCache. When this function return true, the evict thread will stop
     * removing cache entries.
     * @returns Return true if the cache finished evict.
     */
    bool BelowLowWatermark() const
    {
        return static_cast<Derived const *>(this)->BelowLowWatermarkImpl();
    };

    /**
     * @brief If the derived class does not implement BelowLowWatermarkImpl, do not allow evict asynchronously.
     * @returns Return true, and do not allow evict asynchronously.
     */
    bool BelowLowWatermarkImpl() const
    {
        return true;
    };

    /**
     * @brief Updates the policy to advise of the addition of a new object to the LRU cache.
     * @param[in] obj The object that has been added to the LRU cache.
     * @tparam ObjPtr The object type in the cache.
     */
    template <typename ObjPtr>
    void ObjAdd(const ObjPtr obj)
    {
        static_cast<Derived *>(this)->ObjAddImpl(obj);
    };

    /**
     * @brief Optional. Updates the policy to advise that an object got a cache hit.
     * @param[in] obj The object that has been promoted in the LRU cache.
     * @tparam ObjPtr The object type in the cache.
     */
    template <typename ObjPtr>
    void ObjHit(const ObjPtr obj)
    {
        static_cast<Derived *>(this)->ObjHitImpl(obj);
    };

    /**
     * @brief Checks if there is room in the cache for the object.
     * @param[in] obj The object to test with with the policy.
     * @param[in] cacheMiss A flag to tell us if this check is for a cache hit or miss scenario.
     * @tparam ObjPtr The object type in the cache.
     * @returns Status from the call.  Status returns:
     * K_OK : there was room in the cache.
     * K_FC_HARD_LIMIT : there was no room in the cache and a hard eviction will be needed.
     * K_FC_SOFT_LIMIT : there was room in the cache, but the soft limit was hit and soft eviction can be done.
     */
    template <typename ObjPtr>
    Status RoomInCache(ObjPtr obj, bool cacheMiss) const
    {
        return static_cast<Derived const *>(this)->RoomInCacheImpl(obj, cacheMiss);
    };

    /**
     * @brief Executes an eviction against the given object and updates the policy tracking to reflect the removed
     * object.
     * @param[in] obj The object to evict.
     * @param[in] evictFlags Flags that influence eviction behaviour.
     * @tparam ObjPtr The object type in the cache.
     * @return Status of the call.
     */
    template <typename ObjPtr>
    Status Evict(ObjPtr obj, EvictionCtrl evictFlags)
    {
        return static_cast<Derived *>(this)->EvictImpl(obj, evictFlags);
    };

    /**
     * @brief Optional. Updates an input vector with statistics if the given object wants to be evicted.
     * @param[in] stats A pointer to a vector of integers to update with statistics.
     * @param[in] obj The object to compute stats against when an eviction is attempted on it.
     * @tparam ObjPtr The object type in the cache.
     */
    template <typename ObjPtr>
    void UpdateStats(std::vector<uint32_t> *stats, const ObjPtr obj)
    {
        static_cast<Derived *>(this)->UpdateStatsImpl(stats, obj);
    };

    /**
     * @brief Optional. If the lru needs to execute locking on either the container that holds the object, or the
     * object itself, then this method will acquire those locks.
     * @param[in] obj The object is passed in so that the LRU can identify which locks to take.
     * @tparam ObjPtr The object type in the cache.
     * @return Status of the call.
     */
    template <typename ObjPtr>
    Status LockObject(ObjPtr obj)
    {
        return static_cast<Derived *>(this)->LockObjectImpl(obj);
    };

    /**
     * @brief Optional. If the lru needs to execute locking on either the container that holds the object, or the
     * object itself, then this method will release those locks.
     * @param[in] obj The object is passed in so that the LRU can identify which locks to release.
     * @tparam ObjPtr The object type in the cache.
     * @return Status of the call.
     */
    template <typename ObjPtr>
    Status UnlockObject(ObjPtr obj)
    {
        return static_cast<Derived *>(this)->UnlockObjectImpl(obj);
    };

    /**
     * @brief Optional. Invokes the ToString method to display debug info about the policy.
     * @return The object in a string format.
     */
    std::string ToString() const
    {
        return static_cast<Derived const *>(this)->ToStringImpl();
    };

    /**
     * @brief Optional. Performs any checks on the object during the lru Access() code to provide a bypass or no-op.
     * @param[in] obj The object is passed in to perform checks on it.
     * @return True if the object can be bypassed.
     */
    template <typename ObjPtr>
    bool CheckBypassLru(ObjPtr obj) const
    {
        return static_cast<Derived const *>(this)->CheckBypassLruImpl(obj);
    }

    /**
     * @brief Checks a number of conditions within the policy to decide if throttling may be needed to slow
     * down incoming workload.
     * @param[in] numPending The number of pending lru updates that have not completed yet.
     * @param[in] avgSize The average size of the objects currently in this lru.
     * @return True if the code recommends that congestion control is turned on.
     */
    bool IsThrottlingNeeded(uint32_t numPending, uint64_t avgSize) const
    {
        return static_cast<Derived const *>(this)->IsThrottlingNeededImpl(numPending, avgSize);
    };

    /**
     * @brief Queries the policy to see if it can provide the max number of objects that are allowed before soft
     * evictions will happen. If this limit is not possible to determine, returns 0.
     * @return The count of max objects if the policy is able to give it.
     */
    uint32_t GetMaxSoftLimit() const
    {
        return static_cast<Derived const *>(this)->GetMaxSoftLimitImpl();
    }

    /**
     * @brief Optional. Updates the policy to advise that an object got a cache hit. The derived policy only needs to
     * implement this if they have a special action to take for cache hit scenarios on the objects.
     * @param[in] obj The object that has been promoted in the LRU cache.
     * @tparam ObjPtr The object type in the cache.
     */
    template <typename ObjPtr>
    void ObjHitImpl(const ObjPtr obj)
    {
        (void)obj;
        return;
    };

    /**
     * @brief This is the default no-op version of the lock function if the use chooses not to derive on this method.
     * @param[in] obj The object is passed in so that the LRU can identify which locks to take.
     * @tparam ObjPtr The object type in the cache.
     * @return Status of the call.
     */
    template <typename ObjPtr>
    Status LockObjectImpl(ObjPtr obj)
    {
        (void)obj;
        return Status::OK();
    };

    /**
     * @brief This is the default no-op version of the unlock function if the use chooses not to derive on this method.
     * @param[in] obj The object is passed in so that the LRU can identify which locks to take.
     * @tparam ObjPtr The object type in the cache.
     * @return Status of the call.
     */
    template <typename ObjPtr>
    Status UnlockObjectImpl(ObjPtr obj)
    {
        (void)obj;
        return Status::OK();
    };

    /**
     * @brief If the derived class does not implement ToStringImpl, display <NA>.
     * @return The object in a string format.
     */
    std::string ToStringImpl() const
    {
        return "<NA>";
    };

    /**
     * @brief This is the default no-op version if the user does not implement it.
     * @param[in] stats A pointer to a vector of integers to update with statistics.
     * @param[in] obj The object to compute stats against when an eviction is attempted on it.
     * @tparam ObjPtr The object type in the cache.
     */
    template <typename ObjPtr>
    void UpdateStatsImpl(std::vector<uint32_t> *stats, const ObjPtr obj)
    {
        (void)stats;
        (void)obj;
        return;
    };

    /**
     * @brief This is the default version if the user does not implement. Always false.
     * @param[in] obj The object is passed in to perform checks on it.
     * @return True if the object can be bypassed.
     */
    template <typename ObjPtr>
    bool CheckBypassLruImpl(ObjPtr obj) const
    {
        (void)obj;
        return false;
    };

    /**
     * @brief Default version if the user does not implmement. Always false.
     * @param[in] numPending The number of pending lru updates that have not completed yet.
     * @param[in] avgSize The average size of objects in this lru.
     * @return True if the code recommends that congestion control is turned on.
     */
    bool IsThrottlingNeededImpl(uint32_t numPending, uint64_t avgSize) const
    {
        (void)numPending;
        (void)avgSize;
        return false;
    };

    /**
     * @brief Queries the policy to see if it can provide the max number of objects that are allowed before soft
     * evictions will happen.  If the user doesn't implement, we default to this one that returns 0.
     * @return The count of max objects if the policy is able to give it.
     */
    uint32_t GetMaxSoftLimitImpl() const
    {
        return 0;
    }

protected:
    uint32_t lowWatermark_;   // The low watermark ratio.
    uint32_t highWatermark_;  // The high watermark ratio.
    static const uint32_t MAX_WATERMARK = 100;
    static const uint32_t MIN_WATERMARK = 0;
    static const uint32_t DFT_LOW_WATERMARK = 60;
    static const uint32_t DFT_HIGH_WATERMARK = 90;

private:
    friend Derived;  // CRTP pattern, see constructor.

    /**
     * @brief A CRTP pattern technique, private constructor prevents the wrong class from being passed to the CRTP
     * template.
     */
    LruPolicyBase() : lowWatermark_(DFT_LOW_WATERMARK), highWatermark_(DFT_HIGH_WATERMARK){};
};

/**
 * Base class for the objects that can be used with the LruSizePolicy class.
 *
 * LruSizePolicy is one of the pre-built cache eviction policies that you can use, derived from the LruPolicyBase.
 * This policy requires that the objects managed by the policy conform to a specific interface as provided here
 * by this class.
 * This base class is using the CRTP design pattern to define the interface that derived classes must follow.
 * @tparam Derived CRTP pattern technique, the derived class is the type for the base class.
 */
template <typename Derived>
class LruSizePolicyObjBase : public LruObjBase {
public:
    /**
     * @brief Default destructor.
     */
    ~LruSizePolicyObjBase() = default;

    /**
     * @brief Fetches the size of the object.
     * @returns The size of the object.
     */
    uint32_t Size() const
    {
        return static_cast<Derived const *>(this)->SizeImpl();
    };

    /**
     * @brief Executes an eviction of this object.
     * @param[in] evictFlags Flags that influence eviction behaviour.
     * @return Status of the call.
     */
    Status Evict(EvictionCtrl evictFlags)
    {
        return static_cast<Derived *>(this)->EvictImpl(evictFlags);
    };

    /**
     * @brief Invokes the ToString (if implemented) for debugging purposes.
     * @return The object in a string format.
     */
    std::string ToString() const
    {
        return static_cast<Derived const *>(this)->ToStringImpl();
    };

    /**
     * @brief If the derived class does not implement ToStringImpl, display <NA>.
     * @return The object in a string format.
     */
    std::string ToStringImpl() const
    {
        return "<NA>";
    };

private:
    friend Derived;  // CRTP pattern, see constructor.

    /**
     * A CRTP pattern technique, private constructor prevents the wrong class from being passed to the CRTP template.
     */
    LruSizePolicyObjBase(int partitionId) : LruObjBase(partitionId) {};
};

/**
 * A size-based implementation of an LRU cache eviction policy.
 *
 * This is one of the pre-built caching policies that can be used. It serves as an example of how a custom LRU cache
 * eviction policy can be implemented.  In this case, objects in the policy must derive from the LruSizePolicyObjBase.
 * This specific size-based policy means that the user defines a size (in bytes) that is the maximum allowed amount of
 * space in the cache.  Then, as objects are added or removed from the cache, the bytesize of those objects are tracked
 * in the policy to steer eviction decisions if the cache size is exceeded.
 * As per the CRTP pattern, to become a derived class it inherits from the base using a template parameter of itself.
 */
class LruSizePolicy : public LruPolicyBase<LruSizePolicy> {
public:
    /**
     * @brief Simple constructor does not take any arguments.
     */
    LruSizePolicy() : currentSize_(0), maxSize_(LruSizePolicy::DFT_CACHE_SIZE){};

    /**
     * @brief Default destructor.
     */
    ~LruSizePolicy() = default;

    /**
     * @brief Perform init action.
     * @return Status of the call.
     */
    Status InitImpl() const
    {
        // Fail it if the cache size is too small.
        if (maxSize_ < LruSizePolicy::MIN_CACHE_SIZE) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Cache size policy is too small.");
        }
        return Status::OK();
    };

    /**
     * @brief Defines the max size to use for the sum of all object sizes in the cache.
     * @param[in] size The maximum size allowed for all objects in this cache eviction policy.
     */
    void SetCacheSize(uint32_t size)
    {
        maxSize_ = size;
    };

    /**
     * @brief Creates a cloned copy of this object as a unique ptr.
     * @return The unique ptr to the cloned object.
     */
    std::unique_ptr<LruSizePolicy> cloneImpl() const
    {
        auto clonedPolicy = std::make_unique<LruSizePolicy>();
        clonedPolicy->maxSize_ = this->maxSize_;
        clonedPolicy->currentSize_ = this->currentSize_;
        clonedPolicy->lowWatermark_ = this->lowWatermark_;
        clonedPolicy->highWatermark_ = this->highWatermark_;
        return clonedPolicy;
    }

    /**
     * @brief Updates the policy to advise of the addition of a new object to the LRU cache.
     * @param[in] obj The object that has been added to the LRU cache.
     * @tparam ObjPtr the object type in the cache.
     */
    template <typename ObjPtr>
    void ObjAddImpl(const ObjPtr obj)
    {
        currentSize_ += obj->Size();
    };

    /**
     * @brief Checks whether it is need to evict the cache entries asynchronously.
     * @returns Return true, if the cache capacity is greater than or equal to highWatermark_, otherwise return false.
     */
    bool AboveHighWatermarkImpl() const
    {
        if (maxSize_ == 0) {
            return false;
        }
        return (currentSize_ * MAX_WATERMARK / maxSize_) >= highWatermark_;
    };

    /**
     * @brief Checks whether it is need to stop evict the cache entries asynchronously.
     * @returns Return true, if the cache capacity is less than or equal to lowWatermark_, otherwise return false.
     */
    bool BelowLowWatermarkImpl() const
    {
        if (maxSize_ == 0) {
            return false;
        }
        return (currentSize_ * MAX_WATERMARK / maxSize_) <= lowWatermark_;
    };

    /**
     * @brief Checks if there is room in the cache for the object.
     * @param[in] obj The object to test with with the policy.
     * @param[in] cacheMiss A flag to tell us if this check is for a cache hit or miss scenario.
     * @tparam ObjPtr the object type in the cache.
     * @returns Status from the call.  Status returns:
     * K_OK : there was room in the cache.
     * K_FC_HARD_LIMIT : there was no room in the cache and a hard eviction will be needed.
     * K_FC_SOFT_LIMIT : there was room in the cache, but the soft limit was hit and soft eviction can be done.
     */
    template <typename ObjPtr>
    Status RoomInCacheImpl(ObjPtr obj, bool cacheMiss) const
    {
        if (cacheMiss && (currentSize_ + obj->Size()) > maxSize_) {
            RETURN_STATUS(StatusCode::K_FC_HARD_LIMIT, "Lru limit");
        }
        return Status::OK();
    };

    /**
     * @brief Executes an eviction against the given object and updates the policy tracking to reflect the removed
     * object.
     * @param[in] obj The object to evict.
     * @param[in] evictFlags Flags that influence eviction behaviour.
     * @tparam ObjPtr the object type in the cache.
     * @return Status of the call.
     */
    template <typename ObjPtr>
    Status EvictImpl(ObjPtr obj, EvictionCtrl evictFlags)
    {
        uint32_t size_before_evict = obj->Size();
        RETURN_IF_NOT_OK(obj->Evict(evictFlags));
        // If the eviction was clean, now its safe to record the size reduction.
        // We use the saved size because its possible that the evict method did something to change the object.
        currentSize_ -= size_before_evict;
        return Status::OK();
    };

    /**
     * @brief Fetches the policy info to a string for debugging purposes.
     * @return The object in a string format.
     */
    std::string ToStringImpl() const
    {
        return std::string("Current size: ") + std::to_string(currentSize_)
               + " Maximum size: " + std::to_string(maxSize_);
    }

private:
    static const uint32_t DFT_CACHE_SIZE = 8192;  // A default cache size if the user does not set their own size.
    static const uint32_t MIN_CACHE_SIZE = 16;    // A min size.
    uint32_t currentSize_;                        // Current size total of objects accumulated.
    uint32_t maxSize_;                            // Maximum allowed size of all objects.
};

/**
 * Base class for the objects that can be used with the LruCountPolicy class.
 *
 * LruCountPolicy is one of the pre-built cache eviction policies that you can use, derived from the LruPolicyBase.
 * This policy requires that the objects managed by the policy conform to a specific interface as provided here
 * by this class.
 * This base class is using the CRTP design pattern to define the interface that derived classes must follow.
 * @tparam Derived CRTP pattern technique, the derived class is the type for the base class.
 */
template <typename Derived>
class LruCountPolicyObjBase : public LruObjBase {
public:
    /**
     * @brief Default destructor.
     */
    ~LruCountPolicyObjBase() = default;

    /**
     * @brief Executes an eviction of this object.
     * @param[in] soft If set to true will do a soft eviction. This policy only provides hard evictions.
     * @return Status of the call.
     */
    Status Evict(EvictionCtrl evictFlags)
    {
        return static_cast<Derived *>(this)->EvictImpl(evictFlags);
    };

    /**
     * @brief Invokes the ToString (if implemented) for debugging purposes.
     * @return The object in a string format.
     */
    std::string ToString() const
    {
        return static_cast<Derived const *>(this)->ToStringImpl();
    };

    /**
     * @brief If the derived class does not implement ToStringImpl, display <NA>.
     * @return The object in a string format.
     */
    std::string ToStringImpl() const
    {
        return "<NA>";
    };

private:
    friend Derived;  // CRTP pattern, see constructor.

    /**
     * @brief A CRTP pattern technique, private constructor prevents the wrong class from being passed to the CRTP
     * template.
     */
    LruCountPolicyObjBase(int partitionId) : LruObjBase(partitionId) {};
};

/**
 * A count-based implementation of an LRU cache eviction policy.
 *
 * This is one of the pre-built caching policies that can be used. It serves as an example of how a custom LRU cache
 * eviction policy can be implemented.  In this case, objects in the policy must derive from the LruCpimtPolicyObjBase.
 * This specific count-based policy means that the user defines a count that is the maximum allowed amount of items in
 * the cache.  Then, as objects are added or removed from the cache, the counts are tracked in the policy to steer
 * eviction decisions if the cache count is exceeded.
 * As per the CRTP pattern, to become a derived class it inherits from the base using a template parameter of itself.
 */
class LruCountPolicy : public LruPolicyBase<LruCountPolicy> {
public:
    /**
     * @brief Simple constructor does not take any arguments.
     */
    LruCountPolicy() : currentCount_(0), maxCount_(LruCountPolicy::DFT_CACHE_COUNT){};

    /**
     * @brief Default destructor.
     */
    ~LruCountPolicy() = default;

    /**
     * @brief Perform init actions.
     * @return Status of the call.
     */
    Status InitImpl() const
    {
        // Fail it if the cache size is too small.
        if (maxCount_ < LruCountPolicy::MIN_CACHE_COUNT) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Cache count policy is too small.");
        }
        return Status::OK();
    };

    /**
     * @brief Creates a cloned copy of this object as a unique ptr.
     * @return The unique ptr to the cloned object.
     */
    std::unique_ptr<LruCountPolicy> cloneImpl() const
    {
        auto clonedPolicy = std::make_unique<LruCountPolicy>();
        clonedPolicy->maxCount_ = this->maxCount_;
        clonedPolicy->currentCount_ = this->currentCount_.load();
        clonedPolicy->lowWatermark_ = this->lowWatermark_;
        clonedPolicy->highWatermark_ = this->highWatermark_;
        return clonedPolicy;
    }

    /**
     * @brief Defines the max size to use for the sum of all object sizes in the cache.
     * @param[in] size The maximum size allowed for all objects in this cache eviction policy.
     */
    void SetCacheCount(uint32_t count)
    {
        maxCount_ = count;
    };

    /**
     * @brief Updates the policy to advise of the addition of a new object to the LRU cache.
     * @param[in] obj The object that has been added to the LRU cache.
     * @tparam ObjPtr the object type in the cache.
     */
    template <typename ObjPtr>
    void ObjAddImpl(const ObjPtr obj)
    {
        (void)obj;
        currentCount_++;
    };

    /**
     * @brief Checks whether it is need to evict the cache entries asynchronously.
     * @returns Return true, if the cache capacity is greater than or equal to highWatermark_, otherwise return false.
     */
    bool AboveHighWatermarkImpl() const
    {
        if (maxCount_ == 0) {
            return false;
        }
        return (currentCount_ * MAX_WATERMARK / maxCount_) >= highWatermark_;
    };

    /**
     * @brief Checks whether it is need to stop evict the cache entries asynchronously.
     * @returns Return true, if the cache capacity is less than or equal to lowWatermark_, otherwise return false.
     */
    bool BelowLowWatermarkImpl() const
    {
        if (maxCount_ == 0) {
            return false;
        }
        return (currentCount_ * MAX_WATERMARK / maxCount_) <= lowWatermark_;
    };

    /**
     * @brief Checks if there is room in the cache for the object.
     * @param[in] obj The object to test with with the policy.
     * @param[in] cacheMiss A flag to tell us if this check is for a cache hit or miss scenario.
     * @tparam ObjPtr the object type in the cache.
     * @returns Status from the call.  Status returns:
     * K_OK : there was room in the cache.
     * K_FC_HARD_LIMIT : there was no room in the cache and a hard eviction will be needed.
     * K_FC_SOFT_LIMIT : there was room in the cache, but the soft limit was hit and soft eviction can be done.
     */
    template <typename ObjPtr>
    Status RoomInCacheImpl(ObjPtr obj, bool cacheMiss) const
    {
        (void)obj;
        if (cacheMiss && (currentCount_ + 1) > maxCount_) {
            RETURN_STATUS(StatusCode::K_FC_HARD_LIMIT, "Lru limit");
        }
        return Status::OK();
    };

    /**
     * @brief Executes an eviction against the given object and updates the policy tracking to reflect the removed
     * object.
     * @param[in] obj The object to evict.
     * @tparam ObjPtr the object type in the cache.
     * @param[in] evictFlags Flags that influence eviction behaviour.
     * @return Status of the call.
     */
    template <typename ObjPtr>
    Status EvictImpl(ObjPtr obj, EvictionCtrl evictFlags)
    {
        RETURN_IF_NOT_OK(obj->Evict(evictFlags));
        currentCount_--;
        return Status::OK();
    };

    /**
     * @brief Fetches the policy info to a string for debugging purposes.
     * @return The object in a string format.
     */
    std::string ToStringImpl() const
    {
        return std::string("Current count: ") + std::to_string(currentCount_)
               + " Maximum count: " + std::to_string(maxCount_);
    }

private:
    static const uint32_t DFT_CACHE_COUNT = 1000;  // A default cache count if the user does not set their own count.
    static const uint32_t MIN_CACHE_COUNT = 1;     // Min count.
    std::atomic<uint32_t> currentCount_;           // Current count.
    uint32_t maxCount_;                            // Max count.
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LRU_LRU_POLICY_H
