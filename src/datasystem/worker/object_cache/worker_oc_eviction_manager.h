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
 * Description: Defines EvictionList and WorkerOcEvictionManager Interface.
 */
#ifndef DATASYSTEM_WORKER_OC_EVICTION_MANAGER_H
#define DATASYSTEM_WORKER_OC_EVICTION_MANAGER_H

#include <cstdint>
#include <future>
#include <list>
#include <memory>
#include <shared_mutex>
#include <utility>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/worker/object_cache/eviction_list.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

static constexpr double HIGH_WATER_FACTOR = 0.8;
static constexpr double LOW_WATER_FACTOR = 0.6;

namespace datasystem {
namespace ut {
class SpillEvictionTest;
}
}  // namespace datasystem

namespace datasystem {
class EtcdClusterManager;
namespace object_cache {
using datasystem::EtcdClusterManager;

class WorkerOcEvictionManager {
public:
    /**
     * @brief Construct WorkerOcEvictionManager.
     * @param[in] objectTable The pointer to a ObjectTable.
     * @param[in] localAddress Address of the worker.
     * @param[in] masterAddress Address of the local master.
     * @param[in] masterOc Pointer to the master object cache service.
     * @param[in] hashRing Pointer to the hash ring implementation.
     */
    WorkerOcEvictionManager(std::shared_ptr<ObjectTable> objectTable, HostPort localAddress, HostPort masterAddress,
                            master::MasterOCServiceImpl *masterOc = nullptr);

    ~WorkerOcEvictionManager()
    {
        LOG(INFO) << "WorkerOcEvictionManager exit";
        // Wait for the thread execution to complete first to avoid releasing other variables.
        memEvictTaskThreadPool_.reset();
        spillEvictTaskThreadPool_.reset();
        spillTaskThreadPool_.reset();
        masterTaskThreadPool_.reset();
    }

    /**
     * @brief Initialize the WorkerOcEvictionManager Object.
     * @param[in] gRefTable Global ref count table.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @return Status of the call.
     */
    Status Init(const std::shared_ptr<ObjectGlobalRefTable<ImmutableString>> &gRefTable,
                std::shared_ptr<AkSkManager> akSkManager);

    /**
     * @brief Add a object to EvictionManager.
     * @param[in] objectKey The objectKey to add.
     */
    void Add(const std::string &objectKey);

    /**
     * @brief Erase a object from EvictionManager.
     * @param[in] objectKey The object to erase.
     */
    void Erase(const std::string &objectKey);

    /**
     * @brief Trigger asynchronous eviction task.
     * @param[in] needSize The need size.
     * @param[in] cacheType The type of cache.
     */
    void Evict(uint64_t needSize = 0, CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Get all object infos (for testing).
     * @param[out] res All objects info in EvictionManager.
     * @param[out] oldest The oldest object in EvictionManager.
     * @return Status of the call.
     */
    Status GetAllObjectsInfo(std::vector<EvictionList::Node> &res, EvictionList::Node &oldest);

    /**
     * @brief Setter function to assign the cluster manager back pointer.
     * @param[in] etcdCM The cluster manager pointer to assign
     */
    void SetClusterManager(EtcdClusterManager *etcdCM)
    {
        etcdCM_ = etcdCM;
    }

    /**
     * @brief Evict clear object.
     * @param[in] objectKV The object that need to evict.
     * @return Status of the call.
     */
    Status EvictClearObject(ObjectKV &objectKV);

    /**
     * @brief Whether evict is running.
     * @return true Evict is running.
     */
    bool IsRunning()
    {
        return !isDone_;
    }

private:
    enum class Action : int { UNKNOWN, DELETE, FREE_MEMORY, SPILL, END_LIFE, RETAIN };

    struct EvictionTrace {
        Timer timer;
        std::string objectKey;
        uint64_t objectSize;
        Action action;
        std::string info;
        double spillCost;
        Status rc;
        EvictionTrace(std::string id) : objectKey(std::move(id)), objectSize(0), action(Action::UNKNOWN), spillCost(0)
        {
        }
        ~EvictionTrace()
        {
            if (rc.GetCode() == K_TRY_AGAIN || rc.GetCode() == K_NOT_READY) {
                return;
            }
            auto elapsed = timer.ElapsedMilliSecond();
            auto actionName = GetActionName(action);
            std::stringstream ss;
            ss << "[ObjectKey " << objectKey << "] ";
            if (!info.empty()) {
                ss << info << ", ";
            }
            ss << "evict action " << actionName << ", total cost " << elapsed << " ms, " << "obj size: " << objectSize;
            if (action == Action::SPILL) {
                ss << "spill cost " << spillCost << " ms, ";
            }
            ss << "status:" << (rc.IsOk() ? "OK" : rc.GetMsg());
            LOG(INFO) << ss.str();
        }
    };

    struct SpillResult {
        Status rc;
        double elapsed;
    };

    struct SpillTask {
        std::future<SpillResult> future;
        std::unique_ptr<EvictionTrace> trace;
    };

    using EvictFailedList = std::vector<std::pair<std::string, uint8_t>>;

    /**
     * @brief Get the evict action name.
     * @param[in] action The evict action.
     * @return The evict action name.
     */
    static std::string GetActionName(Action action);

    /**
     * @brief Remove meta in master.
     * @param[in] objectKey The ID of the object that need to remove meta.
     * @param[in] version The object version.
     * @return The next action in eviction task.
     */
    Status RemoveMetaFromMasterForEviction(const std::string &objectKey, uint64_t version);

    /**
     * @brief Try to evict a single object.
     * @param[in] objectKV The object entry that need to evict and its corresponding objectKey.
     * @param[in] nextAction The next action.
     * @param[out] spilling Whether this object should be spill.
     * @return Status of the call.
     */
    Status EvictObject(ObjectKV &objectKV, Action nextAction, bool &spilling);

    /**
     * @brief Try to evict a single object.
     * @param[in] entry The object entry that need to spill.
     * @param[in] trace The evict trace object.
     * @param[out] pendingSpillSize The size of data to be spill.
     * @param[out] spillTasks The spill task list.
     * @param[out] locked Still locked or not.
     * @return Status
     */
    Status TryEvictObject(std::shared_ptr<SafeObjType> &entry, std::unique_ptr<EvictionTrace> trace,
                          size_t &pendingSpillSize, std::unordered_map<std::string, SpillTask> &spillTasks,
                          bool &locked);

    /**
     * @brief Eviction task, asynchronous.
     * @param[in] needSize The need size.
     * @param[in] caheType The type of cache.
     */
    void EvictionTask(uint64_t needSize, CacheType caheType = CacheType::MEMORY);

    /**
     * @brief Indicate if now is above low water mark.
     * @param[in] needSize The need size in bytes.
     * @param[in] pendingSpillSize The pending spill size in bytes.
     * @return True if now is above low water mark.
     */
    bool IsAboveLowWaterMark(uint64_t needSize, size_t pendingSpillSize, CacheType cacheType);

    /**
     * @brief Get the next action in eviction task for an object.
     * @param[in] entry The object entry that need to get next action.
     * @param[in] trace The evict trace object.
     * @param[in] pendingSpillSize The size of data to be spill.
     */
    void GetObjectNextAction(SafeObjType &entry, std::unique_ptr<EvictionTrace> &trace, size_t pendingSpillSize);

    /**
     * @brief Async task to remove meta from master.
     * @param[in] objectKey The ID of the object that need to remove meta.
     * @param[in] version The object version.
     */
    void AsyncMasterTask(const std::string &objectKey, uint64_t version);

    /**
     * @brief Submit spill task to thread pool.
     * @param[in] objectKey The object key.
     * @param[in] version The object version.
     * @return The future of the async thread.
     */
    std::future<SpillResult> SubmitSpillTask(const std::string &objectKey, uint64_t version);

    /**
     * @brief Spill object to disk.
     * @param[in] objectKey The object key.
     * @param[in] version The object version.
     * @return Status
     */
    Status SpillImpl(const std::string &objectKey, uint64_t version);

    /**
     * @brief Release finished spill task.
     * @param[in/out] spillTasks The spill task list.
     * @param[out] evictFailedIds Object keys that spill failed.
     * @param[in] last Whether to wait for all spill threads to complete.
     * @return The spilled size.
     */
    size_t ReleaseSpillFutures(std::unordered_map<std::string, SpillTask> &spillTasks,
                               std::vector<std::pair<std::string, uint8_t>> &evictFailedIds, bool last);

    /**
     * @brief Submit async evict task to evict spilled objects when spill happen.
     * @param[in] objectSize Object size.
     */
    void TryEvictSpilledObjects(uint64_t objectSize);

    /**
     * @brief Evict spilled obejcts.
     * @param[in] objectSize Object data size.
     */
    void EvictSpilledObjects(uint64_t objectSize);

    /**
     * @brief Indicate the object is evitable or not.
     * @param[in] entry Safe object entry.
     * @return True if object is evictable.
     */
    bool IsSpilledObjectEvictable(const std::shared_ptr<SafeObjType> &entry);

    /**
     * @brief Delete write back/through object if evictable.
     * @param[in] objectKV The object entry that need to evict and its corresponding objectKey.
     * @return Status of the call.
     */
    Status DeleteNoneL2CacheEvictableObject(const ObjectKV &objectKV);

    /**
     * @brief Delete none l2 cache evictable object if evictable.
     * @param[in] objectKV The object entry that need to evict and its corresponding objectKey.
     * @return Status of the call.
     */
    Status DeleteL2CacheEvictableObject(const ObjectKV &objectKV);

    /**
     * @brief Get a object from ObjectTable and lock it.
     * @param[in] objectKey The ID of the object that need to get.
     * @param[out] entry The object entry that need to get.
     * @param[out] evictFailedIds Object keys that cannot be locked temporarily.
     * @return Status of the call.
     */
    Status GetAndLockEntry(const std::string &objectKey, std::shared_ptr<SafeObjType> &entry,
                           EvictFailedList &evictFailedIds);

    /**
     * @brief Get a object from ObjectTable and lock it.
     * @param[in] objectKey The ID of the object that need to get.
     * @param[in] version The object version.
     * @param[in] isWrite Specifies to add a read lock or write lock to the object.
     * @param[out] entryPtr The object entry that need to get.
     * @return Status of the call.
     */
    Status GetAndLockEntry(const std::string &objectKey, uint64_t version, bool isWrite,
                           std::shared_ptr<SafeObjType> &entryPtr);

    /**
     * @brief Check whether an object can do evict.
     * @param[in] objectKV The object entry that need to check and its corresponding objectKey.
     * @return true if object can do evict.
     */
    bool IsObjectEvictable(const ObjectKV &objectKV);

    /**
     * @brief Check whether an object is exists in L2 cache.
     * @param[in] entry The object entry that need to check.
     * @return true if object exists in L2 cache.
     */
    static bool IsObjectExistInL2Cache(const SafeObjType &entry);

    /**
     * @brief Calculates the low water mark for shared memory dynamically.
     * @param[in] cacheType The type of cache.
     * @return the low water mark based on the available shared memory.
     */
    uint64_t GetLowWaterMark(CacheType cacheType = CacheType::MEMORY);

    std::shared_ptr<ObjectTable> objectTable_;
    EvictionList memEvictionList_;
    std::unique_ptr<ThreadPool> memEvictTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> spillEvictTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> masterTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> spillTaskThreadPool_{ nullptr };
    std::mutex cvMutex_;  // To protect the eviction task
    HostPort localAddress_;
    HostPort masterAddress_;
    std::atomic<bool> isDone_;
    std::shared_ptr<ObjectGlobalRefTable<ImmutableString>> gRefTable_{ nullptr };
    master::MasterOCServiceImpl *masterOc_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    friend class ::datasystem::ut::SpillEvictionTest;
};

/**
 * @brief Try to evict when memory size reach high water maker.
 * @param[in] keyInfo The ID of the object need to allocate.
 * @param[in] needSize The size need to allocate.
 * @param[in] evictionManager The class of eviction process.
 * @param[in] type The service type.
 * @param[in] cacheType The type of cache.
 * @return True if eviction is triggered.
 */
bool EvictWhenMemoryExceedThrehold(const std::string &keyInfo, uint64_t needSize,
                                   const std::shared_ptr<WorkerOcEvictionManager> &evictionManager,
                                   ServiceType type = ServiceType::OBJECT, CacheType cacheType = CacheType::MEMORY);

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OC_EVICTION_MANAGER_H
