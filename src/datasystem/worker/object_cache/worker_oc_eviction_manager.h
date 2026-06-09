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
#include <deque>
#include <future>
#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
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

namespace datasystem {
namespace object_cache {
class AsyncSendManager;
}
}  // namespace datasystem

namespace datasystem {
namespace ut {
class SpillEvictionTest;
}
}  // namespace datasystem

namespace datasystem {

namespace master {
class DeleteAllCopyMetaRspPb;
class MasterOCServiceImpl;
}
class EtcdClusterManager;
namespace object_cache {
using datasystem::EtcdClusterManager;

class WorkerOcEvictionManager : public std::enable_shared_from_this<WorkerOcEvictionManager> {
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
        primaryEndLifeThreadPool_.reset();
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
    Status Init(const std::shared_ptr<ObjectGlobalRefTable<ClientKey>> &gRefTable,
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
     * @brief Setter function to assign the async send manager.
     * @param[in] asyncSendManager The async send manager pointer to assign
     */
    void SetAsyncSendManager(std::shared_ptr<AsyncSendManager> asyncSendManager)
    {
        asyncSendManager_ = asyncSendManager;
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
    enum class Action : int { UNKNOWN, DELETE, FREE_MEMORY, SPILL, END_LIFE, RETAIN, MIGRATE };
    using EvictDeletedObjects = std::unordered_map<std::string, uint64_t>;

    struct EvictionTrace {
        Timer timer;
        std::string taskId;
        uint64_t objectSize;
        std::unordered_map<std::string, uint64_t> objectKeySizeMap;
        Action action;
        std::string info;
        double spillCost;
        Status rc;
        EvictionTrace(std::string id) : taskId(std::move(id)), objectSize(0), action(Action::UNKNOWN), spillCost(0)
        {
        }
        ~EvictionTrace();

        void AddObjectKeySize(const std::string &key, uint64_t size)
        {
            if (size == 0) {
                LOG(WARNING) << "The trace object key [" << key << "] is zero, skip it";
                return;
            }
            auto it = objectKeySizeMap.find(key);
            if (it == objectKeySizeMap.end()) {
                objectKeySizeMap.emplace(key, size);
                objectSize += size;
                return;
            }
            LOG(WARNING) << "The trace object key [" << key << "] is repeated, update it";
            objectSize += size;
            objectSize -= std::min(objectSize, it->second);
            it->second = size;
        }
    };

    struct ActionSummary {
        uint64_t lastLogTimeMs{ 0 };
        std::vector<std::string> successKeys;
        std::vector<std::string> failedKeys;
    };

    struct EvictionTraceAggregator {
        ~EvictionTraceAggregator();

        void Add(const EvictionTrace &trace);

        const std::unordered_map<Action, ActionSummary> &GetSummaries() const;

    private:
        void Flush(Action action, ActionSummary &summary);
        void FlushIfNeeded(Action action, ActionSummary &summary, uint64_t nowMs);

        std::unordered_map<Action, ActionSummary> summaries_;
    };

    struct SpillResult {
        Status rc;
        double elapsed;
        std::vector<std::string> failedKeys;
    };

    enum class TaskType : int { SINGLE, BATCH };

    struct SpillTask {
        TaskType taskType;
        std::future<SpillResult> future;
        std::unique_ptr<EvictionTrace> trace;
    };

    struct PrimaryEndLifeTask {
        std::string objectKey;
        uint64_t version;
        CacheType cacheType;
        uint64_t needSize{ 0 };
        // True when master metadata was already deleted and only local cleanup should be retried.
        bool metaDeleted{ false };
    };

    struct PrimaryEndLifeCandidate {
        PrimaryEndLifeTask task;
        std::shared_ptr<SafeObjType> entry;
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
     */
    Status RemoveMetaFromMasterForEviction(EvictDeletedObjects &objectKeyVersions);

    /**
     * @brief Execute the selected eviction action for a locked object.
     * @param[in] objectKV The object entry that need to evict and its corresponding objectKey.
     * @param[in] nextAction The next action.
     * @return Status of the call.
     */
    Status EvictObject(ObjectKV &objectKV, Action nextAction, EvictDeletedObjects *deletedObjects = nullptr,
                       CacheType cacheType = CacheType::MEMORY, uint64_t needSize = 0);

    /**
     * @brief Run eviction for one locked object and update async spill bookkeeping.
     * @param[in] entry The object entry that need to spill.
     * @param[in] trace The evict trace object.
     * @param[out] pendingSpillSize The size of data to be spill.
     * @param[out] spillTasks The spill task list.
     * @param[out] locked Still locked or not.
     * @return Status
     */
    Status TryEvictObject(std::shared_ptr<SafeObjType> &entry, std::unique_ptr<EvictionTrace> trace,
                          size_t &pendingSpillSize, std::unordered_map<std::string, SpillTask> &spillTasks,
                          bool &locked, CacheType cacheType = CacheType::MEMORY,
                          EvictDeletedObjects *deletedObjects = nullptr, uint64_t needSize = 0);

    /**
     * @brief Eviction task, asynchronous.
     * @param[in] needSize The need size.
     * @param[in] cacheType The type of cache.
     */
    void EvictionTask(uint64_t needSize, CacheType cacheType = CacheType::MEMORY);

    /**
     * @brief Migrate memory data to other workers.
     * @param[in] migrateObjects The list of object keys to be migrated.
     * @param[out] failedMigrateObjectKeys The list of object keys that are failed.
     * @return Status of the call.
     */
    Status MigrateData(const std::string &taskId, const std::unordered_map<std::string, size_t> &migrateObjects,
                       std::vector<std::string> &failedMigrateObjectKeys);

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

    void SubmitAsyncMasterTask(const EvictDeletedObjects &objectKeyVersions);
    void AsyncMasterTask(const EvictDeletedObjects &objectKeyVersions);

    /**
     * @brief Submit a primary-copy END_LIFE object to the async primary end-life lane.
     * @param[in] objectKV The object selected by the memory eviction loop.
     * @param[in] cacheType The cache type being evicted.
     * @param[in] needSize The foreground memory demand that triggered this eviction round.
     * @param[out] accepted Whether this object is newly accepted into the pending set.
     * @return Status of the submit operation.
     */
    Status SubmitPrimaryEndLifeTask(const ObjectKV &objectKV, CacheType cacheType, uint64_t needSize, bool &accepted);

    /**
     * @brief Reserve a primary end-life task in the pending set before enqueueing it.
     * @param[in,out] task The primary end-life task to reserve.
     * @param[out] accepted Whether the task was inserted into the pending set.
     * @return Status of the reservation.
     */
    Status ReservePrimaryEndLifeTask(PrimaryEndLifeTask &task, bool &accepted);

    /**
     * @brief Enqueue a reserved primary end-life task and start the drain worker when needed.
     * @param[in] task The primary end-life task to enqueue.
     * @return Status of the enqueue operation.
     */
    Status EnqueuePrimaryEndLifeTask(const PrimaryEndLifeTask &task);

    /**
     * @brief Remove a primary end-life task from the pending set.
     * @param[in] task The primary end-life task whose pending mark should be cleared.
     */
    void ClearPrimaryEndLifePending(const PrimaryEndLifeTask &task);

    /**
     * @brief Finish a primary end-life task and optionally readd it to the eviction list.
     * @param[in] task The primary end-life task that has finished one lane attempt.
     * @param[in] readd Whether the object should return to the eviction list for a later attempt.
     */
    void FinishPrimaryEndLifeTask(const PrimaryEndLifeTask &task, bool readd);

    /**
     * @brief Readd primary end-life tasks that could not be safely deleted in the lane attempt.
     * @param[in] tasks The skipped or failed tasks to readd.
     */
    void ReaddPrimaryEndLifeTasks(const std::vector<PrimaryEndLifeTask> &tasks);

    /**
     * @brief Drain queued primary end-life tasks until the queue is empty.
     */
    void DrainPrimaryEndLifeTasks();

    /**
     * @brief Pop all currently queued primary end-life tasks as a drain batch.
     * @return A batch of queued primary end-life tasks.
     */
    std::vector<PrimaryEndLifeTask> PopPrimaryEndLifeTasks();

    /**
     * @brief Group primary end-life tasks by current master and process each master batch.
     * @param[in] tasks The tasks popped from the primary end-life queue.
     */
    void ProcessPrimaryEndLifeTasks(std::vector<PrimaryEndLifeTask> tasks);

    /**
     * @brief Process one master batch by deleting remote metadata before local object erase.
     * @param[in] masterAddr The owner master address for this batch.
     * @param[in] tasks The primary end-life tasks routed to the same master.
     */
    void ProcessPrimaryEndLifeMasterBatch(const HostPort &masterAddr, std::vector<PrimaryEndLifeTask> tasks);

    /**
     * @brief Revalidate tasks, acquire object write locks, and select candidates within the release budget.
     * @param[in] tasks The tasks routed to one master.
     * @param[out] candidates Locked candidates that may proceed to DeleteAllCopyMeta.
     * @param[out] skippedTasks Tasks that should be retried by the eviction list later.
     * @return Status of candidate preparation.
     */
    Status PreparePrimaryEndLifeCandidates(const std::vector<PrimaryEndLifeTask> &tasks,
                                           std::vector<PrimaryEndLifeCandidate> &candidates,
                                           std::vector<PrimaryEndLifeTask> &skippedTasks);

    /**
     * @brief Get the object entry and acquire its write lock for a primary end-life attempt.
     * @param[in] task The primary end-life task being prepared.
     * @param[out] entry The locked object entry on success.
     * @return Status of lookup and lock acquisition.
     */
    Status TryLockPrimaryEndLifeTask(const PrimaryEndLifeTask &task, std::shared_ptr<SafeObjType> &entry);

    /**
     * @brief Check whether a locked object is still eligible for primary end-life deletion.
     * @param[in] task The primary end-life task carrying the expected version and cache type.
     * @param[in] entry The locked object entry to validate.
     * @return True if the object can still be deleted by the primary end-life lane.
     */
    bool IsPrimaryEndLifeTaskStillEvictable(const PrimaryEndLifeTask &task, const SafeObjType &entry);

    /**
     * @brief Compute how many bytes this lane may additionally release before reaching the low watermark.
     * @param[in] cacheType The cache type being evicted.
     * @param[in] needSize The foreground memory demand that triggered this eviction round.
     * @return The remaining release budget in bytes.
     */
    uint64_t GetPrimaryEndLifeReleaseBudget(CacheType cacheType, uint64_t needSize);

    /**
     * @brief Delete all-copy metadata for locked primary end-life candidates on one master.
     * @param[in] masterAddr The owner master address for this batch.
     * @param[in] candidates The locked candidates to delete from metadata.
     * @param[out] failedKeys Keys that must not be locally erased and should be readded.
     * @return Status of the metadata delete operation.
     */
    Status DeleteAllCopyMetaForPrimaryEndLife(const HostPort &masterAddr,
                                              const std::vector<PrimaryEndLifeCandidate> &candidates,
                                              std::unordered_set<std::string> &failedKeys);

    /**
     * @brief Collect failed keys from a DeleteAllCopyMeta response, including redirect and moving metadata cases.
     * @param[in] rsp The DeleteAllCopyMeta response.
     * @param[out] failedKeys Keys that should not proceed to local erase.
     * @return Status derived from the response.
     */
    static Status CollectDeleteAllCopyMetaResult(const master::DeleteAllCopyMetaRspPb &rsp,
                                                 std::unordered_set<std::string> &failedKeys);

    /**
     * @brief Cancel the write-back async send task only after local erase succeeded.
     * @param[in] objectKey The object key whose async send task should be canceled.
     */
    void RemovePrimaryEndLifeAsyncSend(const std::string &objectKey);

    /**
     * @brief Erase the locked primary end-life candidate from local disk state and object table.
     * @param[in] candidate The candidate to erase locally.
     * @return Status of local erase.
     */
    Status DeletePrimaryEndLifeLocal(const PrimaryEndLifeCandidate &candidate);

    /**
     * @brief Get the memory release size for a locked primary end-life candidate.
     * @param[in] entry The object entry to inspect.
     * @return Data plus metadata size in bytes, saturated on overflow.
     */
    static uint64_t GetPrimaryEndLifeReleaseSize(const SafeObjType &entry);

    /**
     * @brief Unlock all object entries held by primary end-life candidates.
     * @param[in] candidates The candidates whose entries are write locked.
     */
    static void UnlockPrimaryEndLifeCandidates(const std::vector<PrimaryEndLifeCandidate> &candidates);

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
     * @brief Submit batch spill task to thread pool.
     * @param[in] taskId The batch spill taskId.
     * @param[in] objectKeySizeMap The object key and size map.
     * @return The future of the async thread.
     */
    std::future<WorkerOcEvictionManager::SpillResult> SubmitBatchSpillTask(
        const std::string &taskId, const std::unordered_map<std::string, uint64_t> &objectKeySizeMap);

    /**
     * @brief Batch spill object.
     * @param[in] taskId The batch spill taskId.
     * @param[in] objectKeySizeMap The object key and size map.
     * @param[out] failedKeys The failed keys.
     * @return Status
     */
    Status BatchSpillImpl(const std::string &taskId, const std::unordered_map<std::string, uint64_t> &objectKeySizeMap,
                          std::vector<std::string> &failedKeys);

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

    /**
     * @brief Get spill task id.
     */
    std::string GetSpillTaskId();

    std::shared_ptr<ObjectTable> objectTable_;
    EvictionList memEvictionList_;
    std::unique_ptr<ThreadPool> memEvictTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> primaryEndLifeThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> spillEvictTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> masterTaskThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> spillTaskThreadPool_{ nullptr };
    std::mutex cvMutex_;  // To protect the eviction task
    HostPort localAddress_;
    HostPort masterAddress_;
    std::atomic<bool> isDone_;
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> gRefTable_{ nullptr };
    master::MasterOCServiceImpl *masterOc_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager
    std::unique_ptr<ThreadPool> scheduleEvictThreadPool_{ nullptr };
    std::weak_ptr<AsyncSendManager> asyncSendManager_{};
    std::mutex primaryEndLifeMutex_;
    std::unordered_map<std::string, uint64_t> pendingPrimaryEndLifeObjects_;
    // Tracks metadata-deleted objects whose local cleanup failed and must be retried locally.
    std::unordered_map<std::string, uint64_t> metaDeletedPrimaryEndLifeObjects_;
    std::deque<PrimaryEndLifeTask> primaryEndLifeQueue_;
    bool primaryEndLifeDrainRunning_{ false };
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
