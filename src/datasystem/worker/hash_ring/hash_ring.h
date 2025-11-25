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
 * Description: The consistent hash ring.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_HASH_RING_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_HASH_RING_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/worker/hash_ring/hash_ring_health_check.h"
#include "datasystem/worker/hash_ring/hash_ring_task_executor.h"

namespace datasystem {
namespace worker {
class HashRing {
public:
    /**
     * @brief Create a new HashRing object.
     * @param[in] workerId The worker uuid or ip address and port
     *
     */
    explicit HashRing(std::string workerId, EtcdStore *etcdStore);

    ~HashRing();

    /**
     * @brief Init consistent hash ring.
     * @param[in] isMultiReplicaEnable Used to mark whether multi replica is supported.
     */
    Status InitWithEtcd(bool isMultiReplicaEnable);

    /**
     * @brief Init consistent hash ring during ETCD crash.
     * @param[in] isMultiReplicaEnable Used to mark whether multi replica is supported.
     * @param[in] hashRing The hash ring loaded from rocksdb.
     * @return Status of the call.
     */
    Status InitWithoutEtcd(bool isMultiReplicaEnable, const std::string &hashRing);

    /**
     * @brief When the worker num is equal to HashRingAllocator::defaultHashTokenNum, this function will generate hash
     * tokens and write it to etcd.
     */
    void InspectAndProcessPeriodically();

    /**
     * @brief Return true if the hash ring is in RUNNING state.
     * @return Return true if the hash ring is in RUNNING state.
     */
    bool IsRunning() const
    {
        return state_.load() == RUNNING;
    }

    /**
     * @brief Return true if the hash ring is in INIT state.
     * @return Return true if the hash ring is in INIT state.
     */
    bool IsInit() const
    {
        return state_.load() == INIT;
    }

    /**
     * @brief If the node is voluntary scale down node.
     * @param workerAddr worker address.
     * @return If the node is voluntary scale down node.
     */
    bool IsPreLeaving(const std::string &workerAddr);

    /**
     * @brief If the node state is LEAVING.
     * @param workerAddr worker address.
     * @return If the node state is LEAVING.
     */
    bool IsLeaving(const std::string &workerAddr);

    /**
     * @brief Check local node is update node.
     * @return true if local node is update node.
     */
    bool CheckIsLocalNodeIsUpdate();

    /**
     * @brief Return true if the hash ring is in RUNNING or PRE_RUNNING or PRE_LEAVING state.
     * @return Return true if the hash ring is in RUNNING or PRE_RUNNING or PRE_LEAVING state.
     */
    bool IsWorkable() const
    {
        return state_.load() == RUNNING || state_.load() == PRE_RUNNING || state_.load() == PRE_LEAVING;
    }

    /**
     * @brief Get the primary worker uuid by consistent hash algorithm.
     * @param[in] key Use the key to calculate the uint32 hash value, and then find the first node that is greater than
     * the hash value on the consistent hash ring.
     * @param[out] outWorkerUuid the outWorkerUuid is calc by consistent hash algorithm when enable consistent
     * hash(enable_distribute_master is true and etcd_address is valid);
     * @return Status of the call.
     */
    Status GetPrimaryWorkerUuid(const std::string &key, std::string &outWorkerUuid,
                                std::optional<RouteInfo> &routeInfo) const;

    /**
     * @brief Recovery migrate task when timeout node recovery.
     * @param[in] workerAddr The node address
     */
    void RecoverMigrationTask(const std::string &workerAddr);

    /**
     * @brief Get local worker uuid.
     * @return Return the local worker uuid.
     */
    std::string GetLocalWorkerUuid() const;

    /**
     * @brief Check if receive add node info.
     * @param[in] workerAddr The worker address.
     * @return whether receive add node info.
     */
    bool CheckReceiveMigrateInfo(const std::string &workerAddr);

    /**
     * @brief Check if the hash of objKey is in the given ranges.
     * @param[in] ranges The ranges to be checked.
     * @param[in] objKey ObjKey to be checked.
     * @return Return true if the hash of objKey is in range.
     */
    bool HashInRange(const HashRange &ranges, const std::string &objKey);

    /**
     * @brief Get master address.
     * @param[in] objKey The key of object.
     * @param[out] masterAddr The master address of the object.
     * @return Status of the call.
     */
    Status GetMasterAddr(const std::string &objKey, HostPort &masterAddr);

    /**
     * @brief Get master address.
     * @param[in] objKey The key of object.
     * @param[out] masterUuid The master uuid of the object.
     * @return Status of the call.
     */
    Status GetMasterUuid(const std::string &objKey, std::string &masterUuid);

    /**
     * @brief Called when etcd event is about /datasystem/ring
     * @param[in] event etcd event
     * @param[in] prefix prefix of event
     * @return Status
     */
    Status HandleRingEvent(const mvccpb::Event &event, const std::string &prefix);

    /**
     * @brief Update the hash ring according to newSerializedRingInfo.
     * @param[in] newSerializedRingInfo The serialized hash ring topology information received from etcd,
     * @param[in] version The version of key event.
     * @param[in] forceUpdate Update token map directly and return if true.
     * @return Status of the call.
     */
    Status UpdateRing(const std::string &newSerializedRingInfo, int64_t version, bool forceUpdate = false);

    /**
     * @brief Check if the master of objKey is changed. If yes, return the new master address.
     * @param[in] objKey ObjKey to be checked.
     * @param[out] masterAddr The changed master address.
     * @return Return true if the master of objKey is changed.
     */
    bool NeedRedirect(const std::string &objKey, HostPort &masterAddr);

    /**
     * @brief Check whether this local node restarted or not. We should make sure this method is called
     * after HashRing::Init().
     * @param[out] isRestart
     * @return Status of the call.
     */
    Status IsRestart(bool &isRestart)
    {
        switch (startUpState_.load()) {
            case StartUpState::RESTART:
                isRestart = true;
                break;
            case StartUpState::START:
                isRestart = false;
                break;
            case StartUpState::UNDETERMINED:
                RETURN_STATUS(StatusCode::K_NOT_READY, "HashRing has not finished detecting restart. Try again later.");
        }
        return Status::OK();
    }

    /**
     * @brief Get the number of workers in hashring. For centralized master, return -1.
     * @param[out] workerNum The number of workers in hashring.
     * @param[in] isFromOtherAz If this request comes from a node in another az, there is no need to ensure that the
     * node is in this hash ring
     * @return Status of the call.
     */
    Status GetHashRingWorkerNum(int &workerNum, bool isFromOtherAz = false) const;

    /**
     * @brief Get the workers in hashring.
     * @return The addresses of workers.
     */
    std::set<std::string> GetValidWorkersInHashRing() const;

    /**
     * @brief Get the workers in del_node_info.
     * @return The addresses of workers.
     */
    std::set<std::string> GetWorkersInDelNodeInfo() const;

    /**
     * @brief return true if current worker is new node for initialized cluster.
     */
    bool IsNewNode() const
    {
        return isNewNode_;
    }

    /**
     * @brief remove worker from hash ring
     * @param[in] workerAddr The worker to be removed.
     * @param[in] failWorkers All failed workers to be removed.
     * @return Status of the call.
     */
    Status RemoveWorker(const std::string &workerAddr, const std::unordered_set<std::string> &failWorkers);

    /**
     * @brief remove worker from hash ring
     * @param[in] workers The workers to be removed.
     */
    void RemoveWorkers(const std::unordered_set<std::string> &workers);

    /**
     * @brief Check if the hash of objKey is in the given ranges.
     * @param[in] ranges The ranges to be checked.
     * @param[in] objKey ObjKey to be checked.
     * @return Return true if the hash of objKey is in range.
     */
    bool IsInRange(const HashRange &ranges, const std::string &objKey, const std::string &dbName);

    /**
     * @brief Get hash range stored in cluster manager. It can return empty ranges.
     */
    HashRange GetHashRangeNonBlock()
    {
        std::unique_lock<std::mutex> l(hashRangeMutex_);
        return hashRange_;
    }

    /**
     * @brief Whether the datasystem is deployed with centralized master.
     */
    bool IsCentralized() const
    {
        return !enableDistributedMaster_;
    }

    /**
     * @brief Whether the datasytem support remove faulty master node.
     */
    bool EnableAutoDelDeadNode() const;

    /**
     * @brief Get the address of next worker.
     * @param[in] workerAddr Worker address.
     * @param[out] nextWorker The address of next worker.
     * @return Status of the call.
     */
    Status GetStandbyWorkerByAddr(const std::string &workerAddr, std::string &nextWorker);

    /**
     * @brief Get the address of standby worker.
     * @param[out] standbyWorker The address of standby worker.
     * @return Status of the call.
     */
    Status GetStandbyWorker(std::string &standbyWorker);

    /**
     * @brief Check worker is scale down.
     * @param[in] workerAddr worker address.
     * @return true if worker is scale down
     */
    bool CheckWorkerIsScaleDown(const std::string &workerAddr) const;

    /**
     * @brief Get active workers.
     * @param[in] num Need worker number.
     * @param[out] activeWorkers Active worker list.
     * @return Status of the call.
     */
    Status GetActiveWorkers(uint32_t num, std::vector<std::string> &activeWorkers);

    /**
     * @brief Scale down the worker automatically.
     * @return Status of the call.
     */
    Status VoluntaryScaleDown();

    /**
     * @brief Check status of the scale down process.
     * @return Status of the scale down process.
     */
    bool CheckVoluntaryScaleDown()
    {
        return voluntaryScaleDownDone_;
    }

    /**
     * @brief Check if the voluntary scale down data migration has started.
     * @return true if the scale down data migration has started, false otherwise.
     */
    bool IsDataMigrationStarted()
    {
        return dataMigrationStarted_;
    }

    /**
     * @brief erase worker is migrating data from hashring
     * @param[in] oldRing old hash ring.
     * @param[out] newValue new hash ring value.
     */
    void EraseUnFinishMigrateDataWorker(HashRingPb &oldRing, std::unique_ptr<std::string> &newValue);

    /**
     * @brief Process for scale down finish.
     * @param[in] oldRing Old ring
     * @param[in] newRing New ring
     * @return Status of the call
     */
    Status ProcessForScaleDownFinish(HashRingPb &oldRing, HashRingPb &newRing);

    /**
     * @brief Summarize the ring info.
     * @param[in] ring The HashRingPb object.
     * @return The summarize log string.
     */
    static std::string SummarizeHashRing(const HashRingPb &ring);

    /**
     * @brief Get worker address by uuid.
     * @param[in] workerAddr The worker address.
     * @param[out] uuid The worker uuid.
     * @return Status
     */
    Status GetUuidByWorkerAddr(const std::string &workerAddr, std::string &uuid);

    /**
     * @brief Get worker address by uuid for addressing.
     * @param[in] workerUuid The worker uuid.
     * @param[out] workerAddr The remote worker address.
     * @return Return the local worker uuid.
     */
    Status GetWorkerAddrByUuidForAddressing(const std::string &workerUuid, HostPort &workerAddr);

    /**
     * @brief Get worker address by uuid for multi replica.
     * @param[in] workerUuid The worker uuid.
     * @param[out] workerAddr The remote worker address.
     * @return Return the local worker uuid.
     */
    Status GetWorkerAddrByUuidForMultiReplica(const std::string &workerUuid, HostPort &workerAddr);

    bool CheckVoluntaryTaskExpired(const std::string &taskId)
    {
        return taskExecutor_->CheckTaskExpired(taskId);
    }

    /**
     * @brief Get the next worker in hash ring.
     * @param[in] currWorkerUuid The uuid of worker.
     * @param[out] nextWorkerUuid The uuid of prev worker.
     * @return Status of the call.
     */
    Status GetNextWorker(const std::string &currWorkerUuid, std::string &nextWorkerUuid);

    /**
     * @brief Get the prev worker in hash ring.
     * @param[in] currWorkerUuid The uuid of worker.
     * @param[out] prevWorkerUuid The uuid of prev worker.
     * @return Status of the call.
     */
    Status GetPrevWorker(const std::string &currWorkerUuid, std::string &prevWorkerUuid);

    /**
     * @brief Get uuid of all workers in active state.
     * @param[out] activeDbNames The uuid of workers.
     */
    void GetActiveWorkersDbNames(std::vector<std::string> &activeDbNames);

    using HashFunction = uint32_t (*)(const std::string &);
    static const HashFunction hashFunction_;

    /**
     * @brief Check whether the worker ID belongs to the current AZ. If yes, the worker ID after rehashing is returned.
     * @param[in] oldUuid Original worker ID.
     * @param[out] newUuid The rehashed worker ID.
     * @return Status of the call.
     */
    Status GetUuidInCurrCluster(const std::string &oldUuid, std::string &newUuid, std::optional<RouteInfo> &routeInfo);

    HashRingPb GetHashRingPb()
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        return ringInfo_;
    }

    /**
     * @brief Clean invalid map for workerUuid and workerAddr in hashRing.
     * @param[in] expiredUuids The invalid worker uuids.
     * @return Status of the call.
     */
    Status RemoveExpiredMap(const std::set<std::string> &expiredUuids);

protected:
    enum HashState {
        NO_INIT,      // no need to construct the hash ring, refers to centralized master scenario
        INIT,         // wait for hash ring construction
        RUNNING,      // hash ring works and the current worker is in it.
        PRE_RUNNING,  // hash ring works, but the current worker is not in it yet.
        PRE_LEAVING,  // worker try to scale-down by itself, hash ring works and the current worker is in it.
        FAIL
    };

    /**
     * @brief Get master_address from etcd if the gflag of `master_address` is empty.
     */
    Status InitMasterAddress();

    /**
     * @brief Try get old ring from etcd.
     * @param[out] oldVersionRingVal Get ringpb from etcd ring value.
     * @return Status of the call
     */
    void TryGetOldRing(std::string &oldVersionRingVal);

    /**
     * @brief According to the current ring info to do some init job.
     * @param[in] oldValue The serialized old ring get from etcd.
     * @param[in] newValue The new serialized ring that should be write to etcd.
     * @param[out] retry The flags that used to indicate if or not to retry.
     * @param[in] oldVersionRingVal Get ringpb from etcd ring value.
     * @return Status of the call.
     */
    Status InitRing(const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry,
                    const std::string &oldVersionRingVal);

    /**
     * @brief Return true if the token in ring is enough.
     * @param[in] ring The input hash ring.
     * @return Return true if the token in ring is enough.
     */
    bool HashTokensIsReady(const HashRingPb &ring) const;

    /**
     * @brief Get worker address by uuid.
     * @param[in] workerAddr The worker address.
     * @param[out] uuid The worker uuid.
     * @return Status
     */
    Status GetUuidByWorkerAddrNoLock(const std::string &workerAddr, std::string &uuid) const;

    /**
     * @brief Save hash range of current worker.
     */
    void SaveHashRange();

    /**
     * @brief Add workers to be added into the working hash ring.
     * @param[in] currRing The ring before insert.
     * @return Status of the call.
     */
    Status AddNode(const HashRingPb &currRing);

    /**
     * @brief Process update ring event as a working hash ring.
     * @param[in] oldRing The ring before change.
     * @param[in] newRing The ring after change.
     * @return Status of the call.
     */
    Status ProcessUpdateRingEventIfRunning(const HashRingPb &oldRing, const HashRingPb &newRing);

    /**
     * @brief Process update ring event as a non-working hash ring.
     * @param[in] oldRing The ring before change.
     * @param[in] newRing The ring after change.
     * @return Status of the call.
     */
    Status ProcessUpdateRingEventIfPreparing(const HashRingPb &oldRing, const HashRingPb &newRing);

    /**
     * @brief Try to migrate meta data when hash ring is PRE_LEAVING
     * @param[in] oldRing The ring before change.
     * @param[in] newRing The ring after change.
     */
    void ProcessUpdateRingEventIfLeaving(const HashRingPb &oldRing, const HashRingPb &newRing);

    /**
     * @brief Update the member about hashring
     * @param[in] newRing The ring.
     * @param[in] forceUpdate True if triggered by initialization.
     */
    void UpdateLocalRing(const HashRingPb &newRing, bool forceUpdate);

    /**
     * @brief Update token map
     */
    void UpdateTokenMap();

    /**
     * @brief Update state
     * @param[in] oldRing The ring before change.
     * @param[in] forceUpdate True if triggered by initialization.
     */
    void UpdateLocalState(bool forceUpdate);

    /**
     * @brief Check add node finish when src node failed.
     * @param[in] workerAddr Current processing failed worker.
     * @param[in] failedWorkers All failed workers to be removed.
     * @return If all add node finished.
     */
    bool CheckAllAddNodeFinishWhenSrcFailed(const std::string &workerAddr,
                                            const std::unordered_set<std::string> &failedWorkers);

    /**
     * @brief Try generate hash range for first init or add node.
     * @param[in] func The function to generate hash range.
     */
    void TryGenerateHashRange(int waitTime, std::function<void()> func);

    /**
     * @brief Try to init the hash ring, if the cluster starts up the first time, each worker attempt to assign
     * token ranges in the hash ring for all workers.
     */
    void TryFirstInit();

    /**
     * @brief Try to add nodes into working hash ring.
     */
    void TryAdd();

    /**
     * @brief Change the state and print log.
     * @param[in] newState The new state.
     * @param[in] log Append log if state changed.
     * @return True if the state is modified.
     */
    bool ChangeStateTo(HashState newState, std::string log = "");

    /**
     * @brief Try to add voluntary scale down node info to hash ring.
     */
    void GenerateVoluntaryScaleDownChangingInfo();

    /**
     * @brief Get the workers to be added.
     * @param[in] ring The hash ring to be checked.
     * @return Return the workers to be added.
     */
    std::set<std::string> GetAddingWorkers(const HashRingPb &ring) const;

    /**
     * @brief Get the workers to be leaved.
     * @param[in] ring The hash ring to be checked.
     * @param[out] allScaleDownWorkers The scale down workers.
     * @param[out] runningWorkerSize Size of running worker
     * @return Return the workers to be leaved.
     */
    std::unordered_set<std::string> GetLeavingWorkers(const HashRingPb &ring,
                                                      std::unordered_set<std::string> &allScaleDownWorkers,
                                                      uint32_t &runningWorkerSize) const;

    /**
     * @brief Restore scaling task
     * @param[in] isRestartScenario True if triggered by worker restart.
     */
    void RestoreScalingTaskIfNeeded(bool isRestartScenario);

    /**
     * @brief Get the process worker address for remove worker.
     * @param[in] uuid The uuid of worker.
     * @param[in] failedWorkers The process worker that should be excluded from.
     * @param[out] processWorkerAddrs The addresses of process workers.
     * @return Status of the call.
     */
    Status GetProcessWorkerForRemoveWorker(const std::string &uuid,
                                           const std::unordered_set<std::string> &failedWorkers,
                                           std::set<std::string> &processWorkerAddrs);

    /**
     * @brief check if it's necessary to remove worker from hash ring
     * @param[in] workerAddr The worker to be removed.
     * @param[in] failWorkers All failed workers to be removed.
     * @return True if needed.
     */
    bool NeedToTryRemoveWorker(const std::string &workerAddr, const std::unordered_set<std::string> &failWorkers);

    /**
     * @brief Update hash ring and state if node is restarted
     * @param[in] oldValue  The serialized old ring get from etcd.
     * @param[in] oldRing The old ring get from etcd.
     * @return Status of the call.
     */
    Status UpdateWhenNodeRestart(const std::string &oldValue, const HashRingPb &oldRing);

    /**
     * @brief skip update ring.
     * @param[in] newRing The new ring received from etcd.
     * @param[in] version The version of key event.
     * @param[in] forceUpdate True if triggered by initialization.
     * @return True if no need to process.
     */
    bool SkipUpdateRing(const HashRingPb &newRing, int64_t version, bool forceUpdate);

    /**
     * @brief Call after cluster init
     */
    void ProcessForClusterInit();

    /**
     * @brief Call after scale up finish.
     * @param[in] oldRing The old ring in class member.
     * @param[in] newRing The new ring received from etcd.
     */
    void ProcessForScaleupFinish(const HashRingPb &oldRing, const HashRingPb &newRing);

    /**
     * @brief Get the primary worker by consistent hash algorithm without lock.
     * @param[in] keyHash Find the first node that is greater than the keyHash on the consistent hash ring.
     * @param[out] outWorkerAddr the outWorkerAddr is calc by consistent hash algorithm when enable consistent
     * @return Status of the call.
     */
    Status GetPrimaryWorkerAddrNoLock(uint32_t keyHash, std::string &outWorkerAddr,
                                      std::optional<RouteInfo> &routeInfo) const;

    /**
     * @brief Get the address of standby worker.
     * @param[out] standbyWorker The address of standby worker.
     * @param[out] uuid The worker uuid.
     * @return Status of the call.
     */
    Status GetStandbyWorkerByUuid(const std::string &uuid, std::string &standbyWorker);

    /**
     * @brief Get the address of standby worker excluding the specified ones.
     * @param[in] uuid The worker uuid.
     * @param[in] excludeAddrs The excluding worker address.
     * @param[in] standbyWorker The address of standby worker.
     * @return Status of the call.
     */
    Status GetStandbyWorkerExceptNoLock(const std::string &uuid, const std::unordered_set<std::string> &excludeAddrs,
                                        std::string &standbyWorker);

    /**
     * @brief Get the related worker
     * @param[in] currWorkerUuid The worker uuid.
     * @param[in] states The worker state in hash ring.
     * @param[in] failedWorkers The process worker that should be excluded from.
     * @param[in] getNextNode Get the next or prev node.
     * @param[out] outWorkerUuid The out worker uuid.
     * @param[out] outWorkerAddr The out worker address.
     * @return Status of the call.
     */
    Status GetRelatedWorkerImpl(const std::string &currWorkerUuid, const std::unordered_set<WorkerPb::StatePb> &states,
                                const std::unordered_set<std::string> &failedWorkers, bool getNextNode,
                                std::string &outWorkerUuid, std::string &outWorkerAddr);

    /**
     * @brief Get the worker uuid from update_worker_map.
     * @param[in] ring The hash ring.
     * @return Return the reused uuid.
     */
    std::string GetReusedUuid(HashRingPb &ring) const;

    /**
     * @brief Clear worker map on interval.
     */
    void ClearWorkerMapOnInterval();

    /**
     * @brief Add upgrade range.
     * @param[in] ring The ring.
     * @param[in] workers The workers to process.
     */
    void AddUpgradeRange(HashRingPb &ring, std::set<std::string> &workers);

    /**
     * @brief Get the worker uuid from ring.
     * @param[in] workerAddr The worker address.
     * @param[out] uuid The uuid.
     * @return Status of the call.
     */
    Status GetWorkerUuidFromRing(const std::string &workerAddr, std::string &uuid);

    /**
     * @brief Submit migrate data task if need.
     * @param[in] ring The ring.
     */
    void SubmitMigrateDataTaskIfNeed(const HashRingPb &ring);

    /**
     * @brief Check if workerId is in UpdateWorkerMap
     * @param[in] workerId The workerId.
     * @return T/F
     */
    bool IsInUpdateWorkerMap(const std::string &workerId);

    /**
     * @brief Wait hash ring workable.
     * @return Status of the call.
     */
    Status WaitWorkable();

    /**
     * @brief Get the primary worker by consistent hash algorithm.
     * @param[in] keyHash Find the first node that is greater than the keyHash on the consistent hash ring.
     * @param[out] outWorkerAddr the outWorkerAddr is calc by consistent hash algorithm when enable consistent
     * @return Status of the call.
     */
    Status GetPrimaryWorkerAddr(uint32_t keyHash, std::string &outWorkerAddr) const;

    enum class StartUpState {
        UNDETERMINED,  // Initial state when hashring has not finished Init.
        RESTART,       // The local node restarted.
        START,         // The local started for the first time.
    };

    friend HashRingHealthCheck;
    static constexpr int LOG_LEVEL = 1;
    const std::string MASTER_ADDRESS_KEY = "master_address";
    const std::string workerAddr_;
    std::string workerUuid_;
    EtcdStore *etcdStore_;
    std::atomic<HashState> state_;
    bool enableDistributedMaster_;
    std::atomic<StartUpState> startUpState_{ StartUpState::UNDETERMINED };
    std::atomic<bool> isNewNode_{ false };
    std::unique_ptr<Timer> timer_;
    std::atomic<bool> exitFlag_{ false };
    std::atomic<bool> needVoluntaryScaleDown_{ false };
    std::atomic<bool> dataMigrationStarted_{ false };
    std::atomic<bool> voluntaryScaleDownDone_{ false };
    std::atomic<bool> allWorkersVoluntaryScaleDown_{ false };
    std::atomic<int64_t> baselineModRevisionOfRing_{ 0 };
    int64_t currHashRingVersion_ = 0;
    std::atomic<bool> needForceJoin_{ false };
    std::atomic<bool> isUpdateNode_{ false };
    mutable std::shared_timed_mutex mutex_;  // protect the following variables
    HashRingPb ringInfo_;
    using WorkerAddr = std::string;
    std::map<HashPosition, WorkerAddr> tokenMap_;
    std::map<HashPosition, WorkerAddr> workerUuidHashMap_;
    std::map<std::string, HostPort> workerUuid2AddrMap_;
    std::map<WorkerAddr, std::string> workerAddr2UuidMap_;

    mutable std::mutex hashRangeMutex_;  // for hash range
    HashRange hashRange_;

    bool isMultiReplicaEnable_{ false };

    std::unique_ptr<HashRingTaskExecutor> taskExecutor_;
    std::unique_ptr<HashRingHealthCheck> hashRingHealthCheck_;
};
}  // namespace worker
}  // namespace datasystem
#endif
