/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: The replica manager define.
 */

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"

#ifndef DATASYSTEM_MASTER_REPLICA_MANAGER_H
#define DATASYSTEM_MASTER_REPLICA_MANAGER_H

namespace datasystem {
namespace object_cache {
class MasterWorkerOCServiceImpl;
class WorkerWorkerOCServiceImpl;
}
struct ReplicaManagerParam {
    std::string dbRootPath;
    std::string currWorkerId;
    std::shared_ptr<AkSkManager> akSkManager;
    EtcdStore *etcdStore;
    std::shared_ptr<PersistenceApi> persistenceApi;
    HostPort masterAddress;
    EtcdClusterManager *etcdCM;
    object_cache::MasterWorkerOCServiceImpl *masterWorkerService;
    object_cache::WorkerWorkerOCServiceImpl *workerWorkerService;
    std::shared_ptr<master::RpcSessionManager> rpcSessionManager;
    bool isOcEnabled;
    bool isScEnabled;
};

struct MetadataManager {
    std::shared_ptr<master::OCMetadataManager> oc;
    std::shared_ptr<master::SCMetadataManager> sc;
    void Shutdown();
};

class ReplicaManager {
public:
    ReplicaManager() = default;

    ReplicaManager(const ReplicaManager &) = delete;
    ReplicaManager &operator=(const ReplicaManager &) = delete;
    ~ReplicaManager();

    /**
     * @brief Init replica manager
     * @param[in] param The parameter for init ReplicaManager
     * @return Status of this call
     */
    Status Init(ReplicaManagerParam param);

    /**
     * @brief Add replica if not exists or switch to the specified replica type.
     * @param[in] dbName The rocksdb name
     * @param[in] type The replica type
     * @return Status of this call
     */
    Status AddOrSwitchTo(const std::string &dbName, ReplicaType type);

    /**
     * @brief Remove the specified replica.
     * @param[in] dbName The rocksdb name
     * @return Status of this call
     */
    Status RemoveReplica(const std::string &dbName);

    /**
     * @brief Get the current worker uuid.
     * @return The current worker uuid.
     */
    const std::string &GetCurrentWorkerUuid()
    {
        return currentWorkerId_;
    }

    /**
     * @brief Get the OcMetadataManager instance.
     * @param[in] dbName The rocksdb name.
     * @param[out] ocMetadataManager The OcMetadataManager instance.
     * @return Status of this call.
     */
    Status GetOcMetadataManager(const std::string &dbName,
                                std::shared_ptr<master::OCMetadataManager> &ocMetadataManager);

    /**
     * @brief Get the ScMetadataManager instance.
     * @param[in] dbName The rocksdb name.
     * @param[out] scMetadataManager The ScMetadataManager instance.
     * @return Status of this call.
     */
    Status GetScMetadataManager(const std::string &dbName,
                                std::shared_ptr<master::SCMetadataManager> &scMetadataManager);

    /**
     * @brief Check whether there are any requests for asynchronously writing metadata to ETCD.
     * @return True if there are unfinished async requests.
     */
    bool HaveAsyncMetaRequest();

    /**
     * @brief Add replica event to quque.
     * @param[in] event The event.
     * @return Status of the call.
     */
    Status EnqueEvent(mvccpb::Event event);

    /**
     * @brief Trying to become the primary replica
     * @param[in] dbName The rocksdb name.
     * @return Status
     */
    Status Election(const std::string &dbName);

    /**
     * @brief Check whether the multi replica enabled.
     * @return true if the multi replica is enabled.
     */
    bool MultiReplicaEnabled();

    /**
     * @brief Get the replica type.
     * @param[in] dbName The rocksdb name.
     * @param[out] replicaType The replica type.
     * @return Status of this call.
     */
    Status GetReplicaType(const std::string &dbName, ReplicaType &replicaType);

    /**
     * @brief Get the replica instance.
     * @param[in] dbName The rocksdb name.

     * @return The replica of dbName.
     */
    Status GetReplica(const std::string &dbName, std::shared_ptr<Replica> &replica);

    /**
     * @brief Shutdown the exists meta manager instance.
     */
    void Shutdown();

    /**
     * @brief Get the primary replica location of specific worker.
     * @param[in] srcWorkerUuid The source worker uuid.
     * @param[out] destWorkerUuid The dest worker uuid.
     * @return Status of this call.
     */
    Status GetPrimaryReplicaLocation(const std::string &srcWorkerUuid, std::string &destWorkerUuid);

    /**
     * @brief Get the primary replica db name of specific worker.
     * @param[in] srcUuid The worker uuid.
     * @param[out] dbNames The db name list.
     * @return Status of this call.
     */
    Status GetPrimaryReplicaDbNames(const std::string &workerUuid, std::vector<std::string> &dbNames);

    /**
     * @brief Get the MasterDevOcManager instance.
     * @param[in] dbName The rocksdb name.
     * @param[out] devOcManager The MasterDevOcManager instance.
     * @return Status of this call.
     */
    Status GetDeviceOcManager(const std::string &dbName, std::shared_ptr<master::MasterDevOcManager> &devOcManager);

    /**
     * @brief Init replica when worker start.
     * @param[in] isRestart Is restart or not.
     * @param[in] clusterInfo The necessary cluster information at startup.
     * @return Status of this call.
     */
    Status InitReplicaForStart(bool isRestart, const ClusterInfo &clusterInfo);

    /**
     * @brief Handle node scale down finish.
     * @param[in] workerUuids
     * @return Status
     */
    Status HandleNodeScaleDownFinish(const std::vector<std::string> &workerUuids);

    /**
     * @brief Handle voluntary scale down finsih change primary replica.
     * @param[in] workerUuid Worker uuid.
     * @return Status
     */
    Status HandleVoluntaryScaleDownFinish(const std::string &workerUuid);

    /**
     * @brief Adjust the location of backup replica.
     * @param[in] dbName The rocksdb name.
     * @param[in] addWorkerUuids The workers need add backup replica.
     * @param[in] delWorkerUuids The workers need delete backup replica.
     * @param[in] oldPrimaryLocation The primary replica location.
     * @return Status of this call.
     */
    Status AdjustReplicaLocationImpl(const std::string &dbName, const std::set<std::string> &addWorkerUuids,
                                     const std::set<std::string> &delWorkerUuids = {},
                                     const std::string &oldPrimaryLocation = "");

    /**
     * @brief Notify worker start election.
     * @param[in] dbName The rocksdb name.
     * @param[in] handler The handler function.
     * @return Status of this call.
     */
    Status NotifyStartElection(const std::string &dbName,
                               std::function<bool(const std::string &, ReplicaGroupPb &)> &&handler);

    template <typename Func>
    Status ApplyForAllMetaManager(Func &&func)
    {
        Status lastRc;
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        for (const auto &iter : metadataManagers_) {
            auto rc = func(iter.first, iter.second);
            lastRc = rc.IsError() ? rc : lastRc;
        }
        return lastRc;
    }

    /**
     * @brief Get the primary replicas in worker.
     * @param[in] workerUuid The worker uuid.
     * @param[out] replicaInfos The replica information.
     */
    void GetPrimaryReplicaInfoInWorker(const std::string &workerUuid, std::map<std::string, std::string> &replicaInfos);

protected:
    /**
     * @brief Subscriber event.
     */
    void SubscribeEvent();

    /**
     * @brief Processes events received from etcd.
     */
    void HandleEvent();

    /**
     * @brief Process single event.
     * @param[in] key The event key.
     * @param[in] value The event value.
     * @param[in] isDelete Is delete event or not.
     * @return Status of this call.
     */
    Status HandleOneEvent(const std::string &key, const std::string &value, bool isDelete);

    /**
     * @brief Processes the replica group update events.
     * @param[in] dbName The rocksdb name.
     * @param[in] replicaGroupPb The replica group proto object.
     * @return Status of this call.
     */
    Status HandleUpdateEvent(const std::string &dbName, const ReplicaGroupPb &replicaGroupPb);

    /**
     * @brief Processes the replica group delete events.
     * @param[in] dbName The rocksdb name.
     * @return Status of this call.
     */
    Status HandleDeleteEvent(const std::string &dbName);

    /**
     * @brief Create the metadata manager instance.
     * @param[in] dbName The rocksdb name.
     * @param[in] objectRocksStore The RocksStore instance for object.
     * @param[in] streamRocksStore The RocksStore instance for stream.
     * @return Status of this call.
     */
    virtual Status CreateMetaManager(const std::string &dbName, RocksStore *objectRocksStore,
                                     RocksStore *streamRocksStore);

    /**
     * @brief Destroy the metadata manager instance.
     * @param[in] dbName The rocksdb name.
     * @return Status of this call.
     */
    virtual Status DestroyMetaManager(const std::string &dbName);

    /**
     * @brief The the delay election task.
     * @param[in] delaySec The delay time in second.
     * @param[in] dbName The rocksdb name.
     * @return Status of this call
     */
    Status AddDelayElectionTask(uint64_t delaySec, const std::string &dbName);

    /**
     * @brief Clear the delay election task.
     * @param[in] dbName The rocksdb name.
     */
    void ClearDelayElectionTask(const std::string &dbName);

    /**
     * @brief Try add primary replica and start rocksdb sync task.
     * @param[in] dbName The rocksdb name.
     * @param[in] primaryNodeId The primary node uuid.
     * @return Status of this call.
     */
    virtual Status TryAddPrimary(const std::string &dbName, const std::string &primaryNodeId);

    /**
     * @brief Save ReplicaGroupPb to etcd.
     * @param[in] dbName The rocksdb name.
     * @param[in] replicaGroupPb The ReplicaGroupPb instance.
     * @return Status of this call.
     */
    Status PutReplicaGroupToEtcd(const std::string &dbName, const ReplicaGroupPb &replicaGroupPb);

    /**
     * @brief Process when node timeout.
     * @param[in] workerAddr The timeout worker address.
     * @return Status of this call.
     */
    Status HandleNodeTimeout(const std::string &workerAddr);

    /**
     * @brief Process when current node timeout.
     * @return Status of this call.
     */
    Status HandleCurrentNodeTimeout();

    /**
     * @brief Process when network recovery.
     * @param[in] workerAddr The worker address.
     * @return Status of this call.
     */
    Status HandleNodeNetworkRecovery(const std::string &workerAddr);

    /**
     * @brief Process when current worker network recovery.
     * @return Status of this call.
     */
    Status HandleCurrentNodeNetworkRecovery();

    /**
     * @brief Try switch primary to it's owner node.
     * @return Status of this call.
     */
    Status TrySwitchPrimaryToOwnerNode();

    /**
     * @brief Check for primary replica location.
     * @return Status of this call.
     */
    Status CheckPrimaryReplicaLocation();

    /**
     * @brief Check for backup replica location.
     * @return Status of this call.
     */
    Status CheckBackupReplicaLocation();

    /**
     * @brief Adjust backup replica location for specific worker.
     * @param[in] workerUuid The worker uuid.
     * @return Status of this call
     */
    Status AdjustBackupReplicaLocation(const std::string &workerUuid);

    /**
     * @brief Check replica location.
     */
    void CheckReplicaLocation();

    /**
     * @brief Create timer task cleanup map.
     */
    void SetCleanMapTask();

    /**
     * @brief Search for invalid mappings in key_with_worker_id_meta_map and clear them.
     * @return Status of this call.
     */
    Status CleanKeyWithWorkerIdMetaMap();

    /**
     * @brief Check whether the mapping of workerUuids is invalid.
     * @param[out] expiredUuids The expired workerUuids.
     * @return Stauts of this call.
     */
    Status CheckMappingExpired(std::set<std::string> &expiredUuids);

    /**
     * @brief Try switch replica.
     * @param[in] dbName The rocksdb name.
     * @return Status of this call.
     */
    Status TrySwitchBackReplica(const std::string &dbName);

    /**
     * @brief Check whether the replica is ready to switch.
     * @param[in] dbName The rocksdb name.
     * @param[out] latestSendSeqNo The latest seq number already send to backup.
     * @return Status of this call.
     */
    Status CheckBeforeSwitchReplica(const std::string &dbName, uint64_t &latestSendSeqNo);

    /**
     * @brief Call after worker finish scaleup.
     * @param[in] workerUuid The scaleup worker uuid.
     * @return Status of this call.
     */
    Status HandleNodeScaleupFinish(const std::string &workerUuid);

    /**
     * @brief CheckNeedAdjustReplicaWhenScaleDown
     * @param [in] removeDbName remove worker uuid
     * @param[out] RemoveWorkerPrimaryReplicaIsLocalNode remove worker primary replica is local node
     * @param[out] RemoveNodeIsCurrentNodeReplica  local node replica is remove worker
     */
    void CheckNeedAdjustReplicaWhenScaleDown(const std::string &removeDbName,
                                             bool &RemoveWorkerPrimaryReplicaIsLocalNode,
                                             bool &RemoveNodeIsCurrentNodeReplica);
    /**
     * @brief Check and remove replica from etcd.
     */
    void CheckNeedRemoveReplica();

    /**
     * @brief Check meta is empty or not
     * @param [in] dbName The rocksdb name.
     * @return ture if the meta is empty.
     */
    bool CheckMetaEmpty(const std::string &dbName);

    /**
     * @brief Get the metadata Manager object
     *
     * @param [in] dbName The rocksdb name.
     * @param [out] metadataManager The metadataManager instance.
     * @return Status of this call.
     */
    Status GetMetadataManager(const std::string &dbName, MetadataManager &metadataManager);

    /**
     * @brief Call function for specific replica.
     * @tparam Func The function type.
     * @param[in] dbName The db name.
     * @param[in] func The fucntion instance.
     * @return Status of this call.
     */
    template <typename Func>
    Status WithReplica(const std::string &dbName, Func &&func)
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        auto iter = replicas_.find(dbName);
        if (iter == replicas_.end()) {
            RETURN_STATUS(K_REPLICA_NOT_READY, FormatString("The replica %s not exists", dbName));
        }
        return func(iter->second.get());
    }

    // the rocksdb root path.
    std::string dbRootPath_;
    // current worker uuid.
    std::string currentWorkerId_;
    std::shared_ptr<AkSkManager> akSkManager_;
    EtcdStore *etcdStore_;
    std::shared_ptr<PersistenceApi> persistenceApi_;
    HostPort masterAddress_;
    EtcdClusterManager *etcdCM_;
    object_cache::MasterWorkerOCServiceImpl *masterWorkerService_;
    object_cache::WorkerWorkerOCServiceImpl *workerWorkerService_;
    // rpc seessin manager for stream
    std::shared_ptr<master::RpcSessionManager> rpcSessionManager_;
    bool isOcEnabled_;
    bool isScEnabled_;
    bool isNewNode_;
    std::unique_ptr<ReplicaRpcChannel> channel_;

    std::shared_timed_mutex mutex_;
    // rocksdb name to ReplicaGroupPb.
    std::unordered_map<std::string, std::pair<std::time_t, ReplicaGroupPb>> replicaGroups_;
    // rocksdb name to primary replica worker uuid.
    std::unordered_map<std::string, std::string> primaryReplicaLocation_;
    // rocksdb name to replica instance.
    std::unordered_map<std::string, std::shared_ptr<Replica>> replicas_;
    // rocksdb name to MetadataManager instance.
    std::unordered_map<std::string, MetadataManager> metadataManagers_;

    std::unique_ptr<Queue<mvccpb::Event>> eventQue_;
    std::unique_ptr<ThreadPool> threadPool_;
    std::atomic<bool> stop_{ false };
    std::unordered_map<std::string, std::unique_ptr<TimerQueue::TimerImpl>> timers_;
    std::unique_ptr<TimerQueue::TimerImpl> cleanTimer_;
    std::function<void()> cleanTask_;
};
}  // namespace datasystem

#endif
