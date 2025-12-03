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
 * Description: Code to watch for cluster changes
 */

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_CLUSTER_MANGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_CLUSTER_MANGER_H

#include <condition_variable>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/queue/priority_queue.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/worker/hash_ring/read_hash_ring.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"

namespace datasystem {
class ReplicaManager;

struct ClusterInfo {
    std::vector<std::pair<std::string, std::string>> localHashRing;     // <azName, hashRingPb>
    std::vector<std::pair<std::string, std::string>> otherAzHashrings;  // <azName, hashRingPb>
    std::vector<std::pair<std::string, std::string>> workers;           // <addr, nodeMsg>
    std::vector<std::pair<std::string, std::string>> otherAzWorkers;    // <addr, nodeMsg>
    std::vector<std::pair<std::string, std::string>> replicaGroups;     // <replicaId, replicaMsg>
    int64_t revision = -1;
    bool etcdAvailable = true;

    std::string ToString();
};

/**
 * @brief This class registers with an etcd server and provides node state tracking. It executes event handling
 * resulting from topology/state changes within the cluster when notified by the etcd service.
 */
class EtcdClusterManager {
public:
    /**
     * @brief Constructor
     */
    EtcdClusterManager(const HostPort &workerAddress, const HostPort &masterAddress, EtcdStore *etcdDB,
                       std::shared_ptr<AkSkManager> akSkManager = nullptr, ReplicaManager *replicaManager = nullptr,
                       const int pqSize = 5000);

    ~EtcdClusterManager();

    /**
     * @brief Initialized the cluster management. Contacts etcd server to estasblish lease and initial topology.
     * @param[in] clusterInfo The necessary cluster information at startup.
     * @return Status of the call
     */
    Status Init(const ClusterInfo &clusterInfo);

    /**
     * @brief Shuts down the cluster manager. It is best to call this explicitly rather than rely on destructor codepath
     */
    Status Shutdown();

    /**
     * @brief Set waitPost value to 1 when worker init all services.
     */
    void SetWorkerReady();

    /**
     * @brief Check worker is scale down.
     * @param[in] workerAddr worker address
     */
    bool CheckWorkerIsScaleDown(const std::string &workerAddr);

    /**
     * @brief Check rpc network status between worker and objectKey's master for multiple ids
     * @param[in] objectKeys Container(Vector or list) of objectkeys
     * @param[in] allowInOtherAz If the corresponding node can only be found in other clusters, K_OK will be returned if
     * true, and K_NOT_FOUND will be returned if false, but relevant logs will be printed to indicate this scenario.
     * @return Status - Success only if all connections are good
     */
    template <class container>
    Status CheckConnection(const container &objectKeys, bool allowInOtherAz = false)
    {
        for (std::string objectKey : objectKeys) {
            VLOG(1) << "Check Connection with " << objectKey;
            RETURN_IF_NOT_OK(CheckConnection(objectKey, allowInOtherAz));
        }
        return Status::OK();
    }

    bool CheckVoluntaryTaskExpired(const std::string &taskId)
    {
        return hashRing_->CheckVoluntaryTaskExpired(taskId);
    }

    /**
     * @brief Check rpc network status between worker and the given objectKey's master
     * @param[in] objKey The object to identify its master
     * @param[in] allowInOtherAz If the corresponding node can only be found in other clusters, K_OK will be returned if
     * true, and K_NOT_FOUND will be returned if false, but relevant logs will be printed to indicate this scenario.
     * @return Status of the call
     */
    Status CheckConnection(const std::string &objKey, bool allowInOtherAz = false);

    /**
     * @brief Check rpc network status between the caller node and target node
     * @param[in] nodeAddr The HostPort of the node to check
     * @param[in] allowInOtherAz If the corresponding node can only be found in other clusters, K_OK will be returned if
     * true, and K_NOT_FOUND will be returned if false, but relevant logs will be printed to indicate this scenario.
     * @return Status of the call
     */
    Status CheckConnection(const HostPort &nodeAddr, bool allowInOtherAz = false, bool allowNotFound = false);

    /**
     * @brief Check if the current node is the master node of the cluster.
     * @return T/F true if current node is the master
     */
    bool IsCurrentNodeMaster() const
    {
        return masterAddress_ == workerAddress_ || !IsCentralized();
    }

    /**
     * @brief Check rpc network status between the caller node and target node
     * @param[in] nodeAddr The HostPort of the other AZ's node to check
     * @return true if node is connected
     */
    bool CheckIfOtherAzNodeConnected(const HostPort &nodeAddr);

    /**
     * @brief Get redirct meta address
     * @param objKey The key of object
     * @param masterAddr redirect meta address.
     * @return if need to redirect.
     */
    bool NeedRedirect(const std::string &objKey, HostPort &masterAddr);

    /**
     * @brief Check whether the node on address needs reconciliation or not. If yes, trigger reconciliation.
     * @param[in] address The address of the querier
     * @param[in] timestamp timestamp of the event triggering reconciliation.
     * @param[in] sync Call OCNotifyWorkerManager::PushMetaToWorker or OCNotifyWorkerManager::AsyncPushMetaToWorker
     */
    Status IfNeedTriggerReconciliation(const HostPort &address, int64_t timestamp, bool sync = false,
                                       bool isDRst = false);

    /**
     * @brief Check object key hash value in range or check object's uuid in uuids
     * @param[in] objKey Object key.
     * @param[in] ranges Hash range.
     * @param[in] uuids worker uuids.
     * @return object key hash value in range or not.
     */
    bool NeedToClear(const std::string &objKey, const worker::HashRange &ranges, const std::vector<std::string> &uuids);

    /**
     * @brief Check object key hash value in range or not
     * @param[in] ranges Hash range.
     * @param[in] objKey Object key.
     * @return Whether object key hash value in range.
     */
    bool IsInRange(const worker::HashRange &ranges, const std::string &objKey, const std::string &dbName);

    /**
     * @brief Check if a worker node is in the pre-leaving state.
     * @param[in] workerAddr Worker node address.
     * @return Whether the worker is in pre-leaving state.
     */
    bool IsPreLeaving(const std::string &workerAddr);

    /**
     * @brief Ask ETCD for list of address of active nodes.
     * @param[out] nodeAddrs HostPort address nodes
     * @return Status of the call
     */
    Status GetNodeAddrListFromEtcd(std::vector<HostPort> &nodeAddrs);

    /**
     * @brief Returns all of the currently tracked nodes
     * @param[out] nodeAddrs HostPort address of all the nodes
     * @return Status of the call
     */
    Status GetClusterNodeAddresses(std::vector<HostPort> &nodeAddrs);

    /**
     * @brief Get master address.
     * @param[in] objKey The key of object.
     * @param[out] masterAddr The master address of the object.
     * @return Status of the call.
     */
    Status GetMasterAddr(const std::string &objKey, HostPort &masterAddr);

    /**
     * @brief Get local worker hash range in hash ring.
     * @return HashRange (std::vector<std::pair<uint32_t, uint32_t>>).
     */
    worker::HashRange GetHashRangeNonBlock();

    /**
     * @brief Check if receive add node info.
     * @param[in] worekrAddr Add node address.
     */
    bool CheckReceiveMigrateInfo();

    /**
     * @brief Check if this is a new node for cluster.
     * @return Return true if this is new node.
     */
    bool IsNewNode() const
    {
        return hashRing_->IsNewNode();
    }

    /**
     * @brief Check if this is a exiting node for cluster.
     * @return Return true if this is exiting node.
     */
    bool CheckLocalNodeIsExiting()
    {
        return isLeaving_ == true;
    }

    /**
     * @brief Check if voluntary scale-down data migration has started.
     * @return Return true if voluntary scale-down data migration has started.
     */
    bool IsDataMigrationStarted() const
    {
        return hashRing_->IsDataMigrationStarted();
    }

    /**
     * @brief Get failed worker nodes address.
     * @return All failed worker address.
     */
    std::unordered_set<std::string> GetFailedWorkers();

    /**
     * @brief Check whether the size of the node table is equal to the number of running worker in hashring.
     * If not, wait until they are equal or time is out.
     * @return Status
     */
    Status CheckWaitNodeTableComplete();

    /**
     * @brief Wait for nodes to be added to the cluster node table.
     * @return Status of the call.
     */
    Status WaitNodeJoinToTable();

    void GroupObjKeysByMasterHostPort(
        const std::vector<std::string> &objectKeys, const std::optional<std::set<size_t>> &targetIndexs,
        std::unordered_map<MetaAddrInfo, std::vector<std::pair<std::string, size_t>>> &objKeysGrpByMaster,
        std::unordered_map<std::string, std::unordered_set<std::string>> &objKeysUndecidedMaster)
    {
        WorkerId2MetaInfoType workerId2MetaInfo;
        Hash2MetaInfoType hash2MetaInfo;
        bool disableCache = objectKeys.size() == 1;
        if (targetIndexs) {
            for (auto index : *targetIndexs) {
                const auto &objectKey = objectKeys[index];
                MetaAddrInfo metaAddrInfo;
                std::optional<Status> rc;
                FetchDestAddrFromAnywhere(objectKey, workerId2MetaInfo, hash2MetaInfo, rc, metaAddrInfo, disableCache);
                auto &con = objKeysGrpByMaster.try_emplace(std::move(metaAddrInfo)).first->second;
                con.emplace_back(std::make_pair(objectKey, index));
            }
        } else {
            for (size_t i = 0; i < objectKeys.size(); i++) {
                const auto &objectKey = objectKeys[i];
                MetaAddrInfo metaAddrInfo;
                std::optional<Status> rc;
                FetchDestAddrFromAnywhere(objectKey, workerId2MetaInfo, hash2MetaInfo, rc, metaAddrInfo, disableCache);
                auto &con = objKeysGrpByMaster.try_emplace(std::move(metaAddrInfo)).first->second;
                con.emplace_back(std::make_pair(objectKey, i));
            }
        }
        std::optional<std::unordered_map<std::string, Status>> errInfos;
        ModifyObjKeysGrpByMasterByCheckConnection(objKeysGrpByMaster, errInfos);
        MoveFailedObjKeysFromObjKeysGrpByMaster(objKeysGrpByMaster, objKeysUndecidedMaster);
    }

    /**
     * @brief Groups ObjectKeys by their corresponding worker.
     * @param[in] objectKeys Container(Vector or list) of objectkeys
     * @param[out] objKeysGrpByMaster map with master as key and objectkeys belong to the master as value
     * @param[out] objKeysUndecidedMaster IDs without known master in hash ring
     */
    template <class container>
    void GroupObjKeysByMasterHostPort(
        const container &objectKeys, std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMaster,
        std::unordered_map<std::string, std::unordered_set<std::string>> &objKeysUndecidedMaster)
    {
        objKeysGrpByMaster = GroupObjKeysByMasterHostPort(objectKeys);
        MoveFailedObjKeysFromObjKeysGrpByMaster(objKeysGrpByMaster, objKeysUndecidedMaster);
    }

    /**
     * @brief Groups ObjectKeys by their corresponding worker.
     * @param[in] objectKeys Container(Vector or list) of objectkeys
     * @return map with MetaAddrInfo as key and objectkeys belong to the master as value
     */
    template <class container>
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> GroupObjKeysByMasterHostPort(const container &objectKeys)
    {
        // go through objectKeys and group them by master and db name.
        std::unordered_map<MetaAddrInfo, std::vector<std::string>> objKeysGrpByMaster;
        Timer timer;
        std::optional<std::unordered_map<std::string, Status>> errInfos;
        GroupObjKeysByMasterHostPortWithStatus(objectKeys, objKeysGrpByMaster, errInfos);
        auto elapsedMs = static_cast<uint64_t>(std::round(timer.ElapsedMilliSecond()));
        workerOperationTimeCost.Append("GroupObjKeys", elapsedMs);
        return objKeysGrpByMaster;
    }

    struct TransparentStringHash {
        using is_transparent = void;

        size_t operator()(const std::string &key) const
        {
            return std::hash<std::string>{}(key);
        }

        size_t operator()(const std::string_view &sv) const noexcept
        {
            return std::hash<std::string_view>{}(sv);
        }
    };

    /**
     * @brief Groups ObjectKeys by their corresponding worker.
     * @param[in] objectKeys Container(Vector or list) of objectkeys
     * @param[out] objKeysGrpByMaster map with MetaAddrInfo as key and objectkeys belong to the master as value
     * @param[out] errInfos the error info for objects.
     */
    template <class container>
    void GroupObjKeysByMasterHostPortWithStatus(
        const container &objectKeys, std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMaster,
        std::optional<std::unordered_map<std::string, Status>> &errInfos)
    {
        WorkerId2MetaInfoType workerId2MetaInfo;
        Hash2MetaInfoType hash2MetaInfo;
        bool disableCache = objectKeys.size() == 1;
        // go through objectKeys and group them by master and db name.
        for (const auto &objectKey : objectKeys) {
            MetaAddrInfo metaAddrInfo;
            std::optional<Status> rc;
            rc.emplace();
            FetchDestAddrFromAnywhere(objectKey, workerId2MetaInfo, hash2MetaInfo, rc, metaAddrInfo, disableCache);
            auto &con = objKeysGrpByMaster.try_emplace(std::move(metaAddrInfo)).first->second;
            con.emplace_back(objectKey);
            if (rc->IsOk()) {
                continue;
            }
            if (errInfos) {
                (void)errInfos->emplace(objectKey, *rc);
            }
            VLOG(1) << FormatString("objKey[%s] can not find master, status: %s", objectKey, rc->ToString());
        }
        ModifyObjKeysGrpByMasterByCheckConnection(objKeysGrpByMaster, errInfos);
    }

    /**
     * @brief Get local worker uuid.
     * @return Return the local worker uuid.
     */
    std::string GetLocalWorkerUuid() const
    {
        return hashRing_->GetLocalWorkerUuid();
    }

    /**
     * @brief Get the number of workers in hashring.
     * @param[out] workerNum The number of workers.
     * @return Status of the call.
     */
    Status GetHashRingWorkerNum(int &workerNum, bool withOtherAz = false) const;

    /**
     * @brief Start to voluntary scale down before worker shutdown.
     * @return Status of the call.
     */
    Status VoluntaryScaleDown()
    {
        isLeaving_ = true;
        return hashRing_->VoluntaryScaleDown();
    }

    /**
     * @brief Check the status of async voluntary scale down process.
     */
    bool CheckVoluntaryScaleDown()
    {
        return hashRing_->CheckVoluntaryScaleDown();
    }

    /**
     * @brief Get the workers in hashring.
     * @return The addresses of workers.
     */
    std::set<std::string> GetValidWorkersInHashRing() const;

    /**
     * @brief Get the workers in del_node_info.
     * @return The addresses of workers.
     */
    std::set<std::string> GetWorkersInDelNodeInfo() const
    {
        return hashRing_->GetWorkersInDelNodeInfo();
    }

    /**
     * @brief Whether datasystem is deployed with centralized master.
     */
    bool IsCentralized() const
    {
        return hashRing_->IsCentralized();
    }

    /**
     * @brief Get a pointer to hashring object.
     * @return Return pointer to hashring.
     */
    worker::HashRing *GetHashRing()
    {
        return hashRing_.get();
    }

    /**
     * @brief Check whether ETCD is available when start.
     * @return T/F
     */
    bool IsEtcdAvailableWhenStart()
    {
        return isEtcdAvailableWhenStart_;
    }

    bool IsCreateFirstLease()
    {
        return etcdDB_->IsCreateFirstLease();
    }

    /**
     * @brief Check whether the local node restarted. Call this helper after HashRing::Init() is called.
     * @param[out] isRestart
     * @return Status
     */
    Status IsRestart(bool &isRestart)
    {
        return hashRing_->IsRestart(isRestart);
    }

    /**
     * @brief Get size of cluster node table.
     * @return Table size.
     */
    size_t GetNodeTableSize()
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        return clusterNodeTable_.size();
    }

    /**
     * @brief Get the addresses of nodes in cluster node table.
     * @return The set of nodes' addresses.
     */
    std::set<std::string> GetNodesInTable();

    /**
     * @brief Re-manage cluster nodes after restart by completing the node table with fake node-added event.
     */
    void CompleteNodeTableWithFakeNode();

    /**
     * @brief Get the address of standby worker.
     * @param[out] standbyWorker The address of standby worker.
     * @return Status of the call.
     */
    Status GetStandbyWorker(std::string &standbyWorker)
    {
        return hashRing_->GetStandbyWorker(standbyWorker);
    }

    /**
     * @brief Get active workers.
     * @param[in] num Need worker number.
     * @param[out] activeWorkers Active worker list.
     * @return Status of the call.
     */
    Status GetActiveWorkers(uint32_t num, std::vector<std::string> &activeWorkers)
    {
        return hashRing_->GetActiveWorkers(num, activeWorkers);
    }

    /**
     * @brief Get the address of next worker.
     * @param[in] workerAddr Worker address.
     * @param[out] nextWorker The address of next worker.
     * @return Status of the call.
     */
    Status GetStandbyWorkerByAddr(const std::string &workerAddr, std::string &nextWorker)
    {
        return hashRing_->GetStandbyWorkerByAddr(workerAddr, nextWorker);
    }

    /**
     * @brief Gets master address for object key and checks if connection is successful
     * @param[in] objKey Object key
     * @param[out] metaAddrInfo for the objectKey
     * @return Status
     */
    Status GetMetaAddress(const std::string &objKey, MetaAddrInfo &metaAddrInfo);

    /**
     * @brief Gets master address for object key
     * @param[in] objKey Object key
     * @param[out] metaAddrInfo for the objectKey
     * @return Status
     */
    Status GetMetaAddressNotCheckConnection(const std::string &objKey, MetaAddrInfo &metaAddrInfo,
                                            std::optional<RouteInfo> &routeInfo);

    /**
     * @brief When all reconciliations are done, replace "restart" with "start" in ETCD.
     * @return Return status.
     */
    Status InformEtcdReconciliationDone();

    /**
     * @brief Return the node table in cluster manager in form of string.
     * @return Return a vector of strings, each string for a node.
     */
    std::vector<std::string> ClusterNodeTableToString();

    /**
     * @brief Get the primary replica location by object key.
     * @param[in] objectKey The object key.
     * @param[out] masterAddr The meta address.
     * @param[out] dbName The db name.
     * @return Status of this call
     */
    Status GetPrimaryReplicaLocationByObjectKey(const std::string &objectKey, HostPort &masterAddr,
                                                std::string &dbName);

    /**
     * @brief Get the primary replica location by address.
     * @param[in] address The worker address.
     * @param[out] masterAddr The meta address.
     * @param[out] dbName The db name.
     * @return Status of this call
     */
    Status GetPrimaryReplicaLocationByAddr(const std::string &address, HostPort &masterAddr, std::string &dbName);

    /**
     * @brief Get the primary replica db name of specific worker.
     * @param[in] address The worker address.
     * @param[out] dbNames The db name list.
     * @return Status
     */
    Status GetPrimaryReplicaDbNames(const HostPort &address, std::vector<std::string> &dbNames);

    /**
     * @brief Get worker address by uuid.
     * @param[in] address The worker address.
     * @return The workerId, empty if worker not found.
     */
    std::string GetWorkerIdByWorkerAddr(const std::string &address) const;

    /**
     * @brief Check whether the multi replica enabled.
     * @return true if the multi replica is enabled.
     */
    bool MultiReplicaEnabled();

    /**
     * @brief Query master address in other az using consistent hash algorithm.
     * @param[in] otherAZName The other az name.
     * @param[in] objKey The object key.
     * @param[out] metaAddrInfo The metaAddrInfo for the objectKey.
     * @return Status of the call.
     */
    Status QueryMasterAddrInOtherAz(const std::string &otherAZName, const std::string &objKey,
                                    MetaAddrInfo &metaAddrInfo);

    /**
     * @brief Get node in given other az by hash.
     * @param[in] objKey The object key.
     * @param[in] azName Other az name.
     * @param[out] metaAddrInfo <azName, masterAddr>. The node in given other az.
     * @return Status of the call.
     */
    Status GetNodeInGivenOtherAzByHash(const std::string &objKey, const std::string &azName,
                                       MetaAddrInfo &metaAddrInfo);

    /**
     * @brief Get all nodes in other azs by hash.
     * @param[in] objKey The object key.
     * @param[out] metaAddrInfos <azName, masterAddr>. All nodes in other azs.
     * @param[in] allowNotReady Used to determine whether the request is considered failed if the hash ring of an az is
     * not ready.
     * @return Status of the call.
     */
    Status GetAllNodesInOtherAzsByHash(const std::string &objKey,
                                       std::unordered_map<std::string, MetaAddrInfo> &metaAddrInfos,
                                       bool allowNotReady = false);

    /**
     * @brief Group hash objs in given other az.
     * @param[in] otherAZName Other az name.
     * @param[in] objKeyS The object key.
     * @param[out] objKeysGrpByMaster map with master as key and objectkeys belong to the master as value
     * @param[in] groupFailedObjs Object key of failed grouping.
     * @return Status of the call.
     */
    template <class container>
    Status GroupHashObjsInGivenOtherAz(const std::string &otherAZName, const container &objKeys,
                                       std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMaster,
                                       std::vector<std::string> &groupFailedObjs)
    {
        auto iter = otherAzHashRings_.find(otherAZName);
        CHECK_FAIL_RETURN_STATUS(iter != otherAzHashRings_.end(), K_RUNTIME_ERROR,
                                 "Cannot find hashRing of az: " + otherAZName);
        for (const auto &objKey : objKeys) {
            HostPort masterHostPort;
            std::string dbName;
            auto rc = GetMasterAddrInOtherAzForHashKey(iter, objKey, masterHostPort, dbName);
            if (rc.IsError()) {
                LOG(WARNING) << "Cannot get master addr for obj: " + objKey + " in az: " + otherAZName
                                    + ", rc: " + rc.ToString();
                groupFailedObjs.emplace_back(objKey);
                continue;
            }
            MetaAddrInfo metaAddrInfo;
            metaAddrInfo.SetAddress(masterHostPort);
            metaAddrInfo.SetDbName(dbName);
            metaAddrInfo.MarkMetaIsFromOtherAz();
            auto iter = objKeysGrpByMaster.find(metaAddrInfo);
            if (iter == std::end(objKeysGrpByMaster)) {
                std::vector<std::string> objectKeyList({ objKey });
                objKeysGrpByMaster.insert(std::make_pair(metaAddrInfo, std::move(objectKeyList)));
            } else {
                iter->second.push_back(objKey);
            }
        }
        return Status::OK();
    }

    /**
     * @brief Get other az names.
     * @return Other az names.
     */
    const std::vector<std::string> &GetOtherAzNames() const
    {
        return otherAZNames_;
    }

    /**
     * @brief Get other az name by worker id.
     * @param[in] workerId The worker Id.
     * @return Other az names.
     */
    std::string GetOtherAzNameByWorkerIdInefficient(const std::string &workerId);

    /**
     * @brief Construct cluster info via etcd.
     * @param[in] etcdStore The pointer of etcd store.
     * @param[out] clusterInfo The necessary cluster information at startup.
     * @return Status of the call.
     */
    static Status ConstructClusterInfoViaEtcd(EtcdStore *etcdStore, ClusterInfo &clusterInfo);

    /**
     * @brief Create etcd store table.
     * @param[in] etcdStore The pointer of etcd store.
     * @return Status of the call.
     */
    static Status CreateEtcdStoreTable(EtcdStore *etcdStore);

    /**
     * @brief Retrieves the current worker node's network address
     * @return String representation of the worker node's network address in format: "ip:port"
     */
    std::string GetWorkerAddress() const;

private:
    using WorkerId2MetaInfoType = std::unordered_map<std::string, MetaAddrInfo, TransparentStringHash, std::equal_to<>>;
    using Hash2MetaInfoType = std::pair<std::map<HashPosition, std::pair<Range, MetaAddrInfo>>, int64_t>;

    /**
     * @brief Private nested class to represent a node in the cluster. EtcdClusterManager will maintain a container
     * of these ClusterNodes.
     */
    class ClusterNode;

    /**
     * @brief A small class so that HostPort can be used as the key in a tbb concurrent hash map
     */
    class HashCompare {
    public:
        HashCompare() = default;
        ~HashCompare() = default;
        static size_t hash(const HostPort &address)
        {
            return address.hash();
        }

        static bool equal(const HostPort &address1, const HostPort &address2)
        {
            return (address1 == address2);
        }
    };

    enum class PrefixType { OTHER, RING, CLUSTER };

    /**
     * @brief A wrapper of ETCD event to indicate whether this is hashring-related event.
     */
    struct CmEvent {
        CmEvent() = delete;
        CmEvent(mvccpb::Event &&evt, PrefixType prf) : event(std::forward<mvccpb::Event>(evt)), prefix(prf)
        {
        }

        std::string ToString()
        {
            static auto toString = [](PrefixType type) -> std::string {
                std::string name;
                switch (type) {
                    case PrefixType::CLUSTER:
                        name = "CLUSTER";
                        break;
                    case PrefixType::RING:
                        name = "RING";
                        break;
                    case PrefixType::OTHER:
                        name = "OTHER";
                        break;
                }
                return name;
            };
            std::stringstream s;
            s << "prefix: " << toString(prefix);
            s << ", event msg: " << GetEventMsg(event);
            return s.str();
        }
        mvccpb::Event event;
        PrefixType prefix;
    };

    /**
     * @brief Compare priority of events. hashring-related events have highest priotities.
     */
    struct CmEventCmp {
        int AssignPriority(PrefixType prefix)
        {
            static const int PRIORITY_TWO = 2;
            static const int PRIORITY_ONE = 1;
            static const int PRIORITY_ZERO = 0;
            switch (prefix) {
                case PrefixType::RING:
                    return PRIORITY_TWO;
                case PrefixType::CLUSTER:
                    return PRIORITY_ONE;
                default:
                    return PRIORITY_ZERO;
            }
        }

        bool operator()(const std::unique_ptr<CmEvent> &left, const std::unique_ptr<CmEvent> &right)
        {
            return AssignPriority(left->prefix) < AssignPriority(right->prefix);
        }
    };

    /**
     * @brief Feed priority queue with ETCD event by watch thread.
     * @param[in] event - event from Etcd call back
     */
    void EnqueEvent(mvccpb::Event &&event);

    /**
     * @brief Feed priority queue with ETCD event by watch thread.
     * @param[in/out] isHandleEvent Whether the event was handled
     * @return Status
     */
    Status DequeEventCallHandler(bool &isHandleEvent);

    /**
     * @brief Split AZ Name from eventKey
     * @param[in] key event key
     * @param[out] azName AZ Name
     */
    void TrySplitAzNameFromEventKey(const std::string &key, std::string &azName);

    /**
     * @brief Split AZ Name from eventKey
     * @param[in] key event key
     * @param[out] azName AZ Name
     * @return Status
     */
    Status SplitAzNameFromEventKey(const std::string &key, std::string &azName);

    /**
     * @brief Construct other Az ReadHashRings
     */
    void ConstructOtherAzHashRings();

    /**
     * @brief Called when etcd event is about /datasystem/ring
     * @param[in] event etcd event
     * @return Status
     */
    Status HandleRingEvent(const mvccpb::Event &event);

    /**
     * @brief Called when etcd event is about /datasystem/cluster
     * @param[in] event etcd event
     * @return Status
     */
    Status HandleClusterEvent(const mvccpb::Event &event);

    /**
     * @brief Called when Etcd lease renew fails
     * @param[in] rsp - response from Etcd lease renewal received
     * @param[in] sc - Status code received during lease renewal
     */
    void HandleLeaseError(const EtcdResponse &rsp, const Status &sc);

    /**
     * @brief A helper function to execute actions related to a topology change event that required network recovery.
     * @param recoverNodeKey The host port (key) used to identify the event node
     * @param recoverNode The node that triggered the event handling
     * @return Status of the call
     */
    Status ProcessNetworkRecovery(const HostPort &recoverNodeKey, ClusterNode *recoverNode, ClusterNode *eventNode);

    /**
     * @brief A helper function to execute actions related to an etcd cluster node addition event
     * @param eventNodeKey The host port (key) used to identify the event node
     * @param eventNode The node that triggered the event handling
     * @param foundNode The node of cluster
     * @param isTimeout Whether the node is timeout
     * @return Status of the call
     */
    Status HandleNodeStateToActive(const HostPort &eventNodeKey, const std::unique_ptr<ClusterNode> &eventNode,
                                   ClusterNode *foundNode, bool &isTimeout);

    /**
     * @brief A helper function to execute actions related to an etcd cluster node addition event
     * @param[in] eventNodeKey The host port (key) used to identify the event node
     * @param[in] eventNode The node that triggered the event handling
     * @param[in] azName The AZ where the event occurred
     * @return Status of the call
     */
    Status HandleNodeAdditionEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
                                   const std::string &azName);

    /**
     * @brief Execute actions related to an other AZ's node addition event
     * @param[in] eventNodeKey The host port (key) used to identify the event node
     * @param[in] eventNode The node that triggered the event handling
     * @param[in] azName The AZ where the event occurred
     * @return Status of the call
     */
    Status HandleOtherAzNodeAddEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
                                     const std::string &azName);

    /**
     * @brief A helper function to execute actions related to an etcd cluster node removal event
     * @param eventNodeKey The host port (key) used to identify the event node
     * @param eventNode The node that triggered the event handling
     * @return Status of the call
     */
    Status HandleNodeRemoveEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
                                 const std::string &azName);

    /**
     * @brief Execute actions related to an other AZ's  node removal event
     * @param eventNodeKey The host port (key) used to identify the event node
     * @param eventNode The node that triggered the event handling
     * @return Status of the call
     */
    Status HandleOtherAzNodeRemoveEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
                                        const std::string &azName);

    /**
     * @brief A helper function to execute actions related to a topology change event that dealt with a failed node
     * that is now restarting resulting from an add event
     * @param eventNodeKey The key for the nodes
     * @param eventNode The node that triggered the event handling
     * @param failedNode The failed node within the cluster manager to compare to
     * @return Status of the call
     */
    Status HandleFailedNodeToActive(const HostPort &eventNodeKey, ClusterNode *eventNode, ClusterNode *failedNode);

    /**
     * @brief Starts a background thread that watches the timer for when a timed out node should be demoted to a failed
     * node with the DemoteTimedOutNodes() function, and checks hash ring state to generate hash tokens when the ring
     * is ready with HashRing::InspectAndProcessPeriodically().
     * @return Status of the call
     */
    Status StartNodeUtilThread();

    /**
     * @brief Starts a thread to monitor orphaned nodes.
     * @return Status of the call.
     */
    Status StartOrphanNodeMonitorThread();

    /**
     * @brief Starts background thread.
     * @return Status of the call.
     */
    Status StartBackgroundThread();

    /**
     * @brief Get the to be clean nodes.
     * @param[in] orphanNodes The orphan nodes.
     * @param[out] toBeCleanNodes The to be clean nodes.
     */
    void GetToBeCleanNodes(const std::unordered_map<std::string, std::string> &orphanNodes,
                           std::set<std::pair<std::string, bool>> &toBeCleanNodes);

    /**
     * @brief The function that executes the the check for a timed out node to see if it needs to be demoted to a
     * failed node. Any node that is timed out and meets the criteria for demotion shall have its state changed.
     */
    void DemoteTimedOutNodes();

    /**
     * @brief Complete the node table with fake node-added event.
     */
    void CompleteNodeTableWithFakeNode(const std::string &lackNode);

    /**
     * @brief Re-manage cluster nodes periodically by completing the node table with fake node-added event.
     */
    void ScheduledCheckCompleteNodeTableWithFakeNode();

    /**
     * @brief Display all of the currently tracked nodes
     */
    std::string NodesToString();

    /**
     * @brief Display all of the currently tracked other AZs' nodes
     */
    std::string OtherAzNodesToString();

    /**
     * @brief Helper function to fetch and setup nodes with etcd during Init() call
     * @return Status of the call
     */
    Status SetupInitialClusterNodes(const ClusterInfo &clusterInfo);

    /**
     * @brief A helper function to execute actions related to an addition event of a node not in current list.
     * @param eventNodeKey The host port (key) used to identify the event node
     * @param eventNode The node that triggered the event handling
     * @return Status of the call
     */
    Status AddNewNode(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode);

    /**
     * @brief A helper function to flush the temporary cache of cluster events from ETCD.
     * @return Status of the call
     */
    inline Status FlushTmpClusterEvents()
    {
        while (!IsCentralized() && hashRing_->IsWorkable() && !tmpClusterEvents_.empty()) {
            RETURN_IF_NOT_OK(HandleClusterEvent(tmpClusterEvents_.front()->event));
            tmpClusterEvents_.pop_front();
        }
        return Status::OK();
    }

    /**
     * @brief Wait worker ready if need.
     */
    void WaitWorkerReadyIfNeed();

    /**
     * @brief Check if the worker can be found in the cluster node.
     */
    bool IfFindWorkerInTheClusterNode(HostPort &workerAddress);

    /**
     * @brief Query etcd status from other nodes when a network failure occurs between this node and etcd.
     * @return true if any other node indicates that etcd is writable.
     */
    bool CheckEtcdStateWhenNetworkFailed();

    /**
     * @brief Process if node state is changed to fail.
     * @param[in] addr the addr of node.
     */
    void HandleFailedNode(const HostPort &addr);

    /**
     * @brief Cleanup the resource of worker that has not existed anymore.
     * @param[in] workerAddr the address of worker.
     * @param[in] isFailed true if the node state in clusterNodeTable_ is failed.
     */
    void CleanupWorker(const std::string &workerAddr, bool isFailed);

    /**
     * @brief remove the resource that not exist in hash ring anymore.
     * @param[in] workersInRing the current workers in hashring (include the node that is joining or removing)
     */
    void SyncNodeTableWithHashRing(const std::set<std::string> &workersInRing);

    /**
     * @brief Process GetMetaAddress in case[hasWorkerId = false].
     * @param[in] objKey Object key.
     * @param[out] dbName The dbName.
     * @param[out] masterAddr The address of the master that manages metadata for objKey.
     * @return Status of the call.
     */
    Status ProcessGetMetaAddressByHash(const std::string &objKey, std::string &dbName, HostPort &masterAddr,
                                       std::optional<RouteInfo> &routeInfo);

    /**
     * @brief Process GetMetaAddress in case[cross_az_get_meta_from_worker = true; hasWorkerId = true].
     * @param[in] objKey Object key.
     * @param[in] workerIdInObjKey The workerId in objectKey.
     * @param[out] dbName The dbName.
     * @param[out] masterAddr The address of the master that manages metadata for objKey.
     * @param[out] isFromOtherAz If the master is in other az or not.
     * @return Status of the call.
     */
    Status ProcessGetMetaAddressIfAllowMetaAccessAcrossAZWithWorkerId(const std::string &objKey,
                                                                      const std::string &workerIdInObjKey,
                                                                      std::string &dbName, HostPort &masterAddr,
                                                                      bool &isFromOtherAz,
                                                                      std::optional<RouteInfo> &routeInfo);

    /**
     * @brief Process GetMetaAddress in case[cross_az_get_meta_from_worker = false; hasWorkerId = true].
     * @param[in] workerIdInObjKey The workerId in objectKey.
     * @param[out] dbName The dbName.
     * @param[out] masterAddr The address of the master that manages metadata for objKey.
     * @return Status of the call.
     */
    Status ProcessGetMetaAddressIfNotAllowMetaAccessAcrossAZWithWorkerId(const std::string &workerIdInObjKey,
                                                                         std::string &dbName, HostPort &masterAddr,
                                                                         std::optional<RouteInfo> &routeInfo);

    /**
     * @brief Query master address in other az using consistent hash algorithm.
     * @param[in] iter A pointer of hash ring in other az
     * @param[in] objKey Object key.
     * @param[out] masterHostPort The address of the master that manages metadata for objKey.
     * @param[out] dbName The dbName.
     * @return Status of the call.
     */
    Status GetMasterAddrInOtherAzForHashKey(
        const std::unordered_map<std::string, std::unique_ptr<worker::ReadHashRing>>::iterator &iter,
        const std::string &objKey, HostPort &masterHostPort, std::string &dbName);

    template <typename T>
    void ModifyObjKeysGrpByMasterByCheckConnection(
        std::unordered_map<MetaAddrInfo, std::vector<T>> &objKeysGrpByMaster,
        std::optional<std::unordered_map<std::string, Status>> &errInfos)
    {
        static const auto checkConnectionFunc = [](EtcdClusterManager *ptr,
                                                   const MetaAddrInfo &metaAddrInfo) -> Status {
            const auto &masterAddr = metaAddrInfo.GetAddress();
            if (metaAddrInfo.IsFromOtherAz()) {
                CHECK_FAIL_RETURN_STATUS(ptr->CheckIfOtherAzNodeConnected(masterAddr), K_RPC_UNAVAILABLE,
                                         FormatString("The other az node %s disconnected.", masterAddr.ToString()));
            } else {
                return ptr->CheckConnection(masterAddr);
            }
            return Status::OK();
        };
        auto emptyIt = objKeysGrpByMaster.end();  // Iterator for the key of the target node not found.
        for (auto it = objKeysGrpByMaster.begin(); it != objKeysGrpByMaster.end();) {
            const auto &metaAddrInfo = it->first;
            const auto &objectKeys = it->second;
            if (metaAddrInfo.Empty()) {
                emptyIt = it;
                ++it;
                continue;
            }
            Status rc = checkConnectionFunc(this, metaAddrInfo);
            if (rc.IsOk()) {
                ++it;
                continue;
            }
            if (errInfos) {
                for (const auto &objectKey : objectKeys) {
                    (void)errInfos->emplace(ExtractObjectId(objectKey), rc);
                }
            }
            if (emptyIt == objKeysGrpByMaster.end()) {
                emptyIt = objKeysGrpByMaster.try_emplace(MetaAddrInfo()).first;
            }
            auto &con = emptyIt->second;
            con.insert(con.end(), std::make_move_iterator(it->second.begin()),
                       std::make_move_iterator(it->second.end()));
            it = objKeysGrpByMaster.erase(it);
        }
    }

    template <typename T>
    void MoveFailedObjKeysFromObjKeysGrpByMaster(
        std::unordered_map<MetaAddrInfo, std::vector<T>> &objKeysGrpByMaster,
        std::unordered_map<std::string, std::unordered_set<std::string>> &objKeysUndecidedMaster)
    {
        auto emptyIt = objKeysGrpByMaster.find(MetaAddrInfo());
        if (emptyIt == objKeysGrpByMaster.end()) {
            return;
        }
        for (auto &objKey : emptyIt->second) {
            auto workerId = SplitWorkerIdFromObjecId(ExtractObjectId(objKey));
            auto &con = objKeysUndecidedMaster.try_emplace(std::string(workerId)).first->second;
            (void)con.emplace(ExtractObjectId(std::move(objKey)));
        }
        (void)objKeysGrpByMaster.erase(emptyIt);
    }

    bool IfHitCacheWhenRouting(const std::string &objectKey, WorkerId2MetaInfoType &workerId2MetaInfo,
                               Hash2MetaInfoType &hash2MetaInfo, std::optional<Status> &rc, MetaAddrInfo &metaAddrInfo);
    void ProcessNotHitCacheWhenRouting(const std::string &objectKey, WorkerId2MetaInfoType &workerId2MetaInfo,
                                       Hash2MetaInfoType &hash2MetaInfo, std::optional<Status> &rc,
                                       MetaAddrInfo &metaAddrInfo, bool disableCache);
    void FetchDestAddrFromAnywhere(const std::string &objectKey, WorkerId2MetaInfoType &workerId2MetaInfo,
                                   Hash2MetaInfoType &hash2MetaInfo, std::optional<Status> &rc,
                                   MetaAddrInfo &metaAddrInfo, bool disableCache);
    template <typename Map>
    const typename Map::value_type *FindHetero(const Map &mp, const std::string_view &key)
    {
        size_t bucketCount = mp.bucket_count();
        if (bucketCount == 0) {
            return nullptr;
        }
        size_t h = mp.hash_function()(key);
        size_t b = h % bucketCount;
        for (auto it = mp.begin(b); it != mp.end(b); ++it) {
            if (mp.key_eq()(it->first, key)) {
                return &(*it);
            }
        }
        return nullptr;
    }

    using TbbNodeTable = tbb::concurrent_hash_map<HostPort, std::unique_ptr<ClusterNode>, HashCompare>;
    HostPort workerAddress_;
    HostPort masterAddress_;
    TbbNodeTable clusterNodeTable_;       // Tracks node states of the cluster nodes
    TbbNodeTable otherClusterNodeTable_;  // Tracks node states of the other AZ's cluster nodes
    mutable std::shared_timed_mutex otherClusterNodeMutex_;

    using TbbOrphanTable = tbb::concurrent_hash_map<std::string, std::string>;
    TbbOrphanTable orphanNodeTable_;
    mutable std::shared_timed_mutex orphanNodeMutex_;  // protect orphanNodeTable_
    WaitPost orphanWaitPost_;                          // wait orphanNodeTable_ is not empty
    std::unique_ptr<Thread> orphanNodeMonitorThread_{ nullptr };

    // The timers that generate fake node removal event, used only in StartNodeUtilThread thread.
    std::unordered_map<std::string, TimerQueue::TimerImpl> nodeTableCompletionTimer_;

    EtcdStore *etcdDB_;
    mutable std::shared_timed_mutex mutex_;  // TbbNodeTable is not threadsafe for iterations
    std::unique_ptr<worker::HashRing> hashRing_{ nullptr };

    std::unordered_map<std::string, std::unique_ptr<worker::ReadHashRing>> otherAzHashRings_;

    // Pointer to the timeout-checking thread
    std::unique_ptr<Thread> thread_{ nullptr };
    WaitPost cvLock_;
    std::atomic<bool> exitFlag_{ false };

    std::unique_ptr<PriorityQueue<std::unique_ptr<CmEvent>, CmEventCmp>> eventPq_;
    // A temporary cache of cluster events from ETCD.
    // With distributed master configuration, if a cluster (addition/removal) event comes from ETCD to a node before the
    // hashring is ready, the node cannot process the event. It should fetch the event and cache the event. After the
    // hashring is ready, the node will flush the cache and process all the cached events.
    // Not thread safe. Only the utility thread can access it.
    std::list<std::unique_ptr<CmEvent>> tmpClusterEvents_;

    std::unique_ptr<WaitPost> workerWaitPost_{ nullptr };
    std::string ringPrefix_;
    std::string clusterPrefix_;
    std::atomic<bool> isLeaving_{ false };
    std::shared_ptr<AkSkManager> akSkManager_;
    ReplicaManager *replicaManager_{ nullptr };
    std::vector<std::string> otherAZNames_;

    bool isEtcdAvailableWhenStart_{ true };
};
}  // namespace datasystem
#endif
