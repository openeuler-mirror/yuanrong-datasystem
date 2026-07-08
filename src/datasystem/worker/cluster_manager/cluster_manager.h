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

#include <cmath>
#include <condition_variable>
#include <list>
#include <limits>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/queue/priority_queue.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"

#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"
#include "datasystem/topology/routing/placement_directory.h"
#include "datasystem/topology/routing/placement_facade.h"

namespace datasystem {
struct ClusterInfo {
    std::vector<std::pair<std::string, std::string>> localHashRing;  // <azName, hashRingPb>
    std::vector<std::pair<std::string, std::string>> workers;        // <addr, nodeMsg>
    int64_t revision = -1;
    bool coordinatorAvailable = true;

    std::string ToString();
};

/**
 * @brief This class registers with an etcd server and provides node state tracking. It executes event handling
 * resulting from topology/state changes within the cluster when notified by the etcd service.
 */
class ClusterManager {
public:
    struct MetaOwnerKeyGroups {
        // Metadata owner grouping is keyed by worker HostPort. The current topology has one worker per HostPort.
        std::unordered_map<HostPort, std::vector<std::string>> groups;
        std::unordered_map<std::string, Status> failures;

        /**
         * @brief Appends failed route keys to a group to preserve legacy empty-master handling.
         * @param[in] masterAddr Destination group. Defaults to empty master.
         */
        void AppendFailuresToGroup(const HostPort &masterAddr = HostPort())
        {
            if (failures.empty()) {
                return;
            }
            auto &keys = groups[masterAddr];
            keys.reserve(keys.size() + failures.size());
            for (const auto &failure : failures) {
                keys.emplace_back(failure.first);
            }
        }
    };

    struct MetaOwnerIndexedKeyGroups {
        std::unordered_map<HostPort, std::vector<std::pair<std::string, size_t>>> groups;
        std::unordered_map<std::string, Status> failures;
    };

    /**
     * @brief Constructor
     */
    ClusterManager(const HostPort &workerAddress, const HostPort &masterAddress,
                   topology::ICoordinationBackend *clusterStore, std::shared_ptr<AkSkManager> akSkManager = nullptr,
                   const int pqSize = 5000);

    ~ClusterManager();

    /**
     * @brief Initialized the cluster management. Contacts etcd server to estasblish lease and initial topology.
     * @param[in] clusterInfo The necessary cluster information at startup.
     * @return Status of the call
     */
    Status Init(const ClusterInfo &clusterInfo, bool startKeepAlive = true);

    /**
     * @brief Enqueue an external cluster store event for the cluster manager background thread.
     * @param[in] event The event to enqueue.
     */
    void EnqueueExternalEvent(topology::CoordinationEvent &&event)
    {
        EnqueEvent(std::move(event));
    }

    /**
     * @brief Shuts down the cluster manager. It is best to call this explicitly rather than rely on destructor codepath
     */
    Status Shutdown();

    /**
     * @brief Set waitPost value to 1 when worker init all services.
     */
    void SetWorkerReady();

    /**
     * @brief Start the etcd keepalive that publishes this worker to peers.
     *
     * Must be called after brpc server is listening so peer workers can
     * immediately reach us when they receive the etcd event.  Separated
     * from Init() because brpc services must be registered before the
     * server starts, and the server must start before the keepalive fires.
     */
    Status StartKeepAlive();

    /**
     * @brief Check worker is scale down.
     * @param[in] workerAddr worker address
     */
    bool CheckWorkerIsScaleDown(const std::string &workerAddr);

    /**
     * @brief Check rpc network status between worker and objectKey's master for multiple ids
     * @param[in] objectKeys Container(Vector or list) of objectkeys
     * @return Status - Success only if all connections are good
     */
    template <class container>
    Status CheckConnection(const container &objectKeys)
    {
        for (std::string objectKey : objectKeys) {
            VLOG(1) << "Check Connection with " << objectKey;
            RETURN_IF_NOT_OK(CheckConnection(objectKey));
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
     * @return Status of the call
     */
    Status CheckConnection(const std::string &objKey);

    /**
     * @brief Check rpc network status between the caller node and target node
     * @param[in] nodeAddr The HostPort of the node to check
     * @param[in] allowDirectoryLag Whether to tolerate a target that is present in routing but not yet confirmed by the
     * local placement directory snapshot.
     * @return Status of the call
     */
    Status CheckConnection(const HostPort &nodeAddr, bool allowDirectoryLag = false);

    /**
     * @brief Check if the current node is the master node of the cluster.
     * @return T/F true if current node is the master
     */
    bool IsCurrentNodeMaster() const
    {
        return masterAddress_ == workerAddress_ || !IsCentralized();
    }

    /**
     * @brief Return the immutable placement directory published by cluster-manager node events.
     * @return Placement directory for R0 request read path.
     */
    std::shared_ptr<topology::IPlacementDirectory> GetPlacementDirectory() const
    {
        return placementDirectory_;
    }

    /**
     * @brief Check whether the node on address needs reconciliation or not. If yes, trigger reconciliation.
     * @param[in] address The address of the querier
     * @param[in] timestamp timestamp of the event triggering reconciliation.
     * @param[in] sync Call OCNotifyWorkerManager::PushMetaToWorker or OCNotifyWorkerManager::AsyncPushMetaToWorker
     */
    Status IfNeedTriggerReconciliation(const HostPort &address, int64_t timestamp, bool sync = false,
                                       bool isDRst = false);

    /**
     * @brief Check whether a key hashes into the given ranges via the placement read path.
     * @param[in] ranges Hash range set. Empty ranges never match.
     * @param[in] objKey Object key, stream name, or other business id to hash.
     * @return True if the hashed key falls inside any range, false otherwise.
     *
     * Backed by the local immutable routing snapshot; replaces the legacy hash-ring predicate.
     */
    bool IsInRange(const worker::HashRange &ranges, const std::string &objKey);

    /**
     * @brief Locate the metadata owner endpoint for one object key via the placement read path.
     * @param[in] objKey Business object key.
     * @param[in] requireAvailableTarget Whether the owner must be READY in the local placement directory.
     * @param[out] masterAddr Resolved owner endpoint address.
     * @return K_OK on success; K_NOT_READY if no routing snapshot or facade is available; K_NOT_FOUND if the owner
     * endpoint is absent; K_RPC_UNAVAILABLE when requireAvailableTarget rejects a non-READY owner.
     *
     * Centralized mode is derived from the cluster manager state. This method only reads local immutable snapshots
     * and must not perform repository/backend IO, CAS, or task scan.
     */
    Status LocateMetaOwner(const std::string &objKey, bool requireAvailableTarget, HostPort &masterAddr);

    /**
     * @brief Evaluate whether the local node should serve or redirect one object key via the placement read path.
     * @param[in] key Business object key.
     * @param[out] newAddr Resolved redirect target address when the action is REDIRECT.
     * @return True when the policy decides to REDIRECT (newAddr filled), false for SERVE_LOCAL or when the local
     * routing state cannot evaluate the request.
     *
     * Serves master metadata response filling. Only reads local immutable snapshots and placement-directory facts;
     * does not scan tasks, wait for topology progress, or access the backend.
     */
    bool EvaluateRedirect(const std::string &key, std::string &newAddr);

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

    /**
     * @brief Groups object keys by metadata owner and keeps original input indexes for each key.
     * @param[in] objectKeys Object keys to route.
     * @return Groups by metadata owner and per-key route failures.
     */
    MetaOwnerIndexedKeyGroups GroupKeysByMetaOwnerWithIndex(const std::vector<std::string> &objectKeys);

    /**
     * @brief Groups object keys by metadata owner.
     * @param[in] objectKeys Object keys to route.
     * @return Groups by metadata owner and per-key route failures.
     */
    MetaOwnerKeyGroups GroupKeysByMetaOwner(const std::vector<std::string> &objectKeys);

    /**
     * @brief Groups object keys by metadata owner.
     * @param[in] objectKeys Object keys to route.
     * @return Groups by metadata owner and per-key route failures.
     */
    MetaOwnerKeyGroups GroupKeysByMetaOwner(const std::unordered_set<std::string> &objectKeys)
    {
        std::vector<std::string> keys(objectKeys.begin(), objectKeys.end());
        return GroupKeysByMetaOwnerImpl(keys);
    }

    /**
     * @brief Get local worker uuid.
     * @return Return the local worker uuid.
     */
    std::string GetLocalWorkerUuid() const;

    /**
     * @brief Get the number of workers in hashring.
     * @param[out] workerNum The number of workers.
     * @return Status of the call.
     */
    Status GetHashRingWorkerNum(int &workerNum) const;

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
     * @brief Get stable active workers that can execute new recovery work.
     * @return The addresses of active and ready workers.
     */
    std::set<std::string> GetActiveWorkersInHashRing() const;

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

    bool IsFirstKeepAliveSent()
    {
        return clusterStore_->IsFirstKeepAliveSent();
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
    Status GetStandbyWorker(std::string &standbyWorker);

    /**
     * @brief Get active workers.
     * @param[in] num Need worker number.
     * @param[out] activeWorkers Active worker list.
     * @return Status of the call.
     */
    Status GetActiveWorkers(uint32_t num, std::vector<std::string> &activeWorkers);

    /**
     * @brief Get the address of next worker.
     * @param[in] workerAddr Worker address.
     * @param[out] nextWorker The address of next worker.
     * @return Status of the call.
     */
    Status GetStandbyWorkerByAddr(const std::string &workerAddr, std::string &nextWorker);

    /**
     * @brief Gets master address for object key and checks if connection is successful
     * @param[in] objKey Object key
     * @param[out] masterAddr for the objectKey
     * @return Status
     */
    Status GetMetaAddress(const std::string &objKey, HostPort &masterAddr);

    /**
     * @brief Gets object keys that master is not connected
     * @param[in] masterAddrs Master address and its object keys
     * @param[out] objectKeys the objectKeys
     */
    void GetObjectKeysFromNotConnectedMaster(const std::unordered_map<HostPort, std::vector<std::string>> &masterAddrs,
                                             std::unordered_set<std::string> &objectKeys);

    /**
     * @brief Gets master address for object key
     * @param[in] objKey Object key
     * @param[out] masterAddr for the objectKey
     * @return Status
     */
    Status GetMetaAddressNotCheckConnection(const std::string &objKey, HostPort &masterAddr);

    /**
     * @brief Locate metadata owners for a batch of keys via the placement read path.
     * @param[in] objectKeys Object keys to route.
     * @param[out] decision Batch route decision.
     * @return Batch-level route status. Per-key failures are stored in decision.
     */
    Status LocateMetaOwnersBatch(const std::vector<std::string> &objectKeys, topology::BatchRouteDecision &decision);

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
     * @param[out] workerId The worker id.
     * @return Status of this call
     */
    Status GetPrimaryReplicaLocationByObjectKey(const std::string &objectKey, HostPort &masterAddr);

    /**
     * @brief Get worker address by uuid.
     * @param[in] address The worker address.
     * @return The workerId, empty if worker not found.
     */
    std::string GetWorkerIdByWorkerAddr(const std::string &address) const;

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

protected:
    /**
     * @brief Private nested class to represent a node in the cluster. ClusterManager will maintain a container
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
        CmEvent(topology::CoordinationEvent &&evt, PrefixType prf) : event(std::move(evt)), prefix(prf)
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
            s << ", event msg: " << event.ToString();
            return s.str();
        }
        topology::CoordinationEvent event;
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
    void EnqueEvent(topology::CoordinationEvent &&event);

    /**
     * @brief Feed priority queue with ETCD event by watch thread.
     * @param[in/out] isHandleEvent Whether the event was handled
     * @return Status
     */
    Status DequeEventCallHandler(bool &isHandleEvent);

    /**
     * @brief Called when etcd event is about /datasystem/ring
     * @param[in] event etcd event
     * @return Status
     */
    Status HandleRingEvent(const topology::CoordinationEvent &event);

    /**
     * @brief Called when etcd event is about /datasystem/cluster
     * @param[in] event etcd event
     * @return Status
     */
    Status HandleClusterEvent(const topology::CoordinationEvent &event);

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
     * @brief A helper function to execute actions related to an etcd cluster node removal event
     * @param eventNodeKey The host port (key) used to identify the event node
     * @param eventNode The node that triggered the event handling
     * @return Status of the call
     */
    Status HandleNodeRemoveEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
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
        bool flushed = false;
        while (!IsCentralized() && hashRing_->IsWorkable() && !tmpClusterEvents_.empty()) {
            RETURN_IF_NOT_OK(HandleClusterEvent(tmpClusterEvents_.front()->event));
            tmpClusterEvents_.pop_front();
            flushed = true;
        }
        if (flushed) {
            PublishPlacementDirectorySnapshot();
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
    bool CheckCoordinatorStateWhenNetworkFailed();

    /**
     * @brief Process if node state is changed to fail.
     * @param[in] addr the addr of node.
     */
    void HandleFailedNode(const HostPort &addr);

    /**
     * @brief Notify slot recovery that workers have been confirmed failed.
     * @param[in] failedWorkers Workers that need slot recovery.
     */
    void NotifySlotRecovery(const std::vector<HostPort> &failedWorkers) const;

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
     * @brief Implements grouping object keys by metadata owner.
     */
    MetaOwnerKeyGroups GroupKeysByMetaOwnerImpl(const std::vector<std::string> &keys);

    /**
     * @brief Resolves one key from a batch route decision.
     */
    Status ResolveBatchRouteForKey(const std::string &objectKey, const Status &batchRc,
                                   const topology::BatchRouteDecision &decision, HostPort &masterAddr) const;

    /**
     * @brief Publish an immutable R0 placement directory snapshot from hash-ring worker facts and cluster node state.
     *
     * This method only reads local memory and swaps a shared pointer. It must not perform RPC probing, repository IO,
     * CAS/List/Watch, task scan, migration, recovery, or cleanup.
     */
    void PublishPlacementDirectorySnapshot();

    using TbbNodeTable = tbb::concurrent_hash_map<HostPort, std::unique_ptr<ClusterNode>, HashCompare>;

    Status HandleExitingNodeRemoveEvent(const HostPort &eventNodeKey, const ClusterNode *eventNode,
                                        ClusterNode *foundNode, TbbNodeTable::const_accessor &accessor);

    HostPort workerAddress_;
    HostPort masterAddress_;
    TbbNodeTable clusterNodeTable_;  // Tracks node states of the cluster nodes

    using TbbOrphanTable = tbb::concurrent_hash_map<std::string, std::string>;
    TbbOrphanTable orphanNodeTable_;
    mutable std::shared_timed_mutex orphanNodeMutex_;  // protect orphanNodeTable_
    WaitPost orphanWaitPost_;                          // wait orphanNodeTable_ is not empty
    std::unique_ptr<Thread> orphanNodeMonitorThread_{ nullptr };

    // The timers that generate fake node removal event, used only in StartNodeUtilThread thread.
    std::unordered_map<std::string, TimerQueue::TimerImpl> nodeTableCompletionTimer_;

    topology::ICoordinationBackend *clusterStore_;
    std::shared_ptr<topology::PlacementDirectory> placementDirectory_;
    std::shared_ptr<topology::IRoutingView> routingView_;
    std::shared_ptr<topology::IPlacementFacade> placementFacade_;
    mutable std::shared_timed_mutex mutex_;  // TbbNodeTable is not threadsafe for iterations
    std::unique_ptr<worker::HashRing> hashRing_{ nullptr };

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

    bool isEtcdAvailableWhenStart_{ true };
};
}  // namespace datasystem
#endif
