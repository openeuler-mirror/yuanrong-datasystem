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
 * Description: Defines the worker service processing main class.
 */
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/cluster_manager/cluster_node.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"

DS_DECLARE_int32(heartbeat_interval_ms);
DS_DECLARE_string(etcd_address);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(add_node_wait_time_s);
DS_DECLARE_string(master_address);
DS_DECLARE_string(other_cluster_names);
DS_DECLARE_string(cluster_name);
DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_bool(auto_del_dead_node);
DS_DEFINE_bool(cross_cluster_get_meta_from_worker, false, "cross az to get metadata from worker");

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace datasystem {
// fake event, only be processed when key not exists in cluster node table.
static const std::string FAKE_NODE_EVENT_VALUE = "0;start";
static constexpr int TOTAL_WAIT_NODE_TABLE_TIME_SEC = 60;  // total time of waiting node table complete.
static constexpr int WAIT_NODE_TABLE_INTERVAL_MS = 10;     // interval of waiting node table complete.
static const std::string ETCD_CLUSTER_SUBSCRIBER = "EtcdClusterManager";

EtcdClusterManager::ClusterNode::ClusterNode(const std::string &timeEpoch, const std::string &additionEventType)
    : timeEpoch_(timeEpoch), additionEventType_(additionEventType), state_(NodeState::ACTIVE)
{
}

bool EtcdClusterManager::ClusterNode::DemoteTimedOutNode()
{
    // Both FLAGS_node_dead_timeout_s and FLAGS_node_timeout_s time from when a node loses contact with ETCD.
    if (state_ == NodeState::TIMEOUT && FLAGS_node_dead_timeout_s > FLAGS_node_timeout_s
        && timeoutStamp_.ElapsedSecond() > (FLAGS_node_dead_timeout_s - FLAGS_node_timeout_s)) {
        state_ = NodeState::FAILED;
        return true;
    }
    return false;
}

EtcdClusterManager::EtcdClusterManager(const HostPort &workerAddress, const HostPort &masterAddress, EtcdStore *etcdDB,
                                       bool multiReplicaEnabled, std::shared_ptr<AkSkManager> akSkManager,
                                       const int pqSize)
    : workerAddress_(workerAddress),
      masterAddress_(masterAddress),
      etcdDB_(etcdDB),
      akSkManager_(std::move(akSkManager)),
      multiReplicaEnabled_(multiReplicaEnabled)
{
    hashRing_ = std::make_unique<worker::HashRing>(workerAddress.ToString(), etcdDB);
    eventPq_ = std::make_unique<PriorityQueue<std::unique_ptr<CmEvent>, CmEventCmp>>(pqSize);
    workerWaitPost_ = std::make_unique<WaitPost>();

    if (!FLAGS_other_cluster_names.empty() && FLAGS_enable_distributed_master) {
        ConstructOtherAzHashRings();
        for (const auto &azName : Split(FLAGS_other_cluster_names, ",")) {
            if (azName != FLAGS_cluster_name) {
                otherAZNames_.emplace_back(azName);
            }
        }
    }
    etcdDB_->SetCheckEtcdStateWhenNetworkFailedHandler(
        std::bind(&EtcdClusterManager::CheckEtcdStateWhenNetworkFailed, this));

    HashRingEvent::SyncClusterNodes::GetInstance().AddSubscriber(
        ETCD_CLUSTER_SUBSCRIBER,
        [this](const std::set<std::string> &workersInRing) { return SyncNodeTableWithHashRing(workersInRing); });
    HashRingEvent::GetFailedWorkers::GetInstance().AddSubscriber(
        ETCD_CLUSTER_SUBSCRIBER,
        [this](std::unordered_set<std::string> &failedWorkers) { failedWorkers = GetFailedWorkers(); });
    HashRingEvent::GetDbPrimaryLocation::GetInstance().AddSubscriber(
        ETCD_CLUSTER_SUBSCRIBER, [this](const std::string &address, HostPort &masterAddr, std::string &dbName) {
            return GetPrimaryReplicaLocationByAddr(address, masterAddr, dbName);
        });
    GetHashRangeNonBlockEvent::GetInstance().AddSubscriber("GET_HASH_RANGE_NON_BLOCK",
                                                           [this](worker::HashRange &range) {
                                                               range = GetHashRangeNonBlock();
                                                               return;
                                                           });

    GetLocalWorkerUuidEvent::GetInstance().AddSubscriber("GET_LOCAL_WORKER_UUID", [this](std::string &workerId) {
        workerId = GetLocalWorkerUuid();
        return;
    });

    HashRingEvent::CheckNeedRedirect::GetInstance().AddSubscriber(
        "NEED_REDIRECT", [this](const std::string &id, HostPort &masterAddr, bool &needRedirect) {
            needRedirect = NeedRedirect(id, masterAddr);
            return;
        });

    EtcdClusterMagagerEvent::QueryMasterAddrInOtherAz::GetInstance().AddSubscriber(
        "QUERY_MASTER_ADDR_IN_OTHER_AZ",
        [this](const std::string &otherAzName, const std::string &objKey, MetaAddrInfo &metaAddrInfo) {
            return QueryMasterAddrInOtherAz(otherAzName, objKey, metaAddrInfo);
        });

    EtcdClusterMagagerEvent::CheckIfOtherAzNodeConnected::GetInstance().AddSubscriber(
        "CHECK_IF_OTHER_AZ_NODE_CONNECTED",
        [this](const HostPort &addr, bool &isConnect) { isConnect = CheckIfOtherAzNodeConnected(addr); });
}

EtcdClusterManager::~EtcdClusterManager()
{
    LOG(INFO) << "EtcdClusterManager exit";
    Status rc = Shutdown();
    if (rc.IsError()) {
        LOG(WARNING) << "Errors from shutdown during destructor. Error ignored: " << rc.ToString();
    }
}

void EtcdClusterManager::ConstructOtherAzHashRings()
{
    for (const auto &azName : Split(FLAGS_other_cluster_names, ",")) {
        if (azName != FLAGS_cluster_name) {
            auto readRing = std::make_unique<worker::ReadHashRing>(azName, workerAddress_.ToString(), etcdDB_);
            (void)otherAzHashRings_.insert(std::make_pair(azName, std::move(readRing)));
        }
    }
}

Status EtcdClusterManager::Shutdown()
{
    HashRingEvent::SyncClusterNodes::GetInstance().RemoveSubscriber(ETCD_CLUSTER_SUBSCRIBER);
    HashRingEvent::GetFailedWorkers::GetInstance().RemoveSubscriber(ETCD_CLUSTER_SUBSCRIBER);
    HashRingEvent::GetDbPrimaryLocation::GetInstance().RemoveSubscriber(ETCD_CLUSTER_SUBSCRIBER);
    GetHashRangeNonBlockEvent::GetInstance().RemoveSubscriber("GET_HASH_RANGE_NON_BLOCK");
    GetLocalWorkerUuidEvent::GetInstance().RemoveSubscriber("GET_LOCAL_WORKER_UUID");
    HashRingEvent::CheckNeedRedirect::GetInstance().RemoveSubscriber("NEED_REDIRECT");
    EtcdClusterMagagerEvent::QueryMasterAddrInOtherAz::GetInstance().RemoveSubscriber(
        "QUERY_MASTER_ADDR_IN_OTHER_AZ");
    EtcdClusterMagagerEvent::CheckIfOtherAzNodeConnected::GetInstance().RemoveSubscriber(
        "CHECK_IF_OTHER_AZ_NODE_CONNECTED");

    // Clean up the node demotion thread if it was running
    if (thread_) {
        exitFlag_ = true;
        cvLock_.Set();
        workerWaitPost_->Set();
        thread_->join();
        thread_.reset();
    }

    if (orphanNodeMonitorThread_) {
        exitFlag_ = true;
        orphanWaitPost_.Set();
        orphanNodeMonitorThread_->join();
        orphanNodeMonitorThread_.reset();
    }

    return Status::OK();
}

void EtcdClusterManager::SetWorkerReady()
{
    workerWaitPost_->Set();
}

Status EtcdClusterManager::SetupInitialClusterNodes(const ClusterInfo &clusterInfo)
{
    // Get existing active nodes
    LOG(INFO) << "Query etcd to identify nodes from local az success. Number of nodes: " << clusterInfo.workers.size();
    for (const auto &node : clusterInfo.workers) {
        // node.first is the HostPort string key.  node.second is the timestamp
        // Enqueue a fake event and let the background thread to handle it.
        if (workerAddress_.ToString() == node.first && clusterInfo.etcdAvailable) {
            continue;
        }
        mvccpb::Event fakeEvent;
        auto fakeKv = fakeEvent.mutable_kv();
        fakeKv->set_key(clusterPrefix_ + "/" + node.first);
        fakeKv->set_value(node.second);
        fakeEvent.set_type(mvccpb::Event_EventType::Event_EventType_PUT);
        LOG(INFO) << "Adding key: " << node.first << " value: " << node.second << " to priority queue.";
        RETURN_IF_NOT_OK(eventPq_->EmplaceBack(new CmEvent(std::move(fakeEvent), PrefixType::CLUSTER)));
    }
    {
        std::lock_guard<std::shared_timed_mutex> lck(otherClusterNodeMutex_);
        LOG(INFO) << "Query etcd to identify nodes from other az success. Number of nodes: "
                  << clusterInfo.otherAzWorkers.size();
        for (const auto &node : clusterInfo.otherAzWorkers) {
            HostPort eventNodeKey;
            if (eventNodeKey.ParseString(node.first).IsError()) {
                LOG(ERROR) << "Fail to parse hostport string " << node.first << " from the other AZ's node.";
                continue;
            }
            auto eventNode = std::make_unique<ClusterNode>(node.second, "start");
            otherClusterNodeTable_.emplace(eventNodeKey, std::move(eventNode));
        }
    }
    LOG(INFO) << "Init other az nodes success, node msg: " << this->OtherAzNodesToString();
    return Status::OK();
}

bool EtcdClusterManager::NeedToClear(const std::string &objKey, const worker::HashRange &ranges,
                                     const std::vector<std::string> &uuids)
{
    std::string uuid;
    if (TrySplitWorkerIdFromObjecId(objKey, uuid).IsOk()) {
        return std::find(uuids.begin(), uuids.end(), uuid) != uuids.end();
    }
    return hashRing_->HashInRange(ranges, objKey);
}

bool EtcdClusterManager::IsInRange(const worker::HashRange &ranges, const std::string &objKey,
                                   const std::string &dbName)
{
    return hashRing_->IsInRange(ranges, objKey, dbName);
}

bool EtcdClusterManager::IsPreLeaving(const std::string &workerAddr)
{
    return hashRing_->IsPreLeaving(workerAddr);
}

Status EtcdClusterManager::GetNodeAddrListFromEtcd(std::vector<HostPort> &nodeAddrs)
{
    std::vector<std::pair<std::string, std::string>> activeNodes;
    RETURN_IF_NOT_OK(etcdDB_->GetAll(ETCD_CLUSTER_TABLE, activeNodes));
    nodeAddrs.clear();
    nodeAddrs.resize(activeNodes.size());
    for (size_t i = 0; i < activeNodes.size(); ++i) {
        RETURN_IF_NOT_OK(nodeAddrs[i].ParseString(activeNodes[i].first));
    }
    CHECK_FAIL_RETURN_STATUS(nodeAddrs.size() == activeNodes.size(), StatusCode::K_RUNTIME_ERROR,
                             "Failed to parse all addresses.");
    return Status::OK();
}

Status EtcdClusterManager::Init(const ClusterInfo &clusterInfo)
{
    LOG(INFO) << "Init etcd cluster manager.";
    auto traceId = Trace::Instance().GetTraceID();
    isEtcdAvailableWhenStart_ = clusterInfo.etcdAvailable;
    etcdDB_->SetEventHandler([this, traceId](mvccpb::Event &&event) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        EnqueEvent(std::move(event));
    });
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        FLAGS_node_dead_timeout_s > FLAGS_node_timeout_s, K_INVALID,
        "The value of node_dead_timeout_s must be greater than the value of node_timeout_s.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        (FLAGS_node_timeout_s * MS_PER_SECOND) > uint32_t(FLAGS_heartbeat_interval_ms), K_INVALID,
        "The value of node_timeout_s must be one thousandth greater than the value of heartbeat_interval_ms.");

    // 2. Get prefix, otherwise the later background thread will have data race on these strings.
    RETURN_IF_NOT_OK(etcdDB_->GetEtcdPrefix(ETCD_RING_PREFIX, ringPrefix_));
    RETURN_IF_NOT_OK(etcdDB_->GetEtcdPrefix(ETCD_CLUSTER_TABLE, clusterPrefix_));

    // 3. Launch the background thread, as the hashring relies on it to become RUNNING. We want to start hashring
    // as early as possible. Also, cluster manager needs this thread to to add nodes to its node table.
    // This thread monitors timed out nodes and demotes them to failed nodes. It also tries to generate hash tokens,
    // to give up reconciliation when there is timeout, and to handle etcd events (ring, node addition, node removal).
    RETURN_IF_NOT_OK(StartBackgroundThread());

    RETURN_IF_NOT_OK(SetupInitialClusterNodes(clusterInfo));

    // 4. Watch for future changes to etcd table under directory /datasystem/cluster and /datasystem/ring)
    // The watch thread will enqueue the events in priority queue and the background thread will fetch the events
    // and handle them.
    RETURN_IF_NOT_OK(etcdDB_->WatchEvents({ { ETCD_RING_PREFIX, "", true, clusterInfo.revision },
                                            { ETCD_CLUSTER_TABLE, "", true, clusterInfo.revision },
                                            { ETCD_REPLICA_GROUP_TABLE, "", true, clusterInfo.revision } }));
    // 5. Since the background and watch threads are up, it is time to initialize the hashring.
    if (clusterInfo.etcdAvailable) {
        RETURN_IF_NOT_OK(hashRing_->InitWithEtcd(multiReplicaEnabled_));
    } else {
        RETURN_IF_NOT_OK(hashRing_->InitWithoutEtcd(multiReplicaEnabled_, clusterInfo.localHashRing[0].second));
    }

    if (masterAddress_.Empty()) {
        // The master address was not provided at the beginning. HashRing selected one of the nodes as master.
        // Reinitialize masterAddress_ with the selected node.
        Status s = masterAddress_.ParseString(FLAGS_master_address);
        if (s.IsError()) {
            LOG(WARNING) << "Could not get master address";
        }
    }

    for (const auto &kv : clusterInfo.otherAzHashrings) {
        auto itr = otherAzHashRings_.find(kv.first);
        if (itr == otherAzHashRings_.end()) {
            LOG(INFO) << FormatString(
                "The hashRing for AZ[%s] was found, but the AZ does not exist. Maybe there is old data left", kv.first);
            continue;
        }
        RETURN_IF_NOT_OK(itr->second->Init(kv.second));
    }

    bool isRestart = false;
    RETURN_IF_NOT_OK(IsRestart(isRestart));
    RETURN_IF_NOT_OK(
        etcdDB_->InitKeepAlive(ETCD_CLUSTER_TABLE, workerAddress_.ToString(), isRestart, isEtcdAvailableWhenStart_));

    // Display the final list of nodes that were set up into the log
    LOG(INFO) << "Nodes tracked by cluster manager:\n" << this->NodesToString() << "\n" << this->OtherAzNodesToString();

    return Status::OK();
}

Status EtcdClusterManager::HandleFailedNodeToActive(const HostPort &eventNodeKey, ClusterNode *eventNode,
                                                    ClusterNode *failedNode)
{
    if (eventNode->NodeWasRecovered() || eventNode->NodeWasDowngradeRestart()) {
        // The node was not restarted, so its a network recovery case of a node that did not actually crash
        RETURN_IF_NOT_OK(ProcessNetworkRecovery(eventNodeKey, failedNode, eventNode));
    } else {
        RemoveDeadWorkerEvent::GetInstance().NotifyAll(eventNodeKey.ToString());
    }
    INJECT_POINT("EtcdClusterManager.HandleFailedNodeToActive.sleep");

    return Status::OK();
}

Status EtcdClusterManager::AddNewNode(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    // If it restarted, call for reconciliation, in case of distributed master. For centralized master, the restarted
    // worker will trigger reconciliation by itself.
    const int64_t timestamp = std::atol(eventNode->GetTimeEpoch().c_str());
    if (eventNode->NodeWasRestarted()) {
        LOG(INFO) << "The added node did a restart. Need to consider reconciliation.";
        if (IsCurrentNodeMaster()) {
            LOG_IF_ERROR(IfNeedTriggerReconciliation(eventNodeKey, timestamp, false),
                         "Failed reconciliation between this node and the target node " + eventNodeKey.ToString());
        }
    }
    // It is possible to see node being added to cluster manager with "recover" state. During network issue of node A,
    // node B restarted. When A recovers, it will be added to B with "recover" state.
    if (eventNode->NodeWasRecovered()) {
        if (IsCurrentNodeMaster()) {
            LOG_IF_ERROR(NodeNetworkRecoveryEvent::GetInstance().NotifyAll(eventNodeKey.ToString(), timestamp, false),
                         "Process network recover failed node " + eventNodeKey.ToString());
            LOG_IF_ERROR(CheckNewNodeMetaEvent::GetInstance().NotifyAll(eventNodeKey), "Check meta event failed");
        }
    }

    // If it's not found, then we're adding a new node.
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    LOG(INFO) << "Adding the ClusterNode to active nodes: " << eventNode->ToString(eventNodeKey);
    RemoveDeadWorkerEvent::GetInstance().NotifyAll(eventNodeKey.ToString());
    if (!clusterNodeTable_.emplace(eventNodeKey, std::move(eventNode))) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Failed to add new node into cluster node tracking.");
    }
    return Status::OK();
}

Status EtcdClusterManager::HandleNodeStateToActive(const HostPort &eventNodeKey,
                                                   const std::unique_ptr<ClusterNode> &eventNode,
                                                   ClusterNode *foundNode, bool &isTimeout)
{
    if (foundNode->IsFailed()) {
        // If ClusterNode was in the failed state and now restarted, send metadata to the master service of the node,
        // followed by processing the recovery of the worker service on the (previously failed) node
        if (IsCurrentNodeMaster()) {
            RETURN_IF_NOT_OK(AddLocalFailedNodeEvent::GetInstance().NotifyAll(eventNodeKey));
            RETURN_IF_NOT_OK(HandleFailedNodeToActive(eventNodeKey, eventNode.get(), foundNode));
        } else {
            // This is a worker only node in the centralized master setting. If master restarts, send metadata to it.
            if (masterAddress_ == eventNodeKey) {
                RETURN_IF_NOT_OK(AddLocalFailedNodeEvent::GetInstance().NotifyAll(eventNodeKey));
            } else {
                LOG(INFO) << "A worker restarted in the centralized master setting. Do Nothing";
            }
        }
    } else if (foundNode->IsTimedOut() || eventNode->NodeWasDowngradeRestart()) {
        if (IsCurrentNodeMaster()) {
            isTimeout = true;
            RETURN_IF_NOT_OK(HandleFailedNodeToActive(eventNodeKey, eventNode.get(), foundNode));
        }
    } else if (foundNode->IsActive()) {
        // If the ClusterNode was in the active state for this add event, the worker referred by ClusterNode should
        // call reconciliation by itself. The master will not do anything it not called.
        LOG(INFO) << "The node being added already existed in the active state. Refresh timestamp if needed.";
        if (eventNode->NodeWasRecovered() && eventNodeKey == workerAddress_) {
            // if etcd restart, set worker to ready from recover.
            INJECT_POINT("etcdrecover.worker.delaytoready");
            RETURN_IF_NOT_OK(etcdDB_->UpdateNodeState(ETCD_NODE_READY));
        }
    } else {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Existing node has an unknown state during node addition event.");
    }
    return Status::OK();
}

Status EtcdClusterManager::HandleNodeAdditionEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
                                                   const std::string &azName)
{
    (void)azName;
    INJECT_POINT("EtcdClusterManager.HandleNodeAdditionEvent.delay", [eventNodeKey](std::string addr) {
        if (eventNodeKey.ToString() == addr) {
            sleep(10);  // sleep for 10 s for add node delay;
        }
        return Status::OK();
    });
    auto timer = nodeTableCompletionTimer_.find(eventNodeKey.ToString());
    if (timer != nodeTableCompletionTimer_.end()) {
        TimerQueue::GetInstance()->Cancel(timer->second);
        nodeTableCompletionTimer_.erase(timer);
    }

    // Fetch the cluster node
    typename TbbNodeTable::const_accessor accessor;
    bool nodeExist;
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        nodeExist = clusterNodeTable_.find(accessor, eventNodeKey);
    }
    if (!nodeExist) {
        return AddNewNode(eventNodeKey, std::move(eventNode));
    }

    ClusterNode *foundNode = accessor->second.get();
    bool isTimeout = false;
    // Fake addition event can not cover the real addition event, so ignore fake event if node already exists.
    if (*foundNode == *eventNode || eventNode->EventValue() == FAKE_NODE_EVENT_VALUE) {
        LOG(INFO) << "The same node addition was already processed. Do nothing. Node addition: "
                  << eventNode->ToString(eventNodeKey);
        return Status::OK();
    }

    LOG_IF_ERROR(HandleNodeStateToActive(eventNodeKey, eventNode, foundNode, isTimeout),
                 FormatString("Error occurs when node is switched from %s to %s", foundNode->ToString(eventNodeKey),
                              eventNode->ToString(eventNodeKey)));

    // Regardless of which path was taken above, the node is now added and we want its timestamp to match the one on
    // etcd. Reconciliation is also needed.
    if (eventNode->NodeWasRestarted() || eventNode->NodeWasDowngradeRestart()) {
        if (IsCurrentNodeMaster()) {
            LOG(INFO) << "The added node did a restart. Need to consider reconciliation.";
            const int64_t timestamp = std::atol(eventNode->GetTimeEpoch().c_str());
            LOG_IF_ERROR(
                IfNeedTriggerReconciliation(eventNodeKey, timestamp, false, eventNode->NodeWasDowngradeRestart()),
                "Failed reconciliation between this node and the target node " + eventNodeKey.ToString());
        }
        if (isTimeout) {
            hashRing_->RecoverMigrationTask(eventNodeKey.ToString());
        }
    }

    foundNode->SetActive();
    foundNode->CopyInfoFrom(*eventNode);  // update the timestamp and additionEventType of the existing node

    return Status::OK();
}

Status EtcdClusterManager::HandleOtherAzNodeAddEvent(const HostPort &eventNodeKey,
                                                     std::unique_ptr<ClusterNode> eventNode, const std::string &azName)
{
    (void)azName;
    LOG(INFO) << "Add to the other AZ cluster node: " << eventNodeKey.ToString();
    std::string nodeAddr = eventNodeKey.ToString();
    long timestamp = 0;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Uri::StrToLong(eventNode->GetTimeEpoch().c_str(), timestamp), K_RUNTIME_ERROR,
                                         "timestamp StrToLong failed");
    auto isRecovered = eventNode->NodeWasRecovered();
    auto isRestarted = eventNode->NodeWasRestarted();

    std::shared_lock<std::shared_timed_mutex> lock(otherClusterNodeMutex_);
    TbbNodeTable::const_accessor accessor;
    if (!otherClusterNodeTable_.find(accessor, eventNodeKey)) {
        otherClusterNodeTable_.emplace(eventNodeKey, std::move(eventNode));
    }
    if (isRecovered) {
        RETURN_IF_NOT_OK(AddLocalFailedNodeEvent::GetInstance().NotifyAll(eventNodeKey));
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            RequestMetaFromWorkerEvent::GetInstance().NotifyAll(workerAddress_.ToString(), nodeAddr),
            "Could not request worker to send metadata");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(NodeNetworkRecoveryEvent::GetInstance().NotifyAll(nodeAddr, timestamp, false),
                                         "Network recovery failed for offline node");
        return Status::OK();
    }
    if (isRestarted) {
        RETURN_IF_NOT_OK(AddLocalFailedNodeEvent::GetInstance().NotifyAll(eventNodeKey));
        RETURN_IF_NOT_OK(NodeRestartEvent::GetInstance().NotifyAll(eventNodeKey.ToString(), timestamp, false));
        return Status::OK();
    }
    return Status::OK();
}

Status EtcdClusterManager::HandleNodeRemoveEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
                                                 const std::string &azName)
{
    INJECT_POINT("HandleNodeRemoveEvent.delay");
    (void)azName;
    // Fetch the cluster node
    typename TbbNodeTable::const_accessor accessor;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(clusterNodeTable_.find(accessor, eventNodeKey), K_NOT_FOUND,
                                         "The timeout node could not be found in active nodes list");
    nodeTableCompletionTimer_.erase(eventNodeKey.ToString());

    ClusterNode *foundNode = accessor->second.get();
    // Fake remove event can only remove the node added by fake addition event. This check is used to minimize the
    // possibility of nodes being removed incorrectly.
    if (eventNode != nullptr && eventNode->EventValue() == FAKE_NODE_EVENT_VALUE
        && foundNode->EventValue() != FAKE_NODE_EVENT_VALUE) {
        LOG(INFO) << "Ignore the fake removal event if real node already exists.";
        return Status::OK();
    }

    if (foundNode->IsTimedOut()) {
        LOG(INFO) << "worker has received remove event, no need HandleNodeRemoveEvent again";
        return Status::OK();
    }
    if (foundNode->NodeWasExiting()) {
        return HandleExitingNodeRemoveEvent(eventNodeKey, eventNode.get(), foundNode, accessor);
    }

    std::string workerAddr = eventNodeKey.ToString();
    LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(workerAddr, true, false, false),
                 "Node timeout event process failed");
    LOG(INFO) << FormatString("Mark %s as timeout.", eventNodeKey.ToString());
    foundNode->SetTimedOut();
    return Status::OK();
}

Status EtcdClusterManager::HandleExitingNodeRemoveEvent(const HostPort &eventNodeKey, const ClusterNode *eventNode,
                                                        ClusterNode *foundNode, TbbNodeTable::const_accessor &accessor)
{
    // If the voluntary scale down node still in the hash ring which means the node is removed because of network
    // failure or sudden crashing, then begin the passive scale down process.
    std::string workerAddr = eventNodeKey.ToString();
    if (workerAddr == workerAddress_.ToString()) {
        LOG(INFO) << "The timeout voluntary scale down node is local worker, ready to shutdown";
        foundNode->SetFailed();
        return Status::OK();
    }

    if ((!isLeaving_ && hashRing_->IsPreLeaving(workerAddr)) || hashRing_->IsLeaving(workerAddr)) {
        LOG(INFO) << "The voluntary scale down node " << workerAddr << " crush, Processworkertimeout";
        LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(workerAddr, true, true, false),
                     "Error occurs when voluntary scale down node mark timeout: "
                         + (eventNode == nullptr ? "" : eventNode->ToString(eventNodeKey)));
        // Trigger slot recovery for the crashed voluntary scale down node.
        if (IsCurrentNodeMaster()) {
            LOG_IF_ERROR(SlotRecoveryFailedWorkersEvent::GetInstance().NotifyAll(std::vector<HostPort>{ eventNodeKey }),
                         "Failed to notify slot recovery for crashed voluntary scale down worker.");
        }
        foundNode->SetFailed();
        return Status::OK();
    }

    LOG(INFO) << "The voluntary scale down node finish, try remove worker " << workerAddr << " from cluster node table";
    LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(workerAddr, false, true, false),
                 "Node timeout event process failed");
    ChangePrimaryCopy::GetInstance().NotifyAll(workerAddr, true);
    RemoveDeadWorkerEvent::GetInstance().NotifyAll(workerAddr);
    (void)clusterNodeTable_.erase(accessor);
    HostPort addr = eventNodeKey;
    ClearWorkerMeta::GetInstance().NotifyAll(addr);
    EraseFailedNodeApiEvent::GetInstance().NotifyAll(addr);
    return Status::OK();
}

Status EtcdClusterManager::HandleOtherAzNodeRemoveEvent(const HostPort &eventNodeKey,
                                                        std::unique_ptr<ClusterNode> eventNode,
                                                        const std::string &azName)
{
    (void)eventNode;
    LOG(INFO) << "Delete the node from the other AZ cluster: " << eventNodeKey.ToString();
    std::shared_lock<std::shared_timed_mutex> lock(otherClusterNodeMutex_);
    TbbNodeTable::accessor accessor;
    if (!otherClusterNodeTable_.find(accessor, eventNodeKey)) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "The timeout node could not be found in other AZ nodes list");
    }
    ClusterNode *foundNode = accessor->second.get();
    if (foundNode->NodeWasExiting()) {
        auto iter = otherAzHashRings_.find(azName);
        if (iter == otherAzHashRings_.end()) {
            RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                    "The timeout node's hahs ring could not be found in other AZ hash ring list");
        }
        if (iter->second->IsPreLeaving(eventNodeKey.ToString())) {
            LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(eventNodeKey.ToString(), true, true, true),
                         "Node timeout event process failed");
            otherClusterNodeTable_.erase(accessor);
            return Status::OK();
        }
        auto workerAddr = eventNodeKey.ToString();
        LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(workerAddr, false, true, true),
                     "Node timeout event process failed");
        RemoveDeadWorkerEvent::GetInstance().NotifyAll(workerAddr);
        HostPort addr = eventNodeKey;
        EraseFailedNodeApiEvent::GetInstance().NotifyAll(addr);
        otherClusterNodeTable_.erase(accessor);
        return Status::OK();
    }
    LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(eventNodeKey.ToString(), true, true, true),
                 "Node timeout event process failed");
    otherClusterNodeTable_.erase(accessor);
    return Status::OK();
}

void EtcdClusterManager::EnqueEvent(mvccpb::Event &&event)
{
    INJECT_POINT("EtcdClusterManager.EnqueEvent", [] { return; });
    if (thread_ == nullptr) {
        LOG(INFO) << "The dequeue handle is nil, no need to enqueue event.";
        return;
    }
    // The watch thread got an event. Wrap it in CmEvent with type of prefix and send it to a priority queue.
    // Let the CM background thread instead of this watch thread to handle the events, because in the latter case
    // the watch thread might be blocked when checking if an event of node addition is a restart. It requires to
    // check hashring but the hashring could be not in RUNNING state. However, the hashring initialization also relies
    // on this watch thread to process hashring-related events.
    Status rc;
    const auto &key = event.kv().key();
    Timer timer;
    int maxEnqueueTimeMs = 100, logEveryN = 30;
    if (key.find(ETCD_CLUSTER_TABLE) != std::string::npos) {
        rc = eventPq_->EmplaceBack(new CmEvent(std::move(event), PrefixType::CLUSTER));
    } else if (key.find(ETCD_RING_PREFIX) != std::string::npos) {
        rc = eventPq_->EmplaceBack(new CmEvent(std::move(event), PrefixType::RING));
    } else if (event.kv().key().find(ETCD_REPLICA_GROUP_TABLE) != std::string::npos) {
        rc = ReplicaEvent::GetInstance().NotifyAll(event);
    } else {
        LOG(ERROR) << "Event of PrefixType::OTHER, no need to enqueue and handle it.";
    }
    LOG_IF_ERROR(rc, "Push an element to the priority queue failed");
    if (timer.ElapsedMilliSecond() > maxEnqueueTimeMs) {
        LOG_EVERY_N(WARNING, logEveryN) << "EnqueEvent ElapsedMilliSecond: " << timer.ElapsedMilliSecond()
                                        << ", eventPq_ size: " << eventPq_->Size();
    }
}

Status EtcdClusterManager::DequeEventCallHandler(bool &isHandleEvent)
{
    // Flush the cache in case there is no events coming after hashring is ready.
    // In this case the call of FlushTmpClusterAdditionEvents() in `case PrefixType::CLUSTER` will not occur.
    RETURN_IF_NOT_OK(FlushTmpClusterEvents());

    Status rc;
    std::unique_ptr<CmEvent> toProcess;
    rc = eventPq_->Remove(&toProcess);
    if (rc.IsError() || toProcess == nullptr) {  // return ok if pq is empty
        return Status::OK();
    }

    // If it is an event of node addition, we want to check if the node is a restart. This requires the hashring be in
    // RUNNING state in advance. If not in RUNNING state, we need to cache all cluster events in tmpClusterEvents_.
    // Then return OK and come back later. After hashring is ready, need to flush this cache. Before processing any
    // events directly fetched from priority queue, make sure the cache is flushed.
    // Hashring event has higher priority so it will climb to head to pop up first.
    // If master is centralized, Hashring is not needed, so no need to wait for hashring to be in RUNNING state.
    if (!IsCentralized() && toProcess->prefix == PrefixType::CLUSTER && !hashRing_->IsWorkable()) {
        LOG(INFO) << "Cache cluster event until hashring is workerable: "
                  << LogHelper::IgnoreSensitive(toProcess->event);
        tmpClusterEvents_.emplace_back(std::move(toProcess));
        return Status::OK();
    }
    {
        typename TbbNodeTable::const_accessor accessor;
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        if (clusterNodeTable_.find(accessor, workerAddress_)) {
            if (accessor->second->IsFailed() && FLAGS_auto_del_dead_node) {
                // local worker is failed, need remove, no need to handle event
                LOG(INFO) << "local worker is failed, ready to shutdown, no need to handle events";
                return Status::OK();
            }
        }
    }
    isHandleEvent = true;
    LOG(INFO) << "Process event: " << toProcess->ToString();
    // In other cases, no need to wait for hashring events. Just pop up the head and unlock the lock for priority queue
    // and pass the event to a handler.
    switch (toProcess->prefix) {
        case PrefixType::CLUSTER:
            // Before processing any events fetched from priority queue just now, flush the cache first.
            RETURN_IF_NOT_OK(FlushTmpClusterEvents());
            RETURN_IF_NOT_OK(HandleClusterEvent(toProcess->event));
            // Display the list of nodes to the log now that the handling of cluster event has completed
            LOG(INFO) << "Nodes tracked by cluster manager:\n"
                      << this->NodesToString() << "\n"
                      << this->OtherAzNodesToString();
            break;
        case PrefixType::RING:
            RETURN_IF_NOT_OK(HandleRingEvent(toProcess->event));
            break;
        default:
            LOG(WARNING) << "No handler for ETCD event " << toProcess->ToString();
    }
    return Status::OK();
}

void EtcdClusterManager::TrySplitAzNameFromEventKey(const std::string &key, std::string &azName)
{
    auto res = Split(key, "/");
    if (res.size() > 1) {
        azName = res[1];
    }
}

Status EtcdClusterManager::SplitAzNameFromEventKey(const std::string &key, std::string &azName)
{
    TrySplitAzNameFromEventKey(key, azName);
    if (!azName.empty() && otherAzHashRings_.find(azName) != otherAzHashRings_.end()) {
        return Status::OK();
    }
    azName.clear();
    return { K_RUNTIME_ERROR, FormatString("[etcd eventKey %s] don't contain AZ Name.", key) };
}

Status EtcdClusterManager::HandleRingEvent(const mvccpb::Event &event)
{
    if (event.type() == mvccpb::Event_EventType::Event_EventType_DELETE) {
        return Status::OK();
    }
    Status rc;
    std::string eventKey = event.kv().key();
    if (eventKey == (ringPrefix_ + "/")) {
        // local AZ hash ring
        rc = hashRing_->HandleRingEvent(event, ringPrefix_);
    } else {
        // update other AZ read hash ring
        std::string azName;
        RETURN_IF_NOT_OK(SplitAzNameFromEventKey(eventKey, azName));
        auto iter = otherAzHashRings_.find(azName);
        CHECK_FAIL_RETURN_STATUS(iter != otherAzHashRings_.end(), K_RUNTIME_ERROR, "no readRing: " + azName);
        auto &readRing = iter->second;
        rc = readRing->HandleRingEvent(event);
    }
    return rc;
}

Status EtcdClusterManager::HandleClusterEvent(const mvccpb::Event &event)
{
    Status rc;
    std::string nodeHostPortStr = event.kv().key();
    std::string nodeTimestamp = event.kv().value();
    // Parse the type of event of addition appended to timestamp.
    std::string additionEventType;  // "start", "restart", "recover"
    if (event.type() == mvccpb::Event_EventType::Event_EventType_PUT || nodeTimestamp == FAKE_NODE_EVENT_VALUE) {
        KeepAliveValue keepAliveValue;
        auto parseRc = KeepAliveValue::FromString(nodeTimestamp, keepAliveValue);
        if (parseRc.IsOk()) {
            additionEventType = keepAliveValue.state;
            nodeTimestamp = keepAliveValue.timestamp;
        } else {
            std::stringstream ss;
            ss << "Event of node, key: " << nodeHostPortStr << " value: " << nodeTimestamp
               << " type: " << mvccpb::Event::EventType_Name(event.type()) << ", is not recognized.";
            RETURN_STATUS(K_RUNTIME_ERROR, ss.str());
        }
    }

    using ProcessEventFunc = Status (EtcdClusterManager::*)(
        const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode, const std::string &azName);
    ProcessEventFunc addNodeFunc;
    ProcessEventFunc removeNodeFunc;
    if (nodeHostPortStr.find(clusterPrefix_) != std::string::npos) {
        // Local AZ node process function
        addNodeFunc = &EtcdClusterManager::HandleNodeAdditionEvent;
        removeNodeFunc = &EtcdClusterManager::HandleNodeRemoveEvent;
    } else {
        // Other AZ node process function
        addNodeFunc = &EtcdClusterManager::HandleOtherAzNodeAddEvent;
        removeNodeFunc = &EtcdClusterManager::HandleOtherAzNodeRemoveEvent;
    }

    // Remove /TableName/ from key to get IP and port
    nodeHostPortStr.erase(0, nodeHostPortStr.find(ETCD_CLUSTER_TABLE) + strlen(ETCD_CLUSTER_TABLE) + 1);
    HostPort eventNodeKey;
    RETURN_IF_NOT_OK(eventNodeKey.ParseString(nodeHostPortStr));
    std::string azName;
    TrySplitAzNameFromEventKey(event.kv().key(), azName);
    auto eventNode = std::make_unique<ClusterNode>(nodeTimestamp, additionEventType);
    if (event.type() == mvccpb::Event_EventType::Event_EventType_PUT) {
        LOG(INFO) << "Event Type: Add Node: " << eventNode->ToString(eventNodeKey);
        rc = (this->*addNodeFunc)(eventNodeKey, std::move(eventNode), azName);
    } else if (event.type() == mvccpb::Event_EventType::Event_EventType_DELETE) {
        LOG(INFO) << "Event Type: Remove Node: " << eventNode->ToString(eventNodeKey);
        LOG_IF_ERROR_EXCEPT(RemoveRemoteFastTransportNode(eventNodeKey), "", K_NOT_FOUND);
        rc = (this->*removeNodeFunc)(eventNodeKey, std::move(eventNode), azName);
    } else {
        rc = Status(K_RUNTIME_ERROR, "unknown type: " + event.DebugString());
    }
    return rc;
}

Status EtcdClusterManager::ProcessNetworkRecovery(const HostPort &recoverNodeKey, ClusterNode *recoverNode,
                                                  ClusterNode *eventNode)
{
    LOG(INFO) << "Detected network recovery.";
    std::string nodeAddr = recoverNodeKey.ToString();

    const int64_t timestamp = std::atol(eventNode->GetTimeEpoch().c_str());
    const std::string &err = "The timestamp of an event triggering reconciliation should be greater than 0";
    if (recoverNode->IsTimedOut()) {
        CHECK_FAIL_RETURN_STATUS(timestamp > 0, K_INVALID, err);
        TraceGuard traceGuard = Trace::Instance().SetSubTraceID(GetStringUuid().substr(0, SHORT_TRACEID_SIZE));
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(NodeNetworkRecoveryEvent::GetInstance().NotifyAll(nodeAddr, timestamp, false),
                                         "Network recovery failed for timeout node");
        hashRing_->RecoverMigrationTask(nodeAddr);
    } else if (recoverNode->IsFailed()) {
        CHECK_FAIL_RETURN_STATUS(timestamp > 0, K_INVALID, err);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(NodeNetworkRecoveryEvent::GetInstance().NotifyAll(nodeAddr, timestamp, true),
                                         "Network recovery failed for offline node");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            RequestMetaFromWorkerEvent::GetInstance().NotifyAll(workerAddress_.ToString(), nodeAddr),
            "Could not request worker to send metadata");
    } else if (!eventNode->NodeWasDowngradeRestart()) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Invalid node state for processing network recovery.");
    }
    if (eventNode->NodeWasDowngradeRestart()) {
        RETURN_IF_NOT_OK(CheckNewNodeMetaEvent::GetInstance().NotifyAll(recoverNodeKey));
    }
    return Status::OK();
}

Status EtcdClusterManager::CheckConnection(const std::string &objKey, bool allowInOtherAz)
{
    MetaAddrInfo info;
    std::optional<RouteInfo> routeInfo;
    RETURN_IF_NOT_OK(GetMetaAddressNotCheckConnection(objKey, info, routeInfo));
    return CheckConnection(info.GetAddress(), allowInOtherAz);
}

Status EtcdClusterManager::CheckConnection(const HostPort &nodeAddr, bool allowInOtherAz, bool allowNotFound)
{
    Timer timer;
    INJECT_POINT("EtcdClusterManager.checkConnection");
    typename TbbNodeTable::const_accessor accessor;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto elapsedMs = static_cast<uint64_t>(timer.ElapsedMilliSecondAndReset());
    workerOperationTimeCost.Append("CheckConnection wait", elapsedMs);
    if (!clusterNodeTable_.find(accessor, nodeAddr)) {
        bool nodeInOtherAzAndhealth = CheckIfOtherAzNodeConnected(nodeAddr);
        if (nodeInOtherAzAndhealth && allowInOtherAz) {
            return Status::OK();
        }
        if (nodeInOtherAzAndhealth && !allowInOtherAz) {
            RETURN_STATUS(K_NOT_FOUND, "The node " + nodeAddr.ToString()
                                           + " could not be found in cluster node table, but be found in other az.");
        }
        if (allowNotFound) {
            return Status::OK();
        }
        std::string errMsg = "The node " + nodeAddr.ToString() + " could not be found in cluster node table.";
        LOG(INFO) << errMsg;
        RETURN_STATUS(K_NOT_FOUND, errMsg);
    }

    if (accessor->second->IsFailed() || accessor->second->IsTimedOut()) {
        RETURN_STATUS(StatusCode::K_MASTER_TIMEOUT, "Disconnected from remote node " + nodeAddr.ToString());
    }
    elapsedMs = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    workerOperationTimeCost.Append("CheckConnection", elapsedMs);
    return Status::OK();
}

bool EtcdClusterManager::CheckWorkerIsScaleDown(const std::string &workerAddr)
{
    return hashRing_->CheckWorkerIsScaleDown(workerAddr);
}

std::set<std::string> EtcdClusterManager::GetValidWorkersInHashRing() const
{
    std::set<std::string> validWorkersInHashRing;
    const auto &validWorkersInLocalAz = hashRing_->GetValidWorkersInHashRing();
    validWorkersInHashRing.insert(validWorkersInLocalAz.begin(), validWorkersInLocalAz.end());
    for (auto &otherAzHashRing : otherAzHashRings_) {
        const auto &validWorkersInOtherlAz = otherAzHashRing.second->GetValidWorkersInHashRing();
        validWorkersInHashRing.insert(validWorkersInOtherlAz.begin(), validWorkersInOtherlAz.end());
    }
    return validWorkersInHashRing;
}

std::set<std::string> EtcdClusterManager::GetActiveWorkersInHashRing() const
{
    std::set<std::string> activeWorkers;
    const auto &activeWorkersInLocalAz = hashRing_->GetActiveWorkersInHashRing();
    for (const auto &worker : activeWorkersInLocalAz) {
        HostPort workerAddr;
        if (workerAddr.ParseString(worker).IsError()) {
            continue;
        }
        TbbNodeTable::const_accessor accessor;
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        if (clusterNodeTable_.find(accessor, workerAddr) && accessor->second->IsActive()
            && accessor->second->NodeWasReady()) {
            activeWorkers.emplace(worker);
        }
    }
    return activeWorkers;
}

bool EtcdClusterManager::CheckIfOtherAzNodeConnected(const HostPort &nodeAddr)
{
    std::shared_lock<std::shared_timed_mutex> lock(otherClusterNodeMutex_);
    TbbNodeTable::const_accessor accessor;
    return otherClusterNodeTable_.find(accessor, nodeAddr);
}

bool EtcdClusterManager::CheckReceiveMigrateInfo()
{
    if (!MultiReplicaEnabled()) {
        return hashRing_->CheckReceiveMigrateInfo(workerAddress_.ToString());
    }

    std::map<std::string, std::string> replicaInfos;
    ReplicaMagagerEvent::GetPrimaryReplicaInfoInWorker::GetInstance().NotifyAll(GetLocalWorkerUuid(), replicaInfos);
    for (const auto &info : replicaInfos) {
        if (hashRing_->CheckReceiveMigrateInfo(info.second)) {
            return true;
        }
    }
    return false;
};

void EtcdClusterManager::WaitWorkerReadyIfNeed()
{
    if (!IsCurrentNodeMaster()) {
        // If it is not the master, there is no need to wait for the worker to be ready.
        return;
    }
    Timer timer;
    workerWaitPost_->Wait();
    int64_t costTimeMs = static_cast<int64_t>(timer.ElapsedMilliSecond());
    LOG(INFO) << "Wait worker ready cost(ms): " << costTimeMs;
}

Status EtcdClusterManager::StartNodeUtilThread()
{
    static const int CHECK_INTERVAL_MS = 100;
    auto traceId = GetStringUuid().substr(0, SHORT_TRACEID_SIZE);
    LOG(INFO) << "Start node util thread in cluster manager with traceId: " << traceId;
    const int clearScaledDownNodeInClusterTableMaxIntervelMs = 30'000;
    thread_ = std::make_unique<Thread>([this, traceId]() {
        Status rc;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        WaitWorkerReadyIfNeed();
        INJECT_POINT("EtcdClusterManager.DelayMessageDeque.test", [](int delayTime) { sleep(delayTime); });
        bool isHandleEvent;
        Timer timer;
        while (!exitFlag_) {
            isHandleEvent = false;
            rc = DequeEventCallHandler(isHandleEvent);
            if (rc.IsError()) {
                LOG(ERROR) << "When handling event, received error: " << rc.GetMsg();
            }
            LOG_IF_ERROR(StartNodeCheckEvent::GetInstance().NotifyAll(), "GiveUpReconciliation yields error");

            DemoteTimedOutNodes();

            // make use of existing demotion thread to generate hash tokens
            if (hashRing_ != nullptr) {
                hashRing_->RemoveWorkers(GetFailedWorkers());
                hashRing_->InspectAndProcessPeriodically();
            }
            if (!isHandleEvent || timer.ElapsedMilliSecondAndReset() > clearScaledDownNodeInClusterTableMaxIntervelMs) {
                auto workers = GetKeysFromPairsContainer(hashRing_->GetHashRingPb().workers());
                HashRingEvent::SyncClusterNodes::GetInstance().NotifyAll(workers);
            }

            if (!isHandleEvent) {
                ScheduledCheckCompleteNodeTableWithFakeNode();
                // Wait 100ms when the queue is not processing event.
                cvLock_.WaitFor(CHECK_INTERVAL_MS);
            }
        }
    });
    thread_->set_name("EtcdUtil");
    return Status::OK();
}

void EtcdClusterManager::GetToBeCleanNodes(const std::unordered_map<std::string, std::string> &orphanNodes,
                                           std::set<std::pair<std::string, bool>> &toBeCleanNodes)
{
    static const int timeoutMs = 5000;
    for (const auto &[orphanNode, timeEpoch] : orphanNodes) {
        HostPort addr;
        if (addr.ParseString(orphanNode).IsError()) {
            continue;
        }
        // check the nodes that not found in hash ring
        RangeSearchResult res;
        auto status = etcdDB_->Get(ETCD_CLUSTER_TABLE, orphanNode, res, timeoutMs);

        typename TbbNodeTable::const_accessor accessor;
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto nodeExist = clusterNodeTable_.find(accessor, addr);
        if (!nodeExist) {
            LOG(INFO) << "Node " << orphanNode << " is not found in cluster table";
            continue;
        }
        if (timeEpoch != accessor->second->GetTimeEpoch()) {
            LOG(INFO) << "Node " << orphanNode << " is updated in cluster table";
            continue;
        }
        auto &node = accessor->second;
        if (status.GetCode() == K_NOT_FOUND) {
            // if the node is not found in etcd, it needs to be removed whatever its state in clusterNodeTable_
            // is.
            LOG(INFO) << "Ready to clear resource of worker " << orphanNode
                      << " that not found in etcd, state in cluster node table before cleanup: "
                      << node->ToString(addr);
            if (node->NodeWasExiting()) {
                toBeCleanNodes.emplace(orphanNode, false);
            } else {
                toBeCleanNodes.emplace(orphanNode, node->IsFailed());
            }
        } else if (status.IsOk()) {
            // the node has been rejoined or is ready to rejoin
            if (node->IsFailed()) {
                LOG(INFO) << "Ready to clear resource of worker " << orphanNode
                          << " that has rejoined into etcd, state in cluster node table before cleanup: "
                          << node->ToString(addr);
                // erase the failed node to prevent scale down again and wait for the new node coming
                toBeCleanNodes.emplace(orphanNode, true);
            } else {
                // skip the erasure. we should wait at least until the lease expires to prevent remove the
                // joined node incorrectly
                LOG(INFO) << "Skip to clear resource of worker " << orphanNode
                          << ", state in cluster node table: " << node->ToString(addr)
                          << ", state in etcd: " << res.value;
            }
        } else {
            LOG(INFO) << "Failed to get node " << orphanNode << " from etcd, status: " << status.ToString();
        }
    }
}

Status EtcdClusterManager::StartOrphanNodeMonitorThread()
{
    auto traceId = GetStringUuid().substr(0, SHORT_TRACEID_SIZE);
    LOG(INFO) << "Start orphan node monitor thread in cluster manager with traceId: " << traceId;
    orphanNodeMonitorThread_ = std::make_unique<Thread>([this, traceId]() {
        Timer timer;
        while (!exitFlag_) {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            orphanWaitPost_.WaitAndClear();
            std::unordered_map<std::string, std::string> orphanNodes;
            {
                std::lock_guard<std::shared_timed_mutex> lock(orphanNodeMutex_);
                for (const auto &iter : orphanNodeTable_) {
                    orphanNodes.emplace(iter.first, iter.second);
                }
                orphanNodeTable_.clear();
            }
            auto sz = orphanNodes.size();
            if (sz == 0) {
                continue;
            }
            if (etcdDB_->IsKeepAliveTimeout()) {
                const int logEveryN = 1000;
                LOG_EVERY_N(INFO, logEveryN)
                    << "etcd is currently unavailable, synchronization cannot be completed, waiting for the next round "
                       "of retry";
                return;
            }
            timer.Reset();
            Raii raii([&timer, &sz]() {
                static const int logThresholdMs = 1'000;
                LOG_IF(INFO, timer.ElapsedMilliSecond() > logThresholdMs)
                    << "Cleanup" << sz << " nodes elapsed: " << timer.ElapsedMilliSecond();
            });
            std::set<std::pair<std::string, bool>> toBeCleanNodes;
            GetToBeCleanNodes(orphanNodes, toBeCleanNodes);
            for (const auto &[addr, isFailed] : toBeCleanNodes) {
                CleanupWorker(addr, isFailed);
            }
            if (!toBeCleanNodes.empty()) {
                LOG(INFO) << "After sync with hash ring: " << NodesToString();
            }
        }
    });
    orphanNodeMonitorThread_->set_name("OrphanNodeMonitor");
    return Status::OK();
}

Status EtcdClusterManager::StartBackgroundThread()
{
    RETURN_IF_NOT_OK(StartNodeUtilThread());
    RETURN_IF_NOT_OK(StartOrphanNodeMonitorThread());
    return Status::OK();
}

void EtcdClusterManager::HandleFailedNode(const HostPort &addr)
{
    if (!IsCurrentNodeMaster()) {
        return;
    }
    // Perform dead node handling now to adjust any references
    LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(addr.ToString(), false, true, false),
                 "Failed to process worker timeout in etcd demotion thread");
    LOG_IF_ERROR(StartClearWorkerMeta::GetInstance().NotifyAll(addr), "Failed to clear worker meta data.");
}

void EtcdClusterManager::DemoteTimedOutNodes()
{
    // tbb concurrent hash table does not support thread-safe iteration
    std::unique_lock<std::shared_timed_mutex> lock(mutex_, std::defer_lock);
    bool rec = lock.try_lock();
    if (!rec) {
        return;
    }
    std::vector<HostPort> failedNode;
    for (const auto &iter : clusterNodeTable_) {
        if (iter.second->DemoteTimedOutNode()) {
            LOG(INFO) << "A timed out cluster node was demoted to become a failed node: "
                      << iter.second->ToString(iter.first);
            failedNode.emplace_back(iter.first);
        }
    }

    lock.unlock();
    for (const auto &addr : failedNode) {
        HandleFailedNode(addr);
    }
    if (IsCurrentNodeMaster() && !failedNode.empty()) {
        LOG_IF_ERROR(SlotRecoveryFailedWorkersEvent::GetInstance().NotifyAll(failedNode),
                     "Failed to notify slot recovery for failed workers.");
    }
    LOG_IF(INFO, !failedNode.empty()) << "After demote timeout nodes: " << NodesToString();
}

std::unordered_set<std::string> EtcdClusterManager::GetFailedWorkers()
{
    std::unordered_set<std::string> failedWorkers;
    // tbb concurrent hash table does not support thread-safe iteration
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto workers = hashRing_->GetValidWorkersInHashRing();
    for (const auto &iter : clusterNodeTable_) {
        if (iter.second->IsFailed()) {
            failedWorkers.emplace(iter.first.ToString());
        }
        // 1. voluntary scale down worker not in hashring, but need execute shutdown task, not a failed worker.
        // 2. If the current worker is not in hashring, perhaps the worker has just started. If this is a faulty node,
        // we will recognize it by HashRing::state_, so skip here.
        if (workers.find(iter.first.ToString()) == workers.end() && !iter.second->NodeWasExiting()
            && iter.first != workerAddress_) {
            failedWorkers.emplace(iter.first.ToString());
        }
    }
    INJECT_POINT("worker.GetFailedWorkers", [&failedWorkers](const std::string &workerAddr) {
        failedWorkers.emplace(workerAddr);
        return failedWorkers;
    });
    return failedWorkers;
}

void EtcdClusterManager::SyncNodeTableWithHashRing(const std::set<std::string> &workersInRing)
{
    INJECT_POINT("SyncNodeTableWithHashRing", [] { return; });
    bool isNotify = false;
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        std::string workerAddr;
        for (const auto &iter : clusterNodeTable_) {
            workerAddr = iter.first.ToString();
            if (ContainsKey(workersInRing, workerAddr)) {
                continue;
            }
            {
                std::shared_lock<std::shared_timed_mutex> l(orphanNodeMutex_);
                orphanNodeTable_.emplace(workerAddr, iter.second->GetTimeEpoch());
            }
            isNotify = true;
        }
    }
    if (isNotify) {
        orphanWaitPost_.Set();
    }
}

// we should delete the old resource of the not-exited workers to make sure the worker can re-join.
// include the address in backend store and api in class member.
void EtcdClusterManager::CleanupWorker(const std::string &workerAddr, bool isFailed)
{
    HostPort addr;
    if (addr.ParseString(workerAddr).IsError()) {
        return;
    }
    // 1. clear node table
    {
        // process the node state to failed and then erase.
        if (!isFailed) {
            WARN_IF_ERROR(HandleNodeRemoveEvent(addr, nullptr, ""),
                          ", process to timeout failed.");  // node to timeout
            HandleFailedNode(addr);                         // node to failed
        }
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        (void)clusterNodeTable_.erase(addr);
        (void)nodeTableCompletionTimer_.erase(workerAddr);
    }
    // 2. clear ocnotify api
    RemoveDeadWorkerEvent::GetInstance().NotifyAll(workerAddr);
    // 3. clear workerapi
    EraseFailedNodeApiEvent::GetInstance().NotifyAll(addr);
}

Status EtcdClusterManager::IfNeedTriggerReconciliation(const HostPort &address, int64_t timestamp, bool sync,
                                                       bool isDRst)
{
    INJECT_POINT("EtcdClusterManager.IfNeedTriggerReconciliation.noreconciliation");
    if (!isDRst) {
        // Clear worker metadata first in master
        RETURN_IF_NOT_OK(ClearWorkerMeta::GetInstance().NotifyAll(address));
    }
    // Then reply to worker Reconciliation Done
    RETURN_IF_NOT_OK(NodeRestartEvent::GetInstance().NotifyAll(address.ToString(), timestamp, sync));
    LOG(INFO) << "Reconciliation was sent to worker " << address.ToString();

    return Status::OK();
}

std::string EtcdClusterManager::NodesToString()
{
    int activeNum = 0;
    int timeoutNum = 0;
    int failNum = 0;
    int readyNum = 0;
    // tbb concurrent hash table does not support thread-safe iteration
    std::string nodesStr;
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    for (const auto &iter : clusterNodeTable_) {
        if (iter.second->IsActive() && iter.second->GetTimeEpoch() != "0") {
            activeNum++;
            if (iter.second->NodeWasReady()) {
                readyNum++;
                continue;
            }
        } else if (iter.second->IsTimedOut()) {
            timeoutNum++;
        } else if (iter.second->IsFailed()) {
            failNum++;
        }
        nodesStr += iter.second->ToString(iter.first) + "\n";
    }

    return FormatString("ClusterNodes currently tracked %u nodes: ACTIVE:%d, TIMEOUT:%d, FAIL:%d, ready:%d\n",
                        clusterNodeTable_.size(), activeNum, timeoutNum, failNum, readyNum)
           + nodesStr;
}

std::string EtcdClusterManager::OtherAzNodesToString()
{
    std::string returnStr("Other AZ nodes currently tracked: ");
    // tbb concurrent hash table does not support thread-safe iteration
    std::lock_guard<std::shared_timed_mutex> lock(otherClusterNodeMutex_);
    for (const auto &iter : otherClusterNodeTable_) {
        returnStr += iter.first.ToString() + "; ";
    }
    return returnStr;
}

Status EtcdClusterManager::GetClusterNodeAddresses(std::vector<HostPort> &nodeAddrs)
{
    // tbb concurrent hash table does not support thread-safe iteration
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!clusterNodeTable_.empty(), K_NOT_FOUND,
                             "Cluster node table should never be empty. It should at least contain itself");
    for (const auto &iter : clusterNodeTable_) {
        nodeAddrs.push_back(iter.first);
    }
    return Status::OK();
}

Status EtcdClusterManager::GetMasterAddr(const std::string &objKey, HostPort &masterAddr)
{
    std::string dbName;
    RETURN_IF_NOT_OK(GetPrimaryReplicaLocationByObjectKey(objKey, masterAddr, dbName));
    g_MetaRocksDbName = dbName;
    return Status::OK();
}

worker::HashRange EtcdClusterManager::GetHashRangeNonBlock()
{
    return hashRing_->GetHashRangeNonBlock();
}

bool EtcdClusterManager::NeedRedirect(const std::string &objKey, HostPort &masterAddr)
{
    return hashRing_->NeedRedirect(objKey, masterAddr);
}

Status EtcdClusterManager::ProcessGetMetaAddressIfAllowMetaAccessAcrossAZWithWorkerId(
    const std::string &objKey, const std::string &workerIdInObjKey, std::string &dbName, HostPort &masterAddr,
    bool &isFromOtherAz, std::optional<RouteInfo> &routeInfo)
{
    auto rc = hashRing_->GetUuidInCurrCluster(workerIdInObjKey, dbName, routeInfo);
    RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);

    if (rc.GetCode() != K_NOT_FOUND) {
        std::string destWorkerUuid;
        RETURN_IF_NOT_OK(
            ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destWorkerUuid));
        RETURN_IF_NOT_OK(hashRing_->GetWorkerAddrByUuidForAddressing(destWorkerUuid, masterAddr));
        VLOG(1) << FormatString("Object: %s, dbName: %s, primary replica location: %s, %s", objKey, dbName,
                                masterAddr.ToString(), destWorkerUuid);
        return Status::OK();
    }

    for (auto &i : otherAzHashRings_) {
        if (i.second->GetUuidInCurrCluster(workerIdInObjKey, dbName, routeInfo).IsOk()) {
            VLOG(1) << FormatString("%s is in az: %s", objKey, i.first);
            isFromOtherAz = true;
            std::string destWorkerUuid;
            RETURN_IF_NOT_OK(
                ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destWorkerUuid));
            RETURN_IF_NOT_OK(i.second->GetWorkerAddrByUuidForAddressing(destWorkerUuid, masterAddr));
            VLOG(1) << FormatString("Object: %s, dbName: %s, primary replica location: %s, %s", objKey, dbName,
                                    masterAddr.ToString(), destWorkerUuid);
            return Status::OK();
        }
    }
    RETURN_STATUS(K_NOT_FOUND, "Cannot find workerId: " + workerIdInObjKey);
}

Status EtcdClusterManager::ProcessGetMetaAddressIfNotAllowMetaAccessAcrossAZWithWorkerId(
    const std::string &workerIdInObjKey, std::string &dbName, HostPort &masterAddr, std::optional<RouteInfo> &routeInfo)
{
    RETURN_IF_NOT_OK(hashRing_->GetUuidInCurrCluster(workerIdInObjKey, dbName, routeInfo));
    std::string destWorkerUuid;
    RETURN_IF_NOT_OK(ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destWorkerUuid));
    RETURN_IF_NOT_OK(hashRing_->GetWorkerAddrByUuidForAddressing(destWorkerUuid, masterAddr));
    return Status::OK();
}

Status EtcdClusterManager::ProcessGetMetaAddressByHash(const std::string &objKey, std::string &dbName,
                                                       HostPort &masterAddr, std::optional<RouteInfo> &routeInfo)
{
    RETURN_IF_NOT_OK(hashRing_->GetPrimaryWorkerUuid(objKey, dbName, routeInfo));
    std::string destWorkerUuid;
    RETURN_IF_NOT_OK(ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destWorkerUuid));
    RETURN_IF_NOT_OK(hashRing_->GetWorkerAddrByUuidForMultiReplica(destWorkerUuid, masterAddr));
    return Status::OK();
}

Status EtcdClusterManager::GetMetaAddressNotCheckConnection(const std::string &objKey, MetaAddrInfo &metaAddrInfo,
                                                            std::optional<RouteInfo> &routeInfo)
{
    Timer timer;
    bool isFromOtherAz = false;
    HostPort masterAddr;
    std::string dbName;
    if (IsCentralized()) {
        masterAddr.ParseString(FLAGS_master_address);
        metaAddrInfo.SetAddress(masterAddr);
        return Status::OK();
    }
    std::string workerIdInObjKey;
    bool hasWorkerId = TrySplitWorkerIdFromObjecId(objKey, workerIdInObjKey).IsOk();
    if (!hasWorkerId) {
        RETURN_IF_NOT_OK(ProcessGetMetaAddressByHash(objKey, dbName, masterAddr, routeInfo));
    } else if (FLAGS_cross_cluster_get_meta_from_worker) {
        RETURN_IF_NOT_OK(ProcessGetMetaAddressIfAllowMetaAccessAcrossAZWithWorkerId(
            objKey, workerIdInObjKey, dbName, masterAddr, isFromOtherAz, routeInfo));
    } else {
        RETURN_IF_NOT_OK(ProcessGetMetaAddressIfNotAllowMetaAccessAcrossAZWithWorkerId(workerIdInObjKey, dbName,
                                                                                       masterAddr, routeInfo));
    }

    metaAddrInfo.SetAddress(masterAddr);
    metaAddrInfo.SetDbName(dbName);
    if (isFromOtherAz) {
        metaAddrInfo.MarkMetaIsFromOtherAz();
    }
    workerOperationTimeCost.Append("GetMetaAddress", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status EtcdClusterManager::GetMetaAddress(const std::string &objKey, MetaAddrInfo &metaAddrInfo)
{
    std::optional<RouteInfo> routeInfo;
    RETURN_IF_NOT_OK(GetMetaAddressNotCheckConnection(objKey, metaAddrInfo, routeInfo));
    const auto &masterAddr = metaAddrInfo.GetAddress();
    Status rc;
    if (metaAddrInfo.IsFromOtherAz()) {
        if (!CheckIfOtherAzNodeConnected(masterAddr)) {
            rc = Status(K_RPC_UNAVAILABLE, FormatString("The other az node %s disconnected.", masterAddr.ToString()));
        }
    } else {
        rc = CheckConnection(masterAddr);
    }
    if (rc.IsError()) {
        metaAddrInfo.Clear();
        RETURN_STATUS(K_RPC_UNAVAILABLE, rc.GetMsg());
    }
    return Status::OK();
}

void EtcdClusterManager::GetObjectKeysFromNotConnectedMaster(
    const std::unordered_map<MetaAddrInfo, std::vector<std::string>> &metaAddrInfos,
    std::unordered_set<std::string> &objectKeys)
{
    for (const auto &[metaAddrInfo, keys] : metaAddrInfos) {
        const auto &masterAddr = metaAddrInfo.GetAddress();
        if (metaAddrInfo.IsFromOtherAz()) {
            if (!CheckIfOtherAzNodeConnected(masterAddr)) {
                for (const auto &key : keys) {
                    objectKeys.emplace(key);
                }
            }
        } else {
            if (CheckConnection(masterAddr).IsError()) {
                for (const auto &key : keys) {
                    objectKeys.emplace(key);
                }
            };
        }
    }
}

Status EtcdClusterManager::GetPrimaryReplicaLocationByObjectKey(const std::string &objectKey, HostPort &masterAddr,
                                                                std::string &dbName)
{
    if (IsCentralized()) {
        if (FLAGS_master_address.empty()) {
            RETURN_STATUS(
                K_RUNTIME_ERROR,
                "When disable consistent hash(enable_distribute_master = false or etcd_address is invalid), the "
                "master_address should not empty.");
        }
        return masterAddr.ParseString(FLAGS_master_address);
    }
    std::string destWorkerUuid;
    RETURN_IF_NOT_OK(hashRing_->GetMasterUuid(objectKey, dbName));
    RETURN_IF_NOT_OK(ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destWorkerUuid));
    auto rc = hashRing_->GetWorkerAddrByUuidForMultiReplica(destWorkerUuid, masterAddr);
    VLOG(1) << FormatString("Object: %s, dbName: %s, primary replica location: %s, %s", objectKey, dbName,
                            masterAddr.ToString(), destWorkerUuid);
    return rc;
}

Status EtcdClusterManager::GetPrimaryReplicaLocationByAddr(const std::string &address, HostPort &masterAddr,
                                                           std::string &dbName)
{
    std::string destUuid;
    constexpr int intervalMs = 100;
    auto realRetryTimeMs = reqTimeoutDuration.CalcRemainingTime();
    Timer timer;
    Status status;
    while (timer.ElapsedMilliSecond() < realRetryTimeMs) {
        status = hashRing_->GetUuidByWorkerAddr(address, dbName);
        if (status.IsOk()) {
            break;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        }
    }
    RETURN_IF_NOT_OK(status);
    RETURN_IF_NOT_OK(ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destUuid));
    return hashRing_->GetWorkerAddrByUuidForMultiReplica(destUuid, masterAddr);
}

Status EtcdClusterManager::GetPrimaryReplicaDbNames(const HostPort &address, std::vector<std::string> &dbNames)
{
    if (IsCentralized()) {
        dbNames.emplace_back("");
        return Status::OK();
    }
    std::string workerUuid;
    auto rc = hashRing_->GetUuidByWorkerAddr(address.ToString(), workerUuid);
    if (rc.GetCode() == K_NOT_FOUND) {
        for (auto iter = otherAzHashRings_.begin(); iter != otherAzHashRings_.end() && rc.GetCode() == K_NOT_FOUND;
             ++iter) {
            rc = iter->second->GetUuidByWorkerAddr(address.ToString(), workerUuid);
        }
    }
    RETURN_IF_NOT_OK(rc);
    if (!MultiReplicaEnabled()) {
        dbNames.emplace_back(workerUuid);
        return Status::OK();
    }
    return ReplicaMagagerEvent::GetPrimaryReplicaDbNames::GetInstance().NotifyAll(workerUuid, dbNames);
}

std::set<std::string> EtcdClusterManager::GetNodesInTable()
{
    std::set<std::string> nodes;
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        for (const auto &iter : clusterNodeTable_) {
            nodes.emplace(iter.first.ToString());
        }
    }
    {
        std::lock_guard<std::shared_timed_mutex> lck(otherClusterNodeMutex_);
        for (const auto &iter : otherClusterNodeTable_) {
            nodes.emplace(iter.first.ToString());
        }
    }
    return nodes;
}

void EtcdClusterManager::CompleteNodeTableWithFakeNode()
{
    // Assume that a cluster consists of nodeA, nodeB and nodeC. When the cluster restarts after node_timeout_s, the
    // ETCD_CLUSTER_TABLE in etcd will be empty and SetupInitialClusterNodes cannot get any value.
    // If nodeC fails to be pulled up when cluster restarts, the cluster node table will miss nodeC. In this case, nodeC
    // is in hash ring but cannot be scaled down due to lack of the timeout event of nodeC triggered.
    // To solve it, we will complete the node table with fake event at first. If nodeC cannot be pulled up in time, we
    // put a fake nodeC add-and-remove event in the queue, so that nodeC can be scaled down after node_dead_timeout_s.
    // Furthermore, to prevent the remove event causing incorrectly removal of real node, verification will be made in
    // function HandleClusterEvent.
    auto nodes = GetNodesInTable();
    auto workers = GetValidWorkersInHashRing();
    std::vector<std::string> lackNodes;
    std::set_difference(workers.begin(), workers.end(), nodes.begin(), nodes.end(), std::back_inserter(lackNodes));

    for (auto i : lackNodes) {
        CompleteNodeTableWithFakeNode(i);
    }
}

void EtcdClusterManager::CompleteNodeTableWithFakeNode(const std::string &lackNode)
{
    // Enqueue a fake event and let the background thread to handle it.
    LOG(INFO) << "Create fake add-and-remove event of node " << lackNode << " to priority queue.";
    mvccpb::Event fakeAddEvent;
    auto fakeKv = fakeAddEvent.mutable_kv();
    // Key is the string of HostPort. Value is the timestamp with additionType.
    fakeKv->set_key(clusterPrefix_ + "/" + lackNode);
    fakeKv->set_value(FAKE_NODE_EVENT_VALUE);
    fakeAddEvent.set_type(mvccpb::Event_EventType::Event_EventType_PUT);

    mvccpb::Event fakeDeleteEvent = fakeAddEvent;
    fakeDeleteEvent.set_type(mvccpb::Event_EventType::Event_EventType_DELETE);

    if (eventPq_ && thread_) {
        eventPq_->EmplaceBack(new CmEvent(std::move(fakeAddEvent), PrefixType::CLUSTER));
        eventPq_->EmplaceBack(new CmEvent(std::move(fakeDeleteEvent), PrefixType::CLUSTER));
    }
}

Status EtcdClusterManager::WaitNodeJoinToTable()
{
    using namespace std::chrono;
    std::vector<std::pair<std::string, std::string>> localAzValue;
    RETURN_IF_NOT_OK(etcdDB_->GetAll(ETCD_CLUSTER_TABLE, localAzValue));
    auto initClusterTableNum = localAzValue.size();
    LOG(INFO) << "Start waiting for nodes to join the table, expect nodes num: " << initClusterTableNum;
    auto start = GetSteadyClockTimeStampUs();
    while ((GetSteadyClockTimeStampUs() - start)
           < duration_cast<microseconds>(seconds(TOTAL_WAIT_NODE_TABLE_TIME_SEC)).count()) {
        size_t currentNodeSize = GetNodeTableSize();
        if (currentNodeSize >= initClusterTableNum && IfFindWorkerInTheClusterNode(workerAddress_)) {
            LOG(INFO) << "Waiting for nodes to join table success, node size: " << currentNodeSize;
            return Status::OK();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_NODE_TABLE_INTERVAL_MS));
    }
    LOG(WARNING) << "Waiting for nodes to join table failed, cluster table size: " << GetNodeTableSize()
                 << " expect size: " << initClusterTableNum;
    return Status::OK();
}

void EtcdClusterManager::ScheduledCheckCompleteNodeTableWithFakeNode()
{
    if (!IsHealthy() || IsCentralized() || !hashRing_->IsWorkable()) {
        return;
    }

    auto nodes = GetNodesInTable();
    auto workers = GetValidWorkersInHashRing();
    std::vector<std::string> lackNodes;
    std::set_difference(workers.begin(), workers.end(), nodes.begin(), nodes.end(), std::back_inserter(lackNodes));
    const uint32_t SECOND_TO_MS = 1000;

    for (auto i : lackNodes) {
        if (ContainsKey(nodeTableCompletionTimer_, i)) {
            continue;
        }
        TimerQueue::TimerImpl timer;
        WARN_IF_ERROR(
            TimerQueue::GetInstance()->AddTimer(
                FLAGS_node_timeout_s * SECOND_TO_MS, [this, i]() { CompleteNodeTableWithFakeNode(i); }, timer),
            "Cannot add fake node of " + i);
        nodeTableCompletionTimer_.emplace(i, timer);
    }
}

Status EtcdClusterManager::CheckWaitNodeTableComplete()
{
    using namespace std::chrono;
    if (IsCentralized()) {
        return WaitNodeJoinToTable();
    }

    auto start = GetSteadyClockTimeStampUs();
    int hashWorkerNum = 0;
    auto rc = GetHashRingWorkerNum(hashWorkerNum);
    int tableSize = GetNodeTableSize();
    static const int RESERVED_TIME_SEC = 3;
    static const int TO_SECOND = 1000;
    static const int WAITING_TIME_FOR_EACH_NODE_MS = 300;
    uint32_t totalWaitTime = static_cast<uint32_t>(TOTAL_WAIT_NODE_TABLE_TIME_SEC);
    bool firstInit = hashRing_->IsInit();
    auto hashAddTime = FLAGS_add_node_wait_time_s + RESERVED_TIME_SEC;
    if (rc.IsError() && firstInit && hashAddTime > TOTAL_WAIT_NODE_TABLE_TIME_SEC) {
        totalWaitTime = hashAddTime;
    }
    INJECT_POINT("EtcdClusterManager.CheckWaitNodeTableComplete.waitTime", [&totalWaitTime](uint32_t injectWaitTime) {
        totalWaitTime = injectWaitTime;
        return Status::OK();
    });

    LOG(INFO) << "Begin to wait for the completion of node table. Plan to wait: " << hashWorkerNum
              << ". Current: " << tableSize << ", totalWaitTime:" << totalWaitTime << "s";
    while (!IsTermSignalReceived() && (rc.IsError() || tableSize < hashWorkerNum)
           && GetSteadyClockTimeStampUs() - start < duration_cast<microseconds>(seconds(totalWaitTime)).count()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_NODE_TABLE_INTERVAL_MS));
        auto lastrc = rc;
        rc = GetHashRingWorkerNum(hashWorkerNum);
        if (lastrc.IsError() && rc.IsOk() && firstInit) {
            if (hashAddTime + (static_cast<unsigned int>(hashWorkerNum) * WAITING_TIME_FOR_EACH_NODE_MS / TO_SECOND)
                > static_cast<uint64_t>(totalWaitTime)) {
                totalWaitTime =
                    static_cast<uint32_t>(hashAddTime + (hashWorkerNum * WAITING_TIME_FOR_EACH_NODE_MS / TO_SECOND));
            }
        }
        tableSize = GetNodeTableSize();
    }
    LOG(INFO) << "Finish waiting the node table. Current table size is " << tableSize
              << ", plan to wait: " << hashWorkerNum << ", is terminated: " << IsTermSignalReceived()
              << ", status: " << rc.ToString();

    RETURN_IF_NOT_OK(rc);
    if (tableSize < hashWorkerNum) {
        LOG(WARNING) << "The number of nodes recorded in cluster manager: " << tableSize
                     << " is not equal to the number of running workers in hashring: " << hashWorkerNum;
        CompleteNodeTableWithFakeNode();
    }

    INJECT_POINT("EtcdClusterManager.CheckWaitNodeTableComplete.returnError", [tableSize, hashWorkerNum]() {
        if (tableSize != hashWorkerNum) {
            RETURN_STATUS(K_NOT_READY, "size of table in cluster manager different from number of running workers.");
        }
        return Status::OK();
    });
    return Status::OK();
}

Status EtcdClusterManager::InformEtcdReconciliationDone()
{
    RETURN_IF_NOT_OK(etcdDB_->InformEtcdReconciliationDone(workerAddress_));
    return Status::OK();
}

std::vector<std::string> EtcdClusterManager::ClusterNodeTableToString()
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    std::vector<std::string> content;
    for (const auto &node : clusterNodeTable_) {
        const std::string &str = node.second->ToString(node.first);
        content.emplace_back(str);
    }
    return content;
}

bool EtcdClusterManager::IfFindWorkerInTheClusterNode(HostPort &workerAddress)
{
    typename TbbNodeTable::const_accessor accessor;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return clusterNodeTable_.find(accessor, workerAddress);
}

bool EtcdClusterManager::CheckEtcdStateWhenNetworkFailed()
{
    if (akSkManager_ == nullptr) {
        LOG(ERROR) << "akskManager is nullptr";
        return false;
    }

    auto remoteWorkerApiTable = std::vector<std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi>>();
    auto needWorkerNum = std::min(clusterNodeTable_.size(), MAX_QUERY_WORKER_NUM_FOR_RECONCILIATION);
    remoteWorkerApiTable.reserve(needWorkerNum);
    std::vector<std::string> activeOtherNodes;
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        for (auto &iter : clusterNodeTable_) {
            if (iter.first == workerAddress_ || !iter.second->IsActive()) {
                continue;
            }
            activeOtherNodes.emplace_back(iter.first.ToString());
        }
    }
    size_t curWorkerNum = 0;
    for (const auto &workerAddr : activeOtherNodes) {
        std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi> remoteWorkerApi;
        if (CreateRemoteWorkerApi(workerAddr, akSkManager_, remoteWorkerApi).IsOk()) {
            remoteWorkerApiTable.emplace_back(std::move(remoteWorkerApi));
            ++curWorkerNum;
        }
        if (curWorkerNum >= needWorkerNum) {
            break;
        }
    }
    std::stringstream askNodeInfo;
    for (auto &i : remoteWorkerApiTable) {
        askNodeInfo << i->Address() << ";";
    }
    LOG(INFO) << "The nodes to be queried are: " << askNodeInfo.str();

    std::unordered_map<std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi>, int64_t> api2Tag;
    for (const auto &remoteWorkerApi : remoteWorkerApiTable) {
        CheckEtcdStateReqPb req;
        int64_t tag;
        auto rc = remoteWorkerApi->CheckEtcdStateAsyncWrite(req, tag);
        if (rc.IsError()) {
            LOG(WARNING) << "Rpc write failed, with rc: " << rc.ToString();
            continue;
        };
        api2Tag.emplace(remoteWorkerApi, tag);
    }
    bool etcdAvailable = false;
    for (const auto &pair : api2Tag) {
        CheckEtcdStateRspPb rsp;
        if (pair.first->CheckEtcdStateAsyncRead(pair.second, rsp) && !etcdAvailable && rsp.available()) {
            LOG(INFO) << pair.first->Address() << " confirms that etcd is OK";
            etcdAvailable = true;
            // In ZmqStubImpl, the resources corresponding to the tags will be cleaned up only after reading,
            // so all tags need to be read.
        }
    }
    return etcdAvailable;
}

std::string EtcdClusterManager::GetWorkerIdByWorkerAddr(const std::string &address) const
{
    std::string workerId;
    LOG_IF_ERROR(hashRing_->GetUuidByWorkerAddr(address, workerId), "Cannot find workerid of " + address);
    return workerId;
}

bool EtcdClusterManager::MultiReplicaEnabled()
{
    return multiReplicaEnabled_;
}

Status EtcdClusterManager::QueryMasterAddrInOtherAz(const std::string &otherAZName, const std::string &objKey,
                                                    MetaAddrInfo &metaAddrInfo)
{
    HostPort masterHostPort;
    std::string dbName;
    auto iter = otherAzHashRings_.find(otherAZName);
    if (iter != otherAzHashRings_.end()) {
        auto &readRing = iter->second;
        RETURN_IF_NOT_OK(readRing->GetMasterUuid(objKey, dbName));
        std::string destWorkerUuid;
        RETURN_IF_NOT_OK(
            ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destWorkerUuid));
        RETURN_IF_NOT_OK(readRing->GetWorkerAddrByUuidForMultiReplica(destWorkerUuid, masterHostPort));
        CHECK_FAIL_RETURN_STATUS(
            CheckIfOtherAzNodeConnected(masterHostPort), K_RPC_UNAVAILABLE,
            FormatString("check connection failed, az: %s, addr: %s", otherAZName, masterHostPort.ToString()));
    } else {
        RETURN_STATUS(K_NOT_FOUND, "Cannot find az: " + otherAZName);
    }
    metaAddrInfo.SetAddress(masterHostPort);
    metaAddrInfo.SetDbName(dbName);
    metaAddrInfo.MarkMetaIsFromOtherAz();
    return Status::OK();
}

Status EtcdClusterManager::GetNodeInGivenOtherAzByHash(const std::string &objKey, const std::string &azName,
                                                       MetaAddrInfo &metaAddrInfo)
{
    auto itr = otherAzHashRings_.find(azName);
    CHECK_FAIL_RETURN_STATUS(itr != otherAzHashRings_.end(), K_NOT_FOUND, "can not find hash ring of az: " + azName);
    const auto &otherAzHashRing = itr->second;
    CHECK_FAIL_RETURN_STATUS(otherAzHashRing->IsWorkable(), K_NOT_READY,
                             FormatString("hash ring of az[%s] is not ready", azName));
    std::string dbName;
    std::optional<RouteInfo> routeInfo;
    RETURN_IF_NOT_OK(otherAzHashRing->GetPrimaryWorkerUuid(objKey, dbName, routeInfo));
    std::string destWorkerUuid;
    RETURN_IF_NOT_OK(ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destWorkerUuid));
    HostPort masterAddr;
    RETURN_IF_NOT_OK(otherAzHashRing->GetWorkerAddrByUuidForMultiReplica(destWorkerUuid, masterAddr));
    metaAddrInfo.SetAddress(std::move(masterAddr));
    metaAddrInfo.SetDbName(std::move(dbName));
    return Status::OK();
}

Status EtcdClusterManager::GetAllNodesInOtherAzsByHash(const std::string &objKey,
                                                       std::unordered_map<std::string, MetaAddrInfo> &metaAddrInfos,
                                                       bool allowNotReady)
{
    for (const auto &kv : otherAzHashRings_) {
        MetaAddrInfo metaAddrInfo;
        auto rc = GetNodeInGivenOtherAzByHash(objKey, kv.first, metaAddrInfo);
        if (rc.GetCode() == K_NOT_FOUND) {
            LOG(WARNING) << "Get node in given other az by hash failed: " << rc.ToString();
            continue;
        }
        if (rc.GetCode() == K_NOT_READY && allowNotReady) {
            continue;
        }
        RETURN_IF_NOT_OK(rc);
        metaAddrInfos.insert({ kv.first, std::move(metaAddrInfo) });
    }
    return Status::OK();
}

Status EtcdClusterManager::GetMasterAddrInOtherAzForHashKey(
    const std::unordered_map<std::string, std::unique_ptr<worker::ReadHashRing>>::iterator &iter,
    const std::string &objKey, HostPort &masterHostPort, std::string &dbName)
{
    std::optional<RouteInfo> routeInfo;
    RETURN_IF_NOT_OK(iter->second->GetPrimaryWorkerUuid(objKey, dbName, routeInfo));
    std::string destWorkerUuid;
    RETURN_IF_NOT_OK(ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().NotifyAll(dbName, destWorkerUuid));
    return iter->second->GetWorkerAddrByUuidForMultiReplica(destWorkerUuid, masterHostPort);
}

std::string EtcdClusterManager::GetOtherAzNameByWorkerIdInefficient(const std::string &workerId)
{
    std::string dbName;
    std::optional<RouteInfo> routeInfo;
    for (auto &i : otherAzHashRings_) {
        if (i.second->GetUuidInCurrCluster(workerId, dbName, routeInfo).IsOk()) {
            VLOG(1) << FormatString("%s is in az: %s", workerId, i.first);
            return i.first;
        }
    }
    return "";
}

std::string EtcdClusterManager::GetWorkerAddress() const
{
    return workerAddress_.ToString();
}

Status EtcdClusterManager::GetHashRingWorkerNum(int &workerNum, bool withOtherAz) const
{
    RETURN_IF_NOT_OK(hashRing_->GetHashRingWorkerNum(workerNum));
    if (withOtherAz) {
        for (const auto &otherAzHashRing : otherAzHashRings_) {
            int otherAzWorkerNum = 0;
            auto rc = otherAzHashRing.second->GetHashRingWorkerNum(otherAzWorkerNum);
            if (rc.GetCode() == K_NOT_READY) {
                LOG(WARNING) << FormatString("The hash ring of az[%s] is not ready, rc: %s", otherAzHashRing.first,
                                             rc.ToString());
                continue;
            }
            workerNum += otherAzWorkerNum;
        }
    }
    return Status::OK();
}

Status EtcdClusterManager::CreateEtcdStoreTable(EtcdStore *etcdStore)
{
    RETURN_IF_NOT_OK_EXCEPT(etcdStore->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX), K_DUPLICATED);
    RETURN_IF_NOT_OK_EXCEPT(etcdStore->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)),
                            K_DUPLICATED);
    RETURN_IF_NOT_OK_EXCEPT(etcdStore->CreateTable(ETCD_REPLICA_GROUP_TABLE, ETCD_REPLICA_GROUP_TABLE), K_DUPLICATED);
    RETURN_IF_NOT_OK_EXCEPT(etcdStore->CreateTable(ETCD_MASTER_ADDRESS_TABLE, ETCD_MASTER_ADDRESS_TABLE), K_DUPLICATED);
    return Status::OK();
}

Status EtcdClusterManager::ConstructClusterInfoViaEtcd(EtcdStore *etcdStore, ClusterInfo &clusterInfo)
{
    RETURN_IF_NOT_OK(CreateEtcdStoreTable(etcdStore));
    RETURN_IF_NOT_OK(etcdStore->GetAll(ETCD_CLUSTER_TABLE, clusterInfo.workers, clusterInfo.revision));
    RETURN_IF_NOT_OK(
        etcdStore->GetOtherAzAllValue(ETCD_CLUSTER_TABLE, clusterInfo.revision, clusterInfo.otherAzWorkers));
    RETURN_IF_NOT_OK(etcdStore->GetOtherAzAllHashRing(clusterInfo.revision, clusterInfo.otherAzHashrings));
    RETURN_IF_NOT_OK(etcdStore->GetAll(ETCD_REPLICA_GROUP_TABLE, clusterInfo.revision, clusterInfo.replicaGroups));
    return Status::OK();
}

bool EtcdClusterManager::IfHitCacheWhenRouting(const std::string &objectKey, WorkerId2MetaInfoType &workerId2MetaInfo,
                                               Hash2MetaInfoType &hash2MetaInfo, std::optional<Status> &rc,
                                               MetaAddrInfo &metaAddrInfo)
{
    auto preReturnIfHitCache = [&](const MetaAddrInfo &dest) {
        metaAddrInfo = dest;
        if (rc) {
            rc = metaAddrInfo.GetRc();
        }
    };
    auto workerId = SplitWorkerIdFromObjecId(objectKey);
    if (!workerId.empty()) {
        auto ptr = FindHetero(workerId2MetaInfo, workerId);
        if (!ptr) {
            return false;
        }
        preReturnIfHitCache(ptr->second);
        return true;
    }
    const auto &hashRingCache = hash2MetaInfo.first;
#ifdef WITH_TESTS
    std::stringstream ss;
    Raii raii([&ss, &metaAddrInfo]() {
        ss << ", dest: " << (metaAddrInfo.Empty() ? std::string("NONE") : metaAddrInfo.GetAddress().ToString());
        VLOG(1) << "====IfHitCacheWhenRouting====: " << ss.str();
    });
    ss << "objectKey: " << objectKey << ", hash map: ";
    for (const auto &kv : hashRingCache) {
        ss << "{" << kv.first << ", [[" << kv.second.first.first << ", " << kv.second.first.second << "], "
           << kv.second.second.GetAddress() << "]}";
    }
#endif
    if (hashRingCache.empty()) {
        return false;
    }
    auto hash = MurmurHash3_32(objectKey);
#ifdef WITH_TESTS
    ss << ", hash: " << hash;
#endif
    auto it = hashRingCache.upper_bound(hash);
    if (it == hashRingCache.end()) {
        return false;
    }
    if (it->first == std::numeric_limits<HashPosition>::max()) {
        if (it->second.first.first > hash) {
            return false;
        }
        preReturnIfHitCache(it->second.second);
        return true;
    }
    if (it != hashRingCache.begin()) {
        if (it->second.first.first <= hash) {
            preReturnIfHitCache(it->second.second);
            return true;
        }
        return false;
    }
    auto rIt = hashRingCache.rbegin();
    if (rIt->first == std::numeric_limits<HashPosition>::max() && rIt->second.first.second > hash) {
        preReturnIfHitCache(rIt->second.second);
        return true;
    }
    return false;
}

void EtcdClusterManager::ProcessNotHitCacheWhenRouting(const std::string &objectKey,
                                                       WorkerId2MetaInfoType &workerId2MetaInfo,
                                                       Hash2MetaInfoType &hash2MetaInfo, std::optional<Status> &rc,
                                                       MetaAddrInfo &metaAddrInfo, bool disableCache)
{
    std::optional<RouteInfo> routeInfo;
    routeInfo.emplace();
    Status tmpRc = GetMetaAddressNotCheckConnection(objectKey, metaAddrInfo, routeInfo);
    if (tmpRc.IsError()) {
        metaAddrInfo.UpdateRc(tmpRc);
    }
    if (rc) {
        rc = std::move(tmpRc);
    }
    if (disableCache) {
        return;
    }
    std::visit(
        [&](auto &&arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                return;
            } else if constexpr (std::is_same_v<T, std::string>) {
                workerId2MetaInfo.emplace(arg, metaAddrInfo);
            } else if constexpr (std::is_same_v<T, Range>) {
                auto &cacheVersion = hash2MetaInfo.second;
                auto &cache = hash2MetaInfo.first;
                if (cacheVersion != routeInfo->currHashRingVersion) {
                    cache.clear();
                    cacheVersion = routeInfo->currHashRingVersion;
                }
                cache.emplace(arg.first > arg.second ? std::numeric_limits<HashPosition>::max() : arg.second,
                              std::make_pair(arg, metaAddrInfo));
            } else {
                LOG(ERROR) << "Unexpected behavior";
            }
        },
        routeInfo->payload);
}

void EtcdClusterManager::FetchDestAddrFromAnywhere(const std::string &objectKey,
                                                   WorkerId2MetaInfoType &workerId2MetaInfo,
                                                   Hash2MetaInfoType &hash2MetaInfo, std::optional<Status> &rc,
                                                   MetaAddrInfo &metaAddrInfo, bool disableCache)
{
    if (disableCache) {
        ProcessNotHitCacheWhenRouting(objectKey, workerId2MetaInfo, hash2MetaInfo, rc, metaAddrInfo, true);
        return;
    }
    if (!IfHitCacheWhenRouting(objectKey, workerId2MetaInfo, hash2MetaInfo, rc, metaAddrInfo)) {
        ProcessNotHitCacheWhenRouting(objectKey, workerId2MetaInfo, hash2MetaInfo, rc, metaAddrInfo, false);
    }
}

std::string ClusterInfo::ToString()
{
    std::stringstream msg;
    msg << std::endl << "local hash ring: ";
    for (const auto &pair : localHashRing) {
        msg << pair.first << ", ";
    }
    msg << std::endl << "other az hash ring: ";
    for (const auto &pair : otherAzHashrings) {
        msg << pair.first << ", ";
    }
    msg << std::endl << "local workers: ";
    for (const auto &pair : workers) {
        msg << pair.first << ", ";
    }
    msg << std::endl << "other az workers: ";
    for (const auto &pair : otherAzWorkers) {
        msg << pair.first << ", ";
    }
    msg << std::endl << "replicaGroups: " << std::endl;
    for (const auto &pair : replicaGroups) {
        msg << pair.first << ", ";
    }
    return msg.str();
}
}  // namespace datasystem
