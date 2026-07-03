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
#include "datasystem/worker/cluster_manager/cluster_manager.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/worker/cluster_manager/cluster_constants.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/topology/membership/worker_node_info.h"
#include "datasystem/topology/routing/worker_locator.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/cluster_manager/cluster_node.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/worker/worker_update_flag_check.h"

DS_DECLARE_int32(heartbeat_interval_ms);
DS_DECLARE_string(etcd_address);
DS_DECLARE_uint32(node_timeout_s);
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(add_node_wait_time_s);
DS_DECLARE_string(master_address);
DS_DECLARE_string(cluster_name);
DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_bool(auto_del_dead_node);
DS_DEFINE_bool(cross_cluster_get_meta_from_worker, false,
               "[DEPRECATED] Cross-cluster metadata access from workers has been removed. This flag is kept for "
               "compatibility and is ignored.");

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace datasystem {
// fake event, only be processed when key not exists in cluster node table.
static const std::string FAKE_NODE_EVENT_VALUE = "0;start";
static constexpr int TOTAL_WAIT_NODE_TABLE_TIME_SEC = 60;  // total time of waiting node table complete.
static constexpr int WAIT_NODE_TABLE_INTERVAL_MS = 10;     // interval of waiting node table complete.
static constexpr int NO_PROGRESS_TIMEOUT_SEC = 10;         // terminate wait early if no new nodes discovered.
static const std::string ETCD_CLUSTER_SUBSCRIBER = "ClusterManager";

namespace {
worker::HashRange ToHashRange(const std::vector<topology::RoutingRange> &units)
{
    worker::HashRange ranges;
    ranges.reserve(units.size());
    for (const auto &unit : units) {
        ranges.emplace_back(unit.rangeBegin, unit.rangeEnd);
    }
    return ranges;
}

Status LoadRoutingSnapshot(const std::shared_ptr<topology::IRoutingView> &routingView,
                           std::shared_ptr<const topology::RoutingSnapshot> &snapshot)
{
    CHECK_FAIL_RETURN_STATUS(routingView != nullptr, K_NOT_READY, "Routing view is not provided.");
    return routingView->GetSnapshot(snapshot);
}

Status ResolveWorkerAddress(const std::shared_ptr<topology::IPlacementDirectory> &directory,
                            const std::string &workerId, std::string &address)
{
    CHECK_FAIL_RETURN_STATUS(directory != nullptr, K_NOT_READY, "Placement directory is not provided.");
    topology::PlacementEndpoint endpoint;
    RETURN_IF_NOT_OK(directory->ResolveWorker(workerId, endpoint));
    CHECK_FAIL_RETURN_STATUS(!endpoint.address.Empty(), K_NOT_FOUND, "Worker endpoint address is empty.");
    address = endpoint.address.ToString();
    return Status::OK();
}

bool IsDirectoryLagStatus(const Status &status)
{
    return status.GetCode() == K_NOT_READY || status.GetCode() == K_NOT_FOUND;
}
}  // namespace

ClusterManager::ClusterNode::ClusterNode(const std::string &timeEpoch, const std::string &additionEventType)
    : timeEpoch_(timeEpoch), additionEventType_(additionEventType), state_(NodeState::ACTIVE)
{
}

bool ClusterManager::ClusterNode::DemoteTimedOutNode()
{
    // Both FLAGS_node_dead_timeout_s and FLAGS_node_timeout_s time from when a node loses contact with ETCD.
    if (state_ == NodeState::TIMEOUT && FLAGS_node_dead_timeout_s > FLAGS_node_timeout_s
        && timeoutStamp_.ElapsedSecond() > (FLAGS_node_dead_timeout_s - FLAGS_node_timeout_s)) {
        state_ = NodeState::FAILED;
        return true;
    }
    return false;
}

ClusterManager::ClusterManager(const HostPort &workerAddress, const HostPort &masterAddress,
                               topology::ICoordinationBackend *clusterStore, std::shared_ptr<AkSkManager> akSkManager,
                               const int pqSize)
    : workerAddress_(workerAddress),
      masterAddress_(masterAddress),
      clusterStore_(clusterStore),
      akSkManager_(std::move(akSkManager))
{
    hashRing_ = std::make_unique<worker::HashRing>(workerAddress.ToString(), clusterStore_);
    placementDirectory_ = std::make_shared<topology::PlacementDirectory>();
    routingView_ = hashRing_->GetRoutingView();
    auto workerLocator = std::make_shared<topology::WorkerLocator>(routingView_, placementDirectory_);
    auto redirectPolicy = std::make_shared<topology::RedirectPolicy>(routingView_, placementDirectory_, workerLocator);
    placementFacade_ = std::make_shared<topology::PlacementFacade>(workerLocator, redirectPolicy);
    eventPq_ = std::make_unique<PriorityQueue<std::unique_ptr<CmEvent>, CmEventCmp>>(pqSize);
    workerWaitPost_ = std::make_unique<WaitPost>();

    clusterStore_->SetCheckStoreStateWhenNetworkFailedHandler(
        std::bind(&ClusterManager::CheckCoordinatorStateWhenNetworkFailed, this));

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
}

ClusterManager::~ClusterManager()
{
    LOG(INFO) << "ClusterManager exit";
    Status rc = Shutdown();
    if (rc.IsError()) {
        LOG(WARNING) << "Errors from shutdown during destructor. Error ignored: " << rc.ToString();
    }
}

Status ClusterManager::Shutdown()
{
    HashRingEvent::SyncClusterNodes::GetInstance().RemoveSubscriber(ETCD_CLUSTER_SUBSCRIBER);
    HashRingEvent::GetFailedWorkers::GetInstance().RemoveSubscriber(ETCD_CLUSTER_SUBSCRIBER);
    HashRingEvent::GetDbPrimaryLocation::GetInstance().RemoveSubscriber(ETCD_CLUSTER_SUBSCRIBER);
    GetHashRangeNonBlockEvent::GetInstance().RemoveSubscriber("GET_HASH_RANGE_NON_BLOCK");
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

void ClusterManager::SetWorkerReady()
{
    workerWaitPost_->Set();
}

Status ClusterManager::SetupInitialClusterNodes(const ClusterInfo &clusterInfo)
{
    // Get existing active nodes
    LOG(INFO) << "Query etcd to identify nodes from local az success. Number of nodes: " << clusterInfo.workers.size();
    for (const auto &node : clusterInfo.workers) {
        // node.first is the HostPort string key.  node.second is the timestamp
        // Enqueue a fake event and let the background thread to handle it.
        if (workerAddress_.ToString() == node.first && clusterInfo.coordinatorAvailable) {
            continue;
        }
        topology::CoordinationEvent fakeEvent{ topology::CoordinationEventType::PUT, clusterPrefix_ + "/" + node.first,
                                               node.second };
        LOG(INFO) << "Adding key: " << node.first << " value: " << node.second << " to priority queue.";
        RETURN_IF_NOT_OK(eventPq_->EmplaceBack(new CmEvent(std::move(fakeEvent), PrefixType::CLUSTER)));
    }
    return Status::OK();
}

bool ClusterManager::IsInRange(const worker::HashRange &ranges, const std::string &objKey)
{
    if (placementFacade_ == nullptr) {
        return false;
    }
    return placementFacade_->IsInRange(objKey, ranges);
}

Status ClusterManager::LocateMetaOwner(const std::string &objKey, bool requireAvailableTarget,
                                       MetaAddrInfo &metaAddrInfo)
{
    CHECK_FAIL_RETURN_STATUS(placementFacade_ != nullptr, K_NOT_READY, "Placement facade is not provided.");
    topology::RouteOptions options;
    options.centralizedMode = IsCentralized();
    options.requireAvailableTarget = requireAvailableTarget;
    options.masterAddress = masterAddress_;
    topology::RouteDecision decision;
    RETURN_IF_NOT_OK(placementFacade_->LocateMetaOwner(objKey, options, decision));
    metaAddrInfo = decision.ToMetaAddrInfo();
    return Status::OK();
}

bool ClusterManager::EvaluateRedirect(const std::string &key, std::string &newAddr)
{
    if (placementFacade_ == nullptr) {
        return false;
    }
    topology::RouteOptions options;
    options.centralizedMode = IsCentralized();
    options.masterAddress = masterAddress_;
    topology::RedirectDecision decision;
    if (placementFacade_->EvaluateRedirect(key, options, decision).IsError()
        || decision.action != topology::RedirectAction::REDIRECT) {
        return false;
    }
    newAddr = decision.targetEndpoint.address.ToString();
    return true;
}

bool ClusterManager::IsPreLeaving(const std::string &workerAddr)
{
    return hashRing_->IsPreLeaving(workerAddr);
}

Status ClusterManager::GetNodeAddrListFromEtcd(std::vector<HostPort> &nodeAddrs)
{
    std::vector<std::pair<std::string, std::string>> activeNodes;
    RETURN_IF_NOT_OK(clusterStore_->GetAll(CLUSTER_TABLE, activeNodes));
    nodeAddrs.clear();
    nodeAddrs.resize(activeNodes.size());
    for (size_t i = 0; i < activeNodes.size(); ++i) {
        RETURN_IF_NOT_OK(nodeAddrs[i].ParseString(activeNodes[i].first));
    }
    CHECK_FAIL_RETURN_STATUS(nodeAddrs.size() == activeNodes.size(), StatusCode::K_RUNTIME_ERROR,
                             "Failed to parse all addresses.");
    return Status::OK();
}

Status ClusterManager::Init(const ClusterInfo &clusterInfo)
{
    LOG(INFO) << "Init cluster manager.";
    auto traceId = Trace::Instance().GetTraceID();
    isEtcdAvailableWhenStart_ = clusterInfo.coordinatorAvailable;
    clusterStore_->SetEventHandler([this, traceId](topology::CoordinationEvent &&event) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        EnqueEvent(std::move(event));
    });
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        FLAGS_node_dead_timeout_s > FLAGS_node_timeout_s, K_INVALID,
        "The value of node_dead_timeout_s must be greater than the value of node_timeout_s.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        WorkerValidateHeartbeatIntervalMs(static_cast<uint32_t>(FLAGS_heartbeat_interval_ms)), K_INVALID,
        "invalid heartbeat_interval_ms.");

    // 2. Get prefix, otherwise the later background thread will have data race on these strings.
    RETURN_IF_NOT_OK(clusterStore_->GetStorePrefix(HASHRING_TABLE, ringPrefix_));
    RETURN_IF_NOT_OK(clusterStore_->GetStorePrefix(CLUSTER_TABLE, clusterPrefix_));

    // 3. Launch the background thread, as the hashring relies on it to become RUNNING. We want to start hashring
    // as early as possible. Also, cluster manager needs this thread to to add nodes to its node table.
    // This thread monitors timed out nodes and demotes them to failed nodes. It also tries to generate hash tokens,
    // to give up reconciliation when there is timeout, and to handle etcd events (ring, node addition, node removal).
    RETURN_IF_NOT_OK(StartBackgroundThread());

    RETURN_IF_NOT_OK(SetupInitialClusterNodes(clusterInfo));

    // 4. Watch for future changes to etcd table under directory /datasystem/cluster and /datasystem/ring)
    // The watch thread will enqueue the events in priority queue and the background thread will fetch the events
    // and handle them.
    RETURN_IF_NOT_OK(clusterStore_->WatchEvents(
        { { HASHRING_TABLE, "", clusterInfo.revision }, { CLUSTER_TABLE, "", clusterInfo.revision } }));
    // 5. Since the background and watch threads are up, it is time to initialize the hashring.
    if (clusterInfo.coordinatorAvailable) {
        RETURN_IF_NOT_OK(hashRing_->InitWithCoordinator());
    } else {
        RETURN_IF_NOT_OK(hashRing_->InitWithoutCoordinator(clusterInfo.localHashRing[0].second));
    }
    PublishPlacementDirectorySnapshot();

    if (masterAddress_.Empty()) {
        // The master address was not provided at the beginning. HashRing selected one of the nodes as master.
        // Reinitialize masterAddress_ with the selected node.
        Status s = masterAddress_.ParseString(FLAGS_master_address);
        if (s.IsError()) {
            LOG(WARNING) << "Could not get master address";
        }
    }

    bool isRestart = false;
    RETURN_IF_NOT_OK(IsRestart(isRestart));
    RETURN_IF_NOT_OK(
        clusterStore_->InitKeepAlive(CLUSTER_TABLE, workerAddress_.ToString(), isRestart, isEtcdAvailableWhenStart_));

    // Display the final list of nodes that were set up into the log
    LOG(INFO) << "Nodes tracked by cluster manager:\n" << this->NodesToString();

    return Status::OK();
}

Status ClusterManager::HandleFailedNodeToActive(const HostPort &eventNodeKey, ClusterNode *eventNode,
                                                ClusterNode *failedNode)
{
    if (eventNode->NodeWasRecovered() || eventNode->NodeWasDowngradeRestart()) {
        // The node was not restarted, so its a network recovery case of a node that did not actually crash
        RETURN_IF_NOT_OK(ProcessNetworkRecovery(eventNodeKey, failedNode, eventNode));
    } else {
        RemoveDeadWorkerEvent::GetInstance().NotifyAll(eventNodeKey.ToString());
    }
    INJECT_POINT("ClusterManager.HandleFailedNodeToActive.sleep");

    return Status::OK();
}

Status ClusterManager::AddNewNode(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode)
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

Status ClusterManager::HandleNodeStateToActive(const HostPort &eventNodeKey,
                                               const std::unique_ptr<ClusterNode> &eventNode, ClusterNode *foundNode,
                                               bool &isTimeout)
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
            RETURN_IF_NOT_OK(clusterStore_->UpdateNodeState(topology::MemberLifecycleState::READY));
        }
    } else {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Existing node has an unknown state during node addition event.");
    }
    return Status::OK();
}

Status ClusterManager::HandleNodeAdditionEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
                                               const std::string &azName)
{
    (void)azName;
    INJECT_POINT("ClusterManager.HandleNodeAdditionEvent.delay", [eventNodeKey](std::string addr) {
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

Status ClusterManager::HandleNodeRemoveEvent(const HostPort &eventNodeKey, std::unique_ptr<ClusterNode> eventNode,
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
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(foundNode != nullptr, K_RUNTIME_ERROR,
                                         "The timeout node is null in active nodes list");
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
    LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(workerAddr, true, false),
                 "Node timeout event process failed");
    LOG(INFO) << FormatString("Mark %s as timeout.", eventNodeKey.ToString());
    foundNode->SetTimedOut();
    return Status::OK();
}

Status ClusterManager::HandleExitingNodeRemoveEvent(const HostPort &eventNodeKey, const ClusterNode *eventNode,
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
        LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(workerAddr, true, true),
                     "Error occurs when voluntary scale down node mark timeout: "
                         + (eventNode == nullptr ? "" : eventNode->ToString(eventNodeKey)));
        // Trigger slot recovery for the crashed voluntary scale down node.
        NotifySlotRecovery({ eventNodeKey });
        foundNode->SetFailed();
        return Status::OK();
    }

    LOG(INFO) << "The voluntary scale down node finish, try remove worker " << workerAddr << " from cluster node table";
    LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(workerAddr, false, true),
                 "Node timeout event process failed");
    ChangePrimaryCopy::GetInstance().NotifyAll(workerAddr, true);
    RemoveDeadWorkerEvent::GetInstance().NotifyAll(workerAddr);
    (void)clusterNodeTable_.erase(accessor);
    HostPort addr = eventNodeKey;
    ClearWorkerMeta::GetInstance().NotifyAll(addr);
    EraseFailedNodeApiEvent::GetInstance().NotifyAll(addr);
    return Status::OK();
}

void ClusterManager::EnqueEvent(topology::CoordinationEvent &&event)
{
    INJECT_POINT("ClusterManager.EnqueEvent", [] { return; });
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
    const auto &key = event.key;
    Timer timer;
    const int maxEnqueueTimeMs = 100;
    const int logEveryN = 30;
    if (key.find(clusterPrefix_) != std::string::npos) {
        rc = eventPq_->EmplaceBack(new CmEvent(std::move(event), PrefixType::CLUSTER));
    } else if (key.find(ringPrefix_) != std::string::npos) {
        rc = eventPq_->EmplaceBack(new CmEvent(std::move(event), PrefixType::RING));
    } else {
        LOG(ERROR) << "Event of PrefixType::OTHER, no need to enqueue and handle it.";
    }
    LOG_IF_ERROR(rc, "Push an element to the priority queue failed");
    if (timer.ElapsedMilliSecond() > maxEnqueueTimeMs) {
        LOG_EVERY_N(WARNING, logEveryN) << "EnqueEvent ElapsedMilliSecond: " << timer.ElapsedMilliSecond()
                                        << ", eventPq_ size: " << eventPq_->Size();
    }
}

Status ClusterManager::DequeEventCallHandler(bool &isHandleEvent)
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
        LOG(INFO) << "Cache cluster event until hashring is workerable: " << toProcess->event.ToString();
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
            LOG(INFO) << "Nodes tracked by cluster manager:\n" << this->NodesToString();
            PublishPlacementDirectorySnapshot();
            break;
        case PrefixType::RING:
            RETURN_IF_NOT_OK(HandleRingEvent(toProcess->event));
            PublishPlacementDirectorySnapshot();
            break;
        default:
            LOG(WARNING) << "No handler for ETCD event " << toProcess->ToString();
    }
    return Status::OK();
}

Status ClusterManager::HandleRingEvent(const topology::CoordinationEvent &event)
{
    return hashRing_->HandleRingEvent(event, ringPrefix_);
}

Status ClusterManager::HandleClusterEvent(const topology::CoordinationEvent &event)
{
    Status rc;
    std::string nodeHostPortStr = event.key;
    std::string nodeTimestamp = event.value;
    // Parse the type of event of addition appended to timestamp.
    std::string additionEventType;  // "start", "restart", "recover"
    if (event.type == topology::CoordinationEventType::PUT || nodeTimestamp == FAKE_NODE_EVENT_VALUE) {
        topology::WorkerServiceInfo workerNodeInfo;
        auto parseRc = topology::WorkerServiceInfo::FromProto(nodeTimestamp, workerNodeInfo);
        if (parseRc.IsError()) {
            parseRc = topology::WorkerServiceInfo::FromString(nodeTimestamp, workerNodeInfo);
        }
        if (parseRc.IsError()) {
            std::stringstream ss;
            ss << "Event of node, key: " << nodeHostPortStr << " value: " << nodeTimestamp
               << " type: " << event.ToString() << ", is not recognized.";
            RETURN_STATUS(K_RUNTIME_ERROR, ss.str());
        }
        RETURN_IF_NOT_OK(topology::WorkerServiceStateToString(workerNodeInfo.state, additionEventType));
        nodeTimestamp = std::to_string(workerNodeInfo.timestamp);
    }

    // Remove /TableName/ from key to get IP and port
    nodeHostPortStr.erase(0, nodeHostPortStr.find(clusterPrefix_) + clusterPrefix_.size() + 1);
    HostPort eventNodeKey;
    RETURN_IF_NOT_OK(eventNodeKey.ParseString(nodeHostPortStr));
    auto eventNode = std::make_unique<ClusterNode>(nodeTimestamp, additionEventType);
    if (event.type == topology::CoordinationEventType::PUT) {
        LOG(INFO) << "Event Type: Add Node: " << eventNode->ToString(eventNodeKey);
        rc = HandleNodeAdditionEvent(eventNodeKey, std::move(eventNode), "");
    } else if (event.type == topology::CoordinationEventType::DELETE) {
        LOG(INFO) << "Event Type: Remove Node: " << eventNode->ToString(eventNodeKey);
        LOG_IF_ERROR_EXCEPT(RemoveRemoteFastTransportNode(eventNodeKey), "", K_NOT_FOUND);
        rc = HandleNodeRemoveEvent(eventNodeKey, std::move(eventNode), "");
    } else {
        rc = Status(K_RUNTIME_ERROR, "unknown type: " + event.ToString());
    }
    return rc;
}

void ClusterManager::PublishPlacementDirectorySnapshot()
{
    int64_t version = -1;
    std::string localWorkerUuid;
    std::map<std::string, HostPort> workerUuid2AddrMap;
    hashRing_->GetWorkerUuidAddressMapSnapshot(version, localWorkerUuid, workerUuid2AddrMap);

    auto snapshot = std::make_shared<topology::PlacementDirectorySnapshot>();
    snapshot->version = version;
    snapshot->localWorkerId = std::move(localWorkerUuid);
    snapshot->localAddress = workerAddress_;

    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        for (const auto &worker : workerUuid2AddrMap) {
            topology::WorkerAvailability availability = topology::WorkerAvailability::UNCONFIRMED;
            TbbNodeTable::const_accessor accessor;
            if (clusterNodeTable_.find(accessor, worker.second)) {
                availability = accessor->second->IsActive() ? topology::WorkerAvailability::READY
                                                            : topology::WorkerAvailability::NOT_READY;
            }
            snapshot->workers.emplace(worker.first,
                                      topology::PlacementEndpoint{ worker.first, worker.second, availability });
            snapshot->workerIdsByAddress.emplace(worker.second.ToString(), worker.first);
        }
        // Publish the centralized master under its address string so the routing locator can resolve it through the
        // placement directory and honor requireAvailableTarget, unifying the centralized and distributed availability
        // checks.
        const std::string masterId = masterAddress_.ToString();
        if (!masterId.empty()) {
            topology::WorkerAvailability availability = topology::WorkerAvailability::NOT_READY;
            TbbNodeTable::const_accessor accessor;
            if (clusterNodeTable_.find(accessor, masterAddress_)) {
                availability = accessor->second->IsActive() ? topology::WorkerAvailability::READY
                                                            : topology::WorkerAvailability::NOT_READY;
            }
            snapshot->workers.emplace(masterId, topology::PlacementEndpoint{ masterId, masterAddress_, availability });
            snapshot->workerIdsByAddress.emplace(masterAddress_.ToString(), masterId);
        }
    }
    placementDirectory_->Publish(std::move(snapshot));
}

Status ClusterManager::ProcessNetworkRecovery(const HostPort &recoverNodeKey, ClusterNode *recoverNode,
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

Status ClusterManager::CheckConnection(const std::string &objKey)
{
    MetaAddrInfo info;
    RETURN_IF_NOT_OK(GetMetaAddressNotCheckConnection(objKey, info));
    return CheckConnection(info.GetAddress());
}

Status ClusterManager::CheckConnection(const HostPort &nodeAddr, bool allowDirectoryLag)
{
    Timer timer;
    INJECT_POINT("ClusterManager.checkConnection");
    auto elapsedMs = static_cast<uint64_t>(timer.ElapsedMilliSecondAndReset());
    GetWorkerTimeCost().Append("CheckConnection wait", elapsedMs);
    topology::PlacementEndpoint endpoint;
    auto rc = placementDirectory_->ResolveWorkerByAddress(nodeAddr.ToString(), endpoint);
    if (rc.IsError()) {
        if (allowDirectoryLag && IsDirectoryLagStatus(rc)) {
            return Status::OK();
        }
        std::string errMsg = "The node " + nodeAddr.ToString() + " could not be found in placement directory.";
        LOG(INFO) << errMsg;
        RETURN_STATUS(rc.GetCode(), errMsg);
    }

    if (allowDirectoryLag && endpoint.availability == topology::WorkerAvailability::UNCONFIRMED) {
        return Status::OK();
    }
    if (endpoint.availability != topology::WorkerAvailability::READY) {
        RETURN_STATUS(StatusCode::K_MASTER_TIMEOUT, "Disconnected from remote node " + nodeAddr.ToString());
    }
    elapsedMs = static_cast<uint64_t>(timer.ElapsedMilliSecond());
    GetWorkerTimeCost().Append("CheckConnection", elapsedMs);
    return Status::OK();
}

bool ClusterManager::CheckWorkerIsScaleDown(const std::string &workerAddr)
{
    return hashRing_->CheckWorkerIsScaleDown(workerAddr);
}

std::set<std::string> ClusterManager::GetValidWorkersInHashRing() const
{
    std::set<std::string> workers;
    std::shared_ptr<const topology::RoutingSnapshot> snapshot;
    if (LoadRoutingSnapshot(routingView_, snapshot).IsError()) {
        return workers;
    }
    for (const auto &workerId : snapshot->ValidWorkerIds()) {
        std::string workerAddr;
        if (ResolveWorkerAddress(placementDirectory_, workerId, workerAddr).IsOk()) {
            workers.emplace(std::move(workerAddr));
        }
    }
    return workers;
}

std::set<std::string> ClusterManager::GetActiveWorkersInHashRing() const
{
    std::set<std::string> activeWorkers;
    std::shared_ptr<const topology::RoutingSnapshot> snapshot;
    if (LoadRoutingSnapshot(routingView_, snapshot).IsError()) {
        return activeWorkers;
    }
    for (const auto &workerId : snapshot->ActiveWorkerIds()) {
        topology::PlacementEndpoint endpoint;
        if (placementDirectory_->ResolveWorker(workerId, endpoint).IsOk()
            && endpoint.availability == topology::WorkerAvailability::READY) {
            activeWorkers.emplace(endpoint.address.ToString());
        }
    }
    return activeWorkers;
}

bool ClusterManager::CheckReceiveMigrateInfo()
{
    return hashRing_->CheckReceiveMigrateInfo(workerAddress_.ToString());
};

std::string ClusterManager::GetLocalWorkerUuid() const
{
    topology::PlacementEndpoint endpoint;
    if (placementDirectory_->GetLocalWorker(endpoint).IsError()) {
        return "";
    }
    return endpoint.workerId;
}

Status ClusterManager::GetStandbyWorker(std::string &standbyWorker)
{
    topology::PlacementEndpoint localWorker;
    RETURN_IF_NOT_OK(placementDirectory_->GetLocalWorker(localWorker));
    std::shared_ptr<const topology::RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(LoadRoutingSnapshot(routingView_, snapshot));
    std::string standbyWorkerId;
    RETURN_IF_NOT_OK(snapshot->GetStandbyWorkerId(localWorker.workerId, standbyWorkerId));
    return ResolveWorkerAddress(placementDirectory_, standbyWorkerId, standbyWorker);
}

Status ClusterManager::GetActiveWorkers(uint32_t num, std::vector<std::string> &activeWorkers)
{
    activeWorkers.clear();
    if (num == 0) {
        RETURN_STATUS(StatusCode::K_INVALID, "required number is 0");
    }
    std::shared_ptr<const topology::RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(LoadRoutingSnapshot(routingView_, snapshot));
    topology::PlacementEndpoint localWorker;
    (void)placementDirectory_->GetLocalWorker(localWorker);
    for (const auto &workerId : snapshot->NodeOrder()) {
        if (workerId == localWorker.workerId
            || snapshot->ActiveWorkerIds().find(workerId) == snapshot->ActiveWorkerIds().end()) {
            continue;
        }
        topology::PlacementEndpoint endpoint;
        if (placementDirectory_->ResolveWorker(workerId, endpoint).IsOk()
            && endpoint.availability == topology::WorkerAvailability::READY) {
            activeWorkers.emplace_back(endpoint.address.ToString());
            if (activeWorkers.size() >= num) {
                break;
            }
        }
    }
    return Status::OK();
}

Status ClusterManager::GetStandbyWorkerByAddr(const std::string &workerAddr, std::string &nextWorker)
{
    topology::PlacementEndpoint endpoint;
    RETURN_IF_NOT_OK(placementDirectory_->ResolveWorkerByAddress(workerAddr, endpoint));
    std::shared_ptr<const topology::RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(LoadRoutingSnapshot(routingView_, snapshot));
    std::string nextWorkerId;
    RETURN_IF_NOT_OK(snapshot->GetStandbyWorkerId(endpoint.workerId, nextWorkerId));
    return ResolveWorkerAddress(placementDirectory_, nextWorkerId, nextWorker);
}

void ClusterManager::WaitWorkerReadyIfNeed()
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

Status ClusterManager::StartNodeUtilThread()
{
    static const int CHECK_INTERVAL_MS = 100;
    auto traceId = GetStringUuid().substr(0, SHORT_TRACEID_SIZE);
    LOG(INFO) << "Start node util thread in cluster manager with traceId: " << traceId;
    const int clearScaledDownNodeInClusterTableMaxIntervelMs = 30'000;
    thread_ = std::make_unique<Thread>([this, traceId]() {
        Status rc;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        WaitWorkerReadyIfNeed();
        INJECT_POINT("ClusterManager.DelayMessageDeque.test", [](int delayTime) { sleep(delayTime); });
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

void ClusterManager::GetToBeCleanNodes(const std::unordered_map<std::string, std::string> &orphanNodes,
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
        auto status = clusterStore_->Get(CLUSTER_TABLE, orphanNode, res, timeoutMs);

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

Status ClusterManager::StartOrphanNodeMonitorThread()
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
            if (clusterStore_->IsKeepAliveTimeout()) {
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

Status ClusterManager::StartBackgroundThread()
{
    RETURN_IF_NOT_OK(StartNodeUtilThread());
    RETURN_IF_NOT_OK(StartOrphanNodeMonitorThread());
    return Status::OK();
}

void ClusterManager::HandleFailedNode(const HostPort &addr)
{
    if (!IsCurrentNodeMaster()) {
        return;
    }
    // Perform dead node handling now to adjust any references
    LOG_IF_ERROR(NodeTimeoutEvent::GetInstance().NotifyAll(addr.ToString(), false, true),
                 "Failed to process worker timeout in etcd demotion thread");
    LOG_IF_ERROR(StartClearWorkerMeta::GetInstance().NotifyAll(addr), "Failed to clear worker meta data.");
}

void ClusterManager::NotifySlotRecovery(const std::vector<HostPort> &failedWorkers) const
{
    if (!IsCurrentNodeMaster() || failedWorkers.empty()) {
        return;
    }
    LOG_IF_ERROR(SlotRecoveryFailedWorkersEvent::GetInstance().NotifyAll(failedWorkers),
                 "Failed to notify slot recovery for failed workers.");
}

void ClusterManager::DemoteTimedOutNodes()
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
    NotifySlotRecovery(failedNode);
    LOG_IF(INFO, !failedNode.empty()) << "After demote timeout nodes: " << NodesToString();
    if (!failedNode.empty()) {
        PublishPlacementDirectorySnapshot();
    }
}

std::unordered_set<std::string> ClusterManager::GetFailedWorkers()
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

void ClusterManager::SyncNodeTableWithHashRing(const std::set<std::string> &workersInRing)
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
void ClusterManager::CleanupWorker(const std::string &workerAddr, bool isFailed)
{
    HostPort addr;
    if (addr.ParseString(workerAddr).IsError()) {
        return;
    }
    bool needNotifySlotRecovery = false;
    // 1. clear node table
    {
        // process the node state to failed and then erase.
        if (!isFailed) {
            WARN_IF_ERROR(HandleNodeRemoveEvent(addr, nullptr, ""),
                          ", process to timeout failed.");  // node to timeout
            HandleFailedNode(addr);                         // node to failed
        }
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        if (!isFailed) {
            TbbNodeTable::const_accessor accessor;
            if (clusterNodeTable_.find(accessor, addr)) {
                needNotifySlotRecovery = !accessor->second->NodeWasExiting();
            }
        }
        (void)clusterNodeTable_.erase(addr);
        (void)nodeTableCompletionTimer_.erase(workerAddr);
    }
    if (needNotifySlotRecovery) {
        NotifySlotRecovery({ addr });
    }
    // 2. clear ocnotify api
    RemoveDeadWorkerEvent::GetInstance().NotifyAll(workerAddr);
    // 3. clear workerapi
    EraseFailedNodeApiEvent::GetInstance().NotifyAll(addr);
    PublishPlacementDirectorySnapshot();
}

Status ClusterManager::IfNeedTriggerReconciliation(const HostPort &address, int64_t timestamp, bool sync, bool isDRst)
{
    INJECT_POINT("ClusterManager.IfNeedTriggerReconciliation.noreconciliation");
    if (!isDRst) {
        // Clear worker metadata first in master
        RETURN_IF_NOT_OK(ClearWorkerMeta::GetInstance().NotifyAll(address));
    }
    // Then reply to worker Reconciliation Done
    RETURN_IF_NOT_OK(NodeRestartEvent::GetInstance().NotifyAll(address.ToString(), timestamp, sync));
    LOG(INFO) << "Reconciliation was sent to worker " << address.ToString();

    return Status::OK();
}

std::string ClusterManager::NodesToString()
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

Status ClusterManager::GetClusterNodeAddresses(std::vector<HostPort> &nodeAddrs)
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

Status ClusterManager::GetMasterAddr(const std::string &objKey, HostPort &masterAddr)
{
    MetaAddrInfo metaAddrInfo;
    RETURN_IF_NOT_OK(LocateMetaOwner(objKey, false, metaAddrInfo));
    masterAddr = metaAddrInfo.GetAddress();
    return Status::OK();
}

worker::HashRange ClusterManager::GetHashRangeNonBlock()
{
    std::shared_ptr<const topology::RoutingSnapshot> snapshot;
    if (LoadRoutingSnapshot(routingView_, snapshot).IsError()) {
        return {};
    }
    return ToHashRange(snapshot->LocalOwnedRanges());
}

Status ClusterManager::GetMetaAddressNotCheckConnection(const std::string &objKey, MetaAddrInfo &metaAddrInfo)
{
    Timer timer;
    RETURN_IF_NOT_OK(LocateMetaOwner(objKey, false, metaAddrInfo));
    if (!IsCentralized()) {
        const uint64_t us = static_cast<uint64_t>(timer.ElapsedMicroSecond());
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_GET_META_ADDR_HASHRING_LATENCY))
            .Observe(us);
    }
    GetWorkerTimeCost().Append("GetMetaAddress", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status ClusterManager::GetMetaAddress(const std::string &objKey, MetaAddrInfo &metaAddrInfo)
{
    return LocateMetaOwner(objKey, true, metaAddrInfo);
}

Status ClusterManager::LocateMetaOwnersBatch(const std::vector<std::string> &objectKeys,
                                             topology::BatchRouteDecision &decision)
{
    CHECK_FAIL_RETURN_STATUS(placementFacade_ != nullptr, K_NOT_READY, "Placement facade is not provided.");
    topology::RouteOptions options;
    options.centralizedMode = IsCentralized();
    options.requireAvailableTarget = true;
    options.masterAddress = masterAddress_;
    return placementFacade_->LocateMetaOwnersBatch(objectKeys, options, decision);
}

ClusterManager::MetaOwnerIndexedKeyGroups ClusterManager::GroupKeysByMetaOwnerWithIndex(
    const std::vector<std::string> &objectKeys)
{
    MetaOwnerIndexedKeyGroups result;
    if (objectKeys.empty()) {
        return result;
    }
    topology::BatchRouteDecision decision;
    Status rc = LocateMetaOwnersBatch(objectKeys, decision);
    for (size_t index = 0; index < objectKeys.size(); ++index) {
        const auto &objectKey = objectKeys[index];
        MetaAddrInfo metaAddrInfo;
        Status routeRc = ResolveBatchRouteForKey(objectKey, rc, decision, metaAddrInfo);
        if (routeRc.IsOk()) {
            result.groups[std::move(metaAddrInfo)].emplace_back(objectKey, index);
            continue;
        }
        (void)result.failures.emplace(objectKey, routeRc);
        VLOG(1) << FormatString("objKey[%s] can not find master, status: %s", objectKey, routeRc.ToString());
    }
    if (!result.failures.empty()) {
        LOG(INFO) << "Group object keys by master failed, first errInfo: key is " << result.failures.begin()->first
                  << ", status is " << result.failures.begin()->second.ToString();
    }
    return result;
}

ClusterManager::MetaOwnerKeyGroups ClusterManager::GroupKeysByMetaOwner(const std::vector<std::string> &objectKeys)
{
    return GroupKeysByMetaOwnerImpl(objectKeys);
}

ClusterManager::MetaOwnerKeyGroups ClusterManager::GroupKeysByMetaOwnerImpl(const std::vector<std::string> &keys)
{
    MetaOwnerKeyGroups result;
    if (keys.empty()) {
        return result;
    }
    Timer timer;
    topology::BatchRouteDecision decision;
    Status rc = LocateMetaOwnersBatch(keys, decision);
    for (const auto &objectKey : keys) {
        MetaAddrInfo metaAddrInfo;
        Status routeRc = ResolveBatchRouteForKey(objectKey, rc, decision, metaAddrInfo);
        if (routeRc.IsOk()) {
            result.groups[std::move(metaAddrInfo)].emplace_back(objectKey);
            continue;
        }
        (void)result.failures.emplace(objectKey, routeRc);
        VLOG(1) << FormatString("objKey[%s] can not find master, status: %s", objectKey, routeRc.ToString());
    }
    if (!result.failures.empty()) {
        LOG(INFO) << "Group object keys by master failed, first errInfo: key is " << result.failures.begin()->first
                  << ", status is " << result.failures.begin()->second.ToString();
    }
    auto elapsedMs = static_cast<uint64_t>(std::round(timer.ElapsedMilliSecond()));
    GetWorkerTimeCost().Append("GroupObjKeys", elapsedMs);
    return result;
}

Status ClusterManager::ResolveBatchRouteForKey(const std::string &objectKey, const Status &batchRc,
                                               const topology::BatchRouteDecision &decision,
                                               MetaAddrInfo &metaAddrInfo) const
{
    if (batchRc.IsError()) {
        return batchRc;
    }
    auto routeIter = decision.perKeyDecision.find(objectKey);
    if (routeIter != decision.perKeyDecision.end()) {
        metaAddrInfo = routeIter->second.ToMetaAddrInfo();
        return Status::OK();
    }
    auto failureIter = decision.perKeyFailure.find(objectKey);
    return failureIter == decision.perKeyFailure.end() ? Status(K_NOT_FOUND, "Route decision missing for object key.")
                                                       : failureIter->second;
}

void ClusterManager::GetObjectKeysFromNotConnectedMaster(
    const std::unordered_map<MetaAddrInfo, std::vector<std::string>> &metaAddrInfos,
    std::unordered_set<std::string> &objectKeys)
{
    for (const auto &[metaAddrInfo, keys] : metaAddrInfos) {
        const auto &masterAddr = metaAddrInfo.GetAddress();
        if (CheckConnection(masterAddr).IsError()) {
            for (const auto &key : keys) {
                objectKeys.emplace(key);
            }
        };
    }
}

Status ClusterManager::GetPrimaryReplicaLocationByObjectKey(const std::string &objectKey, HostPort &masterAddr,
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
    MetaAddrInfo metaAddrInfo;
    RETURN_IF_NOT_OK(LocateMetaOwner(objectKey, false, metaAddrInfo));
    masterAddr = metaAddrInfo.GetAddress();
    dbName = metaAddrInfo.GetDbName();
    VLOG(1) << FormatString("Object: %s, dbName: %s, metadata location: %s", objectKey, dbName, masterAddr.ToString());
    return Status::OK();
}

Status ClusterManager::GetPrimaryReplicaLocationByAddr(const std::string &address, HostPort &masterAddr,
                                                       std::string &dbName)
{
    constexpr int intervalMs = 100;
    auto realRetryTimeMs = reqTimeoutDuration.CalcRemainingTime();
    Timer timer;
    Status status(K_NOT_READY, "Placement directory snapshot is not ready.");
    while (timer.ElapsedMilliSecond() < realRetryTimeMs) {
        topology::PlacementEndpoint endpoint;
        status = placementDirectory_->ResolveWorkerByAddress(address, endpoint);
        if (status.IsOk()) {
            dbName = endpoint.workerId;
            masterAddr = endpoint.address;
            break;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        }
    }
    return status;
}

Status ClusterManager::GetPrimaryReplicaDbNames(const HostPort &address, std::vector<std::string> &dbNames)
{
    if (IsCentralized()) {
        dbNames.emplace_back("");
        return Status::OK();
    }
    topology::PlacementEndpoint endpoint;
    RETURN_IF_NOT_OK(placementDirectory_->ResolveWorkerByAddress(address.ToString(), endpoint));
    dbNames.emplace_back(endpoint.workerId);
    return Status::OK();
}

std::set<std::string> ClusterManager::GetNodesInTable()
{
    std::set<std::string> nodes;
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        for (const auto &iter : clusterNodeTable_) {
            nodes.emplace(iter.first.ToString());
        }
    }
    return nodes;
}

void ClusterManager::CompleteNodeTableWithFakeNode()
{
    // Assume that a cluster consists of nodeA, nodeB and nodeC. When the cluster restarts after node_timeout_s, the
    // cluster table in the metadata store will be empty and SetupInitialClusterNodes cannot get any value.
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

void ClusterManager::CompleteNodeTableWithFakeNode(const std::string &lackNode)
{
    // Enqueue a fake event and let the background thread to handle it.
    LOG(INFO) << "Create fake add-and-remove event of node " << lackNode << " to priority queue.";
    // Key is the string of HostPort. Value is the timestamp with additionType.
    topology::CoordinationEvent fakeAddEvent{ topology::CoordinationEventType::PUT, clusterPrefix_ + "/" + lackNode,
                                              FAKE_NODE_EVENT_VALUE };
    topology::CoordinationEvent fakeDeleteEvent{ topology::CoordinationEventType::DELETE,
                                                 clusterPrefix_ + "/" + lackNode, FAKE_NODE_EVENT_VALUE };

    if (eventPq_ && thread_) {
        eventPq_->EmplaceBack(new CmEvent(std::move(fakeAddEvent), PrefixType::CLUSTER));
        eventPq_->EmplaceBack(new CmEvent(std::move(fakeDeleteEvent), PrefixType::CLUSTER));
    }
}

Status ClusterManager::WaitNodeJoinToTable()
{
    using namespace std::chrono;
    std::vector<std::pair<std::string, std::string>> localAzValue;
    RETURN_IF_NOT_OK(clusterStore_->GetAll(CLUSTER_TABLE, localAzValue));
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

void ClusterManager::ScheduledCheckCompleteNodeTableWithFakeNode()
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

Status ClusterManager::CheckWaitNodeTableComplete()
{
    using namespace std::chrono;
    if (IsCentralized()) {
        return WaitNodeJoinToTable();
    }

    auto start = GetSteadyClockTimeStampUs();
    int hashWorkerNum = 0;
    auto rc = GetHashRingWorkerNum(hashWorkerNum);
    int tableSize = GetNodeTableSize();
    bool isRestart = false;
    RETURN_IF_NOT_OK(IsRestart(isRestart));
    INJECT_POINT("ClusterManager.CheckWaitNodeTableComplete.hashWorkerNum", [&hashWorkerNum](int injectWorkerNum) {
        hashWorkerNum = injectWorkerNum;
        return Status::OK();
    });
    static const int RESERVED_TIME_SEC = 3;
    static const int TO_SECOND = 1000;
    static const int WAITING_TIME_FOR_EACH_NODE_MS = 300;
    uint32_t totalWaitTime = static_cast<uint32_t>(TOTAL_WAIT_NODE_TABLE_TIME_SEC);
    bool firstInit = hashRing_->IsInit();
    auto hashAddTime = FLAGS_add_node_wait_time_s + RESERVED_TIME_SEC;
    if (rc.IsError() && firstInit && hashAddTime > TOTAL_WAIT_NODE_TABLE_TIME_SEC) {
        totalWaitTime = hashAddTime;
    }
    INJECT_POINT("ClusterManager.CheckWaitNodeTableComplete.waitTime", [&totalWaitTime](uint32_t injectWaitTime) {
        totalWaitTime = injectWaitTime;
        return Status::OK();
    });

    LOG(INFO) << "Begin to wait for the completion of node table. Plan to wait: " << hashWorkerNum
              << ". Current: " << tableSize << ", totalWaitTime:" << totalWaitTime << "s";
    int lastTableSize = tableSize;
    auto lastProgressTimeUs = start;
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
        if (tableSize != lastTableSize) {
            lastProgressTimeUs = GetSteadyClockTimeStampUs();
            lastTableSize = tableSize;
        }
        INJECT_POINT("ClusterManager.CheckWaitNodeTableComplete.noProgressTimeout",
                     [&lastProgressTimeUs](uint32_t injectSec) {
                         lastProgressTimeUs =
                             GetSteadyClockTimeStampUs()
                             - duration_cast<microseconds>(seconds(NO_PROGRESS_TIMEOUT_SEC + injectSec + 1)).count();
                         return Status::OK();
                     });
        if (isRestart
            && GetSteadyClockTimeStampUs() - lastProgressTimeUs
                   >= duration_cast<microseconds>(seconds(NO_PROGRESS_TIMEOUT_SEC)).count()) {
            INJECT_POINT_NO_RETURN("ClusterManager.CheckWaitNodeTableComplete.noProgressBreak");
            LOG(INFO) << "No progress in node table for " << NO_PROGRESS_TIMEOUT_SEC
                      << "s, terminating wait early. Current: " << tableSize << ", expected: " << hashWorkerNum;
            break;
        }
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

    INJECT_POINT("ClusterManager.CheckWaitNodeTableComplete.returnError", [tableSize, hashWorkerNum]() {
        if (tableSize != hashWorkerNum) {
            RETURN_STATUS(K_NOT_READY, "size of table in cluster manager different from number of running workers.");
        }
        return Status::OK();
    });
    return Status::OK();
}

Status ClusterManager::InformEtcdReconciliationDone()
{
    RETURN_IF_NOT_OK(clusterStore_->InformReconciliationDone(workerAddress_));
    return Status::OK();
}

std::vector<std::string> ClusterManager::ClusterNodeTableToString()
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    std::vector<std::string> content;
    for (const auto &node : clusterNodeTable_) {
        const std::string &str = node.second->ToString(node.first);
        content.emplace_back(str);
    }
    return content;
}

bool ClusterManager::IfFindWorkerInTheClusterNode(HostPort &workerAddress)
{
    typename TbbNodeTable::const_accessor accessor;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return clusterNodeTable_.find(accessor, workerAddress);
}

bool ClusterManager::CheckCoordinatorStateWhenNetworkFailed()
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
        if (CreateRemoteWorkerApi(workerAddr, workerAddress_, akSkManager_, remoteWorkerApi).IsOk()) {
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
        CheckCoordinatorStateReqPb req;
        int64_t tag;
        auto rc = remoteWorkerApi->CheckCoordinatorStateAsyncWrite(req, tag);
        if (rc.IsError()) {
            LOG(WARNING) << "Rpc write failed, with rc: " << rc.ToString();
            continue;
        }
        api2Tag.emplace(remoteWorkerApi, tag);
    }
    bool coordinatorAvailable = false;
    for (const auto &pair : api2Tag) {
        CheckCoordinatorStateRspPb rsp;
        if (pair.first->CheckCoordinatorStateAsyncRead(pair.second, rsp) && !coordinatorAvailable && rsp.available()) {
            LOG(INFO) << pair.first->Address() << " confirms that etcd is OK";
            coordinatorAvailable = true;
            // In ZmqStubImpl, the resources corresponding to the tags will be cleaned up only after reading,
            // so all tags need to be read.
        }
    }
    return coordinatorAvailable;
}

std::string ClusterManager::GetWorkerIdByWorkerAddr(const std::string &address) const
{
    topology::PlacementEndpoint endpoint;
    LOG_IF_ERROR(placementDirectory_->ResolveWorkerByAddress(address, endpoint), "Cannot find workerid of " + address);
    return endpoint.workerId;
}

std::string ClusterManager::GetWorkerAddress() const
{
    return workerAddress_.ToString();
}

Status ClusterManager::GetHashRingWorkerNum(int &workerNum) const
{
    workerNum = 0;
    if (IsCentralized()) {
        static const int DEFAULT_NUM_CENTRALIZED = -1;
        workerNum = DEFAULT_NUM_CENTRALIZED;
        return Status::OK();
    }
    std::shared_ptr<const topology::RoutingSnapshot> snapshot;
    RETURN_IF_NOT_OK(LoadRoutingSnapshot(routingView_, snapshot));
    CHECK_FAIL_RETURN_STATUS(!snapshot->Empty(), K_NOT_READY, "Routing snapshot is not workable. Call again later.");
    workerNum = static_cast<int>(snapshot->ValidWorkerIds().size());
    return Status::OK();
}

Status ClusterManager::CreateEtcdStoreTable(EtcdStore *etcdStore)
{
    RETURN_IF_NOT_OK_EXCEPT(etcdStore->CreateTable(HASHRING_TABLE, HASHRING_TABLE), K_DUPLICATED);
    RETURN_IF_NOT_OK_EXCEPT(etcdStore->CreateTable(CLUSTER_TABLE, "/" + std::string(CLUSTER_TABLE)), K_DUPLICATED);
    RETURN_IF_NOT_OK_EXCEPT(etcdStore->CreateTable(MASTER_ADDRESS_TABLE, MASTER_ADDRESS_TABLE), K_DUPLICATED);
    return Status::OK();
}

Status ClusterManager::ConstructClusterInfoViaEtcd(EtcdStore *etcdStore, ClusterInfo &clusterInfo)
{
    RETURN_IF_NOT_OK(CreateEtcdStoreTable(etcdStore));
    RETURN_IF_NOT_OK(etcdStore->GetAll(CLUSTER_TABLE, clusterInfo.workers, clusterInfo.revision));
    return Status::OK();
}

std::string ClusterInfo::ToString()
{
    std::stringstream msg;
    msg << std::endl << "local hash ring: ";
    for (const auto &pair : localHashRing) {
        msg << pair.first << ", ";
    }
    msg << std::endl << "local workers: ";
    for (const auto &pair : workers) {
        msg << pair.first << ", ";
    }
    return msg.str();
}
}  // namespace datasystem
