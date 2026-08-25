/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"

#include <algorithm>
#include <mutex>
#include <string>
#include <utility>
#include <vector>


#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
namespace {
template <typename State, typename Mutator>
void UpdateImmutableSnapshot(std::shared_ptr<const State> &slot, Mutator &&mutator)
{
    auto current = std::atomic_load_explicit(&slot, std::memory_order_acquire);
    bool updated = false;
    do {
        auto next = std::make_shared<State>(current == nullptr ? State{} : *current);
        mutator(*next);
        std::shared_ptr<const State> desired = std::move(next);
        updated = std::atomic_compare_exchange_weak_explicit(&slot, &current, std::move(desired),
                                                             std::memory_order_acq_rel, std::memory_order_acquire);
    } while (!updated);
}
}  // namespace
static const std::string RESOURCE_MONITOR_MASTER = "RESOURCE_MONITOR";
static const int64_t REPORT_RESOURCE_INTERVAL_TIME_MS = 30 * 1000;
static const int64_t REPORT_RESOURCE_INTERVAL_TIME_MS_IF_FAILED = 500;
static constexpr int RESOURCE_MONITOR_MASTER_ADDRESS_LOG_LEVEL = 1;
static constexpr int RESOURCE_MONITOR_MASTER_ADDRESS_LOG_EVERY_N = 2;

std::string ObjectCopyWatermark::ToMetricsString() const
{
    const auto ratio = [](uint64_t numerator, uint64_t denominator) {
        return denominator == 0 ? 0.0 : static_cast<double>(numerator) / static_cast<double>(denominator);
    };
    return std::to_string(hotPrimaryCopyBytes) + "/" + std::to_string(totalPrimaryCopyBytes) + "/"
           + std::to_string(memoryCapacity) + "/"
           + FormatString("%.9f/%.9f/%.9f/%d", ratio(hotPrimaryCopyBytes, memoryCapacity),
                          ratio(totalPrimaryCopyBytes, memoryCapacity),
                          ratio(hotPrimaryCopyBytes, totalPrimaryCopyBytes), valid ? 1 : 0)
           + "/" + std::to_string(coldPrimaryCopyBytes) + "/" + std::to_string(warmPrimaryCopyBytes) + "/"
           + FormatString("%.9f/%.9f", ratio(coldPrimaryCopyBytes, totalPrimaryCopyBytes),
                          ratio(warmPrimaryCopyBytes, totalPrimaryCopyBytes));
}

NodeSelector &NodeSelector::Instance()
{
    static NodeSelector instance;
    return instance;
}

NodeSelector::NodeSelector()
    : nodeInfos_(std::make_shared<const NodeInfoSnapshot>()),
      running_(false),
      resourceReportState_(std::make_shared<const ResourceReportState>()),
      copyWatermark_(std::make_shared<const ObjectCopyWatermark>()),
      token_(new Token())
{
}

NodeSelector::~NodeSelector()
{
    Shutdown();
}

void NodeSelector::Init(const std::string &localAddress, const cluster::MembershipEndpointView &membership,
                        const std::atomic<bool> *exitRequested,
                        std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager)
{
    if (!apiManager) {
        LOG(WARNING) << "The apiManager is empty, can not set running and start worker thread";
        return;
    }
    if (running_.exchange(true)) {
        LOG(WARNING) << "NodeSelector already initialized";
        return;
    }
    localAddress_ = localAddress;
    membership_ = &membership;
    exitRequested_ = exitRequested;
    apiManager_ = std::move(apiManager);
    running_.store(true);

    workerThread_ = Thread(&NodeSelector::WorkerThread, this);
    workerThread_.set_name("NodeSelector");
    LOG(INFO) << "NodeSelector initialized";
}

void NodeSelector::Shutdown()
{
    if (!running_.exchange(false)) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        running_.store(false);
    }
    token_->alive.store(false, std::memory_order_release);
    taskCv_.notify_all();
    subReadyPost_.Set();

    if (workerThread_.joinable()) {
        workerThread_.join();
    }
    membership_ = nullptr;
    exitRequested_ = nullptr;
    apiManager_.reset();
    LOG(INFO) << "NodeSelector shutdown";
}

void NodeSelector::RegisterRebalanceTaskHandler(RebalanceTaskHandler handler)
{
    auto snapshot = handler ? std::make_shared<const RebalanceTaskHandler>(std::move(handler)) : nullptr;
    std::atomic_store_explicit(&rebalanceTaskHandler_, std::move(snapshot), std::memory_order_release);
}

void NodeSelector::UnregisterRebalanceTaskHandler()
{
    std::atomic_store_explicit(&rebalanceTaskHandler_, std::shared_ptr<const RebalanceTaskHandler>{},
                               std::memory_order_release);
}

void NodeSelector::RegisterEvictionPolicyUpdateHandler(EvictionPolicyUpdateHandler handler)
{
    auto snapshot = handler ? std::make_shared<const EvictionPolicyUpdateHandler>(std::move(handler)) : nullptr;
    std::atomic_store_explicit(&evictionPolicyUpdateHandler_, std::move(snapshot), std::memory_order_release);
}

void NodeSelector::UnregisterEvictionPolicyUpdateHandler()
{
    std::atomic_store_explicit(&evictionPolicyUpdateHandler_, std::shared_ptr<const EvictionPolicyUpdateHandler>{},
                               std::memory_order_release);
}

void NodeSelector::SetEvictionPolicyReport(master::EvictionPolicyPb policy, uint64_t epoch,
                                           master::EvictionPolicyUpdatePhasePb phase)
{
    UpdateImmutableSnapshot(resourceReportState_, [policy, epoch, phase](ResourceReportState &state) {
        state.evictionPolicy = policy;
        state.evictionPolicyEpoch = epoch;
        state.evictionPolicyPhase = phase;
    });
}

void NodeSelector::SetEvictionPolicyControlReport(master::EvictionPolicyWorkerStatusPb status, uint64_t epoch,
                                                  uint64_t totalObjects, uint64_t migratedObjects,
                                                  const Status &failure)
{
    static constexpr size_t MAX_FAILURE_REASON_SIZE = 512;
    Status normalizedFailure = Status::OK();
    if (failure.IsError()) {
        auto reason = failure.GetMsg();
        if (reason.size() > MAX_FAILURE_REASON_SIZE) {
            reason.resize(MAX_FAILURE_REASON_SIZE);
        }
        normalizedFailure = Status(failure.GetCode(), std::move(reason));
    }
    UpdateImmutableSnapshot(resourceReportState_,
                            [status, epoch, totalObjects, migratedObjects,
                             failure = std::move(normalizedFailure)](ResourceReportState &state) {
                                state.evictionPolicyWorkerStatus = status;
                                state.evictionPolicyControlEpoch = epoch;
                                state.evictionPolicyTotalObjects = totalObjects;
                                state.evictionPolicyMigratedObjects = std::min(totalObjects, migratedObjects);
                                state.evictionPolicyFailure = failure;
                            });
}

void NodeSelector::RegisterPreReportHook(std::function<void()> hook, uint64_t minIntervalMs)
{
    std::lock_guard<std::mutex> lock(preReportHooksMutex_);
    preReportHooks_.push_back({ std::move(hook), minIntervalMs, 0, false });
}

void NodeSelector::UnregisterPreReportHooks()
{
    std::lock_guard<std::mutex> lock(preReportHooksMutex_);
    preReportHooks_.clear();
}

void NodeSelector::SetHotPrimaryReport(uint64_t hotPrimaryCopyCount, uint64_t totalPrimaryCopyCount,
                                       uint64_t hotPrimaryCopyBytes, uint64_t totalPrimaryCopyBytes,
                                       uint64_t memoryCapacity)
{
    UpdateImmutableSnapshot(resourceReportState_, [=](ResourceReportState &state) {
        state.hotPrimaryReport = { 0, 0, hotPrimaryCopyCount, totalPrimaryCopyCount, hotPrimaryCopyBytes,
                                   totalPrimaryCopyBytes, memoryCapacity, true };
    });
}

void NodeSelector::SetObjectCopyWatermark(uint64_t coldPrimaryCopyBytes, uint64_t warmPrimaryCopyBytes,
                                          uint64_t hotPrimaryCopyCount, uint64_t totalPrimaryCopyCount,
                                          uint64_t hotPrimaryCopyBytes, uint64_t totalPrimaryCopyBytes,
                                          uint64_t memoryCapacity)
{
    auto snapshot = std::make_shared<const ObjectCopyWatermark>(ObjectCopyWatermark{
        coldPrimaryCopyBytes, warmPrimaryCopyBytes, hotPrimaryCopyCount, totalPrimaryCopyCount, hotPrimaryCopyBytes,
        totalPrimaryCopyBytes, memoryCapacity, true });
    std::atomic_store_explicit(&copyWatermark_, std::move(snapshot), std::memory_order_release);
}

ObjectCopyWatermark NodeSelector::GetObjectCopyWatermark() const
{
    auto snapshot = std::atomic_load_explicit(&copyWatermark_, std::memory_order_acquire);
    return snapshot == nullptr ? ObjectCopyWatermark{} : *snapshot;
}

#ifdef WITH_TESTS
ObjectCopyWatermark NodeSelector::GetHotPrimaryReportForTest() const
{
    auto snapshot = std::atomic_load_explicit(&resourceReportState_, std::memory_order_acquire);
    return snapshot == nullptr ? ObjectCopyWatermark{} : snapshot->hotPrimaryReport;
}
#endif

Status NodeSelector::SelectNode(const std::unordered_set<std::string> &excludeNodes, const std::string &preferNode,
                                size_t needSize, std::string &outNode)
{
    // 1. If rankList_ is empty, obtain Standby worker from TopologyEngine and return;
    // 2. If the maximum remaining capacity in rankList_ is less than 1MB, return K_NO_SPACE;
    // 3. If the remaining capacity of the preferNode > needSize, select it;
    // 4. Randomly select the top n (5) nodes with available capacity > needSize, excluding nodes in excludedNodes;
    // 5. The isReady flag indicates whether the node is in active scaling-down state;
    //   do not select nodes that are not ready.
    auto nodeInfos = std::atomic_load_explicit(&nodeInfos_, std::memory_order_acquire);
    if (nodeInfos == nullptr || nodeInfos->rankList.empty()) {
        return GetLocalStandbyWorker(excludeNodes, outNode);
    }
    const auto &rankList = nodeInfos->rankList;
    auto maxLeftMemory = rankList[0].availableMemory;
    CHECK_FAIL_RETURN_STATUS(maxLeftMemory > 1 * MB_TO_BYTES,
                             K_NO_SPACE, "The max available memory in not enough");

    auto it = std::find_if(rankList.begin(), rankList.end(),
                           [&preferNode](NodeInfo info) { return info.nodeId == preferNode; });
    if (it != rankList.end() && it->isReady && it->availableMemory > needSize) {
        outNode = preferNode;
        return Status::OK();
    }

    uint64_t maxN = 5;
    std::vector<NodeInfo> maxNNodes;
    maxNNodes.reserve(maxN);
    std::string backupNode;
    for (const auto &nodeInfo : rankList) {
        auto it = excludeNodes.find(nodeInfo.nodeId);
        if (it != excludeNodes.end()) {
            continue;
        }
        if (!nodeInfo.isReady) {
            break;
        }
        if (nodeInfo.availableMemory <= needSize) {
            backupNode = nodeInfo.nodeId;
            break;
        }
        maxNNodes.emplace_back(nodeInfo);
        if (maxNNodes.size() == maxN) {
            break;
        }
    }
    if (maxNNodes.empty() && !backupNode.empty()) {
        outNode = backupNode;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!maxNNodes.empty(), K_NOT_FOUND, "not find the profit node");
    // Randomly select one from maxNNodes as the result
    static thread_local std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
    std::shuffle(maxNNodes.begin(), maxNNodes.end(), gen);
    outNode = maxNNodes.front().nodeId;
    return Status::OK();
}

Status NodeSelector::GetLocalStandbyWorker(const std::unordered_set<std::string> &excludeNodes, std::string &outNode)
{
    CHECK_FAIL_RETURN_STATUS(membership_ != nullptr, K_NOT_READY, "Topology membership view is unavailable");
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(membership_->GetSnapshot(snapshot));
    std::string worker = localAddress_;
    constexpr int maxCount = 5;
    for (int i = 0; i < maxCount; ++i) {
        const cluster::Member *standby = nullptr;
        RETURN_IF_NOT_OK(snapshot->FindNextCommittedMember(worker, standby));
        outNode = standby->identity.address;
        if (outNode == localAddress_) {
            outNode.clear();
            RETURN_STATUS(K_NOT_FOUND, "Not found the stand by worker");
        }
        auto it = excludeNodes.find(outNode);
        if (it == excludeNodes.end()) {
            return Status::OK();
        }
        worker = outNode;
        outNode.clear();
    }
    RETURN_STATUS(K_NOT_FOUND, "No key was found within the maxDepth loop count");
}

size_t NodeSelector::GetAvailableMemory(const std::string &address)
{
    size_t availableMemory = 0;
    Status rc = TryGetAvailableMemory(address, availableMemory);
    return rc.IsOk() ? availableMemory : 0;
}

Status NodeSelector::TryGetAvailableMemory(const std::string &address, size_t &availableMemory)
{
    availableMemory = 0;
    const int waitReadyTimeoutMs = 1000;
    bool hasSnapshot = false;
    Status rc = TryGetAvailableMemoryFromSnapshot(address, availableMemory, hasSnapshot);
    if (rc.IsOk() || hasSnapshot) {
        // A non-empty snapshot is authoritative for the current selection round; callers can fall back to remote probe.
        return rc;
    }
    if (!subSuccess_.load() && running_.load()) {
        subReadyPost_.WaitFor(waitReadyTimeoutMs);
    }
    return TryGetAvailableMemoryFromSnapshot(address, availableMemory, hasSnapshot);
}

Status NodeSelector::TryGetAvailableMemoryFromSnapshot(const std::string &address, size_t &availableMemory,
                                                       bool &hasSnapshot) const
{
    availableMemory = 0;
    auto nodeInfos = std::atomic_load_explicit(&nodeInfos_, std::memory_order_acquire);
    hasSnapshot = nodeInfos != nullptr && !nodeInfos->rankList.empty();
    if (!hasSnapshot) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("Remote node %s resource info not found, local node %s", address,
                                                localAddress_));
    }
    const auto &rankList = nodeInfos->rankList;
    auto it = std::find_if(rankList.begin(), rankList.end(),
                           [&address](const NodeInfo &info) { return info.nodeId == address; });
    if (it == rankList.end()) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("Remote node %s resource info not found, local node %s", address,
                                                localAddress_));
    }
    if (!it->isReady) {
        RETURN_STATUS(K_NOT_READY, FormatString("Remote node %s is not ready for resource selection, local node %s",
                                                address, localAddress_));
    }
    availableMemory = it->availableMemory;
    return Status::OK();
}

bool NodeSelector::HasEnoughAvailableMemory(size_t needMemory)
{
    auto nodeInfos = std::atomic_load_explicit(&nodeInfos_, std::memory_order_acquire);
    return nodeInfos != nullptr && nodeInfos->totalSize > needMemory;
}

void NodeSelector::WorkerThread()
{
    LOG(INFO) << "Start worker thread to periodically collect cluster info";
    int64_t intervalMs = REPORT_RESOURCE_INTERVAL_TIME_MS;
    INJECT_POINT_NO_RETURN("NodeSelector.setInterval", [&intervalMs](int interval) { intervalMs = interval; });
    while (running_) {
        SetRequestContext(nullptr);
        ScopedRequestContext ctx("NodeSelector;" + GetStringUuid());
        auto rc = CollectClusterInfo();
        if (!token_->alive.load(std::memory_order_acquire)) {
            break;
        }
        if (rc.IsError()) {
            LOG(WARNING) << "Collect cluster info failed, errMsg is " << rc.GetMsg();
        } else {
            subSuccess_.store(true);
            subReadyPost_.Set();
        }
        std::unique_lock<std::mutex> lock(taskMutex_);
        if (!running_.load()) {
            break;
        }
        auto nodeInfos = std::atomic_load_explicit(&nodeInfos_, std::memory_order_acquire);
        const bool hasSnapshot = nodeInfos != nullptr && !nodeInfos->rankList.empty();
        (void)taskCv_.wait_for(lock,
                               std::chrono::milliseconds((subSuccess_ && hasSnapshot)
                                                             ? intervalMs
                                                             : REPORT_RESOURCE_INTERVAL_TIME_MS_IF_FAILED),
                               [this]() { return !running_.load(); });
    }
}

Status NodeSelector::GetWorkerMasterApi(std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi)
{
    auto rc = apiManager_->GetWorkerMasterApi(RESOURCE_MONITOR_MASTER, workerMasterApi);
    if (rc.IsOk() && workerMasterApi != nullptr) {
        VLOG_EVERY_N(RESOURCE_MONITOR_MASTER_ADDRESS_LOG_LEVEL, RESOURCE_MONITOR_MASTER_ADDRESS_LOG_EVERY_N)
            << "Get " << RESOURCE_MONITOR_MASTER << " address: " << workerMasterApi->GetHostPort();
    } else {
        VLOG_EVERY_N(RESOURCE_MONITOR_MASTER_ADDRESS_LOG_LEVEL, RESOURCE_MONITOR_MASTER_ADDRESS_LOG_EVERY_N)
            << "Get " << RESOURCE_MONITOR_MASTER << " address failed, status: " << rc.ToString();
    }
    return rc;
}

Status NodeSelector::ReportResource(const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                    master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp)
{
    // Report current worker resource info to master
    master::WorkerStat *stat = req.mutable_stat();
    auto *allocator = datasystem::memory::Allocator::Instance();
    const auto availableMemory = allocator->GetMemoryAvailToHighWater();
    const auto usedMemory = allocator->GetTotalRealMemoryUsage();
    const auto memoryLimit = allocator->GetTotalMemoryLimit();
    stat->set_address(localAddress_);
    stat->set_available_memory(availableMemory);
    stat->set_used_memory(usedMemory);
    stat->set_memory_limit(memoryLimit);
    // Report capacity as current used memory plus memory still available to the high watermark.
    stat->set_memory_capacity(usedMemory + availableMemory);
    const bool exitRequested = exitRequested_ != nullptr && exitRequested_->load(std::memory_order_relaxed);
    stat->set_is_ready(!exitRequested);
    // Heat-rebalance reporting is zero under clock eviction and populated under Heat by periodic maintenance.
    auto report = std::atomic_load_explicit(&resourceReportState_, std::memory_order_acquire);
    if (report != nullptr) {
        stat->set_hot_primary_copy_count(report->hotPrimaryReport.hotPrimaryCopyCount);
        stat->set_total_primary_copy_count(report->hotPrimaryReport.totalPrimaryCopyCount);
        stat->set_hot_primary_copy_bytes(report->hotPrimaryReport.hotPrimaryCopyBytes);
        stat->set_eviction_policy(report->evictionPolicy);
        stat->set_eviction_policy_epoch(report->evictionPolicyEpoch);
        stat->set_eviction_policy_update_phase(report->evictionPolicyPhase);
        stat->set_eviction_policy_worker_status(report->evictionPolicyWorkerStatus);
        stat->set_eviction_policy_control_epoch(report->evictionPolicyControlEpoch);
        stat->set_eviction_policy_total_objects(report->evictionPolicyTotalObjects);
        stat->set_eviction_policy_migrated_objects(report->evictionPolicyMigratedObjects);
        if (report->evictionPolicyFailure.IsError()) {
            stat->set_eviction_policy_failure_code(static_cast<int32_t>(report->evictionPolicyFailure.GetCode()));
            stat->set_eviction_policy_failure_reason(report->evictionPolicyFailure.GetMsg());
        }
        if (report->evictionPolicyPhase != master::EVICTION_POLICY_STABLE) {
            // Conversion disables eviction, so remove this worker from both source
            // and target rebalance selection until target-only activation completes.
            stat->set_is_ready(false);
        }
    }
    RETURN_OK_IF_TRUE(!token_->alive.load(std::memory_order_acquire));
    auto rc = workerMasterApi->ReportResource(req, rsp);
    return rc;
}

Status NodeSelector::CollectClusterInfo()
{
    // Run pre-report hooks (e.g. heat-counter periodic decay) before reporting, so the
    // reported state and any rebalance decision reflect post-decay heat. Copy under the
    // lock, run outside it so a slow hook does not block registration.
    RunPreReportHooks(static_cast<uint64_t>(GetSteadyClockTimeStampMs()));
    std::shared_ptr<worker::WorkerMasterOCApi> workerMasterApi;
    RETURN_IF_NOT_OK(GetWorkerMasterApi(workerMasterApi));
    // Skip the report when the resolved master is already marked UNREACHABLE by the
    // topology view. Otherwise the worker keeps retrying a dead master every 500ms
    // (REPORT_RESOURCE_INTERVAL_TIME_MS_IF_FAILED) until the node is confirmed
    // FAILED, flooding brpc with failing RPCs that inflate bthread stack caching.
    if (membership_ != nullptr) {
        const std::string masterAddr = workerMasterApi->GetHostPort();
        cluster::MemberEndpoint endpoint;
        auto resolveRc = membership_->ResolveByAddress(masterAddr, endpoint);
        if (resolveRc.IsOk() && endpoint.localAvailability == cluster::EndpointAvailability::UNREACHABLE) {
            return Status(K_MASTER_TIMEOUT,
                          "Resource-monitor master " + masterAddr + " is unreachable, skip report");
        }
    }
    // Report current worker resource info to master
    master::ResourceReportReqPb req;
    master::ResourceReportRspPb rsp;
    RETURN_IF_NOT_OK(ReportResource(workerMasterApi, req, rsp));
    if (!token_->alive.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    if (!rsp.rebalance_task().task_id().empty()) {
        auto handler = std::atomic_load_explicit(&rebalanceTaskHandler_, std::memory_order_acquire);
        if (handler != nullptr && *handler != nullptr) {
            // Preserve the exact master that returned the task. Resolving the owner again in the executor can race
            // with failover and incorrectly bind an old task to the new master.
            (*handler)(rsp.rebalance_task(), workerMasterApi->GetHostPort());
        }
    }
    if (rsp.has_eviction_policy_update() && rsp.eviction_policy_update().epoch() != 0) {
        auto handler = std::atomic_load_explicit(&evictionPolicyUpdateHandler_, std::memory_order_acquire);
        if (handler != nullptr && *handler != nullptr) {
            (*handler)(rsp.eviction_policy_update());
        }
    }
    auto nextNodeInfos = std::make_shared<NodeInfoSnapshot>();
    nextNodeInfos->rankList.reserve(rsp.stats().size());
    for (const auto &info : rsp.stats()) {
        nextNodeInfos->rankList.emplace_back(info.address(), info.available_memory(), info.is_ready(), 0,
                                             info.used_memory(), info.memory_capacity(), info.memory_limit(),
                                             info.hot_primary_copy_count(), info.total_primary_copy_count(),
                                             info.hot_primary_copy_bytes());
        if (info.is_ready()) {
            nextNodeInfos->totalSize += info.available_memory();
        }
    }
    std::sort(nextNodeInfos->rankList.begin(), nextNodeInfos->rankList.end(),
              [](const NodeInfo &a, const NodeInfo &b) { return b < a; });
    std::atomic_store_explicit(&nodeInfos_, std::shared_ptr<const NodeInfoSnapshot>(std::move(nextNodeInfos)),
                               std::memory_order_release);
    return Status::OK();
}

void NodeSelector::RunPreReportHooks(uint64_t nowMs)
{
    std::vector<std::function<void()>> hooks;
    {
        std::lock_guard<std::mutex> lock(preReportHooksMutex_);
        hooks.reserve(preReportHooks_.size());
        for (auto &hook : preReportHooks_) {
            const bool due = !hook.hasRun || hook.minIntervalMs == 0 || nowMs - hook.lastRunMs >= hook.minIntervalMs;
            if (due) {
                hook.hasRun = true;
                hook.lastRunMs = nowMs;
                hooks.emplace_back(hook.callback);
            }
        }
    }
    for (const auto &hook : hooks) {
        if (hook != nullptr) {
            try {
                hook();
            } catch (const std::exception &e) {
                LOG(WARNING) << "PreReportHook threw, ignoring: " << e.what();
            } catch (...) {
                LOG(WARNING) << "PreReportHook threw non-std exception, ignoring.";
            }
        }
    }
}
}  // namespace object_cache
}  // namespace datasystem
