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

/**
 * Description: The node info for available memory resource.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_NODE_SELECTOR_H
#define DATASYSTEM_MIGRATE_DATA_NODE_SELECTOR_H

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/worker_master_api_manager_base.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
struct ObjectCopyWatermark {
    uint64_t coldPrimaryCopyBytes{ 0 };
    uint64_t warmPrimaryCopyBytes{ 0 };
    uint64_t hotPrimaryCopyCount{ 0 };
    uint64_t totalPrimaryCopyCount{ 0 };
    uint64_t hotPrimaryCopyBytes{ 0 };
    uint64_t totalPrimaryCopyBytes{ 0 };
    uint64_t memoryCapacity{ 0 };
    bool valid{ false };

    std::string ToMetricsString() const;
};

class NodeSelector {
public:
    using EvictionPolicyUpdateHandler = std::function<void(const master::EvictionPolicyUpdatePb &)>;
    using RebalanceTaskHandler = std::function<void(const master::RebalanceTaskPb &, const std::string &)>;

    /**
     * @brief Get the singleton instance.
     * @return The singleton instance.
     */
    static NodeSelector &Instance();

    /**
     * @brief Init NodeSelector.
     * @param[in] localAddress The worker local address.
     * @param[in] membership Read-only topology membership view.
     * @param[in] exitRequested Local graceful-exit flag that outlives this selector.
     * @param[in] apiManager The manager of worker master api.
     */
    void Init(const std::string &localAddress, const cluster::MembershipEndpointView &membership,
              const std::atomic<bool> *exitRequested,
              std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager);

    /**
     * Shutdown the NodeSelector and cleanup resources.
     */
    void Shutdown();

    /**
     * @brief Select node from other workers.
     * @param[in] excludeNodes The nodes don't be selected.
     * @param[in] preferNode The prior node that should be selected.
     * @param[in] needSize The size that the selected node should be larger than.
     * @param[out] outNode The selected node.
     */
    Status SelectNode(const std::unordered_set<std::string> &excludeNodes, const std::string &preferNode,
                      size_t needSize, std::string &outNode);

    /**
     * @brief Get the available memory size from the target worker.
     * @param[in] address The address that the target worker is.
     * @return The available memory size from the address worker.
     */
    size_t GetAvailableMemory(const std::string &address);

    /**
     * @brief Try to get the available memory size from the target worker.
     * @param[in] address The address that the target worker is.
     * @param[out] availableMemory The available memory size from the address worker.
     * @return K_OK if the resource snapshot has a ready target worker, otherwise the error status.
     */
    Status TryGetAvailableMemory(const std::string &address, size_t &availableMemory);

    /**
     * @brief Has enough available memory from all workers.
     * @brief[in] needMemory the need  memory.
     * @return If the sum available memory from all workers is larger than the needMemory, return true, else false.
     */
    bool HasEnoughAvailableMemory(size_t needMemory);

    /**
     * @brief Register the callback that consumes memory rebalance tasks returned by master.
     * @param[in] handler The callback implemented by worker-side rebalance executor. The second argument is the
     *                    exact master address that returned the task.
     */
    void RegisterRebalanceTaskHandler(RebalanceTaskHandler handler);

    /**
     * @brief Clear the registered memory rebalance task callback.
     */
    void UnregisterRebalanceTaskHandler();

    void RegisterEvictionPolicyUpdateHandler(EvictionPolicyUpdateHandler handler);
    void UnregisterEvictionPolicyUpdateHandler();
    void SetEvictionPolicyReport(master::EvictionPolicyPb policy, uint64_t epoch,
                                 master::EvictionPolicyUpdatePhasePb phase);
    void SetEvictionPolicyControlReport(master::EvictionPolicyWorkerStatusPb status, uint64_t epoch,
                                        uint64_t totalObjects, uint64_t migratedObjects,
                                        const Status &failure = Status::OK());

    /**
     * @brief Register a callback invoked at the start of each periodic resource report
     *        (before CollectClusterInfo sends the report RPC). Used by the heat eviction
     *        strategy to run periodic heat decay aligned with the report cadence. No-op
     *        under the clock strategy. Multiple hooks may be registered and run in order.
     * @param[in] hook The callback to run before each report.
     * @param[in] minIntervalMs Minimum monotonic interval between executions. Zero runs on every report attempt.
     */
    void RegisterPreReportHook(std::function<void()> hook, uint64_t minIntervalMs = 0);

    /**
     * @brief Clear all registered pre-report hooks.
     */
    void UnregisterPreReportHooks();

    /**
     * @brief Set the local worker's hot primary copy statistics, reported in the next ResourceReport
     *        for the heat rebalance strategy. Computed by a pre-report hook (heat mode only); stays zero
     *        under clock eviction, so non-heat workers report zeros and the master treats them as non-hot.
     */
    void SetHotPrimaryReport(uint64_t hotPrimaryCopyCount, uint64_t totalPrimaryCopyCount,
                             uint64_t hotPrimaryCopyBytes, uint64_t totalPrimaryCopyBytes,
                             uint64_t memoryCapacity);

    /**
     * @brief Publish a telemetry-only copy-watermark snapshot.
     *
     * This cache is deliberately separate from the heat-rebalance report consumed by master. Workload tests refresh
     * it immediately before periodic resource reporting without changing the heat-maintenance cadence or scheduler
     * input.
     */
    void SetObjectCopyWatermark(uint64_t coldPrimaryCopyBytes, uint64_t warmPrimaryCopyBytes,
                                uint64_t hotPrimaryCopyCount, uint64_t totalPrimaryCopyCount,
                                uint64_t hotPrimaryCopyBytes, uint64_t totalPrimaryCopyBytes, uint64_t memoryCapacity);

    ObjectCopyWatermark GetObjectCopyWatermark() const;
#ifdef WITH_TESTS
    void RunPreReportHooksForTest(uint64_t nowMs)
    {
        RunPreReportHooks(nowMs);
    }

    ObjectCopyWatermark GetHotPrimaryReportForTest() const;
#endif
protected:
    NodeSelector();
    ~NodeSelector();

    /**
     * @brief Collect cluster info, report self memory info and get all workers info.
     * @return Status of this call.
     */
    Status CollectClusterInfo();

    /**
     * @brief Report resource.
     * @param[in] workerMasterApi The worker master api.
     * @param[in] req The request req.
     * @param[out] rsp The request response.
     * @return Status of this call.
     */
    Status ReportResource(const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                          master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp);

    /**
     * @brief Get worker master api.
     * @param[out] workerMasterApi The worker master api.
     * @return Status of this call.
     */
    Status GetWorkerMasterApi(std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi);

    /**
     * @brief Get standby worker.
     * @param[in] excludeNodes The nodes don't be selected.
     * @param[out] outNode The selected node.
     * @return Status of this call.
     */
    Status GetLocalStandbyWorker(const std::unordered_set<std::string> &excludeNodes, std::string &outNode);

    /**
     * @brief Try to get available memory from the current resource snapshot.
     * @param[in] address The address that the target worker is.
     * @param[out] availableMemory The available memory size from the address worker.
     * @param[out] hasSnapshot Whether the resource snapshot already has any node information.
     * @return K_OK if the snapshot has a ready target worker, otherwise the error status.
     */
    Status TryGetAvailableMemoryFromSnapshot(const std::string &address, size_t &availableMemory,
                                             bool &hasSnapshot) const;

    struct NodeInfoSnapshot {
        std::vector<NodeInfo> rankList;
        size_t totalSize{ 0 };
    };
    // CollectClusterInfo rebuilds the whole table already. Publishing that immutable table atomically removes the
    // selection-path shared mutex and prevents unlocked rankList reads in the worker retry loop.
    std::shared_ptr<const NodeInfoSnapshot> nodeInfos_;
private:
    NodeSelector(const NodeSelector &) = delete;
    NodeSelector(NodeSelector &&) = delete;
    NodeSelector &operator=(const NodeSelector &) = delete;
    NodeSelector &operator=(NodeSelector &&) = delete;

    /**
     * @brief The worker thread.
     */
    void WorkerThread();
    void RunPreReportHooks(uint64_t nowMs);

    std::string localAddress_;
    const cluster::MembershipEndpointView *membership_{ nullptr };
    const std::atomic<bool> *exitRequested_{ nullptr };
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager_{ nullptr };

    std::atomic<bool> running_;
    Thread workerThread_;
    std::mutex taskMutex_;
    std::condition_variable taskCv_;
    // Immutable callbacks let the reporting thread take a lifetime-safe snapshot without contending with the rare
    // register/unregister path. C++17 atomic shared_ptr operations are used at the access sites.
    std::shared_ptr<const RebalanceTaskHandler> rebalanceTaskHandler_;
    std::shared_ptr<const EvictionPolicyUpdateHandler> evictionPolicyUpdateHandler_;
    struct ResourceReportState {
        master::EvictionPolicyPb evictionPolicy{ master::EVICTION_POLICY_CLOCK };
        uint64_t evictionPolicyEpoch{ 0 };
        master::EvictionPolicyUpdatePhasePb evictionPolicyPhase{ master::EVICTION_POLICY_STABLE };
        master::EvictionPolicyWorkerStatusPb evictionPolicyWorkerStatus{ master::EVICTION_POLICY_WORKER_NONE };
        uint64_t evictionPolicyControlEpoch{ 0 };
        uint64_t evictionPolicyTotalObjects{ 0 };
        uint64_t evictionPolicyMigratedObjects{ 0 };
        Status evictionPolicyFailure;
        ObjectCopyWatermark hotPrimaryReport;
    };
    // Setters publish a copy-on-write snapshot with CAS so independent concurrent writers cannot overwrite each
    // other. ReportResource atomically loads one snapshot and therefore observes a coherent protobuf view.
    std::shared_ptr<const ResourceReportState> resourceReportState_;
    std::shared_ptr<const ObjectCopyWatermark> copyWatermark_;
    struct PreReportHook {
        std::function<void()> callback;
        uint64_t minIntervalMs{ 0 };
        uint64_t lastRunMs{ 0 };
        bool hasRun{ false };
    };
    // Hook cadence is mutable scheduler state. Keep it separate from report/callback snapshots and invoke callbacks
    // after releasing this lock.
    std::mutex preReportHooksMutex_;
    std::vector<PreReportHook> preReportHooks_;
    std::atomic<bool> subSuccess_{ false };
    WaitPost subReadyPost_;
    struct Token {
        std::atomic<bool> alive{true};
    };
    Token* token_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif
