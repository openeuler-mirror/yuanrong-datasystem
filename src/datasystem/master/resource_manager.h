/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: The resource manager define.
 */
#ifndef DATASYSTEM_MASTER_RESOURCE_MANAGER_H
#define DATASYSTEM_MASTER_RESOURCE_MANAGER_H

#include <array>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/heat_rebalance_scheduler.h"
#include "datasystem/master/memory_rebalance_scheduler.h"
#include "datasystem/master/rebalance_scheduler.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace master {
class ResourceManager {
public:
    using StoreProcessFunction = std::function<Status(const std::string &, std::unique_ptr<std::string> &, bool &)>;
    using RolloutLoader = std::function<Status(std::string &)>;
    using RolloutCas = std::function<Status(const StoreProcessFunction &)>;

    /**
     * @brief Construct the resource manager.
     */
    ResourceManager();

    /**
     * @brief Deconstruct the reousrce manager.
     */
    ~ResourceManager();

    /**
     * @brief Bind the read-only topology membership view used by memory rebalance scheduling.
     * @param[in] topologyMembership Non-owning view that outlives this resource manager.
     */
    void SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership);

    /**
     * @brief Report the memory info to master.
     * @param[in] req The req info.
     * @param[out] rsp The response of the call.
     * @return Status of this call.
     */
    Status ReportResource(const master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp);

    /**
     * @brief Accept the memory rebalance result reported by source worker.
     * @param[in] req The reported rebalance result.
     * @param[out] rsp The response of the call.
     * @return Status of this call.
     */
    Status ReportRebalanceResult(const master::ReportRebalanceResultReqPb &req,
                                 master::ReportRebalanceResultRspPb &rsp);

    /**
     * @brief Persist a complete PRECHECK or COMMIT rollout command.
     */
    Status SetEvictionPolicyUpdate(const master::EvictionPolicyUpdatePb &update, uint32_t cohortPercent);

    /**
     * @brief Return the latest worker acknowledgements observed for one rollout.
     */
    Status GetEvictionPolicyUpdateProgress(uint64_t epoch, master::GetEvictionPolicyUpdateProgressRspPb &rsp);

    /**
     * @brief Bind the shared rollout store and recover the last committed intent before serving requests.
     * @param[in] loader Exact-read callback for the rollout key.
     * @param[in] cas Callback-form CAS for the rollout key.
     * @return K_OK when the store is bound and any durable intent is recovered.
     */
    Status InitEvictionPolicyRolloutStore(RolloutLoader loader, RolloutCas cas);

#ifdef WITH_TESTS
    Status RefreshEvictionPolicyRolloutForTest()
    {
        return RefreshEvictionPolicyRollout();
    }
#endif

protected:
    /**
     * @brief Clear the expired resource in write snapshot.
     */
    void ClearWriteSnapshot();

    /**
     * @brief Switch the read/write snapshot.
     */
    void SwitchSnapshots();

private:
    /**
     * @brief Build a scheduling snapshot from the latest entry in both snapshot buffers.
     * @param[out] snapshot The merged scheduling snapshot.
     */
    void BuildLatestSnapshot(std::unordered_map<std::string, NodeInfo> &snapshot);

    /**
     * The worker thread.
     */
    void WorkerThread();

    /**
     * @brief Refresh the local intent from the shared store for multi-Master convergence.
     * @return Store read, decode, or validation status.
     */
    Status RefreshEvictionPolicyRollout();

    /**
     * @brief Validate one rollout and publish it when it is not older than local state.
     */
    Status ApplyEvictionPolicyRollout(const master::EvictionPolicyRolloutPb &rollout);

    void ApplyEvictionPolicyRolloutToReport(const master::ResourceReportReqPb &req, NodeInfo &nodeInfo,
                                            master::ResourceReportRspPb &rsp);

    /**
     * @brief Get current read snapshot.
     */
    const std::unordered_map<std::string, NodeInfo> &GetReadSnapshot() const
    {
        return readSnapshot_;
    };

    static constexpr int64_t WORKER_THREAD_INTERVAL_MS = 10 * 1000;
    Thread workerThread_;
    std::mutex taskMutex_;
    std::condition_variable taskCv_;
    std::atomic<bool> running_ = true;

    // Keep buffer identities stable while BuildLatestSnapshot copies them under separate data locks.
    SharedMutex snapshotSwapMutex_;
    std::mutex writeSnapshotMutex_;
    SharedMutex readSnapshotMutex_;
    std::unordered_map<std::string, NodeInfo> readSnapshot_{};
    std::unordered_map<std::string, NodeInfo> writeSnapshot_{};
    std::unique_ptr<RebalanceScheduler> rebalanceScheduler_;
    // Protects the rollout, its per-worker progress, and the immutable store callback snapshots. Store callbacks are
    // copied while holding this mutex and always invoked after releasing it.
    std::mutex evictionPolicyMutex_;
    // Resource reports atomically read this immutable snapshot. The mutex still protects publishing a new epoch
    // together with clearing/updating the per-worker progress map.
    std::shared_ptr<const master::EvictionPolicyRolloutPb> evictionPolicyRollout_;
    RolloutLoader evictionPolicyRolloutLoader_;
    RolloutCas evictionPolicyRolloutCas_;
    // Progress belongs to the active rollout epoch and is protected by evictionPolicyMutex_ so publishing a
    // new epoch and clearing stale worker observations is one atomic state transition.
    std::unordered_map<std::string, master::EvictionPolicyWorkerProgressPb> evictionPolicyWorkerProgress_;
};
}  // namespace master
}  // namespace datasystem
#endif
