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
 * Description: WorkerRouter - core worker selection component.
 * Holds hash ring data (written by Refresher), traverses filter chain,
 * selects worker by data placement policy. Does NOT do RPC calls, retries, or error handling.
 */
#ifndef DATASYSTEM_CLIENT_ROUTING_WORKER_ROUTER_H
#define DATASYSTEM_CLIENT_ROUTING_WORKER_ROUTER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/client/object_cache/routing/client_read_bandwidth_scheduler.h"
#include "datasystem/client/object_cache/routing/data_placement_policy.h"
#include "datasystem/client/object_cache/routing/i_worker_filter.h"
#include "datasystem/client/object_cache/routing/worker_ub_health_registry.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

enum class WorkerRingState {
    UNKNOWN,
    INITIAL,
    JOINING,
    ACTIVE,
    PRE_LEAVING,
    LEAVING,
    FAILED,
};

struct TokenIndex {
    std::vector<std::pair<uint32_t, int>> tokenToWorker;
    std::vector<HostPort> workers;
};

class PreparedClusterTopology {
public:
    struct ConstructionToken {
    private:
        ConstructionToken() = default;

        friend class PreparedClusterTopology;
    };

    static Status Create(::datasystem::ClusterTopologyPb &&ring,
                         std::unique_ptr<PreparedClusterTopology> &prepared);
    PreparedClusterTopology(ConstructionToken, std::shared_ptr<const ::datasystem::ClusterTopologyPb> topology,
                            std::shared_ptr<const TokenIndex> tokenIndex);
    ~PreparedClusterTopology() = default;

    const ::datasystem::ClusterTopologyPb &GetTopology() const noexcept;
    const std::shared_ptr<const TokenIndex> &GetTokenIndex() const noexcept;

private:
    static Status BuildTokenIndex(const ::datasystem::ClusterTopologyPb &ring,
                                  std::shared_ptr<const TokenIndex> &tokenIndex);

    std::shared_ptr<const ::datasystem::ClusterTopologyPb> topology_;
    std::shared_ptr<const TokenIndex> tokenIndex_;

    friend class WorkerRouter;
};

class WorkerRouter {
public:
    explicit WorkerRouter(std::string myHostId,
                          std::vector<std::shared_ptr<IWorkerFilter>> additionalFilters = {},
                          std::shared_ptr<ClientReadBandwidthScheduler> bandwidthScheduler = nullptr);
    WorkerRouter(std::string myHostId, std::shared_ptr<WorkerUbHealthRegistry> ubHealthRegistry,
                 std::vector<std::shared_ptr<IWorkerFilter>> additionalFilters = {},
                 std::shared_ptr<ClientReadBandwidthScheduler> bandwidthScheduler = nullptr);
    ~WorkerRouter() = default;

    void SetHostId(std::string hostId);

    // Single key selection. exclude list avoids specific workers (e.g., LEAVING on write retry).
    // action is required so every call site classifies the selection (see WorkerAccessAction).
    Status SelectWorker(const std::string &key, DataPlacementPolicy policy, WorkerAccessAction action,
                        HostPort &worker, const std::vector<HostPort> &exclude = {}) const;

    Status SelectWorkerFromCandidates(const std::vector<HostPort> &candidates, DataPlacementPolicy policy,
                                      WorkerAccessAction action, HostPort &worker,
                                      const std::vector<HostPort> &exclude = {}) const;

    // Batch selection: group keys by owner, return map<worker, keys>.
    Status SelectWorkers(const std::vector<std::string> &keys, DataPlacementPolicy policy,
                         WorkerAccessAction action,
                         std::unordered_map<HostPort, std::vector<std::string>> &groups,
                         const std::vector<HostPort> &exclude = {}) const;

    Status SelectWorkerByScheduling(const std::string &key, DataPlacementPolicy policy, WorkerAccessAction action,
                                    HostPort &worker, const std::vector<HostPort> &exclude = {}) const;

    std::vector<HostPort> GetAvailableSameNodeWorkers() const;

    std::vector<HostPort> GetAvailableWorkers() const;

    bool IsWorkerConnectionBroken(const HostPort &addr) const;

    // The returned immutable snapshot remains valid independently of later health updates.
    std::shared_ptr<const UbRoutingHealthSnapshot> GetUbRoutingHealthSnapshot() const;

    // Called by Refresher to update hash ring data.
    void UpdateHashRing(const PreparedClusterTopology &prepared,
                        const std::unordered_map<std::string, std::string> &hostIdMap);

    // Broadcast state change to all filters.
    void UpdateState(const HostPort &addr, StatusCode status);

    // Query ring state for a worker (used by StateFilter).
    WorkerRingState GetRingState(const HostPort &addr) const;

    std::shared_ptr<ClientReadBandwidthScheduler> GetBandwidthScheduler() const
    {
        return bandwidthScheduler_;
    }

private:
    std::string myHostId_;
    std::shared_ptr<WorkerUbHealthRegistry> ubHealthRegistry_;
    std::vector<std::shared_ptr<IWorkerFilter>> filters_;
    std::atomic<bool> initialized_{ false };
    std::shared_ptr<ClientReadBandwidthScheduler> bandwidthScheduler_;

    // Single atomic snapshot — all readers see consistent ring + index + sameNode.
    struct RingView {
        std::shared_ptr<const ::datasystem::ClusterTopologyPb> ring;
        std::shared_ptr<const std::vector<HostPort>> sameNodeWorkers;
        std::shared_ptr<const TokenIndex> tokenIndex;
    };
    std::shared_ptr<const RingView> ringView_;

    bool IsWorkerAvailable(const HostPort &addr, WorkerAccessAction action) const;
    bool IsExcluded(const HostPort &addr, const std::vector<HostPort> &exclude) const;
    Status SelectWorkerFromView(const std::string &key, DataPlacementPolicy policy, WorkerAccessAction action,
                                HostPort &worker, const std::vector<HostPort> &exclude,
                                const std::shared_ptr<const RingView> &view) const;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_WORKER_ROUTER_H
