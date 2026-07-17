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
 * selects worker by strategy. Does NOT do RPC calls, retries, or error handling.
 */
#ifndef DATASYSTEM_CLIENT_ROUTING_WORKER_ROUTER_H
#define DATASYSTEM_CLIENT_ROUTING_WORKER_ROUTER_H

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/client/routing/i_worker_filter.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

enum class DataPlacementPolicy;

// Legacy compatibility enum. New routing code must use DataPlacementPolicy so REQUIRED_SAME_NODE remains expressible.
enum class SelectStrategy {
    HASH_RING_AFFINITY,     // Select by key hash on ring (metadata owner)
    SAME_NODE_PREFERRED,    // Prefer same-node worker, fallback to ring
};

enum class WorkerRingState {
    UNKNOWN,
    INITIAL,
    JOINING,
    ACTIVE,
    PRE_LEAVING,
    LEAVING,
    FAILED,
};

class WorkerRouter {
public:
    explicit WorkerRouter(std::string myHostId,
                          std::vector<std::shared_ptr<IWorkerFilter>> additionalFilters = {});
    ~WorkerRouter() = default;

    void SetHostId(std::string hostId);

    // Single key selection. exclude list avoids specific workers (e.g., LEAVING on write retry).
    // Legacy compatibility overload. New callers must use DataPlacementPolicy.
    Status SelectWorker(const std::string &key, SelectStrategy strategy, HostPort &worker,
                        const std::vector<HostPort> &exclude = {}) const;

    Status SelectWorker(const std::string &key, DataPlacementPolicy policy, HostPort &worker,
                        const std::vector<HostPort> &exclude = {}) const;

    // Batch selection: group keys by owner, return map<worker, keys>.
    // Legacy compatibility overload. New callers must use DataPlacementPolicy.
    Status SelectWorkers(const std::vector<std::string> &keys, SelectStrategy strategy,
                         std::unordered_map<HostPort, std::vector<std::string>> &groups) const;

    Status SelectWorkers(const std::vector<std::string> &keys, DataPlacementPolicy policy,
                         std::unordered_map<HostPort, std::vector<std::string>> &groups,
                         const std::vector<HostPort> &exclude = {}) const;

    std::vector<HostPort> GetAvailableWorkers() const;

    // Called by Refresher to update hash ring data.
    void UpdateHashRing(std::shared_ptr<const ::datasystem::ClusterTopologyPb> ring,
                        std::shared_ptr<const std::unordered_map<std::string, std::string>> hostIdMap);

    // Broadcast state change to all filters.
    void UpdateState(const HostPort &addr, StatusCode status);

    // Query ring state for a worker (used by StateFilter).
    WorkerRingState GetRingState(const HostPort &addr) const;

private:
    std::string myHostId_;
    std::vector<std::shared_ptr<IWorkerFilter>> filters_;

    // Single atomic snapshot — all readers see consistent ring + index + sameNode.
    struct RingView {
        std::shared_ptr<const ::datasystem::ClusterTopologyPb> ring;
        std::shared_ptr<const std::vector<HostPort>> sameNodeWorkers;
        struct TokenIndex {
            std::vector<std::pair<uint32_t, int>> tokenToWorker;
            std::vector<HostPort> workers;
        };
        std::shared_ptr<const TokenIndex> tokenIndex;
    };
    std::shared_ptr<const RingView> ringView_;

    bool IsWorkerAvailable(const HostPort &addr) const;
    bool IsExcluded(const HostPort &addr, const std::vector<HostPort> &exclude) const;
    Status SelectWorkerFromView(const std::string &key, DataPlacementPolicy policy, HostPort &worker,
                                const std::vector<HostPort> &exclude,
                                const std::shared_ptr<const RingView> &view) const;
    static std::shared_ptr<const RingView::TokenIndex> BuildTokenIndex(const ::datasystem::ClusterTopologyPb &ring);
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_WORKER_ROUTER_H
