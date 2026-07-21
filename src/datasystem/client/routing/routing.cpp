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

#include "datasystem/client/routing/routing.h"

#include <unordered_map>
#include <utility>

#include "datasystem/client/routing/routing_rpc_client.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace client {

Routing::Routing(BrpcChannelConfig channelConfig, std::shared_ptr<Signature> signature,
                 HashRingRefresher::RingUpdateHook ringUpdateHook, int64_t refreshIntervalMs)
    : router_(std::make_shared<WorkerRouter>("")),
      rpcClient_(std::make_shared<RoutingRpcClient>(std::move(channelConfig), std::move(signature))),
      refreshIntervalMs_(refreshIntervalMs)
{
    auto applyRingUpdate = [this, ringUpdateHook = std::move(ringUpdateHook)](
                               uint64_t newVersion, const ::datasystem::ClusterTopologyPb &ring,
                               const std::unordered_map<std::string, std::string> &hostIdMap) {
        if (ringUpdateHook) {
            RETURN_IF_NOT_OK(ringUpdateHook(newVersion, ring, hostIdMap));
        }
        rpcClient_->PruneConnections(ring);
        return Status::OK();
    };
    auto fetchRpc = [this](const HostPort &workerAddr, uint64_t currentVersion,
                           ::datasystem::ClusterTopologyPb &ring, std::string &masterAddress,
                           uint64_t &newVersion, bool &changed,
                           std::unordered_map<std::string, std::string> &hostIdMap) {
        return FetchHashRing(workerAddr, currentVersion, ring, masterAddress, newVersion, changed, hostIdMap);
    };
    refresher_ = std::make_shared<HashRingRefresher>(router_, std::move(fetchRpc), std::move(applyRingUpdate));
}

Routing::Routing(std::shared_ptr<WorkerRouter> router, std::shared_ptr<HashRingRefresher> refresher,
                 int64_t refreshIntervalMs)
    : router_(std::move(router)), refresher_(std::move(refresher)), refreshIntervalMs_(refreshIntervalMs)
{
}

Routing::~Routing()
{
    Shutdown();
}

Status Routing::Init(const std::string &hostId, const HostPort &initialWorkerAddr, bool initialWorkerIsLocal)
{
    RETURN_RUNTIME_ERROR_IF_NULL(router_);
    RETURN_RUNTIME_ERROR_IF_NULL(refresher_);
    CHECK_FAIL_RETURN_STATUS(!initialWorkerAddr.Empty(), K_INVALID, "Initial worker address must not be empty");
    CHECK_FAIL_RETURN_STATUS(!initialized_.load(), K_INVALID, "Routing is already initialized");
    if (rpcClient_ != nullptr) {
        RETURN_IF_NOT_OK(rpcClient_->Init());
    }

    initialWorkerAddr_ = initialWorkerAddr;
    initialWorkerIsLocal_ = initialWorkerIsLocal;
    hostIdResolutionAttempted_.store(!hostId.empty() || !initialWorkerIsLocal);
    router_->SetHostId(hostId);
    RETURN_IF_NOT_OK(refresher_->InitialFetch(initialWorkerAddr));
    RETURN_IF_NOT_OK(refresher_->StartPeriodicRefresh(refreshIntervalMs_));
    initialized_.store(true);
    return Status::OK();
}

Status Routing::FetchHashRing(const HostPort &workerAddr, uint64_t currentVersion,
                              ::datasystem::ClusterTopologyPb &ring, std::string &masterAddress,
                              uint64_t &newVersion, bool &changed,
                              std::unordered_map<std::string, std::string> &hostIdMap)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    GetHashRingRspPb response;
    RETURN_IF_NOT_OK(rpcClient_->GetHashRing(workerAddr, currentVersion, response));
    newVersion = response.version();
    changed = response.hash_ring_changed();
    masterAddress = response.master_address();
    if (!changed) {
        return Status::OK();
    }
    // Only consume the one-shot hostId resolution flag when the ring actually changed. If the
    // coordinator reports changed==false on the first fetch (ring already stable at SDK startup),
    // consuming the flag here would mean hostId never resolves for the SDK's lifetime, leaving
    // sameNodeWorkers empty and disabling same-host SHM partitioning. Move it past the early return.
    const bool resolveInitialHostId =
        initialWorkerIsLocal_ && !hostIdResolutionAttempted_.exchange(true, std::memory_order_acq_rel);
    CHECK_FAIL_RETURN_STATUS(response.has_hash_ring(), K_RUNTIME_ERROR,
                             "GetHashRing response is missing the changed hash ring");
    ring = response.hash_ring();
    hostIdMap.clear();
    for (const auto &entry : response.host_id_map()) {
        hostIdMap.emplace(entry.first, entry.second);
    }
    if (resolveInitialHostId) {
        auto iter = hostIdMap.find(initialWorkerAddr_.ToString());
        if (iter != hostIdMap.end() && !iter->second.empty()) {
            router_->SetHostId(iter->second);
        } else {
            // The initial worker reported an empty host_id (worker started without --host_id_env_name),
            // so the SDK's own host_id stays empty and same-node worker partitioning is disabled.
            // With sdk_data_placement_policy=PREFERRED_SAME_NODE, every key degrades to the hash ring
            // (cross-node routing), which can time out for large payloads. Check the worker's startup
            // log for the matching "host_id_env_name is not set" warning.
            LOG(WARNING) << "[Routing] Initial worker host ID is absent from GetHashRing response, endpoint="
                         << initialWorkerAddr_.ToString()
                         << "; same-node worker affinity is disabled, routing degrades to the hash ring"
                            " (cross-node). Verify --host_id_env_name is set on workers.";
        }
    }
    return Status::OK();
}

Status Routing::SelectWorker(const std::string &key, SelectStrategy strategy, HostPort &worker,
                             const std::vector<HostPort> &exclude)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorker(key, strategy, worker, exclude);
}

Status Routing::SelectWorker(const std::string &key, DataPlacementPolicy policy, HostPort &worker,
                             const std::vector<HostPort> &exclude)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorker(key, policy, worker, exclude);
}

Status Routing::SelectWorkers(const std::vector<std::string> &keys, SelectStrategy strategy,
                              std::unordered_map<HostPort, std::vector<std::string>> &groups)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorkers(keys, strategy, groups);
}

Status Routing::SelectWorkers(const std::vector<std::string> &keys, DataPlacementPolicy policy,
                              std::unordered_map<HostPort, std::vector<std::string>> &groups,
                              const std::vector<HostPort> &exclude)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorkers(keys, policy, groups, exclude);
}

void Routing::UpdateState(const HostPort &addr, StatusCode status)
{
    if (initialized_.load()) {
        router_->UpdateState(addr, status);
    }
}

void Routing::Shutdown()
{
    if (refresher_ != nullptr) {
        refresher_->Stop();
    }
    if (rpcClient_ != nullptr) {
        rpcClient_->Shutdown();
    }
    initialized_.store(false);
}

Status ParseDataPlacementPolicy(const std::string &value, DataPlacementPolicy &policy)
{
    const std::string normalized = StringToUpper(Trim(value));
    if (normalized == "PREFERRED_SAME_NODE") {
        policy = DataPlacementPolicy::PREFERRED_SAME_NODE;
        return Status::OK();
    }
    if (normalized == "REQUIRED_SAME_NODE") {
        policy = DataPlacementPolicy::REQUIRED_SAME_NODE;
        return Status::OK();
    }
    if (normalized == "PREFERRED_META_OWNER") {
        policy = DataPlacementPolicy::PREFERRED_META_OWNER;
        return Status::OK();
    }
    return Status(K_INVALID, "Unknown data placement policy: '" + value
                                 + "'. Valid: PREFERRED_SAME_NODE, REQUIRED_SAME_NODE, PREFERRED_META_OWNER");
}

}  // namespace client
}  // namespace datasystem
