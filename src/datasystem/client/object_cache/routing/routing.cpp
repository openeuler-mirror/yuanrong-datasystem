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

#include "datasystem/client/object_cache/routing/routing.h"

#include <unordered_map>
#include <utility>

#include "datasystem/client/object_cache/routing/routing_rpc_client.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace client {
Routing::Routing(BrpcChannelConfig channelConfig, std::shared_ptr<Signature> signature,
                 HashRingRefresher::RingUpdateHook ringUpdateHook,
                 std::vector<std::shared_ptr<IWorkerFilter>> additionalFilters, int64_t refreshIntervalMs,
                 std::function<void(uint64_t)> refreshConfirmedHook)
    : Routing(std::move(channelConfig), std::move(signature), std::move(ringUpdateHook),
              std::make_shared<WorkerUbHealthRegistry>(), std::move(additionalFilters), refreshIntervalMs,
              std::move(refreshConfirmedHook))
{
}

Routing::Routing(BrpcChannelConfig channelConfig, std::shared_ptr<Signature> signature,
                 HashRingRefresher::RingUpdateHook ringUpdateHook,
                 std::shared_ptr<WorkerUbHealthRegistry> ubHealthRegistry,
                 std::vector<std::shared_ptr<IWorkerFilter>> additionalFilters, int64_t refreshIntervalMs,
                 std::function<void(uint64_t)> refreshConfirmedHook)
    : router_(std::make_shared<WorkerRouter>(
          "", std::move(ubHealthRegistry), std::move(additionalFilters),
          std::make_shared<ClientReadBandwidthScheduler>(ClientReadBandwidthScheduler::Config::FromFlags()))),
      rpcClient_(std::make_shared<RoutingRpcClient>(std::move(channelConfig), std::move(signature))),
      refreshIntervalMs_(refreshIntervalMs),
      refreshConfirmedHook_(std::move(refreshConfirmedHook))
{
    auto applyRingUpdate = [this, ringUpdateHook = std::move(ringUpdateHook)](
                               uint64_t newVersion, const ::datasystem::ClusterTopologyPb &ring,
                               const std::unordered_map<std::string, std::string> &hostIdMap,
                               bool epochResetConfirmed) {
        if (ringUpdateHook) {
            RETURN_IF_NOT_OK(ringUpdateHook(newVersion, ring, hostIdMap, epochResetConfirmed));
        }
        rpcClient_->PruneConnections(ring);
        return Status::OK();
    };
    auto fetchRpc = [this](const HostPort &workerAddr, uint64_t currentVersion, ::datasystem::ClusterTopologyPb &ring,
                           std::string &masterAddress, uint64_t &newVersion, bool &changed,
                           std::unordered_map<std::string, std::string> &hostIdMap, int32_t timeoutMs) {
        return FetchHashRing(workerAddr, currentVersion, ring, masterAddress, newVersion, changed, hostIdMap,
                             timeoutMs);
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
                              ::datasystem::ClusterTopologyPb &ring, std::string &masterAddress, uint64_t &newVersion,
                              bool &changed, std::unordered_map<std::string, std::string> &hostIdMap, int32_t timeoutMs)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    GetHashRingRspPb response;
    RETURN_IF_NOT_OK(rpcClient_->GetHashRing(workerAddr, currentVersion, response, timeoutMs,
                                             refresher_->GetHostIdsDigest(currentVersion)));
    newVersion = response.version();
    changed = response.hash_ring_changed();
    masterAddress = response.master_address();
    if (!changed) {
        if (newVersion == currentVersion && refreshConfirmedHook_) {
            refreshConfirmedHook_(newVersion);
        }
        return Status::OK();
    }
    // Retry unresolved local host identity on any full response, including hostId-only updates.
    const bool resolveInitialHostId = initialWorkerIsLocal_ && !hostIdResolutionAttempted_.load();
    CHECK_FAIL_RETURN_STATUS(response.has_hash_ring(), K_RUNTIME_ERROR,
                             "GetHashRing response is missing the changed hash ring");
    ring = response.hash_ring();
    hostIdMap.clear();
    for (const auto &entry : response.host_id_map()) {
        hostIdMap.emplace(entry.first, entry.second);
    }
    if (!response.host_ids_digest().empty()) {
        Hasher hasher;
        std::string digest;
        RETURN_IF_NOT_OK(hasher.GetStringMapSha256Hex(hostIdMap, digest));
        CHECK_FAIL_RETURN_STATUS(digest == response.host_ids_digest(), K_INVALID,
                                 "GetHashRing host ID digest does not match its payload");
    }
    if (resolveInitialHostId) {
        auto iter = hostIdMap.find(initialWorkerAddr_.ToString());
        if (iter != hostIdMap.end() && !iter->second.empty()) {
            router_->SetHostId(iter->second);
            hostIdResolutionAttempted_.store(true, std::memory_order_release);
        } else {
            LOG(WARNING) << "[Routing] Initial worker host ID is absent from GetHashRing response, endpoint="
                         << initialWorkerAddr_.ToString()
                         << "; same-node worker affinity stays degraded until a routing snapshot "
                            "update resolves it; verify --host_id_env_name is set on workers.";
        }
    }
    return Status::OK();
}

Status Routing::SelectWorker(const std::string &key, DataPlacementPolicy policy, WorkerAccessAction action,
                             HostPort &worker, const std::vector<HostPort> &exclude)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorker(key, policy, action, worker, exclude);
}

Status Routing::SelectWorkerFromCandidates(const std::vector<HostPort> &candidates, DataPlacementPolicy policy,
                                           WorkerAccessAction action, HostPort &worker,
                                           const std::vector<HostPort> &exclude)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorkerFromCandidates(candidates, policy, action, worker, exclude);
}

Status Routing::SelectWorkers(const std::vector<std::string> &keys, DataPlacementPolicy policy,
                              WorkerAccessAction action,
                              std::unordered_map<HostPort, std::vector<std::string>> &groups,
                              const std::vector<HostPort> &exclude)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(), K_NOT_READY, "Routing is not initialized");
    return router_->SelectWorkers(keys, policy, action, groups, exclude);
}

std::vector<HostPort> Routing::GetAvailableWorkers() const
{
    return router_->GetAvailableWorkers();
}

bool Routing::IsWorkerConnectionBroken(const HostPort &addr) const
{
    return router_ != nullptr && router_->IsWorkerConnectionBroken(addr);
}

std::vector<HostPort> Routing::GetAvailableSameNodeWorkers() const
{
    return router_->GetAvailableSameNodeWorkers();
}

void Routing::UpdateState(const HostPort &addr, StatusCode status)
{
    if (initialized_.load()) {
        router_->UpdateState(addr, status);
        if (status == K_CLIENT_WORKER_DISCONNECT && refresher_ != nullptr) {
            refresher_->ForceRefresh();
        }
    }
}

bool Routing::ForceRefresh()
{
    if (initialized_.load() && refresher_ != nullptr) {
        return refresher_->ForceRefresh();
    }
    return false;
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
