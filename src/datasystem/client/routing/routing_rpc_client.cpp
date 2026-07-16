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

#include "datasystem/client/routing/routing_rpc_client.h"

#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_options.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

RoutingRpcClient::RoutingRpcClient(BrpcChannelConfig channelConfig, std::shared_ptr<Signature> signature,
                                   ChannelFactory channelFactory)
    : channelConfig_(std::move(channelConfig)),
      signature_(std::move(signature)),
      channelFactory_(std::move(channelFactory))
{
    if (!channelFactory_) {
        channelFactory_ = [](const BrpcChannelConfig &config) {
            return std::shared_ptr<brpc::Channel>(BrpcChannelFactory::Create(config));
        };
    }
}

RoutingRpcClient::~RoutingRpcClient() = default;

Status RoutingRpcClient::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(signature_);
    CHECK_FAIL_RETURN_STATUS(channelConfig_.timeout_ms > 0, K_INVALID, "RPC timeout must be positive");
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(channelFactory_), K_INVALID, "Routing channel factory must be set");
    initialized_.store(true, std::memory_order_release);
    return Status::OK();
}

Status RoutingRpcClient::GetHashRing(const HostPort &workerAddr, uint64_t currentVersion, GetHashRingRspPb &response)
{
    CHECK_FAIL_RETURN_STATUS(initialized_.load(std::memory_order_acquire), K_NOT_READY,
                             "Routing RPC client is not initialized");
    std::shared_ptr<Connection> connection;
    RETURN_IF_NOT_OK(GetOrCreateConnection(workerAddr, connection));
    GetHashRingReqPb request;
    request.set_version(currentVersion);
    RETURN_IF_NOT_OK(signature_->GenerateSignature(request));
    RpcOptions options;
    options.SetTimeout(channelConfig_.timeout_ms);
    Status rc = connection->stub->GetHashRing(options, request, response);
    if (rc.IsError()) {
        rc = WithRpcDiag(rc, "GetHashRing", workerAddr);
        LOG(WARNING) << "[Routing] GetHashRing failed, endpoint=" << workerAddr.ToString()
                     << ", version=" << currentVersion << ", status=" << rc.ToString();
    }
    return rc;
}

void RoutingRpcClient::Shutdown()
{
    decltype(connections_) connections;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connections.swap(connections_);
    }
    initialized_.store(false, std::memory_order_release);
    VLOG(1) << "[Routing] Shutdown " << connections.size() << " control RPC connection(s)";
}

void RoutingRpcClient::PruneConnections(const ::datasystem::ClusterTopologyPb &ring)
{
    std::unordered_set<std::string> activeEndpoints;
    activeEndpoints.reserve(ring.members_size());
    for (const auto &member : ring.members()) {
        activeEndpoints.emplace(member.first);
    }
    std::vector<std::shared_ptr<Connection>> staleConnections;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto iter = connections_.begin(); iter != connections_.end();) {
            if (activeEndpoints.count(iter->first) == 0) {
                staleConnections.emplace_back(std::move(iter->second));
                iter = connections_.erase(iter);
            } else {
                ++iter;
            }
        }
    }
    VLOG(1) << "[Routing] Pruned " << staleConnections.size() << " stale control RPC connection(s)";
}

Status RoutingRpcClient::GetOrCreateConnection(const HostPort &workerAddr, std::shared_ptr<Connection> &connection)
{
    const auto endpoint = workerAddr.ToString();
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = connections_.find(endpoint);
    if (iter != connections_.end()) {
        connection = iter->second;
        return Status::OK();
    }
    auto config = channelConfig_;
    config.endpoint = endpoint;
    auto channel = channelFactory_(config);
    CHECK_FAIL_RETURN_STATUS(channel != nullptr, K_RPC_UNAVAILABLE,
                             "Failed to create routing brpc channel for " + endpoint);
    auto stub = std::make_shared<WorkerOCService_BrpcGenericStub>(channel.get(), config.timeout_ms);
    connection = std::make_shared<Connection>(Connection{ std::move(channel), std::move(stub) });
    connections_.emplace(endpoint, connection);
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
