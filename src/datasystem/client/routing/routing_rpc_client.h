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

#ifndef DATASYSTEM_CLIENT_ROUTING_ROUTING_RPC_CLIENT_H
#define DATASYSTEM_CLIENT_ROUTING_ROUTING_RPC_CLIENT_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/object_posix.brpc.stub.pb.h"

namespace datasystem {
namespace client {

class RoutingRpcClient {
public:
    using ChannelFactory = std::function<std::shared_ptr<brpc::Channel>(const BrpcChannelConfig &)>;

    RoutingRpcClient(BrpcChannelConfig channelConfig, std::shared_ptr<Signature> signature,
                     ChannelFactory channelFactory = {});
    ~RoutingRpcClient();

    Status Init();

    Status GetHashRing(const HostPort &workerAddr, uint64_t currentVersion, GetHashRingRspPb &response);

    void Shutdown();

    void PruneConnections(const ::datasystem::ClusterTopologyPb &ring);

private:
    struct Connection {
        std::shared_ptr<brpc::Channel> channel;
        std::shared_ptr<WorkerOCService_BrpcGenericStub> stub;
    };

    Status GetOrCreateConnection(const HostPort &workerAddr, std::shared_ptr<Connection> &connection);

    BrpcChannelConfig channelConfig_;
    std::shared_ptr<Signature> signature_;
    ChannelFactory channelFactory_;
    std::atomic<bool> initialized_{ false };
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<Connection>> connections_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_ROUTING_RPC_CLIENT_H
