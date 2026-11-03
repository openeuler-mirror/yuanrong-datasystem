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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_WORKER_STREAM_SERVICES_H
#define DATASYSTEM_WORKER_STREAM_CACHE_WORKER_STREAM_SERVICES_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/rpc/brpc_auth.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/master/metadata_manager_holder.h"
#include "datasystem/worker/metadata_route_resolver.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/request_authenticator.h"

#include "datasystem/utils/status.h"

namespace datasystem {
namespace worker {
struct StreamFailureActions {
    std::function<Status(const cluster::TopologyCallbackContext &)> recover;
    std::function<Status(const cluster::TopologyCallbackContext &)> cleanup;
};

class WorkerStreamServices {
public:
    WorkerStreamServices();
    ~WorkerStreamServices();
    WorkerStreamServices(const WorkerStreamServices &) = delete;
    WorkerStreamServices &operator=(const WorkerStreamServices &) = delete;

    bool Enabled() const;

    Status CheckEncryptSecretKey() const;

    void CreateMasterServices(const HostPort &host, const std::shared_ptr<AkSkManager> &akSkManager,
                              MetadataManagerHolder *holder, const cluster::MembershipEndpointView &membership,
                              bool isRestart);
    void CreateWorkerServices(const HostPort &host,
                              const std::shared_ptr<const RequestAuthenticator> &requestAuthenticator,
                              const std::shared_ptr<object_cache::WorkerOcEvictionManager> &evictionManager,
                              const MetadataRouteResolver &metadataRoute,
                              const cluster::MembershipEndpointView &membership);
    void ConfigureMetadataManagerHolderParam(MetadataManagerHolderParam &param) const;
    void EnableLocalBypass(const HostPort &host);
    Status StartCheckMetadata() const;

    Status InitMasterSCService(RpcServer *rpcServer, RpcServer::Builder &builder, bool rpcAuthEnabled,
                               const std::shared_ptr<AkSkManager> &akSkManager,
                               std::vector<std::unique_ptr<BrpcAuthService>> &rpcAuthServices);
    Status InitClientWorkerSCService(RpcServer *rpcServer, RpcServer::Builder &builder, bool rpcAuthEnabled,
                                     const std::shared_ptr<AkSkManager> &akSkManager,
                                     std::vector<std::unique_ptr<BrpcAuthService>> &rpcAuthServices);
    Status InitMasterWorkerSCService(RpcServer *rpcServer, RpcServer::Builder &builder, bool rpcAuthEnabled,
                                     const std::shared_ptr<AkSkManager> &akSkManager,
                                     std::vector<std::unique_ptr<BrpcAuthService>> &rpcAuthServices);
    Status InitWorkerWorkerSCService(RpcServer *rpcServer, RpcServer::Builder &builder, bool rpcAuthEnabled,
                                     const std::shared_ptr<AkSkManager> &akSkManager,
                                     std::vector<std::unique_ptr<BrpcAuthService>> &rpcAuthServices);

    void RegisterWorkerResourceMetrics(ResMetricCollector &instance) const;
    Status ClosePubSubForClientLost(const ClientKey &clientId) const;
    bool HaveTasksToProcess() const;

    void ShutdownMaster();
    void ReleaseAdapters();
    void ReleaseServices();

    Status MigrateTopologyMetadata(const cluster::TopologyCallbackContext &context) const;
    Status PrepareFailureActions(MetadataManagerHolder &metadataManagers, StreamFailureActions &actions) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_STREAM_CACHE_WORKER_STREAM_SERVICES_H
