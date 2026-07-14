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
  * Description: Coordinator RPC service implementation skeleton.
 */
#ifndef DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H
#define DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H

#include <memory>
#include <string>

#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/coordinator/watch_dispatcher_impl.h"
#include "datasystem/protos/coordinator.brpc.pb.h"
#include "datasystem/protos/coordinator.service.rpc.pb.h"

namespace datasystem {
namespace coordinator {
class CoordinatorServiceImpl : public CoordinatorService, public ICoordinatorService {
public:
    CoordinatorServiceImpl(const HostPort &localAddress);
    ~CoordinatorServiceImpl() override = default;

    /// Self-contained initialization: parses flags, loads auth keys, builds full component tree, configures RPC.
    Status Init();
    /// Start the RPC server and bind ports.
    Status Start();
    /// Shutdown RPC server and destroy all components in reverse dependency order.
    Status Shutdown();

    Status Put(const PutReqPb &req, PutRspPb &rsp) override;
    Status Range(const RangeReqPb &req, RangeRspPb &rsp) override;
    Status DeleteRange(const DeleteRangeReqPb &req, DeleteRangeRspPb &rsp) override;
    Status WatchRange(const WatchRangeReqPb &req, WatchRangeRspPb &rsp) override;
    Status CancelWatch(const CancelWatchReqPb &req, CancelWatchRspPb &rsp) override;
    Status KeepAlive(const KeepAliveReqPb &req, KeepAliveRspPb &rsp) override;

private:
    HostPort coordinatorAddr_;
    RpcServer::Builder builder_;
    // brpc mode — MUST be declared before rpcServer_: the brpc adapter holds a
    // reference to *this and is registered with the brpc::Server inside rpcServer_.
    // On destruction, rpcServer_ (~RpcServer → StopBrpcServer) must outlive the adapter.
    std::unique_ptr<CoordinatorServiceBrpcAdapter> brpcAdapter_;
    std::unique_ptr<RpcServer> rpcServer_;
    std::shared_ptr<MemoryKvStore> memStore_;
    std::shared_ptr<WatchRegistry> watchRegistry_;
    std::shared_ptr<WatchDispatcherImpl> watchDispatcher_;
    std::shared_ptr<SteadyClockReal> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::shared_ptr<CoordinatorStore> store_;
    // brpc mode address (set in Init, consumed in Start)
    std::string brpcAddr_;
    int brpcPort_ = 0;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_COORDINATOR_SERVICE_IMPL_H
