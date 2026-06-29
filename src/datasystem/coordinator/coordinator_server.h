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
 * Description: Lightweight coordinator server lifecycle.
 */
#ifndef DATASYSTEM_COORDINATOR_COORDINATOR_SERVER_H
#define DATASYSTEM_COORDINATOR_COORDINATOR_SERVER_H

#include <memory>

#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/coordinator/coordinator_service_impl.h"
#include "datasystem/coordinator/watch_dispatcher_impl.h"

namespace datasystem {
namespace coordinator {
class CoordinatorServer {
public:
    CoordinatorServer() = default;
    ~CoordinatorServer() = default;

    Status Init();
    Status Start();
    Status Shutdown();

private:
    HostPort coordinatorAddr_;
    RpcServer::Builder builder_;
    std::unique_ptr<RpcServer> rpcServer_;
    std::shared_ptr<MemoryKvStore> memStore_;
    std::shared_ptr<WatchRegistry> watchRegistry_;
    std::shared_ptr<WatchDispatcherImpl> watchDispatcher_;
    std::shared_ptr<SteadyClockReal> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::shared_ptr<CoordinatorStore> store_;
    std::unique_ptr<CoordinatorServiceImpl> coordinatorService_;
};
}  // namespace coordinator
}  // namespace datasystem
#endif  // DATASYSTEM_COORDINATOR_COORDINATOR_SERVER_H
