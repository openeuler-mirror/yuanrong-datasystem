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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_RPC_DISPATCHER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_RPC_DISPATCHER_H

#include <functional>
#include <memory>
#include <string>

#include "datasystem/client/object_cache/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

enum class RpcExecutionState {
    NOT_SENT,
    MAY_HAVE_EXECUTED,
    RESPONSE_RECEIVED,
};

struct RpcAttempt {
    Status status;
    RpcExecutionState executionState{ RpcExecutionState::NOT_SENT };
};

class WorkerRpcDispatcher {
public:
    explicit WorkerRpcDispatcher(std::shared_ptr<DataPlaneManager> manager);
    virtual ~WorkerRpcDispatcher() = default;

    virtual RpcAttempt Invoke(const HostPort &workerAddr, const std::string &operationName,
                              const std::function<Status(WorkerRpcClient &)> &operation);

private:
    std::shared_ptr<DataPlaneManager> manager_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_WORKER_RPC_DISPATCHER_H
