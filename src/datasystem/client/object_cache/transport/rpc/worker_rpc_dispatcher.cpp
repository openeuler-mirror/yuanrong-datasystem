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

#include "datasystem/client/object_cache/transport/rpc/worker_rpc_dispatcher.h"

#include <utility>

#include "datasystem/client/object_cache/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

WorkerRpcDispatcher::WorkerRpcDispatcher(std::shared_ptr<DataPlaneManager> manager) : manager_(std::move(manager))
{
}

RpcAttempt WorkerRpcDispatcher::Invoke(const HostPort &workerAddr, const std::string &operationName,
                                       const std::function<Status(WorkerRpcClient &)> &operation)
{
    if (manager_ == nullptr) {
        return { Status(K_RUNTIME_ERROR, "Worker RPC endpoint manager is unavailable"), RpcExecutionState::NOT_SENT };
    }
    std::shared_ptr<WorkerRpcClient> rpcClient;
    Status rc = manager_->GetOrCreateRpcClient(workerAddr, rpcClient);
    if (rc.IsError()) {
        return { std::move(rc), RpcExecutionState::NOT_SENT };
    }
    if (rpcClient == nullptr) {
        return { Status(K_RUNTIME_ERROR, "Worker RPC client is unavailable"), RpcExecutionState::NOT_SENT };
    }
    rc = operation(*rpcClient);
    RpcExecutionState executionState =
        rc.IsOk() ? RpcExecutionState::RESPONSE_RECEIVED : RpcExecutionState::MAY_HAVE_EXECUTED;
    if (IsRetryableRpcError(rc) || IsNonRetryableRpcError(rc)) {
        LOG(WARNING) << "Tear down RPC client for worker " << workerAddr.ToString() << " after " << operationName
                     << " failed: " << rc;
        manager_->Teardown(workerAddr);
    }
    return { std::move(rc), executionState };
}

}  // namespace client
}  // namespace datasystem
