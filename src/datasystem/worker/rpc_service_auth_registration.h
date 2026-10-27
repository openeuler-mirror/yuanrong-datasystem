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

#ifndef DATASYSTEM_WORKER_RPC_SERVICE_AUTH_REGISTRATION_H
#define DATASYSTEM_WORKER_RPC_SERVICE_AUTH_REGISTRATION_H

#include <memory>
#include <utility>
#include <vector>

#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/worker/authenticate.h"

namespace datasystem {
namespace worker {
inline Status RegisterRpcAuthService(RpcServer &server, google::protobuf::Service &native,
                                     const google::protobuf::ServiceDescriptor &authenticated,
                                     std::shared_ptr<AkSkManager> manager,
                                     std::vector<std::unique_ptr<BrpcAuthService>> &owners)
{
    BrpcAuthService::Authenticate authenticate;
    RETURN_IF_NOT_OK(CreateRpcAuthenticator(*native.GetDescriptor(), std::move(manager), authenticate));
    for (const auto *descriptor : { native.GetDescriptor(), &authenticated }) {
        std::unique_ptr<BrpcAuthService> entry;
        RETURN_IF_NOT_OK(BrpcAuthService::Create(native, *descriptor, authenticate, entry));
        auto *service = entry.get();
        // Registration can partially succeed; owners must survive until StopBrpcServer().
        owners.emplace_back(std::move(entry));
        RETURN_IF_NOT_OK(server.AddBrpcService(service));
    }
    return Status::OK();
}
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_RPC_SERVICE_AUTH_REGISTRATION_H
