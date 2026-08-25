// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <brpc/server.h>

#include "datasystem/coordinator/raft/coordinator_raft_service.h"

#include <braft/raft.h>

#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/coordinator/raft/coordinator_raft_peer.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"

namespace datasystem::coordinator {

Status RegisterCoordinatorRaftServices(RpcServer &rpcServer, const std::string &localAddress)
{
    braft::PeerId localPeer;
    RETURN_IF_NOT_OK(ParseCoordinatorRaftPeer(localAddress, localPeer));
    const auto endpoint = localPeer.addr;
    const auto registrationStatus = rpcServer.AddBrpcServices([endpoint, localAddress](brpc::Server &server) {
        const int rc = braft::add_service(&server, endpoint);
        if (rc != 0) {
            return Status(K_RUNTIME_ERROR,
                          FormatString("Failed to register braft services for group %s, local peer %s, rc=%d",
                                       kCoordinatorRaftGroupId, localAddress, rc));
        }
        return Status::OK();
    });
    if (registrationStatus.IsError()) {
        return Status(
            registrationStatus.GetCode(),
            FormatString("Failed to register braft services for group %s, local peer %s; underlying status: %s; "
                         "must discard and recreate the shared brpc server generation",
                         kCoordinatorRaftGroupId, localAddress, registrationStatus.ToString()));
    }
    return Status::OK();
}

}  // namespace datasystem::coordinator
