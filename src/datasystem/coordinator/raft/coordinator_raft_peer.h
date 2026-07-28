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

/**
 * Description: Coordinator raft peer identity parsing and formatting.
 */
#ifndef DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_PEER_H
#define DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_PEER_H

#include <string>

#include <braft/configuration.h>

#include "datasystem/utils/status.h"

namespace datasystem::coordinator {

Status ParseCoordinatorRaftPeer(const std::string &address, braft::PeerId &peer);
std::string CoordinatorRaftPeerAddress(const braft::PeerId &peer);

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_PEER_H
