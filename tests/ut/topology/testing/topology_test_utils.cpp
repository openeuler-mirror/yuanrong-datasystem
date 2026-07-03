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
 * Description: Shared topology test helpers.
 */
#include "tests/ut/topology/testing/topology_test_utils.h"

#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/repository/topology_repository_codec.h"

namespace datasystem {
namespace topology {
namespace testing {

const char WORKER_A[] = "127.0.0.1:7001";
const char WORKER_B[] = "127.0.0.1:7002";
const char WORKER_C[] = "127.0.0.1:7003";

TopologyNode MakeTopologyNodeForTest(const TopologyNodeId &nodeId, std::initializer_list<uint32_t> tokens,
                                     TopologyNodeState state)
{
    TopologyNode node;
    node.nodeId = nodeId;
    node.state = state;
    node.tokens.assign(tokens.begin(), tokens.end());
    return node;
}

TopologyDescriptor MakeTopologyForTest(std::initializer_list<TopologyNode> members, int64_t version)
{
    TopologyDescriptor topology;
    topology.version = version;
    topology.clusterHasInit = true;
    topology.members.assign(members.begin(), members.end());
    return topology;
}

Status SeedCommittedTopologyForTest(FakeCoordinationBackend &store, const TopologyDescriptor &topology)
{
    TopologyRepositoryCodec codec;
    std::string bytes;
    RETURN_IF_NOT_OK(codec.EncodeTopology(topology, bytes));
    return store.PutForTest(COORDINATION_HASHRING_TABLE, "", bytes);
}

}  // namespace testing
}  // namespace topology
}  // namespace datasystem
