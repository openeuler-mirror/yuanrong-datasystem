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
#ifndef DATASYSTEM_TESTS_UT_TOPOLOGY_TESTING_TOPOLOGY_TEST_UTILS_H
#define DATASYSTEM_TESTS_UT_TOPOLOGY_TESTING_TOPOLOGY_TEST_UTILS_H

#include <cstdint>
#include <initializer_list>
#include <string>

#include "datasystem/topology/model/topology_types.h"
#include "tests/ut/topology/fake_coordination_backend.h"

namespace datasystem {
namespace topology {
namespace testing {

extern const char WORKER_A[];
extern const char WORKER_B[];
extern const char WORKER_C[];

TopologyNode MakeTopologyNodeForTest(const TopologyNodeId &nodeId, std::initializer_list<uint32_t> tokens,
                                     TopologyNodeState state = TopologyNodeState::ACTIVE);
TopologyDescriptor MakeTopologyForTest(std::initializer_list<TopologyNode> members, int64_t version);
Status SeedCommittedTopologyForTest(FakeCoordinationBackend &store, const TopologyDescriptor &topology);

}  // namespace testing
}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TESTS_UT_TOPOLOGY_TESTING_TOPOLOGY_TEST_UTILS_H
