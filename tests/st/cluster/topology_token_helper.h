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

#ifndef DATASYSTEM_TESTS_ST_CLUSTER_TOPOLOGY_TOKEN_HELPER_H
#define DATASYSTEM_TESTS_ST_CLUSTER_TOPOLOGY_TOKEN_HELPER_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/common/util/hash_ring_token.h"
#include "datasystem/protos/cluster_topology.pb.h"

namespace datasystem::st {

inline std::vector<uint32_t> RebuildTopologyMemberTokens(const ClusterTopologyPb &topology,
                                                         const std::string &address, const MembershipPb &member)
{
    std::vector<uint32_t> seeds(topology.tokens_per_member());
    for (const auto &seedOverride : member.token_seed_overrides()) {
        seeds.at(seedOverride.token_index()) = seedOverride.token_seed();
    }
    std::vector<uint32_t> tokens;
    MakeHashRingTokens(address, seeds, tokens);
    return tokens;
}

}  // namespace datasystem::st

#endif  // DATASYSTEM_TESTS_ST_CLUSTER_TOPOLOGY_TOKEN_HELPER_H
