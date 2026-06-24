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
 * Description: Topology repository key helper.
 */
#include "datasystem/common/topology/repository/topology_key_helper.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char RING_KEY[] = "/datasystem/ring";
constexpr char RING_KEY_WITH_SLASH[] = "/datasystem/ring/";

}  // namespace

std::string TopologyKeyHelper::CommittedTopologyKey()
{
    return RING_KEY;
}

Status TopologyKeyHelper::Parse(const std::string &key, TopologyKeyParts &parts)
{
    parts = {};
    if (key == RING_KEY || key == RING_KEY_WITH_SLASH) {
        parts.type = TopologyKeyType::COMMITTED_TOPOLOGY;
        return Status::OK();
    }
    parts.type = TopologyKeyType::UNRELATED;
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
