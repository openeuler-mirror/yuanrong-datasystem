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

#ifndef DATASYSTEM_CLIENT_ROUTING_DATA_PLACEMENT_POLICY_H
#define DATASYSTEM_CLIENT_ROUTING_DATA_PLACEMENT_POLICY_H

#include <cstdint>

namespace datasystem {
namespace client {

enum class DataPlacementPolicy : uint8_t {
    PREFERRED_SAME_NODE,   // Prefer a same-node worker, then fall back to the metadata owner.
    REQUIRED_SAME_NODE,    // Select only from same-node workers.
    PREFERRED_META_OWNER,  // Prefer the metadata owner selected by the hash ring.
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_DATA_PLACEMENT_POLICY_H
