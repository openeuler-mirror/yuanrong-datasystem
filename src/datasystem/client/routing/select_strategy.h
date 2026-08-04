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

#ifndef DATASYSTEM_CLIENT_ROUTING_SELECT_STRATEGY_H
#define DATASYSTEM_CLIENT_ROUTING_SELECT_STRATEGY_H

namespace datasystem {
namespace client {

// Legacy compatibility enum. New routing code must use DataPlacementPolicy so REQUIRED_SAME_NODE remains expressible.
enum class SelectStrategy {
    HASH_RING_AFFINITY,     // Select by key hash on ring (metadata owner)
    SAME_NODE_PREFERRED,    // Prefer same-node worker, fallback to ring
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_SELECT_STRATEGY_H
