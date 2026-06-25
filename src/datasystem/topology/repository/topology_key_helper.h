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
#ifndef DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_KEY_HELPER_H
#define DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_KEY_HELPER_H

#include <string>

#include "datasystem/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

enum class TopologyKeyType {
    COMMITTED_TOPOLOGY,
    UNRELATED,
};

struct TopologyKeyParts {
    TopologyKeyType type{ TopologyKeyType::UNRELATED };
};

class TopologyKeyHelper {
public:
    TopologyKeyHelper() = delete;
    ~TopologyKeyHelper() = delete;

    /**
     * @brief Build the exact committed-topology key.
     * @return The exact `/datasystem/ring` key.
     */
    static std::string CommittedTopologyKey();

    /**
     * @brief Parse one backend key into topology key parts.
     * @param[in] key Backend key from Get/List/Watch.
     * @param[out] parts Parsed key type.
     * @return K_OK for known and unrelated keys.
     */
    static Status Parse(const std::string &key, TopologyKeyParts &parts);
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_TOPOLOGY_REPOSITORY_TOPOLOGY_KEY_HELPER_H
