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
 * Description: Degraded startup checker for topology snapshots.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_DEGRADED_STARTUP_CHECKER_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_DEGRADED_STARTUP_CHECKER_H

#include <vector>

#include "datasystem/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

class TopologyDegradedStartupChecker final {
public:
    TopologyDegradedStartupChecker() = default;
    ~TopologyDegradedStartupChecker() = default;

    /**
     * @brief Decide whether backend-unavailable startup may use local snapshot.
     * @param[in] localSnapshot Local snapshot loaded from durable storage.
     * @param[in] peerStates Peer states returned by live peers.
     * @param[out] decision Degraded startup decision and reason.
     * @return K_OK when the decision is computed.
     */
    Status Check(const LocalTopologySnapshot &localSnapshot, const std::vector<PeerStateSnapshot> &peerStates,
                 DegradedStartupDecision &decision) const;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_DEGRADED_STARTUP_CHECKER_H
