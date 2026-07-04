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
 * Description: Readiness view for topology routing snapshots.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_READINESS_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_READINESS_H

#include <cstdint>

#include "datasystem/topology/routing/routing_view.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

enum class TopologyReadinessState { NOT_READY = 0, READY = 1 };

struct TopologyReadinessSnapshot {
    TopologyReadinessState state{ TopologyReadinessState::NOT_READY };
    bool ready{ false };
    int64_t routingVersion{ 0 };
    Status lastStatus;
};

class TopologyReadiness final {
public:
    explicit TopologyReadiness(const IRoutingView &routingView);
    ~TopologyReadiness() = default;
    TopologyReadiness(const TopologyReadiness &) = delete;
    TopologyReadiness &operator=(const TopologyReadiness &) = delete;
    TopologyReadiness(TopologyReadiness &&) = delete;
    TopologyReadiness &operator=(TopologyReadiness &&) = delete;

    /**
     * @brief Inspect whether a routing snapshot has been published.
     * @param[out] snapshot Readiness state and the observed routing version when ready.
     * @return K_OK when the readiness query itself succeeds.
     */
    Status GetSnapshot(TopologyReadinessSnapshot &snapshot) const;

private:
    const IRoutingView &routingView_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_READINESS_H
