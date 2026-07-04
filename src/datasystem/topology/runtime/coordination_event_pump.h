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
 * Description: Lifecycle guard for topology coordination events.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_COORDINATION_EVENT_PUMP_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_COORDINATION_EVENT_PUMP_H

#include <cstdint>
#include <functional>
#include <mutex>

#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/topology/model/topology_types.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

struct CoordinationEventPumpStats {
    bool running{ false };
    uint64_t receivedEvents{ 0 };
    uint64_t appliedEvents{ 0 };
    uint64_t failedEvents{ 0 };
    uint64_t ignoredEvents{ 0 };
    uint64_t gapEvents{ 0 };
    uint64_t outOfOrderEvents{ 0 };
    Revision lastRevision{ 0 };
    Status lastStatus;
};

class CoordinationEventPump final {
public:
    using EventHandler = std::function<Status(CoordinationEvent &&event)>;
    using RebuildHandler = std::function<Status()>;

    CoordinationEventPump() = default;
    ~CoordinationEventPump() = default;
    CoordinationEventPump(const CoordinationEventPump &) = delete;
    CoordinationEventPump &operator=(const CoordinationEventPump &) = delete;
    CoordinationEventPump(CoordinationEventPump &&) = delete;
    CoordinationEventPump &operator=(CoordinationEventPump &&) = delete;

    /**
     * @brief Start accepting events and route them through one handler.
     * @param[in] handler Synchronous event handler copied before dispatch; it must not depend on caller stack lifetime.
     * @return K_OK on success; K_INVALID when handler is empty.
     */
    Status Start(EventHandler handler, RebuildHandler rebuildHandler = {});

    /**
     * @brief Stop accepting new events and detach the handler.
     */
    void Shutdown();

    /**
     * @brief Submit one backend notification event to the started handler.
     * @param[in] event Coordination event produced by a backend or test.
     * @return Handler status, or K_NOT_READY when the pump is stopped.
     */
    Status Submit(CoordinationEvent &&event);

    /**
     * @brief Return a copy of lifecycle and dispatch counters.
     */
    CoordinationEventPumpStats GetStats() const;

private:
    mutable std::mutex mutex_;
    bool running_{ false };
    EventHandler handler_;
    RebuildHandler rebuildHandler_;
    CoordinationEventPumpStats stats_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_COORDINATION_EVENT_PUMP_H
