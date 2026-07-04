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
 * Description: Read-only topology diagnostics snapshot.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_DIAGNOSTICS_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_DIAGNOSTICS_H

#include "datasystem/topology/executor/topology_task_executor.h"
#include "datasystem/topology/runtime/coordination_event_pump.h"
#include "datasystem/topology/runtime/topology_readiness.h"
#include "datasystem/topology/runtime/topology_snapshot_rebuilder.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

struct TopologyDiagnosticsSnapshot {
    TopologyReadinessSnapshot readiness;
    CoordinationEventPumpStats eventPump;
    TopologySnapshotRebuilderStats rebuild;
    bool hasExecutorStats{ false };
    TopologyTaskExecutorStats executor;
};

class TopologyDiagnostics final {
public:
    TopologyDiagnostics(const CoordinationEventPump &eventPump, const TopologySnapshotRebuilder &rebuilder,
                        const TopologyReadiness &readiness, const TopologyTaskExecutor *executor = nullptr);
    ~TopologyDiagnostics() = default;
    TopologyDiagnostics(const TopologyDiagnostics &) = delete;
    TopologyDiagnostics &operator=(const TopologyDiagnostics &) = delete;
    TopologyDiagnostics(TopologyDiagnostics &&) = delete;
    TopologyDiagnostics &operator=(TopologyDiagnostics &&) = delete;

    /**
     * @brief Capture readiness and event pump counters for local diagnostics.
     * @param[out] snapshot Combined diagnostics snapshot.
     * @return K_OK when the diagnostics query itself succeeds.
     */
    Status GetSnapshot(TopologyDiagnosticsSnapshot &snapshot) const;

private:
    const CoordinationEventPump &eventPump_;
    const TopologySnapshotRebuilder &rebuilder_;
    const TopologyReadiness &readiness_;
    const TopologyTaskExecutor *executor_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_DIAGNOSTICS_H
