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
#include "datasystem/topology/runtime/topology_diagnostics.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

TopologyDiagnostics::TopologyDiagnostics(const CoordinationEventPump &eventPump,
                                         const TopologySnapshotRebuilder &rebuilder,
                                         const TopologyReadiness &readiness,
                                         const TopologyTaskExecutor *executor)
    : eventPump_(eventPump), rebuilder_(rebuilder), readiness_(readiness), executor_(executor)
{
}

Status TopologyDiagnostics::GetSnapshot(TopologyDiagnosticsSnapshot &snapshot) const
{
    snapshot = {};
    RETURN_IF_NOT_OK(readiness_.GetSnapshot(snapshot.readiness));
    snapshot.eventPump = eventPump_.GetStats();
    snapshot.rebuild = rebuilder_.GetStats();
    if (executor_ != nullptr) {
        snapshot.hasExecutorStats = true;
        snapshot.executor = executor_->GetStats();
    }
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
