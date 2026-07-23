/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Maps topology availability to local worker runtime state.
 */
#include "datasystem/worker/runtime/worker_topology_availability_admission.h"

namespace datasystem::worker {
void ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel level, WorkerRuntimeFacade &runtime,
                                             const WorkerRecoveryEvidenceReport *recoveryReport)
{
    (void)runtime.ApplyTopologyAvailability(level, recoveryReport);
}

bool RefreshTopologyAvailabilityAdmission(cluster::TopologyAvailabilityLevel level, WorkerRuntimeFacade &runtime,
                                          const WorkerRecoveryEvidenceReport &recoveryReport)
{
    return runtime.ApplyTopologyAvailability(level, &recoveryReport);
}

bool ShouldOpenTopologyServingAdmission(cluster::TopologyAvailabilityLevel level,
                                        const WorkerRuntimeStateSnapshot &runtimeState)
{
    const bool topologyCanServe = level == cluster::TopologyAvailabilityLevel::NORMAL
                                  || level == cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED;
    return topologyCanServe && IsServingMode(runtimeState.mode);
}

bool ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel level,
                                              const WorkerRuntimeStateSnapshot &runtimeState,
                                              const WorkerRecoveryEvidenceReport &recoveryReport)
{
    if (level != cluster::TopologyAvailabilityLevel::NORMAL || IsComplete(recoveryReport.evidence)) {
        return false;
    }
    if (runtimeState.mode == WorkerServiceMode::LOCAL_ISOLATED) {
        return true;
    }
    return runtimeState.mode == WorkerServiceMode::RECOVERING
           && runtimeState.reason == WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE
           && runtimeState.recoveryPhase != WorkerRecoveryPhase::RESOURCE;
}
}  // namespace datasystem::worker
