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
namespace {
WorkerRecoveryEvidenceReport TopologyAvailableEvidenceReport(const WorkerRecoveryEvidenceReport *recoveryReport)
{
    WorkerRecoveryEvidenceBuilder builder;
    builder.MarkMembershipReady("topology runtime reported membership available")
        .MarkTopologyReady("topology runtime reported topology available");
    std::string detail = "topology available";
    if (recoveryReport != nullptr) {
        if (recoveryReport->evidence.metadataReady) {
            builder.MarkMetadataReady(recoveryReport->detail);
        }
        if (recoveryReport->evidence.slotReady) {
            builder.MarkSlotReady(recoveryReport->detail);
        }
        if (recoveryReport->evidence.ownershipReady) {
            builder.MarkOwnershipReady(recoveryReport->detail);
        }
        if (recoveryReport->evidence.resourceReady) {
            builder.MarkResourceReady(recoveryReport->detail);
        }
        detail += "; " + recoveryReport->detail;
    } else {
        detail += "; waiting for recovery evidence";
    }
    return builder.BuildReport(detail);
}
}  // namespace

void ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel level,
                                             WorkerRuntimeStateManager &runtimeState,
                                             WorkerRecoveryController *recoveryController,
                                             const WorkerRecoveryEvidenceReport *recoveryReport)
{
    switch (level) {
        case cluster::TopologyAvailabilityLevel::NORMAL:
        case cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED:
            if (recoveryController != nullptr) {
                const auto mode = runtimeState.GetSnapshot().mode;
                if (mode == WorkerServiceMode::LOCAL_ISOLATED) {
                    runtimeState.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                                                "topology available; validating recovery evidence",
                                                WorkerRecoveryPhase::TOPOLOGY);
                } else if (mode == WorkerServiceMode::OUT_OF_MEMORY) {
                    runtimeState.MarkRecovering(WorkerIsolationReason::OUT_OF_MEMORY,
                                                "topology available; validating resource recovery evidence",
                                                WorkerRecoveryPhase::RESOURCE);
                } else if (mode == WorkerServiceMode::RUNNING
                           && (recoveryReport == nullptr || !IsComplete(recoveryReport->evidence))) {
                    break;
                }
                const auto report = TopologyAvailableEvidenceReport(recoveryReport);
                (void)recoveryController->TryCompleteRecovery(report.evidence, report.detail);
            } else {
                runtimeState.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE,
                                            "topology available; waiting for recovery evidence",
                                            WorkerRecoveryPhase::METADATA);
            }
            break;
        case cluster::TopologyAvailabilityLevel::ROLE_ISOLATED:
            runtimeState.MarkLocalIsolated(WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN,
                                           "topology availability is role-isolated");
            break;
        case cluster::TopologyAvailabilityLevel::NOT_READY:
            runtimeState.MarkJoining("topology availability is not ready");
            break;
        case cluster::TopologyAvailabilityLevel::SHUTTING_DOWN:
            runtimeState.MarkStopping(WorkerIsolationReason::PROCESS_STOPPING, "topology runtime is shutting down");
            break;
        default:
            runtimeState.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE,
                                        "unknown topology availability", WorkerRecoveryPhase::TOPOLOGY);
            break;
    }
}

bool RefreshTopologyAvailabilityAdmission(cluster::TopologyAvailabilityLevel level,
                                          WorkerRuntimeStateManager &runtimeState,
                                          WorkerRecoveryController &recoveryController,
                                          const WorkerRecoveryEvidenceReport &recoveryReport)
{
    ApplyTopologyAvailabilityToRuntimeState(level, runtimeState, &recoveryController, &recoveryReport);
    return ShouldOpenTopologyServingAdmission(level, runtimeState.GetSnapshot());
}

void ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel level, WorkerRuntimeFacade &runtime,
                                             const WorkerRecoveryEvidenceReport *recoveryReport)
{
    switch (level) {
        case cluster::TopologyAvailabilityLevel::NORMAL:
        case cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED: {
            const auto mode = runtime.GetSnapshot().mode;
            if (mode == WorkerServiceMode::LOCAL_ISOLATED) {
                runtime.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                                       "topology available; validating recovery evidence",
                                       WorkerRecoveryPhase::TOPOLOGY);
            } else if (mode == WorkerServiceMode::OUT_OF_MEMORY) {
                runtime.MarkRecovering(WorkerIsolationReason::OUT_OF_MEMORY,
                                       "topology available; validating resource recovery evidence",
                                       WorkerRecoveryPhase::RESOURCE);
            } else if (mode == WorkerServiceMode::RUNNING
                       && (recoveryReport == nullptr || !IsComplete(recoveryReport->evidence))) {
                break;
            }
            const auto report = TopologyAvailableEvidenceReport(recoveryReport);
            (void)runtime.TryCompleteRecovery(report.evidence, report.detail);
            break;
        }
        case cluster::TopologyAvailabilityLevel::ROLE_ISOLATED:
            runtime.MarkLocalIsolated(WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN,
                                      "topology availability is role-isolated");
            break;
        case cluster::TopologyAvailabilityLevel::NOT_READY:
            runtime.MarkJoining("topology availability is not ready");
            break;
        case cluster::TopologyAvailabilityLevel::SHUTTING_DOWN:
            runtime.MarkStopping(WorkerIsolationReason::PROCESS_STOPPING, "topology runtime is shutting down");
            break;
        default:
            runtime.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "unknown topology availability",
                                   WorkerRecoveryPhase::TOPOLOGY);
            break;
    }
}

bool RefreshTopologyAvailabilityAdmission(cluster::TopologyAvailabilityLevel level, WorkerRuntimeFacade &runtime,
                                          const WorkerRecoveryEvidenceReport &recoveryReport)
{
    ApplyTopologyAvailabilityToRuntimeState(level, runtime, &recoveryReport);
    return ShouldOpenTopologyServingAdmission(level, runtime.GetSnapshot());
}

bool ShouldOpenTopologyServingAdmission(cluster::TopologyAvailabilityLevel level,
                                        const WorkerRuntimeStateSnapshot &runtimeState)
{
    const bool topologyCanServe = level == cluster::TopologyAvailabilityLevel::NORMAL
                                  || level == cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED;
    return topologyCanServe && IsServingMode(runtimeState.mode);
}
}  // namespace datasystem::worker
