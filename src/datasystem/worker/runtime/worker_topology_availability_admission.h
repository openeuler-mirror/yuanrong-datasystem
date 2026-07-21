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
#ifndef DATASYSTEM_WORKER_WORKER_TOPOLOGY_AVAILABILITY_ADMISSION_H
#define DATASYSTEM_WORKER_WORKER_TOPOLOGY_AVAILABILITY_ADMISSION_H

#include "datasystem/cluster/runtime/topology_runtime_types.h"
#include "datasystem/worker/runtime/worker_recovery_controller.h"
#include "datasystem/worker/runtime/worker_runtime_facade.h"
#include "datasystem/worker/runtime/worker_runtime_state.h"

namespace datasystem::worker {
void ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel level,
                                             WorkerRuntimeStateManager &runtimeState,
                                             WorkerRecoveryController *recoveryController = nullptr,
                                             const WorkerRecoveryEvidenceReport *recoveryReport = nullptr);
bool RefreshTopologyAvailabilityAdmission(cluster::TopologyAvailabilityLevel level,
                                          WorkerRuntimeStateManager &runtimeState,
                                          WorkerRecoveryController &recoveryController,
                                          const WorkerRecoveryEvidenceReport &recoveryReport);
void ApplyTopologyAvailabilityToRuntimeState(cluster::TopologyAvailabilityLevel level, WorkerRuntimeFacade &runtime,
                                             const WorkerRecoveryEvidenceReport *recoveryReport = nullptr);
bool RefreshTopologyAvailabilityAdmission(cluster::TopologyAvailabilityLevel level, WorkerRuntimeFacade &runtime,
                                          const WorkerRecoveryEvidenceReport &recoveryReport);
bool ShouldOpenTopologyServingAdmission(cluster::TopologyAvailabilityLevel level,
                                        const WorkerRuntimeStateSnapshot &runtimeState);
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_TOPOLOGY_AVAILABILITY_ADMISSION_H
