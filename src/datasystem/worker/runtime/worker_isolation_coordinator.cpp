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
 * Description: Worker local isolation and recovery action coordinator.
 */
#include "datasystem/worker/runtime/worker_isolation_coordinator.h"

#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"

namespace datasystem::worker {
WorkerIsolationCoordinator::WorkerIsolationCoordinator(WorkerRuntimeFacade &runtime,
                                                       WorkerIsolationCoordinatorHooks hooks)
    : runtime_(runtime), hooks_(std::move(hooks))
{
}

void WorkerIsolationCoordinator::OnLocalIsolation(const Status &status)
{
    LOG(WARNING) << "Close worker business admission after confirmed local control-backend isolation: "
                 << status.ToString();
    runtime_.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, status.ToString());
    INJECT_POINT_NO_RETURN("WorkerIsolationCoordinator.AfterMarkLocalIsolated");
    INJECT_POINT_NO_RETURN("WorkerOCServer.AfterMarkLocalIsolated");
    SetTopologyServingAdmission(false);
    if (hooks_.reconcileLocalIsolationOwnership != nullptr) {
        LOG_IF_ERROR(hooks_.reconcileLocalIsolationOwnership(),
                     "Schedule metadata ownership handoff after local isolation");
    }
}

void WorkerIsolationCoordinator::OnLocalRecovery()
{
    runtime_.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE,
                            "control backend keepalive renewal recovered; rebuilding membership",
                            WorkerRecoveryPhase::MEMBERSHIP);
    SetTopologyServingAdmission(false);
    if (hooks_.isTopologyRuntimeReady != nullptr && !hooks_.isTopologyRuntimeReady()) {
        LOG(WARNING) << "Control backend keepalive recovered before topology runtime was ready";
        return;
    }
    if (hooks_.publishRecoveringMembership != nullptr) {
        LOG_IF_ERROR(hooks_.publishRecoveringMembership(),
                     "Publish RECOVERING membership after control backend recovery");
    }
    if (hooks_.reconcileNetworkRecoveryOwnership != nullptr) {
        LOG_IF_ERROR(hooks_.reconcileNetworkRecoveryOwnership(),
                     "Schedule metadata ownership recovery after local recovery");
    }
    if (hooks_.requestRecoveryReconciliation == nullptr) {
        LOG(WARNING) << "Recovery reconciliation handler is not configured";
        return;
    }
    LOG_IF_ERROR(hooks_.requestRecoveryReconciliation([this] {
        runtime_.MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE,
                                "control backend keepalive renewal recovered; reconciling topology",
                                WorkerRecoveryPhase::TOPOLOGY);
        SetTopologyServingAdmission(false);
    }),
                 "Queue authoritative topology reconciliation after control backend recovery");
}

void WorkerIsolationCoordinator::SetTopologyServingAdmission(bool open) const
{
    if (hooks_.setTopologyServingAdmission != nullptr) {
        hooks_.setTopologyServingAdmission(open);
    }
}
}  // namespace datasystem::worker
