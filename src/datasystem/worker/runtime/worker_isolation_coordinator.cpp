/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker local-isolation action coordinator.
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
    if (hooks_.setTopologyServingAdmission != nullptr) {
        hooks_.setTopologyServingAdmission(false);
    }
}

void WorkerIsolationCoordinator::OnLocalRecovery()
{
    runtime_.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                            "control backend keepalive renewal recovered; reopen after serving evidence",
                            WorkerRecoveryPhase::MEMBERSHIP);
    WorkerRunningEvidence evidence;
    evidence.membershipReady = true;
    evidence.topologyReady = true;
    evidence.resourceReady = true;
    const bool serving = runtime_.TryMarkRunning(evidence, "control backend keepalive renewal recovered");
    if (hooks_.setTopologyServingAdmission != nullptr) {
        hooks_.setTopologyServingAdmission(serving);
    }
}
}  // namespace datasystem::worker
