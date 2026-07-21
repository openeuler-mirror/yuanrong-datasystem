/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade.
 */
#include "datasystem/worker/runtime/worker_runtime_facade.h"

#include <utility>

namespace datasystem::worker {
WorkerRuntimeFacade::WorkerRuntimeFacade() : admission_(state_), recovery_(state_)
{
}

WorkerRuntimeStateSnapshot WorkerRuntimeFacade::GetSnapshot() const
{
    return state_.GetSnapshot();
}

void WorkerRuntimeFacade::MarkStarting(std::string detail)
{
    state_.MarkStarting(std::move(detail));
}

void WorkerRuntimeFacade::MarkJoining(std::string detail)
{
    state_.MarkJoining(std::move(detail));
}

void WorkerRuntimeFacade::MarkDraining(std::string detail)
{
    state_.MarkDraining(std::move(detail));
}

void WorkerRuntimeFacade::MarkLocalIsolated(WorkerIsolationReason reason, std::string detail)
{
    state_.MarkLocalIsolated(reason, std::move(detail));
}

void WorkerRuntimeFacade::MarkOutOfMemory(std::string detail)
{
    state_.MarkOutOfMemory(std::move(detail));
}

void WorkerRuntimeFacade::MarkRecovering(WorkerIsolationReason reason, std::string detail, WorkerRecoveryPhase phase)
{
    state_.MarkRecovering(reason, std::move(detail), phase);
}

void WorkerRuntimeFacade::MarkStopping(WorkerIsolationReason reason, std::string detail)
{
    state_.MarkStopping(reason, std::move(detail));
}

bool WorkerRuntimeFacade::TryCompleteRecovery(const WorkerRunningEvidence &evidence, const std::string &detail)
{
    return recovery_.TryCompleteRecovery(evidence, detail);
}

Status WorkerRuntimeFacade::CheckAdmission(WorkerAdmissionKind kind, const std::string &operation) const
{
    return admission_.Check(kind, operation);
}

Status WorkerRuntimeFacade::AcquireNormalReadGuard(const std::string &operation,
                                                   std::optional<WorkerRuntimeStateReadGuard> &guard) const
{
    return admission_.AcquireNormalReadGuard(operation, guard);
}

void WorkerRuntimeFacade::PublishMetrics() const
{
    state_.PublishMetrics();
}
}  // namespace datasystem::worker
