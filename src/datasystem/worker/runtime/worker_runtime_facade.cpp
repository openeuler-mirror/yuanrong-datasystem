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
WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard() = default;
WorkerRuntimeFacade::AdmissionGuard::~AdmissionGuard() = default;
WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard(AdmissionGuard &&) noexcept = default;
WorkerRuntimeFacade::AdmissionGuard &WorkerRuntimeFacade::AdmissionGuard::operator=(AdmissionGuard &&) noexcept =
    default;

WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard(WorkerRuntimeStateReadGuard guard) : guard_(std::move(guard))
{
}

WorkerRuntimeFacade::WorkerRuntimeFacade() : admission_(state_)
{
}
WorkerRuntimeFacade::~WorkerRuntimeFacade() = default;

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

bool WorkerRuntimeFacade::TryMarkRunning(const WorkerRunningEvidence &evidence, std::string detail)
{
    return state_.TryMarkRunning(evidence, std::move(detail));
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

Status WorkerRuntimeFacade::CheckAdmission(WorkerAdmissionKind kind, const std::string &operation) const
{
    return admission_.Check(kind, operation);
}

Status WorkerRuntimeFacade::AcquireAdmissionGuard(WorkerAdmissionKind kind, const std::string &operation,
                                                  AdmissionGuard &guard) const
{
    std::optional<WorkerRuntimeStateReadGuard> stateGuard;
    auto rc = admission_.AcquireGuard(kind, operation, stateGuard);
    if (rc.IsError()) {
        return rc;
    }
    if (stateGuard.has_value()) {
        guard = AdmissionGuard(std::move(*stateGuard));
    }
    return Status::OK();
}

Status WorkerRuntimeFacade::AcquireNormalReadGuard(const std::string &operation, AdmissionGuard &guard) const
{
    return AcquireAdmissionGuard(WorkerAdmissionKind::NORMAL_READ, operation, guard);
}
}  // namespace datasystem::worker
