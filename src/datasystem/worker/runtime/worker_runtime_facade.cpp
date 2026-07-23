/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade.
 */
#include "datasystem/worker/runtime/worker_runtime_facade.h"

#include <optional>
#include <utility>

#include "datasystem/worker/runtime/worker_admission_facade.h"
#include "datasystem/worker/runtime/worker_recovery_controller.h"
#include "datasystem/worker/runtime/worker_runtime_state.h"

namespace datasystem::worker {
class WorkerRuntimeFacade::AdmissionGuard::Impl {
public:
    explicit Impl(WorkerRuntimeStateReadGuard guard) : guard_(std::move(guard))
    {
    }

private:
    WorkerRuntimeStateReadGuard guard_;
};

WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard() = default;
WorkerRuntimeFacade::AdmissionGuard::~AdmissionGuard() = default;
WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard(AdmissionGuard &&) noexcept = default;
WorkerRuntimeFacade::AdmissionGuard &WorkerRuntimeFacade::AdmissionGuard::operator=(AdmissionGuard &&) noexcept =
    default;

WorkerRuntimeFacade::AdmissionGuard::AdmissionGuard(std::unique_ptr<Impl> impl) : impl_(std::move(impl))
{
}

class WorkerRuntimeFacade::Impl {
public:
    Impl() : admission(state), recovery(state)
    {
    }

    // Keep state before members that store references to it; C++ destroys members in reverse declaration order.
    WorkerRuntimeStateManager state;
    WorkerAdmissionFacade admission;
    WorkerRecoveryController recovery;
};

WorkerRuntimeFacade::WorkerRuntimeFacade() : impl_(std::make_unique<Impl>())
{
}

WorkerRuntimeFacade::~WorkerRuntimeFacade() = default;

WorkerRuntimeStateSnapshot WorkerRuntimeFacade::GetSnapshot() const
{
    return impl_->state.GetSnapshot();
}

void WorkerRuntimeFacade::MarkStarting(std::string detail)
{
    impl_->state.MarkStarting(std::move(detail));
}

void WorkerRuntimeFacade::MarkJoining(std::string detail)
{
    impl_->state.MarkJoining(std::move(detail));
}

void WorkerRuntimeFacade::MarkDraining(std::string detail)
{
    impl_->state.MarkDraining(std::move(detail));
}

void WorkerRuntimeFacade::MarkLocalIsolated(WorkerIsolationReason reason, std::string detail)
{
    impl_->state.MarkLocalIsolated(reason, std::move(detail));
}

void WorkerRuntimeFacade::MarkOutOfMemory(std::string detail)
{
    impl_->state.MarkOutOfMemory(std::move(detail));
}

void WorkerRuntimeFacade::MarkRecovering(WorkerIsolationReason reason, std::string detail, WorkerRecoveryPhase phase)
{
    impl_->state.MarkRecovering(reason, std::move(detail), phase);
}

void WorkerRuntimeFacade::MarkStopping(WorkerIsolationReason reason, std::string detail)
{
    impl_->state.MarkStopping(reason, std::move(detail));
}

bool WorkerRuntimeFacade::TryCompleteRecovery(const WorkerRunningEvidence &evidence, const std::string &detail)
{
    return impl_->recovery.TryCompleteRecovery(evidence, detail);
}

Status WorkerRuntimeFacade::CheckAdmission(WorkerAdmissionKind kind, const std::string &operation) const
{
    return impl_->admission.Check(kind, operation);
}

Status WorkerRuntimeFacade::AcquireAdmissionGuard(WorkerAdmissionKind kind, const std::string &operation,
                                                  AdmissionGuard &guard) const
{
    std::optional<WorkerRuntimeStateReadGuard> stateGuard;
    RETURN_IF_NOT_OK(impl_->admission.AcquireGuard(kind, operation, stateGuard));
    CHECK_FAIL_RETURN_STATUS(stateGuard.has_value(), K_RUNTIME_ERROR, "runtime admission guard missing");
    guard = AdmissionGuard(std::make_unique<AdmissionGuard::Impl>(std::move(*stateGuard)));
    return Status::OK();
}

Status WorkerRuntimeFacade::AcquireNormalReadGuard(const std::string &operation, AdmissionGuard &guard) const
{
    return AcquireAdmissionGuard(WorkerAdmissionKind::NORMAL_READ, operation, guard);
}

void WorkerRuntimeFacade::PublishMetrics() const
{
    impl_->state.PublishMetrics();
}
}  // namespace datasystem::worker
