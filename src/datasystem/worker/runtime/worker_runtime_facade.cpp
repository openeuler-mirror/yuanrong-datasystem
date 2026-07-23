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

WorkerRecoveryEvidenceReport ControlDegradedEvidenceReport(const WorkerRecoveryEvidenceReport *recoveryReport)
{
    WorkerRecoveryEvidenceBuilder builder;
    builder.MarkMembershipReady("control backend degraded globally")
        .MarkTopologyReady("topology scope classified control backend as globally degraded")
        .MarkMetadataReady("object-cache metadata recovery is not required for global control backend degradation")
        .MarkSlotReady("slot recovery is not required for global control backend degradation")
        .MarkOwnershipReady("ownership reconciliation is not required for global control backend degradation");
    std::string detail = "control backend globally degraded";
    if (recoveryReport != nullptr && recoveryReport->evidence.resourceReady) {
        builder.MarkResourceReady(recoveryReport->detail);
        detail += "; " + recoveryReport->detail;
    } else {
        builder.MarkResourceReady("resource recovery is not required for global control backend degradation");
    }
    return builder.BuildReport(detail);
}
}  // namespace

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

bool WorkerRuntimeFacade::ApplyTopologyAvailability(cluster::TopologyAvailabilityLevel level,
                                                    const WorkerRecoveryEvidenceReport *recoveryReport)
{
    switch (level) {
        case cluster::TopologyAvailabilityLevel::NORMAL:
        case cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED: {
            const auto mode = GetSnapshot().mode;
            if (mode == WorkerServiceMode::LOCAL_ISOLATED) {
                MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION,
                               "topology available; validating recovery evidence", WorkerRecoveryPhase::TOPOLOGY);
            } else if (mode == WorkerServiceMode::OUT_OF_MEMORY) {
                MarkRecovering(WorkerIsolationReason::OUT_OF_MEMORY,
                               "topology available; validating resource recovery evidence",
                               WorkerRecoveryPhase::RESOURCE);
            } else if (mode == WorkerServiceMode::RUNNING
                       && (recoveryReport == nullptr || !IsComplete(recoveryReport->evidence))) {
                break;
            }
            const auto report = level == cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED
                                        && mode != WorkerServiceMode::OUT_OF_MEMORY
                                    ? ControlDegradedEvidenceReport(recoveryReport)
                                    : TopologyAvailableEvidenceReport(recoveryReport);
            (void)TryCompleteRecovery(report.evidence, report.detail);
            break;
        }
        case cluster::TopologyAvailabilityLevel::ROLE_ISOLATED:
            MarkLocalIsolated(WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN,
                              "topology availability is role-isolated");
            break;
        case cluster::TopologyAvailabilityLevel::NOT_READY:
            MarkJoining("topology availability is not ready");
            break;
        case cluster::TopologyAvailabilityLevel::SHUTTING_DOWN:
            MarkStopping(WorkerIsolationReason::PROCESS_STOPPING, "topology runtime is shutting down");
            break;
        default:
            MarkRecovering(WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, "unknown topology availability",
                           WorkerRecoveryPhase::TOPOLOGY);
            break;
    }
    const bool topologyCanServe = level == cluster::TopologyAvailabilityLevel::NORMAL
                                  || level == cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED;
    return topologyCanServe && IsServingMode(GetSnapshot().mode);
}

bool WorkerRuntimeFacade::ShouldRequestObjectCacheRecoveryEvidence(
    cluster::TopologyAvailabilityLevel level, const WorkerRecoveryEvidenceReport &recoveryReport) const
{
    if (level != cluster::TopologyAvailabilityLevel::NORMAL || IsComplete(recoveryReport.evidence)) {
        return false;
    }
    const auto runtimeState = GetSnapshot();
    if (runtimeState.mode == WorkerServiceMode::LOCAL_ISOLATED) {
        return true;
    }
    return runtimeState.mode == WorkerServiceMode::RECOVERING
           && runtimeState.reason == WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE
           && runtimeState.recoveryPhase != WorkerRecoveryPhase::RESOURCE;
}

void WorkerRuntimeFacade::PublishMetrics() const
{
    impl_->state.PublishMetrics();
}
}  // namespace datasystem::worker
