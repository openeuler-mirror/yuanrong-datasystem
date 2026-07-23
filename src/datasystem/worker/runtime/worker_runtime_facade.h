/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade.
 */
#ifndef DATASYSTEM_WORKER_WORKER_RUNTIME_FACADE_H
#define DATASYSTEM_WORKER_WORKER_RUNTIME_FACADE_H

#include <memory>
#include <string>

#include "datasystem/cluster/runtime/topology_runtime_types.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/runtime/worker_recovery_evidence.h"
#include "datasystem/worker/runtime/worker_runtime_types.h"

namespace datasystem::worker {
class WorkerRuntimeFacade {
public:
    class AdmissionGuard {
    public:
        AdmissionGuard();
        ~AdmissionGuard();

        AdmissionGuard(AdmissionGuard &&) noexcept;
        AdmissionGuard &operator=(AdmissionGuard &&) noexcept;

        AdmissionGuard(const AdmissionGuard &) = delete;
        AdmissionGuard &operator=(const AdmissionGuard &) = delete;

    private:
        friend class WorkerRuntimeFacade;
        class Impl;
        explicit AdmissionGuard(std::unique_ptr<Impl> impl);

        std::unique_ptr<Impl> impl_;
    };

    WorkerRuntimeFacade();
    ~WorkerRuntimeFacade();

    WorkerRuntimeFacade(const WorkerRuntimeFacade &) = delete;
    WorkerRuntimeFacade &operator=(const WorkerRuntimeFacade &) = delete;

    WorkerRuntimeStateSnapshot GetSnapshot() const;

    void MarkStarting(std::string detail = {});
    void MarkJoining(std::string detail = {});
    void MarkDraining(std::string detail = {});
    void MarkLocalIsolated(WorkerIsolationReason reason, std::string detail = {});
    void MarkOutOfMemory(std::string detail = {});
    void MarkRecovering(WorkerIsolationReason reason, std::string detail = {},
                        WorkerRecoveryPhase phase = WorkerRecoveryPhase::TOPOLOGY);
    void MarkStopping(WorkerIsolationReason reason, std::string detail = {});

    bool TryCompleteRecovery(const WorkerRunningEvidence &evidence, const std::string &detail);
    Status CheckAdmission(WorkerAdmissionKind kind, const std::string &operation) const;
    Status AcquireAdmissionGuard(WorkerAdmissionKind kind, const std::string &operation, AdmissionGuard &guard) const;
    Status AcquireNormalReadGuard(const std::string &operation, AdmissionGuard &guard) const;
    bool ApplyTopologyAvailability(cluster::TopologyAvailabilityLevel level,
                                   const WorkerRecoveryEvidenceReport *recoveryReport = nullptr);
    bool ShouldRequestObjectCacheRecoveryEvidence(cluster::TopologyAvailabilityLevel level,
                                                  const WorkerRecoveryEvidenceReport &recoveryReport) const;
    void PublishMetrics() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_RUNTIME_FACADE_H
