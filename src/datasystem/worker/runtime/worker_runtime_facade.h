/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade.
 */
#ifndef DATASYSTEM_WORKER_RUNTIME_WORKER_RUNTIME_FACADE_H
#define DATASYSTEM_WORKER_RUNTIME_WORKER_RUNTIME_FACADE_H

#include <optional>
#include <string>

#include "datasystem/worker/runtime/worker_admission_facade.h"

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
        explicit AdmissionGuard(WorkerRuntimeStateReadGuard guard);

    private:
        std::optional<WorkerRuntimeStateReadGuard> guard_;
    };

    WorkerRuntimeFacade();
    ~WorkerRuntimeFacade();
    WorkerRuntimeFacade(const WorkerRuntimeFacade &) = delete;
    WorkerRuntimeFacade &operator=(const WorkerRuntimeFacade &) = delete;

    WorkerRuntimeStateSnapshot GetSnapshot() const;
    void MarkStarting(std::string detail = {});
    void MarkJoining(std::string detail = {});
    bool TryMarkRunning(const WorkerRunningEvidence &evidence, std::string detail = {});
    void MarkDraining(std::string detail = {});
    void MarkLocalIsolated(WorkerIsolationReason reason, std::string detail = {});
    void MarkOutOfMemory(std::string detail = {});
    void MarkRecovering(WorkerIsolationReason reason, std::string detail = {},
                        WorkerRecoveryPhase phase = WorkerRecoveryPhase::TOPOLOGY);
    void MarkStopping(WorkerIsolationReason reason, std::string detail = {});

    Status CheckAdmission(WorkerAdmissionKind kind, const std::string &operation) const;
    Status AcquireAdmissionGuard(WorkerAdmissionKind kind, const std::string &operation, AdmissionGuard &guard) const;
    Status AcquireNormalReadGuard(const std::string &operation, AdmissionGuard &guard) const;

private:
    WorkerRuntimeState state_;
    WorkerAdmissionFacade admission_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_RUNTIME_WORKER_RUNTIME_FACADE_H
