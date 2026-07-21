/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade.
 */
#ifndef DATASYSTEM_WORKER_WORKER_RUNTIME_FACADE_H
#define DATASYSTEM_WORKER_WORKER_RUNTIME_FACADE_H

#include <optional>
#include <string>

#include "datasystem/worker/runtime/worker_admission_facade.h"
#include "datasystem/worker/runtime/worker_recovery_controller.h"

namespace datasystem::worker {
class WorkerRuntimeFacade {
public:
    WorkerRuntimeFacade();
    ~WorkerRuntimeFacade() = default;

    WorkerRuntimeFacade(const WorkerRuntimeFacade &) = delete;
    WorkerRuntimeFacade &operator=(const WorkerRuntimeFacade &) = delete;

    WorkerRuntimeStateSnapshot GetSnapshot() const;
    WorkerRuntimeStateManager &RuntimeState();
    const WorkerRuntimeStateManager &RuntimeState() const;

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
    Status AcquireNormalReadGuard(const std::string &operation,
                                  std::optional<WorkerRuntimeStateReadGuard> &guard) const;

private:
    WorkerRuntimeStateManager state_;
    WorkerAdmissionFacade admission_;
    WorkerRecoveryController recovery_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_RUNTIME_FACADE_H
