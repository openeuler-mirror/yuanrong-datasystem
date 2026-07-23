/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker admission facade.
 */
#ifndef DATASYSTEM_WORKER_RUNTIME_WORKER_ADMISSION_FACADE_H
#define DATASYSTEM_WORKER_RUNTIME_WORKER_ADMISSION_FACADE_H

#include <optional>
#include <string>

#include "datasystem/worker/runtime/worker_service_admission.h"

namespace datasystem::worker {
class WorkerAdmissionFacade {
public:
    explicit WorkerAdmissionFacade(const WorkerRuntimeState &runtimeState);
    ~WorkerAdmissionFacade() = default;

    Status Check(WorkerAdmissionKind kind, const std::string &operation) const;
    Status AcquireGuard(WorkerAdmissionKind kind, const std::string &operation,
                        std::optional<WorkerRuntimeStateReadGuard> &guard) const;

private:
    const WorkerRuntimeState &runtimeState_;
    WorkerServiceAdmission admission_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_RUNTIME_WORKER_ADMISSION_FACADE_H
