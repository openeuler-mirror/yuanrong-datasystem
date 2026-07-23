/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker service admission.
 */
#ifndef DATASYSTEM_WORKER_RUNTIME_WORKER_SERVICE_ADMISSION_H
#define DATASYSTEM_WORKER_RUNTIME_WORKER_SERVICE_ADMISSION_H

#include <string>

#include "datasystem/utils/status.h"
#include "datasystem/worker/runtime/worker_runtime_state.h"

namespace datasystem::worker {
class WorkerServiceAdmission {
public:
    explicit WorkerServiceAdmission(const WorkerRuntimeState &runtimeState);
    ~WorkerServiceAdmission() = default;

    WorkerServiceAdmission(const WorkerServiceAdmission &) = delete;
    WorkerServiceAdmission &operator=(const WorkerServiceAdmission &) = delete;

    Status Check(WorkerAdmissionKind kind, const std::string &operation) const;
    Status Check(const WorkerRuntimeStateSnapshot &snapshot, WorkerAdmissionKind kind,
                 const std::string &operation) const;
    Status CheckServing(const std::string &operation) const;
    bool IsServing() const;

private:
    const WorkerRuntimeState &runtimeState_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_RUNTIME_WORKER_SERVICE_ADMISSION_H
