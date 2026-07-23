/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker local-isolation action coordinator.
 */
#ifndef DATASYSTEM_WORKER_RUNTIME_WORKER_ISOLATION_COORDINATOR_H
#define DATASYSTEM_WORKER_RUNTIME_WORKER_ISOLATION_COORDINATOR_H

#include <functional>

#include "datasystem/worker/runtime/worker_runtime_facade.h"

namespace datasystem::worker {
struct WorkerIsolationCoordinatorHooks {
    std::function<void(bool)> setTopologyServingAdmission;
};

class WorkerIsolationCoordinator {
public:
    WorkerIsolationCoordinator(WorkerRuntimeFacade &runtime, WorkerIsolationCoordinatorHooks hooks);
    ~WorkerIsolationCoordinator() = default;

    WorkerIsolationCoordinator(const WorkerIsolationCoordinator &) = delete;
    WorkerIsolationCoordinator &operator=(const WorkerIsolationCoordinator &) = delete;

    void OnLocalIsolation(const Status &status);
    void OnLocalRecovery();

private:
    WorkerRuntimeFacade &runtime_;
    WorkerIsolationCoordinatorHooks hooks_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_RUNTIME_WORKER_ISOLATION_COORDINATOR_H
