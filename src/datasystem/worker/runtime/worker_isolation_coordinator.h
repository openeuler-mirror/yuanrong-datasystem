/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker local isolation and recovery action coordinator.
 */
#ifndef DATASYSTEM_WORKER_WORKER_ISOLATION_COORDINATOR_H
#define DATASYSTEM_WORKER_WORKER_ISOLATION_COORDINATOR_H

#include <functional>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/runtime/worker_runtime_facade.h"

namespace datasystem::worker {
struct WorkerIsolationCoordinatorHooks {
    std::function<void(bool)> setTopologyServingAdmission;
    std::function<Status()> reconcileLocalIsolationOwnership;
    std::function<bool()> isTopologyRuntimeReady;
    std::function<Status()> publishRecoveringMembership;
    std::function<Status()> reconcileNetworkRecoveryOwnership;
    std::function<Status(const std::function<void()> &)> requestRecoveryReconciliation;
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
    void SetTopologyServingAdmission(bool open) const;

    WorkerRuntimeFacade &runtime_;
    WorkerIsolationCoordinatorHooks hooks_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_ISOLATION_COORDINATOR_H
