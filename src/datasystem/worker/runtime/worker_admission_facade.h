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
 * Description: Worker admission facade.
 */
#ifndef DATASYSTEM_WORKER_WORKER_ADMISSION_FACADE_H
#define DATASYSTEM_WORKER_WORKER_ADMISSION_FACADE_H

#include <optional>
#include <string>

#include "datasystem/worker/runtime/worker_service_admission.h"
#include "datasystem/worker/runtime/worker_runtime_state.h"

namespace datasystem::worker {
class WorkerAdmissionFacade {
public:
    explicit WorkerAdmissionFacade(const WorkerRuntimeStateManager &runtimeState);
    ~WorkerAdmissionFacade() = default;

    WorkerAdmissionFacade(const WorkerAdmissionFacade &) = delete;
    WorkerAdmissionFacade &operator=(const WorkerAdmissionFacade &) = delete;

    Status CheckNormalRead(const std::string &operation) const;
    Status CheckNormalWrite(const std::string &operation) const;
    Status CheckMigrationTarget(const std::string &operation) const;
    Status CheckRecoveryRpc(const std::string &operation) const;
    Status Check(WorkerAdmissionKind kind, const std::string &operation) const;
    Status AcquireGuard(WorkerAdmissionKind kind, const std::string &operation,
                        std::optional<WorkerRuntimeStateReadGuard> &guard) const;
    Status AcquireNormalReadGuard(const std::string &operation,
                                  std::optional<WorkerRuntimeStateReadGuard> &guard) const;
    std::optional<WorkerRuntimeStateReadGuard> TryAcquireNormalGuard(const std::string &operation) const;

private:
    const WorkerRuntimeStateManager &runtimeState_;
    WorkerServiceAdmission admission_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_ADMISSION_FACADE_H
