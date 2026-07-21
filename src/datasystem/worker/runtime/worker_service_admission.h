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
 * Description: Worker service admission.
 */
#ifndef DATASYSTEM_WORKER_WORKER_SERVICE_ADMISSION_H
#define DATASYSTEM_WORKER_WORKER_SERVICE_ADMISSION_H

#include <cstdint>
#include <string>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/runtime/worker_runtime_state.h"

namespace datasystem::worker {
enum class WorkerAdmissionKind : std::uint8_t {
    NORMAL_READ = 0,
    NORMAL_WRITE,
    MIGRATION_TARGET,
    RECOVERY_RPC,
    CLEANUP_RPC,
    RESOURCE_RECOVERY_RPC,
    INTERNAL_JOINING_RPC,
    DIAGNOSTIC_RPC,
};

const char *ToString(WorkerAdmissionKind kind);

class WorkerServiceAdmission {
public:
    explicit WorkerServiceAdmission(const WorkerRuntimeStateManager &runtimeState);
    ~WorkerServiceAdmission() = default;

    WorkerServiceAdmission(const WorkerServiceAdmission &) = delete;
    WorkerServiceAdmission &operator=(const WorkerServiceAdmission &) = delete;

    Status Check(WorkerAdmissionKind kind, const std::string &operation) const;
    Status Check(const WorkerRuntimeStateSnapshot &snapshot, WorkerAdmissionKind kind,
                 const std::string &operation) const;
    Status CheckServing(const std::string &operation) const;
    bool IsServing() const;

private:
    const WorkerRuntimeStateManager &runtimeState_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_SERVICE_ADMISSION_H
