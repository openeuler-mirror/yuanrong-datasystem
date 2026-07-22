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
 * Description: Worker recovery evidence controller.
 */
#ifndef DATASYSTEM_WORKER_WORKER_RECOVERY_CONTROLLER_H
#define DATASYSTEM_WORKER_WORKER_RECOVERY_CONTROLLER_H

#include <string>

#include "datasystem/worker/runtime/worker_recovery_evidence.h"
#include "datasystem/worker/runtime/worker_runtime_state.h"

namespace datasystem::worker {
class WorkerRecoveryController {
public:
    explicit WorkerRecoveryController(WorkerRuntimeStateManager &runtimeState);
    ~WorkerRecoveryController() = default;

    WorkerRecoveryController(const WorkerRecoveryController &) = delete;
    WorkerRecoveryController &operator=(const WorkerRecoveryController &) = delete;

    bool TryCompleteRecovery(const WorkerRunningEvidence &evidence, const std::string &detail);

private:
    WorkerRuntimeStateManager &runtimeState_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_RECOVERY_CONTROLLER_H
