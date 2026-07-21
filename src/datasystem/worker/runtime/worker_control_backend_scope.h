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
 * Description: Worker control backend failure scope classifier.
 */
#ifndef DATASYSTEM_WORKER_WORKER_CONTROL_BACKEND_SCOPE_H
#define DATASYSTEM_WORKER_WORKER_CONTROL_BACKEND_SCOPE_H

#include <cstdint>
#include <vector>

#include "datasystem/cluster/runtime/control_backend_state.h"

namespace datasystem::worker {
enum class ControlBackendFailureScope : std::uint8_t {
    INCONCLUSIVE,
    LOCAL_ISOLATION,
    GLOBAL_OUTAGE,
};

bool ConfirmsGlobalBackendOutage(const cluster::ControlBackendObservation &local,
                                 const std::vector<cluster::MemberIdentity> &targets,
                                 const std::vector<cluster::ControlBackendObservation> &observations);

ControlBackendFailureScope ClassifyControlBackendFailureScope(
    const cluster::ControlBackendObservation &local, const std::vector<cluster::MemberIdentity> &targets,
    const std::vector<cluster::ControlBackendObservation> &observations);
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_CONTROL_BACKEND_SCOPE_H
