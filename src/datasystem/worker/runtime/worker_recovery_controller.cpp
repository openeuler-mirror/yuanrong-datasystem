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
#include "datasystem/worker/runtime/worker_recovery_controller.h"

#include <sstream>
#include <vector>

#include "datasystem/common/inject/inject_point.h"

namespace datasystem::worker {
namespace {
void AppendMissing(std::vector<std::string> &missing, bool ready, const std::string &name)
{
    if (!ready) {
        missing.push_back(name);
    }
}

std::string WithMissingEvidenceDetail(const WorkerRunningEvidence &evidence, const std::string &detail)
{
    std::vector<std::string> missing;
    AppendMissing(missing, evidence.membershipReady, "membership");
    AppendMissing(missing, evidence.topologyReady, "topology");
    AppendMissing(missing, evidence.metadataReady, "metadata");
    AppendMissing(missing, evidence.slotReady, "slot");
    AppendMissing(missing, evidence.ownershipReady, "ownership");
    AppendMissing(missing, evidence.resourceReady, "resource");
    if (missing.empty()) {
        return detail;
    }
    if (detail.find("missing=") != std::string::npos) {
        return detail;
    }
    std::ostringstream oss;
    if (!detail.empty()) {
        oss << detail << "; ";
    }
    oss << "missing=";
    for (size_t i = 0; i < missing.size(); ++i) {
        if (i != 0) {
            oss << ",";
        }
        oss << missing[i];
    }
    return oss.str();
}

}  // namespace

WorkerRecoveryController::WorkerRecoveryController(WorkerRuntimeStateManager &runtimeState)
    : runtimeState_(runtimeState)
{
}

bool WorkerRecoveryController::TryCompleteRecovery(const WorkerRunningEvidence &evidence, const std::string &detail)
{
    INJECT_POINT_NO_RETURN("WorkerRecoveryController.BeforeMarkRunning");
    const bool recovered = runtimeState_.TryMarkRunning(evidence, WithMissingEvidenceDetail(evidence, detail));
    INJECT_POINT_NO_RETURN("WorkerRecoveryController.AfterTryMarkRunning");
    return recovered;
}
}  // namespace datasystem::worker
