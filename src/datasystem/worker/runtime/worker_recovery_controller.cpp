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
#include <utility>
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

void AppendReady(std::vector<std::string> &ready, bool value, const std::string &name)
{
    if (value) {
        ready.push_back(name);
    }
}

std::string Join(const std::vector<std::string> &items)
{
    std::ostringstream oss;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i != 0) {
            oss << ",";
        }
        oss << items[i];
    }
    return oss.str();
}

void AppendDetail(std::ostringstream &oss, bool &hasPrevious, const std::string &key, const std::string &value)
{
    if (value.empty()) {
        return;
    }
    if (hasPrevious) {
        oss << "; ";
    }
    oss << key << "=" << value;
    hasPrevious = true;
}

void RedactFieldValue(std::string &detail, const std::string &field)
{
    size_t pos = 0;
    while ((pos = detail.find(field, pos)) != std::string::npos) {
        const auto valueBegin = pos + field.size();
        auto valueEnd = detail.find_first_of(" ,;", valueBegin);
        if (valueEnd == std::string::npos) {
            valueEnd = detail.size();
        }
        detail.replace(valueBegin, valueEnd - valueBegin, "<redacted>");
        pos = valueBegin + std::string("<redacted>").size();
    }
}

std::string SanitizeEvidenceDetail(std::string detail)
{
    RedactFieldValue(detail, "objectKey=");
    RedactFieldValue(detail, "object_key=");
    return detail;
}
}  // namespace

WorkerRecoveryEvidenceBuilder &WorkerRecoveryEvidenceBuilder::MarkMembershipReady(std::string detail)
{
    return SetReady(MEMBERSHIP, std::move(detail));
}

WorkerRecoveryEvidenceBuilder &WorkerRecoveryEvidenceBuilder::MarkTopologyReady(std::string detail)
{
    return SetReady(TOPOLOGY, std::move(detail));
}

WorkerRecoveryEvidenceBuilder &WorkerRecoveryEvidenceBuilder::MarkMetadataReady(std::string detail)
{
    return SetReady(METADATA, std::move(detail));
}

WorkerRecoveryEvidenceBuilder &WorkerRecoveryEvidenceBuilder::MarkSlotReady(std::string detail)
{
    return SetReady(SLOT, std::move(detail));
}

WorkerRecoveryEvidenceBuilder &WorkerRecoveryEvidenceBuilder::MarkOwnershipReady(std::string detail)
{
    return SetReady(OWNERSHIP, std::move(detail));
}

WorkerRecoveryEvidenceBuilder &WorkerRecoveryEvidenceBuilder::MarkResourceReady(std::string detail)
{
    return SetReady(RESOURCE, std::move(detail));
}

WorkerRecoveryEvidenceReport WorkerRecoveryEvidenceBuilder::BuildReport(const std::string &detail) const
{
    std::vector<std::string> ready;
    std::vector<std::string> missing;
    AppendReady(ready, evidence_.membershipReady, "membership");
    AppendReady(ready, evidence_.topologyReady, "topology");
    AppendReady(ready, evidence_.metadataReady, "metadata");
    AppendReady(ready, evidence_.slotReady, "slot");
    AppendReady(ready, evidence_.ownershipReady, "ownership");
    AppendReady(ready, evidence_.resourceReady, "resource");
    AppendMissing(missing, evidence_.membershipReady, "membership");
    AppendMissing(missing, evidence_.topologyReady, "topology");
    AppendMissing(missing, evidence_.metadataReady, "metadata");
    AppendMissing(missing, evidence_.slotReady, "slot");
    AppendMissing(missing, evidence_.ownershipReady, "ownership");
    AppendMissing(missing, evidence_.resourceReady, "resource");

    bool hasPrevious = false;
    std::ostringstream oss;
    if (!detail.empty()) {
        oss << SanitizeEvidenceDetail(detail);
        hasPrevious = true;
    }
    AppendDetail(oss, hasPrevious, "ready", Join(ready));
    AppendDetail(oss, hasPrevious, "missing", Join(missing));
    AppendDetail(oss, hasPrevious, "membership", SanitizeEvidenceDetail(details_[MEMBERSHIP]));
    AppendDetail(oss, hasPrevious, "topology", SanitizeEvidenceDetail(details_[TOPOLOGY]));
    AppendDetail(oss, hasPrevious, "metadata", SanitizeEvidenceDetail(details_[METADATA]));
    AppendDetail(oss, hasPrevious, "slot", SanitizeEvidenceDetail(details_[SLOT]));
    AppendDetail(oss, hasPrevious, "ownership", SanitizeEvidenceDetail(details_[OWNERSHIP]));
    AppendDetail(oss, hasPrevious, "resource", SanitizeEvidenceDetail(details_[RESOURCE]));
    return WorkerRecoveryEvidenceReport{ evidence_, oss.str() };
}

WorkerRecoveryEvidenceBuilder &WorkerRecoveryEvidenceBuilder::SetReady(EvidenceIndex index, std::string detail)
{
    switch (index) {
        case MEMBERSHIP:
            evidence_.membershipReady = true;
            break;
        case TOPOLOGY:
            evidence_.topologyReady = true;
            break;
        case METADATA:
            evidence_.metadataReady = true;
            break;
        case SLOT:
            evidence_.slotReady = true;
            break;
        case OWNERSHIP:
            evidence_.ownershipReady = true;
            break;
        case RESOURCE:
            evidence_.resourceReady = true;
            break;
        case EVIDENCE_COUNT:
            break;
    }
    details_[index] = std::move(detail);
    return *this;
}

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
