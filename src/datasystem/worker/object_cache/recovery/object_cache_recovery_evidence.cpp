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
 * Description: Object cache recovery evidence builders.
 */
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_evidence.h"

#include <sstream>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
namespace {
std::string MetadataSummaryDetail(const MetaDataRecoveryManager::RecoverySummary &summary)
{
    std::ostringstream oss;
    oss << "metadata_recovered=" << summary.recoveredCount << "/" << summary.requestedCount;
    oss << "; metadata_failed=" << summary.failedIds.size();
    if (summary.status.IsError()) {
        oss << "; metadata_status=" << summary.status.GetMsg();
    }
    return oss.str();
}

std::string SlotSummaryDetail(size_t readyCount, size_t totalCount, size_t failedCount)
{
    std::ostringstream oss;
    oss << "slot_incidents_ready=" << readyCount << "/" << totalCount;
    oss << "; slot_incidents_failed=" << failedCount;
    return oss.str();
}

bool IsSlotIncidentFullyTerminal(const SlotRecoveryInfoPb &incident)
{
    return incident.total_slots() != 0
           && incident.completed_slots() + incident.failed_slots() == incident.total_slots();
}
}  // namespace

worker::WorkerRecoveryEvidenceReport BuildMetadataRecoveryEvidenceReport(
    const MetaDataRecoveryManager::RecoverySummary &summary)
{
    worker::WorkerRecoveryEvidenceBuilder builder;
    const bool recoveredAll =
        summary.status.IsOk() && summary.failedIds.empty() && summary.recoveredCount == summary.requestedCount;
    if (recoveredAll) {
        builder.MarkMetadataReady(MetadataSummaryDetail(summary));
    }
    return builder.BuildReport(MetadataSummaryDetail(summary));
}

worker::WorkerRecoveryEvidenceReport BuildSlotRecoveryEvidenceReport(const std::vector<SlotRecoveryInfoPb> &incidents)
{
    size_t readyCount = 0;
    size_t failedCount = 0;
    for (const auto &incident : incidents) {
        if (IsSlotIncidentFullyTerminal(incident) && incident.failed_slots() == 0) {
            ++readyCount;
        }
        if (incident.failed_slots() != 0) {
            ++failedCount;
        }
    }

    worker::WorkerRecoveryEvidenceBuilder builder;
    if (readyCount == incidents.size() && failedCount == 0) {
        builder.MarkSlotReady(SlotSummaryDetail(readyCount, incidents.size(), failedCount));
    }
    return builder.BuildReport(SlotSummaryDetail(readyCount, incidents.size(), failedCount));
}

worker::WorkerRecoveryEvidenceReport BuildOwnershipRecoveryEvidenceReport(bool ownershipReady,
                                                                          const std::string &detail)
{
    std::ostringstream oss;
    oss << "ownership_ready=" << (ownershipReady ? "true" : "false");
    if (!detail.empty()) {
        oss << "; " << detail;
    }

    worker::WorkerRecoveryEvidenceBuilder builder;
    if (ownershipReady) {
        builder.MarkOwnershipReady(oss.str());
    }
    return builder.BuildReport(oss.str());
}

worker::WorkerRecoveryEvidenceReport BuildObjectCacheRecoveryEvidenceReport(
    const worker::WorkerRecoveryEvidenceReport &metadataReport, const worker::WorkerRecoveryEvidenceReport &slotReport,
    const worker::WorkerRecoveryEvidenceReport &ownershipReport, bool resourceReady)
{
    worker::WorkerRecoveryEvidenceBuilder builder;
    if (metadataReport.evidence.metadataReady) {
        builder.MarkMetadataReady(metadataReport.detail);
    }
    if (slotReport.evidence.slotReady) {
        builder.MarkSlotReady(slotReport.detail);
    }
    if (metadataReport.evidence.metadataReady && slotReport.evidence.slotReady
        && ownershipReport.evidence.ownershipReady) {
        builder.MarkOwnershipReady(ownershipReport.detail);
    }
    if (resourceReady) {
        builder.MarkResourceReady("allocator resources are below their recovery low watermarks");
    }
    return builder.BuildReport(ownershipReport.detail);
}
}  // namespace object_cache
}  // namespace datasystem
