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
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_EVIDENCE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_EVIDENCE_H

#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/protos/slot_recovery.pb.h"
#include "datasystem/worker/object_cache/metadata_recovery_manager.h"
#include "datasystem/worker/runtime/worker_recovery_evidence.h"

namespace datasystem {
namespace object_cache {
worker::WorkerRecoveryEvidenceReport BuildMetadataRecoveryEvidenceReport(
    const MetaDataRecoveryManager::RecoverySummary &summary);

worker::WorkerRecoveryEvidenceReport BuildSlotRecoveryEvidenceReport(const std::vector<SlotRecoveryInfoPb> &incidents);

worker::WorkerRecoveryEvidenceReport BuildOwnershipRecoveryEvidenceReport(bool ownershipReady,
                                                                          const std::string &detail);

worker::WorkerRecoveryEvidenceReport BuildObjectCacheRecoveryEvidenceReport(
    const worker::WorkerRecoveryEvidenceReport &metadataReport, const worker::WorkerRecoveryEvidenceReport &slotReport,
    const worker::WorkerRecoveryEvidenceReport &ownershipReport, bool resourceReady);
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_RECOVERY_OBJECT_CACHE_RECOVERY_EVIDENCE_H
