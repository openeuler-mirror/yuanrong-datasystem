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
 * Description: Generation-aware worker recovery evidence tracker.
 */
#ifndef DATASYSTEM_WORKER_WORKER_RECOVERY_EVIDENCE_TRACKER_H
#define DATASYSTEM_WORKER_WORKER_RECOVERY_EVIDENCE_TRACKER_H

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>

#include "datasystem/worker/runtime/worker_recovery_controller.h"

namespace datasystem::worker {
class WorkerRecoveryEvidenceTracker {
public:
    WorkerRecoveryEvidenceTracker() = default;
    ~WorkerRecoveryEvidenceTracker() = default;

    WorkerRecoveryEvidenceTracker(const WorkerRecoveryEvidenceTracker &) = delete;
    WorkerRecoveryEvidenceTracker &operator=(const WorkerRecoveryEvidenceTracker &) = delete;

    WorkerRecoveryGeneration BeginRecovery(std::string detail);
    WorkerRecoveryGeneration CurrentGeneration() const;
    void ResetEvidence(WorkerRecoveryGeneration generation, std::string detail);
    bool UpdateEvidence(WorkerRecoveryGeneration generation, WorkerRecoveryEvidenceReport report);
    std::optional<GenerationedWorkerRecoveryEvidenceReport> GetEvidence(WorkerRecoveryGeneration generation) const;
    bool IsComplete(WorkerRecoveryGeneration generation) const;

private:
    bool IsCurrentGenerationLocked(WorkerRecoveryGeneration generation) const;

    mutable std::mutex mutex_;
    WorkerRecoveryGeneration generation_{ 0 };
    WorkerRecoveryEvidenceReport report_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_RECOVERY_EVIDENCE_TRACKER_H
