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
#include "datasystem/worker/runtime/worker_recovery_evidence_tracker.h"

#include <utility>

namespace datasystem::worker {
WorkerRecoveryGeneration WorkerRecoveryEvidenceTracker::BeginRecovery(std::string detail)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ++generation_;
    report_ = WorkerRecoveryEvidenceReport{};
    report_.detail = std::move(detail);
    return generation_;
}

WorkerRecoveryGeneration WorkerRecoveryEvidenceTracker::CurrentGeneration() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return generation_;
}

void WorkerRecoveryEvidenceTracker::ResetEvidence(WorkerRecoveryGeneration generation, std::string detail)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsCurrentGenerationLocked(generation)) {
        return;
    }
    report_ = WorkerRecoveryEvidenceReport{};
    report_.detail = std::move(detail);
}

bool WorkerRecoveryEvidenceTracker::UpdateEvidence(WorkerRecoveryGeneration generation,
                                                   WorkerRecoveryEvidenceReport report)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsCurrentGenerationLocked(generation)) {
        return false;
    }
    report_ = std::move(report);
    return true;
}

std::optional<GenerationedWorkerRecoveryEvidenceReport> WorkerRecoveryEvidenceTracker::GetEvidence(
    WorkerRecoveryGeneration generation) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsCurrentGenerationLocked(generation)) {
        return std::nullopt;
    }
    return GenerationedWorkerRecoveryEvidenceReport{ generation_, report_ };
}

bool WorkerRecoveryEvidenceTracker::IsComplete(WorkerRecoveryGeneration generation) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return IsCurrentGenerationLocked(generation) && datasystem::worker::IsComplete(report_.evidence);
}

bool WorkerRecoveryEvidenceTracker::IsCurrentGenerationLocked(WorkerRecoveryGeneration generation) const
{
    return generation != 0 && generation == generation_;
}
}  // namespace datasystem::worker
