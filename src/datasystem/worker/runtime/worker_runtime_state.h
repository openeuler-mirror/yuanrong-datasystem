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
 * Description: Worker runtime state manager.
 */
#ifndef DATASYSTEM_WORKER_WORKER_RUNTIME_STATE_H
#define DATASYSTEM_WORKER_WORKER_RUNTIME_STATE_H

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>

#include "datasystem/worker/runtime/worker_runtime_types.h"

namespace datasystem::worker {
class WorkerRuntimeStateManager;

class WorkerRuntimeStateTransitionGuard {
public:
    WorkerRuntimeStateTransitionGuard(std::atomic<uint32_t> &pendingTransitions, std::shared_mutex &admissionMutex);
    WorkerRuntimeStateTransitionGuard(const WorkerRuntimeStateTransitionGuard &) = delete;
    WorkerRuntimeStateTransitionGuard &operator=(const WorkerRuntimeStateTransitionGuard &) = delete;
    ~WorkerRuntimeStateTransitionGuard() = default;

private:
    class PendingTransitionIntent {
    public:
        explicit PendingTransitionIntent(std::atomic<uint32_t> &pendingTransitions);
        PendingTransitionIntent(const PendingTransitionIntent &) = delete;
        PendingTransitionIntent &operator=(const PendingTransitionIntent &) = delete;
        ~PendingTransitionIntent();

    private:
        std::atomic<uint32_t> &pendingTransitions_;
    };

    PendingTransitionIntent intent_;
    std::unique_lock<std::shared_mutex> lock_;
};

class WorkerRuntimeStateReadGuard {
public:
    WorkerRuntimeStateReadGuard(WorkerRuntimeStateReadGuard &&) = default;
    WorkerRuntimeStateReadGuard &operator=(WorkerRuntimeStateReadGuard &&) = default;
    ~WorkerRuntimeStateReadGuard() = default;

    const WorkerRuntimeStateSnapshot &GetSnapshot() const;

private:
    friend class WorkerRuntimeStateManager;
    WorkerRuntimeStateReadGuard(std::shared_lock<std::shared_mutex> lock, const WorkerRuntimeStateSnapshot &snapshot);

    std::shared_lock<std::shared_mutex> lock_;
    WorkerRuntimeStateSnapshot snapshot_;
};

class WorkerRuntimeStateManager {
public:
    WorkerRuntimeStateManager();
    ~WorkerRuntimeStateManager() = default;

    WorkerRuntimeStateManager(const WorkerRuntimeStateManager &) = delete;
    WorkerRuntimeStateManager &operator=(const WorkerRuntimeStateManager &) = delete;

    WorkerRuntimeStateSnapshot GetSnapshot() const;
    void PublishMetrics() const;
    bool IsTransitionPending() const;
    bool IsFastRunningForAdmission() const;
    std::optional<WorkerRuntimeStateReadGuard> TryAcquireReadGuard() const;

    void MarkStarting(std::string detail = {});
    void MarkJoining(std::string detail = {});
    bool TryMarkRunning(const WorkerRunningEvidence &evidence, std::string detail = {});
    void MarkDraining(std::string detail = {});
    void MarkLocalIsolated(WorkerIsolationReason reason, std::string detail = {});
    void MarkOutOfMemory(std::string detail = {});
    void MarkRecovering(WorkerIsolationReason reason, std::string detail = {},
                        WorkerRecoveryPhase phase = WorkerRecoveryPhase::TOPOLOGY);
    void MarkStopping(WorkerIsolationReason reason, std::string detail = {});

private:
    WorkerRuntimeStateTransitionGuard AcquireTransitionGuard();
    bool UpdateLocked(WorkerServiceMode mode, WorkerIsolationReason reason, WorkerRunningEvidence evidence,
                      WorkerRecoveryPhase phase, std::string detail);
    bool IsServingTransitionTerminalLocked() const;

    mutable std::shared_mutex admissionMutex_;
    mutable std::atomic<uint32_t> pendingTransitions_{ 0 };
    std::atomic<WorkerServiceMode> fastMode_{ WorkerServiceMode::STARTING };
    mutable std::mutex mutex_;
    WorkerRuntimeStateSnapshot snapshot_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_RUNTIME_STATE_H
