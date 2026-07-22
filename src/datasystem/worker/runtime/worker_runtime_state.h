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
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>

namespace datasystem::worker {
enum class WorkerServiceMode : std::uint8_t {
    STARTING = 0,
    JOINING,
    RUNNING,
    DRAINING,
    LOCAL_ISOLATED,
    OUT_OF_MEMORY,
    RECOVERING,
    STOPPING,
};

enum class WorkerIsolationReason : std::uint8_t {
    NONE = 0,
    STARTUP_NOT_READY,
    RECOVERY_EVIDENCE_INCOMPLETE,
    CONTROL_BACKEND_LOCAL_ISOLATION,
    CONTROL_BACKEND_GLOBAL_OUTAGE,
    TOPOLOGY_PASSIVE_SCALE_DOWN,
    OUT_OF_MEMORY,
    PROCESS_STOPPING,
};

enum class WorkerRecoveryPhase : std::uint8_t {
    NONE = 0,
    MEMBERSHIP,
    TOPOLOGY,
    METADATA,
    SLOT,
    OWNERSHIP,
    RESOURCE,
    COMPLETE,
};

struct WorkerRunningEvidence {
    bool membershipReady{ false };
    bool topologyReady{ false };
    bool metadataReady{ false };
    bool slotReady{ false };
    bool ownershipReady{ false };
    bool resourceReady{ false };
};

struct WorkerRuntimeStateSnapshot {
    WorkerServiceMode mode{ WorkerServiceMode::STARTING };
    WorkerIsolationReason reason{ WorkerIsolationReason::STARTUP_NOT_READY };
    WorkerRecoveryPhase recoveryPhase{ WorkerRecoveryPhase::NONE };
    WorkerRunningEvidence evidence;
    std::string detail;
    std::chrono::steady_clock::time_point changedAt;
};

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

const char *ToString(WorkerServiceMode mode);
const char *ToString(WorkerIsolationReason reason);
const char *ToString(WorkerRecoveryPhase phase);
bool IsComplete(const WorkerRunningEvidence &evidence);
WorkerRecoveryPhase FirstMissingRecoveryPhase(const WorkerRunningEvidence &evidence);
uint32_t RecoveryEvidenceMask(const WorkerRunningEvidence &evidence);
bool IsServingMode(WorkerServiceMode mode);

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
