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
#include "datasystem/worker/runtime/worker_runtime_state.h"

#include <thread>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"

namespace datasystem::worker {
namespace {
constexpr uint32_t MEMBERSHIP_READY_BIT = 0U;
constexpr uint32_t TOPOLOGY_READY_BIT = 1U;
constexpr uint32_t METADATA_READY_BIT = 2U;
constexpr uint32_t SLOT_READY_BIT = 3U;
constexpr uint32_t OWNERSHIP_READY_BIT = 4U;
constexpr uint32_t RESOURCE_READY_BIT = 5U;

void PublishSnapshotMetrics(const WorkerRuntimeStateSnapshot &snapshot)
{
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_SERVICE_MODE))
        .Set(static_cast<int64_t>(snapshot.mode) + 1);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_SERVICE_REASON))
        .Set(static_cast<int64_t>(snapshot.reason) + 1);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RECOVERY_PHASE))
        .Set(static_cast<int64_t>(snapshot.recoveryPhase) + 1);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_RECOVERY_EVIDENCE_MASK))
        .Set(static_cast<int64_t>(RecoveryEvidenceMask(snapshot.evidence)) + 1);
}
}  // namespace

WorkerRuntimeStateTransitionGuard::PendingTransitionIntent::PendingTransitionIntent(
    std::atomic<uint32_t> &pendingTransitions)
    : pendingTransitions_(pendingTransitions)
{
    pendingTransitions_.fetch_add(1, std::memory_order_acq_rel);
}

WorkerRuntimeStateTransitionGuard::PendingTransitionIntent::~PendingTransitionIntent()
{
    pendingTransitions_.fetch_sub(1, std::memory_order_acq_rel);
}

WorkerRuntimeStateTransitionGuard::WorkerRuntimeStateTransitionGuard(std::atomic<uint32_t> &pendingTransitions,
                                                                     std::shared_mutex &admissionMutex)
    : intent_(pendingTransitions), lock_(admissionMutex)
{
}

WorkerRuntimeStateReadGuard::WorkerRuntimeStateReadGuard(std::shared_lock<std::shared_mutex> lock,
                                                         const WorkerRuntimeStateSnapshot &snapshot)
    : lock_(std::move(lock)), snapshot_(snapshot)
{
}

const WorkerRuntimeStateSnapshot &WorkerRuntimeStateReadGuard::GetSnapshot() const
{
    return snapshot_;
}

const char *ToString(WorkerServiceMode mode)
{
    switch (mode) {
        case WorkerServiceMode::STARTING:
            return "STARTING";
        case WorkerServiceMode::JOINING:
            return "JOINING";
        case WorkerServiceMode::RUNNING:
            return "RUNNING";
        case WorkerServiceMode::DRAINING:
            return "DRAINING";
        case WorkerServiceMode::LOCAL_ISOLATED:
            return "LOCAL_ISOLATED";
        case WorkerServiceMode::OUT_OF_MEMORY:
            return "OUT_OF_MEMORY";
        case WorkerServiceMode::RECOVERING:
            return "RECOVERING";
        case WorkerServiceMode::STOPPING:
            return "STOPPING";
        default:
            return "UNKNOWN";
    }
}

const char *ToString(WorkerIsolationReason reason)
{
    switch (reason) {
        case WorkerIsolationReason::NONE:
            return "NONE";
        case WorkerIsolationReason::STARTUP_NOT_READY:
            return "STARTUP_NOT_READY";
        case WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE:
            return "RECOVERY_EVIDENCE_INCOMPLETE";
        case WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION:
            return "CONTROL_BACKEND_LOCAL_ISOLATION";
        case WorkerIsolationReason::CONTROL_BACKEND_GLOBAL_OUTAGE:
            return "CONTROL_BACKEND_GLOBAL_OUTAGE";
        case WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN:
            return "TOPOLOGY_PASSIVE_SCALE_DOWN";
        case WorkerIsolationReason::OUT_OF_MEMORY:
            return "OUT_OF_MEMORY";
        case WorkerIsolationReason::PROCESS_STOPPING:
            return "PROCESS_STOPPING";
        default:
            return "UNKNOWN";
    }
}

const char *ToString(WorkerRecoveryPhase phase)
{
    switch (phase) {
        case WorkerRecoveryPhase::NONE:
            return "NONE";
        case WorkerRecoveryPhase::MEMBERSHIP:
            return "MEMBERSHIP";
        case WorkerRecoveryPhase::TOPOLOGY:
            return "TOPOLOGY";
        case WorkerRecoveryPhase::METADATA:
            return "METADATA";
        case WorkerRecoveryPhase::SLOT:
            return "SLOT";
        case WorkerRecoveryPhase::OWNERSHIP:
            return "OWNERSHIP";
        case WorkerRecoveryPhase::RESOURCE:
            return "RESOURCE";
        case WorkerRecoveryPhase::COMPLETE:
            return "COMPLETE";
        default:
            return "UNKNOWN";
    }
}

bool IsComplete(const WorkerRunningEvidence &evidence)
{
    return evidence.membershipReady && evidence.topologyReady && evidence.metadataReady && evidence.slotReady
           && evidence.ownershipReady && evidence.resourceReady;
}

WorkerRecoveryPhase FirstMissingRecoveryPhase(const WorkerRunningEvidence &evidence)
{
    if (!evidence.membershipReady) {
        return WorkerRecoveryPhase::MEMBERSHIP;
    }
    if (!evidence.topologyReady) {
        return WorkerRecoveryPhase::TOPOLOGY;
    }
    if (!evidence.metadataReady) {
        return WorkerRecoveryPhase::METADATA;
    }
    if (!evidence.slotReady) {
        return WorkerRecoveryPhase::SLOT;
    }
    if (!evidence.ownershipReady) {
        return WorkerRecoveryPhase::OWNERSHIP;
    }
    if (!evidence.resourceReady) {
        return WorkerRecoveryPhase::RESOURCE;
    }
    return WorkerRecoveryPhase::COMPLETE;
}

uint32_t RecoveryEvidenceMask(const WorkerRunningEvidence &evidence)
{
    return (static_cast<uint32_t>(evidence.membershipReady) << MEMBERSHIP_READY_BIT)
           | (static_cast<uint32_t>(evidence.topologyReady) << TOPOLOGY_READY_BIT)
           | (static_cast<uint32_t>(evidence.metadataReady) << METADATA_READY_BIT)
           | (static_cast<uint32_t>(evidence.slotReady) << SLOT_READY_BIT)
           | (static_cast<uint32_t>(evidence.ownershipReady) << OWNERSHIP_READY_BIT)
           | (static_cast<uint32_t>(evidence.resourceReady) << RESOURCE_READY_BIT);
}

bool IsServingMode(WorkerServiceMode mode)
{
    return mode == WorkerServiceMode::RUNNING;
}

WorkerRuntimeStateManager::WorkerRuntimeStateManager()
{
    snapshot_.changedAt = std::chrono::steady_clock::now();
}

WorkerRuntimeStateSnapshot WorkerRuntimeStateManager::GetSnapshot() const
{
    INJECT_POINT_NO_RETURN("WorkerRuntimeState.GetSnapshot");
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_;
}

void WorkerRuntimeStateManager::PublishMetrics() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    INJECT_POINT_NO_RETURN("WorkerRuntimeState.PublishMetrics.BeforePublish",
                           [](int delayMs) { std::this_thread::sleep_for(std::chrono::milliseconds(delayMs)); });
    PublishSnapshotMetrics(snapshot_);
}

bool WorkerRuntimeStateManager::IsTransitionPending() const
{
    return pendingTransitions_.load(std::memory_order_acquire) != 0;
}

bool WorkerRuntimeStateManager::IsFastRunningForAdmission() const
{
    if (IsTransitionPending()) {
        return false;
    }
    const bool running = fastMode_.load(std::memory_order_acquire) == WorkerServiceMode::RUNNING;
    return running && !IsTransitionPending();
}

std::optional<WorkerRuntimeStateReadGuard> WorkerRuntimeStateManager::TryAcquireReadGuard() const
{
    if (IsTransitionPending()) {
        return std::nullopt;
    }
    std::shared_lock<std::shared_mutex> admissionLock(admissionMutex_);
    if (IsTransitionPending()) {
        return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    return WorkerRuntimeStateReadGuard(std::move(admissionLock), snapshot_);
}

WorkerRuntimeStateTransitionGuard WorkerRuntimeStateManager::AcquireTransitionGuard()
{
    return WorkerRuntimeStateTransitionGuard(pendingTransitions_, admissionMutex_);
}

void WorkerRuntimeStateManager::MarkStarting(std::string detail)
{
    auto admissionLock = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsServingTransitionTerminalLocked()) {
        return;
    }
    (void)UpdateLocked(WorkerServiceMode::STARTING, WorkerIsolationReason::STARTUP_NOT_READY, {},
                       WorkerRecoveryPhase::NONE, std::move(detail));
}

void WorkerRuntimeStateManager::MarkJoining(std::string detail)
{
    auto admissionLock = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsServingTransitionTerminalLocked()) {
        return;
    }
    (void)UpdateLocked(WorkerServiceMode::JOINING, WorkerIsolationReason::STARTUP_NOT_READY, {},
                       WorkerRecoveryPhase::NONE, std::move(detail));
}

bool WorkerRuntimeStateManager::TryMarkRunning(const WorkerRunningEvidence &evidence, std::string detail)
{
    bool running = false;
    auto admissionLock = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsServingTransitionTerminalLocked()) {
        return false;
    }
    if (snapshot_.mode == WorkerServiceMode::LOCAL_ISOLATED || snapshot_.mode == WorkerServiceMode::OUT_OF_MEMORY) {
        return false;
    }
    if (!IsComplete(evidence)) {
        if (snapshot_.mode == WorkerServiceMode::RUNNING) {
            return false;
        }
        (void)UpdateLocked(WorkerServiceMode::RECOVERING, WorkerIsolationReason::RECOVERY_EVIDENCE_INCOMPLETE, evidence,
                           FirstMissingRecoveryPhase(evidence), std::move(detail));
    } else {
        (void)UpdateLocked(WorkerServiceMode::RUNNING, WorkerIsolationReason::NONE, evidence,
                           WorkerRecoveryPhase::COMPLETE, std::move(detail));
        running = true;
    }
    return running;
}

void WorkerRuntimeStateManager::MarkDraining(std::string detail)
{
    auto admissionLock = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsServingTransitionTerminalLocked()) {
        return;
    }
    (void)UpdateLocked(WorkerServiceMode::DRAINING, WorkerIsolationReason::NONE, snapshot_.evidence,
                       WorkerRecoveryPhase::NONE, std::move(detail));
}

void WorkerRuntimeStateManager::MarkLocalIsolated(WorkerIsolationReason reason, std::string detail)
{
    auto admissionLock = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsServingTransitionTerminalLocked()) {
        return;
    }
    (void)UpdateLocked(WorkerServiceMode::LOCAL_ISOLATED, reason, {}, WorkerRecoveryPhase::NONE, std::move(detail));
}

void WorkerRuntimeStateManager::MarkOutOfMemory(std::string detail)
{
    auto admissionLock = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsServingTransitionTerminalLocked()) {
        return;
    }
    auto evidence = snapshot_.evidence;
    evidence.resourceReady = false;
    (void)UpdateLocked(WorkerServiceMode::OUT_OF_MEMORY, WorkerIsolationReason::OUT_OF_MEMORY, evidence,
                       WorkerRecoveryPhase::RESOURCE, std::move(detail));
}

void WorkerRuntimeStateManager::MarkRecovering(WorkerIsolationReason reason, std::string detail,
                                               WorkerRecoveryPhase phase)
{
    auto admissionLock = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsServingTransitionTerminalLocked()) {
        return;
    }
    (void)UpdateLocked(WorkerServiceMode::RECOVERING, reason, snapshot_.evidence, phase, std::move(detail));
}

void WorkerRuntimeStateManager::MarkStopping(WorkerIsolationReason reason, std::string detail)
{
    auto admissionLock = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    (void)UpdateLocked(WorkerServiceMode::STOPPING, reason, snapshot_.evidence, WorkerRecoveryPhase::NONE,
                       std::move(detail));
}

bool WorkerRuntimeStateManager::UpdateLocked(WorkerServiceMode mode, WorkerIsolationReason reason,
                                             WorkerRunningEvidence evidence, WorkerRecoveryPhase phase,
                                             std::string detail)
{
    const bool modeChanged = snapshot_.mode != mode;
    const auto now = std::chrono::steady_clock::now();
    snapshot_.mode = mode;
    snapshot_.reason = reason;
    snapshot_.recoveryPhase = phase;
    snapshot_.evidence = evidence;
    snapshot_.detail = std::move(detail);
    snapshot_.changedAt = now;
    fastMode_.store(mode, std::memory_order_release);
    PublishSnapshotMetrics(snapshot_);
    return modeChanged;
}

bool WorkerRuntimeStateManager::IsServingTransitionTerminalLocked() const
{
    return snapshot_.mode == WorkerServiceMode::DRAINING || snapshot_.mode == WorkerServiceMode::STOPPING;
}
}  // namespace datasystem::worker
