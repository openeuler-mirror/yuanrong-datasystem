/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime state manager.
 */
#include "datasystem/worker/runtime/worker_runtime_state.h"

#include <utility>

namespace datasystem::worker {
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
        case WorkerIsolationReason::RUNTIME_READY_INCOMPLETE:
            return "RUNTIME_READY_INCOMPLETE";
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
        case WorkerRecoveryPhase::RESOURCE:
            return "RESOURCE";
        case WorkerRecoveryPhase::COMPLETE:
            return "COMPLETE";
        default:
            return "UNKNOWN";
    }
}

bool IsServingReady(const WorkerRunningEvidence &evidence)
{
    return evidence.membershipReady && evidence.topologyReady && evidence.resourceReady;
}

bool IsServingMode(WorkerServiceMode mode)
{
    return mode == WorkerServiceMode::RUNNING;
}

WorkerRuntimeState::WorkerRuntimeState()
{
    snapshot_.changedAt = std::chrono::steady_clock::now();
}

WorkerRuntimeStateSnapshot WorkerRuntimeState::GetSnapshot() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_;
}

bool WorkerRuntimeState::IsTransitionPending() const
{
    return pendingTransitions_.load(std::memory_order_acquire) != 0;
}

bool WorkerRuntimeState::IsFastRunningForAdmission() const
{
    if (IsTransitionPending()) {
        return false;
    }
    return fastMode_.load(std::memory_order_acquire) == WorkerServiceMode::RUNNING && !IsTransitionPending();
}

std::optional<WorkerRuntimeStateReadGuard> WorkerRuntimeState::TryAcquireReadGuard() const
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

WorkerRuntimeStateTransitionGuard WorkerRuntimeState::AcquireTransitionGuard()
{
    return WorkerRuntimeStateTransitionGuard(pendingTransitions_, admissionMutex_);
}

void WorkerRuntimeState::MarkStarting(std::string detail)
{
    auto guard = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsTerminalLocked() && !IsIsolationModeLocked()) {
        UpdateLocked(WorkerServiceMode::STARTING, WorkerIsolationReason::STARTUP_NOT_READY, {},
                     WorkerRecoveryPhase::NONE, std::move(detail));
    }
}

void WorkerRuntimeState::MarkJoining(std::string detail)
{
    auto guard = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsTerminalLocked() && !IsIsolationModeLocked()) {
        UpdateLocked(WorkerServiceMode::JOINING, WorkerIsolationReason::STARTUP_NOT_READY, {},
                     WorkerRecoveryPhase::NONE, std::move(detail));
    }
}

bool WorkerRuntimeState::TryMarkRunning(const WorkerRunningEvidence &evidence, std::string detail)
{
    auto guard = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsTerminalLocked() || snapshot_.mode == WorkerServiceMode::LOCAL_ISOLATED
        || snapshot_.mode == WorkerServiceMode::OUT_OF_MEMORY) {
        return false;
    }
    if (!IsServingReady(evidence)) {
        UpdateLocked(WorkerServiceMode::RECOVERING, WorkerIsolationReason::RUNTIME_READY_INCOMPLETE, evidence,
                     WorkerRecoveryPhase::MEMBERSHIP, std::move(detail));
        return false;
    }
    UpdateLocked(WorkerServiceMode::RUNNING, WorkerIsolationReason::NONE, evidence, WorkerRecoveryPhase::COMPLETE,
                 std::move(detail));
    return true;
}

void WorkerRuntimeState::MarkDraining(std::string detail)
{
    auto guard = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsTerminalLocked() && !IsIsolationModeLocked()) {
        UpdateLocked(WorkerServiceMode::DRAINING, WorkerIsolationReason::NONE, snapshot_.evidence,
                     WorkerRecoveryPhase::NONE, std::move(detail));
    }
}

void WorkerRuntimeState::MarkLocalIsolated(WorkerIsolationReason reason, std::string detail)
{
    auto guard = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsTerminalLocked()) {
        UpdateLocked(WorkerServiceMode::LOCAL_ISOLATED, reason, {}, WorkerRecoveryPhase::NONE, std::move(detail));
    }
}

void WorkerRuntimeState::MarkOutOfMemory(std::string detail)
{
    auto guard = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsTerminalLocked()) {
        auto evidence = snapshot_.evidence;
        evidence.resourceReady = false;
        UpdateLocked(WorkerServiceMode::OUT_OF_MEMORY, WorkerIsolationReason::OUT_OF_MEMORY, evidence,
                     WorkerRecoveryPhase::RESOURCE, std::move(detail));
    }
}

void WorkerRuntimeState::MarkRecovering(WorkerIsolationReason reason, std::string detail, WorkerRecoveryPhase phase)
{
    auto guard = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsTerminalLocked()) {
        UpdateLocked(WorkerServiceMode::RECOVERING, reason, snapshot_.evidence, phase, std::move(detail));
    }
}

void WorkerRuntimeState::MarkStopping(WorkerIsolationReason reason, std::string detail)
{
    auto guard = AcquireTransitionGuard();
    std::lock_guard<std::mutex> lock(mutex_);
    UpdateLocked(WorkerServiceMode::STOPPING, reason, snapshot_.evidence, WorkerRecoveryPhase::NONE, std::move(detail));
}

void WorkerRuntimeState::UpdateLocked(WorkerServiceMode mode, WorkerIsolationReason reason,
                                      WorkerRunningEvidence evidence, WorkerRecoveryPhase phase, std::string detail)
{
    snapshot_.mode = mode;
    snapshot_.reason = reason;
    snapshot_.recoveryPhase = phase;
    snapshot_.evidence = evidence;
    snapshot_.detail = std::move(detail);
    snapshot_.changedAt = std::chrono::steady_clock::now();
    fastMode_.store(mode, std::memory_order_release);
}

bool WorkerRuntimeState::IsTerminalLocked() const
{
    return snapshot_.mode == WorkerServiceMode::DRAINING || snapshot_.mode == WorkerServiceMode::STOPPING;
}

bool WorkerRuntimeState::IsIsolationModeLocked() const
{
    return snapshot_.mode == WorkerServiceMode::LOCAL_ISOLATED || snapshot_.mode == WorkerServiceMode::OUT_OF_MEMORY;
}
}  // namespace datasystem::worker
