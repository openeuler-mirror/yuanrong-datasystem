/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Public worker runtime data types.
 */
#ifndef DATASYSTEM_WORKER_WORKER_RUNTIME_TYPES_H
#define DATASYSTEM_WORKER_WORKER_RUNTIME_TYPES_H

#include <chrono>
#include <cstdint>
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

enum class WorkerAdmissionKind : std::uint8_t {
    NORMAL_READ = 0,
    NORMAL_WRITE,
    MIGRATION_TARGET,
    RECOVERY_RPC,
    CLEANUP_RPC,
    RESOURCE_RECOVERY_RPC,
    INTERNAL_JOINING_RPC,
    DIAGNOSTIC_RPC,
};

const char *ToString(WorkerServiceMode mode);
const char *ToString(WorkerIsolationReason reason);
const char *ToString(WorkerRecoveryPhase phase);
const char *ToString(WorkerAdmissionKind kind);

bool IsComplete(const WorkerRunningEvidence &evidence);
WorkerRecoveryPhase FirstMissingRecoveryPhase(const WorkerRunningEvidence &evidence);
uint32_t RecoveryEvidenceMask(const WorkerRunningEvidence &evidence);
bool IsServingMode(WorkerServiceMode mode);
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_RUNTIME_TYPES_H
