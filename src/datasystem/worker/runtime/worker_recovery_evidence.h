/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker recovery evidence data contract.
 */
#ifndef DATASYSTEM_WORKER_WORKER_RECOVERY_EVIDENCE_H
#define DATASYSTEM_WORKER_WORKER_RECOVERY_EVIDENCE_H

#include <array>
#include <cstdint>
#include <string>

#include "datasystem/worker/runtime/worker_runtime_state.h"

namespace datasystem::worker {
struct WorkerRecoveryEvidenceReport {
    WorkerRunningEvidence evidence;
    std::string detail;
};

using WorkerRecoveryGeneration = uint64_t;

struct GenerationedWorkerRecoveryEvidenceReport {
    WorkerRecoveryGeneration generation{ 0 };
    WorkerRecoveryEvidenceReport report;
};

class WorkerRecoveryEvidenceBuilder {
public:
    WorkerRecoveryEvidenceBuilder() = default;
    ~WorkerRecoveryEvidenceBuilder() = default;

    WorkerRecoveryEvidenceBuilder &MarkMembershipReady(std::string detail = {});
    WorkerRecoveryEvidenceBuilder &MarkTopologyReady(std::string detail = {});
    WorkerRecoveryEvidenceBuilder &MarkMetadataReady(std::string detail = {});
    WorkerRecoveryEvidenceBuilder &MarkSlotReady(std::string detail = {});
    WorkerRecoveryEvidenceBuilder &MarkOwnershipReady(std::string detail = {});
    WorkerRecoveryEvidenceBuilder &MarkResourceReady(std::string detail = {});

    WorkerRecoveryEvidenceReport BuildReport(const std::string &detail = {}) const;

private:
    enum EvidenceIndex : std::uint8_t {
        MEMBERSHIP = 0,
        TOPOLOGY,
        METADATA,
        SLOT,
        OWNERSHIP,
        RESOURCE,
        EVIDENCE_COUNT,
    };

    WorkerRecoveryEvidenceBuilder &SetReady(EvidenceIndex index, std::string detail);

    WorkerRunningEvidence evidence_;
    std::array<std::string, EVIDENCE_COUNT> details_;
};
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_WORKER_RECOVERY_EVIDENCE_H
