/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#ifndef DATASYSTEM_CLUSTER_RUNTIME_WORKER_LIVENESS_H
#define DATASYSTEM_CLUSTER_RUNTIME_WORKER_LIVENESS_H

#include <cstdint>
#include <string>

#include "datasystem/cluster/model/topology_types.h"

namespace datasystem::cluster {

enum class WorkerLivenessResult : uint8_t { UNKNOWN = 0, REACHABLE, UNREACHABLE };

inline const char *WorkerLivenessResultName(WorkerLivenessResult result) noexcept
{
    switch (result) {
        case WorkerLivenessResult::REACHABLE:
            return "REACHABLE";
        case WorkerLivenessResult::UNREACHABLE:
            return "UNREACHABLE";
        default:
            return "UNKNOWN";
    }
}

struct WorkerProbeRequest {
    std::string clusterName;
    std::string probeEpoch;
    uint64_t probeRound{ 0 };
    MemberIdentity target;
};

struct WorkerLivenessReport {
    std::string probeEpoch;
    std::string witnessAddress;
    MemberIdentity target;
    uint64_t probeRound{ 0 };
    WorkerLivenessResult result{ WorkerLivenessResult::UNKNOWN };
    uint32_t deliveryAttempts{ 0 };
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_WORKER_LIVENESS_H
