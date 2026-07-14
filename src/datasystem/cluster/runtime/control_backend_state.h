/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Process-local control-backend evidence types.
 */
#ifndef DATASYSTEM_CLUSTER_RUNTIME_CONTROL_BACKEND_STATE_H
#define DATASYSTEM_CLUSTER_RUNTIME_CONTROL_BACKEND_STATE_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "datasystem/cluster/model/topology_types.h"

namespace datasystem::cluster {

enum class ControlBackendState : uint8_t { AVAILABLE, UNAVAILABLE, UNKNOWN };

/**
 * @brief Fresh local control-backend evidence bound to one authority stamp.
 */
struct ControlBackendObservation {
    MemberIdentity reporter;
    ControlBackendState state{ ControlBackendState::UNKNOWN };
    uint64_t topologyVersion{ 0 };
    int64_t topologyRevision{ 0 };
    std::string topologyDigest;
    std::chrono::steady_clock::time_point observedAt;
};

using ControlBackendProbe = std::function<std::vector<ControlBackendObservation>(
    const ControlBackendObservation &, const std::vector<MemberIdentity> &, std::chrono::steady_clock::time_point)>;

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_CONTROL_BACKEND_STATE_H
