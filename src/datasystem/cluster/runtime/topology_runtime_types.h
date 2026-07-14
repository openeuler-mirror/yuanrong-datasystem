/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker-role cluster topology runtime values and bounded options.
 */
#ifndef DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_RUNTIME_TYPES_H
#define DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_RUNTIME_TYPES_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

#include "datasystem/cluster/executor/topology_task_executor.h"
#include "datasystem/cluster/runtime/control_backend_state.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"

namespace datasystem::cluster {

enum class TopologyEngineState : uint8_t { STOPPED, STARTING, RUNNING, STOPPING };

enum class TopologyAvailabilityLevel : uint8_t {
    NORMAL,
    CONTROL_DEGRADED,
    ROLE_ISOLATED,
    NOT_READY,
    SHUTTING_DOWN,
};

/**
 * @brief Worker-role identity, budgets, probe and callback limits.
 */
struct TopologyEngineOptions {
    std::string clusterName;
    std::string localAddress;
    bool isRestart{ false };
    size_t eventQueueCapacity{ 1'024 };
    std::chrono::seconds scopeProbeDeadline{ 2 };
    std::chrono::seconds scopeProbeInterval{ 5 };
    std::chrono::seconds stopGrace{ 10 };
    ControlBackendProbe controlBackendProbe;
    // Non-blocking Host hook that gates business ingress when Worker-role availability changes.
    std::function<void(TopologyAvailabilityLevel)> availabilityHandler;
    TopologyTaskExecutorOptions executor;
};

/**
 * @brief Flattened Worker-role diagnostic snapshot with no PB or task payload.
 */
struct TopologyDiagnostics {
    TopologyEngineState state{ TopologyEngineState::STOPPED };
    TopologyAvailabilityLevel availability{ TopologyAvailabilityLevel::NOT_READY };
    uint64_t topologyVersion{ 0 };
    int64_t topologyRevision{ 0 };
    std::string topologyDigestPrefix;
    bool backendHealthy{ false };
    ControlBackendState controlBackendState{ ControlBackendState::UNKNOWN };
    std::string isolationReason;
    CoordinationEventDispatcherStats dispatcher;
    TopologyTaskExecutorDiagnostics executor;
    std::string lastError;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_RUNTIME_TOPOLOGY_RUNTIME_TYPES_H
