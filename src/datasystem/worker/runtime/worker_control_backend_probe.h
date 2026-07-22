/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Probe worker control-backend state through worker RPC.
 */
#ifndef DATASYSTEM_WORKER_RUNTIME_WORKER_CONTROL_BACKEND_PROBE_H
#define DATASYSTEM_WORKER_RUNTIME_WORKER_CONTROL_BACKEND_PROBE_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "datasystem/cluster/runtime/control_backend_state.h"
#include "datasystem/utils/status.h"

namespace datasystem::worker {

class WorkerControlBackendProbeClient {
public:
    virtual ~WorkerControlBackendProbeClient() = default;

    virtual Status Start(int32_t timeoutMs, int64_t &tag) = 0;

    virtual Status Finish(const cluster::MemberIdentity &peer, int64_t tag,
                          cluster::ControlBackendObservation &observation) = 0;
};

using WorkerControlBackendProbeClientFactory =
    std::function<Status(const cluster::MemberIdentity &, std::unique_ptr<WorkerControlBackendProbeClient> &)>;

std::vector<cluster::ControlBackendObservation> ProbeControlBackendPeers(
    const std::vector<cluster::MemberIdentity> &peers, std::chrono::steady_clock::time_point deadline,
    const WorkerControlBackendProbeClientFactory &clientFactory);
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_RUNTIME_WORKER_CONTROL_BACKEND_PROBE_H
