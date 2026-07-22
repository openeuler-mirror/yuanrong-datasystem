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

class WorkerControlBackendProbe {
public:
    using StartFn = std::function<Status(int32_t, int64_t &)>;
    using FinishFn =
        std::function<Status(const cluster::MemberIdentity &, int64_t, cluster::ControlBackendObservation &)>;

    WorkerControlBackendProbe(StartFn start, FinishFn finish);
    ~WorkerControlBackendProbe() = default;

    Status Start(int32_t timeoutMs, int64_t &tag);

    Status Finish(const cluster::MemberIdentity &peer, int64_t tag, cluster::ControlBackendObservation &observation);

private:
    StartFn start_;
    FinishFn finish_;
};

using WorkerControlBackendProbeFactory =
    std::function<Status(const cluster::MemberIdentity &, std::unique_ptr<WorkerControlBackendProbe> &)>;

std::vector<cluster::ControlBackendObservation> ProbeControlBackendPeers(
    const std::vector<cluster::MemberIdentity> &peers, std::chrono::steady_clock::time_point deadline,
    const WorkerControlBackendProbeFactory &clientFactory);
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_RUNTIME_WORKER_CONTROL_BACKEND_PROBE_H
