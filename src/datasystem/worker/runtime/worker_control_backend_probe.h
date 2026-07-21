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
#include <memory>
#include <vector>

#include "datasystem/cluster/runtime/control_backend_state.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem::worker {
std::vector<cluster::ControlBackendObservation> ProbeControlBackendPeers(
    const HostPort &localAddress, const std::shared_ptr<AkSkManager> &akSkManager,
    const std::vector<cluster::MemberIdentity> &peers, std::chrono::steady_clock::time_point deadline);
}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_RUNTIME_WORKER_CONTROL_BACKEND_PROBE_H
