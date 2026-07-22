/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Probe worker control-backend state through worker RPC.
 */
#include "datasystem/worker/runtime/worker_control_backend_probe.h"

#include <algorithm>
#include <limits>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::worker {
namespace {
struct PendingControlBackendProbe {
    cluster::MemberIdentity peer;
    std::unique_ptr<WorkerControlBackendProbeClient> client;
    int64_t tag{ -1 };
};

Status StartControlBackendProbe(const WorkerControlBackendProbeClientFactory &clientFactory,
                                const cluster::MemberIdentity &peer, std::chrono::steady_clock::time_point deadline,
                                PendingControlBackendProbe &pending)
{
    const auto now = std::chrono::steady_clock::now();
    CHECK_FAIL_RETURN_STATUS(now < deadline, K_RPC_DEADLINE_EXCEEDED, "cluster-state probe deadline exceeded");
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    const auto timeout =
        static_cast<int32_t>(std::min<int64_t>(std::numeric_limits<int32_t>::max(), std::max<int64_t>(remaining, 1)));
    std::unique_ptr<WorkerControlBackendProbeClient> client;
    RETURN_IF_NOT_OK(clientFactory(peer, client));
    CHECK_FAIL_RETURN_STATUS(client != nullptr, K_RUNTIME_ERROR, "cluster-state probe client is null");
    int64_t tag = -1;
    RETURN_IF_NOT_OK(client->Start(timeout, tag));
    pending = { peer, std::move(client), tag };
    return Status::OK();
}

Status FinishControlBackendProbe(const PendingControlBackendProbe &pending,
                                 std::chrono::steady_clock::time_point deadline,
                                 cluster::ControlBackendObservation &observation)
{
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "cluster-state probe deadline exceeded");
    return pending.client->Finish(pending.peer, pending.tag, observation);
}
}  // namespace

std::vector<cluster::ControlBackendObservation> ProbeControlBackendPeers(
    const std::vector<cluster::MemberIdentity> &peers, std::chrono::steady_clock::time_point deadline,
    const WorkerControlBackendProbeClientFactory &clientFactory)
{
    std::vector<PendingControlBackendProbe> pending;
    pending.reserve(peers.size());
    for (const auto &peer : peers) {
        PendingControlBackendProbe probe;
        auto rc = StartControlBackendProbe(clientFactory, peer, deadline, probe);
        if (rc.IsError()) {
            VLOG(1) << "Cluster-state probe start failed for " << peer.address << ": " << rc.ToString();
            return {};
        }
        pending.push_back(std::move(probe));
    }

    std::vector<cluster::ControlBackendObservation> observations;
    observations.reserve(pending.size());
    for (const auto &probe : pending) {
        cluster::ControlBackendObservation observation;
        auto rc = FinishControlBackendProbe(probe, deadline, observation);
        if (rc.IsError()) {
            VLOG(1) << "Cluster-state probe read failed for " << probe.peer.address << ": " << rc.ToString();
            return {};
        }
        observations.push_back(std::move(observation));
    }
    return observations;
}
}  // namespace datasystem::worker
