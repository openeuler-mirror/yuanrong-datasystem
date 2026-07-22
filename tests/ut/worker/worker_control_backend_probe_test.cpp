/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker control backend peer probe tests.
 */
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/worker/runtime/worker_control_backend_probe.h"

namespace datasystem::worker {
namespace {
cluster::MemberIdentity Peer(const std::string &address)
{
    cluster::MemberIdentity peer;
    peer.id = address;
    peer.address = address;
    return peer;
}

cluster::ControlBackendObservation AvailableObservation(const cluster::MemberIdentity &peer)
{
    cluster::ControlBackendObservation observation;
    observation.reporter = peer;
    observation.state = cluster::ControlBackendState::AVAILABLE;
    observation.topologyVersion = 1;
    observation.topologyRevision = 2;
    observation.topologyDigest = "digest";
    observation.observedAt = std::chrono::steady_clock::now();
    return observation;
}
}  // namespace

TEST(WorkerControlBackendProbeTest, KeepsSuccessfulObservationsWhenOnePeerProbeFails)
{
    const auto availablePeer = Peer("worker1");
    const auto failedPeer = Peer("worker2");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);

    auto observations = ProbeControlBackendPeers(
        { availablePeer, failedPeer }, deadline,
        [availablePeer](const cluster::MemberIdentity &, std::unique_ptr<WorkerControlBackendProbe> &client) {
            client = std::make_unique<WorkerControlBackendProbe>(
                [](int32_t, int64_t &tag) {
                    tag = 1;
                    return Status::OK();
                },
                [availablePeer](const cluster::MemberIdentity &peer, int64_t,
                                cluster::ControlBackendObservation &observation) {
                    if (peer.address != availablePeer.address) {
                        return Status(K_RPC_UNAVAILABLE, "injected peer probe failure");
                    }
                    observation = AvailableObservation(peer);
                    return Status::OK();
                });
            return Status::OK();
        });

    ASSERT_EQ(observations.size(), 1ul);
    EXPECT_EQ(observations[0].reporter, availablePeer);
    EXPECT_EQ(observations[0].state, cluster::ControlBackendState::AVAILABLE);
}
}  // namespace datasystem::worker
