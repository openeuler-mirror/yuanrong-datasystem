/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Control backend failure scope classifier tests.
 */
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

#include "datasystem/cluster/runtime/control_backend_scope_classifier.h"

namespace datasystem::cluster {
namespace {
constexpr auto STALE_BACKEND_EVIDENCE_AGE = std::chrono::seconds(10);

ControlBackendObservation LocalUnavailableObservation()
{
    ControlBackendObservation observation;
    observation.reporter.address = "worker0";
    observation.state = ControlBackendState::UNAVAILABLE;
    observation.topologyVersion = 1;
    observation.topologyRevision = 2;
    observation.topologyDigest = "digest";
    observation.observedAt = std::chrono::steady_clock::now();
    return observation;
}

MemberIdentity Peer(const std::string &address)
{
    MemberIdentity peer;
    peer.id = address;
    peer.address = address;
    return peer;
}

ControlBackendObservation PeerObservation(const MemberIdentity &peer, ControlBackendState state)
{
    auto observation = LocalUnavailableObservation();
    observation.reporter = peer;
    observation.state = state;
    return observation;
}
}  // namespace

TEST(ControlBackendScopeClassifierTest, EmptyPeerTargetsCannotProveGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, {}, {}));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, {}, {}), ControlBackendFailureScope::INCONCLUSIVE);
}

TEST(ControlBackendScopeClassifierTest, MatchingUnavailablePeerEvidenceConfirmsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    EXPECT_TRUE(
        ConfirmsGlobalBackendOutage(local, { peer }, { PeerObservation(peer, ControlBackendState::UNAVAILABLE) }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer },
                                                 { PeerObservation(peer, ControlBackendState::UNAVAILABLE) }),
              ControlBackendFailureScope::GLOBAL_OUTAGE);
}

TEST(ControlBackendScopeClassifierTest, AvailablePeerEvidenceRejectsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    EXPECT_FALSE(
        ConfirmsGlobalBackendOutage(local, { peer }, { PeerObservation(peer, ControlBackendState::AVAILABLE) }));
    EXPECT_EQ(
        ClassifyControlBackendFailureScope(local, { peer }, { PeerObservation(peer, ControlBackendState::AVAILABLE) }),
        ControlBackendFailureScope::LOCAL_ISOLATION);
}

TEST(ControlBackendScopeClassifierTest, StalePeerEvidenceRejectsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    auto stale = PeerObservation(peer, ControlBackendState::UNAVAILABLE);
    stale.observedAt -= STALE_BACKEND_EVIDENCE_AGE;
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer }, { stale }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer }, { stale }), ControlBackendFailureScope::INCONCLUSIVE);
}

TEST(ControlBackendScopeClassifierTest, MismatchedTopologyStampRejectsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    auto mismatched = PeerObservation(peer, ControlBackendState::UNAVAILABLE);
    mismatched.topologyDigest = "other-digest";
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer }, { mismatched }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer }, { mismatched }),
              ControlBackendFailureScope::INCONCLUSIVE);
}

TEST(ControlBackendScopeClassifierTest, AvailablePeerWithMismatchedTopologyStampConfirmsLocalIsolation)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    auto mismatched = PeerObservation(peer, ControlBackendState::AVAILABLE);
    mismatched.topologyVersion = local.topologyVersion + 1;
    mismatched.topologyRevision = local.topologyRevision + 1;
    mismatched.topologyDigest = "newer-digest";

    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer }, { mismatched }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer }, { mismatched }),
              ControlBackendFailureScope::LOCAL_ISOLATION);
}

TEST(ControlBackendScopeClassifierTest, PartialAvailablePeerEvidenceConfirmsLocalIsolation)
{
    const auto local = LocalUnavailableObservation();
    const auto availablePeer = Peer("worker1");
    const auto failedProbePeer = Peer("worker2");

    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { availablePeer, failedProbePeer },
                                                 { PeerObservation(availablePeer, ControlBackendState::AVAILABLE) }),
              ControlBackendFailureScope::LOCAL_ISOLATION);
}

TEST(ControlBackendScopeClassifierTest, DuplicatePeerEvidenceRejectsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    const auto otherPeer = Peer("worker2");
    auto observation = PeerObservation(peer, ControlBackendState::UNAVAILABLE);
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer, otherPeer }, { observation, observation }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer, otherPeer }, { observation, observation }),
              ControlBackendFailureScope::INCONCLUSIVE);
}
}  // namespace datasystem::cluster
