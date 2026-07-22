/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Worker control backend failure scope tests.
 */
#include <gtest/gtest.h>

#include <vector>

#include "datasystem/worker/runtime/worker_control_backend_scope.h"

namespace datasystem::worker {
namespace {
constexpr auto STALE_BACKEND_EVIDENCE_AGE = std::chrono::seconds(10);

cluster::ControlBackendObservation LocalUnavailableObservation()
{
    cluster::ControlBackendObservation observation;
    observation.reporter.address = "worker0";
    observation.state = cluster::ControlBackendState::UNAVAILABLE;
    observation.topologyVersion = 1;
    observation.topologyRevision = 2;
    observation.topologyDigest = "digest";
    observation.observedAt = std::chrono::steady_clock::now();
    return observation;
}

cluster::MemberIdentity Peer(const std::string &address)
{
    cluster::MemberIdentity peer;
    peer.id = address;
    peer.address = address;
    return peer;
}

cluster::ControlBackendObservation PeerObservation(const cluster::MemberIdentity &peer,
                                                   cluster::ControlBackendState state)
{
    auto observation = LocalUnavailableObservation();
    observation.reporter = peer;
    observation.state = state;
    return observation;
}
}  // namespace

TEST(WorkerControlBackendScopeTest, EmptyPeerTargetsCannotProveGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, {}, {}));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, {}, {}), ControlBackendFailureScope::INCONCLUSIVE);
}

TEST(WorkerControlBackendScopeTest, MatchingUnavailablePeerEvidenceConfirmsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    EXPECT_TRUE(ConfirmsGlobalBackendOutage(local, { peer },
                                            { PeerObservation(peer, cluster::ControlBackendState::UNAVAILABLE) }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer },
                                                 { PeerObservation(peer, cluster::ControlBackendState::UNAVAILABLE) }),
              ControlBackendFailureScope::GLOBAL_OUTAGE);
}

TEST(WorkerControlBackendScopeTest, AvailablePeerEvidenceRejectsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer },
                                             { PeerObservation(peer, cluster::ControlBackendState::AVAILABLE) }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer },
                                                 { PeerObservation(peer, cluster::ControlBackendState::AVAILABLE) }),
              ControlBackendFailureScope::LOCAL_ISOLATION);
}

TEST(WorkerControlBackendScopeTest, StalePeerEvidenceRejectsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    auto stale = PeerObservation(peer, cluster::ControlBackendState::UNAVAILABLE);
    stale.observedAt -= STALE_BACKEND_EVIDENCE_AGE;
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer }, { stale }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer }, { stale }), ControlBackendFailureScope::INCONCLUSIVE);
}

TEST(WorkerControlBackendScopeTest, MismatchedTopologyStampRejectsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    auto mismatched = PeerObservation(peer, cluster::ControlBackendState::UNAVAILABLE);
    mismatched.topologyDigest = "other-digest";
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer }, { mismatched }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer }, { mismatched }),
              ControlBackendFailureScope::INCONCLUSIVE);
}

TEST(WorkerControlBackendScopeTest, AvailablePeerWithMismatchedTopologyStampConfirmsLocalIsolation)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    auto mismatched = PeerObservation(peer, cluster::ControlBackendState::AVAILABLE);
    mismatched.topologyVersion = local.topologyVersion + 1;
    mismatched.topologyRevision = local.topologyRevision + 1;
    mismatched.topologyDigest = "newer-digest";

    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer }, { mismatched }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer }, { mismatched }),
              ControlBackendFailureScope::LOCAL_ISOLATION);
}

TEST(WorkerControlBackendScopeTest, DuplicatePeerEvidenceRejectsGlobalBackendOutage)
{
    const auto local = LocalUnavailableObservation();
    const auto peer = Peer("worker1");
    const auto otherPeer = Peer("worker2");
    auto observation = PeerObservation(peer, cluster::ControlBackendState::UNAVAILABLE);
    EXPECT_FALSE(ConfirmsGlobalBackendOutage(local, { peer, otherPeer }, { observation, observation }));
    EXPECT_EQ(ClassifyControlBackendFailureScope(local, { peer, otherPeer }, { observation, observation }),
              ControlBackendFailureScope::INCONCLUSIVE);
}
}  // namespace datasystem::worker
