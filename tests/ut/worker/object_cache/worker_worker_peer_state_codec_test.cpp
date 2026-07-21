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
 * Description: Test topology peer state conversion for worker-to-worker cluster state RPC.
 */
#include "datasystem/worker/object_cache/worker_worker_peer_state_codec.h"

#include <cstdint>

#include <gtest/gtest.h>

#include "datasystem/common/util/uuid_generator.h"
#include "ut/common.h"

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
namespace {
constexpr uint64_t kTopologyVersion = 7;
constexpr int64_t kTopologyRevision = 11;
constexpr size_t kWorkerIdSize = 16;
constexpr size_t kTopologyDigestSize = 64;
}  // namespace

class WorkerWorkerPeerStateCodecTest : public CommonTest {
};

TEST_F(WorkerWorkerPeerStateCodecTest, ControlBackendObservationRoundTrip)
{
    cluster::ControlBackendObservation observation;
    observation.reporter.id = std::string(kWorkerIdSize, 'a');
    observation.reporter.address = "127.0.0.1:10001";
    observation.state = cluster::ControlBackendState::UNAVAILABLE;
    observation.topologyVersion = kTopologyVersion;
    observation.topologyRevision = kTopologyRevision;
    observation.topologyDigest = std::string(kTopologyDigestSize, 'b');
    observation.observedAt = std::chrono::steady_clock::now();

    GetClusterStateRspPb rsp;
    DS_ASSERT_OK(object_cache::FillGetClusterStateRspPbFromControlBackendObservation(observation, rsp));
    EXPECT_FALSE(rsp.coordinator_available());
    EXPECT_TRUE(rsp.ready());
    EXPECT_EQ(rsp.node_id(), BytesUuidToString(observation.reporter.id));
    EXPECT_EQ(rsp.topology_version(), observation.topologyVersion);
    EXPECT_EQ(rsp.topology_revision(), observation.topologyRevision);
    EXPECT_EQ(rsp.topology_digest(), observation.topologyDigest);

    cluster::ControlBackendObservation converted;
    DS_ASSERT_OK(object_cache::FillControlBackendObservationFromGetClusterStateRspPb(
        observation.reporter.address, rsp, converted));
    EXPECT_EQ(converted.reporter, observation.reporter);
    EXPECT_EQ(converted.state, observation.state);
    EXPECT_EQ(converted.topologyVersion, observation.topologyVersion);
    EXPECT_EQ(converted.topologyRevision, observation.topologyRevision);
    EXPECT_EQ(converted.topologyDigest, observation.topologyDigest);
}

TEST_F(WorkerWorkerPeerStateCodecTest, CurrentBackendStatePreservesAuthorityEvidence)
{
    cluster::ControlBackendObservation observation;
    observation.reporter.id = std::string(kWorkerIdSize, 'a');
    observation.reporter.address = "127.0.0.1:10001";
    observation.state = cluster::ControlBackendState::AVAILABLE;
    observation.topologyVersion = kTopologyVersion;
    observation.topologyRevision = kTopologyRevision;
    observation.topologyDigest = std::string(kTopologyDigestSize, 'b');
    observation.observedAt = std::chrono::steady_clock::now() - std::chrono::seconds(1);

    const auto unavailable = RefreshControlBackendObservationState(observation, false);
    EXPECT_EQ(unavailable.reporter, observation.reporter);
    EXPECT_EQ(unavailable.state, cluster::ControlBackendState::UNAVAILABLE);
    EXPECT_EQ(unavailable.topologyVersion, observation.topologyVersion);
    EXPECT_EQ(unavailable.topologyRevision, observation.topologyRevision);
    EXPECT_EQ(unavailable.topologyDigest, observation.topologyDigest);
    EXPECT_GT(unavailable.observedAt, observation.observedAt);

    const auto available = RefreshControlBackendObservationState(unavailable, true);
    EXPECT_EQ(available.reporter, observation.reporter);
    EXPECT_EQ(available.state, cluster::ControlBackendState::AVAILABLE);
    EXPECT_EQ(available.topologyVersion, observation.topologyVersion);
    EXPECT_EQ(available.topologyRevision, observation.topologyRevision);
    EXPECT_EQ(available.topologyDigest, observation.topologyDigest);
    EXPECT_GE(available.observedAt, unavailable.observedAt);
}

TEST_F(WorkerWorkerPeerStateCodecTest, RejectsStaleOrMalformedControlBackendEvidenceWithoutMutation)
{
    cluster::ControlBackendObservation stale;
    stale.reporter.id = std::string(kWorkerIdSize, 'a');
    stale.reporter.address = "127.0.0.1:10001";
    stale.state = cluster::ControlBackendState::AVAILABLE;
    stale.topologyVersion = kTopologyVersion;
    stale.topologyRevision = kTopologyRevision;
    stale.topologyDigest = std::string(kTopologyDigestSize, 'b');
    stale.observedAt = std::chrono::steady_clock::now() - std::chrono::seconds(6);
    GetClusterStateRspPb unchanged;
    unchanged.set_node_id("unchanged");
    EXPECT_EQ(object_cache::FillGetClusterStateRspPbFromControlBackendObservation(stale, unchanged).GetCode(),
              K_NOT_READY);
    EXPECT_EQ(unchanged.node_id(), "unchanged");

    GetClusterStateRspPb malformed;
    malformed.set_ready(true);
    malformed.set_node_id("short");
    malformed.set_topology_version(kTopologyVersion);
    malformed.set_topology_revision(kTopologyRevision);
    malformed.set_topology_digest(std::string(kTopologyDigestSize, 'c'));
    cluster::ControlBackendObservation output;
    output.reporter.id = "unchanged";
    EXPECT_EQ(object_cache::FillControlBackendObservationFromGetClusterStateRspPb(
                  "127.0.0.1:10002", malformed, output).GetCode(),
              K_INVALID);
    EXPECT_EQ(output.reporter.id, "unchanged");

    malformed.set_node_id(BytesUuidToString(std::string(kWorkerIdSize, 'd')));
    malformed.set_ready(false);
    EXPECT_EQ(object_cache::FillControlBackendObservationFromGetClusterStateRspPb(
                  "127.0.0.1:10002", malformed, output).GetCode(),
              K_NOT_READY);
    EXPECT_EQ(output.reporter.id, "unchanged");
}
}  // namespace ut
}  // namespace datasystem
