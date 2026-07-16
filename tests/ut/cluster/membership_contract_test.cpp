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
 * Description: Membership lease wire-value contract tests.
 */
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"

#include <array>
#include <utility>

#include "datasystem/protos/coordinator.pb.h"
#include "gtest/gtest.h"

#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr size_t MAX_MEMBERSHIP_VALUE_BYTES = 64 * 1024;

struct LifecycleCase {
    MemberLifecycleState state;
    coordinator::WorkerServiceInfoPb::StatePb statePb;
};

constexpr std::array<LifecycleCase, 6> LIFECYCLE_CASES = {
    LifecycleCase{ MemberLifecycleState::STARTING, coordinator::WorkerServiceInfoPb::START },
    LifecycleCase{ MemberLifecycleState::RESTARTING, coordinator::WorkerServiceInfoPb::RESTART },
    LifecycleCase{ MemberLifecycleState::RECOVERING, coordinator::WorkerServiceInfoPb::RECOVER },
    LifecycleCase{ MemberLifecycleState::READY, coordinator::WorkerServiceInfoPb::READY },
    LifecycleCase{ MemberLifecycleState::EXITING, coordinator::WorkerServiceInfoPb::EXITING },
    LifecycleCase{ MemberLifecycleState::DOWNGRADE_RESTARTING, coordinator::WorkerServiceInfoPb::DOWNGRADE_RESTART },
};

TEST(MembershipContractTest, PreservesMasterGoldenPayloadForEveryWireState)
{
    for (const auto &testCase : LIFECYCLE_CASES) {
        MembershipValue input{ 123456, testCase.state, "host-a", "v1" };
        std::string bytes;
        DS_ASSERT_OK(MembershipValueCodec::Encode(input, bytes));

        coordinator::WorkerServiceInfoPb wire;
        ASSERT_TRUE(wire.ParseFromString(bytes));
        EXPECT_EQ(wire.timestamp(), input.timestamp);
        EXPECT_EQ(wire.state(), testCase.statePb);
        EXPECT_EQ(wire.host_id(), input.hostId);
        EXPECT_EQ(wire.compatibility_version(), input.compatibilityVersion);

        MembershipValue output;
        DS_ASSERT_OK(MembershipValueCodec::Decode(bytes, output));
        EXPECT_EQ(output.timestamp, input.timestamp);
        EXPECT_EQ(output.lifecycleState, input.lifecycleState);
        EXPECT_EQ(output.hostId, input.hostId);
        EXPECT_EQ(output.compatibilityVersion, input.compatibilityVersion);
    }
}

TEST(MembershipContractTest, DecodesLegacyEtcdPayload)
{
    const std::array<std::pair<const char *, MemberLifecycleState>, 6> legacyCases = {
        std::pair{ "start", MemberLifecycleState::STARTING },
        std::pair{ "restart", MemberLifecycleState::RESTARTING },
        std::pair{ "recover", MemberLifecycleState::RECOVERING },
        std::pair{ "ready", MemberLifecycleState::READY },
        std::pair{ "exiting", MemberLifecycleState::EXITING },
        std::pair{ "d_rst", MemberLifecycleState::DOWNGRADE_RESTARTING },
    };
    for (const auto &[stateBytes, expectedState] : legacyCases) {
        MembershipValue output;
        DS_ASSERT_OK(MembershipValueCodec::Decode("123456;" + std::string(stateBytes) + ";host-a;v1", output));
        EXPECT_EQ(output.timestamp, 123456);
        EXPECT_EQ(output.lifecycleState, expectedState);
        EXPECT_EQ(output.hostId, "host-a");
        EXPECT_EQ(output.compatibilityVersion, "v1");
    }
}

TEST(MembershipContractTest, RejectsNonWireLifecycleStates)
{
    std::string bytes;
    EXPECT_EQ(MembershipValueCodec::Encode({ 1, MemberLifecycleState::UNKNOWN, "", "" }, bytes).GetCode(), K_INVALID);
    EXPECT_EQ(MembershipValueCodec::Encode({ 1, MemberLifecycleState::FAILED, "", "" }, bytes).GetCode(), K_INVALID);
}

TEST(MembershipContractTest, RejectsMalformedAndUnspecifiedPayload)
{
    MembershipValue output{ 1, MemberLifecycleState::READY, "old-host", "old-version" };
    EXPECT_EQ(MembershipValueCodec::Decode("malformed", output).GetCode(), K_INVALID);
    EXPECT_EQ(output.lifecycleState, MemberLifecycleState::UNKNOWN);

    output = { 1, MemberLifecycleState::READY, "old-host", "old-version" };
    EXPECT_EQ(MembershipValueCodec::Decode("123;invalid", output).GetCode(), K_INVALID);
    EXPECT_EQ(output.lifecycleState, MemberLifecycleState::UNKNOWN);

    coordinator::WorkerServiceInfoPb wire;
    wire.set_state(coordinator::WorkerServiceInfoPb::UNSPECIFIED);
    std::string bytes;
    ASSERT_TRUE(wire.SerializeToString(&bytes));
    EXPECT_EQ(MembershipValueCodec::Decode(bytes, output).GetCode(), K_INVALID);
    EXPECT_EQ(output.lifecycleState, MemberLifecycleState::UNKNOWN);
}

TEST(MembershipContractTest, RejectsOversizedPayload)
{
    MembershipValue output{ 1, MemberLifecycleState::READY, "old-host", "old-version" };
    const std::string oversized(MAX_MEMBERSHIP_VALUE_BYTES + 1, 'x');
    EXPECT_EQ(MembershipValueCodec::Decode(oversized, output).GetCode(), K_INVALID);
    EXPECT_EQ(output.lifecycleState, MemberLifecycleState::UNKNOWN);
}

TEST(MembershipContractTest, SeparatesLeaseStateFromTopologyState)
{
    MembershipRecord record{ "127.0.0.1:31501", MemberLifecycleState::READY, 0, "" };
    EXPECT_EQ(record.address, "127.0.0.1:31501");
    EXPECT_EQ(record.state, MemberLifecycleState::READY);
}

TEST(MembershipEndpointViewTest, FencesLocalObservationByTopologyVersionAndIdentity)
{
    TopologyState topology;
    topology.version = 1;
    topology.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    MembershipEndpointView view(snapshots);
    EndpointObservation observation{ topology.members.front().identity, 1, EndpointAvailability::REACHABLE,
                                     std::chrono::steady_clock::now() };
    DS_ASSERT_OK(view.UpdateObservation(observation));
    MemberEndpoint endpoint;
    DS_ASSERT_OK(view.ResolveByAddress(observation.identity.address, endpoint));
    EXPECT_EQ(endpoint.localAvailability, EndpointAvailability::REACHABLE);

    topology.version = 2;
    topology.members.front().state = MemberState::FAILED;
    topology.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 2 };
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'b'), snapshot));
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    DS_ASSERT_OK(view.ResolveByAddress(observation.identity.address, endpoint));
    EXPECT_EQ(endpoint.localAvailability, EndpointAvailability::UNREACHABLE);
    EXPECT_EQ(view.UpdateObservation(observation).GetCode(), K_INVALID);
}

}  // namespace
}  // namespace datasystem::cluster
