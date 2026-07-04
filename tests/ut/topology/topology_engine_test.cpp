/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Topology engine lifecycle unit tests.
 */
#include "datasystem/topology/runtime/topology_engine.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/algorithm/hash_algorithm.h"
#include "datasystem/topology/repository/topology_key_helper.h"
#include "datasystem/topology/repository/topology_repository_codec.h"
#include "datasystem/topology/runtime/coordination_event_pump.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/fake_coordination_backend.h"
#include "tests/ut/topology/testing/topology_test_utils.h"

namespace datasystem {
namespace topology {
namespace {

constexpr int64_t TOPOLOGY_VERSION_1 = 1;
constexpr int64_t TOPOLOGY_VERSION_2 = 2;
constexpr int64_t TOPOLOGY_VERSION_3 = 3;
constexpr Revision GAP_EVENT_REVISION = 5;
constexpr uint32_t NODE_A_TOKEN = 100;
constexpr uint32_t NODE_B_TOKEN = 200;
constexpr int NODE_A_PORT = 12001;
constexpr int NODE_B_PORT = 12002;
constexpr char NODE_A[] = "node-a";
constexpr char NODE_B[] = "node-b";
constexpr char ROUTE_KEY[] = "topology-engine-route-key";

TopologyDescriptor MakeTopology(int64_t version)
{
    return testing::MakeTopologyForTest({
        testing::MakeTopologyNodeForTest(NODE_A, { NODE_A_TOKEN }),
        testing::MakeTopologyNodeForTest(NODE_B, { NODE_B_TOKEN }),
    },
                                        version);
}

Status MakeCommittedTopologyEvent(const TopologyDescriptor &topology, CoordinationEvent &event)
{
    TopologyRepositoryCodec codec;
    event = {};
    event.type = CoordinationEventType::PUT;
    event.key = TopologyKeyHelper::CommittedTopologyKey();
    event.revision = topology.version;
    return codec.EncodeTopology(topology, event.value);
}

Status GetRoutingVersion(TopologyEngine &engine, int64_t &version)
{
    TopologyReadinessSnapshot snapshot;
    RETURN_IF_NOT_OK(engine.Readiness().GetSnapshot(snapshot));
    version = snapshot.routingVersion;
    return Status::OK();
}

std::shared_ptr<MembershipEndpointSnapshot> MakeEndpointSnapshot()
{
    auto snapshot = std::make_shared<MembershipEndpointSnapshot>();
    snapshot->version = TOPOLOGY_VERSION_1;
    snapshot->localNodeId = NODE_A;
    snapshot->localAddress = HostPort("127.0.0.1", NODE_A_PORT);
    snapshot->members.emplace(NODE_A, MemberEndpoint{ NODE_A, HostPort("127.0.0.1", NODE_A_PORT),
                                                      MemberAvailability::READY });
    snapshot->members.emplace(NODE_B, MemberEndpoint{ NODE_B, HostPort("127.0.0.1", NODE_B_PORT),
                                                      MemberAvailability::READY });
    snapshot->nodeIdsByAddress.emplace(snapshot->members.at(NODE_A).address.ToString(), NODE_A);
    snapshot->nodeIdsByAddress.emplace(snapshot->members.at(NODE_B).address.ToString(), NODE_B);
    return snapshot;
}

}  // namespace

TEST(TopologyEngineTest, StartFailsWithoutCommittedTopologyAndLeavesBackendDetached)
{
    FakeCoordinationBackend backend;
    HashAlgorithm algorithm;
    TopologyEngine engine(backend, algorithm);

    EXPECT_EQ(engine.Start().GetCode(), K_NOT_FOUND);
    EXPECT_EQ(engine.GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(backend.HasEventHandlerForTest());

    TopologyReadinessSnapshot readiness;
    DS_ASSERT_OK(engine.Readiness().GetSnapshot(readiness));
    EXPECT_FALSE(readiness.ready);
    EXPECT_EQ(readiness.lastStatus.GetCode(), K_NOT_READY);

    TopologyDiagnosticsSnapshot diagnostics;
    DS_ASSERT_OK(engine.Diagnostics().GetSnapshot(diagnostics));
    EXPECT_FALSE(diagnostics.eventPump.running);
}

TEST(TopologyEngineTest, StartRebuildsCommittedTopologyAndReportsReady)
{
    FakeCoordinationBackend backend;
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(backend, MakeTopology(TOPOLOGY_VERSION_1)));
    HashAlgorithm algorithm;
    TopologyEngine engine(backend, algorithm);

    DS_ASSERT_OK(engine.Start());
    EXPECT_EQ(engine.GetState(), TopologyEngineState::RUNNING);
    EXPECT_TRUE(backend.HasEventHandlerForTest());
    int64_t version = 0;
    DS_ASSERT_OK(GetRoutingVersion(engine, version));
    EXPECT_EQ(version, TOPOLOGY_VERSION_1);

    TopologyReadinessSnapshot readiness;
    DS_ASSERT_OK(engine.Readiness().GetSnapshot(readiness));
    EXPECT_TRUE(readiness.ready);
    EXPECT_EQ(readiness.routingVersion, TOPOLOGY_VERSION_1);
}

TEST(TopologyEngineTest, ExposesPlacementAndMembershipFacades)
{
    FakeCoordinationBackend backend;
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(backend, MakeTopology(TOPOLOGY_VERSION_1)));
    HashAlgorithm algorithm;
    TopologyEngine engine(backend, algorithm);
    DS_ASSERT_OK(engine.Start());
    engine.Membership().Publish(MakeEndpointSnapshot());

    RouteDecision decision;
    RouteOptions options;
    DS_ASSERT_OK(engine.Placement().LocateMetaOwner(ROUTE_KEY, options, decision));
    EXPECT_FALSE(decision.ownerNodeId.empty());
    EXPECT_FALSE(decision.ownerEndpoint.Empty());

    MemberEndpoint localEndpoint;
    DS_ASSERT_OK(engine.Membership().GetLocalEndpoint(localEndpoint));
    EXPECT_EQ(localEndpoint.nodeId, NODE_A);
}

TEST(TopologyEngineTest, BackendEventsUpdateSnapshotWithoutVersionRollback)
{
    FakeCoordinationBackend backend;
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(backend, MakeTopology(TOPOLOGY_VERSION_1)));
    HashAlgorithm algorithm;
    TopologyEngine engine(backend, algorithm);
    DS_ASSERT_OK(engine.Start());

    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(backend, MakeTopology(TOPOLOGY_VERSION_2)));
    int64_t version = 0;
    DS_ASSERT_OK(GetRoutingVersion(engine, version));
    EXPECT_EQ(version, TOPOLOGY_VERSION_2);

    CoordinationEvent staleEvent;
    DS_ASSERT_OK(MakeCommittedTopologyEvent(MakeTopology(TOPOLOGY_VERSION_1), staleEvent));
    DS_ASSERT_OK(engine.HandleCoordinationEvent(std::move(staleEvent)));
    DS_ASSERT_OK(GetRoutingVersion(engine, version));
    EXPECT_EQ(version, TOPOLOGY_VERSION_2);

    TopologyDiagnosticsSnapshot diagnostics;
    DS_ASSERT_OK(engine.Diagnostics().GetSnapshot(diagnostics));
    EXPECT_EQ(diagnostics.eventPump.receivedEvents, 2);
    EXPECT_EQ(diagnostics.eventPump.appliedEvents, 2);
    EXPECT_EQ(diagnostics.eventPump.outOfOrderEvents, 1);
    EXPECT_EQ(diagnostics.rebuild.successfulRebuilds, 3);
    EXPECT_EQ(diagnostics.rebuild.lastTopologyVersion, TOPOLOGY_VERSION_2);
}

TEST(TopologyEngineTest, EventPumpRecordsGapFailureWithoutRebuildHandler)
{
    CoordinationEventPump eventPump;
    uint64_t handledEvents = 0;
    DS_ASSERT_OK(eventPump.Start([&handledEvents](CoordinationEvent &&) {
        ++handledEvents;
        return Status::OK();
    }));
    CoordinationEvent firstEvent;
    firstEvent.revision = TOPOLOGY_VERSION_1;
    DS_ASSERT_OK(eventPump.Submit(std::move(firstEvent)));

    CoordinationEvent gapEvent;
    gapEvent.revision = TOPOLOGY_VERSION_3;
    EXPECT_EQ(eventPump.Submit(std::move(gapEvent)).GetCode(), K_TRY_AGAIN);

    auto stats = eventPump.GetStats();
    EXPECT_EQ(handledEvents, 1);
    EXPECT_EQ(stats.gapEvents, 1);
    EXPECT_EQ(stats.failedEvents, 1);
    EXPECT_EQ(stats.lastStatus.GetCode(), K_TRY_AGAIN);
}

TEST(TopologyEngineTest, DiagnosticsExposeWatchGapAndRebuildFailure)
{
    FakeCoordinationBackend backend;
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(backend, MakeTopology(TOPOLOGY_VERSION_1)));
    HashAlgorithm algorithm;
    TopologyEngine engine(backend, algorithm);
    DS_ASSERT_OK(engine.Start());

    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(backend, MakeTopology(TOPOLOGY_VERSION_2)));

    CoordinationEvent gapEvent;
    DS_ASSERT_OK(MakeCommittedTopologyEvent(MakeTopology(TOPOLOGY_VERSION_3), gapEvent));
    gapEvent.revision = GAP_EVENT_REVISION;
    DS_ASSERT_OK(engine.HandleCoordinationEvent(std::move(gapEvent)));
    int64_t version = 0;
    DS_ASSERT_OK(GetRoutingVersion(engine, version));
    EXPECT_EQ(version, TOPOLOGY_VERSION_2);

    CoordinationEvent badEvent;
    badEvent.type = CoordinationEventType::PUT;
    badEvent.key = TopologyKeyHelper::CommittedTopologyKey();
    badEvent.revision = GAP_EVENT_REVISION + 1;
    badEvent.value = "bad topology payload";
    EXPECT_EQ(engine.HandleCoordinationEvent(std::move(badEvent)).GetCode(), K_INVALID);

    TopologyDiagnosticsSnapshot diagnostics;
    DS_ASSERT_OK(engine.Diagnostics().GetSnapshot(diagnostics));
    EXPECT_EQ(diagnostics.eventPump.gapEvents, 1);
    EXPECT_EQ(diagnostics.rebuild.failedRebuilds, 1);
    EXPECT_EQ(diagnostics.rebuild.lastStatus.GetCode(), K_INVALID);
}

TEST(TopologyEngineTest, ShutdownDetachesBackendAndIgnoresLateEvents)
{
    FakeCoordinationBackend backend;
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(backend, MakeTopology(TOPOLOGY_VERSION_1)));
    HashAlgorithm algorithm;
    TopologyEngine engine(backend, algorithm);
    DS_ASSERT_OK(engine.Start());

    engine.Shutdown();
    EXPECT_EQ(engine.GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(backend.HasEventHandlerForTest());

    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(backend, MakeTopology(TOPOLOGY_VERSION_2)));
    int64_t version = 0;
    DS_ASSERT_OK(GetRoutingVersion(engine, version));
    EXPECT_EQ(version, TOPOLOGY_VERSION_1);

    CoordinationEvent lateEvent;
    DS_ASSERT_OK(MakeCommittedTopologyEvent(MakeTopology(TOPOLOGY_VERSION_2), lateEvent));
    EXPECT_EQ(engine.HandleCoordinationEvent(std::move(lateEvent)).GetCode(), K_NOT_READY);

    TopologyDiagnosticsSnapshot diagnostics;
    DS_ASSERT_OK(engine.Diagnostics().GetSnapshot(diagnostics));
    EXPECT_FALSE(diagnostics.eventPump.running);
    EXPECT_EQ(diagnostics.eventPump.ignoredEvents, 1);
}

}  // namespace topology
}  // namespace datasystem
