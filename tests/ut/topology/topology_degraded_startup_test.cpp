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
 * Description: Topology degraded startup unit tests.
 */
#include "datasystem/topology/runtime/topology_degraded_startup_checker.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/util/file_util.h"
#include "datasystem/topology/runtime/topology_local_snapshot_store.h"
#include "datasystem/topology/runtime/topology_state_digest.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/testing/topology_test_utils.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char NODE_A[] = "node-a";
constexpr char NODE_B[] = "node-b";
constexpr char NODE_C[] = "node-c";
constexpr char NODE_D[] = "node-d";
constexpr uint32_t NODE_A_TOKEN = 100;
constexpr uint32_t NODE_B_TOKEN = 200;
constexpr uint32_t NODE_C_TOKEN = 300;
constexpr uint32_t NODE_D_TOKEN = 400;
constexpr int64_t VERSION_1 = 1;
constexpr Revision REVISION_7 = 7;

TopologyDescriptor MakeTopology()
{
    return testing::MakeTopologyForTest({
        testing::MakeTopologyNodeForTest(NODE_A, { NODE_A_TOKEN }),
        testing::MakeTopologyNodeForTest(NODE_B, { NODE_B_TOKEN }),
    },
                                        VERSION_1);
}

TopologyDescriptor MakeFourNodeTopology()
{
    return testing::MakeTopologyForTest({
        testing::MakeTopologyNodeForTest(NODE_A, { NODE_A_TOKEN }),
        testing::MakeTopologyNodeForTest(NODE_B, { NODE_B_TOKEN }),
        testing::MakeTopologyNodeForTest(NODE_C, { NODE_C_TOKEN }),
        testing::MakeTopologyNodeForTest(NODE_D, { NODE_D_TOKEN }),
    },
                                        VERSION_1);
}

Status MakeLocalSnapshot(LocalTopologySnapshot &snapshot)
{
    snapshot = {};
    TopologyTaskSummary summary;
    summary.unfinishedTransferTasks = 1;
    return TopologyStateDigest::BuildSnapshot(MakeTopology(), REVISION_7, summary, snapshot);
}

PeerStateSnapshot MakePeerSnapshot(const LocalTopologySnapshot &localSnapshot)
{
    PeerStateSnapshot peer;
    peer.nodeId = NODE_B;
    peer.topologyVersion = localSnapshot.topology.version;
    peer.topologyRevision = localSnapshot.topologyRevision;
    peer.taskSummary = localSnapshot.taskSummary;
    peer.ready = true;
    peer.backendAvailable = false;
    peer.digest = localSnapshot.digest;
    return peer;
}

std::string MakeSnapshotPath()
{
    return ut::GetTestCaseDataDir() + "/topology.snapshot";
}

}  // namespace

class TopologyDegradedStartupTest : public ut::CommonTest {
};

TEST_F(TopologyDegradedStartupTest, LocalSnapshotRoundTripVerifiesDigest)
{
    const auto path = MakeSnapshotPath();
    FileTopologyLocalSnapshotStore store(path);
    LocalTopologySnapshot snapshot;
    DS_ASSERT_OK(MakeLocalSnapshot(snapshot));

    DS_ASSERT_OK(store.Save(snapshot));
    LocalTopologySnapshot loaded;
    DS_ASSERT_OK(store.Load(loaded));

    EXPECT_EQ(loaded.topology.version, snapshot.topology.version);
    EXPECT_EQ(loaded.topologyRevision, snapshot.topologyRevision);
    EXPECT_EQ(loaded.taskSummary.unfinishedTransferTasks, snapshot.taskSummary.unfinishedTransferTasks);
    EXPECT_EQ(loaded.digest, snapshot.digest);
}

TEST_F(TopologyDegradedStartupTest, SaveRejectsDigestMismatch)
{
    const auto path = MakeSnapshotPath();
    FileTopologyLocalSnapshotStore store(path);
    LocalTopologySnapshot snapshot;
    DS_ASSERT_OK(MakeLocalSnapshot(snapshot));
    snapshot.digest = "stale";

    EXPECT_EQ(store.Save(snapshot).GetCode(), K_INVALID);
}

TEST_F(TopologyDegradedStartupTest, CheckerRejectsDigestMismatch)
{
    LocalTopologySnapshot localSnapshot;
    DS_ASSERT_OK(MakeLocalSnapshot(localSnapshot));
    auto peer = MakePeerSnapshot(localSnapshot);
    localSnapshot.digest = "stale";
    TopologyDegradedStartupChecker checker;
    DegradedStartupDecision decision;

    EXPECT_EQ(checker.Check(localSnapshot, { peer }, decision).GetCode(), K_INVALID);
}

TEST_F(TopologyDegradedStartupTest, MissingOrCorruptLocalSnapshotIsRejected)
{
    const auto path = MakeSnapshotPath();
    FileTopologyLocalSnapshotStore store(path);
    LocalTopologySnapshot loaded;
    EXPECT_EQ(store.Load(loaded).GetCode(), K_NOT_FOUND);

    DS_ASSERT_OK(AtomicWriteTextFile(path, "schema=1\nrevision=1\n"));
    EXPECT_EQ(store.Load(loaded).GetCode(), K_INVALID);
}

TEST_F(TopologyDegradedStartupTest, AcceptsOnlyWhenAllReadyPeersMatchLocalDigest)
{
    LocalTopologySnapshot localSnapshot;
    DS_ASSERT_OK(MakeLocalSnapshot(localSnapshot));
    auto peer = MakePeerSnapshot(localSnapshot);
    TopologyDegradedStartupChecker checker;
    DegradedStartupDecision decision;

    DS_ASSERT_OK(checker.Check(localSnapshot, { peer }, decision));
    EXPECT_TRUE(decision.accepted);

    peer.digest = "different";
    DS_ASSERT_OK(checker.Check(localSnapshot, { peer }, decision));
    EXPECT_FALSE(decision.accepted);
}

TEST_F(TopologyDegradedStartupTest, RejectsNoPeerNotReadyPeerAndBackendAvailablePeer)
{
    LocalTopologySnapshot localSnapshot;
    DS_ASSERT_OK(MakeLocalSnapshot(localSnapshot));
    TopologyDegradedStartupChecker checker;
    DegradedStartupDecision decision;

    DS_ASSERT_OK(checker.Check(localSnapshot, {}, decision));
    EXPECT_FALSE(decision.accepted);

    auto peer = MakePeerSnapshot(localSnapshot);
    peer.ready = false;
    DS_ASSERT_OK(checker.Check(localSnapshot, { peer }, decision));
    EXPECT_FALSE(decision.accepted);

    peer = MakePeerSnapshot(localSnapshot);
    peer.backendAvailable = true;
    DS_ASSERT_OK(checker.Check(localSnapshot, { peer }, decision));
    EXPECT_FALSE(decision.accepted);
}

TEST_F(TopologyDegradedStartupTest, RejectsPeerVersionRevisionMismatchAndDuplicatePeer)
{
    LocalTopologySnapshot localSnapshot;
    DS_ASSERT_OK(MakeLocalSnapshot(localSnapshot));
    TopologyDegradedStartupChecker checker;
    DegradedStartupDecision decision;

    auto peer = MakePeerSnapshot(localSnapshot);
    ++peer.topologyVersion;
    DS_ASSERT_OK(checker.Check(localSnapshot, { peer }, decision));
    EXPECT_FALSE(decision.accepted);

    peer = MakePeerSnapshot(localSnapshot);
    ++peer.topologyRevision;
    DS_ASSERT_OK(checker.Check(localSnapshot, { peer }, decision));
    EXPECT_FALSE(decision.accepted);

    peer = MakePeerSnapshot(localSnapshot);
    DS_ASSERT_OK(checker.Check(localSnapshot, { peer, peer }, decision));
    EXPECT_FALSE(decision.accepted);
}

TEST_F(TopologyDegradedStartupTest, RejectsPeerOutsideMembershipAndPeerMinority)
{
    LocalTopologySnapshot localSnapshot;
    DS_ASSERT_OK(MakeLocalSnapshot(localSnapshot));
    TopologyDegradedStartupChecker checker;
    DegradedStartupDecision decision;

    auto peer = MakePeerSnapshot(localSnapshot);
    peer.nodeId = NODE_C;
    DS_ASSERT_OK(checker.Check(localSnapshot, { peer }, decision));
    EXPECT_FALSE(decision.accepted);

    TopologyTaskSummary summary;
    DS_ASSERT_OK(TopologyStateDigest::BuildSnapshot(MakeFourNodeTopology(), REVISION_7, summary, localSnapshot));
    peer = MakePeerSnapshot(localSnapshot);
    DS_ASSERT_OK(checker.Check(localSnapshot, { peer }, decision));
    EXPECT_FALSE(decision.accepted);
}

}  // namespace topology
}  // namespace datasystem
