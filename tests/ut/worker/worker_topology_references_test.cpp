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
 * Description: Worker cluster-topology reference tests.
 */
#include "datasystem/worker/worker_topology_references.h"

#include <limits>

#include "gtest/gtest.h"

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "ut/common.h"

namespace datasystem::worker {
namespace {
constexpr char SOURCE_ADDRESS[] = "127.0.0.1:1";
constexpr char TARGET_ADDRESS[] = "127.0.0.1:2";
constexpr char LEAVING_PEER_ADDRESS[] = "127.0.0.1:3";
constexpr char ACTIVE_PEER_ADDRESS[] = "127.0.0.1:4";

std::shared_ptr<const cluster::TopologySnapshot> MakeScaleOutSnapshot(uint64_t version, uint64_t epoch,
                                                                      uint32_t sourceToken = 0,
                                                                      uint32_t targetToken = 1)
{
    cluster::TopologyState state;
    state.version = version;
    state.clusterHasInit = true;
    state.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_OUT, epoch };
    state.members = {
        cluster::Member{ { std::string(16, 'a'), SOURCE_ADDRESS }, cluster::MemberState::ACTIVE, { sourceToken } },
        cluster::Member{ { std::string(16, 'b'), TARGET_ADDRESS }, cluster::MemberState::JOINING, { targetToken } },
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    EXPECT_TRUE(cluster::TopologySnapshot::Create(std::move(state), version, std::string(64, 'a'), snapshot).IsOk());
    return snapshot;
}

std::shared_ptr<const cluster::TopologySnapshot> MakeScaleInSnapshot()
{
    cluster::TopologyState state;
    state.version = 2;
    state.clusterHasInit = true;
    state.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_IN, 2 };
    state.members = {
        cluster::Member{ { std::string(16, 'a'), SOURCE_ADDRESS }, cluster::MemberState::ACTIVE, { 0 } },
        cluster::Member{ { std::string(16, 'b'), TARGET_ADDRESS }, cluster::MemberState::LEAVING, { 1 } },
        cluster::Member{ { std::string(16, 'c'), LEAVING_PEER_ADDRESS }, cluster::MemberState::LEAVING, { 2 } },
        cluster::Member{ { std::string(16, 'd'), ACTIVE_PEER_ADDRESS }, cluster::MemberState::ACTIVE, { 3 } },
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    EXPECT_TRUE(cluster::TopologySnapshot::Create(std::move(state), 2, std::string(64, 'b'), snapshot).IsOk());
    return snapshot;
}

TEST(WorkerTopologyReferencesTest, RejectsDelayedOrIncompleteMigrationRpcFence)
{
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(MakeScaleOutSnapshot(2, 2), outcome));
    cluster::MembershipEndpointView membership(snapshots);
    WorkerTopologyReferences references;
    references.membership = &membership;
    references.localAddress = TARGET_ADDRESS;

    DS_ASSERT_OK(ValidateTopologyMigrationRequest(&references, 2, 2, std::string(16, 'a'),
                                                  std::string(16, 'b'), SOURCE_ADDRESS));
    EXPECT_EQ(ValidateTopologyMigrationRequest(&references, 2, 0, std::string(16, 'a'),
                                               std::string(16, 'b'), SOURCE_ADDRESS)
                  .GetCode(),
              K_INVALID);
    EXPECT_EQ(ValidateTopologyMigrationRequest(&references, 3, 3, std::string(16, 'a'),
                                               std::string(16, 'b'), SOURCE_ADDRESS)
                  .GetCode(),
              K_TRY_AGAIN);

    DS_ASSERT_OK(snapshots.Publish(MakeScaleOutSnapshot(3, 3), outcome));
    EXPECT_EQ(ValidateTopologyMigrationRequest(&references, 2, 2, std::string(16, 'a'),
                                               std::string(16, 'b'), SOURCE_ADDRESS)
                  .GetCode(),
              K_INVALID);
    DS_ASSERT_OK(ValidateTopologyMigrationRequest(nullptr, 0, 0, "", "", SOURCE_ADDRESS));
}

TEST(WorkerTopologyReferencesTest, StructuredRedirectDecisionDefersScaleOutMovingRange)
{
    constexpr char movingKey[] = "scaleout-moving-key";
    cluster::HashAlgorithm algorithm;
    const auto targetToken = algorithm.Hash(movingKey);
    const auto sourceToken = targetToken == std::numeric_limits<uint32_t>::max() ? 0 : targetToken + 1;
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(MakeScaleOutSnapshot(2, 2, sourceToken, targetToken), outcome));
    cluster::PlacementFacade placement(snapshots, algorithm);
    WorkerTopologyReferences references;
    references.placement = &placement;
    references.localAddress = SOURCE_ADDRESS;

    bool redirect = true;
    bool moving = false;
    std::string targetAddress;
    uint64_t topologyVersion = 0;
    DS_ASSERT_OK(EvaluateTopologyRedirect(&references, movingKey, redirect, moving, targetAddress, &topologyVersion));
    EXPECT_FALSE(redirect);
    EXPECT_TRUE(moving);
    EXPECT_EQ(targetAddress, SOURCE_ADDRESS);
    EXPECT_EQ(topologyVersion, 2u);
}

TEST(WorkerTopologyReferencesTest, ScaleInStandbySelectionSkipsEveryLeavingMember)
{
    cluster::TopologySnapshotState snapshots;
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(MakeScaleInSnapshot(), outcome));
    cluster::MembershipEndpointView membership(snapshots);
    WorkerTopologyReferences references;
    references.membership = &membership;

    std::string standby;
    DS_ASSERT_OK(GetActiveStandbyTopologyMember(&references, SOURCE_ADDRESS, standby));
    EXPECT_EQ(standby, ACTIVE_PEER_ADDRESS);
    DS_ASSERT_OK(GetActiveStandbyTopologyMember(&references, TARGET_ADDRESS, standby));
    EXPECT_EQ(standby, ACTIVE_PEER_ADDRESS);
    DS_ASSERT_OK(GetActiveStandbyTopologyMember(&references, ACTIVE_PEER_ADDRESS, standby));
    EXPECT_EQ(standby, SOURCE_ADDRESS);
}
}  // namespace
}  // namespace datasystem::worker
