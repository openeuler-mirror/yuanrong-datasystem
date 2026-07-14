/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Foreground immutable cluster placement facade tests.
 */
#include "datasystem/cluster/routing/placement_facade.h"

#include <limits>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

TEST(PlacementFacadeTest, UsesOneSnapshotForTokenBatchAndRedirectDecisions)
{
    TopologyState topology;
    topology.version = 1;
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 10, 100 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 50, 150 } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 1, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    HashAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm);
    PlacementDecision placement;
    DS_ASSERT_OK(facade.LocateToken(40, placement));
    EXPECT_EQ(placement.committedOwnerAddress, "127.0.0.1:2");
    RedirectDecision redirect;
    DS_ASSERT_OK(facade.EvaluateRedirect("key", "127.0.0.1:1", redirect));
    EXPECT_EQ(redirect.topologyVersion, 1);
    std::vector<std::string_view> keys{ "a", "b", "c" };
    BatchPlacementDecision batch;
    DS_ASSERT_OK(facade.LocateBatch(keys, batch));
    EXPECT_EQ(batch.decisions.size(), keys.size());
    EXPECT_EQ(batch.topologyVersion, 1);
}

TEST(PlacementFacadeTest, WaitsConcurrentWritesOnScaleOutTransferRanges)
{
    TopologyState topology;
    topology.version = 2;
    topology.clusterHasInit = true;
    topology.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    topology.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 0 } },
        Member{
            { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { std::numeric_limits<uint32_t>::max() } },
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(topology, 2, std::string(64, 'a'), snapshot));
    TopologySnapshotState snapshots;
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    HashAlgorithm algorithm;
    PlacementFacade facade(snapshots, algorithm);

    RedirectDecision decision;
    DS_ASSERT_OK(facade.EvaluateRedirect("key", "127.0.0.1:1", decision));
    EXPECT_EQ(decision.action, RedirectAction::WAIT);
    EXPECT_EQ(decision.committedOwnerAddress, "127.0.0.1:1");
}

}  // namespace
}  // namespace datasystem::cluster
