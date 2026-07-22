/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Pure cluster topology state-transition builder tests.
 */
#include "datasystem/cluster/control/topology_plan_builder.h"

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

Member MakeControlMember(char id, std::string address, MemberState state, std::vector<uint32_t> tokens = {})
{
    return { { std::string(16, id), std::move(address) }, state, std::move(tokens) };
}

TEST(TopologyPlanBuilderTest, BootstrapsMultipleInitialMembersInOneVersion)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 1;
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::INITIAL),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::INITIAL) };
    TopologyState next;
    DS_ASSERT_OK(builder.BuildBootstrap(latest, { latest.members[0].identity, latest.members[1].identity }, next));
    EXPECT_EQ(next.version, 2);
    EXPECT_TRUE(next.clusterHasInit);
    ASSERT_EQ(next.members.size(), 2);
    EXPECT_EQ(next.members[0].state, MemberState::ACTIVE);
    EXPECT_EQ(next.members[1].state, MemberState::ACTIVE);
}

TEST(TopologyPlanBuilderTest, StartsAndFinalizesOneMultiMemberScaleOutBatch)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 2;
    latest.clusterHasInit = true;
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10, 100 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::INITIAL),
                       MakeControlMember('c', "127.0.0.1:3", MemberState::INITIAL) };
    TopologyPlan plan;
    DS_ASSERT_OK(builder.BuildScaleOutStart(latest, { latest.members[1].identity, latest.members[2].identity }, plan));
    ASSERT_TRUE(plan.next.activeBatch.has_value());
    EXPECT_EQ(plan.next.activeBatch->type, TopologyChangeType::SCALE_OUT);
    EXPECT_EQ(plan.next.activeBatch->epoch, 3);
    EXPECT_EQ(plan.next.members[1].state, MemberState::JOINING);
    EXPECT_EQ(plan.next.members[2].state, MemberState::JOINING);
    TopologyState final;
    DS_ASSERT_OK(builder.BuildScaleOutFinal(plan.next, final));
    EXPECT_FALSE(final.activeBatch.has_value());
    EXPECT_TRUE(std::all_of(final.members.begin(), final.members.end(),
                            [](const auto &member) { return member.state == MemberState::ACTIVE; }));
}

TEST(TopologyPlanBuilderTest, ScaleOutReplanDropsFailedJoiningAndKeepsRemainingJoiningBatch)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 4;
    latest.clusterHasInit = true;
    latest.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 4 };
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10, 100 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::JOINING, { 50, 150 }),
                       MakeControlMember('c', "127.0.0.1:3", MemberState::JOINING, { 75, 175 }) };

    TopologyPlan replan;
    DS_ASSERT_OK(builder.BuildScaleOutReplan(latest, { latest.members[1].identity }, replan));

    EXPECT_EQ(replan.next.version, 5);
    ASSERT_TRUE(replan.next.activeBatch.has_value());
    EXPECT_EQ(replan.next.activeBatch->type, TopologyChangeType::SCALE_OUT);
    ASSERT_EQ(replan.next.members.size(), 2);
    EXPECT_EQ(replan.next.members[0].identity.address, "127.0.0.1:1");
    EXPECT_EQ(replan.next.members[0].state, MemberState::ACTIVE);
    EXPECT_EQ(replan.next.members[1].identity.address, "127.0.0.1:3");
    EXPECT_EQ(replan.next.members[1].state, MemberState::JOINING);
    EXPECT_FALSE(replan.ownerChanges.empty());
    for (const auto &change : replan.ownerChanges) {
        ASSERT_TRUE(change.source.has_value());
        EXPECT_EQ(change.source->address, "127.0.0.1:1");
        EXPECT_EQ(change.target.address, "127.0.0.1:3");
    }
}

TEST(TopologyPlanBuilderTest, FailurePreemptsOrdinaryBatchWithoutRollingBackJoiningFacts)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 4;
    latest.clusterHasInit = true;
    latest.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 4 };
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10, 100 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::ACTIVE, { 50, 150 }),
                       MakeControlMember('c', "127.0.0.1:3", MemberState::JOINING, { 75, 175 }) };
    TopologyPlan failure;
    DS_ASSERT_OK(builder.BuildFailureStartOrReplan(latest, { latest.members.front().identity }, failure));
    EXPECT_EQ(failure.next.version, 5);
    ASSERT_TRUE(failure.next.activeBatch.has_value());
    EXPECT_EQ(failure.next.activeBatch->type, TopologyChangeType::FAILURE);
    EXPECT_EQ(failure.next.members.front().state, MemberState::FAILED);
    EXPECT_EQ(failure.next.members.back().state, MemberState::JOINING);
    EXPECT_EQ(failure.next.members.back().tokens, latest.members.back().tokens);
}

TEST(TopologyPlanBuilderTest, FailureMarksSelectedLeavingMemberAndPreservesOnlyUnaffectedLeavingFacts)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 4;
    latest.clusterHasInit = true;
    latest.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, 4 };
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::LEAVING, { 10, 100 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::ACTIVE, { 50, 150 }),
                       MakeControlMember('c', "127.0.0.1:3", MemberState::LEAVING, { 75, 175 }) };
    TopologyPlan failure;
    DS_ASSERT_OK(builder.BuildFailureStartOrReplan(latest, { latest.members.front().identity }, failure));
    EXPECT_EQ(failure.next.members.front().state, MemberState::FAILED);
    EXPECT_EQ(failure.next.members.back().state, MemberState::LEAVING);
    ASSERT_TRUE(failure.next.activeBatch.has_value());
    EXPECT_EQ(failure.next.activeBatch->type, TopologyChangeType::FAILURE);
}

TEST(TopologyPlanBuilderTest, ScaleInSourceStaysLeavingWhenPeerFails)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 4;
    latest.clusterHasInit = true;
    latest.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, 4 };
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::LEAVING, { 10, 100 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::ACTIVE, { 50, 150 }),
                       MakeControlMember('c', "127.0.0.1:3", MemberState::ACTIVE, { 75, 175 }) };

    TopologyPlan failure;
    DS_ASSERT_OK(builder.BuildFailureStartOrReplan(latest, { latest.members[1].identity }, failure));

    EXPECT_EQ(failure.next.version, 5);
    ASSERT_TRUE(failure.next.activeBatch.has_value());
    EXPECT_EQ(failure.next.activeBatch->type, TopologyChangeType::FAILURE);
    EXPECT_EQ(failure.next.members[0].state, MemberState::LEAVING);
    EXPECT_EQ(failure.next.members[1].state, MemberState::FAILED);
    EXPECT_EQ(failure.next.members[2].state, MemberState::ACTIVE);
}

TEST(TopologyPlanBuilderTest, ClusterShutdownClearsTopologyWithoutTasks)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 7;
    latest.clusterHasInit = true;
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::PRE_LEAVING, { 1 }) };
    TopologyState next;
    DS_ASSERT_OK(builder.BuildClusterShutdownFinal(latest, next));
    EXPECT_EQ(next.version, 8);
    EXPECT_TRUE(next.members.empty());
    EXPECT_FALSE(next.activeBatch.has_value());
}

TEST(TopologyPlanBuilderTest, LastCommittedFailureHasNoRecoveryTaskAndStillHeals)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 3;
    latest.clusterHasInit = true;
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10, 100 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::JOINING, { 50, 150 }) };
    latest.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 3 };
    TopologyPlan failure;
    DS_ASSERT_OK(builder.BuildFailureStartOrReplan(latest, { latest.members.front().identity }, failure));
    EXPECT_TRUE(failure.ownerChanges.empty());
    ASSERT_TRUE(failure.next.activeBatch.has_value());
    EXPECT_EQ(failure.next.activeBatch->type, TopologyChangeType::FAILURE);
    TopologyState final;
    DS_ASSERT_OK(builder.BuildFailureFinal(failure.next, final));
    EXPECT_TRUE(final.members.empty());
    EXPECT_FALSE(final.activeBatch.has_value());
    final.members.push_back(MakeControlMember('c', "127.0.0.1:3", MemberState::INITIAL));
    TopologyState recovered;
    DS_ASSERT_OK(builder.BuildBootstrap(final, { final.members.front().identity }, recovered));
    EXPECT_TRUE(recovered.clusterHasInit);
    EXPECT_EQ(recovered.members.front().state, MemberState::ACTIVE);
}

}  // namespace
}  // namespace datasystem::cluster
