/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Pure cluster topology state-transition builder tests.
 */
#include "datasystem/cluster/control/topology_plan_builder.h"

#include <set>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/raii.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

Member MakeControlMember(char id, std::string address, MemberState state, std::vector<uint32_t> tokens = {})
{
    return { { std::string(16, id), std::move(address) }, state, std::move(tokens) };
}

std::set<std::string> FailureSources(const TopologyPlan &plan)
{
    std::set<std::string> sources;
    for (const auto &change : plan.ownerChanges) {
        EXPECT_TRUE(change.source.has_value());
        if (change.source.has_value()) {
            sources.insert(change.source->address);
        }
    }
    return sources;
}

TEST(TopologyPlanBuilderTest, BootstrapsMultipleInitialMembersWithConfiguredTokenCount)
{
    const auto savedTokensPerMember = FLAGS_hash_ring_tokens_per_member;
    Raii restore([savedTokensPerMember] { FLAGS_hash_ring_tokens_per_member = savedTokensPerMember; });
    FLAGS_hash_ring_tokens_per_member = 64;
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
    EXPECT_EQ(next.members[0].tokens.size(), 64);
    EXPECT_EQ(next.members[1].tokens.size(), 64);
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

TEST(TopologyPlanBuilderTest, FailurePreemptsThreeMemberScaleInWithTwoCrashes)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 4;
    latest.clusterHasInit = true;
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::PRE_LEAVING, { 10, 110 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::PRE_LEAVING, { 30, 130 }),
                       MakeControlMember('c', "127.0.0.1:3", MemberState::PRE_LEAVING, { 50, 150 }),
                       MakeControlMember('d', "127.0.0.1:4", MemberState::ACTIVE, { 70, 170 }),
                       MakeControlMember('e', "127.0.0.1:5", MemberState::ACTIVE, { 90, 190 }) };
    TopologyPlan scaleIn;
    DS_ASSERT_OK(builder.BuildScaleInStart(
        latest, { latest.members[0].identity, latest.members[1].identity, latest.members[2].identity }, scaleIn));
    TopologyPlan failure;
    DS_ASSERT_OK(builder.BuildFailureStartOrReplan(
        scaleIn.next, { scaleIn.next.members[0].identity, scaleIn.next.members[1].identity }, failure));
    EXPECT_EQ(failure.next.members[0].state, MemberState::FAILED);
    EXPECT_EQ(failure.next.members[1].state, MemberState::FAILED);
    EXPECT_EQ(failure.next.members[2].state, MemberState::LEAVING);
    const std::set<std::string> confirmed{ latest.members[0].identity.address, latest.members[1].identity.address };
    const auto sources = FailureSources(failure);
    ASSERT_FALSE(sources.empty());
    for (const auto &source : sources) {
        EXPECT_EQ(confirmed.count(source), 1);
    }
}

TEST(TopologyPlanBuilderTest, FailurePreemptsMixedScaleOutAndPendingScaleIn)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 4;
    latest.clusterHasInit = true;
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10, 110 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::ACTIVE, { 60, 160 }),
                       MakeControlMember('c', "127.0.0.1:3", MemberState::PRE_LEAVING, { 30, 130 }),
                       MakeControlMember('d', "127.0.0.1:4", MemberState::PRE_LEAVING, { 90, 190 }),
                       MakeControlMember('e', "127.0.0.1:5", MemberState::INITIAL),
                       MakeControlMember('f', "127.0.0.1:6", MemberState::INITIAL) };
    TopologyPlan scaleOut;
    DS_ASSERT_OK(
        builder.BuildScaleOutStart(latest, { latest.members[4].identity, latest.members[5].identity }, scaleOut));

    for (const auto failedIndex : { 0ul, 2ul }) {
        SCOPED_TRACE(::testing::Message() << "failed_index=" << failedIndex);
        TopologyPlan failure;
        DS_ASSERT_OK(
            builder.BuildFailureStartOrReplan(scaleOut.next, { scaleOut.next.members[failedIndex].identity }, failure));
        const auto sources = FailureSources(failure);
        ASSERT_FALSE(sources.empty());
        for (const auto &source : sources) {
            EXPECT_EQ(source, scaleOut.next.members[failedIndex].identity.address);
        }
        EXPECT_EQ(failure.next.members[4].state, MemberState::JOINING);
        EXPECT_EQ(failure.next.members[5].state, MemberState::JOINING);
        const size_t unaffectedPreLeaving = failedIndex == 2 ? 3 : 2;
        EXPECT_EQ(failure.next.members[unaffectedPreLeaving].state, MemberState::PRE_LEAVING);
    }
}

TEST(TopologyPlanBuilderTest, FailureReplanKeepsExistingFailedAndMixedOrdinaryFacts)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState latest;
    latest.version = 7;
    latest.clusterHasInit = true;
    latest.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 7 };
    latest.members = { MakeControlMember('a', "127.0.0.1:1", MemberState::FAILED, { 10 }),
                       MakeControlMember('b', "127.0.0.1:2", MemberState::LEAVING, { 30 }),
                       MakeControlMember('c', "127.0.0.1:3", MemberState::INITIAL),
                       MakeControlMember('d', "127.0.0.1:4", MemberState::JOINING, { 50 }),
                       MakeControlMember('e', "127.0.0.1:5", MemberState::PRE_LEAVING, { 70 }),
                       MakeControlMember('f', "127.0.0.1:6", MemberState::LEAVING, { 90 }),
                       MakeControlMember('g', "127.0.0.1:7", MemberState::ACTIVE, { 110 }),
                       MakeControlMember('h', "127.0.0.1:8", MemberState::ACTIVE, { 130 }) };
    TopologyPlan failure;
    DS_ASSERT_OK(
        builder.BuildFailureStartOrReplan(latest, { latest.members[0].identity, latest.members[1].identity }, failure));
    EXPECT_EQ(failure.next.members[0].state, MemberState::FAILED);
    EXPECT_EQ(failure.next.members[1].state, MemberState::FAILED);
    EXPECT_EQ(failure.next.members[2].state, MemberState::INITIAL);
    EXPECT_EQ(failure.next.members[3].state, MemberState::JOINING);
    EXPECT_EQ(failure.next.members[4].state, MemberState::PRE_LEAVING);
    EXPECT_EQ(failure.next.members[5].state, MemberState::LEAVING);
    EXPECT_EQ(failure.next.members[6].state, MemberState::ACTIVE);
    EXPECT_EQ(failure.next.members[7].state, MemberState::ACTIVE);
    const std::set<std::string> confirmed{ latest.members[0].identity.address, latest.members[1].identity.address };
    const auto sources = FailureSources(failure);
    ASSERT_FALSE(sources.empty());
    for (const auto &source : sources) {
        EXPECT_EQ(confirmed.count(source), 1);
    }
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
