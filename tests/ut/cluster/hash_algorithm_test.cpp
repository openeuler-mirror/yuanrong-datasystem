/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Built-in cluster hash routing and planning algorithm tests.
 */
#include "datasystem/cluster/algorithm/hash_algorithm.h"

#include <chrono>
#include <set>

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr uint32_t LARGE_ORDINARY_BATCH_SIZE = 2'500;
constexpr uint32_t LARGE_BATCH_PORT_BASE = 30'000;

MemberIdentity MakeIndexedIdentity(uint32_t index, uint32_t portBase)
{
    std::string id(16, '\0');
    std::copy_n(reinterpret_cast<const char *>(&index), sizeof(index), id.begin());
    return { std::move(id), "127.0.0.1:" + std::to_string(portBase + index) };
}

TEST(HashAlgorithmTest, LocatesCommittedOwnerWithRingWrapAndSkipsJoining)
{
    TopologyState state;
    state.version = 3;
    state.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 10, 100 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { 50 } },
    };
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 3 };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    HashAlgorithm algorithm;
    const Member *owner = nullptr;

    DS_ASSERT_OK(algorithm.LocateOwner(*snapshot, 40, owner));
    ASSERT_NE(owner, nullptr);
    EXPECT_EQ(owner->identity.address, "127.0.0.1:1");
    DS_ASSERT_OK(algorithm.LocateOwner(*snapshot, 101, owner));
    ASSERT_NE(owner, nullptr);
    EXPECT_EQ(owner->identity.address, "127.0.0.1:1");
}

TEST(HashAlgorithmTest, BootstrapAllocatesFourUniqueDeterministicTokensPerMember)
{
    HashAlgorithm algorithm;
    ScaleOutPlanInput input;
    input.joining = { { std::string(16, 'b'), "127.0.0.1:2" }, { std::string(16, 'a'), "127.0.0.1:1" } };
    TopologyPlan first;
    TopologyPlan second;
    DS_ASSERT_OK(algorithm.BuildInitialPlacement(input, first));
    DS_ASSERT_OK(algorithm.BuildInitialPlacement(input, second));
    ASSERT_EQ(first.next.members.size(), 2);
    std::set<uint32_t> tokens;
    for (const auto &member : first.next.members) {
        EXPECT_EQ(member.tokens.size(), 4);
        tokens.insert(member.tokens.begin(), member.tokens.end());
    }
    EXPECT_EQ(tokens.size(), 8);
    EXPECT_EQ(first.next.members[0].tokens, second.next.members[0].tokens);
}

TEST(HashAlgorithmTest, PlansMultiMemberScaleOutAsOneDeterministicOwnerChangeSet)
{
    HashAlgorithm algorithm;
    ScaleOutPlanInput input;
    input.current.clusterHasInit = true;
    input.current.version = 1;
    input.current.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 10, 100 } },
    };
    input.joining = { { std::string(16, 'c'), "127.0.0.1:3" }, { std::string(16, 'b'), "127.0.0.1:2" } };
    TopologyPlan first;
    TopologyPlan second;
    DS_ASSERT_OK(algorithm.PlanScaleOut(input, first));
    DS_ASSERT_OK(algorithm.PlanScaleOut(input, second));
    ASSERT_EQ(first.next.members.size(), 3);
    EXPECT_FALSE(first.ownerChanges.empty());
    EXPECT_EQ(first.ownerChanges.size(), second.ownerChanges.size());
    EXPECT_EQ(first.next.members[1].tokens, second.next.members[1].tokens);
}

TEST(HashAlgorithmTest, PlansScaleInAndFailureWithoutChangingCurrentCommittedTokens)
{
    HashAlgorithm algorithm;
    TopologyState current;
    current.clusterHasInit = true;
    current.version = 1;
    current.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 10, 100 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 50, 150 } },
    };
    TopologyPlan scaleIn;
    DS_ASSERT_OK(algorithm.PlanScaleIn({ current, { current.members.front().identity } }, scaleIn));
    EXPECT_EQ(scaleIn.next.members.front().state, MemberState::PRE_LEAVING);
    EXPECT_EQ(scaleIn.next.members.front().tokens, current.members.front().tokens);
    EXPECT_FALSE(scaleIn.ownerChanges.empty());
    TopologyPlan failure;
    DS_ASSERT_OK(algorithm.PlanFailure({ current, { current.members.back().identity } }, failure));
    EXPECT_EQ(failure.next.members.back().state, MemberState::FAILED);
    EXPECT_EQ(failure.next.members.back().tokens, current.members.back().tokens);
    EXPECT_FALSE(failure.ownerChanges.empty());
}

TEST(HashAlgorithmTest, LocatesOneThousandMemberSnapshotWithoutPerLookupRebuild)
{
    constexpr uint32_t memberCount = 1'000;
    constexpr uint32_t tokensPerMember = 4;
    TopologyState state;
    state.version = 1;
    state.clusterHasInit = true;
    state.members.reserve(memberCount);
    for (uint32_t index = 0; index < memberCount; ++index) {
        std::string id(16, '\0');
        std::copy_n(reinterpret_cast<const char *>(&index), sizeof(index), id.begin());
        Member member{ { std::move(id), "127.0.0.1:" + std::to_string(index + 1) }, MemberState::ACTIVE, {} };
        for (uint32_t token = 0; token < tokensPerMember; ++token) {
            member.tokens.push_back(index * tokensPerMember + token);
        }
        state.members.emplace_back(std::move(member));
    }
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(std::move(state), 1, std::string(64, 'a'), snapshot));
    HashAlgorithm algorithm;
    const Member *owner = nullptr;
    const auto started = std::chrono::steady_clock::now();
    for (uint32_t token = 0; token < memberCount; ++token) {
        DS_ASSERT_OK(algorithm.LocateOwner(*snapshot, token, owner));
        ASSERT_NE(owner, nullptr);
    }
    EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(1));
}

TEST(HashAlgorithmTest, PlansTwentyFiveHundredMembersInOneOrdinaryScaleOutBatch)
{
    constexpr uint32_t tokensPerMember = 4;
    HashAlgorithm algorithm;
    ScaleOutPlanInput input;
    input.current.clusterHasInit = true;
    input.current.version = 1;
    input.current.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1, 2, 3, 4 } },
    };
    input.tokensPerMember = tokensPerMember;
    input.joining.reserve(LARGE_ORDINARY_BATCH_SIZE);
    for (uint32_t index = 0; index < LARGE_ORDINARY_BATCH_SIZE; ++index) {
        input.joining.emplace_back(MakeIndexedIdentity(index, LARGE_BATCH_PORT_BASE));
    }
    TopologyPlan plan;
    DS_ASSERT_OK(algorithm.PlanScaleOut(input, plan));
    EXPECT_EQ(plan.next.members.size(), LARGE_ORDINARY_BATCH_SIZE + 1);
    EXPECT_EQ(std::count_if(plan.next.members.begin(), plan.next.members.end(),
                            [](const auto &member) { return member.state == MemberState::JOINING; }),
              LARGE_ORDINARY_BATCH_SIZE);
    EXPECT_FALSE(plan.ownerChanges.empty());
}

}  // namespace
}  // namespace datasystem::cluster
