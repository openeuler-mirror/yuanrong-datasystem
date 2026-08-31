/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Built-in cluster hash routing and planning algorithm tests.
 */
#include "datasystem/cluster/algorithm/hash_algorithm.h"

#include <array>
#include <chrono>
#include <cmath>
#include <iostream>
#include <map>
#include <set>
#include <tuple>

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr uint32_t LARGE_ORDINARY_BATCH_SIZE = 2'500;
constexpr uint32_t LARGE_BATCH_PORT_BASE = 30'000;
constexpr uint64_t HASH_RING_SIZE = uint64_t{ 1 } << 32;
constexpr std::array<uint32_t, 6> DISTRIBUTION_TOKEN_COUNTS{ 4, 8, 16, 32, 64, 128 };
constexpr std::array<uint32_t, 5> DISTRIBUTION_MEMBER_COUNTS{ 8, 32, 128, 512, 2'048 };

MemberIdentity MakeIndexedIdentity(uint32_t index, uint32_t portBase)
{
    std::string id(16, '\0');
    std::copy_n(reinterpret_cast<const char *>(&index), sizeof(index), id.begin());
    return { std::move(id), "127.0.0.1:" + std::to_string(portBase + index) };
}

Member MakePlanningMember(char id, uint32_t port, MemberState state, std::vector<uint32_t> tokens)
{
    return { { std::string(16, id), "127.0.0.1:" + std::to_string(port) }, state, std::move(tokens) };
}

void ExpectFailureOwnerContract(const TopologyPlan &plan, const std::set<std::string> &failedAddresses)
{
    ASSERT_FALSE(plan.ownerChanges.empty());
    for (const auto &change : plan.ownerChanges) {
        ASSERT_TRUE(change.source.has_value());
        EXPECT_EQ(failedAddresses.count(change.source->address), 1);
        const auto target = std::find_if(plan.next.members.begin(), plan.next.members.end(),
                                         [&](const auto &member) { return member.identity == change.target; });
        ASSERT_NE(target, plan.next.members.end());
        EXPECT_EQ(target->state, MemberState::ACTIVE);
        EXPECT_EQ(failedAddresses.count(target->identity.address), 0);
        EXPECT_FALSE(change.ranges.empty());
    }
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
        for (uint32_t index = 0; index < member.tokens.size(); ++index) {
            EXPECT_EQ(member.tokens[index], HashAlgorithm::MakeToken(member.identity.address, index, 0));
        }
    }
    EXPECT_EQ(tokens.size(), 8);
    EXPECT_EQ(first.next.members[0].tokens, second.next.members[0].tokens);
}

TEST(HashAlgorithmTest, TokenDerivationMatchesSchemaTwoGoldenValues)
{
    struct TokenGolden {
        uint32_t index;
        uint32_t seed;
        uint32_t token;
    };
    const std::array<TokenGolden, 5> goldenValues{
        TokenGolden{ 0, 0, 729'231'097 },
        TokenGolden{ 9, 1, 4'287'726'424 },
        TokenGolden{ 10, 0, 1'351'649'087 },
        TokenGolden{ 99, HashAlgorithm::MAX_TOKEN_SEEDS - 1, 1'603'672'655 },
        TokenGolden{ 100, 0, 2'123'156'085 },
    };
    const std::string address = "127.0.0.1:12345";
    std::vector<uint32_t> seeds(101);
    for (const auto &golden : goldenValues) {
        seeds[golden.index] = golden.seed;
        EXPECT_EQ(HashAlgorithm::MakeToken(address, golden.index, golden.seed), golden.token);
    }

    std::vector<uint32_t> tokens;
    HashAlgorithm::MakeTokens(address, seeds, tokens);

    ASSERT_EQ(tokens.size(), seeds.size());
    for (const auto &golden : goldenValues) {
        EXPECT_EQ(tokens[golden.index], golden.token);
    }
}

class HashAlgorithmDistributionTest : public ::testing::TestWithParam<std::tuple<uint32_t, uint32_t>> {
};

TEST_P(HashAlgorithmDistributionTest, ReportsTokenRangeDistribution)
{
    const auto [tokensPerMember, memberCount] = GetParam();
    HashAlgorithm algorithm;
    ScaleOutPlanInput input;
    input.tokensPerMember = tokensPerMember;
    input.joining.reserve(memberCount);
    for (uint32_t index = 0; index < memberCount; ++index) {
        input.joining.emplace_back(MakeIndexedIdentity(index, LARGE_BATCH_PORT_BASE));
    }
    TopologyPlan plan;
    DS_ASSERT_OK(algorithm.BuildInitialPlacement(input, plan));

    std::vector<std::pair<uint32_t, std::string>> ring;
    ring.reserve(memberCount * tokensPerMember);
    std::map<std::string, uint64_t> rangeSizeByAddress;
    for (const auto &member : plan.next.members) {
        ASSERT_EQ(member.tokens.size(), tokensPerMember);
        rangeSizeByAddress.emplace(member.identity.address, 0);
        for (const auto token : member.tokens) {
            ring.emplace_back(token, member.identity.address);
        }
    }
    std::sort(ring.begin(), ring.end());
    for (size_t index = 0; index < ring.size(); ++index) {
        const auto previousToken = index == 0 ? ring.back().first : ring[index - 1].first;
        const auto rangeSize = (HASH_RING_SIZE + ring[index].first - previousToken) % HASH_RING_SIZE;
        rangeSizeByAddress.at(ring[index].second) += rangeSize;
    }

    uint64_t totalRangeSize = 0;
    uint64_t minRangeSize = HASH_RING_SIZE;
    uint64_t maxRangeSize = 0;
    double squaredRelativeDeviationSum = 0;
    const auto idealRangeSize = static_cast<double>(HASH_RING_SIZE) / memberCount;
    for (const auto &entry : rangeSizeByAddress) {
        const auto rangeSize = entry.second;
        totalRangeSize += rangeSize;
        minRangeSize = std::min(minRangeSize, rangeSize);
        maxRangeSize = std::max(maxRangeSize, rangeSize);
        const auto relativeDeviation = (static_cast<double>(rangeSize) - idealRangeSize) / idealRangeSize;
        squaredRelativeDeviationSum += relativeDeviation * relativeDeviation;
    }
    const auto coefficientOfVariation = std::sqrt(squaredRelativeDeviationSum / static_cast<double>(memberCount));
    std::cout << "members=" << memberCount << " tokens_per_member=" << tokensPerMember
              << " min_share=" << 100.0 * static_cast<double>(minRangeSize) / HASH_RING_SIZE << "%"
              << " max_share=" << 100.0 * static_cast<double>(maxRangeSize) / HASH_RING_SIZE << "%"
              << " max_to_min=" << static_cast<double>(maxRangeSize) / static_cast<double>(minRangeSize)
              << " cv=" << coefficientOfVariation << std::endl;
    EXPECT_EQ(totalRangeSize, HASH_RING_SIZE);
    EXPECT_GT(minRangeSize, 0);
    EXPECT_GE(maxRangeSize, minRangeSize);
}

INSTANTIATE_TEST_SUITE_P(DistributionMatrix, HashAlgorithmDistributionTest,
                         ::testing::Combine(::testing::ValuesIn(DISTRIBUTION_TOKEN_COUNTS),
                                            ::testing::ValuesIn(DISTRIBUTION_MEMBER_COUNTS)));

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
    for (size_t memberIndex = 1; memberIndex < first.next.members.size(); ++memberIndex) {
        const auto &member = first.next.members[memberIndex];
        for (uint32_t tokenIndex = 0; tokenIndex < member.tokens.size(); ++tokenIndex) {
            EXPECT_EQ(member.tokens[tokenIndex], HashAlgorithm::MakeToken(member.identity.address, tokenIndex, 0));
        }
    }
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

TEST(HashAlgorithmTest, FailureOwnerChangesOnlyUseSelectedCommittedSourcesForEveryUnrelatedState)
{
    constexpr std::array<MemberState, 3> selectedStates{ MemberState::ACTIVE, MemberState::PRE_LEAVING,
                                                         MemberState::LEAVING };
    constexpr std::array<MemberState, 5> unrelatedStates{ MemberState::INITIAL, MemberState::JOINING,
                                                          MemberState::ACTIVE, MemberState::PRE_LEAVING,
                                                          MemberState::LEAVING };
    HashAlgorithm algorithm;
    for (const auto selectedState : selectedStates) {
        for (const auto unrelatedState : unrelatedStates) {
            SCOPED_TRACE(::testing::Message() << "selected_state=" << static_cast<int>(selectedState)
                                              << " unrelated_state=" << static_cast<int>(unrelatedState));
            TopologyState current;
            current.clusterHasInit = true;
            current.version = 1;
            current.members = {
                MakePlanningMember('a', 1, MemberState::ACTIVE, { 10, 110 }),
                MakePlanningMember('b', 2, MemberState::ACTIVE, { 60, 160 }),
                MakePlanningMember('c', 3, selectedState, { 30, 130 }),
                MakePlanningMember('d', 4, unrelatedState,
                                   unrelatedState == MemberState::INITIAL ? std::vector<uint32_t>{}
                                                                          : std::vector<uint32_t>{ 90, 190 }),
            };
            TopologyPlan plan;
            DS_ASSERT_OK(algorithm.PlanFailure({ current, { current.members[2].identity } }, plan));
            ExpectFailureOwnerContract(plan, { current.members[2].identity.address });
            EXPECT_EQ(plan.next.members[2].state, MemberState::FAILED);
            EXPECT_EQ(plan.next.members[3].state, unrelatedState);
        }
    }
}

TEST(HashAlgorithmTest, PlansMultipleFailuresWithoutOrdinaryOwnerSources)
{
    HashAlgorithm algorithm;
    TopologyState current;
    current.clusterHasInit = true;
    current.version = 1;
    current.members = {
        MakePlanningMember('a', 1, MemberState::ACTIVE, { 10, 110 }),
        MakePlanningMember('b', 2, MemberState::ACTIVE, { 60, 160 }),
        MakePlanningMember('c', 3, MemberState::ACTIVE, { 30, 130 }),
        MakePlanningMember('d', 4, MemberState::LEAVING, { 90, 190 }),
        MakePlanningMember('e', 5, MemberState::PRE_LEAVING, { 45, 145 }),
    };
    TopologyPlan plan;
    DS_ASSERT_OK(
        algorithm.PlanFailure({ current, { current.members[2].identity, current.members[3].identity } }, plan));
    ExpectFailureOwnerContract(plan, { current.members[2].identity.address, current.members[3].identity.address });
    EXPECT_EQ(plan.next.members[4].state, MemberState::PRE_LEAVING);
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
