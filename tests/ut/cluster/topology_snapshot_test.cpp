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
 * Description: Immutable cluster topology snapshot contract tests.
 */
#include "datasystem/cluster/model/topology_snapshot.h"

#include <algorithm>
#include <numeric>

#include "gtest/gtest.h"

#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr uint32_t LARGE_TOPOLOGY_MEMBER_COUNT = 10'000;
constexpr uint32_t TOKENS_PER_LARGE_MEMBER = 4;
constexpr uint32_t LARGE_TOPOLOGY_PORT_BASE = 20'000;

std::string MakeIndexedId(uint32_t index)
{
    std::string id(16, '\0');
    std::copy_n(reinterpret_cast<const char *>(&index), sizeof(index), id.begin());
    return id;
}

Member MakeMember(char idByte, std::string address, MemberState state, std::vector<uint32_t> tokens)
{
    return Member{ { std::string(16, idByte), std::move(address) }, state, std::move(tokens) };
}

TEST(TopologySnapshotTest, BuildsStableIndexesAndCommittedOwnerView)
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = 7;
    state.members = { MakeMember('b', "127.0.0.1:2", MemberState::JOINING, { 30, 40 }),
                      MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10, 20 }) };
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 7 };
    std::shared_ptr<const TopologySnapshot> snapshot;

    DS_ASSERT_OK(TopologySnapshot::Create(std::move(state), 11, std::string(64, 'd'), snapshot));

    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->Version(), 7);
    EXPECT_EQ(snapshot->AuthorityRevision(), 11);
    ASSERT_EQ(snapshot->Members().size(), 2);
    EXPECT_EQ(snapshot->Members()[0].identity.address, "127.0.0.1:1");
    ASSERT_EQ(snapshot->CommittedMembers().size(), 1);
    const Member *member = nullptr;
    DS_ASSERT_OK(snapshot->FindMemberById(std::string(16, 'a'), member));
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(member->identity.address, "127.0.0.1:1");
}

TEST(TopologySnapshotTest, BuildsCanonicalStableActiveAndFailedProjections)
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = 9;
    state.members = { MakeMember('c', "127.0.0.1:3", MemberState::FAILED, { 30 }),
                      MakeMember('b', "127.0.0.1:2", MemberState::PRE_LEAVING, { 20 }),
                      MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10 }) };
    state.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 9 };
    std::shared_ptr<const TopologySnapshot> snapshot;

    DS_ASSERT_OK(TopologySnapshot::Create(std::move(state), 9, std::string(64, 'd'), snapshot));

    ASSERT_EQ(snapshot->ActiveMembers().size(), 1);
    ASSERT_EQ(snapshot->FailedMembers().size(), 1);
    EXPECT_EQ(snapshot->ActiveMembers().front()->identity.address, "127.0.0.1:1");
    EXPECT_EQ(snapshot->FailedMembers().front()->identity.address, "127.0.0.1:3");
    EXPECT_EQ(snapshot->ActiveMembers().front(), &snapshot->Members().front());
    EXPECT_EQ(&snapshot->ActiveMembers(), &snapshot->ActiveMembers());
}

TEST(TopologySnapshotTest, FindsNextCommittedMemberWithWrapAndPreservesOutputOnFailure)
{
    TopologyState state;
    state.version = 4;
    state.members = { MakeMember('c', "127.0.0.1:3", MemberState::ACTIVE, { 30 }),
                      MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10 }),
                      MakeMember('b', "127.0.0.1:2", MemberState::PRE_LEAVING, { 20 }) };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(std::move(state), 4, std::string(64, 'd'), snapshot));

    const Member *next = nullptr;
    DS_ASSERT_OK(snapshot->FindNextCommittedMember("127.0.0.1:1", next));
    ASSERT_NE(next, nullptr);
    EXPECT_EQ(next->identity.address, "127.0.0.1:2");
    DS_ASSERT_OK(snapshot->FindNextCommittedMember("127.0.0.1:3", next));
    EXPECT_EQ(next->identity.address, "127.0.0.1:1");

    const Member *sentinel = next;
    EXPECT_EQ(snapshot->FindNextCommittedMember("127.0.0.1:9", next).GetCode(), K_NOT_FOUND);
    EXPECT_EQ(next, sentinel);
}

TEST(TopologySnapshotTest, FindsNextActiveMemberAcrossNonActiveBoundaryAndRejectsSelfOnly)
{
    TopologyState state;
    state.version = 4;
    state.members = { MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10 }),
                      MakeMember('b', "127.0.0.1:2", MemberState::PRE_LEAVING, { 20 }),
                      MakeMember('c', "127.0.0.1:3", MemberState::ACTIVE, { 30 }) };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(std::move(state), 4, std::string(64, 'd'), snapshot));

    const Member *next = nullptr;
    DS_ASSERT_OK(snapshot->FindNextActiveMember("127.0.0.1:2", next));
    ASSERT_NE(next, nullptr);
    EXPECT_EQ(next->identity.address, "127.0.0.1:3");
    DS_ASSERT_OK(snapshot->FindNextActiveMember("127.0.0.1:3", next));
    EXPECT_EQ(next->identity.address, "127.0.0.1:1");

    TopologyState single;
    single.version = 5;
    single.members = { MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10 }) };
    DS_ASSERT_OK(TopologySnapshot::Create(std::move(single), 5, std::string(64, 'e'), snapshot));
    const Member sentinel;
    next = &sentinel;
    EXPECT_EQ(snapshot->FindNextActiveMember("127.0.0.1:1", next).GetCode(), K_NOT_FOUND);
    EXPECT_EQ(next, &sentinel);
}

TEST(TopologySnapshotTest, ValidatesExactScaleOutMigrationFenceAndVersionDirection)
{
    TopologyState state;
    state.version = 8;
    state.members = { MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10 }),
                      MakeMember('b', "127.0.0.1:2", MemberState::JOINING, { 20 }) };
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 8 };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 8, std::string(64, 'd'), snapshot));

    TopologyMigrationFence fence{ 8, 8, state.members[0].identity, state.members[1].identity };
    DS_ASSERT_OK(snapshot->ValidateMigrationFence(fence));

    fence.topologyVersion = 9;
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_TRY_AGAIN);
    fence.topologyVersion = 7;
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_INVALID);
}

TEST(TopologySnapshotTest, RejectsWrongMigrationEpochIdentityAndParticipantState)
{
    TopologyState state;
    state.version = 8;
    state.members = { MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10 }),
                      MakeMember('b', "127.0.0.1:2", MemberState::JOINING, { 20 }) };
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 8 };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 8, std::string(64, 'd'), snapshot));
    TopologyMigrationFence fence{ 8, 8, state.members[0].identity, state.members[1].identity };

    fence.batchEpoch = 7;
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_INVALID);
    fence.batchEpoch = 8;
    fence.source.id = std::string(16, 'x');
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_INVALID);
    fence.source = state.members[0].identity;
    fence.target.address = "127.0.0.1:9";
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_INVALID);
    fence.target = state.members[1].identity;
    std::swap(fence.source, fence.target);
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_INVALID);
}

TEST(TopologySnapshotTest, AcceptsScaleInFenceAndRejectsFailureBatch)
{
    TopologyState scaleIn;
    scaleIn.version = 12;
    scaleIn.members = { MakeMember('a', "127.0.0.1:1", MemberState::LEAVING, { 10 }),
                        MakeMember('b', "127.0.0.1:2", MemberState::ACTIVE, { 20 }),
                        MakeMember('c', "127.0.0.1:3", MemberState::LEAVING, { 30 }),
                        MakeMember('d', "127.0.0.1:4", MemberState::PRE_LEAVING, { 40 }) };
    scaleIn.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, 12 };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(scaleIn, 12, std::string(64, 'd'), snapshot));
    TopologyMigrationFence fence{ 12, 12, scaleIn.members[0].identity, scaleIn.members[1].identity };
    DS_ASSERT_OK(snapshot->ValidateMigrationFence(fence));
    fence.target = scaleIn.members[2].identity;
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_INVALID);
    fence.target = scaleIn.members[0].identity;
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_INVALID);
    fence.target = scaleIn.members[3].identity;
    DS_ASSERT_OK(snapshot->ValidateMigrationFence(fence));

    TopologyState failure;
    failure.version = 13;
    failure.members = { MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10 }),
                        MakeMember('b', "127.0.0.1:2", MemberState::FAILED, { 20 }) };
    failure.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 13 };
    DS_ASSERT_OK(TopologySnapshot::Create(failure, 13, std::string(64, 'e'), snapshot));
    fence = { 13, 13, failure.members[0].identity, failure.members[1].identity };
    EXPECT_EQ(snapshot->ValidateMigrationFence(fence).GetCode(), K_INVALID);
}

TEST(TopologySnapshotTest, RejectsInvalidIdentityTokenAndBatchStateWithoutPublishing)
{
    TopologyState state;
    state.version = 3;
    state.members = { MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, { 10 }),
                      MakeMember('b', "127.0.0.1:2", MemberState::JOINING, { 10 }) };
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, 3 };
    auto original = std::shared_ptr<const TopologySnapshot>();

    EXPECT_EQ(TopologySnapshot::Create(std::move(state), 4, std::string(64, 'd'), original).GetCode(), K_INVALID);
    EXPECT_EQ(original, nullptr);
}

TEST(TopologySnapshotTest, RejectsCommittedMemberWithoutPlacementToken)
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = 3;
    state.members = { MakeMember('a', "127.0.0.1:1", MemberState::ACTIVE, {}) };
    std::shared_ptr<const TopologySnapshot> snapshot;

    EXPECT_EQ(TopologySnapshot::Create(std::move(state), 4, std::string(64, 'd'), snapshot).GetCode(), K_INVALID);
    EXPECT_EQ(snapshot, nullptr);
}

TEST(TopologySnapshotTest, AllowsScaleInRequestToQueueDuringScaleOut)
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = 8;
    state.members = { MakeMember('a', "127.0.0.1:1", MemberState::PRE_LEAVING, { 10 }),
                      MakeMember('b', "127.0.0.1:2", MemberState::JOINING, { 20 }) };
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 7 };
    std::shared_ptr<const TopologySnapshot> snapshot;

    DS_ASSERT_OK(TopologySnapshot::Create(std::move(state), 4, std::string(64, 'd'), snapshot));
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->CommittedMembers().front()->state, MemberState::PRE_LEAVING);
}

TEST(TopologySnapshotTest, BuildsTenThousandMemberFortyThousandTokenIndexes)
{
    TopologyState state;
    state.version = 1;
    state.clusterHasInit = true;
    state.members.reserve(LARGE_TOPOLOGY_MEMBER_COUNT);
    for (uint32_t index = 0; index < LARGE_TOPOLOGY_MEMBER_COUNT; ++index) {
        Member member{ { MakeIndexedId(index), "127.0.0.1:" + std::to_string(LARGE_TOPOLOGY_PORT_BASE + index) },
                       MemberState::ACTIVE,
                       {} };
        for (uint32_t token = 0; token < TOKENS_PER_LARGE_MEMBER; ++token) {
            member.tokens.push_back(index * TOKENS_PER_LARGE_MEMBER + token);
        }
        state.members.emplace_back(std::move(member));
    }
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(std::move(state), 1, std::string(64, 'a'), snapshot));
    ASSERT_EQ(snapshot->Members().size(), LARGE_TOPOLOGY_MEMBER_COUNT);
    ASSERT_EQ(snapshot->CommittedMembers().size(), LARGE_TOPOLOGY_MEMBER_COUNT);
    ASSERT_EQ(snapshot->ActiveMembers().size(), LARGE_TOPOLOGY_MEMBER_COUNT);
    EXPECT_EQ(&snapshot->ActiveMembers(), &snapshot->ActiveMembers());
    const auto tokenCount =
        std::accumulate(snapshot->Members().begin(), snapshot->Members().end(), size_t{ 0 },
                        [](size_t count, const auto &member) { return count + member.tokens.size(); });
    EXPECT_EQ(tokenCount, LARGE_TOPOLOGY_MEMBER_COUNT * TOKENS_PER_LARGE_MEMBER);
}

}  // namespace
}  // namespace datasystem::cluster
