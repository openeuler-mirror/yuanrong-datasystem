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
 * Description: Cluster topology repository codec contract tests.
 */
#include "datasystem/cluster/repository/topology_repository_codec.h"

#include <algorithm>
#include <array>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_plan_builder.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr size_t MAX_NOTIFY_TASK_REFS = 4'096;
constexpr size_t MAX_NOTIFY_VALUE_BYTES = 4 * 1'024 * 1'024;
constexpr size_t MAX_RESTART_NOTIFY_ENTRIES = 10'000;
constexpr size_t MAX_TASK_RANGES = 4'096;
constexpr size_t MAX_REPOSITORY_VALUE_BYTES = 4 * 1'024 * 1'024;
constexpr size_t MAX_TOPOLOGY_MEMBERS = 10'000;
constexpr size_t LARGE_TOPOLOGY_MEMBER_COUNT = 2'000;
constexpr std::array<std::pair<uint32_t, size_t>, 6> LARGE_TOPOLOGY_SIZE_CASES{ {
    { 4, 82'009 },
    { 8, 82'009 },
    { 16, 82'009 },
    { 32, 82'009 },
    { 64, 82'021 },
    { 128, 82'070 },
} };

std::string BuildCapacityTaskId(size_t index)
{
    std::ostringstream digest;
    digest << std::hex << std::setfill('0') << std::setw(32) << index;
    return "m-e1-" + digest.str();
}

std::string BuildCapacityAddress(size_t index)
{
    constexpr size_t FIRST_TEST_PORT = 10'000;
    return "127.0.0.1:" + std::to_string(FIRST_TEST_PORT + index);
}

void BuildLargeTopology(std::string &encoded)
{
    TopologyState latest;
    latest.version = 1;
    latest.members.reserve(LARGE_TOPOLOGY_MEMBER_COUNT);
    std::vector<MemberIdentity> members;
    members.reserve(LARGE_TOPOLOGY_MEMBER_COUNT);
    for (size_t index = 0; index < LARGE_TOPOLOGY_MEMBER_COUNT; ++index) {
        std::string id(16, '\0');
        std::copy_n(reinterpret_cast<const char *>(&index), sizeof(index), id.begin());
        members.emplace_back(MemberIdentity{ std::move(id), BuildCapacityAddress(index) });
        latest.members.emplace_back(Member{ members.back(), MemberState::INITIAL, {} });
    }

    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState topology;
    DS_ASSERT_OK(builder.BuildBootstrap(latest, members, topology));
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(topology, encoded));
}

void ExpectLargeTopologyEncodedSize(uint32_t tokensPerMember, size_t expectedBytes)
{
    const auto savedTokensPerMember = FLAGS_hash_ring_tokens_per_member;
    Raii restore([savedTokensPerMember] { FLAGS_hash_ring_tokens_per_member = savedTokensPerMember; });
    FLAGS_hash_ring_tokens_per_member = tokensPerMember;

    std::string encoded;
    BuildLargeTopology(encoded);
    TopologyState decoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeTopology(encoded, decoded));
    EXPECT_EQ(decoded.members.size(), LARGE_TOPOLOGY_MEMBER_COUNT);
    EXPECT_EQ(decoded.tokensPerMember, tokensPerMember);
    std::cout << "members=" << LARGE_TOPOLOGY_MEMBER_COUNT << " tokens_per_member=" << tokensPerMember
              << " encoded_bytes=" << encoded.size() << std::endl;
    EXPECT_EQ(encoded.size(), expectedBytes);
}

TEST(TopologyRepositoryCodecTest, TopologyRoundTripIsCanonical)
{
    const auto savedTokensPerMember = FLAGS_hash_ring_tokens_per_member;
    Raii restore([savedTokensPerMember] { FLAGS_hash_ring_tokens_per_member = savedTokensPerMember; });
    FLAGS_hash_ring_tokens_per_member = 4;
    HashAlgorithm algorithm;
    ScaleOutPlanInput placementInput;
    placementInput.joining = { { std::string(16, 'b'), "127.0.0.1:2" },
                               { std::string(16, 'a'), "127.0.0.1:1" } };
    TopologyPlan plan;
    DS_ASSERT_OK(algorithm.BuildInitialPlacement(placementInput, plan));
    TopologyState input = std::move(plan.next);
    input.clusterHasInit = true;
    input.version = 9;
    input.members.back().state = MemberState::JOINING;
    input.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 9 };
    std::string first;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(input, first));
    TopologyState decoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeTopology(first, decoded));
    std::string second;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(decoded, second));
    EXPECT_EQ(first, second);
    EXPECT_EQ(decoded.members[0].identity.address, "127.0.0.1:1");
}

class LargeTopologyCodecTest : public ::testing::TestWithParam<std::pair<uint32_t, size_t>> {
};

TEST_P(LargeTopologyCodecTest, UsesExpectedSerializedBytesForTwoThousandMembers)
{
    const auto [tokensPerMember, expectedBytes] = GetParam();
    ExpectLargeTopologyEncodedSize(tokensPerMember, expectedBytes);
}

INSTANTIATE_TEST_SUITE_P(TokensPerMember, LargeTopologyCodecTest, ::testing::ValuesIn(LARGE_TOPOLOGY_SIZE_CASES));

TEST(TopologyRepositoryCodecTest, PersistsOnlyNonDefaultTokenSeeds)
{
    const auto savedTokensPerMember = FLAGS_hash_ring_tokens_per_member;
    Raii restore([savedTokensPerMember] { FLAGS_hash_ring_tokens_per_member = savedTokensPerMember; });
    FLAGS_hash_ring_tokens_per_member = 4;
    const std::string address = "127.0.0.1:1";
    Member member{ { std::string(16, 'a'), address }, MemberState::ACTIVE, {} };
    for (uint32_t index = 0; index < FLAGS_hash_ring_tokens_per_member; ++index) {
        const auto seed = index == 2 ? 1 : 0;
        member.tokens.emplace_back(HashAlgorithm::MakeToken(address, index, seed));
    }
    member.tokenSeedOverrides.emplace_back(TokenSeedOverride{ 2, 1 });
    TopologyState input;
    input.clusterHasInit = true;
    input.version = 1;
    input.members.emplace_back(member);
    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(input, encoded));

    ::datasystem::ClusterTopologyPb pb;
    ASSERT_TRUE(pb.ParseFromString(encoded));
    const auto &memberPb = pb.members().at(address);
    ASSERT_EQ(memberPb.token_seed_overrides_size(), 1);
    EXPECT_EQ(memberPb.token_seed_overrides(0).token_index(), 2);
    EXPECT_EQ(memberPb.token_seed_overrides(0).token_seed(), 1);
    EXPECT_EQ(pb.tokens_per_member(), FLAGS_hash_ring_tokens_per_member);

    TopologyState decoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeTopology(encoded, decoded));
    ASSERT_EQ(decoded.members.size(), 1);
    EXPECT_EQ(decoded.members.front().tokens, member.tokens);
    EXPECT_EQ(decoded.members.front().tokenSeedOverrides, member.tokenSeedOverrides);
    for (uint32_t index = 0; index < FLAGS_hash_ring_tokens_per_member; ++index) {
        const auto seed = index == 2 ? 1 : 0;
        EXPECT_EQ(decoded.members.front().tokens[index], HashAlgorithm::MakeToken(address, index, seed));
    }
}

TEST(TopologyRepositoryCodecTest, DecodesTokenCountFromTopology)
{
    const auto savedTokensPerMember = FLAGS_hash_ring_tokens_per_member;
    Raii restore([savedTokensPerMember] { FLAGS_hash_ring_tokens_per_member = savedTokensPerMember; });
    FLAGS_hash_ring_tokens_per_member = 4;
    ::datasystem::ClusterTopologyPb pb;
    pb.set_cluster_has_init(true);
    pb.set_version(1);
    pb.set_schema_version("2");
    pb.set_tokens_per_member(8);
    auto &member = (*pb.mutable_members())["127.0.0.1:1"];
    member.set_id(std::string(16, 'a'));
    member.set_state(::datasystem::MembershipPb::ACTIVE);
    std::string encoded;
    ASSERT_TRUE(pb.SerializeToString(&encoded));

    TopologyState decoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeTopology(encoded, decoded));
    EXPECT_EQ(decoded.tokensPerMember, 8);
    ASSERT_EQ(decoded.members.size(), 1);
    EXPECT_EQ(decoded.members.front().tokens.size(), 8);
}

TEST(TopologyRepositoryCodecTest, RejectsTokenVectorThatDoesNotMatchStoredSeedOverrides)
{
    const auto savedTokensPerMember = FLAGS_hash_ring_tokens_per_member;
    Raii restore([savedTokensPerMember] { FLAGS_hash_ring_tokens_per_member = savedTokensPerMember; });
    FLAGS_hash_ring_tokens_per_member = 1;
    const std::string address = "127.0.0.1:1";
    TopologyState topology;
    topology.clusterHasInit = true;
    topology.members.emplace_back(
        Member{ { std::string(16, 'a'), address }, MemberState::ACTIVE, { HashAlgorithm::MakeToken(address, 0, 1) } });

    std::string encoded;
    EXPECT_EQ(TopologyRepositoryCodec::EncodeTopology(topology, encoded).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryCodecTest, PreservesCollisionSeedAcrossCanonicalMemberReordering)
{
    const auto savedTokensPerMember = FLAGS_hash_ring_tokens_per_member;
    Raii restore([savedTokensPerMember] { FLAGS_hash_ring_tokens_per_member = savedTokensPerMember; });
    FLAGS_hash_ring_tokens_per_member = 1;
    const std::string existingAddress = "10.2.116.157:20925";
    const std::string joiningAddress = "10.1.181.156:22028";
    ASSERT_EQ(HashAlgorithm::MakeToken(existingAddress, 0, 0), HashAlgorithm::MakeToken(joiningAddress, 0, 0));

    ScaleOutPlanInput input;
    input.tokensPerMember = 1;
    input.current.clusterHasInit = true;
    input.current.version = 1;
    input.current.members.emplace_back(Member{ { std::string(16, 'b'), existingAddress }, MemberState::ACTIVE,
                                                { HashAlgorithm::MakeToken(existingAddress, 0, 0) } });
    input.joining.emplace_back(MemberIdentity{ std::string(16, 'a'), joiningAddress });
    HashAlgorithm algorithm;
    TopologyPlan plan;
    DS_ASSERT_OK(algorithm.PlanScaleOut(input, plan));
    const auto joining = std::find_if(plan.next.members.begin(), plan.next.members.end(),
                                      [&](const Member &member) { return member.identity.address == joiningAddress; });
    ASSERT_NE(joining, plan.next.members.end());
    ASSERT_EQ(joining->tokenSeedOverrides.size(), 1);
    EXPECT_EQ(joining->tokenSeedOverrides.front(), (TokenSeedOverride{ 0, 1 }));
    joining->state = MemberState::ACTIVE;

    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(plan.next, encoded));
    TopologyState decoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeTopology(encoded, decoded));
    ASSERT_EQ(decoded.members.size(), 2);
    EXPECT_EQ(decoded.members.front().identity.address, joiningAddress);
    EXPECT_EQ(decoded.members.front().tokens, joining->tokens);
    EXPECT_EQ(decoded.members.front().tokenSeedOverrides, joining->tokenSeedOverrides);
}

TEST(TopologyRepositoryCodecTest, TaskRoundTripDerivesSingleExecutor)
{
    TopologyMigrateTask input;
    input.taskId = "m-e9-0123456789abcdef0123456789abcdef";
    input.type = TopologyChangeType::SCALE_OUT;
    input.epoch = 9;
    input.executorAddress = "127.0.0.1:1";
    input.targetAddress = "127.0.0.1:2";
    input.sourceRanges = { { input.executorAddress, { 1, 10 }, false }, { input.executorAddress, { 11, 20 }, true } };
    std::string bytes;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeMigrateTask(input, bytes));
    TopologyMigrateTask output;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeMigrateTask(input.taskId, input.type, input.epoch, bytes, output));
    EXPECT_EQ(output.executorAddress, input.executorAddress);
    EXPECT_EQ(output.sourceRanges.size(), 2);
}

TEST(TopologyRepositoryCodecTest, AllowsRestartOnlyNotify)
{
    TopologyTaskNotify input;
    input.restartTimestampsByAddress.emplace("127.0.0.1:1", 100);
    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(input, encoded));

    TopologyTaskNotify output;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeNotify(encoded, output));
    EXPECT_FALSE(output.activeBatch.has_value());
    EXPECT_TRUE(output.taskIds.empty());
    EXPECT_EQ(output.restartTimestampsByAddress, input.restartTimestampsByAddress);
}

TEST(TopologyRepositoryCodecTest, ActiveTaskNotifyRoundTripIsCanonical)
{
    TopologyTaskNotify input;
    input.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 9 };
    input.taskIds = { "m-e9-0123456789abcdef0123456789abcdef" };
    input.restartTimestampsByAddress.emplace("127.0.0.1:2", 200);
    std::string first;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(input, first));

    TopologyTaskNotify output;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeNotify(first, output));
    ASSERT_TRUE(output.activeBatch.has_value());
    EXPECT_EQ(output.activeBatch->type, input.activeBatch->type);
    EXPECT_EQ(output.activeBatch->epoch, input.activeBatch->epoch);
    EXPECT_EQ(output.taskIds, input.taskIds);
    EXPECT_EQ(output.restartTimestampsByAddress, input.restartTimestampsByAddress);

    std::string second;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(output, second));
    EXPECT_EQ(first, second);
}

TEST(TopologyRepositoryCodecTest, RejectsUnsortedDuplicateNotify)
{
    TopologyTaskNotify notify;
    notify.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 1 };
    notify.taskIds = { "task-b", "task-a", "task-a" };
    std::string bytes;
    EXPECT_EQ(TopologyRepositoryCodec::EncodeNotify(notify, bytes).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryCodecTest, EnforcesNotifyReferenceAndPayloadLimits)
{
    TopologyTaskNotify notify;
    notify.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 1 };
    notify.taskIds.reserve(MAX_NOTIFY_TASK_REFS + 1);
    for (size_t index = 0; index < MAX_NOTIFY_TASK_REFS; ++index) {
        notify.taskIds.emplace_back(BuildCapacityTaskId(index));
    }
    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(notify, encoded));
    notify.taskIds.emplace_back(BuildCapacityTaskId(MAX_NOTIFY_TASK_REFS));
    EXPECT_EQ(TopologyRepositoryCodec::EncodeNotify(notify, encoded).GetCode(), K_INVALID);

    TopologyTaskNotify decoded;
    const std::string oversized(MAX_NOTIFY_VALUE_BYTES + 1, 'x');
    EXPECT_EQ(TopologyRepositoryCodec::DecodeNotify(oversized, decoded).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryCodecTest, RejectsNonCanonicalNotifyPayload)
{
    TopologyTaskNotify notify;
    notify.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 1 };
    notify.taskIds = { "m-e1-0123456789abcdef0123456789abcdef" };
    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(notify, encoded));
    encoded.append("\xA0\x06\x01", 3);

    TopologyTaskNotify decoded;
    EXPECT_EQ(TopologyRepositoryCodec::DecodeNotify(encoded, decoded).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryCodecTest, RestartNotifyEncodingIsDeterministic)
{
    TopologyTaskNotify first;
    first.restartTimestampsByAddress.emplace("127.0.0.1:2", 200);
    first.restartTimestampsByAddress.emplace("127.0.0.1:1", 100);
    TopologyTaskNotify second;
    second.restartTimestampsByAddress.emplace("127.0.0.1:1", 100);
    second.restartTimestampsByAddress.emplace("127.0.0.1:2", 200);

    std::string firstBytes;
    std::string secondBytes;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(first, firstBytes));
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(second, secondBytes));
    EXPECT_EQ(firstBytes, secondBytes);
}

TEST(TopologyRepositoryCodecTest, RejectsInvalidRestartNotifyWithoutChangingOutput)
{
    TopologyTaskNotify invalidAddress;
    invalidAddress.restartTimestampsByAddress.emplace("invalid-address", 100);
    std::string encoded = "unchanged";
    EXPECT_EQ(TopologyRepositoryCodec::EncodeNotify(invalidAddress, encoded).GetCode(), K_INVALID);
    EXPECT_EQ(encoded, "unchanged");

    TopologyTaskNotify invalidTimestamp;
    invalidTimestamp.restartTimestampsByAddress.emplace("127.0.0.1:1", 0);
    EXPECT_EQ(TopologyRepositoryCodec::EncodeNotify(invalidTimestamp, encoded).GetCode(), K_INVALID);
    EXPECT_EQ(encoded, "unchanged");

    TopologyTaskNotify tooMany;
    for (size_t index = 0; index <= MAX_RESTART_NOTIFY_ENTRIES; ++index) {
        tooMany.restartTimestampsByAddress.emplace(BuildCapacityAddress(index), 100);
    }
    EXPECT_EQ(TopologyRepositoryCodec::EncodeNotify(tooMany, encoded).GetCode(), K_INVALID);
    EXPECT_EQ(encoded, "unchanged");

    ::datasystem::TaskNotifyPb invalidPb;
    (*invalidPb.mutable_restart_timestamps_by_address())["127.0.0.1:1"] = -1;
    std::string invalidBytes;
    ASSERT_TRUE(invalidPb.SerializeToString(&invalidBytes));
    TopologyTaskNotify output;
    output.restartTimestampsByAddress.emplace("127.0.0.1:2", 200);
    EXPECT_EQ(TopologyRepositoryCodec::DecodeNotify(invalidBytes, output).GetCode(), K_INVALID);
    EXPECT_EQ(output.restartTimestampsByAddress.at("127.0.0.1:2"), 200);
}

TEST(TopologyRepositoryCodecTest, EnforcesTaskRangeAndPayloadLimits)
{
    TopologyMigrateTask task;
    task.taskId = "m-e1-0123456789abcdef0123456789abcdef";
    task.type = TopologyChangeType::SCALE_OUT;
    task.epoch = 1;
    task.executorAddress = "127.0.0.1:1";
    task.targetAddress = "127.0.0.1:2";
    task.sourceRanges.reserve(MAX_TASK_RANGES + 1);
    for (size_t index = 0; index < MAX_TASK_RANGES; ++index) {
        const auto token = static_cast<uint32_t>(index);
        task.sourceRanges.push_back({ task.executorAddress, { token, token }, false });
    }
    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeMigrateTask(task, encoded));
    task.sourceRanges.push_back({ task.executorAddress, { 0, 0 }, false });
    EXPECT_EQ(TopologyRepositoryCodec::EncodeMigrateTask(task, encoded).GetCode(), K_INVALID);

    TopologyMigrateTask decoded;
    const std::string oversized(MAX_REPOSITORY_VALUE_BYTES + 1, 'x');
    EXPECT_EQ(
        TopologyRepositoryCodec::DecodeMigrateTask(task.taskId, task.type, task.epoch, oversized, decoded).GetCode(),
        K_INVALID);
}

TEST(TopologyRepositoryCodecTest, RejectsTopologyAboveMemberLimit)
{
    ::datasystem::ClusterTopologyPb topology;
    topology.set_schema_version("2");
    for (size_t index = 0; index <= MAX_TOPOLOGY_MEMBERS; ++index) {
        auto &member = (*topology.mutable_members())["127.0.0.1:" + std::to_string(index)];
        member.set_id(std::string(16, 'a'));
        member.set_state(::datasystem::MembershipPb::INITIAL);
    }
    std::string encoded;
    ASSERT_TRUE(topology.SerializeToString(&encoded));

    TopologyState decoded;
    EXPECT_EQ(TopologyRepositoryCodec::DecodeTopology(encoded, decoded).GetCode(), K_INVALID);
}

TEST(TopologyRepositoryCodecTest, RejectsSchemaVersionOne)
{
    ::datasystem::ClusterTopologyPb topology;
    topology.set_schema_version("1");
    topology.set_tokens_per_member(4);
    std::string encoded;
    ASSERT_TRUE(topology.SerializeToString(&encoded));

    TopologyState decoded;
    EXPECT_EQ(TopologyRepositoryCodec::DecodeTopology(encoded, decoded).GetCode(), K_INVALID);
}

}  // namespace
}  // namespace datasystem::cluster
