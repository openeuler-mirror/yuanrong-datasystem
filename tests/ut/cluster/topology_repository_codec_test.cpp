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

#include <iomanip>
#include <sstream>

#include "gtest/gtest.h"

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

TEST(TopologyRepositoryCodecTest, TopologyRoundTripIsCanonical)
{
    TopologyState input;
    input.clusterHasInit = true;
    input.version = 9;
    input.members = { Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { 30, 40 } },
                      Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 10, 20 } } };
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
    topology.set_schema_version("1");
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

}  // namespace
}  // namespace datasystem::cluster
