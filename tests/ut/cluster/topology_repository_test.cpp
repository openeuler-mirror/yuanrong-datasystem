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
 * Description: Single-key semantic topology repository tests.
 */
#include "datasystem/cluster/repository/topology_repository.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"

#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr size_t LARGE_MEMBERSHIP_COUNT = 10'000;
constexpr uint32_t LARGE_MEMBERSHIP_PORT_BASE = 10'000;

TEST(TopologyRepositoryTest, TopologyCasCommitsAndReadsBackConflict)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("repo", keys));
    TopologyRepository repository(backend, *keys);
    TopologyState initial;
    initial.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);

    TopologyState desired = initial;
    desired.version = 2;
    TopologyCasResult result;
    DS_ASSERT_OK(repository.CompareAndSwapTopology(1, desired, result));
    EXPECT_EQ(result.outcome, TopologyCasOutcome::COMMITTED);

    TopologyState concurrent = desired;
    concurrent.version = 4;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), concurrent);
    DS_ASSERT_OK(repository.CompareAndSwapTopology(1, desired, result));
    EXPECT_EQ(result.outcome, TopologyCasOutcome::CONFLICT);
    ASSERT_TRUE(result.observed.has_value());
    EXPECT_EQ(result.observed->version, 4);
}

TEST(TopologyRepositoryTest, DerivedRecordsCannotReplaceTopologyAuthority)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("repo", keys));
    TopologyRepository repository(backend, *keys);
    backend.PutBytes(keys->MigrateTaskTable(), "m-e2-0123456789abcdef0123456789abcdef", "task");
    TopologyState state;
    int64_t revision = -1;

    EXPECT_EQ(repository.ReadTopology(100, state, revision).GetCode(), K_NOT_FOUND);
}

TEST(TopologyRepositoryTest, FinishesCompleteFencedTaskScopeInOneCas)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("repo", keys));
    TopologyRepository repository(backend, *keys);
    const std::string taskId = "m-e2-0123456789abcdef0123456789abcdef";
    TopologyMigrateTask migrate{ taskId,        TopologyChangeType::SCALE_OUT,          2, "127.0.0.1:1",
                                 "127.0.0.1:2", { { "127.0.0.1:1", { 1, 10 }, false } } };
    DS_ASSERT_OK(repository.CreateTaskIfAbsent(TopologyTask{ migrate }));
    TopologyExecutionFence fence;
    fence.taskId = taskId;
    fence.batchEpoch = 2;
    fence.executor = { std::string(16, 'a'), "127.0.0.1:1" };
    fence.ranges = { { 1, 10 } };
    TaskProgressOutcome outcome;

    backend.FailNextCasAfterCommit();
    DS_ASSERT_OK(repository.MarkTaskScopeFinished(fence, outcome));
    EXPECT_EQ(outcome, TaskProgressOutcome::ALREADY_FINISHED);
    DS_ASSERT_OK(repository.MarkTaskScopeFinished(fence, outcome));
    EXPECT_EQ(outcome, TaskProgressOutcome::ALREADY_FINISHED);
}

TEST(TopologyRepositoryTest, TaskMaterializationAcceptsAndPreservesMonotonicProgress)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("repo", keys));
    TopologyRepository repository(backend, *keys);
    const std::string taskId = "m-e2-0123456789abcdef0123456789abcdef";
    TopologyMigrateTask task{ taskId,        TopologyChangeType::SCALE_OUT,          2, "127.0.0.1:1",
                              "127.0.0.1:2", { { "127.0.0.1:1", { 1, 10 }, false } } };
    DS_ASSERT_OK(repository.CreateTaskIfAbsent(TopologyTask{ task }));
    TopologyExecutionFence fence{ taskId, TopologyTaskKind::MIGRATE, TopologyChangeType::SCALE_OUT, 2 };
    fence.executor = { std::string(16, 'a'), task.executorAddress };
    fence.ranges = { { 1, 10 } };
    TaskProgressOutcome outcome;
    DS_ASSERT_OK(repository.MarkTaskScopeFinished(fence, outcome));
    DS_ASSERT_OK(repository.CreateTaskIfAbsent(TopologyTask{ task }));
    TopologyTask observed;
    DS_ASSERT_OK(repository.ReadTask(TopologyTaskKind::MIGRATE, taskId, task.type, task.epoch, observed));
    EXPECT_TRUE(std::get<TopologyMigrateTask>(observed).sourceRanges.front().finished);
}

TEST(TopologyRepositoryTest, ResolvesCasUnknownByExactTopologyReadBack)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("repo", keys));
    TopologyRepository repository(backend, *keys);
    TopologyState initial;
    initial.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    TopologyState desired = initial;
    desired.version = 2;
    TopologyCasResult result;

    backend.FailNextCasAfterCommit();
    DS_ASSERT_OK(repository.CompareAndSwapTopology(1, desired, result));
    EXPECT_EQ(result.outcome, TopologyCasOutcome::COMMITTED);

    desired.version = 3;
    backend.FailNextCasBeforeCommit();
    DS_ASSERT_OK(repository.CompareAndSwapTopology(2, desired, result));
    EXPECT_EQ(result.outcome, TopologyCasOutcome::CONFLICT);
    ASSERT_TRUE(result.observed.has_value());
    EXPECT_EQ(result.observed->version, 2);
}

TEST(TopologyRepositoryTest, BootstrapsMissingTopologyWithExpectedVersionZero)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("repo", keys));
    TopologyRepository repository(backend, *keys);
    TopologyState desired;
    desired.version = 1;
    TopologyCasResult result;

    DS_ASSERT_OK(repository.CompareAndSwapTopology(0, desired, result));
    EXPECT_EQ(result.outcome, TopologyCasOutcome::COMMITTED);
}

TEST(TopologyRepositoryTest, RejectsSameVersionWithDifferentCanonicalBytes)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("repo", keys));
    TopologyRepository repository(backend, *keys);
    TopologyState concurrent;
    concurrent.version = 2;
    concurrent.clusterHasInit = true;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), concurrent);
    TopologyState desired;
    desired.version = 2;
    TopologyCasResult result;

    EXPECT_EQ(repository.CompareAndSwapTopology(1, desired, result).GetCode(), K_INVALID);
    EXPECT_EQ(result.outcome, TopologyCasOutcome::CONFLICT);
}

TEST(TopologyRepositoryTest, ResolvesTaskCreateUnknownByExactReadBack)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("repo", keys));
    TopologyRepository repository(backend, *keys);
    TopologyMigrateTask task{
        "m-e2-0123456789abcdef0123456789abcdef", TopologyChangeType::SCALE_OUT, 2, "127.0.0.1:1", "127.0.0.1:2",
        { { "127.0.0.1:1", { 1, 10 }, false } }
    };

    backend.FailNextCasAfterCommit();
    DS_ASSERT_OK(repository.CreateTaskIfAbsent(TopologyTask{ task }));
}

TEST(TopologyRepositoryTest, ReadsTenThousandMembershipLeasesWithoutTruncation)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("membership-capacity", keys));
    TopologyRepository repository(backend, *keys);
    for (size_t index = 0; index < LARGE_MEMBERSHIP_COUNT; ++index) {
        MembershipValue value{ static_cast<int64_t>(index), MemberLifecycleState::READY, "host", "v1" };
        std::string bytes;
        DS_ASSERT_OK(MembershipValueCodec::Encode(value, bytes));
        const auto address = "127.0.0.1:" + std::to_string(LARGE_MEMBERSHIP_PORT_BASE + index);
        backend.PutBytes(keys->MembershipTable(), address, std::move(bytes));
    }

    std::vector<MembershipRecord> memberships;
    DS_ASSERT_OK(repository.ReadMemberships(memberships));
    ASSERT_EQ(memberships.size(), LARGE_MEMBERSHIP_COUNT);
    EXPECT_TRUE(std::all_of(memberships.begin(), memberships.end(),
                            [](const auto &member) { return member.state == MemberLifecycleState::READY; }));
}

}  // namespace
}  // namespace datasystem::cluster
