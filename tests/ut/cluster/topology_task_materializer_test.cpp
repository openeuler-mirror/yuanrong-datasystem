/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Deterministic cluster topology task materialization tests.
 */
#include "datasystem/cluster/control/topology_task_materializer.h"

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

TEST(TopologyTaskMaterializerTest, BuildsStableOneExecutorTasksAndCompleteNotifies)
{
    HashAlgorithm algorithm;
    TopologyState current;
    current.version = 1;
    current.clusterHasInit = true;
    current.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 10, 100 } },
    };
    ScaleOutPlanInput input{ current, { { std::string(16, 'b'), "127.0.0.1:2" } }, 4 };
    TopologyPlan plan;
    DS_ASSERT_OK(algorithm.PlanScaleOut(input, plan));
    plan.next.version = 2;
    plan.next.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    std::string bytes;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(plan.next, bytes));
    TopologyState canonical;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeTopology(bytes, canonical));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(canonical, 1, std::string(64, 'a'), snapshot));
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState first;
    ExpectedDerivedState second;
    DS_ASSERT_OK(materializer.BuildExpected(*snapshot, plan, first));
    DS_ASSERT_OK(materializer.BuildExpected(*snapshot, plan, second));
    ASSERT_FALSE(first.tasks.empty());
    EXPECT_EQ(first.tasks.size(), second.tasks.size());
    const auto &task = std::get<TopologyMigrateTask>(first.tasks.front());
    EXPECT_EQ(task.taskId.rfind("m-e2-", 0), 0);
    EXPECT_FALSE(task.executorAddress.empty());
    ASSERT_EQ(first.notifiesByAddress.count(task.executorAddress), 1);
    EXPECT_EQ(first.notifiesByAddress.at(task.executorAddress).taskIds.front(), task.taskId);
}

TEST(TopologyTaskMaterializerTest, BusinessIdIgnoresEpochButFencesMemberGenerationAndScope)
{
    TopologyExecutionFence first;
    first.taskId = "m-e2-0123456789abcdef0123456789abcdef";
    first.batchEpoch = 2;
    first.executor = { std::string(16, 'a'), "127.0.0.1:1" };
    first.source = first.executor;
    first.target = MemberIdentity{ std::string(16, 'b'), "127.0.0.1:2" };
    first.ranges = { { 1, 10 } };
    auto second = first;
    second.taskId = "m-e3-0123456789abcdef0123456789abcdef";
    second.batchEpoch = 3;
    const auto firstId = TopologyTaskMaterializer::BuildBusinessOperationId(TopologyCallbackPhase::SCALE_OUT, first);
    EXPECT_EQ(firstId, "op-2d7bf969932bc5b33fe330677ab5ea5b");
    EXPECT_EQ(firstId, TopologyTaskMaterializer::BuildBusinessOperationId(TopologyCallbackPhase::SCALE_OUT, second));
    second.target->id = std::string(16, 'c');
    EXPECT_NE(TopologyTaskMaterializer::BuildBusinessOperationId(TopologyCallbackPhase::SCALE_OUT, first),
              TopologyTaskMaterializer::BuildBusinessOperationId(TopologyCallbackPhase::SCALE_OUT, second));
}

TEST(TopologyTaskMaterializerTest, RebuildsEmptyRecoverySetWhenNoCommittedTargetRemains)
{
    TopologyState state;
    state.version = 5;
    state.clusterHasInit = true;
    state.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 5 };
    state.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::FAILED, { 1 } },
                      Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { 2 } } };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    DS_ASSERT_OK(materializer.RebuildExpected(*snapshot, algorithm, expected));
    EXPECT_TRUE(expected.tasks.empty());
    EXPECT_TRUE(expected.notifiesByAddress.empty());
}

TEST(TopologyTaskMaterializerTest, SplitsLargeCanonicalScopeAtTaskRangeLimit)
{
    constexpr size_t FIRST_TASK_RANGES = 4'096;
    TopologyState state;
    state.version = 6;
    state.clusterHasInit = true;
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 6 };
    state.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                      Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::JOINING, { 2 } } };
    TopologyPlan plan;
    plan.next = state;
    TopologyOwnerChange change{ state.members.front().identity, state.members.back().identity, {} };
    for (uint32_t token = 0; token <= FIRST_TASK_RANGES; ++token) {
        change.ranges.push_back({ token, token });
    }
    plan.ownerChanges.push_back(std::move(change));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    DS_ASSERT_OK(materializer.BuildExpected(*snapshot, plan, expected));
    ASSERT_EQ(expected.tasks.size(), 2);
    std::vector<size_t> rangeCounts;
    for (const auto &task : expected.tasks) {
        rangeCounts.push_back(std::get<TopologyMigrateTask>(task).sourceRanges.size());
    }
    std::sort(rangeCounts.begin(), rangeCounts.end());
    EXPECT_EQ(rangeCounts, (std::vector<size_t>{ 1, FIRST_TASK_RANGES }));
}

}  // namespace
}  // namespace datasystem::cluster
