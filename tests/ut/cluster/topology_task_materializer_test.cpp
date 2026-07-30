/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Deterministic cluster topology task materialization tests.
 */
#include "datasystem/cluster/control/topology_task_materializer.h"

#include <set>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_plan_builder.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

Member MakeMaterializerMember(char id, uint32_t port, MemberState state, std::vector<uint32_t> tokens)
{
    return { { std::string(16, id), "127.0.0.1:" + std::to_string(port) }, state, std::move(tokens) };
}

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

TEST(TopologyTaskMaterializerTest, BuildsOneSharedRestartSetForEveryMembershipRecipient)
{
    TopologyState state;
    state.version = 3;
    state.clusterHasInit = true;
    state.members = {
        MakeMaterializerMember('a', 1, MemberState::ACTIVE, { 10 }),
        MakeMaterializerMember('b', 2, MemberState::ACTIVE, { 20 }),
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    const std::vector<MembershipRecord> memberships{
        { "127.0.0.1:1", MemberLifecycleState::READY, 100, "" },
        { "127.0.0.1:2", MemberLifecycleState::RESTARTING, 200, "" },
    };
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    DS_ASSERT_OK(materializer.RebuildExpected(*snapshot, algorithm, memberships, true, expected));

    EXPECT_TRUE(expected.notifiesByAddress.empty());
    EXPECT_EQ(expected.notifyRecipients,
              (std::vector<std::string>{ "127.0.0.1:1", "127.0.0.1:2" }));
    EXPECT_EQ(expected.restartTimestampsByAddress,
              (std::map<std::string, int64_t>{ { "127.0.0.1:2", 200 } }));
    for (const auto &address : expected.notifyRecipients) {
        TopologyTaskNotify notify;
        DS_ASSERT_OK(materializer.BuildNotifyFor(expected, address, notify));
        EXPECT_FALSE(notify.activeBatch.has_value());
        EXPECT_TRUE(notify.taskIds.empty());
        EXPECT_EQ(notify.restartTimestampsByAddress, expected.restartTimestampsByAddress);
        std::string regularBytes;
        std::string optimizedBytes;
        DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(notify, regularBytes));
        DS_ASSERT_OK(materializer.BuildEncodedNotifyFor(expected, address, optimizedBytes));
        EXPECT_EQ(optimizedBytes, regularBytes);
    }
    auto &taskNotify = expected.notifiesByAddress["127.0.0.1:1"];
    taskNotify.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 3 };
    taskNotify.taskIds = { "m-e3-0123456789abcdef0123456789abcdef" };
    TopologyTaskNotify combined;
    DS_ASSERT_OK(materializer.BuildNotifyFor(expected, "127.0.0.1:1", combined));
    std::string regularBytes;
    std::string optimizedBytes;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeNotify(combined, regularBytes));
    DS_ASSERT_OK(materializer.BuildEncodedNotifyFor(expected, "127.0.0.1:1", optimizedBytes));
    EXPECT_EQ(optimizedBytes, regularBytes);
    TopologyTaskNotify decoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeNotify(optimizedBytes, decoded));
    EXPECT_EQ(decoded.taskIds, combined.taskIds);
    EXPECT_EQ(decoded.restartTimestampsByAddress, combined.restartTimestampsByAddress);
}

TEST(TopologyTaskMaterializerTest, InvalidRestartSetDoesNotReplacePreviousGeneration)
{
    TopologyState state;
    state.version = 3;
    state.clusterHasInit = true;
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    const std::vector<MembershipRecord> memberships{
        { "127.0.0.1:1", MemberLifecycleState::READY, 100, "" },
        { "127.0.0.1:2", MemberLifecycleState::RESTARTING, 0, "" },
    };
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    expected.notifyRecipients = { "unchanged" };
    expected.canonicalRestartNotify = "unchanged";

    EXPECT_EQ(materializer.RebuildExpected(*snapshot, algorithm, memberships, true, expected).GetCode(), K_INVALID);
    EXPECT_EQ(expected.notifyRecipients, (std::vector<std::string>{ "unchanged" }));
    EXPECT_TRUE(expected.restartTimestampsByAddress.empty());
    EXPECT_EQ(expected.canonicalRestartNotify, "unchanged");
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

TEST(TopologyTaskMaterializerTest, RebuildsFailureTasksForTwoCrashedScaleInMembers)
{
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyState scaleIn;
    scaleIn.version = 4;
    scaleIn.clusterHasInit = true;
    scaleIn.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, 4 };
    scaleIn.members = {
        MakeMaterializerMember('a', 1, MemberState::LEAVING, { 10, 110 }),
        MakeMaterializerMember('b', 2, MemberState::LEAVING, { 30, 130 }),
        MakeMaterializerMember('c', 3, MemberState::LEAVING, { 50, 150 }),
        MakeMaterializerMember('d', 4, MemberState::ACTIVE, { 70, 170 }),
        MakeMaterializerMember('e', 5, MemberState::ACTIVE, { 90, 190 }),
    };
    const std::set<std::string> confirmed{ scaleIn.members[0].identity.address, scaleIn.members[1].identity.address };
    TopologyPlan failure;
    DS_ASSERT_OK(builder.BuildFailureStartOrReplan(
        scaleIn, { scaleIn.members[0].identity, scaleIn.members[1].identity }, failure));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(failure.next, 1, std::string(64, 'a'), snapshot));
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState first;
    auto rc = materializer.RebuildExpected(*snapshot, algorithm, first);
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    ASSERT_FALSE(first.tasks.empty());
    std::set<std::string> taskSources;
    for (const auto &task : first.tasks) {
        ASSERT_TRUE(std::holds_alternative<TopologyDeleteTask>(task));
        const auto &deletion = std::get<TopologyDeleteTask>(task);
        EXPECT_EQ(confirmed.count(deletion.failedAddress), 1);
        taskSources.insert(deletion.failedAddress);
        const Member *executor = nullptr;
        DS_ASSERT_OK(snapshot->FindMemberByAddress(deletion.executorAddress, executor));
        ASSERT_NE(executor, nullptr);
        EXPECT_EQ(executor->state, MemberState::ACTIVE);
    }
    EXPECT_EQ(taskSources, confirmed);

    ExpectedDerivedState second;
    DS_ASSERT_OK(materializer.RebuildExpected(*snapshot, algorithm, second));
    ASSERT_EQ(first.tasks.size(), second.tasks.size());
    for (size_t index = 0; index < first.tasks.size(); ++index) {
        const auto taskId = [](const auto &task) {
            return std::visit([](const auto &value) { return value.taskId; }, task);
        };
        EXPECT_EQ(taskId(first.tasks[index]), taskId(second.tasks[index]));
    }
    ASSERT_EQ(first.notifiesByAddress.size(), second.notifiesByAddress.size());
    for (const auto &[address, notify] : first.notifiesByAddress) {
        const auto iter = second.notifiesByAddress.find(address);
        ASSERT_NE(iter, second.notifiesByAddress.end());
        ASSERT_TRUE(iter->second.activeBatch.has_value());
        ASSERT_TRUE(notify.activeBatch.has_value());
        EXPECT_EQ(iter->second.activeBatch->type, notify.activeBatch->type);
        EXPECT_EQ(iter->second.activeBatch->epoch, notify.activeBatch->epoch);
        EXPECT_EQ(iter->second.taskIds, notify.taskIds);
    }
}

TEST(TopologyTaskMaterializerTest, ReportsFailureSourceStateViolation)
{
    TopologyState state;
    state.version = 5;
    state.clusterHasInit = true;
    state.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 5 };
    state.members = {
        MakeMaterializerMember('a', 1, MemberState::FAILED, { 10 }),
        MakeMaterializerMember('b', 2, MemberState::ACTIVE, { 50 }),
        MakeMaterializerMember('c', 3, MemberState::PRE_LEAVING, { 100 }),
        MakeMaterializerMember('d', 4, MemberState::LEAVING, { 150 }),
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    TopologyPlan plan;
    plan.next = state;
    plan.ownerChanges.push_back({ state.members[3].identity, state.members[1].identity, { { 0, 20 } } });
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    const auto rc = materializer.BuildExpected(*snapshot, plan, expected);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_NE(rc.GetMsg().find("Failure owner change source is not failed"), std::string::npos) << rc.GetMsg();
}

TEST(TopologyTaskMaterializerTest, ReportsFailureTargetStateViolation)
{
    TopologyState state;
    state.version = 5;
    state.clusterHasInit = true;
    state.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, 5 };
    state.members = {
        MakeMaterializerMember('a', 1, MemberState::FAILED, { 10 }),
        MakeMaterializerMember('b', 2, MemberState::ACTIVE, { 50 }),
        MakeMaterializerMember('c', 3, MemberState::PRE_LEAVING, { 100 }),
        MakeMaterializerMember('d', 4, MemberState::LEAVING, { 150 }),
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    TopologyPlan plan;
    plan.next = state;
    plan.ownerChanges.push_back({ state.members[0].identity, state.members[2].identity, { { 0, 20 } } });
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    const auto rc = materializer.BuildExpected(*snapshot, plan, expected);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_NE(rc.GetMsg().find("Failure owner change target is not active"), std::string::npos) << rc.GetMsg();
}

TEST(TopologyTaskMaterializerTest, RejectsScaleInTargetThatHasStartedLeaving)
{
    TopologyState state;
    state.version = 6;
    state.clusterHasInit = true;
    state.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, 6 };
    state.members = {
        MakeMaterializerMember('a', 1, MemberState::LEAVING, { 10 }),
        MakeMaterializerMember('b', 2, MemberState::PRE_LEAVING, { 50 }),
    };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot));
    TopologyPlan plan;
    plan.next = state;
    plan.ownerChanges.push_back({ state.members[0].identity, state.members[1].identity, { { 0, 20 } } });
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;

    EXPECT_EQ(materializer.BuildExpected(*snapshot, plan, expected).GetCode(), K_INVALID);
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
