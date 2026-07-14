/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: ETCD-only stale topology task Janitor tests.
 */
#include "datasystem/cluster/control/topology_task_janitor.h"

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_plan_builder.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

struct JanitorScenario {
    Status SetUp(bool finalize)
    {
        RETURN_IF_NOT_OK(TopologyKeyHelper::Create("janitor", keys));
        repository = std::make_unique<TopologyRepository>(backend, *keys);
        TopologyState current;
        current.version = 1;
        current.clusterHasInit = true;
        current.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1, 100 } },
                            Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::INITIAL, {} } };
        TopologyPlanBuilder builder(algorithm);
        RETURN_IF_NOT_OK(builder.BuildScaleOutStart(current, { current.members.back().identity }, plan));
        std::shared_ptr<const TopologySnapshot> snapshot;
        RETURN_IF_NOT_OK(TopologySnapshot::Create(plan.next, 1, std::string(64, 'a'), snapshot));
        RETURN_IF_NOT_OK(materializer.BuildExpected(*snapshot, plan, expected));
        for (const auto &task : expected.tasks) {
            RETURN_IF_NOT_OK(repository->CreateTaskIfAbsent(task));
        }
        for (const auto &[address, notify] : expected.notifiesByAddress) {
            RETURN_IF_NOT_OK(repository->RewriteNotify(address, notify));
        }
        TopologyState persisted = plan.next;
        if (finalize) {
            RETURN_IF_NOT_OK(builder.BuildScaleOutFinal(plan.next, persisted));
        }
        backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), persisted);
        return Status::OK();
    }

    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    std::unique_ptr<TopologyRepository> repository;
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    TopologyPlan plan;
    ExpectedDerivedState expected;
};

bool DerivedRecordsExist(TopologyRepository &repository, const ExpectedDerivedState &expected)
{
    for (const auto &task : expected.tasks) {
        const auto kind = std::holds_alternative<TopologyMigrateTask>(task) ? TopologyTaskKind::MIGRATE
                                                                            : TopologyTaskKind::DELETE_MEMBER;
        const auto &value = std::get<TopologyMigrateTask>(task);
        TopologyTask observed;
        if (repository.ReadTask(kind, value.taskId, value.type, value.epoch, observed).IsError()) {
            return false;
        }
    }
    for (const auto &[address, notify] : expected.notifiesByAddress) {
        TopologyTaskNotify observed;
        if (repository.ReadNotify(address, observed).IsError() || observed.taskIds != notify.taskIds) {
            return false;
        }
    }
    return true;
}

TEST(TopologyTaskJanitorTest, DeletesOnlyStaleDerivedRecordsAfterReadingLatestTopology)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(true));
    ASSERT_TRUE(DerivedRecordsExist(*scenario.repository, scenario.expected));
    TopologyTaskJanitorOptions options;
    options.scanLimit = 128;
    options.deleteBatch = 128;
    TopologyTaskJanitor janitor(*scenario.repository, scenario.algorithm, scenario.materializer, options);
    DS_ASSERT_OK(janitor.RunOnce());
    EXPECT_FALSE(DerivedRecordsExist(*scenario.repository, scenario.expected));
}

TEST(TopologyTaskJanitorTest, KeepsCurrentEpochRecordsAndDeletesNothingWhenTopologyReadFails)
{
    JanitorScenario current;
    DS_ASSERT_OK(current.SetUp(false));
    TopologyTaskJanitorOptions options;
    TopologyTaskJanitor keep(*current.repository, current.algorithm, current.materializer, options);
    DS_ASSERT_OK(keep.RunOnce());
    EXPECT_TRUE(DerivedRecordsExist(*current.repository, current.expected));

    JanitorScenario failed;
    DS_ASSERT_OK(failed.SetUp(true));
    TopologyTaskJanitor noDelete(*failed.repository, failed.algorithm, failed.materializer, options);
    failed.backend.FailNextGet();
    EXPECT_EQ(noDelete.RunOnce().GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_TRUE(DerivedRecordsExist(*failed.repository, failed.expected));
}

TEST(TopologyTaskJanitorTest, ConditionalCleanupPreservesConcurrentTaskAndNotifyWrites)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(true));
    std::vector<TaskJanitorCandidate> tasks;
    DS_ASSERT_OK(scenario.repository->ListTaskCandidatesForJanitor(TopologyTaskKind::MIGRATE, 16, tasks));
    ASSERT_FALSE(tasks.empty());
    const std::string concurrentTask = "concurrent-task-value";
    scenario.backend.PutBytes(scenario.keys->MigrateTaskTable(), tasks.front().taskId, concurrentTask);
    bool deleted = true;
    DS_ASSERT_OK(scenario.repository->DeleteTaskIfMatches(tasks.front(), deleted));
    EXPECT_FALSE(deleted);
    std::string observedTask;
    DS_ASSERT_OK(scenario.backend.Get(scenario.keys->MigrateTaskTable(), tasks.front().taskId, observedTask));
    EXPECT_EQ(observedTask, concurrentTask);

    std::vector<NotifyJanitorCandidate> notifies;
    DS_ASSERT_OK(scenario.repository->ListNotifyCandidatesForJanitor(16, notifies));
    ASSERT_FALSE(notifies.empty());
    auto concurrentNotify = notifies.front().notify;
    concurrentNotify.type = TopologyChangeType::SCALE_IN;
    DS_ASSERT_OK(scenario.repository->RewriteNotify(notifies.front().address, concurrentNotify));
    notifies.front().notify = {};
    deleted = true;
    DS_ASSERT_OK(scenario.repository->DeleteNotifyIfMatches(notifies.front(), deleted));
    EXPECT_FALSE(deleted);
    TopologyTaskNotify observedNotify;
    DS_ASSERT_OK(scenario.repository->ReadNotify(notifies.front().address, observedNotify));
    EXPECT_EQ(observedNotify.type, TopologyChangeType::SCALE_IN);
    EXPECT_EQ(observedNotify.taskIds, concurrentNotify.taskIds);
}

TEST(TopologyTaskJanitorTest, TaskDeleteTombstoneFencesConcurrentRematerialization)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(true));
    std::vector<TaskJanitorCandidate> tasks;
    DS_ASSERT_OK(scenario.repository->ListTaskCandidatesForJanitor(TopologyTaskKind::MIGRATE, 16, tasks));
    ASSERT_FALSE(tasks.empty());
    const auto task = scenario.expected.tasks.front();
    Status concurrentWrite;
    scenario.backend.SetBeforeDeleteHandler(
        [&] { concurrentWrite = scenario.repository->CreateTaskIfAbsent(task); });

    bool deleted = false;
    DS_ASSERT_OK(scenario.repository->DeleteTaskIfMatches(tasks.front(), deleted));
    EXPECT_TRUE(deleted);
    EXPECT_EQ(concurrentWrite.GetCode(), K_TRY_AGAIN);
    DS_ASSERT_OK(scenario.repository->CreateTaskIfAbsent(task));

    TopologyTask observed;
    const auto &migrate = std::get<TopologyMigrateTask>(task);
    DS_ASSERT_OK(scenario.repository->ReadTask(TopologyTaskKind::MIGRATE, migrate.taskId, migrate.type, migrate.epoch,
                                              observed));
}

TEST(TopologyTaskJanitorTest, PreservesFutureEpochTaskCreatedAfterItsTopologySnapshot)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("janitor-future", keys));
    TopologyState topology;
    topology.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    const std::string futureTaskId = "m-e2-" + std::string(32, 'a');
    const std::string futureTaskBytes = "future-epoch-task";
    backend.PutBytes(keys->MigrateTaskTable(), futureTaskId, futureTaskBytes);
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    TopologyTaskJanitor janitor(repository, algorithm, materializer, {});
    DS_ASSERT_OK(janitor.RunOnce());
    std::string observed;
    DS_ASSERT_OK(backend.Get(keys->MigrateTaskTable(), futureTaskId, observed));
    EXPECT_EQ(observed, futureTaskBytes);
}

TEST(TopologyTaskJanitorTest, PreservesUnexpectedTaskFromCurrentActiveEpoch)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(false));
    const auto epoch = scenario.plan.next.activeBatch->epoch;
    const std::string currentTaskId = "m-e" + std::to_string(epoch) + "-" + std::string(32, 'c');
    const std::string currentTaskBytes = "current-epoch-task";
    scenario.backend.PutBytes(scenario.keys->MigrateTaskTable(), currentTaskId, currentTaskBytes);
    TopologyTaskJanitor janitor(*scenario.repository, scenario.algorithm, scenario.materializer, {});
    DS_ASSERT_OK(janitor.RunOnce());
    std::string observed;
    DS_ASSERT_OK(scenario.backend.Get(scenario.keys->MigrateTaskTable(), currentTaskId, observed));
    EXPECT_EQ(observed, currentTaskBytes);
}

}  // namespace
}  // namespace datasystem::cluster
