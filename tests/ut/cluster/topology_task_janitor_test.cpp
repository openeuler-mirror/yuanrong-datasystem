/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Backend-neutral stale topology task Janitor tests.
 */
#include "datasystem/cluster/control/topology_task_janitor.h"

#include <algorithm>
#include <vector>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_plan_builder.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

std::vector<uint32_t> MakeMemberTokens(const std::string &address)
{
    constexpr uint32_t tokenCount = 4;
    std::vector<uint32_t> tokens;
    tokens.reserve(tokenCount);
    for (uint32_t index = 0; index < tokenCount; ++index) {
        tokens.emplace_back(HashAlgorithm::MakeToken(address, index, 0));
    }
    return tokens;
}

struct JanitorScenario {
    Status SetUp(bool finalize)
    {
        RETURN_IF_NOT_OK(TopologyKeyHelper::Create("janitor", keys));
        repository = std::make_unique<TopologyRepository>(backend, *keys);
        TopologyState current;
        current.version = 1;
        current.clusterHasInit = true;
        current.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE,
                                    MakeMemberTokens("127.0.0.1:1") },
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
    const auto &migrate = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    const std::string sourceId(16, 'a');
    DS_ASSERT_OK(scenario.repository->MarkScaleInMetadataDone({ migrate.epoch, sourceId, migrate.taskId, "op" }));
    TopologyTaskJanitorOptions options;
    options.scanLimit = 128;
    options.deleteBatch = 128;
    TopologyTaskJanitor janitor(
        "janitor", *scenario.repository, scenario.algorithm, scenario.materializer, options);
    DS_ASSERT_OK(janitor.RunOnce());
    EXPECT_FALSE(DerivedRecordsExist(*scenario.repository, scenario.expected));
    std::vector<std::pair<std::string, std::string>> physicalNotifies;
    DS_ASSERT_OK(scenario.backend.GetAll(scenario.keys->NotifyTable(), physicalNotifies));
    EXPECT_TRUE(physicalNotifies.empty());
    size_t markerCount = 1;
    DS_ASSERT_OK(scenario.repository->CountScaleInMetadataDone(migrate.epoch, sourceId, markerCount));
    EXPECT_EQ(markerCount, 0);
}

TEST(TopologyTaskJanitorTest, KeepsCurrentEpochRecordsAndDeletesNothingWhenTopologyReadFails)
{
    JanitorScenario current;
    DS_ASSERT_OK(current.SetUp(false));
    TopologyTaskJanitorOptions options;
    TopologyTaskJanitor keep("janitor", *current.repository, current.algorithm, current.materializer, options);
    const auto &migrate = std::get<TopologyMigrateTask>(current.expected.tasks.front());
    const std::string sourceId(16, 'a');
    DS_ASSERT_OK(current.repository->MarkScaleInMetadataDone({ migrate.epoch, sourceId, migrate.taskId, "op" }));
    DS_ASSERT_OK(keep.RunOnce());
    EXPECT_TRUE(DerivedRecordsExist(*current.repository, current.expected));
    size_t markerCount = 0;
    DS_ASSERT_OK(current.repository->CountScaleInMetadataDone(migrate.epoch, sourceId, markerCount));
    EXPECT_EQ(markerCount, 1);

    JanitorScenario failed;
    DS_ASSERT_OK(failed.SetUp(true));
    TopologyTaskJanitor noDelete("janitor", *failed.repository, failed.algorithm, failed.materializer, options);
    failed.backend.FailNextGet();
    EXPECT_EQ(noDelete.RunOnce().GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_TRUE(DerivedRecordsExist(*failed.repository, failed.expected));
}

TEST(TopologyTaskJanitorTest, MetadataMarkerDeleteBatchCountsOnlySuccessfulDeletes)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("janitor-marker", keys));
    TopologyRepository repository(backend, *keys);
    TopologyState topology;
    topology.version = 9;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    const std::string sourceId(16, 'a');
    const std::string firstTaskId = "m-e7-" + std::string(32, 'a');
    const std::string secondTaskId = "m-e7-" + std::string(32, 'b');
    DS_ASSERT_OK(repository.MarkScaleInMetadataDone({ 7, sourceId, firstTaskId, "operation-a" }));
    DS_ASSERT_OK(repository.MarkScaleInMetadataDone({ 7, sourceId, secondTaskId, "operation-b" }));
    std::vector<ScaleInMetadataDoneJanitorCandidate> markers;
    std::string markerCursor;
    DS_ASSERT_OK(repository.ListScaleInMetadataDoneCandidatesForJanitor(8, markerCursor, markers));
    ASSERT_GE(markers.size(), 2);
    const auto concurrentKey = markers.front().key;
    backend.SetBeforeCasHandler([&] {
        backend.PutBytes(keys->ScaleInMetadataDoneTable(), concurrentKey, "operation-concurrent");
    });
    TopologyTaskJanitorOptions options;
    options.scanLimit = 8;
    options.deleteBatch = 1;
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    TopologyTaskJanitor janitor("janitor-marker", repository, algorithm, materializer, options);

    DS_ASSERT_OK(janitor.RunOnce());

    size_t markerCount = 0;
    DS_ASSERT_OK(repository.CountScaleInMetadataDone(7, sourceId, markerCount));
    EXPECT_EQ(markerCount, 1);
}

TEST(TopologyTaskJanitorTest, ConditionalCleanupPreservesConcurrentTaskAndNotifyWrites)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(true));
    std::vector<TaskJanitorCandidate> tasks;
    std::string taskCursor;
    DS_ASSERT_OK(scenario.repository->ListTaskCandidatesForJanitor(TopologyTaskKind::MIGRATE, 16, taskCursor, tasks));
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
    std::string notifyCursor;
    DS_ASSERT_OK(scenario.repository->ListNotifyCandidatesForJanitor(16, notifyCursor, notifies));
    ASSERT_FALSE(notifies.empty());
    auto concurrentNotify = notifies.front().notify;
    ASSERT_TRUE(concurrentNotify.activeBatch.has_value());
    concurrentNotify.activeBatch->type = TopologyChangeType::SCALE_IN;
    DS_ASSERT_OK(scenario.repository->RewriteNotify(notifies.front().address, concurrentNotify));
    notifies.front().notify = {};
    deleted = true;
    DS_ASSERT_OK(scenario.repository->ReconcileNotifyIfMatches(notifies.front(), deleted));
    EXPECT_FALSE(deleted);
    TopologyTaskNotify observedNotify;
    DS_ASSERT_OK(scenario.repository->ReadNotify(notifies.front().address, observedNotify));
    ASSERT_TRUE(observedNotify.activeBatch.has_value());
    EXPECT_EQ(observedNotify.activeBatch->type, TopologyChangeType::SCALE_IN);
    EXPECT_EQ(observedNotify.taskIds, concurrentNotify.taskIds);
}

TEST(TopologyTaskJanitorTest, NotifyDeleteTombstoneFencesConcurrentRematerialization)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(true));
    std::vector<NotifyJanitorCandidate> notifies;
    std::string notifyCursor;
    DS_ASSERT_OK(scenario.repository->ListNotifyCandidatesForJanitor(16, notifyCursor, notifies));
    ASSERT_FALSE(notifies.empty());
    const auto address = notifies.front().address;
    const auto expected = notifies.front().notify;
    notifies.front().notify = {};
    Status staleDeleteStatus;
    bool staleChanged = true;
    Status concurrentStatus;
    scenario.backend.SetBeforeDeleteHandler([&] {
        staleDeleteStatus = scenario.repository->ReconcileNotifyIfMatches(notifies.front(), staleChanged);
        concurrentStatus = scenario.repository->RewriteNotify(address, expected);
    });
    bool changed = false;

    DS_ASSERT_OK(scenario.repository->ReconcileNotifyIfMatches(notifies.front(), changed));

    EXPECT_TRUE(changed);
    EXPECT_TRUE(staleDeleteStatus.IsOk());
    EXPECT_FALSE(staleChanged);
    EXPECT_EQ(concurrentStatus.GetCode(), K_TRY_AGAIN);
    TopologyTaskNotify observed;
    EXPECT_EQ(scenario.repository->ReadNotify(address, observed).GetCode(), K_NOT_FOUND);
    DS_ASSERT_OK(scenario.repository->RewriteNotify(address, expected));
    DS_ASSERT_OK(scenario.repository->ReadNotify(address, observed));
    EXPECT_EQ(observed.taskIds, expected.taskIds);
}

TEST(TopologyTaskJanitorTest, LegacyEmptyNotifiesCannotStarveLaterCandidates)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(true));
    constexpr size_t legacyTombstoneCount = 9;
    for (size_t index = 0; index < legacyTombstoneCount; ++index) {
        scenario.backend.PutBytes(scenario.keys->NotifyTable(),
                                  "127.0.0.1:" + std::to_string(10'000 + index), "");
    }
    TopologyTaskJanitorOptions options;
    options.scanLimit = 4;
    options.deleteBatch = 4;
    TopologyTaskJanitor janitor("janitor", *scenario.repository, scenario.algorithm, scenario.materializer, options);

    for (size_t pass = 0; pass < 4; ++pass) {
        DS_ASSERT_OK(janitor.RunOnce());
    }

    std::vector<std::pair<std::string, std::string>> physicalNotifies;
    DS_ASSERT_OK(scenario.backend.GetAll(scenario.keys->NotifyTable(), physicalNotifies));
    EXPECT_TRUE(physicalNotifies.empty());
}

TEST(TopologyTaskJanitorTest, ThirtyGenerationsLeaveNoPhysicalDerivedKeys)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("janitor-generations", keys));
    TopologyState topology;
    topology.version = 100;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    TopologyTaskJanitor janitor("janitor-generations", repository, algorithm, materializer, {});
    constexpr size_t generationCount = 30;
    const std::string sourceId(16, 's');
    for (size_t generation = 1; generation <= generationCount; ++generation) {
        const auto suffix = std::string(32, static_cast<char>('a' + generation % 6));
        const auto taskId = "m-e" + std::to_string(generation) + "-" + suffix;
        backend.PutBytes(keys->MigrateTaskTable(), taskId, "stale-task");
        TopologyTaskNotify notify;
        notify.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, generation };
        notify.taskIds = { taskId };
        DS_ASSERT_OK(repository.RewriteNotify("127.0.0.1:" + std::to_string(10'000 + generation), notify));
        DS_ASSERT_OK(repository.MarkScaleInMetadataDone({ generation, sourceId, taskId, "operation" }));
        DS_ASSERT_OK(janitor.RunOnce());
    }

    std::vector<std::pair<std::string, std::string>> physicalRecords;
    DS_ASSERT_OK(backend.GetAll(keys->MigrateTaskTable(), physicalRecords));
    EXPECT_TRUE(physicalRecords.empty());
    DS_ASSERT_OK(backend.GetAll(keys->NotifyTable(), physicalRecords));
    EXPECT_TRUE(physicalRecords.empty());
    DS_ASSERT_OK(backend.GetAll(keys->ScaleInMetadataDoneTable(), physicalRecords));
    EXPECT_TRUE(physicalRecords.empty());
}

TEST(TopologyTaskJanitorTest, RotatingScanEventuallyReachesStaleSuffix)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("janitor-rotation", keys));
    TopologyState topology;
    topology.version = 2;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    constexpr size_t immutablePrefixCount = 6;
    for (size_t index = 0; index < immutablePrefixCount; ++index) {
        backend.PutBytes(keys->MigrateTaskTable(), "a-unparseable-" + std::to_string(index), "keep");
    }
    const std::string staleTaskId = "m-e1-" + std::string(32, 'a');
    backend.PutBytes(keys->MigrateTaskTable(), staleTaskId, "stale");
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    TopologyTaskJanitorOptions options;
    options.scanLimit = 4;
    options.deleteBatch = 4;
    TopologyTaskJanitor janitor("janitor-rotation", repository, algorithm, materializer, options);

    DS_ASSERT_OK(janitor.RunOnce());
    std::string observed;
    DS_ASSERT_OK(backend.Get(keys->MigrateTaskTable(), staleTaskId, observed));
    DS_ASSERT_OK(janitor.RunOnce());
    EXPECT_EQ(backend.Get(keys->MigrateTaskTable(), staleTaskId, observed).GetCode(), K_NOT_FOUND);
}

TEST(TopologyTaskJanitorTest, PreservesRestartFactsOnlyForPresentMembers)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(true));
    const std::string presentAddress = "127.0.0.1:9";
    const std::string absentAddress = "127.0.0.1:10";
    const std::string presentRestartAddress = "127.0.0.1:2";
    TopologyTaskNotify presentNotify;
    presentNotify.restartTimestampsByAddress.emplace(presentRestartAddress, 100);
    presentNotify.restartTimestampsByAddress.emplace(absentAddress, 200);
    DS_ASSERT_OK(scenario.repository->RewriteNotify(presentAddress, presentNotify));
    DS_ASSERT_OK(scenario.repository->RewriteNotify(absentAddress, presentNotify));
    MembershipValue membership{ 1, MemberLifecycleState::READY, "", "" };
    std::string encodedMembership;
    DS_ASSERT_OK(MembershipValueCodec::Encode(membership, encodedMembership));
    scenario.backend.PutBytes(scenario.keys->MembershipTable(), presentAddress, encodedMembership);
    scenario.backend.PutBytes(scenario.keys->MembershipTable(), presentRestartAddress, encodedMembership);
    TopologyTaskJanitor janitor(
        "janitor", *scenario.repository, scenario.algorithm, scenario.materializer, {});

    DS_ASSERT_OK(janitor.RunOnce());

    TopologyTaskNotify observed;
    DS_ASSERT_OK(scenario.repository->ReadNotify(presentAddress, observed));
    EXPECT_EQ(observed.restartTimestampsByAddress,
              (std::map<std::string, int64_t>{ { presentRestartAddress, 100 } }));
    EXPECT_EQ(scenario.repository->ReadNotify(absentAddress, observed).GetCode(), K_NOT_FOUND);
}

TEST(TopologyTaskJanitorTest, TaskDeleteTombstoneFencesConcurrentRematerialization)
{
    JanitorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(true));
    std::vector<TaskJanitorCandidate> tasks;
    std::string taskCursor;
    DS_ASSERT_OK(scenario.repository->ListTaskCandidatesForJanitor(TopologyTaskKind::MIGRATE, 16, taskCursor, tasks));
    ASSERT_FALSE(tasks.empty());
    const auto task = scenario.expected.tasks.front();
    Status staleDeleteStatus;
    bool staleDeleted = true;
    Status concurrentWrite;
    scenario.backend.SetBeforeDeleteHandler([&] {
        staleDeleteStatus = scenario.repository->DeleteTaskIfMatches(tasks.front(), staleDeleted);
        concurrentWrite = scenario.repository->CreateTaskIfAbsent(task);
    });

    bool deleted = false;
    DS_ASSERT_OK(scenario.repository->DeleteTaskIfMatches(tasks.front(), deleted));
    EXPECT_TRUE(deleted);
    EXPECT_TRUE(staleDeleteStatus.IsOk());
    EXPECT_FALSE(staleDeleted);
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
    TopologyTaskJanitor janitor("janitor", repository, algorithm, materializer, {});
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
    TopologyTaskJanitor janitor(
        "janitor", *scenario.repository, scenario.algorithm, scenario.materializer, {});
    DS_ASSERT_OK(janitor.RunOnce());
    std::string observed;
    DS_ASSERT_OK(scenario.backend.Get(scenario.keys->MigrateTaskTable(), currentTaskId, observed));
    EXPECT_EQ(observed, currentTaskBytes);
}

}  // namespace
}  // namespace datasystem::cluster
