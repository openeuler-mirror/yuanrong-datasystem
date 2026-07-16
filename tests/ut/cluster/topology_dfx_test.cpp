/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology DFX healing and cascade-failure tests.
 */
#include "datasystem/common/flags/common_flags.h"  // FLAGS_use_brpc
#include "datasystem/cluster/control/topology_controller.h"

#include <algorithm>
#include <atomic>
#include <functional>
#include <thread>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {
constexpr auto DFX_WAIT = std::chrono::seconds(1);
constexpr size_t DFX_QUEUE_CAPACITY = 32;
constexpr int32_t DFX_READ_TIMEOUT_MS = 100;
constexpr size_t RECONCILE_CLOCK_READS = 4;
constexpr int64_t EXPIRED_ORDINARY_SECONDS = 120;
constexpr int64_t ORDINARY_BUDGET_OBSERVATIONS = 90;

struct ManualDfxClock {
    std::chrono::steady_clock::time_point Read()
    {
        ++reads;
        return std::chrono::steady_clock::time_point(std::chrono::seconds(seconds.load()));
    }
    std::atomic<int64_t> seconds{ 0 };
    std::atomic<size_t> reads{ 0 };
};

void PutDfxMembership(FakeCoordinationBackend &backend, const TopologyKeyHelper &keys, const std::string &address,
                      MemberLifecycleState state, int64_t timestamp = 0, std::string hostId = {})
{
    MembershipValue value;
    value.lifecycleState = state;
    value.timestamp = timestamp;
    value.hostId = std::move(hostId);
    std::string bytes;
    DS_ASSERT_OK(MembershipValueCodec::Encode(value, bytes));
    backend.PutBytes(keys.MembershipTable(), address, std::move(bytes));
}

bool WaitDfxTopology(TopologyRepository &repository, const std::function<bool(const TopologyState &)> &predicate)
{
    const auto deadline = std::chrono::steady_clock::now() + DFX_WAIT;
    while (std::chrono::steady_clock::now() < deadline) {
        TopologyState state;
        int64_t revision = 0;
        if (repository.ReadTopology(DFX_READ_TIMEOUT_MS, state, revision).IsOk() && predicate(state)) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool WaitDfxClockReads(const std::shared_ptr<ManualDfxClock> &clock, size_t target)
{
    const auto deadline = std::chrono::steady_clock::now() + DFX_WAIT;
    while (clock->reads.load() < target && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    return clock->reads.load() >= target;
}

TopologyState MakeDfxTopology()
{
    TopologyState state;
    state.version = 1;
    state.clusterHasInit = true;
    state.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1, 100 } },
                      Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 50, 150 } } };
    return state;
}

TopologyControllerOptions FastFailureOptions()
{
    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureBatchWindow = std::chrono::seconds(3);
    options.reconcileTick = std::chrono::milliseconds(1);
    auto clock = std::make_shared<std::atomic<int64_t>>(0);
    options.now = [clock] { return std::chrono::steady_clock::time_point(std::chrono::seconds(clock->fetch_add(1))); };
    return options;
}

TopologyControllerOptions AutoBudgetOptions(const std::shared_ptr<std::atomic<int64_t>> &clock)
{
    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(30);
    options.ordinaryBatchWindow = std::chrono::minutes(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.now = [clock] { return std::chrono::steady_clock::time_point(std::chrono::seconds(clock->fetch_add(1))); };
    return options;
}

TopologyControllerOptions ManualBudgetOptions(const std::shared_ptr<ManualDfxClock> &clock)
{
    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(30);
    options.ordinaryBatchWindow = std::chrono::minutes(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.now = [clock] { return clock->Read(); };
    return options;
}

void FinishScaleOutTarget(FakeCoordinationBackend &backend, const TopologyKeyHelper &keys,
                          const ExpectedDerivedState &expected, const std::string &targetAddress)
{
    for (const auto &task : expected.tasks) {
        if (!std::holds_alternative<TopologyMigrateTask>(task)) {
            continue;
        }
        auto migrate = std::get<TopologyMigrateTask>(task);
        if (migrate.targetAddress != targetAddress) {
            continue;
        }
        for (auto &range : migrate.sourceRanges) {
            range.finished = true;
        }
        std::string bytes;
        DS_ASSERT_OK(TopologyRepositoryCodec::EncodeMigrateTask(migrate, bytes));
        backend.PutBytes(keys.MigrateTaskTable(), migrate.taskId, std::move(bytes));
    }
}

TEST(TopologyDfxTest, FailureWindowFinalizesUnfinishedRecoveryAndAllowsLaterAdmission)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("failure-final", keys));
    TopologyRepository repository(backend, *keys);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeDfxTopology());
    PutDfxMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(DFX_QUEUE_CAPACITY);
    auto options = FastFailureOptions();
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());
    EXPECT_TRUE(WaitDfxTopology(repository, [](const auto &state) {
        return !state.activeBatch.has_value() && state.members.size() == 1
               && state.members.front().identity.address == "127.0.0.1:2";
    }));
    PutDfxMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    EXPECT_TRUE(WaitDfxTopology(repository, [](const auto &state) {
        return std::any_of(state.members.begin(), state.members.end(),
                           [](const auto &member) { return member.identity.address == "127.0.0.1:3"; });
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
}

TEST(TopologyDfxTest, FailureExecutorCrashReplansWithoutExtendingTheSharedWindow)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("cascade", keys));
    TopologyRepository repository(backend, *keys);
    auto state = MakeDfxTopology();
    state.members.emplace_back(Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::ACTIVE, { 25, 125 } });
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    PutDfxMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    PutDfxMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(DFX_QUEUE_CAPACITY);
    auto options = FastFailureOptions();
    options.failureBatchWindow = std::chrono::seconds(30);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(WaitDfxTopology(repository, [](const auto &latest) {
        return latest.activeBatch.has_value() && latest.activeBatch->type == TopologyChangeType::FAILURE
               && std::count_if(latest.members.begin(), latest.members.end(), [](const auto &member) {
                      return member.state == MemberState::FAILED;
                  }) == 1;
    }));
    DS_ASSERT_OK(backend.Delete(keys->MembershipTable(), "127.0.0.1:2"));
    EXPECT_TRUE(WaitDfxTopology(repository, [](const auto &latest) {
        return !latest.activeBatch.has_value() && latest.members.size() == 1
               && latest.members.front().identity.address == "127.0.0.1:3";
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
}

TEST(TopologyDfxTest, ScaleOutBudgetRemovesFailedGenerationAndAllowsReadmission)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("scaleout-exhaust", keys));
    TopologyRepository repository(backend, *keys);
    auto state = MakeDfxTopology();
    state.members.pop_back();
    const std::string failedId(16, 'b');
    state.members.push_back({ { failedId, "127.0.0.1:2" }, MemberState::INITIAL, {} });
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    PutDfxMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    PutDfxMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(DFX_QUEUE_CAPACITY);
    auto clock = std::make_shared<std::atomic<int64_t>>(0);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, AutoBudgetOptions(clock));
    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(WaitDfxTopology(repository, [&](const auto &latest) {
        return latest.activeBatch.has_value() && latest.activeBatch->type == TopologyChangeType::SCALE_OUT;
    }));
    EXPECT_TRUE(WaitDfxTopology(repository, [&](const auto &latest) {
        return std::none_of(latest.members.begin(), latest.members.end(),
                            [&](const auto &member) { return member.identity.id == failedId; });
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
}

TEST(TopologyDfxTest, MultiMemberScaleOutReplansRetainedCompletionAndRemovesOnlyFailedGeneration)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("scaleout-partial", keys));
    TopologyRepository repository(backend, *keys);
    auto state = MakeDfxTopology();
    state.members.pop_back();
    state.members.push_back({ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::INITIAL, {} });
    state.members.push_back({ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::INITIAL, {} });
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    for (const auto &member : state.members) {
        PutDfxMembership(backend, *keys, member.identity.address, MemberLifecycleState::READY);
    }
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(DFX_QUEUE_CAPACITY);
    auto clock = std::make_shared<ManualDfxClock>();
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, ManualBudgetOptions(clock));
    DS_ASSERT_OK(controller.Start());
    const bool activeBatch = WaitDfxTopology(repository, [](const auto &latest) {
        return latest.activeBatch.has_value() && latest.activeBatch->type == TopologyChangeType::SCALE_OUT;
    });
    EXPECT_TRUE(activeBatch);
    if (!activeBatch) {
        DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
        return;
    }
    TopologyReader reader(repository);
    std::shared_ptr<const TopologySnapshot> active;
    DS_ASSERT_OK(reader.Read(DFX_READ_TIMEOUT_MS, active));
    ExpectedDerivedState expected;
    TopologyTaskMaterializer materializer;
    DS_ASSERT_OK(materializer.RebuildExpected(*active, algorithm, expected));
    FinishScaleOutTarget(backend, *keys, expected, "127.0.0.1:2");
    const auto oldEpoch = active->GetActiveBatch()->epoch;
    const auto observedReads = clock->reads.load();
    const bool reconciled = WaitDfxClockReads(clock, observedReads + RECONCILE_CLOCK_READS);
    EXPECT_TRUE(reconciled);
    if (!reconciled) {
        DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
        return;
    }
    clock->seconds.store(EXPIRED_ORDINARY_SECONDS);
    EXPECT_TRUE(WaitDfxTopology(repository, [&](const auto &latest) {
        const auto kept = std::find_if(latest.members.begin(), latest.members.end(),
                                       [](const auto &member) { return member.identity.id == std::string(16, 'b'); });
        const bool removed = std::none_of(latest.members.begin(), latest.members.end(), [](const auto &member) {
            return member.identity.id == std::string(16, 'c');
        });
        return kept != latest.members.end() && kept->state == MemberState::JOINING && removed
               && latest.activeBatch.has_value() && latest.activeBatch->epoch > oldEpoch;
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
}

TEST(TopologyDfxTest, ScaleInBudgetKeepsLeavingUntilExternalTerminationBecomesFailure)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("scalein-exhaust", keys));
    TopologyRepository repository(backend, *keys);
    auto state = MakeDfxTopology();
    state.members.front().state = MemberState::PRE_LEAVING;
    HashAlgorithm algorithm;
    TopologyPlanBuilder builder(algorithm);
    TopologyPlan plan;
    DS_ASSERT_OK(builder.BuildScaleInStart(state, { state.members.front().identity }, plan));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), plan.next);
    PutDfxMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    PutDfxMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    CoordinationEventDispatcher dispatcher(DFX_QUEUE_CAPACITY);
    auto clock = std::make_shared<std::atomic<int64_t>>(0);
    auto options = AutoBudgetOptions(clock);
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureBatchWindow = std::chrono::seconds(3);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());
    const auto budgetDeadline = std::chrono::steady_clock::now() + DFX_WAIT;
    while (clock->load() < ORDINARY_BUDGET_OBSERVATIONS && std::chrono::steady_clock::now() < budgetDeadline) {
        std::this_thread::yield();
    }
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(DFX_READ_TIMEOUT_MS, observed, revision));
    EXPECT_TRUE(observed.activeBatch.has_value());
    EXPECT_EQ(observed.activeBatch->type, TopologyChangeType::SCALE_IN);
    EXPECT_EQ(observed.members.front().state, MemberState::LEAVING);
    DS_ASSERT_OK(backend.Delete(keys->MembershipTable(), "127.0.0.1:1"));
    EXPECT_TRUE(WaitDfxTopology(repository, [](const auto &latest) {
        return !latest.activeBatch.has_value() && latest.members.size() == 1
               && latest.members.front().identity.address == "127.0.0.1:2";
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
}

TEST(TopologyDfxTest, AllExitingMembersUseClusterShutdownWithoutDerivedTasks)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("all-out", keys));
    TopologyRepository repository(backend, *keys);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeDfxTopology());
    PutDfxMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutDfxMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::EXITING);
    PutDfxMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(DFX_QUEUE_CAPACITY);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());
    EXPECT_TRUE(WaitDfxTopology(repository, [](const auto &latest) {
        return latest.clusterHasInit && latest.members.empty() && !latest.activeBatch.has_value();
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
    std::vector<TaskJanitorCandidate> tasks;
    DS_ASSERT_OK(repository.ListTaskCandidatesForJanitor(TopologyTaskKind::MIGRATE, 16, tasks));
    EXPECT_TRUE(tasks.empty());
}

TEST(TopologyDfxTest, ReadyMembershipGenerationProducesDeterministicMemberId)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("member-id", keys));
    TopologyRepository repository(backend, *keys);
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    constexpr int64_t timestamp = 123;
    PutDfxMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY, timestamp, "host-a");
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(DFX_QUEUE_CAPACITY);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());
    std::string observedId;
    EXPECT_TRUE(WaitDfxTopology(repository, [&](const auto &latest) {
        if (latest.members.empty()) {
            return false;
        }
        observedId = latest.members.front().identity.id;
        return true;
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + DFX_WAIT));

    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    CoordinationEventDispatcher secondDispatcher(DFX_QUEUE_CAPACITY);
    TopologyController second(backend, repository, *keys, algorithm, secondDispatcher, options);
    DS_ASSERT_OK(second.Start());
    EXPECT_TRUE(WaitDfxTopology(repository, [&](const auto &latest) {
        return !latest.members.empty() && latest.members.front().identity.id == observedId;
    }));
    DS_ASSERT_OK(second.Stop(std::chrono::steady_clock::now() + DFX_WAIT));
}

}  // namespace
}  // namespace datasystem::cluster
