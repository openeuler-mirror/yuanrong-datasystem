/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Multi-instance CAS cluster topology Controller tests.
 */
#include "datasystem/cluster/control/topology_controller.h"

#include <algorithm>
#include <atomic>
#include <functional>
#include <stdexcept>
#include <thread>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {
constexpr size_t RECOVERY_DISPATCHER_CAPACITY = 32;
constexpr std::chrono::milliseconds RECOVERY_RECONCILE_TICK{ 10 };
constexpr size_t INJECTED_NOT_READY_GET_ATTEMPTS = 5;
constexpr size_t BOOTSTRAP_SUCCESS_GET_ATTEMPTS = 1;
constexpr size_t EXPECTED_RECOVERY_GET_ATTEMPTS =
    INJECTED_NOT_READY_GET_ATTEMPTS + BOOTSTRAP_SUCCESS_GET_ATTEMPTS;
constexpr std::chrono::milliseconds RECOVERY_WAIT_TIMEOUT{ 150 };
constexpr std::chrono::seconds RECOVERY_STOP_TIMEOUT{ 1 };
constexpr auto FAILURE_PROBE_WAIT_TIMEOUT = std::chrono::seconds(1);

std::string EncodeMembership(MemberLifecycleState state, int64_t timestamp = 0)
{
    MembershipValue value{ timestamp, state, "", "" };
    std::string bytes;
    EXPECT_TRUE(MembershipValueCodec::Encode(value, bytes).IsOk());
    return bytes;
}

void PutMembership(FakeCoordinationBackend &backend, const TopologyKeyHelper &keys, const std::string &address,
                   MemberLifecycleState state)
{
    backend.PutBytes(keys.MembershipTable(), address, EncodeMembership(state));
}

bool WaitForTopology(TopologyRepository &repository, std::chrono::steady_clock::time_point deadline,
                     const std::function<bool(const TopologyState &)> &predicate)
{
    while (std::chrono::steady_clock::now() < deadline) {
        TopologyState state;
        int64_t revision = 0;
        if (repository.ReadTopology(100, state, revision).IsOk() && predicate(state)) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool WaitForDerivedState(TopologyRepository &repository, const ExpectedDerivedState &expected,
                         std::chrono::steady_clock::time_point deadline)
{
    while (std::chrono::steady_clock::now() < deadline) {
        bool complete = true;
        for (const auto &task : expected.tasks) {
            const auto &id = std::visit([](const auto &value) -> const std::string & { return value.taskId; }, task);
            const auto kind = std::holds_alternative<TopologyMigrateTask>(task) ? TopologyTaskKind::MIGRATE
                                                                                : TopologyTaskKind::DELETE_MEMBER;
            TopologyTask observed;
            complete = complete
                       && repository
                              .ReadTask(kind, id, expected.notifiesByAddress.begin()->second.type,
                                        std::visit([](const auto &value) { return value.epoch; }, task), observed)
                              .IsOk();
        }
        for (const auto &[address, notify] : expected.notifiesByAddress) {
            TopologyTaskNotify observed;
            complete = complete && repository.ReadNotify(address, observed).IsOk() && observed.type == notify.type
                       && observed.taskIds == notify.taskIds;
        }
        if (complete) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

TEST(TopologyControllerTest, TwoInstancesCanJoinOneCommittedEpochWithoutPersistedOwner)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("controller", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher firstDispatcher(32);
    CoordinationEventDispatcher secondDispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.maxDerivedOperationsPerTick = 1;
    TopologyController first(backend, repository, *keys, algorithm, firstDispatcher, options);
    TopologyController second(backend, repository, *keys, algorithm, secondDispatcher, options);
    TopologyState current;
    current.version = 1;
    current.clusterHasInit = true;
    current.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    TopologyPlan plan;
    DS_ASSERT_OK(
        algorithm.PlanScaleOut({ current, { MemberIdentity{ std::string(16, 'b'), "127.0.0.1:2" } }, 4 }, plan));
    plan.next.version = 2;
    plan.next.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(plan.next, 1, std::string(64, 'a'), snapshot));
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    DS_ASSERT_OK(materializer.BuildExpected(*snapshot, plan, expected));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), plan.next);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);

    DS_ASSERT_OK(first.Start());
    DS_ASSERT_OK(second.Start());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    EXPECT_TRUE(WaitForDerivedState(repository, expected, deadline));
    DS_ASSERT_OK(first.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
    DS_ASSERT_OK(second.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
    EXPECT_FALSE(first.GetDiagnostics().running);
    EXPECT_FALSE(second.GetDiagnostics().running);
}

TEST(TopologyControllerTest, BootstrapsAllReadyInitialMembers)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("bootstrap", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest;
    latest.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    EXPECT_TRUE(WaitForTopology(repository, deadline, [](const auto &state) {
        return state.version >= 3 && state.clusterHasInit && state.members.size() == 2
               && std::all_of(state.members.begin(), state.members.end(),
                              [](const auto &member) { return member.state == MemberState::ACTIVE; });
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, RecoveryNotReadyDoesNotBackoffTopologyBootstrap)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("bootstrap-recovering", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    TopologyControllerOptions options;
    options.reconcileTick = RECOVERY_RECONCILE_TICK;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.ReturnNotReadyOnNextGets(INJECTED_NOT_READY_GET_ATTEMPTS);

    DS_ASSERT_OK(controller.Start());
    EXPECT_TRUE(
        backend.WaitForGetAttempts(EXPECTED_RECOVERY_GET_ATTEMPTS,
                                   std::chrono::steady_clock::now() + RECOVERY_WAIT_TIMEOUT));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, ConvertsExitingMembershipToOneScaleInBatch)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("scale-in", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    // 0ms disables coalescing: legacy immediate-batch admission within one reconcile.
    options.scaleInCollectWindow = std::chrono::milliseconds(0);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    EXPECT_TRUE(WaitForTopology(repository, deadline, [](const auto &state) {
        return state.activeBatch.has_value() && state.activeBatch->type == TopologyChangeType::SCALE_IN
               && state.members.front().state == MemberState::LEAVING;
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, StartsReadyInitialScaleOutBeforePendingScaleIn)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("scale-out-before-scale-in", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::PRE_LEAVING, { 1 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::INITIAL, {} },
    };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    EXPECT_TRUE(WaitForTopology(repository, deadline, [](const auto &state) {
        return state.activeBatch.has_value() && state.activeBatch->type == TopologyChangeType::SCALE_OUT
               && std::any_of(state.members.begin(), state.members.end(), [](const auto &member) {
                      return member.identity.address == "127.0.0.1:2" && member.state == MemberState::JOINING;
                  });
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, PreservesRestartGenerationAcrossDoorbellCoalescing)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("restart-event", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    std::atomic<size_t> restartCalls{ 0 };
    options.membershipRestartHandler = [&](const std::string &address, int64_t timestamp) {
        EXPECT_EQ(address, "127.0.0.1:2");
        EXPECT_EQ(timestamp, 123);
        ++restartCalls;
        return Status::OK();
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());

    const std::string eventKey = keys->MembershipTable() + "/127.0.0.1:2";
    backend.EmitEvent(
        { CoordinationEventType::PUT, eventKey, EncodeMembership(MemberLifecycleState::RESTARTING, 123), 1, 2 });
    backend.EmitEvent(
        { CoordinationEventType::PUT, eventKey, EncodeMembership(MemberLifecycleState::RECOVERING, 124), 2, 3 });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (std::chrono::steady_clock::now() < deadline && restartCalls.load() == 0) {
        std::this_thread::yield();
    }
    EXPECT_EQ(restartCalls.load(), 1);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// Shared initial state for scale-in coalescing tests: two ACTIVE members, first one exiting.
TopologyState MakeTwoActiveMembersScaleInInitialState()
{
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    return latest;
}

// A third READY member keeps multi-member scale-in tests out of the cluster-shutdown path.
TopologyState MakeThreeActiveMembersScaleInInitialState()
{
    TopologyState latest = MakeTwoActiveMembersScaleInInitialState();
    latest.members.push_back(Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::ACTIVE, { 3 } });
    return latest;
}

// Wait until topology reports no active batch or the deadline elapses.
bool WaitForNoActiveBatch(TopologyRepository &repository, std::chrono::steady_clock::time_point deadline)
{
    return WaitForTopology(repository, deadline, [](const auto &state) { return !state.activeBatch.has_value(); });
}

bool ActiveScaleInBatchHasLeavingCount(TopologyRepository &repository,
                                       std::chrono::steady_clock::time_point deadline, size_t leavingCount)
{
    return WaitForTopology(repository, deadline, [leavingCount](const auto &state) {
        if (!state.activeBatch.has_value() || state.activeBatch->type != TopologyChangeType::SCALE_IN) {
            return false;
        }
        return static_cast<size_t>(std::count_if(state.members.begin(), state.members.end(),
                                                  [](const auto &m) { return m.state == MemberState::LEAVING; }))
               == leavingCount;
    });
}

// 1. Default window: batch is not started before the collect deadline elapses.
TEST(TopologyControllerTest, ScaleInCollectWindowHoldsBatchBeforeDeadline)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-hold", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.scaleInCollectWindow = std::chrono::milliseconds(500);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeTwoActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    // Within a window shorter than the collect deadline, no SCALE_IN batch must be started.
    EXPECT_TRUE(WaitForNoActiveBatch(repository, std::chrono::steady_clock::now() + std::chrono::milliseconds(200)));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 2. After the deadline elapses, a single-node SCALE_IN batch is started.
TEST(TopologyControllerTest, ScaleInCollectWindowStartsSingleBatchAfterDeadline)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-finish-single", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.scaleInCollectWindow = std::chrono::milliseconds(50);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeTwoActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    EXPECT_TRUE(ActiveScaleInBatchHasLeavingCount(repository, deadline, 1));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 3. Two members exiting within the window land in the same SCALE_IN epoch.
TEST(TopologyControllerTest, ScaleInCollectWindowCoalescesTwoMembersInSameEpoch)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-coalesce-two", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.scaleInCollectWindow = std::chrono::milliseconds(200);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeThreeActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    PutMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    // While the first EXITING is being collected, the second member exits too.
    ASSERT_TRUE(WaitForNoActiveBatch(repository, std::chrono::steady_clock::now() + std::chrono::milliseconds(50)));
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::EXITING);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    EXPECT_TRUE(ActiveScaleInBatchHasLeavingCount(repository, deadline, 2));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 4. A member exiting only after the batch is committed does not join the running epoch.
TEST(TopologyControllerTest, ScaleInCollectWindowDoesNotAdmitLateMember)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-late-member", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.scaleInCollectWindow = std::chrono::milliseconds(50);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeThreeActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    PutMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    // Wait for the first member's batch to commit, then exit the second member.
    ASSERT_TRUE(ActiveScaleInBatchHasLeavingCount(repository, std::chrono::steady_clock::now() + std::chrono::seconds(2),
                                                 1));
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::EXITING);
    // The second member becomes PRE_LEAVING but stays out of the already-committed batch.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
    EXPECT_TRUE(WaitForTopology(repository, deadline, [](const auto &state) {
        return state.activeBatch.has_value() && state.activeBatch->type == TopologyChangeType::SCALE_IN
               && std::any_of(state.members.begin(), state.members.end(), [](const auto &member) {
                      return member.identity.address == "127.0.0.1:2" && member.state == MemberState::PRE_LEAVING;
                  });
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 5. 0ms disables coalescing (covered by ConvertsExitingMembershipToOneScaleInBatch above).
TEST(TopologyControllerTest, ScaleInCollectWindowZeroDisablesCoalescing)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-zero", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    options.scaleInCollectWindow = std::chrono::milliseconds(0);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeTwoActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    EXPECT_TRUE(ActiveScaleInBatchHasLeavingCount(repository, std::chrono::steady_clock::now() + std::chrono::seconds(1),
                                                  1));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 6. A scale-out candidate during the window clears the collect state and keeps scale-out priority.
TEST(TopologyControllerTest, ScaleInCollectWindowClearedByScaleOutCandidate)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-scaleout-interrupt", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.scaleInCollectWindow = std::chrono::milliseconds(500);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest = MakeTwoActiveMembersScaleInInitialState();
    // Second member is INITIAL (joining) instead of ACTIVE; its READY membership makes it a scale-out candidate.
    latest.members[1].state = MemberState::INITIAL;
    latest.members[1].tokens = {};
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    // Scale-out must start immediately despite the long collect window.
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + std::chrono::seconds(1),
                                [](const auto &state) {
                                    return state.activeBatch.has_value()
                                           && state.activeBatch->type == TopologyChangeType::SCALE_OUT;
                                }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 7. maxMembersPerBatch caps participants even within the collect window.
TEST(TopologyControllerTest, ScaleInCollectWindowRespectsMaxMembersPerBatch)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-max-batch", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.scaleInCollectWindow = std::chrono::milliseconds(50);
    options.maxMembersPerBatch = 1;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeThreeActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    // Both exit within the window, but only one is admitted to this batch.
    EXPECT_TRUE(ActiveScaleInBatchHasLeavingCount(repository, std::chrono::steady_clock::now() + std::chrono::seconds(2),
                                                  1));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 8. No PRE_LEAVING candidate means the collect window is never opened; no batch is started.
TEST(TopologyControllerTest, ScaleInCollectWindowNeverOpenedWithoutCandidate)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-no-candidate", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.scaleInCollectWindow = std::chrono::milliseconds(50);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    // No PRE_LEAVING member and no EXITING membership: collect is never opened.
    TopologyState latest = MakeTwoActiveMembersScaleInInitialState();
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    EXPECT_TRUE(WaitForNoActiveBatch(repository, std::chrono::steady_clock::now() + std::chrono::milliseconds(300)));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 9. IsValid rejects values above the hard upper bound and accepts 0 and the bound.
TEST(TopologyControllerTest, ScaleInCollectWindowIsValidBounds)
{
    TopologyControllerOptions options;
    EXPECT_TRUE(options.IsValid());
    options.scaleInCollectWindow = std::chrono::milliseconds(0);
    EXPECT_TRUE(options.IsValid());
    options.scaleInCollectWindow = std::chrono::milliseconds(5'000);
    EXPECT_TRUE(options.IsValid());
    options.scaleInCollectWindow = std::chrono::milliseconds(5'001);
    EXPECT_FALSE(options.IsValid());
}

TEST(TopologyControllerTest, ReachableMissingMemberIsNotCommittedAsFailure)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("reachable-missing", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    TopologyReader reader(repository);
    std::shared_ptr<const TopologySnapshot> expected;
    DS_ASSERT_OK(reader.Read(100, expected));
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        return start + (clockCalls.fetch_add(1) == 0 ? std::chrono::seconds(0) : std::chrono::seconds(2));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        EXPECT_EQ(targets.size(), 1);
        EXPECT_EQ(targets.front().address, "127.0.0.1:2");
        return std::vector<ControlBackendObservation>{
            { targets.front(), ControlBackendState::UNAVAILABLE, expected->Version(), expected->AuthorityRevision(),
              expected->CanonicalDigest(), std::chrono::steady_clock::now() }
        };
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline && probeCalls.load() == 0) {
        std::this_thread::yield();
    }
    EXPECT_EQ(probeCalls.load(), 1);
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(100, observed, revision));
    EXPECT_FALSE(observed.activeBatch.has_value());
    ASSERT_EQ(observed.members.size(), 2);
    EXPECT_EQ(observed.members.back().state, MemberState::ACTIVE);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, SingleMemberNotReadyResponseStartsNewAbsenceWindow)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("single-member-reachable", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(2);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    std::atomic<int64_t> clockSeconds{ 0 };
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        clockCalls.fetch_add(1);
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        EXPECT_EQ(targets.size(), 1);
        if (targets.empty()) {
            return std::vector<ControlBackendObservation>{};
        }
        EXPECT_EQ(targets.front(), latest.members.front().identity);
        return std::vector<ControlBackendObservation>{
            { targets.front(), ControlBackendState::UNKNOWN, 0, 0, "", std::chrono::steady_clock::now() }
        };
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto initialDeadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < initialDeadline && clockCalls.load() < 2) {
        std::this_thread::yield();
    }
    ASSERT_GE(clockCalls.load(), 2);
    clockSeconds.store(2);
    const auto firstProbeDeadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < firstProbeDeadline && probeCalls.load() < 1) {
        std::this_thread::yield();
    }
    ASSERT_EQ(probeCalls.load(), 1);

    const auto callsAfterProbe = clockCalls.load();
    const auto resetObservedDeadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < resetObservedDeadline
           && clockCalls.load() < callsAfterProbe + 2) {
        std::this_thread::yield();
    }
    EXPECT_GE(clockCalls.load(), callsAfterProbe + 2);
    EXPECT_EQ(probeCalls.load(), 1);

    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(100, observed, revision));
    EXPECT_FALSE(observed.activeBatch.has_value());
    ASSERT_EQ(observed.members.size(), 1);
    EXPECT_EQ(observed.members.front().state, MemberState::ACTIVE);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, ProbeCallbackExceptionRetriesWithoutTopologyMutation)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("probe-exception-retry", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, latest.members.front().identity.address, MemberLifecycleState::READY);
    TopologyReader reader(repository);
    std::shared_ptr<const TopologySnapshot> expected;
    DS_ASSERT_OK(reader.Read(100, expected));

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    std::atomic<int64_t> clockSeconds{ 0 };
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        clockCalls.fetch_add(1);
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        if (probeCalls.fetch_add(1) == 0) {
            throw std::runtime_error("injected member probe failure");
        }
        EXPECT_EQ(targets.size(), 1);
        if (targets.empty()) {
            return std::vector<ControlBackendObservation>{};
        }
        return std::vector<ControlBackendObservation>{
            { targets.front(), ControlBackendState::UNAVAILABLE, expected->Version(),
              expected->AuthorityRevision(), expected->CanonicalDigest(), std::chrono::steady_clock::now() }
        };
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto initialDeadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < initialDeadline && clockCalls.load() < 2) {
        std::this_thread::yield();
    }
    ASSERT_GE(clockCalls.load(), 2);
    clockSeconds.store(2);
    const auto retryDeadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < retryDeadline && probeCalls.load() < 2) {
        std::this_thread::yield();
    }
    ASSERT_EQ(probeCalls.load(), 2);

    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(100, observed, revision));
    EXPECT_FALSE(observed.activeBatch.has_value());
    ASSERT_EQ(observed.members.size(), 2);
    EXPECT_EQ(observed.members.back().state, MemberState::ACTIVE);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, UnreachableMissingMemberUsesExistingFailurePlan)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("unreachable-missing", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        return start + (clockCalls.fetch_add(1) == 0 ? std::chrono::seconds(0) : std::chrono::seconds(4));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        EXPECT_EQ(targets.size(), 1);
        return std::vector<ControlBackendObservation>{};
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    EXPECT_TRUE(WaitForTopology(repository, deadline, [](const auto &state) {
        return state.activeBatch.has_value() && state.activeBatch->type == TopologyChangeType::FAILURE;
    }));
    EXPECT_EQ(probeCalls.load(), 1);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, OneEarlyDirectProbeFailureDoesNotCommitFailure)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("early-unreachable-missing", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(2);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        return start + (clockCalls.fetch_add(1) == 0 ? std::chrono::seconds(0) : std::chrono::seconds(2));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &, auto) {
        ++probeCalls;
        return std::vector<ControlBackendObservation>{};
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline && probeCalls.load() == 0) {
        std::this_thread::yield();
    }
    EXPECT_GE(probeCalls.load(), 1);
    EXPECT_FALSE(WaitForTopology(repository, std::chrono::steady_clock::now() + std::chrono::milliseconds(100),
                                 [](const auto &state) {
                                     return state.activeBatch.has_value()
                                            && state.activeBatch->type == TopologyChangeType::FAILURE;
                                 }));
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(100, observed, revision));
    EXPECT_FALSE(observed.activeBatch.has_value());
    ASSERT_EQ(observed.members.size(), 2);
    EXPECT_EQ(observed.members.back().state, MemberState::ACTIVE);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, TopologyReadOutageDoesNotConsumeMissingMemberBudget)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("topology-read-pauses-missing", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, latest.members.front().identity.address, MemberLifecycleState::READY);
    TopologyReader reader(repository);
    std::shared_ptr<const TopologySnapshot> expected;
    DS_ASSERT_OK(reader.Read(100, expected));

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(5);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    std::atomic<int64_t> clockSeconds{ 0 };
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        clockCalls.fetch_add(1);
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        return std::vector<ControlBackendObservation>{
            { targets.front(), ControlBackendState::UNAVAILABLE, expected->Version(),
              expected->AuthorityRevision(), expected->CanonicalDigest(), std::chrono::steady_clock::now() }
        };
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(3, std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(2);
    backend.FailNextGet();
    backend.BlockNextGet();
    ASSERT_TRUE(backend.WaitUntilGetBlocked(std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    const auto callsAtBlock = clockCalls.load();
    clockSeconds.store(20);
    backend.ReleaseBlockedGet();
    const auto resumedDeadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < resumedDeadline && clockCalls.load() < callsAtBlock + 2) {
        std::this_thread::yield();
    }
    EXPECT_GE(clockCalls.load(), callsAtBlock + 2);
    EXPECT_EQ(probeCalls.load(), 0);

    clockSeconds.store(24);
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline && probeCalls.load() == 0) {
        std::this_thread::yield();
    }
    EXPECT_EQ(probeCalls.load(), 1);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, OneMissingMemberHasOneClusterWideProbeOwner)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("cluster-wide-probe-owner", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    constexpr size_t memberCount = 8;
    for (size_t index = 0; index < memberCount; ++index) {
        latest.members.push_back(
            Member{ { std::string(16, static_cast<char>('a' + index)), "127.0.0.1:" + std::to_string(index + 1) },
                    MemberState::ACTIVE,
                    { static_cast<uint32_t>(index + 1) } });
    }
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    TopologyReader reader(repository);
    std::shared_ptr<const TopologySnapshot> expected;
    DS_ASSERT_OK(reader.Read(100, expected));
    for (size_t index = 1; index < memberCount; ++index) {
        PutMembership(backend, *keys, latest.members[index].identity.address, MemberLifecycleState::READY);
    }

    std::atomic<size_t> probeCalls{ 0 };
    std::vector<std::shared_ptr<std::atomic<size_t>>> controllerClockCalls;
    std::vector<std::unique_ptr<CoordinationEventDispatcher>> dispatchers;
    std::vector<std::unique_ptr<TopologyController>> controllers;
    for (size_t index = 1; index < memberCount; ++index) {
        auto clockCalls = std::make_shared<std::atomic<size_t>>(0);
        controllerClockCalls.push_back(clockCalls);
        TopologyControllerOptions options;
        options.nodeDeadTimeout = std::chrono::seconds(1);
        options.reconcileTick = std::chrono::milliseconds(1);
        options.localAddress = latest.members[index].identity.address;
        const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));
        options.now = [clockCalls, start] {
            return start
                   + (clockCalls->fetch_add(1) == 0 ? std::chrono::seconds(0) : std::chrono::seconds(2));
        };
        options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
            ++probeCalls;
            EXPECT_EQ(targets.size(), 1);
            EXPECT_EQ(targets.front(), latest.members.front().identity);
            return std::vector<ControlBackendObservation>{
                { targets.front(), ControlBackendState::UNAVAILABLE, expected->Version(),
                  expected->AuthorityRevision(), expected->CanonicalDigest(), std::chrono::steady_clock::now() }
            };
        };
        dispatchers.push_back(std::make_unique<CoordinationEventDispatcher>(32));
        controllers.push_back(std::make_unique<TopologyController>(
            backend, repository, *keys, algorithm, *dispatchers.back(), std::move(options)));
    }
    for (auto &controller : controllers) {
        DS_ASSERT_OK(controller->Start());
    }
    const auto allControllersReconciled = [&] {
        return std::all_of(controllerClockCalls.begin(), controllerClockCalls.end(),
                           [](const auto &calls) { return calls->load() >= 3; });
    };
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline && !allControllersReconciled()) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(allControllersReconciled());
    for (auto &controller : controllers) {
        DS_ASSERT_OK(controller->Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
    }
    EXPECT_EQ(probeCalls.load(), 1);
}

TEST(TopologyControllerTest, LargeTopologyControllerProbesOnlyItsOwnedMissingMember)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("bounded-missing-probe", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    constexpr size_t memberCount = 8;
    for (size_t index = 0; index < memberCount; ++index) {
        latest.members.push_back(
            Member{ { std::string(16, static_cast<char>('a' + index)), "127.0.0.1:" + std::to_string(index + 1) },
                    MemberState::ACTIVE,
                    { static_cast<uint32_t>(index + 1) } });
    }
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    TopologyReader reader(repository);
    std::shared_ptr<const TopologySnapshot> expected;
    DS_ASSERT_OK(reader.Read(100, expected));
    PutMembership(backend, *keys, latest.members.front().identity.address, MemberLifecycleState::READY);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        return start + (clockCalls.fetch_add(1) == 0 ? std::chrono::seconds(0) : std::chrono::seconds(2));
    };
    std::atomic<size_t> probeCalls{ 0 };
    std::atomic<size_t> probedMembers{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        probedMembers.fetch_add(targets.size());
        EXPECT_EQ(targets.size(), 1);
        EXPECT_EQ(targets.front(), latest.members.back().identity);
        std::vector<ControlBackendObservation> observations;
        observations.reserve(targets.size());
        for (const auto &target : targets) {
            observations.push_back({ target, ControlBackendState::UNAVAILABLE, expected->Version(),
                                     expected->AuthorityRevision(), expected->CanonicalDigest(),
                                     std::chrono::steady_clock::now() });
        }
        return observations;
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    while (std::chrono::steady_clock::now() < deadline && probedMembers.load() == 0) {
        std::this_thread::yield();
    }
    EXPECT_EQ(probedMembers.load(), 1);
    EXPECT_EQ(probeCalls.load(), 1);
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(100, observed, revision));
    EXPECT_FALSE(observed.activeBatch.has_value());
    EXPECT_EQ(observed.members.size(), memberCount);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

}  // namespace
}  // namespace datasystem::cluster
