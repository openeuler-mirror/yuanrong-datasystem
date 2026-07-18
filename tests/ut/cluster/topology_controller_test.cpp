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
#include <thread>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
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

}  // namespace
}  // namespace datasystem::cluster
