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
#include <iterator>
#include <set>
#include <stdexcept>
#include <thread>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/coordinator.pb.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {
constexpr size_t RECOVERY_DISPATCHER_CAPACITY = 32;
constexpr std::chrono::milliseconds RECOVERY_RECONCILE_TICK{ 10 };
constexpr size_t INJECTED_NOT_READY_GET_ATTEMPTS = 5;
constexpr size_t BOOTSTRAP_SUCCESS_GET_ATTEMPTS = 1;
constexpr size_t EXTERNAL_REBUILD_ATTEMPTS = 8;
constexpr size_t EXPECTED_RECOVERY_GET_ATTEMPTS =
    INJECTED_NOT_READY_GET_ATTEMPTS + BOOTSTRAP_SUCCESS_GET_ATTEMPTS;
constexpr std::chrono::milliseconds RECOVERY_WAIT_TIMEOUT{ 150 };
constexpr std::chrono::seconds RECOVERY_STOP_TIMEOUT{ 1 };
constexpr std::chrono::seconds LARGE_BATCH_WAIT_TIMEOUT{ 5 };
constexpr auto FAILURE_PROBE_WAIT_TIMEOUT = std::chrono::seconds(1);
constexpr size_t COLLECTIVE_ABSENCE_MEMBER_COUNT = 2;
constexpr size_t COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW = 3;
constexpr int64_t COLLECTIVE_ABSENCE_FIRST_WINDOW_SECONDS = 4;
constexpr int64_t COLLECTIVE_ABSENCE_SECOND_WINDOW_SECONDS = 8;
constexpr int CONTROLLER_TEST_READ_TIMEOUT_MS = 100;
constexpr size_t MEMBER_ID_SIZE = 16;
constexpr size_t LARGE_BATCH_MEMBER_COUNT = 500;
constexpr size_t LARGE_STEADY_MEMBER_COUNT = 2'000;
constexpr size_t LARGE_EXPANDED_MEMBER_COUNT = LARGE_STEADY_MEMBER_COUNT + LARGE_BATCH_MEMBER_COUNT;
constexpr uint32_t LARGE_TOKENS_PER_MEMBER = 4;
constexpr uint32_t LARGE_PORT_BASE = 20'000;
constexpr auto LARGE_COLLECT_WINDOW = std::chrono::seconds(1);
constexpr auto LARGE_CONTROL_BUDGET = std::chrono::seconds(3);
constexpr auto DEFAULT_COLLECT_WINDOW = std::chrono::seconds(3);
constexpr size_t STALE_TOPOLOGY_PROBE_SAMPLE_COUNT = 5;
constexpr uint32_t STALE_TOPOLOGY_OLD_PORT_BASE = 10'000;
constexpr uint32_t STALE_TOPOLOGY_NEW_PORT_BASE = 30'000;

class CountingPlanningAlgorithm final : public IPlanningAlgorithm {
public:
    CountingPlanningAlgorithm() = default;
    ~CountingPlanningAlgorithm() override = default;

    TopologyAlgorithmId GetId() const override
    {
        return delegate_.GetId();
    }

    Status BuildInitialPlacement(const ScaleOutPlanInput &input, TopologyPlan &plan) const override
    {
        return delegate_.BuildInitialPlacement(input, plan);
    }

    Status PlanScaleOut(const ScaleOutPlanInput &input, TopologyPlan &plan) const override
    {
        ++scaleOutCalls_;
        return delegate_.PlanScaleOut(input, plan);
    }

    Status PlanScaleIn(const ScaleInPlanInput &input, TopologyPlan &plan) const override
    {
        return delegate_.PlanScaleIn(input, plan);
    }

    Status PlanFailure(const FailurePlanInput &input, TopologyPlan &plan) const override
    {
        return delegate_.PlanFailure(input, plan);
    }

    Status Validate(const TopologyState &state) const override
    {
        return delegate_.Validate(state);
    }

    size_t ScaleOutCalls() const noexcept
    {
        return scaleOutCalls_.load();
    }

private:
    HashAlgorithm delegate_;
    mutable std::atomic<size_t> scaleOutCalls_{ 0 };
};

TEST(TopologyControllerTest, DefaultOrdinaryCollectionWindowsAreThreeSeconds)
{
    const TopologyControllerOptions options;

    EXPECT_EQ(options.scaleOutCollectWindow, DEFAULT_COLLECT_WINDOW);
    EXPECT_EQ(options.scaleInCollectWindow, DEFAULT_COLLECT_WINDOW);
}

template <typename Predicate>
bool WaitForCondition(Predicate predicate,
                      std::chrono::steady_clock::duration timeout = FAILURE_PROBE_WAIT_TIMEOUT)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

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

TopologyState MakeActiveTopology(size_t memberCount, uint32_t portBase)
{
    TopologyState topology;
    topology.version = 1;
    topology.clusterHasInit = true;
    topology.members.reserve(memberCount);
    for (size_t index = 0; index < memberCount; ++index) {
        topology.members.push_back(
            { { std::string(MEMBER_ID_SIZE, static_cast<char>('a' + index)),
                "127.0.0.1:" + std::to_string(portBase + index) },
              MemberState::ACTIVE,
              { static_cast<uint32_t>(index + 1) } });
    }
    return topology;
}

std::set<std::string> MemberAddresses(const TopologyState &topology)
{
    std::set<std::string> addresses;
    for (const auto &member : topology.members) {
        addresses.insert(member.identity.address);
    }
    return addresses;
}

std::set<std::string> PutReadyReplacementMemberships(FakeCoordinationBackend &backend,
                                                      const TopologyKeyHelper &keys, size_t memberCount)
{
    std::set<std::string> addresses;
    for (size_t index = 0; index < memberCount; ++index) {
        const auto address = "127.0.0.1:" + std::to_string(STALE_TOPOLOGY_NEW_PORT_BASE + index);
        PutMembership(backend, keys, address, MemberLifecycleState::READY);
        addresses.insert(address);
    }
    return addresses;
}

bool IsExactActiveTopology(const TopologyState &topology, const std::set<std::string> &expectedAddresses)
{
    return !topology.activeBatch.has_value() && topology.members.size() == expectedAddresses.size()
           && MemberAddresses(topology) == expectedAddresses
           && std::all_of(topology.members.begin(), topology.members.end(),
                          [](const Member &member) { return member.state == MemberState::ACTIVE; });
}

std::vector<ControlBackendProbeResult> RespondedProbe(ControlBackendObservation observation,
                                                      std::chrono::milliseconds elapsed = std::chrono::milliseconds(0))
{
    auto target = observation.reporter;
    return { { std::move(target), std::move(observation), ControlBackendProbeOutcome::RESPONSE, elapsed } };
}

std::vector<ControlBackendProbeResult> NoResponseProbe(
    const std::vector<MemberIdentity> &targets,
    ControlBackendProbeOutcome outcome = ControlBackendProbeOutcome::DEADLINE_EXCEEDED,
    std::chrono::milliseconds elapsed = std::chrono::milliseconds(0))
{
    std::vector<ControlBackendProbeResult> results;
    results.reserve(targets.size());
    for (const auto &target : targets) {
        results.push_back({ target, std::nullopt, outcome, elapsed });
    }
    return results;
}

uint64_t ProbeRoundFor(const coordinator::WorkerProbeEventValuePb &value, const std::string &targetAddress)
{
    return value.target_address() == targetAddress ? value.probe_round() : 0;
}

void ExpectActiveTopologyUnchanged(const TopologyState &observed, uint64_t expectedVersion,
                                   size_t expectedMemberCount)
{
    EXPECT_EQ(observed.version, expectedVersion);
    EXPECT_FALSE(observed.activeBatch.has_value());
    EXPECT_EQ(observed.members.size(), expectedMemberCount);
    EXPECT_TRUE(std::all_of(observed.members.begin(), observed.members.end(), [](const Member &member) {
        return member.state == MemberState::ACTIVE;
    }));
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
            if (expected.notifiesByAddress.empty()
                || !expected.notifiesByAddress.begin()->second.activeBatch.has_value()) {
                return false;
            }
            complete = complete
                       && repository
                              .ReadTask(kind, id, expected.notifiesByAddress.begin()->second.activeBatch->type,
                                        std::visit([](const auto &value) { return value.epoch; }, task), observed)
                              .IsOk();
        }
        for (const auto &[address, notify] : expected.notifiesByAddress) {
            TopologyTaskNotify observed;
            complete = complete && repository.ReadNotify(address, observed).IsOk()
                       && observed.activeBatch.has_value() == notify.activeBatch.has_value()
                       && observed.taskIds == notify.taskIds;
            if (complete && notify.activeBatch.has_value()) {
                complete = observed.activeBatch->type == notify.activeBatch->type
                           && observed.activeBatch->epoch == notify.activeBatch->epoch;
            }
        }
        if (complete) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

Status RebuildExpectedFromRepository(TopologyRepository &repository, const IPlanningAlgorithm &algorithm,
                                     ExpectedDerivedState &expected)
{
    TopologyState active;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(repository.ReadTopology(100, active, revision));
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(TopologySnapshot::Create(active, revision, std::string(64, 'a'), snapshot));
    std::vector<MembershipRecord> memberships;
    RETURN_IF_NOT_OK(repository.ReadMemberships(memberships));
    TopologyTaskMaterializer materializer;
    return materializer.RebuildExpected(*snapshot, algorithm, memberships, false, expected);
}

Status FinishExpectedMigrateTasks(TopologyRepository &repository, const ExpectedDerivedState &expected)
{
    for (const auto &task : expected.tasks) {
        CHECK_FAIL_RETURN_STATUS(std::holds_alternative<TopologyMigrateTask>(task), K_INVALID,
                                 "expected migrate task");
        const auto &migrate = std::get<TopologyMigrateTask>(task);
        TopologyExecutionFence fence;
        fence.taskId = migrate.taskId;
        fence.batchType = migrate.type;
        fence.batchEpoch = migrate.epoch;
        fence.executor.address = migrate.executorAddress;
        for (const auto &range : migrate.sourceRanges) {
            fence.ranges.push_back(range.range);
        }
        TaskProgressOutcome outcome;
        RETURN_IF_NOT_OK(repository.MarkTaskScopeFinished(fence, outcome));
        CHECK_FAIL_RETURN_STATUS(outcome == TaskProgressOutcome::UPDATED, K_RUNTIME_ERROR,
                                 "failed to finish expected migrate task");
    }
    return Status::OK();
}

bool ActiveBatchHasMemberStateCount(TopologyRepository &repository,
                                    std::chrono::steady_clock::time_point deadline,
                                    TopologyChangeType batchType, MemberState memberState, size_t expectedCount)
{
    return WaitForTopology(repository, deadline, [batchType, memberState, expectedCount](const auto &state) {
        if (!state.activeBatch.has_value() || state.activeBatch->type != batchType) {
            return false;
        }
        const auto memberCount = std::count_if(state.members.begin(), state.members.end(),
                                               [memberState](const auto &member) {
                                                   return member.state == memberState;
                                               });
        return static_cast<size_t>(memberCount) == expectedCount;
    });
}

Status FinishActiveMigrateBatch(FakeCoordinationBackend &backend, TopologyRepository &repository,
                                const IPlanningAlgorithm &algorithm, TopologyChangeType expectedBatchType,
                                std::chrono::steady_clock::time_point deadline)
{
    TopologyState active;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, active, revision));
    CHECK_FAIL_RETURN_STATUS(active.activeBatch.has_value(), K_RUNTIME_ERROR,
                             "expected active migrate batch");
    CHECK_FAIL_RETURN_STATUS(active.activeBatch->type == expectedBatchType, K_RUNTIME_ERROR,
                             "active migrate batch type does not match expected type");
    const auto expectedBatchEpoch = active.activeBatch->epoch;

    ExpectedDerivedState expected;
    RETURN_IF_NOT_OK(RebuildExpectedFromRepository(repository, algorithm, expected));
    CHECK_FAIL_RETURN_STATUS(!expected.tasks.empty(), K_RUNTIME_ERROR,
                             "active migrate batch yielded no migrate tasks");
    CHECK_FAIL_RETURN_STATUS(WaitForDerivedState(repository, expected, deadline), K_RUNTIME_ERROR,
                             "timed out waiting for active migrate batch derived state");

    RETURN_IF_NOT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, active, revision));
    CHECK_FAIL_RETURN_STATUS(active.activeBatch.has_value(), K_RUNTIME_ERROR,
                             "active migrate batch disappeared before finishing tasks");
    CHECK_FAIL_RETURN_STATUS(active.activeBatch->type == expectedBatchType, K_RUNTIME_ERROR,
                             "active migrate batch type changed before finishing tasks");
    CHECK_FAIL_RETURN_STATUS(active.activeBatch->epoch == expectedBatchEpoch, K_RUNTIME_ERROR,
                             "active migrate batch epoch changed before finishing tasks");
    RETURN_IF_NOT_OK(FinishExpectedMigrateTasks(repository, expected));
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    return Status::OK();
}

void BlockCasAfter(FakeCoordinationBackend &backend, size_t precedingCasCount, WaitPost &entered, WaitPost &release)
{
    backend.SetBeforeCasHandler([&backend, precedingCasCount, &entered, &release] {
        if (precedingCasCount > 0) {
            BlockCasAfter(backend, precedingCasCount - 1, entered, release);
            return;
        }
        entered.Set();
        release.Wait();
    });
}

Status PrepareCompletedScaleOut(FakeCoordinationBackend &backend, const TopologyKeyHelper &keys,
                                TopologyRepository &repository, const HashAlgorithm &algorithm,
                                TopologyState &next)
{
    TopologyState current{ true, 1, {}, std::nullopt };
    current.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    TopologyPlan plan;
    RETURN_IF_NOT_OK(
        algorithm.PlanScaleOut({ current, { MemberIdentity{ std::string(16, 'b'), "127.0.0.1:2" } }, 4 }, plan));
    plan.next.version = 2;
    plan.next.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    std::shared_ptr<const TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(TopologySnapshot::Create(plan.next, 1, std::string(64, 'a'), snapshot));
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    RETURN_IF_NOT_OK(materializer.BuildExpected(*snapshot, plan, expected));
    CHECK_FAIL_RETURN_STATUS(expected.tasks.size() == 1, K_RUNTIME_ERROR,
                             "single joining member must yield one test task");
    auto completedTask = expected.tasks.front();
    for (auto &range : std::get<TopologyMigrateTask>(completedTask).sourceRanges) {
        range.finished = true;
    }
    RETURN_IF_NOT_OK(repository.CreateTaskIfAbsent(completedTask));
    backend.PutRaw(keys.TopologyTable(), TopologyKeyHelper::TopologyKey(), plan.next);
    for (const auto &member : plan.next.members) {
        PutMembership(backend, keys, member.identity.address, MemberLifecycleState::READY);
    }
    next = std::move(plan.next);
    return Status::OK();
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

TEST(TopologyControllerTest, ProgressSlicesContinueWithoutWaitingForThePeriodicTick)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("progress-slices", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    TopologyState current;
    current.version = 1;
    current.clusterHasInit = true;
    current.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    TopologyPlan plan;
    std::vector<MemberIdentity> joining = {
        { std::string(16, 'b'), "127.0.0.1:2" },
        { std::string(16, 'c'), "127.0.0.1:3" },
    };
    DS_ASSERT_OK(algorithm.PlanScaleOut({ current, joining, 4 }, plan));
    plan.next.version = 2;
    plan.next.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(plan.next, 1, std::string(64, 'a'), snapshot));
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    DS_ASSERT_OK(materializer.BuildExpected(*snapshot, plan, expected));
    ASSERT_GT(expected.tasks.size(), 1);
    for (auto task : expected.tasks) {
        auto &ranges = std::get<TopologyMigrateTask>(task).sourceRanges;
        for (auto &range : ranges) {
            range.finished = true;
        }
        DS_ASSERT_OK(repository.CreateTaskIfAbsent(task));
    }
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), plan.next);
    for (const auto &member : plan.next.members) {
        PutMembership(backend, *keys, member.identity.address, MemberLifecycleState::READY);
    }
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::seconds(5);
    options.ordinaryBatchWindow = std::chrono::minutes(1);
    options.maxProgressReadsPerTick = 1;
    std::atomic<int64_t> nowCalls{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::minutes(nowCalls.fetch_add(1) * 2));
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT;
    EXPECT_TRUE(WaitForTopology(repository, deadline, [](const auto &state) {
        return !state.activeBatch.has_value()
               && std::all_of(state.members.begin(), state.members.end(),
                              [](const auto &member) { return member.state == MemberState::ACTIVE; });
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, DoesNotFinalizeBatchBeforeAllDerivedNotifiesAreMaterialized)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("derived-finalize-fence", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    TopologyState activeBatch;
    DS_ASSERT_OK(PrepareCompletedScaleOut(backend, *keys, repository, algorithm, activeBatch));
    WaitPost casEntered;
    WaitPost casRelease;
    BlockCasAfter(backend, 2, casEntered, casRelease);
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    options.maxDerivedOperationsPerTick = 1;
    options.materializeRestartFacts = true;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const bool blocked = casEntered.WaitFor(RECOVERY_STOP_TIMEOUT.count() * 1'000);
    EXPECT_TRUE(blocked);
    if (!blocked) {
        casRelease.Set();
        DS_EXPECT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
        return;
    }
    TopologyState observed;
    int64_t revision = 0;
    DS_EXPECT_OK(repository.ReadTopology(100, observed, revision));
    EXPECT_EQ(observed.version, activeBatch.version);
    EXPECT_TRUE(observed.activeBatch.has_value());
    casRelease.Set();
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, DerivedSlicesReuseOnePlanningGeneration)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("derived-generation", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm setupAlgorithm;
    TopologyState current;
    current.version = 1;
    current.clusterHasInit = true;
    current.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    std::vector<MemberIdentity> joining = {
        { std::string(16, 'b'), "127.0.0.1:2" },
        { std::string(16, 'c'), "127.0.0.1:3" },
        { std::string(16, 'd'), "127.0.0.1:4" },
    };
    TopologyPlan plan;
    DS_ASSERT_OK(setupAlgorithm.PlanScaleOut({ current, joining, 4 }, plan));
    plan.next.version = 2;
    plan.next.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, 2 };
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(plan.next, 1, std::string(64, 'a'), snapshot));
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    DS_ASSERT_OK(materializer.BuildExpected(*snapshot, plan, expected));
    ASSERT_GT(expected.tasks.size() + expected.notifyRecipients.size(), 2);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), plan.next);
    for (const auto &member : plan.next.members) {
        PutMembership(backend, *keys, member.identity.address, MemberLifecycleState::READY);
    }

    CountingPlanningAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::seconds(5);
    options.maxDerivedOperationsPerTick = 1;
    options.maxProgressReadsPerTick = 1;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    EXPECT_TRUE(WaitForDerivedState(
        repository, expected, std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
    EXPECT_EQ(algorithm.ScaleOutCalls(), 1);
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
    options.scaleOutCollectWindow = std::chrono::milliseconds(0);
    options.scaleInCollectWindow = std::chrono::milliseconds(0);
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

TEST(TopologyControllerTest, BootstrapCollectWindowCoalescesStaggeredReadyMembers)
{
    constexpr size_t bootstrapMemberCount = 500;
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("bootstrap-collect", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    const auto baseTime = std::chrono::steady_clock::now();
    std::atomic<int64_t> elapsedMs{ 0 };
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    options.scaleOutCollectWindow = LARGE_COLLECT_WINDOW;
    options.now = [&] { return baseTime + std::chrono::milliseconds(elapsedMs.load()); };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest;
    latest.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    DS_ASSERT_OK(controller.Start());

    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    ASSERT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT,
                                [](const auto &state) {
                                    return !state.clusterHasInit && !state.activeBatch.has_value()
                                           && state.members.size() == 1
                                           && state.members.front().state == MemberState::INITIAL;
                                }));
    for (size_t index = 1; index < bootstrapMemberCount; ++index) {
        PutMembership(backend, *keys, "127.0.0.1:" + std::to_string(index + 1),
                      MemberLifecycleState::READY);
    }
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    ASSERT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT,
                                [bootstrapMemberCount](const auto &state) {
                                    return !state.clusterHasInit && state.members.size() == bootstrapMemberCount
                                           && std::all_of(
                                               state.members.begin(), state.members.end(),
                                               [](const auto &member) { return member.state == MemberState::INITIAL; });
                                }));

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    elapsedMs.store(std::chrono::duration_cast<std::chrono::milliseconds>(LARGE_COLLECT_WINDOW).count());
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT,
                                [bootstrapMemberCount](const auto &state) {
                                    return state.clusterHasInit && !state.activeBatch.has_value()
                                           && state.members.size() == bootstrapMemberCount
                                           && std::all_of(
                                               state.members.begin(), state.members.end(),
                                               [](const auto &member) { return member.state == MemberState::ACTIVE; });
                                }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT));
}

TEST(TopologyControllerTest, ScaleOutCollectWindowCoalescesStaggeredReadyMembers)
{
    constexpr size_t joiningCount = 500;
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("scale-out-collect", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    const auto baseTime = std::chrono::steady_clock::now();
    std::atomic<int64_t> elapsedMs{ 0 };
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::seconds(5);
    options.scaleOutCollectWindow = std::chrono::milliseconds(MAX_SCALE_OUT_COLLECT_WINDOW_MS);
    options.now = [&] { return baseTime + std::chrono::milliseconds(elapsedMs.load()); };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());

    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    ASSERT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT,
                                [](const auto &state) {
                                    return !state.activeBatch.has_value() && state.members.size() == 2;
                                }));
    for (size_t index = 2; index <= joiningCount; ++index) {
        PutMembership(backend, *keys, "127.0.0.1:" + std::to_string(index + 1),
                      MemberLifecycleState::READY);
    }
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    ASSERT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT,
                                [joiningCount](const auto &state) {
                                    return !state.activeBatch.has_value()
                                           && state.members.size() == joiningCount + 1;
                                }));

    elapsedMs.store(MAX_SCALE_OUT_COLLECT_WINDOW_MS);
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT,
                                [joiningCount](const auto &state) {
                                    return state.activeBatch.has_value()
                                           && state.activeBatch->type == TopologyChangeType::SCALE_OUT
                                           && std::count_if(state.members.begin(), state.members.end(),
                                                            [](const auto &member) {
                                                                return member.state == MemberState::JOINING;
                                                            })
                                                  == static_cast<std::ptrdiff_t>(joiningCount);
                                }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT));
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
    DS_ASSERT_OK(TopologyKeyHelper::Create("scale-out-before-pending-scale-in", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    options.scaleOutCollectWindow = std::chrono::milliseconds(0);
    options.scaleInCollectWindow = std::chrono::milliseconds(0);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::PRE_LEAVING, { 1 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } },
        Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::INITIAL, {} },
    };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    PutMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    EXPECT_TRUE(ActiveBatchHasMemberStateCount(repository, deadline, TopologyChangeType::SCALE_OUT,
                                               MemberState::JOINING, 1));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, ReadyReplacementScalesOutBeforeAllCommittedOwnersScaleIn)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("scale-out-before-all-owner-scale-in", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    options.scaleOutCollectWindow = std::chrono::milliseconds(0);
    options.scaleInCollectWindow = std::chrono::milliseconds(0);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = {
        Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::PRE_LEAVING, { 1 } },
        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::PRE_LEAVING, { 2 } },
        Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::INITIAL, {} },
    };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);

    DS_ASSERT_OK(controller.Start());
    const auto scaleOutActiveDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    ASSERT_TRUE(ActiveBatchHasMemberStateCount(repository, scaleOutActiveDeadline,
                                               TopologyChangeType::SCALE_OUT, MemberState::JOINING, 1));
    const auto scaleOutDerivedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    DS_ASSERT_OK(FinishActiveMigrateBatch(backend, repository, algorithm, TopologyChangeType::SCALE_OUT,
                                          scaleOutDerivedDeadline));

    const auto scaleInActiveDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    ASSERT_TRUE(ActiveBatchHasMemberStateCount(repository, scaleInActiveDeadline,
                                               TopologyChangeType::SCALE_IN, MemberState::LEAVING, 2));
    const auto scaleInDerivedDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    DS_ASSERT_OK(FinishActiveMigrateBatch(backend, repository, algorithm, TopologyChangeType::SCALE_IN,
                                          scaleInDerivedDeadline));

    const auto finalDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    EXPECT_TRUE(WaitForTopology(repository, finalDeadline, [](const auto &state) {
        return !state.activeBatch.has_value() && state.members.size() == 1
               && state.members.front().identity.address == "127.0.0.1:3"
               && state.members.front().state == MemberState::ACTIVE;
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

TEST(TopologyControllerTest, ExternalBootstrapRetriesAcrossMembershipRevisionBoundary)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("bootstrap-watermark", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members.front().identity.address, MemberLifecycleState::READY);
    auto advanced = initial;
    advanced.version = 2;
    backend.SetAfterRevisionGetAllHandler(
        [&] { backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), advanced); });
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(5);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    EXPECT_EQ(backend.RevisionGetAllCount(keys->MembershipTable()), 2U);
    EXPECT_EQ(controller.GetBootstrapRevision(), backend.CurrentRevision());
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, ExternalControllerDoesNotReconcileBeforeWatchActivation)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("bootstrap-activation-gate", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members.front().identity.address, MemberLifecycleState::EXITING);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, initial.version, initial.members.size());

    PutMembership(backend, *keys, initial.members.front().identity.address, MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.SubmitCoordinationEvent({ CoordinationEventType::RESET, "", "", 0, 0 }));
    ASSERT_TRUE(WaitForCondition([&] { return backend.RevisionGetAllCount(keys->MembershipTable()) >= 2U; }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, initial.version, initial.members.size());
}

TEST(TopologyControllerTest, ExternalResyncChurnUsesReconcileBackoff)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("resync-backoff", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members.front().identity.address, MemberLifecycleState::READY);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(100);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    auto churn = std::make_shared<std::function<void()>>();
    auto next = std::make_shared<TopologyState>(initial);
    auto stopped = std::make_shared<std::atomic<bool>>(false);
    auto churnCalls = std::make_shared<std::atomic<size_t>>(0);
    auto firstCycleStartNs = std::make_shared<std::atomic<int64_t>>(0);
    auto secondCycleDelayMs = std::make_shared<std::atomic<int64_t>>(-1);
    *churn = [&, churn, next, stopped] {
        if (stopped->load()) {
            return;
        }
        const auto call = churnCalls->fetch_add(1);
        const auto nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now().time_since_epoch())
                               .count();
        if (call == 0) {
            firstCycleStartNs->store(nowNs);
        } else if (call == EXTERNAL_REBUILD_ATTEMPTS) {
            secondCycleDelayMs->store((nowNs - firstCycleStartNs->load()) / 1'000'000);
        }
        ++next->version;
        backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), *next);
        backend.SetAfterRevisionGetAllHandler(*churn);
    };
    backend.SetAfterRevisionGetAllHandler(*churn);
    backend.ResetReadCounts();
    DS_ASSERT_OK(controller.SubmitCoordinationEvent({ CoordinationEventType::RESET, "", "", 0, 0 }));
    ASSERT_TRUE(WaitForCondition([&] { return secondCycleDelayMs->load() >= 0; }, std::chrono::seconds(1)));
    EXPECT_GE(secondCycleDelayMs->load(), 90);

    stopped->store(true);
    backend.SetAfterRevisionGetAllHandler({});
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
    *churn = {};
}

TEST(TopologyControllerTest, ExternalTopologyWatchValueAdvancesWithoutPeriodicReads)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("watch-value", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members.front().identity.address, MemberLifecycleState::READY);
    std::atomic<size_t> clockCalls{ 0 };
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(5);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    options.now = [&] {
        ++clockCalls;
        return std::chrono::steady_clock::now();
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    backend.ResetReadCounts();
    const auto before = clockCalls.load();
    auto next = initial;
    next.version = 2;
    std::string payload;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(next, payload));
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::PUT, keys->TopologyTable() + "/", std::move(payload), 2,
          backend.CurrentRevision() + 1 }));
    EXPECT_TRUE(WaitForCondition([&] { return controller.GetDiagnostics().topologyVersion == 2; }));
    EXPECT_TRUE(WaitForCondition([&] { return clockCalls.load() >= before + 5; }));
    EXPECT_EQ(backend.ExactGetCount(keys->TopologyTable(), TopologyKeyHelper::TopologyKey()), 0U);
    EXPECT_EQ(backend.GetAllCount(keys->MembershipTable()), 0U);
    EXPECT_EQ(backend.RevisionGetAllCount(keys->MembershipTable()), 0U);
    const auto beforeTaskDoorbell = clockCalls.load();
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::PUT, keys->MigrateTaskTable() + "/task", "", 1,
          backend.CurrentRevision() + 2 }));
    EXPECT_TRUE(WaitForCondition([&] { return clockCalls.load() >= beforeTaskDoorbell + 2; }));
    EXPECT_EQ(backend.ExactGetCount(keys->TopologyTable(), TopologyKeyHelper::TopologyKey()), 0U);
    EXPECT_EQ(backend.RevisionGetAllCount(keys->MembershipTable()), 0U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, ExternalMembershipPutValueAdvancesWithoutGetAll)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("watch-membership-put", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members.front().identity.address, MemberLifecycleState::READY);
    backend.SetStorePrefix(keys->MembershipTable(), keys->EtcdMembershipTablePrefix());
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(5);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    backend.ResetReadCounts();
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::PUT,
          keys->EtcdMembershipTablePrefix() + "/" + initial.members.front().identity.address,
          EncodeMembership(MemberLifecycleState::EXITING),
          2,
          backend.CurrentRevision() + 1 }));
    ASSERT_TRUE(WaitForCondition([&] { return controller.GetDiagnostics().topologyVersion >= 2; }));
    EXPECT_EQ(backend.GetAllCount(keys->MembershipTable()), 0U);
    EXPECT_EQ(backend.RevisionGetAllCount(keys->MembershipTable()), 0U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, ExternalMembershipDeleteUsesExactPrefixCompensation)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("watch-membership", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members[0].identity.address, MemberLifecycleState::READY);
    PutMembership(backend, *keys, initial.members[1].identity.address, MemberLifecycleState::READY);
    backend.SetStorePrefix(keys->MembershipTable(), keys->EtcdMembershipTablePrefix());
    std::atomic<int64_t> nowSeconds{ 0 };
    std::atomic<size_t> clockCalls{ 0 };
    std::atomic<size_t> probeCalls{ 0 };
    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(5);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    options.localAddress = initial.members[0].identity.address;
    options.now = [&] {
        ++clockCalls;
        return std::chrono::steady_clock::time_point(std::chrono::seconds(nowSeconds.load()));
    };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        EXPECT_EQ(targets.size(), 1U);
        return RespondedProbe(
            { targets.front(), ControlBackendState::UNKNOWN, 0, 0, "", std::chrono::steady_clock::now() }
        );
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    backend.ResetReadCounts();
    clockCalls = 0;
    DS_ASSERT_OK(backend.Delete(keys->MembershipTable(), initial.members[1].identity.address));
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::DELETE,
          keys->EtcdMembershipTablePrefix() + "/" + initial.members[1].identity.address, "", 0,
          backend.CurrentRevision() }));
    ASSERT_TRUE(WaitForCondition([&] { return clockCalls.load() >= 3; }, std::chrono::milliseconds(200)));
    nowSeconds = 2;
    EXPECT_TRUE(WaitForCondition([&] { return probeCalls.load() > 0; }));
    EXPECT_EQ(backend.GetAllCount(keys->MembershipTable()), 0U);
    EXPECT_EQ(backend.RevisionGetAllCount(keys->MembershipTable()), 1U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, ExternalCollectiveMembershipDeleteUsesCompleteCompensatedSnapshot)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("watch-collective-membership-delete", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                        Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members[0].identity.address, MemberLifecycleState::READY);
    PutMembership(backend, *keys, initial.members[1].identity.address, MemberLifecycleState::READY);
    backend.SetStorePrefix(keys->MembershipTable(), keys->EtcdMembershipTablePrefix());
    std::atomic<size_t> probeCalls{ 0 };
    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(0);
    options.failureProbeTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(5);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    options.localAddress = initial.members[0].identity.address;
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        return NoResponseProbe(targets);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    backend.ResetReadCounts();
    DS_ASSERT_OK(backend.Delete(keys->MembershipTable(), initial.members[0].identity.address));
    DS_ASSERT_OK(backend.Delete(keys->MembershipTable(), initial.members[1].identity.address));
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::DELETE,
          keys->EtcdMembershipTablePrefix() + "/" + initial.members[0].identity.address, "", 0,
          backend.CurrentRevision() }));
    ASSERT_TRUE(WaitForCondition(
        [&] { return backend.RevisionGetAllCount(keys->MembershipTable()) == 1U; }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
    EXPECT_EQ(probeCalls.load(), 0U);
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, initial.version, initial.members.size());
}

TEST(TopologyControllerTest, ExternalResetRevisionFloorRejectsQueuedStaleValue)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("watch-resync-floor", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members.front().identity.address, MemberLifecycleState::READY);
    std::atomic<size_t> clockCalls{ 0 };
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(5);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    options.now = [&] {
        ++clockCalls;
        return std::chrono::steady_clock::now();
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    auto rebuilt = initial;
    rebuilt.version = 2;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), rebuilt);
    std::string stalePayload;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(initial, stalePayload));
    backend.BlockNextGet();
    DS_ASSERT_OK(controller.SubmitCoordinationEvent({ CoordinationEventType::RESET, "", "", 0, 0 }));
    const bool rebuildBlocked =
        backend.WaitUntilGetBlocked(std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT);
    if (!rebuildBlocked) {
        backend.ReleaseBlockedGet();
    }
    ASSERT_TRUE(rebuildBlocked);
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::PUT, keys->TopologyTable() + "/", std::move(stalePayload), 1,
          backend.CurrentRevision() }));
    backend.ReleaseBlockedGet();
    ASSERT_TRUE(WaitForCondition([&] { return controller.GetDiagnostics().topologyVersion == 2; }));
    const auto exactReads =
        backend.ExactGetCount(keys->TopologyTable(), TopologyKeyHelper::TopologyKey());
    const auto membershipReads = backend.RevisionGetAllCount(keys->MembershipTable());
    const auto before = clockCalls.load();
    ASSERT_TRUE(WaitForCondition([&] { return clockCalls.load() >= before + 2; }));
    EXPECT_EQ(controller.GetDiagnostics().topologyVersion, 2U);
    EXPECT_EQ(backend.ExactGetCount(keys->TopologyTable(), TopologyKeyHelper::TopologyKey()), exactReads);
    EXPECT_EQ(backend.RevisionGetAllCount(keys->MembershipTable()), membershipReads);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, ExternalQueueOverflowTriggersExactResync)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("watch-overflow-resync", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(1);
    TopologyState initial;
    initial.version = 1;
    initial.clusterHasInit = true;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    PutMembership(backend, *keys, initial.members.front().identity.address, MemberLifecycleState::READY);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(5);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    backend.BlockNextGet();
    DS_ASSERT_OK(controller.SubmitCoordinationEvent({ CoordinationEventType::RESET, "", "", 0, 0 }));
    const bool rebuildBlocked =
        backend.WaitUntilGetBlocked(std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT);
    if (!rebuildBlocked) {
        backend.ReleaseBlockedGet();
    }
    ASSERT_TRUE(rebuildBlocked);
    const auto membershipKey = keys->MembershipTable() + "/" + initial.members.front().identity.address;
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::PUT, membershipKey, EncodeMembership(MemberLifecycleState::READY), 1, 1 }));
    EXPECT_EQ(controller
                  .SubmitCoordinationEvent({ CoordinationEventType::PUT,
                                             keys->MembershipTable() + "/127.0.0.1:2",
                                             EncodeMembership(MemberLifecycleState::READY),
                                             1,
                                             2 })
                  .GetCode(),
              K_TRY_AGAIN);
    auto rebuilt = initial;
    rebuilt.version = 2;
    backend.SetAfterRevisionGetAllHandler(
        [&] { backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), rebuilt); });
    backend.ReleaseBlockedGet();

    ASSERT_TRUE(WaitForCondition([&] { return controller.GetDiagnostics().topologyVersion == 2; }));
    EXPECT_GE(backend.RevisionGetAllCount(keys->MembershipTable()), 3U);
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

MemberIdentity MakeLargeIdentity(uint32_t index)
{
    std::string identityBytes(16, '\0');
    std::copy_n(reinterpret_cast<const char *>(&index), sizeof(index), identityBytes.begin());
    return { std::move(identityBytes), "127.0.0.1:" + std::to_string(LARGE_PORT_BASE + index) };
}

TopologyState MakeLargeActiveTopology(size_t memberCount)
{
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members.reserve(memberCount);
    for (uint32_t index = 0; index < memberCount; ++index) {
        Member member{ MakeLargeIdentity(index), MemberState::ACTIVE, {} };
        for (uint32_t offset = 0; offset < LARGE_TOKENS_PER_MEMBER; ++offset) {
            member.tokens.push_back(index * LARGE_TOKENS_PER_MEMBER + offset);
        }
        latest.members.emplace_back(std::move(member));
    }
    return latest;
}

void PutLargeReadyMemberships(FakeCoordinationBackend &backend, const TopologyKeyHelper &keys, size_t memberCount)
{
    for (uint32_t index = 0; index < memberCount; ++index) {
        PutMembership(backend, keys, MakeLargeIdentity(index).address, MemberLifecycleState::READY);
    }
}

// Wait until topology reports no active batch or the deadline elapses.
bool WaitForNoActiveBatch(TopologyRepository &repository, std::chrono::steady_clock::time_point deadline)
{
    return WaitForTopology(repository, deadline, [](const auto &state) { return !state.activeBatch.has_value(); });
}

bool ActiveScaleInBatchHasLeavingCount(TopologyRepository &repository,
                                       std::chrono::steady_clock::time_point deadline, size_t leavingCount)
{
    return ActiveBatchHasMemberStateCount(repository, deadline, TopologyChangeType::SCALE_IN,
                                          MemberState::LEAVING, leavingCount);
}

bool ActiveScaleOutBatchHasJoiningCount(TopologyRepository &repository,
                                        std::chrono::steady_clock::time_point deadline, size_t joiningCount)
{
    return ActiveBatchHasMemberStateCount(repository, deadline, TopologyChangeType::SCALE_OUT,
                                          MemberState::JOINING, joiningCount);
}

bool WaitForFinalActiveMemberCount(TopologyRepository &repository,
                                   std::chrono::steady_clock::time_point deadline, size_t memberCount)
{
    return WaitForTopology(repository, deadline, [memberCount](const auto &state) {
        return !state.activeBatch.has_value() && state.members.size() == memberCount
               && std::all_of(state.members.begin(), state.members.end(),
                              [](const auto &member) { return member.state == MemberState::ACTIVE; });
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
    options.scaleOutCollectWindow = std::chrono::milliseconds(0);
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
    options.reconcileTick = std::chrono::seconds(5);
    options.scaleInCollectWindow = std::chrono::milliseconds(50);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeTwoActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    ASSERT_TRUE(ActiveScaleInBatchHasLeavingCount(repository, deadline, 1));
    TopologyState active;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(100, active, revision));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(active, revision, std::string(64, 'a'), snapshot));
    TopologyTaskMaterializer materializer;
    ExpectedDerivedState expected;
    DS_ASSERT_OK(materializer.RebuildExpected(*snapshot, algorithm, expected));
    EXPECT_TRUE(WaitForDerivedState(
        repository, expected, std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// The collect deadline wakes the controller instead of waiting for the full reconcile tick.
TEST(TopologyControllerTest, ScaleInCollectWindowDeadlinePreemptsReconcileTick)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-deadline-wakeup", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(500);
    options.scaleInCollectWindow = std::chrono::milliseconds(50);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeTwoActiveMembersScaleInInitialState());
    DS_ASSERT_OK(controller.Start());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    // The old fixed-tick loop needs about 1s after the event; deadline-aware waiting finishes in about 550ms.
    EXPECT_TRUE(ActiveScaleInBatchHasLeavingCount(
        repository, std::chrono::steady_clock::now() + std::chrono::milliseconds(750), 1));
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
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(),
                   MakeThreeActiveMembersScaleInInitialState());
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

TEST(TopologyControllerTest, ScaleOutCollectWindowCoalescesFiveHundredMembersInSameEpoch)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-scale-out-500", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    TopologyControllerOptions options;
    options.scaleOutCollectWindow = LARGE_COLLECT_WINDOW;
    options.scaleInCollectWindow = std::chrono::milliseconds(0);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(),
                   MakeLargeActiveTopology(LARGE_STEADY_MEMBER_COUNT));
    PutLargeReadyMemberships(backend, *keys, LARGE_EXPANDED_MEMBER_COUNT);
    const auto startedAt = std::chrono::steady_clock::now();
    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(ActiveScaleOutBatchHasJoiningCount(
        repository, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT, LARGE_BATCH_MEMBER_COUNT));
    ExpectedDerivedState expected;
    DS_ASSERT_OK(RebuildExpectedFromRepository(repository, algorithm, expected));
    ASSERT_EQ(expected.tasks.size(), LARGE_BATCH_MEMBER_COUNT);
    ASSERT_TRUE(WaitForDerivedState(
        repository, expected, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT));
    DS_ASSERT_OK(FinishExpectedMigrateTasks(repository, expected));
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    EXPECT_TRUE(WaitForFinalActiveMemberCount(
        repository, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT, LARGE_EXPANDED_MEMBER_COUNT));
    const auto elapsed = std::chrono::steady_clock::now() - startedAt;
    std::cout << "CLUSTER_PERF scope=controller_scale_out joining=" << LARGE_BATCH_MEMBER_COUNT
              << " elapsed_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
              << std::endl;
    EXPECT_LT(elapsed, LARGE_CONTROL_BUDGET);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT));
}

TEST(TopologyControllerTest, ScaleInCollectWindowCoalescesFiveHundredMembersInSameEpoch)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-coalesce-500", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    TopologyControllerOptions options;
    options.scaleInCollectWindow = LARGE_COLLECT_WINDOW;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    const TopologyState latest = MakeLargeActiveTopology(LARGE_EXPANDED_MEMBER_COUNT);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    for (size_t index = 0; index < latest.members.size(); ++index) {
        const auto state =
            index < LARGE_BATCH_MEMBER_COUNT ? MemberLifecycleState::EXITING : MemberLifecycleState::READY;
        PutMembership(backend, *keys, latest.members[index].identity.address, state);
    }
    const auto startedAt = std::chrono::steady_clock::now();
    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(ActiveScaleInBatchHasLeavingCount(
        repository, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT, LARGE_BATCH_MEMBER_COUNT));
    ExpectedDerivedState expected;
    DS_ASSERT_OK(RebuildExpectedFromRepository(repository, algorithm, expected));
    ASSERT_EQ(expected.tasks.size(), LARGE_BATCH_MEMBER_COUNT);
    ASSERT_EQ(expected.notifiesByAddress.size(), LARGE_BATCH_MEMBER_COUNT);
    ASSERT_TRUE(WaitForDerivedState(
        repository, expected, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT));
    DS_ASSERT_OK(FinishExpectedMigrateTasks(repository, expected));
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    EXPECT_TRUE(WaitForFinalActiveMemberCount(
        repository, std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT, LARGE_STEADY_MEMBER_COUNT));
    const auto elapsed = std::chrono::steady_clock::now() - startedAt;
    std::cout << "CLUSTER_PERF scope=controller_scale_in leaving=" << LARGE_BATCH_MEMBER_COUNT
              << " elapsed_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
              << std::endl;
    EXPECT_LT(elapsed, LARGE_CONTROL_BUDGET);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + LARGE_BATCH_WAIT_TIMEOUT));
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
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(),
                   MakeThreeActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    PutMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    // Wait for the first member's batch to commit, then exit the second member.
    ASSERT_TRUE(ActiveScaleInBatchHasLeavingCount(
        repository, std::chrono::steady_clock::now() + std::chrono::seconds(2), 1));
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
    EXPECT_TRUE(ActiveScaleInBatchHasLeavingCount(
        repository, std::chrono::steady_clock::now() + std::chrono::seconds(1), 1));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

// 6. A READY scale-out candidate takes priority over an uncommitted scale-in collection.
TEST(TopologyControllerTest, ReadyScaleOutCandidateCancelsPendingScaleInCollection)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collect-scaleout-priority", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(10);
    options.scaleInCollectWindow = std::chrono::milliseconds(500);
    options.scaleOutCollectWindow = std::chrono::milliseconds(0);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeTwoActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());

    // PRE_LEAVING without an active batch confirms that the ScaleIn collection is pending.
    ASSERT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + std::chrono::seconds(1),
                                [](const auto &state) {
                                    return !state.activeBatch.has_value()
                                           && std::any_of(state.members.begin(), state.members.end(),
                                                          [](const auto &member) {
                                                              return member.identity.address == "127.0.0.1:1"
                                                                     && member.state == MemberState::PRE_LEAVING;
                                                          });
                                }));

    // The new READY member is first admitted as INITIAL, then its ScaleOut cancels the pending ScaleIn collection.
    PutMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    backend.EmitEvent({ CoordinationEventType::RESET, "", "", 0, 0 });
    EXPECT_TRUE(ActiveScaleOutBatchHasJoiningCount(
        repository, std::chrono::steady_clock::now() + std::chrono::seconds(1), 1));
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
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(),
                   MakeThreeActiveMembersScaleInInitialState());
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::EXITING);
    PutMembership(backend, *keys, "127.0.0.1:3", MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.Start());
    // Both exit within the window, but only one is admitted to this batch.
    EXPECT_TRUE(ActiveScaleInBatchHasLeavingCount(
        repository, std::chrono::steady_clock::now() + std::chrono::seconds(2), 1));
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

TEST(TopologyControllerTest, ExternalWitnessProbeRequiresProbeEpoch)
{
    TopologyControllerOptions options;
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL;
    EXPECT_FALSE(options.IsValid());
    options.probeEpoch = "coordinator-test";
    EXPECT_TRUE(options.IsValid());
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    options.probeEpoch.clear();
    EXPECT_TRUE(options.IsValid());
}

TEST(TopologyControllerTest, WitnessProbeOptionsRequireValidWitnessCountAndRound)
{
    TopologyControllerOptions options;
    EXPECT_TRUE(options.IsValid());
    options.failureProbeWitnessCount = 0;
    EXPECT_FALSE(options.IsValid());
    options.failureProbeWitnessCount = 3;
    options.initialProbeRound = 0;
    EXPECT_FALSE(options.IsValid());
    options.initialProbeRound = 1;
    options.witnessProbeRoundTimeout = options.failureProbeTimeout;
    EXPECT_FALSE(options.IsValid());
}

TEST(TopologyControllerTest, OneWitnessKeyReceivesIndependentEventsForOutstandingTargets)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("witness-target-events", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } },
                       Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::ACTIVE, { 3 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, "127.0.0.1:1", MemberLifecycleState::READY);
    const auto revisionBeforeProbe = backend.CurrentRevision();

    TopologyControllerOptions options;
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL;
    options.probeEpoch = "coordinator-test";
    options.reconcileTick = std::chrono::milliseconds(1);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(WaitForCondition([&] { return backend.CurrentRevision() >= revisionBeforeProbe + 2; }));
    coordinator::WorkerProbeEventValuePb value;
    std::string encoded;
    DS_ASSERT_OK(backend.Get(keys->ProbeTable(), "127.0.0.1:1", encoded));
    ASSERT_TRUE(value.ParseFromString(encoded));
    EXPECT_TRUE(value.target_address() == "127.0.0.1:2" || value.target_address() == "127.0.0.1:3");
    EXPECT_GT(value.probe_round(), 0U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, CoordinatorWitnessEvidenceContinuesToProtectMissingMember)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("witness-protected", keys));
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

    std::atomic<int64_t> nowSeconds{ 0 };
    TopologyControllerOptions options;
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL;
    options.probeEpoch = "coordinator-test";
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(1);
    options.witnessProbeRoundTimeout = std::chrono::seconds(2);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.now = [&] { return std::chrono::steady_clock::time_point(std::chrono::seconds(nowSeconds.load())); };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    coordinator::WorkerProbeEventValuePb first;
    ASSERT_TRUE(WaitForCondition([&] {
        std::string value;
        return backend.Get(keys->ProbeTable(), "127.0.0.1:1", value).IsOk() && first.ParseFromString(value);
    }));
    const auto firstRound = ProbeRoundFor(first, "127.0.0.1:2");
    ASSERT_GT(firstRound, 0U);
    DS_ASSERT_OK(controller.SubmitWorkerLivenessReport(
        { "coordinator-test", "127.0.0.1:1", { std::string(16, 'b'), "127.0.0.1:2" }, firstRound,
          WorkerLivenessResult::UNREACHABLE }));
    DS_ASSERT_OK(controller.SubmitWorkerLivenessReport(
        { "coordinator-test", "127.0.0.1:1", { std::string(16, 'b'), "127.0.0.1:2" }, firstRound,
          WorkerLivenessResult::REACHABLE }));
    ASSERT_TRUE(WaitForCondition([&] { return dispatcher.GetStats().queueDepth == 0; }));

    nowSeconds.store(2);
    coordinator::WorkerProbeEventValuePb second;
    ASSERT_TRUE(WaitForCondition([&] {
        std::string value;
        return backend.Get(keys->ProbeTable(), "127.0.0.1:1", value).IsOk() && second.ParseFromString(value)
               && ProbeRoundFor(second, "127.0.0.1:2") != firstRound;
    }));
    const auto secondRound = ProbeRoundFor(second, "127.0.0.1:2");
    ASSERT_GT(secondRound, 0U);
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    EXPECT_FALSE(observed.activeBatch.has_value());

    DS_ASSERT_OK(controller.SubmitWorkerLivenessReport(
        { "coordinator-test", "127.0.0.1:1", { std::string(16, 'b'), "127.0.0.1:2" }, secondRound,
          WorkerLivenessResult::REACHABLE }));
    ASSERT_TRUE(WaitForCondition([&] { return dispatcher.GetStats().queueDepth == 0; }));
    nowSeconds.store(4);
    coordinator::WorkerProbeEventValuePb third;
    ASSERT_TRUE(WaitForCondition([&] {
        std::string value;
        return backend.Get(keys->ProbeTable(), "127.0.0.1:1", value).IsOk() && third.ParseFromString(value)
               && ProbeRoundFor(third, "127.0.0.1:2") != secondRound;
    }));
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    EXPECT_FALSE(observed.activeBatch.has_value());
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, CoordinatorOldRoundEvidenceDoesNotProtectCurrentProbe)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("witness-old-round", keys));
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

    std::atomic<int64_t> nowSeconds{ 0 };
    TopologyControllerOptions options;
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL;
    options.probeEpoch = "coordinator-test";
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(1);
    options.witnessProbeRoundTimeout = std::chrono::seconds(2);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.now = [&] { return std::chrono::steady_clock::time_point(std::chrono::seconds(nowSeconds.load())); };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());

    coordinator::WorkerProbeEventValuePb first;
    ASSERT_TRUE(WaitForCondition([&] {
        std::string value;
        return backend.Get(keys->ProbeTable(), "127.0.0.1:1", value).IsOk() && first.ParseFromString(value);
    }));
    const auto firstRound = ProbeRoundFor(first, "127.0.0.1:2");
    ASSERT_GT(firstRound, 0U);
    DS_ASSERT_OK(controller.SubmitWorkerLivenessReport(
        { "coordinator-test", "127.0.0.1:1", { std::string(16, 'b'), "127.0.0.1:2" }, firstRound,
          WorkerLivenessResult::REACHABLE }));
    ASSERT_TRUE(WaitForCondition([&] { return dispatcher.GetStats().queueDepth == 0; }));
    nowSeconds.store(2);

    coordinator::WorkerProbeEventValuePb second;
    ASSERT_TRUE(WaitForCondition([&] {
        std::string value;
        return backend.Get(keys->ProbeTable(), "127.0.0.1:1", value).IsOk() && second.ParseFromString(value)
               && ProbeRoundFor(second, "127.0.0.1:2") != firstRound;
    }));
    DS_ASSERT_OK(controller.SubmitWorkerLivenessReport(
        { "coordinator-test", "127.0.0.1:1", { std::string(16, 'b'), "127.0.0.1:2" }, firstRound,
          WorkerLivenessResult::REACHABLE }));
    ASSERT_TRUE(WaitForCondition([&] { return dispatcher.GetStats().queueDepth == 0; }));
    nowSeconds.store(4);
    EXPECT_TRUE(WaitForCondition([&] {
        TopologyState observed;
        int64_t revision = 0;
        return repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision).IsOk()
               && observed.activeBatch.has_value()
               && observed.activeBatch->type == TopologyChangeType::FAILURE;
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, CoordinatorMissingMemberWithoutWitnessEvidenceCommitsFailure)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("witness-unreachable", keys));
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

    std::atomic<int64_t> nowSeconds{ 0 };
    TopologyControllerOptions options;
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL;
    options.probeEpoch = "coordinator-test";
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(1);
    options.witnessProbeRoundTimeout = std::chrono::seconds(2);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.now = [&] { return std::chrono::steady_clock::time_point(std::chrono::seconds(nowSeconds.load())); };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());
    coordinator::WorkerProbeEventValuePb value;
    ASSERT_TRUE(WaitForCondition([&] {
        std::string encoded;
        return backend.Get(keys->ProbeTable(), "127.0.0.1:1", encoded).IsOk() && value.ParseFromString(encoded);
    }));
    const auto round = ProbeRoundFor(value, "127.0.0.1:2");
    ASSERT_GT(round, 0U);
    DS_ASSERT_OK(controller.SubmitWorkerLivenessReport(
        { "stale-coordinator", "127.0.0.1:1", { std::string(16, 'b'), "127.0.0.1:2" }, round,
          WorkerLivenessResult::REACHABLE }));
    ASSERT_TRUE(WaitForCondition([&] { return dispatcher.GetStats().queueDepth == 0; }));

    nowSeconds.store(2);
    EXPECT_TRUE(WaitForCondition([&] {
        TopologyState observed;
        int64_t revision = 0;
        return repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision).IsOk()
               && observed.activeBatch.has_value()
               && observed.activeBatch->type == TopologyChangeType::FAILURE;
    }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
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
        return RespondedProbe(
            { targets.front(), ControlBackendState::UNAVAILABLE, expected->Version(), expected->AuthorityRevision(),
              expected->CanonicalDigest(), std::chrono::steady_clock::now() }
        );
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
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, 1);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(2);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = *replacements.begin();
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
            return std::vector<ControlBackendProbeResult>{};
        }
        EXPECT_EQ(targets.front(), latest.members.front().identity);
        return RespondedProbe(
            { targets.front(), ControlBackendState::UNKNOWN, 0, 0, "", std::chrono::steady_clock::now() }
        );
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
            return std::vector<ControlBackendProbeResult>{};
        }
        return RespondedProbe(
            { targets.front(), ControlBackendState::UNAVAILABLE, expected->Version(),
              expected->AuthorityRevision(), expected->CanonicalDigest(), std::chrono::steady_clock::now() }
        );
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

TEST(TopologyControllerTest, RepeatedEmptyMembershipDoesNotStartFailureBatch)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collective-membership-absence", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(MEMBER_ID_SIZE, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(MEMBER_ID_SIZE, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(2);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    std::atomic<int64_t> clockSeconds{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        return NoResponseProbe(targets);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(COLLECTIVE_ABSENCE_FIRST_WINDOW_SECONDS);
    const auto firstWindowAttempts = backend.GetAttemptCount() + COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW;
    ASSERT_TRUE(backend.WaitForGetAttempts(firstWindowAttempts,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, latest.version, COLLECTIVE_ABSENCE_MEMBER_COUNT);
    clockSeconds.store(COLLECTIVE_ABSENCE_SECOND_WINDOW_SECONDS);
    const auto secondWindowAttempts = backend.GetAttemptCount() + COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW;
    ASSERT_TRUE(backend.WaitForGetAttempts(secondWindowAttempts,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, latest.version, COLLECTIVE_ABSENCE_MEMBER_COUNT);
    EXPECT_EQ(probeCalls.load(), 0);

    PutMembership(backend, *keys, latest.members.front().identity.address, MemberLifecycleState::READY);
    const auto recoveryAttempts = backend.GetAttemptCount() + COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW;
    ASSERT_TRUE(backend.WaitForGetAttempts(recoveryAttempts,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(COLLECTIVE_ABSENCE_SECOND_WINDOW_SECONDS + 4);
    const auto recoveryDeadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    EXPECT_TRUE(WaitForTopology(repository, recoveryDeadline, [](const auto &state) {
        return state.activeBatch.has_value() && state.activeBatch->type == TopologyChangeType::FAILURE;
    }));
    EXPECT_GE(probeCalls.load(), 1);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, CollectiveStaleTopologyBootstrapsReadyReplacementMembers)
{
    constexpr size_t memberCount = 4;
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collective-stale-bootstrap", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    auto latest = MakeActiveTopology(memberCount, STALE_TOPOLOGY_OLD_PORT_BASE);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, memberCount);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = *replacements.begin();
    std::atomic<int64_t> clockSeconds{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    std::set<std::string> probedAddresses;
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        EXPECT_EQ(targets.size(), 1U);
        if (targets.size() == 1) {
            probedAddresses.insert(targets.front().address);
        }
        const auto call = probeCalls.fetch_add(1);
        auto results = NoResponseProbe(targets);
        if (call % 2 == 1 && !results.empty()) {
            results.front().outcome = ControlBackendProbeOutcome::UNAVAILABLE;
        }
        return results;
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    EXPECT_EQ(probeCalls.load(), 0U);
    clockSeconds.store(2);
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT,
                                [&](const TopologyState &state) {
                                    return IsExactActiveTopology(state, replacements);
                                }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
    EXPECT_EQ(probeCalls.load(), memberCount);
    EXPECT_EQ(probedAddresses, MemberAddresses(latest));
}

TEST(TopologyControllerTest, ExternalCollectiveControlEpochFencesProbeProgress)
{
    constexpr size_t memberCount = STALE_TOPOLOGY_PROBE_SAMPLE_COUNT;
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collective-control-epoch", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    const auto latest = MakeActiveTopology(memberCount, STALE_TOPOLOGY_OLD_PORT_BASE);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, memberCount);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(0);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL;
    options.probeEpoch = "coordinator-test";
    std::atomic<uint64_t> controlEpoch{ 0 };
    std::atomic<size_t> epochReads{ 0 };
    options.collectiveControlEpoch = [&]() -> std::optional<uint64_t> {
        ++epochReads;
        const auto epoch = controlEpoch.load();
        return epoch == 0 ? std::nullopt : std::optional<uint64_t>{ epoch };
    };
    WaitPost rejectedEpochTwoEntered;
    WaitPost releaseRejectedEpochTwo;
    std::atomic<size_t> rejectedEpochTwo{ 0 };
    std::atomic<size_t> allowedEpochThree{ 0 };
    options.collectiveReplacementFence =
        [&](uint64_t expectedEpoch, const std::function<Status()> &mutation) {
            if (controlEpoch.load() != expectedEpoch) {
                if (expectedEpoch == 2) {
                    ++rejectedEpochTwo;
                    rejectedEpochTwoEntered.Set();
                    releaseRejectedEpochTwo.Wait();
                }
                return Status(K_NOT_READY, "injected stale collective control epoch");
            }
            if (expectedEpoch == 3) {
                ++allowedEpochThree;
            }
            return mutation();
        };
    std::atomic<size_t> probeCalls{ 0 };
    std::vector<std::string> probedAddresses;
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        EXPECT_EQ(targets.size(), 1U);
        if (targets.empty()) {
            return std::vector<ControlBackendProbeResult>{};
        }
        probedAddresses.push_back(targets.front().address);
        const auto call = probeCalls.fetch_add(1) + 1;
        if (call == memberCount) {
            controlEpoch.store(3);
        }
        return NoResponseProbe(targets, ControlBackendProbeOutcome::UNAVAILABLE);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(WaitForCondition([&] {
        return epochReads.load() >= COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW;
    }));
    EXPECT_EQ(probeCalls.load(), 0U);
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, latest.version, memberCount);

    controlEpoch.store(2);
    const bool staleFenceEntered = rejectedEpochTwoEntered.WaitFor(RECOVERY_STOP_TIMEOUT.count() * 1'000);
    EXPECT_TRUE(staleFenceEntered);
    if (!staleFenceEntered) {
        releaseRejectedEpochTwo.Set();
        DS_EXPECT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
        return;
    }
    EXPECT_EQ(probeCalls.load(), memberCount);
    EXPECT_EQ(rejectedEpochTwo.load(), 1U);
    DS_EXPECT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, latest.version, memberCount);
    releaseRejectedEpochTwo.Set();
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT,
                                [&](const TopologyState &state) {
                                    return IsExactActiveTopology(state, replacements);
                                }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));

    EXPECT_EQ(probeCalls.load(), 10U);
    EXPECT_EQ(rejectedEpochTwo.load(), 1U);
    EXPECT_EQ(allowedEpochThree.load(), 1U);
    ASSERT_EQ(probedAddresses.size(), 10U);
    EXPECT_EQ(probedAddresses[0], probedAddresses[5]);
}

TEST(TopologyControllerTest, SingleStaleTopologyBootstrapsReadyReplacementMember)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("single-stale-bootstrap", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    auto latest = MakeActiveTopology(1, STALE_TOPOLOGY_OLD_PORT_BASE);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, 1);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = *replacements.begin();
    std::atomic<int64_t> clockSeconds{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        return NoResponseProbe(targets, ControlBackendProbeOutcome::UNAVAILABLE);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(2);
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT,
                                [&](const TopologyState &state) {
                                    return IsExactActiveTopology(state, replacements);
                                }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
    EXPECT_GE(probeCalls.load(), 1U);
}

TEST(TopologyControllerTest, CollectiveBootstrapUsesFiveEvenlySpacedAddressSamples)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collective-stale-sampling", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    auto latest = MakeLargeActiveTopology(LARGE_STEADY_MEMBER_COUNT);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, 4);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = *replacements.begin();
    std::atomic<int64_t> clockSeconds{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    std::set<std::string> sampledAddresses;
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        EXPECT_EQ(targets.size(), 1U);
        if (targets.size() == 1) {
            sampledAddresses.insert(targets.front().address);
        }
        ++probeCalls;
        return NoResponseProbe(targets);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(2);
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT,
                                [&](const TopologyState &state) {
                                    return IsExactActiveTopology(state, replacements);
                                }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));

    EXPECT_EQ(probeCalls.load(), STALE_TOPOLOGY_PROBE_SAMPLE_COUNT);
    const auto sortedAddresses = MemberAddresses(latest);
    std::vector<std::string> addresses(sortedAddresses.begin(), sortedAddresses.end());
    std::set<std::string> expected;
    for (size_t index = 0; index < STALE_TOPOLOGY_PROBE_SAMPLE_COUNT; ++index) {
        expected.insert(addresses[index * (addresses.size() - 1) / (STALE_TOPOLOGY_PROBE_SAMPLE_COUNT - 1)]);
    }
    EXPECT_EQ(sampledAddresses, expected);
}

TEST(TopologyControllerTest, ExternalEtcdOwnerRoundTripInvalidatesCollectiveProbeProgress)
{
    constexpr size_t memberCount = 4;
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collective-owner-round-trip", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    const auto latest = MakeActiveTopology(memberCount, STALE_TOPOLOGY_OLD_PORT_BASE);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, 2);
    const auto ownerA = *replacements.begin();
    backend.SetStorePrefix(keys->MembershipTable(), keys->EtcdMembershipTablePrefix());

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    options.localAddress = ownerA;
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(
            clockCalls.fetch_add(1) == 0 ? std::chrono::seconds(0) : std::chrono::seconds(2));
    };
    std::atomic<size_t> probeCalls{ 0 };
    std::vector<std::string> probedAddresses;
    TopologyController *controllerPtr = nullptr;
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        EXPECT_EQ(targets.size(), 1U);
        if (targets.empty()) {
            return std::vector<ControlBackendProbeResult>{};
        }
        probedAddresses.push_back(targets.front().address);
        if (probeCalls.fetch_add(1) == 0) {
            DS_EXPECT_OK(backend.Delete(keys->MembershipTable(), ownerA));
            DS_EXPECT_OK(controllerPtr->SubmitCoordinationEvent(
                { CoordinationEventType::DELETE, keys->EtcdMembershipTablePrefix() + "/" + ownerA, {}, 0,
                  backend.CurrentRevision() }));
        }
        return NoResponseProbe(targets, ControlBackendProbeOutcome::UNAVAILABLE);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    controllerPtr = &controller;

    DS_ASSERT_OK(controller.Start());
    for (const auto &address : replacements) {
        DS_ASSERT_OK(controller.SubmitCoordinationEvent(
            { CoordinationEventType::PUT, keys->EtcdMembershipTablePrefix() + "/" + address,
              EncodeMembership(MemberLifecycleState::READY), 0, backend.CurrentRevision() }));
    }
    ASSERT_TRUE(WaitForCondition([&] { return probeCalls.load() == 1U; }));
    const auto callsWithOwnerB = clockCalls.load() + COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW;
    ASSERT_TRUE(WaitForCondition([&] { return clockCalls.load() >= callsWithOwnerB; }));
    EXPECT_EQ(probeCalls.load(), 1U);

    PutMembership(backend, *keys, ownerA, MemberLifecycleState::READY);
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::PUT, keys->EtcdMembershipTablePrefix() + "/" + ownerA,
          EncodeMembership(MemberLifecycleState::READY), 0, backend.CurrentRevision() }));
    ASSERT_TRUE(WaitForCondition([&] { return probeCalls.load() == 2U; }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));

    ASSERT_EQ(probedAddresses.size(), 2U);
    EXPECT_EQ(probedAddresses[0], probedAddresses[1]);
}

TEST(TopologyControllerTest, ReachableCollectiveBootstrapSamplePreservesStaleTopology)
{
    constexpr size_t memberCount = 4;
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collective-stale-reachable", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    auto latest = MakeActiveTopology(memberCount, STALE_TOPOLOGY_OLD_PORT_BASE);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, memberCount);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = *replacements.begin();
    std::atomic<int64_t> clockSeconds{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        EXPECT_EQ(targets.size(), 1U);
        ++probeCalls;
        if (targets.empty()) {
            return std::vector<ControlBackendProbeResult>{};
        }
        const auto &target = targets.front();
        return std::vector<ControlBackendProbeResult>{
            { target,
              ControlBackendObservation{ target, ControlBackendState::UNKNOWN, 0, 0, "",
                                         std::chrono::steady_clock::now() },
              ControlBackendProbeOutcome::ERROR,
              std::chrono::milliseconds(1) }
        };
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(2);
    EXPECT_TRUE(WaitForCondition([&] { return probeCalls.load() == 1U; }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
    EXPECT_EQ(probeCalls.load(), 1U);
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    EXPECT_EQ(MemberAddresses(observed), MemberAddresses(latest));
}

TEST(TopologyControllerTest, CollectiveBootstrapRetriesNeutralProbeResults)
{
    constexpr size_t memberCount = 4;
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collective-stale-neutral-retry", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    auto latest = MakeActiveTopology(memberCount, STALE_TOPOLOGY_OLD_PORT_BASE);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, memberCount);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = *replacements.begin();
    std::atomic<int64_t> clockSeconds{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    std::vector<std::string> probedAddresses;
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        EXPECT_EQ(targets.size(), 1U);
        if (targets.size() == 1) {
            probedAddresses.push_back(targets.front().address);
        }
        const auto call = probeCalls.fetch_add(1);
        clockSeconds.fetch_add(2);
        if (call == 0) {
            return NoResponseProbe(targets, ControlBackendProbeOutcome::CANCELLED);
        }
        if (call == 1) {
            return std::vector<ControlBackendProbeResult>{};
        }
        return NoResponseProbe(targets);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(2);
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT,
                                [&](const TopologyState &state) {
                                    return IsExactActiveTopology(state, replacements);
                                }));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
    EXPECT_EQ(probeCalls.load(), memberCount + 2);
    ASSERT_GE(probedAddresses.size(), 3U);
    EXPECT_EQ(probedAddresses[0], probedAddresses[1]);
    EXPECT_EQ(probedAddresses[1], probedAddresses[2]);
}

TEST(TopologyControllerTest, CollectiveBootstrapStopsWhenExactReadFindsOldMember)
{
    constexpr size_t memberCount = 4;
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("collective-stale-exact-recovery", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    const auto latest = MakeActiveTopology(memberCount, STALE_TOPOLOGY_OLD_PORT_BASE);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    const auto replacements = PutReadyReplacementMemberships(backend, *keys, memberCount);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = *replacements.begin();
    std::atomic<int64_t> clockSeconds{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    std::atomic<size_t> membershipReadsBeforeExact{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        EXPECT_EQ(targets.size(), 1U);
        if (probeCalls.fetch_add(1) + 1 == memberCount) {
            for (const auto &address : replacements) {
                DS_EXPECT_OK(backend.Delete(keys->MembershipTable(), address));
            }
            for (const auto &member : latest.members) {
                PutMembership(backend, *keys, member.identity.address, MemberLifecycleState::READY);
            }
            membershipReadsBeforeExact.store(backend.GetAllCount(keys->MembershipTable()));
        }
        return NoResponseProbe(targets, ControlBackendProbeOutcome::UNAVAILABLE);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(2);
    ASSERT_TRUE(WaitForCondition([&] {
        return membershipReadsBeforeExact.load() > 0
               && backend.GetAllCount(keys->MembershipTable()) > membershipReadsBeforeExact.load();
    }));
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    EXPECT_EQ(observed.version, latest.version);
    EXPECT_EQ(MemberAddresses(observed), MemberAddresses(latest));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, SingleCommittedAbsenceWithoutReadyReplacementPreservesTopology)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("single-committed-no-replacement", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(RECOVERY_DISPATCHER_CAPACITY);
    const auto latest = MakeActiveTopology(1, STALE_TOPOLOGY_OLD_PORT_BASE);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.localAddress = latest.members.front().identity.address;
    std::atomic<int64_t> clockSeconds{ 0 };
    options.now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::seconds(clockSeconds.load()));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        return NoResponseProbe(targets);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(backend.WaitForGetAttempts(COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    clockSeconds.store(2);
    const auto attempts = backend.GetAttemptCount() + COLLECTIVE_ABSENCE_RECONCILES_PER_WINDOW;
    ASSERT_TRUE(backend.WaitForGetAttempts(attempts,
                                           std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT));
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    EXPECT_EQ(observed.version, latest.version);
    EXPECT_EQ(MemberAddresses(observed), MemberAddresses(latest));
    EXPECT_EQ(probeCalls.load(), 0U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
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
        return std::vector<ControlBackendProbeResult>{
            { targets.front(), std::nullopt, ControlBackendProbeOutcome::DEADLINE_EXCEEDED,
              std::chrono::milliseconds(7) }
        };
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

TEST(TopologyControllerTest, ObservationFromDifferentTargetDoesNotProveMissingMemberReachable)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("mismatched-probe-target", keys));
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
        EXPECT_EQ(targets.size(), 1U);
        ControlBackendObservation wrongTarget{ latest.members.front().identity, ControlBackendState::AVAILABLE, 1, 1,
                                               std::string(64, 'a'), std::chrono::steady_clock::now() };
        return std::vector<ControlBackendProbeResult>{
            { targets.front(), std::move(wrongTarget), ControlBackendProbeOutcome::RESPONSE,
              std::chrono::milliseconds(1) }
        };
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    EXPECT_TRUE(WaitForTopology(repository, std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT,
                                [](const auto &state) {
                                    return state.activeBatch.has_value()
                                           && state.activeBatch->type == TopologyChangeType::FAILURE;
                                }));
    EXPECT_EQ(probeCalls.load(), 1U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, ExternalEventSourceConfirmsMultipleFailuresWithoutWorkerProbe)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("external-multiple-failure", keys));
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyState latest;
    latest.version = 1;
    latest.clusterHasInit = true;
    latest.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1 } },
                       Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 2 } },
                       Member{ { std::string(16, 'c'), "127.0.0.1:3" }, MemberState::ACTIVE, { 3 } } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), latest);
    PutMembership(backend, *keys, latest.members.front().identity.address, MemberLifecycleState::READY);

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.failureProbeTimeout = std::chrono::seconds(1);
    options.witnessProbeRoundTimeout = std::chrono::seconds(2);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL;
    options.probeEpoch = "coordinator-test";
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &, auto) {
        ++probeCalls;
        return std::vector<ControlBackendProbeResult>{};
    };
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));
    std::atomic<int64_t> elapsedSeconds{ 0 };
    options.now = [&] { return start + std::chrono::seconds(elapsedSeconds.load()); };
    const auto revisionBeforeProbe = backend.CurrentRevision();
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(WaitForCondition([&] { return backend.CurrentRevision() >= revisionBeforeProbe + 2; }));
    elapsedSeconds.store(2);
    DS_ASSERT_OK(controller.SubmitCoordinationEvent({ CoordinationEventType::RESET, "", "", 0, 0 }));
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    EXPECT_TRUE(WaitForTopology(repository, deadline, [](const auto &state) {
        if (!state.activeBatch.has_value() || state.activeBatch->type != TopologyChangeType::FAILURE) {
            return false;
        }
        const auto failed = std::count_if(state.members.begin(), state.members.end(), [](const auto &member) {
            return member.state == MemberState::FAILED;
        });
        return failed == 2;
    }));
    EXPECT_EQ(probeCalls.load(), 0U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, CentralizedControllerMaterializesAndClearsRestartFacts)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("centralized-restart", keys));
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
    backend.PutBytes(keys->MembershipTable(), "127.0.0.1:2",
                     EncodeMembership(MemberLifecycleState::RESTARTING, 100));
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL;
    options.probeEpoch = "coordinator-test";
    options.materializeRestartFacts = true;
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    TopologyTaskNotify observed;
    while (std::chrono::steady_clock::now() < deadline
           && (repository.ReadNotify("127.0.0.1:1", observed).IsError()
               || observed.restartTimestampsByAddress.count("127.0.0.1:2") == 0)) {
        std::this_thread::yield();
    }
    ASSERT_EQ(observed.restartTimestampsByAddress.at("127.0.0.1:2"), 100);

    PutMembership(backend, *keys, "127.0.0.1:2", MemberLifecycleState::READY);
    while (std::chrono::steady_clock::now() < deadline
           && repository.ReadNotify("127.0.0.1:1", observed).GetCode() != K_NOT_FOUND) {
        std::this_thread::yield();
    }
    EXPECT_EQ(repository.ReadNotify("127.0.0.1:1", observed).GetCode(), K_NOT_FOUND);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + std::chrono::seconds(1)));
}

TEST(TopologyControllerTest, MembershipRestoredDuringProbeSuppressesFailureAfterExactRead)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("probe-exact-read-self-managed", keys));
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
        EXPECT_EQ(targets.size(), 1U);
        PutMembership(backend, *keys, targets.front().address, MemberLifecycleState::READY);
        return NoResponseProbe(targets, ControlBackendProbeOutcome::DEADLINE_EXCEEDED,
                               std::chrono::milliseconds(5));
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(WaitForCondition([&] { return probeCalls.load() == 1U && clockCalls.load() >= 3U; }));
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, latest.version, latest.members.size());
    EXPECT_EQ(probeCalls.load(), 1U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, ExternalEtcdMembershipRestoredDuringProbeSuppressesFailureAfterExactRead)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("probe-exact-read-external-etcd", keys));
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
    backend.SetStorePrefix(keys->MembershipTable(), keys->EtcdMembershipTablePrefix());

    TopologyControllerOptions options;
    options.nodeDeadTimeout = std::chrono::seconds(1);
    options.reconcileTick = std::chrono::milliseconds(1);
    options.eventSourceMode = TopologyEventSourceMode::EXTERNAL_ETCD;
    options.localAddress = latest.members.front().identity.address;
    const auto start = std::chrono::steady_clock::time_point(std::chrono::seconds(10));
    std::atomic<size_t> clockCalls{ 0 };
    options.now = [&] {
        return start + (clockCalls.fetch_add(1) == 0 ? std::chrono::seconds(0) : std::chrono::seconds(2));
    };
    std::atomic<size_t> probeCalls{ 0 };
    TopologyController *controllerPtr = nullptr;
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        EXPECT_EQ(targets.size(), 1U);
        PutMembership(backend, *keys, targets.front().address, MemberLifecycleState::READY);
        EXPECT_NE(controllerPtr, nullptr);
        if (controllerPtr != nullptr) {
            DS_EXPECT_OK(controllerPtr->SubmitCoordinationEvent(
                { CoordinationEventType::PUT, keys->EtcdMembershipTablePrefix() + "/" + targets.front().address,
                  EncodeMembership(MemberLifecycleState::READY), 0, backend.CurrentRevision() }));
        }
        return NoResponseProbe(targets, ControlBackendProbeOutcome::UNAVAILABLE, std::chrono::milliseconds(2));
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    controllerPtr = &controller;

    DS_ASSERT_OK(controller.Start());
    DS_ASSERT_OK(controller.SubmitCoordinationEvent(
        { CoordinationEventType::PUT,
          keys->EtcdMembershipTablePrefix() + "/" + latest.members.front().identity.address,
          EncodeMembership(MemberLifecycleState::READY), 0, backend.CurrentRevision() }));
    ASSERT_TRUE(WaitForCondition([&] { return probeCalls.load() == 1U && clockCalls.load() >= 3U; }));
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, latest.version, latest.members.size());
    EXPECT_EQ(probeCalls.load(), 1U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, ExactReadFailurePausesMissingBudgetAndSkipsFailureCommit)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("probe-exact-read-failure", keys));
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
        EXPECT_EQ(targets.size(), 1U);
        backend.FailNextGetAll();
        PutMembership(backend, *keys, targets.front().address, MemberLifecycleState::READY);
        return NoResponseProbe(targets, ControlBackendProbeOutcome::CANCELLED, std::chrono::milliseconds(1));
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    ASSERT_TRUE(WaitForCondition([&] { return probeCalls.load() == 1U && clockCalls.load() >= 3U; }));
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(CONTROLLER_TEST_READ_TIMEOUT_MS, observed, revision));
    ExpectActiveTopologyUnchanged(observed, latest.version, latest.members.size());
    EXPECT_EQ(probeCalls.load(), 1U);
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + RECOVERY_STOP_TIMEOUT));
}

TEST(TopologyControllerTest, OneCompleteDirectProbeWithoutResponseCommitsFailure)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("one-probe-unreachable-commits", keys));
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
        // Classifier confirms at missingMs≈2s (< nodeDead+probe=3s). One empty probe must still commit Failure.
        return start + (clockCalls.fetch_add(1) == 0 ? std::chrono::seconds(0) : std::chrono::seconds(2));
    };
    std::atomic<size_t> probeCalls{ 0 };
    options.memberLivenessProbe = [&](const std::vector<MemberIdentity> &targets, auto) {
        ++probeCalls;
        EXPECT_EQ(targets.size(), 1);
        return NoResponseProbe(targets);
    };
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);

    DS_ASSERT_OK(controller.Start());
    const auto deadline = std::chrono::steady_clock::now() + FAILURE_PROBE_WAIT_TIMEOUT;
    EXPECT_TRUE(WaitForTopology(repository, deadline, [](const auto &state) {
        return state.activeBatch.has_value() && state.activeBatch->type == TopologyChangeType::FAILURE;
    }));
    EXPECT_EQ(probeCalls.load(), 1);
    TopologyState observed;
    int64_t revision = 0;
    DS_ASSERT_OK(repository.ReadTopology(100, observed, revision));
    ASSERT_EQ(observed.members.size(), 2);
    EXPECT_EQ(observed.members.back().state, MemberState::FAILED);
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
        return RespondedProbe(
            { targets.front(), ControlBackendState::UNAVAILABLE, expected->Version(),
              expected->AuthorityRevision(), expected->CanonicalDigest(), std::chrono::steady_clock::now() }
        );
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
            return RespondedProbe(
                { targets.front(), ControlBackendState::UNAVAILABLE, expected->Version(),
                  expected->AuthorityRevision(), expected->CanonicalDigest(), std::chrono::steady_clock::now() }
            );
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
        std::vector<ControlBackendProbeResult> results;
        results.reserve(targets.size());
        for (const auto &target : targets) {
            results.push_back(
                { target,
                  ControlBackendObservation{ target, ControlBackendState::UNAVAILABLE, expected->Version(),
                                             expected->AuthorityRevision(), expected->CanonicalDigest(),
                                             std::chrono::steady_clock::now() },
                  ControlBackendProbeOutcome::RESPONSE, std::chrono::milliseconds(0) });
        }
        return results;
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
