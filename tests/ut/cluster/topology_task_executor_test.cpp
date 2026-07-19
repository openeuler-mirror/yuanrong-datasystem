/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Fenced task Executor component tests.
 */
#include "datasystem/cluster/executor/topology_task_executor.h"

#include <future>
#include <stdexcept>
#include <thread>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_controller.h"
#include "datasystem/cluster/control/topology_plan_builder.h"
#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/executor/key_filter.h"
#include "datasystem/cluster/executor/storage_scan_plan_access.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_reader.h"
#include "datasystem/cluster/runtime/topology_snapshot_state.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {
constexpr auto TEST_WAIT = std::chrono::seconds(1);
constexpr auto RETRY_TICK_ADVANCE = std::chrono::seconds(2);
constexpr int32_t TEST_AUTHORITY_READ_TIMEOUT_MS = 100;
constexpr size_t MAX_CALLBACK_THREADS = 16;
constexpr size_t MAX_CALLBACK_QUEUE_CAPACITY = 1'024;

class RecordingCallbacks final : public ITopologyPhaseCallbacks {
public:
    /**
     * @brief Destroy the recording callback.
     */
    ~RecordingCallbacks() override = default;
    Status OnScaleOut(const TopologyCallbackContext &context) override
    {
        ++scaleOutCalls;
        EXPECT_FALSE(context.businessOperationId.empty());
        EXPECT_FALSE(context.keyFilter.DebugString().empty());
        std::vector<StorageScanSegment> segments;
        EXPECT_TRUE(StorageScanPlanAccess::GetSegments(context.storageScanPlan, segments).IsOk());
        EXPECT_FALSE(segments.empty());
        {
            std::lock_guard<std::mutex> lock(mutex);
            scaleOutEntered = true;
        }
        entered.notify_all();
        if (throwScaleOut) {
            throw std::runtime_error("injected callback exception");
        }
        if (blockScaleOutUntilCancelled) {
            EXPECT_TRUE(context.cancellation.WaitUntil(context.deadline));
        }
        if (blockScaleOutUntilReleased) {
            std::unique_lock<std::mutex> lock(mutex);
            released.wait(lock, [this] { return releaseScaleOut; });
        }
        return scaleOutStatus;
    }

    Status OnScaleIn(const TopologyCallbackContext &context) override
    {
        ++scaleInCalls;
        scaleInOperation = context.businessOperationId;
        return scaleInStatus;
    }

    Status PrepareScaleInCleanup(const TopologyCallbackContext &,
                                 std::unique_ptr<TopologyPreparedCleanup> &cleanup) override
    {
        if (cleanupStatus.IsError()) {
            return cleanupStatus;
        }
        cleanup = std::make_unique<TopologyPreparedCleanup>(
            [this] {
                if (throwCleanupAuthorization) {
                    throw std::runtime_error("injected cleanup authorization exception");
                }
                ++cleanupAuthorizations;
                return Status::OK();
            },
            [this](std::chrono::steady_clock::time_point deadline, const CancellationToken &cancellation) {
                if (throwCleanupEffect) {
                    throw std::runtime_error("injected cleanup effect exception");
                }
                EXPECT_GT(deadline, std::chrono::steady_clock::now());
                EXPECT_FALSE(cancellation.IsCancelled());
                cleanupEffectThread = std::this_thread::get_id();
                ++cleanupEffects;
                return Status::OK();
            });
        return Status::OK();
    }

    Status OnFailure(const TopologyCallbackContext &) override
    {
        ++failureCalls;
        return failureStatus;
    }

    bool WaitUntilScaleOutEntered(std::chrono::steady_clock::time_point deadline)
    {
        std::unique_lock<std::mutex> lock(mutex);
        return entered.wait_until(lock, deadline, [this] { return scaleOutEntered; });
    }

    void ReleaseScaleOut()
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            releaseScaleOut = true;
        }
        released.notify_all();
    }

    Status scaleOutStatus{ Status::OK() };
    Status scaleInStatus{ Status::OK() };
    Status cleanupStatus{ Status::OK() };
    Status failureStatus{ Status::OK() };
    bool blockScaleOutUntilCancelled{ false };
    bool blockScaleOutUntilReleased{ false };
    bool throwScaleOut{ false };
    bool throwCleanupAuthorization{ false };
    bool throwCleanupEffect{ false };
    std::string scaleInOperation;
    std::atomic<size_t> scaleOutCalls{ 0 };
    std::atomic<size_t> scaleInCalls{ 0 };
    std::atomic<size_t> failureCalls{ 0 };
    std::atomic<size_t> cleanupAuthorizations{ 0 };
    std::atomic<size_t> cleanupEffects{ 0 };
    std::thread::id cleanupEffectThread;
    // Protects scaleOutEntered and coordinates the entered condition variable.
    std::mutex mutex;
    // Uses mutex to signal changes to scaleOutEntered.
    std::condition_variable entered;
    // Uses mutex to release a deliberately non-cooperative callback in shutdown tests.
    std::condition_variable released;
    bool scaleOutEntered{ false };
    bool releaseScaleOut{ false };
};

struct ExecutorScenario {
    /**
     * @brief Build one test scenario with a bounded runtime queue.
     * @param[in] dispatcherCapacity Queue capacity.
     */
    explicit ExecutorScenario(size_t dispatcherCapacity = 32) : dispatcher(dispatcherCapacity)
    {
    }
    Status BuildPlan(TopologyChangeType type)
    {
        current.version = 1;
        current.clusterHasInit = true;
        TopologyPlanBuilder builder(algorithm);
        if (type == TopologyChangeType::SCALE_OUT) {
            current.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, MemberState::ACTIVE, { 1, 100 } },
                                Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::INITIAL, {} } };
            return builder.BuildScaleOutStart(current, { current.members.back().identity }, plan);
        }
        const auto firstState = type == TopologyChangeType::SCALE_IN ? MemberState::PRE_LEAVING : MemberState::ACTIVE;
        current.members = { Member{ { std::string(16, 'a'), "127.0.0.1:1" }, firstState, { 1, 50 } },
                            Member{ { std::string(16, 'b'), "127.0.0.1:2" }, MemberState::ACTIVE, { 100, 150 } } };
        if (type == TopologyChangeType::SCALE_IN) {
            return builder.BuildScaleInStart(current, { current.members.front().identity }, plan);
        }
        return builder.BuildFailureStartOrReplan(current, { current.members.front().identity }, plan);
    }

    Status SetUp(TopologyChangeType type = TopologyChangeType::SCALE_OUT)
    {
        RETURN_IF_NOT_OK(TopologyKeyHelper::Create("executor", keys));
        repository = std::make_unique<TopologyRepository>(backend, *keys);
        RETURN_IF_NOT_OK(BuildPlan(type));
        backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), plan.next);
        TopologyReader reader(*repository);
        RETURN_IF_NOT_OK(reader.Read(TEST_AUTHORITY_READ_TIMEOUT_MS, snapshot));
        SnapshotUpdateOutcome outcome;
        RETURN_IF_NOT_OK(snapshots.Publish(snapshot, outcome));
        TopologyTaskMaterializer materializer;
        RETURN_IF_NOT_OK(materializer.BuildExpected(*snapshot, plan, expected));
        CHECK_FAIL_RETURN_STATUS(!expected.tasks.empty(), K_RUNTIME_ERROR, "expected ScaleOut tasks are empty");
        for (const auto &task : expected.tasks) {
            RETURN_IF_NOT_OK(repository->CreateTaskIfAbsent(task));
        }
        for (const auto &[address, notify] : expected.notifiesByAddress) {
            RETURN_IF_NOT_OK(repository->RewriteNotify(address, notify));
        }
        return dispatcher.Start();
    }

    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    std::unique_ptr<TopologyRepository> repository;
    HashAlgorithm algorithm;
    TopologyState current;
    TopologyPlan plan;
    std::shared_ptr<const TopologySnapshot> snapshot;
    TopologySnapshotState snapshots;
    ExpectedDerivedState expected;
    CoordinationEventDispatcher dispatcher;
    RecordingCallbacks callbacks;
};

TopologyCallbackCompletion WaitCompletion(CoordinationEventDispatcher &dispatcher)
{
    RuntimeEvent event;
    auto rc = dispatcher.WaitPop(std::chrono::steady_clock::now() + TEST_WAIT, event);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    if (rc.IsError()) {
        return { {}, "", {}, rc, nullptr };
    }
    return std::move(std::get<TopologyCallbackCompletion>(event.payload));
}

bool WaitForExecutorFailure(const TopologyTaskExecutor &executor, std::chrono::steady_clock::time_point deadline)
{
    while (std::chrono::steady_clock::now() < deadline) {
        if (executor.GetDiagnostics().failed > 0) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool WaitForFinalTopology(TopologyRepository &repository, std::chrono::steady_clock::time_point deadline)
{
    while (std::chrono::steady_clock::now() < deadline) {
        TopologyState state;
        int64_t revision = 0;
        if (repository.ReadTopology(100, state, revision).IsOk() && !state.activeBatch.has_value()) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

TEST(TopologyTaskExecutorTest, ExecutesOneTaskCallbackAndCommitsWholeScopeProgress)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    auto completion = WaitCompletion(scenario.dispatcher);
    DS_ASSERT_OK(executor.HandleCompletion(std::move(completion)));
    TopologyTask observed;
    DS_ASSERT_OK(
        scenario.repository->ReadTask(TopologyTaskKind::MIGRATE, task.taskId, task.type, task.epoch, observed));
    EXPECT_TRUE(std::all_of(std::get<TopologyMigrateTask>(observed).sourceRanges.begin(),
                            std::get<TopologyMigrateTask>(observed).sourceRanges.end(),
                            [](const auto &range) { return range.finished; }));
    EXPECT_EQ(scenario.callbacks.scaleOutCalls, 1);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, RejectsLateCompletionAfterEpochChanges)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    auto completion = WaitCompletion(scenario.dispatcher);
    TopologyPlanBuilder builder(scenario.algorithm);
    TopologyState final;
    DS_ASSERT_OK(builder.BuildScaleOutFinal(scenario.plan.next, final));
    std::shared_ptr<const TopologySnapshot> newer;
    DS_ASSERT_OK(TopologySnapshot::Create(final, 2, std::string(64, 'b'), newer));
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(scenario.snapshots.Publish(newer, outcome));
    scenario.backend.PutRaw(scenario.keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), final);
    EXPECT_EQ(executor.HandleCompletion(std::move(completion)).GetCode(), K_INVALID);
    EXPECT_EQ(executor.GetDiagnostics().stale, 1);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, ExactAuthorityRejectsEffectWhenCachedSnapshotMissesFinalCas)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    TopologyPlanBuilder builder(scenario.algorithm);
    TopologyState final;
    DS_ASSERT_OK(builder.BuildScaleOutFinal(scenario.plan.next, final));
    scenario.backend.PutRaw(scenario.keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), final);
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    auto completion = WaitCompletion(scenario.dispatcher);
    EXPECT_EQ(completion.status.GetCode(), K_INVALID);
    EXPECT_EQ(scenario.callbacks.scaleOutCalls.load(), 0);
    EXPECT_EQ(executor.HandleCompletion(std::move(completion)).GetCode(), K_INVALID);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, RetriesOnlyRetryableOrdinaryCallbackWithinAttemptBudget)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    scenario.callbacks.scaleOutStatus = Status(K_TRY_AGAIN, "retry");
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutorOptions options;
    options.backoffInitial = std::chrono::milliseconds(1);
    options.backoffMaximum = std::chrono::seconds(1);
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, options);
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    scenario.callbacks.scaleOutStatus = Status::OK();
    DS_ASSERT_OK(executor.HandleTick(std::chrono::steady_clock::now() + RETRY_TICK_ADVANCE));
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    EXPECT_EQ(scenario.callbacks.scaleOutCalls.load(), 2);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, NotifyFailureDoesNotBlockLaterTaskAdmission)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    auto notify = scenario.expected.notifiesByAddress.at(task.executorAddress);
    auto missingTaskId = task.taskId;
    missingTaskId.back() = missingTaskId.back() == '0' ? '1' : '0';
    notify.taskIds.insert(notify.taskIds.begin(), std::move(missingTaskId));
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    EXPECT_EQ(executor.HandleNotify(notify).GetCode(), K_NOT_FOUND);
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    EXPECT_EQ(scenario.callbacks.scaleOutCalls.load(), 1);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, TransientFenceRebuildFailurePreservesRetryLedger)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    scenario.callbacks.scaleOutStatus = Status(K_TRY_AGAIN, "retry");
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutorOptions options;
    options.backoffInitial = std::chrono::milliseconds(1);
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, options);
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    scenario.snapshots.Clear();
    EXPECT_EQ(executor.HandleTick(std::chrono::steady_clock::now() + RETRY_TICK_ADVANCE).GetCode(), K_NOT_READY);
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(scenario.snapshots.Publish(scenario.snapshot, outcome));
    scenario.callbacks.scaleOutStatus = Status::OK();
    DS_ASSERT_OK(executor.HandleTick(std::chrono::steady_clock::now() + RETRY_TICK_ADVANCE));
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    EXPECT_EQ(scenario.callbacks.scaleOutCalls.load(), 2);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, FailureErrorIsNotRetriedAndStillAdvancesProgress)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(TopologyChangeType::FAILURE));
    scenario.callbacks.failureStatus = Status(K_TRY_AGAIN, "best effort failed");
    const auto &task = std::get<TopologyDeleteTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    TopologyTask observed;
    DS_ASSERT_OK(scenario.repository->ReadTask(TopologyTaskKind::DELETE_MEMBER, task.taskId,
                                               TopologyChangeType::FAILURE, task.epoch, observed));
    EXPECT_TRUE(std::all_of(std::get<TopologyDeleteTask>(observed).recoveryRanges.begin(),
                            std::get<TopologyDeleteTask>(observed).recoveryRanges.end(),
                            [](const auto &range) { return range.finished; }));
    DS_ASSERT_OK(executor.HandleTick(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(scenario.callbacks.failureCalls.load(), 1);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, FailureCompletionOverflowRetriesOnlyProgress)
{
    ExecutorScenario scenario(1);
    DS_ASSERT_OK(scenario.SetUp(TopologyChangeType::FAILURE));
    scenario.callbacks.failureStatus = Status(K_TRY_AGAIN, "best effort failed");
    const auto &task = std::get<TopologyDeleteTask>(scenario.expected.tasks.front());
    DS_ASSERT_OK(scenario.dispatcher.SubmitCoordination(
        CoordinationEvent{ CoordinationEventType::PUT, "occupied", "value", 1 }));
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    ASSERT_TRUE(WaitForExecutorFailure(executor, std::chrono::steady_clock::now() + TEST_WAIT));
    RuntimeEvent occupied;
    DS_ASSERT_OK(scenario.dispatcher.WaitPop(std::chrono::steady_clock::now() + TEST_WAIT, occupied));
    DS_ASSERT_OK(executor.HandleTick(std::chrono::steady_clock::now() + TEST_WAIT));
    TopologyTask observed;
    DS_ASSERT_OK(scenario.repository->ReadTask(TopologyTaskKind::DELETE_MEMBER, task.taskId,
                                               TopologyChangeType::FAILURE, task.epoch, observed));
    EXPECT_TRUE(std::all_of(std::get<TopologyDeleteTask>(observed).recoveryRanges.begin(),
                            std::get<TopologyDeleteTask>(observed).recoveryRanges.end(),
                            [](const auto &range) { return range.finished; }));
    EXPECT_EQ(scenario.callbacks.failureCalls.load(), 1);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, StaleScaleInCompletionCannotCommitPreparedCleanup)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(TopologyChangeType::SCALE_IN));
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    auto completion = WaitCompletion(scenario.dispatcher);
    TopologyState changed = scenario.plan.next;
    ++changed.version;
    changed.activeBatch->epoch = changed.version;
    std::shared_ptr<const TopologySnapshot> newer;
    DS_ASSERT_OK(TopologySnapshot::Create(changed, 2, std::string(64, 'b'), newer));
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(scenario.snapshots.Publish(newer, outcome));
    scenario.backend.PutRaw(scenario.keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), changed);
    EXPECT_EQ(executor.HandleCompletion(std::move(completion)).GetCode(), K_INVALID);
    EXPECT_EQ(scenario.callbacks.cleanupAuthorizations.load(), 0);
    EXPECT_EQ(scenario.callbacks.cleanupEffects.load(), 0);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, RetriesOnlyProgressAfterScaleInProgressCasFailure)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(TopologyChangeType::SCALE_IN));
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    const auto &notify = scenario.expected.notifiesByAddress.at(task.executorAddress);
    DS_ASSERT_OK(executor.HandleNotify(notify));
    scenario.backend.FailNextCasBeforeCommit();
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    auto effectCompletion = WaitCompletion(scenario.dispatcher);
    EXPECT_NE(scenario.callbacks.cleanupEffectThread, std::this_thread::get_id());
    EXPECT_EQ(executor.HandleCompletion(std::move(effectCompletion)).GetCode(), K_RPC_UNAVAILABLE);
    DS_ASSERT_OK(executor.HandleNotify(notify));
    DS_ASSERT_OK(executor.HandleTick(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(scenario.callbacks.scaleInCalls.load(), 1);
    EXPECT_EQ(scenario.callbacks.cleanupAuthorizations.load(), 1);
    EXPECT_EQ(scenario.callbacks.cleanupEffects.load(), 1);
    TopologyTask observed;
    DS_ASSERT_OK(
        scenario.repository->ReadTask(TopologyTaskKind::MIGRATE, task.taskId, task.type, task.epoch, observed));
    EXPECT_TRUE(std::all_of(std::get<TopologyMigrateTask>(observed).sourceRanges.begin(),
                            std::get<TopologyMigrateTask>(observed).sourceRanges.end(),
                            [](const auto &range) { return range.finished; }));
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, ConvertsPreparedCleanupExceptionsToTerminalStatus)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(TopologyChangeType::SCALE_IN));
    scenario.callbacks.throwCleanupEffect = true;
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));

    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    EXPECT_EQ(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(scenario.callbacks.cleanupAuthorizations.load(), 1);
    EXPECT_EQ(scenario.callbacks.cleanupEffects.load(), 0);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, ConvertsCleanupAuthorizationExceptionToTerminalStatus)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp(TopologyChangeType::SCALE_IN));
    scenario.callbacks.throwCleanupAuthorization = true;
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));

    EXPECT_EQ(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(scenario.callbacks.cleanupAuthorizations.load(), 0);
    EXPECT_EQ(scenario.callbacks.cleanupEffects.load(), 0);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, StopCooperativelyCancelsAndDrainsAcceptedCallback)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    scenario.callbacks.blockScaleOutUntilCancelled = true;
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    ASSERT_TRUE(scenario.callbacks.WaitUntilScaleOutEntered(std::chrono::steady_clock::now() + TEST_WAIT));
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_FALSE(executor.GetDiagnostics().running);
}

TEST(TopologyTaskExecutorTest, StopJoinsNonCooperativeCallbackAfterReportingDrainTimeout)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    scenario.callbacks.blockScaleOutUntilReleased = true;
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutorOptions options;
    options.ordinaryDrain = std::chrono::seconds(1);
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, options);
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    ASSERT_TRUE(scenario.callbacks.WaitUntilScaleOutEntered(std::chrono::steady_clock::now() + TEST_WAIT));

    auto stop = std::async(std::launch::async, [&executor] {
        return executor.Stop(std::chrono::steady_clock::now() + std::chrono::milliseconds(10));
    });
    EXPECT_EQ(stop.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    scenario.callbacks.ReleaseScaleOut();
    EXPECT_EQ(stop.get().GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_FALSE(executor.GetDiagnostics().running);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, StartedExecutorIsSafelyStoppedByDestructor)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    {
        TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots,
                                      scenario.callbacks, scenario.dispatcher, {});
        DS_ASSERT_OK(executor.Start());
    }
}

TEST(TopologyTaskExecutorTest, RejectsOptionsThatExceedCallbackThreadLimit)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutorOptions options;
    options.callbackThreads = MAX_CALLBACK_THREADS + 1;
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, options);
    EXPECT_EQ(executor.Start().GetCode(), K_INVALID);
}

TEST(TopologyTaskExecutorTest, RejectsOptionsThatExceedCallbackQueueLimit)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutorOptions options;
    options.callbackQueueCapacity = MAX_CALLBACK_QUEUE_CAPACITY + 1;
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, options);
    EXPECT_EQ(executor.Start().GetCode(), K_INVALID);
}

TEST(TopologyTaskExecutorTest, DuplicateNotifyAfterProgressIsIdempotent)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    const auto &notify = scenario.expected.notifiesByAddress.at(task.executorAddress);
    DS_ASSERT_OK(executor.HandleNotify(notify));
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    DS_ASSERT_OK(executor.HandleNotify(notify));
    EXPECT_EQ(scenario.callbacks.scaleOutCalls.load(), 1);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, IgnoresNotifyAfterBatchFinalizes)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyPlanBuilder builder(scenario.algorithm);
    TopologyState final;
    DS_ASSERT_OK(builder.BuildScaleOutFinal(scenario.plan.next, final));
    std::shared_ptr<const TopologySnapshot> finalized;
    DS_ASSERT_OK(TopologySnapshot::Create(final, 2, std::string(64, 'b'), finalized));
    SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(scenario.snapshots.Publish(finalized, outcome));

    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    EXPECT_EQ(scenario.callbacks.scaleOutCalls.load(), 0);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, RejectsCompletionWhoseOperationIdDoesNotMatchFence)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    auto completion = WaitCompletion(scenario.dispatcher);
    completion.businessOperationId = "invalid-operation";
    EXPECT_EQ(executor.HandleCompletion(std::move(completion)).GetCode(), K_INVALID);
    EXPECT_EQ(executor.GetDiagnostics().stale, 1);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, ConvertsCallbackExceptionToTerminalStatus)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    scenario.callbacks.throwScaleOut = true;
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    EXPECT_EQ(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)).GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(executor.GetDiagnostics().failed, 1);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, ReschedulesSuccessfulCallbackWhenCompletionQueueIsFull)
{
    ExecutorScenario scenario(1);
    DS_ASSERT_OK(scenario.SetUp());
    DS_ASSERT_OK(
        scenario.dispatcher.SubmitCoordination(CoordinationEvent{ CoordinationEventType::PUT, "doorbell", "", 1, 1 }));
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    ASSERT_TRUE(WaitForExecutorFailure(executor, std::chrono::steady_clock::now() + TEST_WAIT));
    RuntimeEvent doorbell;
    DS_ASSERT_OK(scenario.dispatcher.WaitPop(std::chrono::steady_clock::now() + TEST_WAIT, doorbell));
    DS_ASSERT_OK(executor.HandleTick(std::chrono::steady_clock::now() + RETRY_TICK_ADVANCE));
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    EXPECT_EQ(scenario.callbacks.scaleOutCalls.load(), 2);
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyTaskExecutorTest, CompletedTaskLetsControllerFinalizeCommittedBatch)
{
    ExecutorScenario scenario;
    DS_ASSERT_OK(scenario.SetUp());
    const auto &task = std::get<TopologyMigrateTask>(scenario.expected.tasks.front());
    TopologyTaskExecutor executor(task.executorAddress, *scenario.repository, scenario.snapshots, scenario.callbacks,
                                  scenario.dispatcher, {});
    DS_ASSERT_OK(executor.Start());
    DS_ASSERT_OK(executor.HandleNotify(scenario.expected.notifiesByAddress.at(task.executorAddress)));
    DS_ASSERT_OK(executor.HandleCompletion(WaitCompletion(scenario.dispatcher)));
    DS_ASSERT_OK(executor.Stop(std::chrono::steady_clock::now() + TEST_WAIT));

    CoordinationEventDispatcher controllerDispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    TopologyController controller(scenario.backend, *scenario.repository, *scenario.keys, scenario.algorithm,
                                  controllerDispatcher, options);
    DS_ASSERT_OK(controller.Start());
    EXPECT_TRUE(WaitForFinalTopology(*scenario.repository, std::chrono::steady_clock::now() + TEST_WAIT));
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

}  // namespace
}  // namespace datasystem::cluster
