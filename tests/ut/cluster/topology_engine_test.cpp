/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker-role cluster topology Engine lifecycle tests.
 */
#include "datasystem/cluster/runtime/topology_engine.h"

#include <algorithm>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>
#include <type_traits>
#include <vector>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_plan_builder.h"
#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {
constexpr auto TEST_WAIT = std::chrono::seconds(1);
constexpr auto RECOVERY_WAIT = std::chrono::seconds(3);
constexpr size_t MAX_WORKER_EVENT_QUEUE_CAPACITY = 1'024;

class NoopTopologyCallbacks final : public ITopologyPhaseCallbacks {
public:
    ~NoopTopologyCallbacks() override = default;
    Status OnScaleOut(const TopologyCallbackContext &) override
    {
        ++scaleOutCalls;
        return Status::OK();
    }
    Status OnScaleIn(const TopologyCallbackContext &) override
    {
        return Status::OK();
    }
    Status PrepareScaleInCleanup(const TopologyCallbackContext &,
                                 std::unique_ptr<TopologyPreparedCleanup> &cleanup) override
    {
        cleanup = std::make_unique<TopologyPreparedCleanup>(
            [] { return Status::OK(); },
            [](std::chrono::steady_clock::time_point, const CancellationToken &) { return Status::OK(); });
        return Status::OK();
    }
    Status OnFailure(const TopologyCallbackContext &) override
    {
        return Status::OK();
    }
    std::atomic<size_t> scaleOutCalls{ 0 };
};

TopologyState MakeEngineTopology(uint64_t version)
{
    TopologyState state;
    state.version = version;
    state.clusterHasInit = true;
    state.members = { Member{ { std::string(16, 'a'), "127.0.0.1:10001" }, MemberState::ACTIVE, { 1, 100 } } };
    return state;
}

TopologyState MakeEngineTopologyWithPeer(uint64_t version)
{
    auto state = MakeEngineTopology(version);
    state.members.emplace_back(Member{ { std::string(16, 'b'), "127.0.0.1:10002" }, MemberState::ACTIVE, { 50, 150 } });
    return state;
}

bool AllTaskScopesFinished(TopologyRepository &repository, const std::vector<TopologyTask> &tasks,
                           TopologyChangeType type, uint64_t epoch)
{
    for (const auto &task : tasks) {
        const auto kind = std::holds_alternative<TopologyMigrateTask>(task) ? TopologyTaskKind::MIGRATE
                                                                            : TopologyTaskKind::DELETE_MEMBER;
        const auto id = std::visit([](const auto &value) { return value.taskId; }, task);
        TopologyTask observed;
        if (repository.ReadTask(kind, id, type, epoch, observed).IsError()) {
            return false;
        }
        const bool finished = std::visit(
            [](const auto &value) {
                const auto &ranges = [&]() -> const std::vector<TopologyTaskRange> & {
                    if constexpr (std::is_same_v<std::decay_t<decltype(value)>, TopologyMigrateTask>) {
                        return value.sourceRanges;
                    } else {
                        return value.recoveryRanges;
                    }
                }();
                return std::all_of(ranges.begin(), ranges.end(), [](const auto &range) { return range.finished; });
            },
            observed);
        if (!finished) {
            return false;
        }
    }
    return true;
}

TEST(TopologyEngineTest, StartsWorkerRolePublishesEvidenceAndStopsWithoutClosingBackend)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("engine", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(1));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options;
    options.clusterName = "engine";
    options.localAddress = "127.0.0.1:10001";
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);
    ASSERT_EQ(backend.WatchKeys().size(), 2);
    EXPECT_TRUE(backend.HasEventHandler());
    auto observation = engine->GetControlBackendObservation();
    EXPECT_EQ(observation.reporter.id, std::string(16, 'a'));
    EXPECT_EQ(observation.reporter.address, options.localAddress);
    EXPECT_EQ(observation.topologyVersion, 1);
    EXPECT_EQ(observation.state, ControlBackendState::AVAILABLE);

    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(backend.HasEventHandler());
    EXPECT_EQ(engine->GetControlBackendObservation().state, ControlBackendState::UNKNOWN);
    std::string topology;
    DS_ASSERT_OK(backend.Get(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology));
}

TEST(TopologyEngineTest, PublishesMembershipBeforeRegisteringTopologyWatches)
{
    FakeCoordinationBackend backend;
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "startup-order", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(backend.LifecycleCalls(), (std::vector<std::string>{ "membership", "watch" }));
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, EnforcesWorkerEventQueueCapacity)
{
    FakeCoordinationBackend backend;
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options;
    options.clusterName = "queue-limit";
    options.localAddress = "127.0.0.1:10001";
    options.eventQueueCapacity = MAX_WORKER_EVENT_QUEUE_CAPACITY;
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    engine.reset();

    options.eventQueueCapacity = MAX_WORKER_EVENT_QUEUE_CAPACITY + 1;
    EXPECT_EQ(TopologyEngine::Create(options, backend, algorithm, callbacks, engine).GetCode(), K_INVALID);
}

TEST(TopologyEngineTest, FailedStartCannotReuseStoppedEventSources)
{
    FakeCoordinationBackend backend;
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "one-shot", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    backend.FailNextWatch();

    EXPECT_EQ(engine->Start().GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(engine->Start().GetCode(), K_INVALID);
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(backend.HasEventHandler());
}

TEST(TopologyEngineTest, MissingInitialTopologyStaysNotReadyUntilControllerBootstrap)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("missing", keys));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "missing", "127.0.0.1:10001" };
    options.scopeProbeInterval = std::chrono::seconds(1);
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NOT_READY);
    EXPECT_TRUE(backend.HasEventHandler());

    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(1));
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 1, 1 });
    const auto deadline = std::chrono::steady_clock::now() + RECOVERY_WAIT;
    while (std::chrono::steady_clock::now() < deadline
           && engine->GetAvailability() != TopologyAvailabilityLevel::NORMAL) {
        std::this_thread::yield();
    }
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, RecoveringBackendDoesNotFailWorkerStartup)
{
    FakeCoordinationBackend backend;
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "recovering", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    backend.ReturnNotReadyOnNextGet();

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NOT_READY);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, ExportsCanonicalLastGoodRecoveryTopology)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("export", keys));
    const auto expected = MakeEngineTopology(7);
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), expected);
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "export", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    uint64_t version = 99;
    std::string canonical = "unchanged";
    EXPECT_EQ(engine->GetRecoveryTopology(version, canonical).GetCode(), K_NOT_FOUND);
    EXPECT_EQ(version, 99);
    EXPECT_EQ(canonical, "unchanged");

    DS_ASSERT_OK(engine->Start());
    DS_ASSERT_OK(engine->GetRecoveryTopology(version, canonical));
    EXPECT_EQ(version, expected.version);
    TopologyState decoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeTopology(canonical, decoded));
    EXPECT_EQ(decoded.version, expected.version);
    EXPECT_EQ(decoded.members.front().identity.address, expected.members.front().identity.address);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, RecoveringExactReadPreservesLastGoodSnapshotAndAvailability)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("last-good", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(7));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "last-good", "127.0.0.1:10001" };
    std::mutex levelsMutex;
    std::vector<TopologyAvailabilityLevel> levels;
    options.availabilityHandler = [&](TopologyAvailabilityLevel level) {
        std::lock_guard<std::mutex> lock(levelsMutex);
        levels.emplace_back(level);
    };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());
    ASSERT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);

    const auto expectedAttempts = backend.GetAttemptCount() + 1;
    backend.ReturnNotReadyOnNextGet();
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 8, 8 });
    ASSERT_TRUE(backend.WaitForGetAttempts(expectedAttempts, std::chrono::steady_clock::now() + TEST_WAIT));

    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(engine->GetSnapshot(snapshot));
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->Version(), 7);
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);

    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(8));
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 8, 9 });
    const auto repairDeadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < repairDeadline) {
        if (engine->GetSnapshot(snapshot).IsOk() && snapshot->Version() == 8) {
            break;
        }
        std::this_thread::yield();
    }
    ASSERT_EQ(snapshot->Version(), 8);
    {
        std::lock_guard<std::mutex> lock(levelsMutex);
        EXPECT_EQ(std::count(levels.begin(), levels.end(), TopologyAvailabilityLevel::NOT_READY), 0);
    }
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, AuthorityRollbackIsolationPersistsForEngineLifetime)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("authority-fence", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(2));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "authority-fence", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());

    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(1));
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 1, 1 });
    const auto deadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < deadline
           && engine->GetAvailability() != TopologyAvailabilityLevel::ROLE_ISOLATED) {
        std::this_thread::yield();
    }
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::ROLE_ISOLATED);

    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(3));
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 3, 3 });
    uint64_t recoveredVersion = 0;
    std::string canonical;
    const auto rebuildDeadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < rebuildDeadline) {
        if (engine->GetRecoveryTopology(recoveredVersion, canonical).IsOk() && recoveredVersion == 3) {
            break;
        }
        std::this_thread::yield();
    }
    EXPECT_EQ(recoveredVersion, 3);
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::ROLE_ISOLATED);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, PeriodicExactReadRepairsSilentWatchLoss)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("periodic", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(1));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "periodic", "127.0.0.1:10001" };
    options.scopeProbeInterval = std::chrono::seconds(1);
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());

    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(2));
    const auto deadline = std::chrono::steady_clock::now() + RECOVERY_WAIT;
    std::shared_ptr<const TopologySnapshot> snapshot;
    while (std::chrono::steady_clock::now() < deadline) {
        if (engine->GetSnapshot(snapshot).IsOk() && snapshot->Version() == 2) {
            break;
        }
        std::this_thread::yield();
    }
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->Version(), 2);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, UsesMatchingPeerEvidenceToDistinguishGlobalBackendOutage)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("scope", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopologyWithPeer(1));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options;
    options.clusterName = "scope";
    options.localAddress = "127.0.0.1:10001";
    options.controlBackendProbe = [](const ControlBackendObservation &local, const auto &peers, auto) {
        EXPECT_EQ(peers.size(), 1U);
        auto peer = local;
        peer.reporter = peers.front();
        peer.state = ControlBackendState::UNAVAILABLE;
        peer.observedAt = std::chrono::steady_clock::now();
        return std::vector<ControlBackendObservation>{ peer };
    };
    std::atomic<TopologyAvailabilityLevel> admittedLevel{ TopologyAvailabilityLevel::NOT_READY };
    options.availabilityHandler = [&admittedLevel](TopologyAvailabilityLevel level) { admittedLevel.store(level); };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(admittedLevel.load(), TopologyAvailabilityLevel::NORMAL);

    backend.FailNextGet();
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 2, 2 });
    const auto deadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < deadline
           && engine->GetAvailability() != TopologyAvailabilityLevel::CONTROL_DEGRADED) {
        std::this_thread::yield();
    }
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::CONTROL_DEGRADED);
    EXPECT_EQ(engine->GetControlBackendObservation().state, ControlBackendState::UNAVAILABLE);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, IsolatesAsymmetricOutageAndRecoversOnPeriodicExactRead)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("asymmetric", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopologyWithPeer(1));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options;
    options.clusterName = "asymmetric";
    options.localAddress = "127.0.0.1:10001";
    options.scopeProbeInterval = std::chrono::seconds(1);
    options.controlBackendProbe = [](const ControlBackendObservation &local, const auto &peers, auto) {
        auto peer = local;
        peer.reporter = peers.front();
        peer.state = ControlBackendState::AVAILABLE;
        peer.observedAt = std::chrono::steady_clock::now();
        return std::vector<ControlBackendObservation>{ peer };
    };
    std::atomic<TopologyAvailabilityLevel> admittedLevel{ TopologyAvailabilityLevel::NOT_READY };
    options.availabilityHandler = [&admittedLevel](TopologyAvailabilityLevel level) { admittedLevel.store(level); };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());
    backend.FailNextGet();
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 2, 2 });
    const auto isolatedDeadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < isolatedDeadline
           && engine->GetAvailability() != TopologyAvailabilityLevel::ROLE_ISOLATED) {
        std::this_thread::yield();
    }
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::ROLE_ISOLATED);
    EXPECT_EQ(admittedLevel.load(), TopologyAvailabilityLevel::ROLE_ISOLATED);
    const auto recoveredDeadline = std::chrono::steady_clock::now() + RECOVERY_WAIT;
    while (std::chrono::steady_clock::now() < recoveredDeadline
           && engine->GetAvailability() != TopologyAvailabilityLevel::NORMAL) {
        std::this_thread::yield();
    }
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);
    EXPECT_EQ(admittedLevel.load(), TopologyAvailabilityLevel::NORMAL);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(admittedLevel.load(), TopologyAvailabilityLevel::SHUTTING_DOWN);
}

TEST(TopologyEngineTest, IsolatesBackendOutageWhenPeerQuorumIsMissing)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("missing-quorum", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopologyWithPeer(1));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options;
    options.clusterName = "missing-quorum";
    options.localAddress = "127.0.0.1:10001";
    options.controlBackendProbe = [](const auto &, const auto &, auto) {
        return std::vector<ControlBackendObservation>{};
    };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());

    backend.FailNextGet();
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 2, 2 });
    const auto deadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < deadline
           && engine->GetAvailability() != TopologyAvailabilityLevel::ROLE_ISOLATED) {
        std::this_thread::yield();
    }
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::ROLE_ISOLATED);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, ResolvesOwnNotifyByTaskIdAndCompletesThroughTheSerialRuntime)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("notify", keys));
    TopologyState current = MakeEngineTopology(1);
    current.members.emplace_back(Member{ { std::string(16, 'b'), "127.0.0.1:10002" }, MemberState::INITIAL, {} });
    HashAlgorithm algorithm;
    TopologyPlan plan;
    TopologyPlanBuilder builder(algorithm);
    DS_ASSERT_OK(builder.BuildScaleOutStart(current, { current.members.back().identity }, plan));
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(TopologySnapshot::Create(plan.next, 1, std::string(64, 'c'), snapshot));
    ExpectedDerivedState expected;
    TopologyTaskMaterializer materializer;
    DS_ASSERT_OK(materializer.BuildExpected(*snapshot, plan, expected));
    TopologyRepository repository(backend, *keys);
    for (const auto &task : expected.tasks) {
        DS_ASSERT_OK(repository.CreateTaskIfAbsent(task));
    }
    for (const auto &[address, notify] : expected.notifiesByAddress) {
        DS_ASSERT_OK(repository.RewriteNotify(address, notify));
    }
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), plan.next);
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "notify", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());
    backend.EmitEvent({ CoordinationEventType::PUT, "notify", "doorbell", 2, 2 });
    const auto deadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < deadline && callbacks.scaleOutCalls.load() == 0) {
        std::this_thread::yield();
    }
    EXPECT_EQ(callbacks.scaleOutCalls.load(), 1);
    while (std::chrono::steady_clock::now() < deadline
           && !AllTaskScopesFinished(repository, expected.tasks, plan.next.activeBatch->type,
                                     plan.next.activeBatch->epoch)) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(
        AllTaskScopesFinished(repository, expected.tasks, plan.next.activeBatch->type, plan.next.activeBatch->epoch));
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, SerializesStopBehindAnInProgressStartTransaction)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("lifecycle", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(1));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "lifecycle", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    backend.BlockNextGet();
    auto start = std::async(std::launch::async, [&engine] { return engine->Start(); });
    ASSERT_TRUE(backend.WaitUntilGetBlocked(std::chrono::steady_clock::now() + TEST_WAIT));
    auto stop = std::async(std::launch::async,
                           [&engine] { return engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT); });
    const bool serialized = stop.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout;
    backend.ReleaseBlockedGet();
    DS_ASSERT_OK(start.get());
    DS_ASSERT_OK(stop.get());
    EXPECT_TRUE(serialized);
    if (engine->GetState() != TopologyEngineState::STOPPED) {
        DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
    }
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
}

TEST(TopologyEngineTest, ShutdownTimeoutStillLeavesEngineSafelyStopped)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("engine-stop", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(1));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "engine-stop", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());

    backend.BlockNextGet();
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 2, 2 });
    ASSERT_TRUE(backend.WaitUntilGetBlocked(std::chrono::steady_clock::now() + TEST_WAIT));
    auto stop = std::async(std::launch::async,
                           [&engine] { return engine->Stop(std::chrono::steady_clock::now()); });
    EXPECT_EQ(stop.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    backend.ReleaseBlockedGet();
    EXPECT_EQ(stop.get().GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);

    std::string topology;
    DS_ASSERT_OK(backend.Get(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology));
}

TEST(TopologyEngineTest, StartedEngineIsSafelyStoppedByDestructor)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("engine-destructor", keys));
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(1));
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "engine-destructor", "127.0.0.1:10001" };
    {
        std::unique_ptr<TopologyEngine> engine;
        DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
        DS_ASSERT_OK(engine->Start());
    }
    EXPECT_FALSE(backend.HasEventHandler());
}

TEST(TopologyEngineTest, RemainsNotReadyUntilSelfAppearsInAuthoritativeTopology)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("admission", keys));
    TopologyState empty;
    empty.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), empty);
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "admission", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NOT_READY);

    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(2));
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 2, 2 });
    const auto deadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < deadline
           && engine->GetAvailability() != TopologyAvailabilityLevel::NORMAL) {
        std::this_thread::yield();
    }
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, DoesNotReportReadyBeforeTheLocalMemberIsCommitted)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("initial", keys));
    TopologyState initial;
    initial.version = 1;
    initial.members = { Member{ { std::string(16, 'a'), "127.0.0.1:10001" }, MemberState::INITIAL, {} } };
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), initial);
    HashAlgorithm algorithm;
    NoopTopologyCallbacks callbacks;
    TopologyEngineOptions options{ "initial", "127.0.0.1:10001" };
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(TopologyEngine::Create(options, backend, algorithm, callbacks, engine));
    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NOT_READY);
    EXPECT_EQ(engine->GetControlBackendObservation().state, ControlBackendState::UNKNOWN);

    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), MakeEngineTopology(2));
    backend.EmitEvent({ CoordinationEventType::PUT, "topology", "doorbell", 2, 2 });
    const auto deadline = std::chrono::steady_clock::now() + TEST_WAIT;
    while (std::chrono::steady_clock::now() < deadline
           && engine->GetAvailability() != TopologyAvailabilityLevel::NORMAL) {
        std::this_thread::yield();
    }
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);
    DS_ASSERT_OK(engine->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

}  // namespace
}  // namespace datasystem::cluster
