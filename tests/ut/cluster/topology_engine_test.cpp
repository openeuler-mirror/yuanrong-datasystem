/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker-role cluster topology Engine composition tests.
 */
#include "datasystem/cluster/runtime/topology_engine.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "gtest/gtest.h"
#include "ut/cluster/testing/fake_coordinator_service_proxy.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr char LOCAL_ADDRESS[] = "127.0.0.1:10001";
constexpr char LOCAL_ID[] = "aaaaaaaaaaaaaaaa";
constexpr int BUILD_ENGINE_FAILED_EXIT = 1;
constexpr int START_FAILED_EXIT = 2;
constexpr int START_RETURNED_EXIT = 3;
constexpr int EMIT_EVENT_FAILED_EXIT = 4;
constexpr auto TEST_WAIT = std::chrono::seconds(3);

class NoopTopologyCallbacks final : public ITopologyPhaseCallbacks {
public:
    NoopTopologyCallbacks() = default;
    ~NoopTopologyCallbacks() override = default;

    Status OnScaleOut(const TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status OnScaleIn(const TopologyCallbackContext &) override
    {
        return Status::OK();
    }

    Status OnScaleInDataDrain(const TopologyCallbackContext &) override
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
};

class TestWatchIngress final {
public:
    TestWatchIngress() = default;
    ~TestWatchIngress() = default;

    CoordinatorWatchIngress Contract()
    {
        CoordinatorWatchIngress ingress;
        ingress.bind = [this](CoordinatorWatchIngress::Handler handler) { return Bind(std::move(handler)); };
        ingress.unbindAndDrain = [this](std::chrono::steady_clock::time_point) { return Unbind(); };
        return ingress;
    }

    void FailNextUnbind()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        failNextUnbind_ = true;
    }

    void BlockNextBind()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        blockNextBind_ = true;
    }

    bool WaitUntilBindBlocked(std::chrono::steady_clock::time_point deadline)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_until(lock, deadline, [this] { return bindBlocked_; });
    }

    void ReleaseBind()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        releaseBind_ = true;
        cv_.notify_all();
    }

    Status Emit(const std::string &coordinatorId, int64_t watchId, CoordinationEvent event)
    {
        CoordinatorWatchIngress::Handler handler;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handler = handler_;
        }
        CHECK_FAIL_RETURN_STATUS(handler != nullptr, K_NOT_READY, "test watch ingress is not bound");
        return handler(coordinatorId, watchId, std::move(event));
    }

    bool IsBound() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return handler_ != nullptr;
    }

private:
    Status Bind(CoordinatorWatchIngress::Handler handler)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        CHECK_FAIL_RETURN_STATUS(handler_ == nullptr && handler != nullptr, K_INVALID,
                                 "test watch ingress is already bound");
        if (blockNextBind_) {
            blockNextBind_ = false;
            bindBlocked_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return releaseBind_; });
            bindBlocked_ = false;
            releaseBind_ = false;
        }
        handler_ = std::move(handler);
        return Status::OK();
    }

    Status Unbind()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (failNextUnbind_) {
            failNextUnbind_ = false;
            RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "injected watch ingress drain timeout");
        }
        handler_ = nullptr;
        return Status::OK();
    }

    // Protects handler_, failure injection, and bind synchronization state.
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    CoordinatorWatchIngress::Handler handler_;
    bool failNextUnbind_{ false };
    bool blockNextBind_{ false };
    bool bindBlocked_{ false };
    bool releaseBind_{ false };
};

TopologyState MakeTopology(uint64_t version = 1)
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    state.members = { Member{ { LOCAL_ID, LOCAL_ADDRESS }, MemberState::ACTIVE, { 0, 1'000'000'000 } } };
    return state;
}

TopologyState MakeTopologyWithPeer(uint64_t version = 1)
{
    auto state = MakeTopology(version);
    state.members.emplace_back(
        Member{ { std::string(16, 'b'), "127.0.0.1:10002" }, MemberState::ACTIVE, { 500'000'000 } });
    return state;
}

TopologyState MakeTopologyWithoutLocal(uint64_t version = 1)
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    state.members = {
        Member{ { std::string(16, 'b'), "127.0.0.1:10002" }, MemberState::ACTIVE, { 0, 1'000'000'000 } }
    };
    return state;
}

template <typename Predicate>
bool WaitFor(Predicate predicate, std::chrono::steady_clock::duration timeout = TEST_WAIT)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

std::unique_ptr<TopologyKeyHelper> MakeKeys(const std::string &clusterName)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    EXPECT_TRUE(TopologyKeyHelper::Create(clusterName, keys).IsOk());
    return keys;
}

std::string TopologyStorageKey(const TopologyKeyHelper &keys)
{
    return keys.TopologyTable() + "/" + TopologyKeyHelper::TopologyKey();
}

int64_t FindWatchId(const testing::FakeCoordinatorServiceProxy &proxy, const std::string &key)
{
    const auto watches = proxy.WatchCalls();
    auto found = std::find_if(watches.begin(), watches.end(), [&key](const auto &watch) { return watch.key == key; });
    EXPECT_NE(found, watches.end());
    return found == watches.end() ? 0 : found->watchId;
}

Status EmitTopologyEvent(testing::FakeCoordinatorServiceProxy &proxy, TestWatchIngress &ingress,
                         const TopologyKeyHelper &keys, uint64_t version)
{
    const auto key = TopologyStorageKey(keys);
    return ingress.Emit("coordinator-test", FindWatchId(proxy, key),
                        { CoordinationEventType::PUT, key, "", static_cast<int64_t>(version),
                          static_cast<int64_t>(version) });
}

void PutTopology(testing::FakeCoordinatorServiceProxy &proxy, const std::string &clusterName,
                 const TopologyState &state)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create(clusterName, keys));
    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(state, encoded));
    DS_ASSERT_OK(proxy.PutRaw(keys->TopologyTable() + "/" + TopologyKeyHelper::TopologyKey(), encoded));
}

void ConfigureBuilder(TopologyEngine::Builder &builder, testing::FakeCoordinatorServiceProxy &proxy,
                      TestWatchIngress &ingress, NoopTopologyCallbacks &callbacks,
                      const std::string &clusterName)
{
    builder.SetClusterName(clusterName)
        .SetLocalAddress(LOCAL_ADDRESS)
        .UseCoordinator(proxy, ingress.Contract())
        .SetPhaseCallbacks(callbacks)
        .SetNodeDeadTimeout(std::chrono::seconds(30));
}

std::unique_ptr<TopologyEngine> BuildEngine(testing::FakeCoordinatorServiceProxy &proxy,
                                            TestWatchIngress &ingress, NoopTopologyCallbacks &callbacks,
                                            const std::string &clusterName)
{
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, clusterName);
    std::unique_ptr<TopologyEngine> engine;
    EXPECT_TRUE(builder.Build(engine).IsOk());
    return engine;
}

TEST(TopologyEngineTest, BuilderRejectsIncompleteAndConflictingConfiguration)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    auto existing = BuildEngine(proxy, ingress, callbacks, "existing");
    auto *const existingAddress = existing.get();
    std::unique_ptr<TopologyEngine> output = std::move(existing);
    TopologyEngine::Builder missing;
    EXPECT_EQ(missing.Build(output).GetCode(), K_INVALID);
    EXPECT_EQ(output.get(), existingAddress);

    TopologyEngine::Builder conflict;
    ConfigureBuilder(conflict, proxy, ingress, callbacks, "conflict");
    conflict.UseCoordinator(proxy, ingress.Contract());
    EXPECT_EQ(conflict.Build(output).GetCode(), K_INVALID);
    EXPECT_EQ(output.get(), existingAddress);
}

TEST(TopologyEngineTest, BuilderAcceptsEmptyClusterAndDerivesMissingTopologyAsFreshStart)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    auto engine = BuildEngine(proxy, ingress, callbacks, "");

    ASSERT_NE(engine, nullptr);
    EXPECT_FALSE(engine->IsRestart());
    EXPECT_FALSE(ingress.IsBound());
    EXPECT_TRUE(proxy.WatchCalls().empty());
    std::shared_ptr<const TopologySnapshot> snapshot;
    EXPECT_EQ(engine->GetSnapshot(snapshot).GetCode(), K_NOT_READY);
}

TEST(TopologyEngineTest, BuilderAllowsRecoveringCoordinatorWithoutGuessingRestart)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    auto keys = MakeKeys("recovering");
    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_NOT_READY);
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "recovering");
    std::unique_ptr<TopologyEngine> engine;

    DS_ASSERT_OK(builder.Build(engine));
    ASSERT_NE(engine, nullptr);
    EXPECT_FALSE(engine->IsRestart());
    EXPECT_FALSE(ingress.IsBound());
    EXPECT_TRUE(proxy.WatchCalls().empty());
}

TEST(TopologyEngineTest, BuilderExactReadSetsRestartWithoutStartingRuntimeSideEffects)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "restart", MakeTopology());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "restart");
    std::unique_ptr<TopologyEngine> engine;

    DS_ASSERT_OK(builder.Build(engine));
    EXPECT_TRUE(engine->IsRestart());
    EXPECT_FALSE(ingress.IsBound());
    EXPECT_TRUE(proxy.WatchCalls().empty());
    builder.SetClusterName("ignored-after-consumption");
    EXPECT_EQ(builder.Build(engine).GetCode(), K_INVALID);
}

TEST(TopologyEngineTest, StartPublishesCapabilitiesAndShutdownDrainsOwnedRoles)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "lifecycle", MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, "lifecycle");

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    EXPECT_TRUE(ingress.IsBound());
    EXPECT_TRUE(engine->HasEstablishedMemberLease());
    EXPECT_GE(proxy.WatchCalls().size(), 2U);
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(engine->GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), 1U);
    PlacementDecision placement;
    DS_ASSERT_OK(engine->Placement().Locate("key", placement));
    EXPECT_EQ(placement.committedOwnerAddress, LOCAL_ADDRESS);
    DS_ASSERT_OK(engine->MarkReady());
    DS_ASSERT_OK(engine->MarkExiting());
    DS_ASSERT_OK(engine->NotifyReconciliationDone());

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(ingress.IsBound());
    EXPECT_GT(proxy.CancelledWatchCount(), 0U);
}

TEST(TopologyEngineTest, InitialSnapshotWithoutLocalMemberRemainsNotReady)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "missing-local", MakeTopologyWithoutLocal());
    auto engine = BuildEngine(proxy, ingress, callbacks, "missing-local");

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NOT_READY);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineDeathTest, LocalMemberRemovedFromSnapshotTriggersSigkill)
{
    ASSERT_EXIT(
        {
            testing::FakeCoordinatorServiceProxy proxy;
            TestWatchIngress ingress;
            NoopTopologyCallbacks callbacks;
            const std::string clusterName = "removed-local";
            auto keys = MakeKeys(clusterName);
            PutTopology(proxy, clusterName, MakeTopology());
            auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
            if (engine == nullptr) {
                std::exit(BUILD_ENGINE_FAILED_EXIT);
            }
            if (engine->Start().IsError()) {
                std::exit(START_FAILED_EXIT);
            }
            PutTopology(proxy, clusterName, MakeTopologyWithoutLocal(2));
            if (EmitTopologyEvent(proxy, ingress, *keys, 2).IsError()) {
                std::exit(EMIT_EVENT_FAILED_EXIT);
            }
            std::this_thread::sleep_for(TEST_WAIT);
            std::exit(START_RETURNED_EXIT);
        },
        ::testing::KilledBySignal(SIGKILL), "");
}

TEST(TopologyEngineTest, SnapshotPublicationCallbackRunsOnlyAfterStartPublishes)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "publish", MakeTopology());
    std::atomic<size_t> published{ 0 };
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "publish");
    builder.SetSnapshotPublishedHandler([&published](std::shared_ptr<const TopologySnapshot> snapshot) {
        if (snapshot != nullptr) {
            published.fetch_add(1);
        }
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    EXPECT_EQ(published.load(), 0U);

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(published.load(), 1U);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, DrainTimeoutRetainsDependenciesAndShutdownCanRetry)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "retry-shutdown", MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, "retry-shutdown");
    DS_ASSERT_OK(engine->Start());
    ingress.FailNextUnbind();

    EXPECT_EQ(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT).GetCode(),
              K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPING);
    EXPECT_TRUE(ingress.IsBound());
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(ingress.IsBound());
}

TEST(TopologyEngineTest, BuilderRejectsInvalidAddressAndTimeout)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    TopologyEngine::Builder invalidAddress;
    ConfigureBuilder(invalidAddress, proxy, ingress, callbacks, "invalid-address");
    invalidAddress.SetLocalAddress("not-an-address");
    std::unique_ptr<TopologyEngine> output;
    EXPECT_EQ(invalidAddress.Build(output).GetCode(), K_INVALID);

    TopologyEngine::Builder invalidTimeout;
    ConfigureBuilder(invalidTimeout, proxy, ingress, callbacks, "invalid-timeout");
    invalidTimeout.SetNodeDeadTimeout(std::chrono::seconds(0));
    EXPECT_EQ(invalidTimeout.Build(output).GetCode(), K_INVALID);
}

TEST(TopologyEngineTest, RecoveryReporterExportsCanonicalRuntimeSnapshot)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto expected = MakeTopology(7);
    PutTopology(proxy, "export", expected);
    auto engine = BuildEngine(proxy, ingress, callbacks, "export");
    proxy.RequireRecoveryPayload();
    EXPECT_EQ(proxy.RecoveryRequestCount(), 0U);
    DS_ASSERT_OK(engine->Start());
    ASSERT_TRUE(WaitFor([&] { return proxy.RecoveryRequestCount() == 2; }));
    const auto evidence = proxy.RecoveryRequestAt(0);
    const auto payload = proxy.RecoveryRequestAt(1);
    EXPECT_EQ(evidence.topology_version(), expected.version);
    EXPECT_TRUE(evidence.canonical_topology().empty());
    EXPECT_EQ(payload.topology_digest(), evidence.topology_digest());
    TopologyState decoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::DecodeTopology(payload.canonical_topology(), decoded));
    EXPECT_EQ(decoded.version, expected.version);
    EXPECT_EQ(decoded.members.front().identity.address, LOCAL_ADDRESS);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, WatchDoorbellExactReadRepairsMissingPayload)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("repair");
    PutTopology(proxy, "repair", MakeTopology(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, "repair");
    DS_ASSERT_OK(engine->Start());

    PutTopology(proxy, "repair", MakeTopology(2));
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    std::shared_ptr<const TopologySnapshot> snapshot;
    ASSERT_TRUE(WaitFor([&] { return engine->GetSnapshot(snapshot).IsOk() && snapshot->Version() == 2; }));
    EXPECT_EQ(snapshot->Version(), 2U);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, MatchingPeerOutageEvidenceEntersControlDegraded)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("global-outage");
    PutTopology(proxy, "global-outage", MakeTopologyWithPeer());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "global-outage");
    builder.SetControlBackendProbe([](const ControlBackendObservation &local, const auto &peers, auto) {
        auto peer = local;
        peer.reporter = peers.front();
        peer.state = ControlBackendState::UNAVAILABLE;
        peer.observedAt = std::chrono::steady_clock::now();
        return std::vector<ControlBackendObservation>{ peer };
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::CONTROL_DEGRADED; }));
    EXPECT_EQ(engine->GetControlBackendObservation().state, ControlBackendState::UNAVAILABLE);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, AsymmetricBackendOutageIsolatesThenRecovers)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("asymmetric");
    PutTopology(proxy, "asymmetric", MakeTopologyWithPeer());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "asymmetric");
    builder.SetControlBackendProbe([](const ControlBackendObservation &local, const auto &peers, auto) {
        auto peer = local;
        peer.reporter = peers.front();
        peer.state = ControlBackendState::AVAILABLE;
        peer.observedAt = std::chrono::steady_clock::now();
        return std::vector<ControlBackendObservation>{ peer };
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::ROLE_ISOLATED; }));
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 3));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::NORMAL; }));
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, MissingPeerQuorumIsolatesBackendOutage)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("missing-quorum");
    PutTopology(proxy, "missing-quorum", MakeTopologyWithPeer());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "missing-quorum");
    builder.SetControlBackendProbe([](const auto &, const auto &, auto) {
        return std::vector<ControlBackendObservation>{};
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::ROLE_ISOLATED; }));
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, CoordinatorWatchEventFlowsThroughBoundedDispatcher)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("event-queue");
    PutTopology(proxy, "event-queue", MakeTopology(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, "event-queue");
    DS_ASSERT_OK(engine->Start());
    const auto submittedBefore = engine->GetDiagnostics().dispatcher.submitted;

    PutTopology(proxy, "event-queue", MakeTopology(2));
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    std::shared_ptr<const TopologySnapshot> snapshot;
    ASSERT_TRUE(WaitFor([&] { return engine->GetSnapshot(snapshot).IsOk() && snapshot->Version() == 2; }));
    EXPECT_GT(engine->GetDiagnostics().dispatcher.submitted, submittedBefore);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, ControllerStartFailureNeverPublishesHostAdmission)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("start-admission");
    PutTopology(proxy, "start-admission", MakeTopology());
    std::atomic<size_t> normalAdmissions{ 0 };
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "start-admission");
    builder.SetAvailabilityHandler([&](TopologyAvailabilityLevel level) {
        if (level == TopologyAvailabilityLevel::NORMAL) {
            normalAdmissions.fetch_add(1);
        }
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    proxy.FailNextWatchForKey(keys->MembershipTable() + "/", K_RPC_UNAVAILABLE);

    EXPECT_EQ(engine->Start().GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(normalAdmissions.load(), 0U);
}

TEST(TopologyEngineTest, StartRollbackCleanupFailureRemainsRetryable)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("start-rollback");
    PutTopology(proxy, "start-rollback", MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, "start-rollback");
    proxy.FailNextWatchForKey(keys->MembershipTable() + "/", K_RPC_UNAVAILABLE);
    ingress.FailNextUnbind();

    EXPECT_EQ(engine->Start().GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPING);
    EXPECT_TRUE(ingress.IsBound());
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(ingress.IsBound());
}

TEST(TopologyEngineTest, ShutdownRejectsConcurrentStartWithoutCorruptingLifecycle)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "concurrent-start", MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, "concurrent-start");
    ingress.BlockNextBind();
    auto start = std::async(std::launch::async, [&] { return engine->Start(); });
    const bool bindBlocked = ingress.WaitUntilBindBlocked(std::chrono::steady_clock::now() + TEST_WAIT);
    if (!bindBlocked) {
        ingress.ReleaseBind();
    }
    ASSERT_TRUE(bindBlocked);

    EXPECT_EQ(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT).GetCode(), K_TRY_AGAIN);
    ingress.ReleaseBind();
    DS_ASSERT_OK(start.get());
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

}  // namespace
}  // namespace datasystem::cluster
