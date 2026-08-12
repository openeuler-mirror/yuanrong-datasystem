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
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/protos/coordinator.pb.h"
#include "gtest/gtest.h"
#include "ut/cluster/testing/fake_coordinator_service_proxy.h"
#include "ut/common.h"

namespace datasystem::cluster {

class TopologyEngineTestPeer final {
public:
    static Status RestoreReadyAfterLocalRecovery(TopologyEngine &engine)
    {
        return engine.RestoreReadyAfterLocalRecovery();
    }

    static bool ReadyMembershipPublished(const TopologyEngine &engine)
    {
        return engine.readyMembershipPublished_.load(std::memory_order_acquire);
    }

    static Status OnMembershipEnsured(TopologyEngine &engine, const std::string &coordinatorId,
                                      int64_t membershipModRevision)
    {
        auto *backend = dynamic_cast<DsCoordinationBackend *>(engine.memberBackend_.get());
        CHECK_FAIL_RETURN_STATUS(backend != nullptr, K_RUNTIME_ERROR, "expected Coordinator membership backend");
        return backend->OnMembershipEnsured(coordinatorId, membershipModRevision);
    }
};

namespace {

constexpr char LOCAL_ADDRESS[] = "127.0.0.1:10001";
constexpr char LOCAL_ID[] = "aaaaaaaaaaaaaaaa";
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

TopologyState MakeTopologyWithPeer(uint64_t version = 1, size_t peerCount = 1)
{
    auto state = MakeTopology(version);
    for (size_t i = 0; i < peerCount; ++i) {
        state.members.emplace_back(Member{ { std::string(16, static_cast<char>('b' + i)),
                                             "127.0.0.1:" + std::to_string(10'002 + i) },
                                           MemberState::ACTIVE,
                                           { static_cast<uint32_t>((i + 1) * 1'000'000'000 / (peerCount + 1)) } });
    }
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

void ConfigureBuilder(
    TopologyEngine::Builder &builder, testing::FakeCoordinatorServiceProxy &proxy, TestWatchIngress &ingress,
    NoopTopologyCallbacks &callbacks, const std::string &clusterName,
    std::function<Status(WorkerProbeRequest)> probeHandler = [](WorkerProbeRequest) { return Status::OK(); })
{
    builder.SetClusterName(clusterName)
        .SetLocalAddress(LOCAL_ADDRESS)
        .UseCoordinator(proxy, ingress.Contract())
        .SetPhaseCallbacks(callbacks)
        .SetWorkerProbeHandler(std::move(probeHandler))
        .SetNodeDeadTimeout(std::chrono::seconds(30));
}

Status ReadCoordinatorMembershipState(testing::FakeCoordinatorServiceProxy &proxy, const std::string &clusterName,
                                      MemberLifecycleState &state)
{
    auto keys = MakeKeys(clusterName);
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(proxy.Range(keys->MembershipTable() + "/" + LOCAL_ADDRESS, "", entries, revision, 0, nullptr));
    CHECK_FAIL_RETURN_STATUS(entries.size() == 1, K_NOT_FOUND, "expected one local membership");
    MembershipValue value;
    RETURN_IF_NOT_OK(MembershipValueCodec::Decode(entries.front().value, value));
    state = value.lifecycleState;
    return Status::OK();
}

Status SetCoordinatorMembershipState(testing::FakeCoordinatorServiceProxy &proxy, const std::string &clusterName,
                                     MemberLifecycleState state, int64_t &modRevision)
{
    auto keys = MakeKeys(clusterName);
    const auto key = keys->MembershipTable() + "/" + LOCAL_ADDRESS;
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    RETURN_IF_NOT_OK(proxy.Range(key, "", entries, revision, 0, nullptr));
    CHECK_FAIL_RETURN_STATUS(entries.size() == 1, K_NOT_FOUND, "expected one local membership");
    MembershipValue value;
    RETURN_IF_NOT_OK(MembershipValueCodec::Decode(entries.front().value, value));
    value.lifecycleState = state;
    std::string encoded;
    RETURN_IF_NOT_OK(MembershipValueCodec::Encode(value, encoded));
    RETURN_IF_NOT_OK(proxy.PutRaw(key, encoded));
    entries.clear();
    RETURN_IF_NOT_OK(proxy.Range(key, "", entries, revision, 0, nullptr));
    CHECK_FAIL_RETURN_STATUS(entries.size() == 1, K_NOT_FOUND, "expected recreated local membership");
    modRevision = entries.front().modRevision;
    return Status::OK();
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

TEST(TopologyEngineTest, BuilderUsesFreshStartWhileCoordinatorRecoveryIsNotReady)
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

TEST(TopologyEngineTest, BuilderUsesFreshStartWhenCoordinatorBootstrapReadTimesOut)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    auto keys = MakeKeys("bootstrap-timeout");
    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_RPC_DEADLINE_EXCEEDED);
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "bootstrap-timeout");
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

TEST(TopologyEngineTest, CoordinatorBootstrapReadFailurePreventsWatchRegistration)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "bootstrap-read-failure";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    ASSERT_NE(engine, nullptr);
    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE);

    EXPECT_EQ(engine->Start().GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_TRUE(proxy.WatchCalls().empty());
    EXPECT_FALSE(ingress.IsBound());
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
}

TEST(TopologyEngineTest, CoordinatorMissingTopologyContinuesToWatchAndStart)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    auto engine = BuildEngine(proxy, ingress, callbacks, "missing-bootstrap");
    ASSERT_NE(engine, nullptr);

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    EXPECT_TRUE(ingress.IsBound());
    EXPECT_GE(proxy.WatchCalls().size(), 2U);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, CoordinatorNotReadyTopologyContinuesToWatchAndStart)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "not-ready-bootstrap";
    auto keys = MakeKeys(clusterName);
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    ASSERT_NE(engine, nullptr);
    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_NOT_READY);

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    EXPECT_TRUE(ingress.IsBound());
    EXPECT_GE(proxy.WatchCalls().size(), 2U);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
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
    EXPECT_EQ(engine->NotifyReconciliationDone().GetCode(), K_NOT_READY);
    EXPECT_EQ(engine->MarkReady().GetCode(), K_NOT_READY);

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(ingress.IsBound());
    EXPECT_GT(proxy.CancelledWatchCount(), 0U);
}

TEST(TopologyEngineTest, LocalRecoveryCannotPublishReadyBeforeAdmissionCompletes)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    auto engine = BuildEngine(proxy, ingress, callbacks, "recovery-before-admission");
    ASSERT_NE(engine, nullptr);
    DS_ASSERT_OK(engine->Start());

    EXPECT_EQ(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine).GetCode(), K_NOT_READY);

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, SuccessfulReadyPublicationEnablesLocalRecoveryRepublish)
{
    {
        testing::FakeCoordinatorServiceProxy proxy;
        TestWatchIngress ingress;
        NoopTopologyCallbacks callbacks;
        auto engine = BuildEngine(proxy, ingress, callbacks, "recovery-after-ready");
        ASSERT_NE(engine, nullptr);
        DS_ASSERT_OK(engine->Start());
        DS_ASSERT_OK(engine->MarkReady());
        DS_ASSERT_OK(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine));
        DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    }
    {
        testing::FakeCoordinatorServiceProxy proxy;
        TestWatchIngress ingress;
        NoopTopologyCallbacks callbacks;
        auto engine = BuildEngine(proxy, ingress, callbacks, "failed-recovery-keeps-gate-open");
        ASSERT_NE(engine, nullptr);
        DS_ASSERT_OK(engine->Start());
        DS_ASSERT_OK(engine->MarkReady());
        ASSERT_TRUE(TopologyEngineTestPeer::ReadyMembershipPublished(*engine));
        proxy.FailNextPut(Status(K_RPC_UNAVAILABLE, "injected recovery failure"));
        EXPECT_EQ(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine).GetCode(), K_RPC_UNAVAILABLE);
        EXPECT_TRUE(TopologyEngineTestPeer::ReadyMembershipPublished(*engine));
        DS_ASSERT_OK(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine));
        DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    }
    {
        testing::FakeCoordinatorServiceProxy proxy;
        TestWatchIngress ingress;
        NoopTopologyCallbacks callbacks;
        auto engine = BuildEngine(proxy, ingress, callbacks, "failed-ready-keeps-gate-closed");
        ASSERT_NE(engine, nullptr);
        DS_ASSERT_OK(engine->Start());
        proxy.FailNextPut(Status(K_RPC_UNAVAILABLE, "injected ready failure"));
        EXPECT_EQ(engine->MarkReady().GetCode(), K_RPC_UNAVAILABLE);
        EXPECT_EQ(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine).GetCode(), K_NOT_READY);
        DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    }
    {
        constexpr char CLUSTER_NAME[] = "recovery-after-reconciliation";
        testing::FakeCoordinatorServiceProxy proxy;
        TestWatchIngress ingress;
        NoopTopologyCallbacks callbacks;
        PutTopology(proxy, CLUSTER_NAME, MakeTopology());
        auto engine = BuildEngine(proxy, ingress, callbacks, CLUSTER_NAME);
        ASSERT_NE(engine, nullptr);
        DS_ASSERT_OK(engine->Start());
        DS_ASSERT_OK(engine->NotifyReconciliationDone());
        DS_ASSERT_OK(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine));
        DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    }
    {
        constexpr char CLUSTER_NAME[] = "failed-reconciliation-keeps-gate-closed";
        testing::FakeCoordinatorServiceProxy proxy;
        TestWatchIngress ingress;
        NoopTopologyCallbacks callbacks;
        PutTopology(proxy, CLUSTER_NAME, MakeTopology());
        auto engine = BuildEngine(proxy, ingress, callbacks, CLUSTER_NAME);
        ASSERT_NE(engine, nullptr);
        DS_ASSERT_OK(engine->Start());
        proxy.FailNextPut(Status(K_RPC_UNAVAILABLE, "injected reconciliation failure"));
        EXPECT_EQ(engine->NotifyReconciliationDone().GetCode(), K_RPC_UNAVAILABLE);
        EXPECT_EQ(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine).GetCode(), K_NOT_READY);
        DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    }
}

TEST(TopologyEngineTest, CoordinatorTopologyReloadRestoresReadyAfterMembershipRecreateWithoutNewTopologyVersion)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "coordinator-recreate-ready";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    ASSERT_NE(engine, nullptr);
    DS_ASSERT_OK(engine->Start());
    DS_ASSERT_OK(engine->MarkReady());

    int64_t membershipModRevision = 0;
    DS_ASSERT_OK(SetCoordinatorMembershipState(proxy, clusterName, MemberLifecycleState::RECOVERING,
                                               membershipModRevision));
    DS_ASSERT_OK(TopologyEngineTestPeer::OnMembershipEnsured(*engine, "coordinator-test", membershipModRevision));

    ASSERT_TRUE(WaitFor([&] {
        MemberLifecycleState state = MemberLifecycleState::UNKNOWN;
        return ReadCoordinatorMembershipState(proxy, clusterName, state).IsOk()
               && state == MemberLifecycleState::READY;
    }));

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, ExitingAndStoppingDisableLocalRecoveryRepublish)
{
    constexpr char CLUSTER_NAME[] = "recovery-exit-serialization";
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    auto engine = BuildEngine(proxy, ingress, callbacks, CLUSTER_NAME);
    ASSERT_NE(engine, nullptr);
    DS_ASSERT_OK(engine->Start());
    DS_ASSERT_OK(engine->MarkReady());
    proxy.BlockNextPut();
    auto recovery = std::async(std::launch::async,
                               [&] { return TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine); });
    const bool recoveryBlocked = proxy.WaitUntilPutBlocked(std::chrono::steady_clock::now() + TEST_WAIT);
    if (!recoveryBlocked) {
        proxy.ReleaseBlockedPut();
    }
    ASSERT_TRUE(recoveryBlocked);
    auto exiting = std::async(std::launch::async, [&] { return engine->MarkExiting(); });

    proxy.ReleaseBlockedPut();
    DS_ASSERT_OK(recovery.get());
    DS_ASSERT_OK(exiting.get());
    MemberLifecycleState storedState = MemberLifecycleState::UNKNOWN;
    DS_ASSERT_OK(ReadCoordinatorMembershipState(proxy, CLUSTER_NAME, storedState));
    EXPECT_EQ(storedState, MemberLifecycleState::EXITING);
    EXPECT_EQ(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine).GetCode(), K_NOT_READY);
    EXPECT_EQ(engine->NotifyReconciliationDone().GetCode(), K_NOT_READY);
    EXPECT_EQ(engine->MarkReady().GetCode(), K_NOT_READY);

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine).GetCode(), K_NOT_READY);
}

TEST(TopologyEngineTest, ShutdownDoesNotWaitForBlockedMembershipPublicationAndClosesRecoveryGate)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    auto engine = BuildEngine(proxy, ingress, callbacks, "bounded-membership-shutdown");
    ASSERT_NE(engine, nullptr);
    DS_ASSERT_OK(engine->Start());
    DS_ASSERT_OK(engine->MarkReady());
    ASSERT_TRUE(TopologyEngineTestPeer::ReadyMembershipPublished(*engine));

    proxy.BlockNextPut();
    auto publication = std::async(
        std::launch::async, [&] { return TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine); });
    const bool publicationBlocked = proxy.WaitUntilPutBlocked(std::chrono::steady_clock::now() + TEST_WAIT);
    if (!publicationBlocked) {
        proxy.ReleaseBlockedPut();
    }
    ASSERT_TRUE(publicationBlocked);

    const auto startedAt = std::chrono::steady_clock::now();
    const auto shutdownStatus = engine->Shutdown(startedAt + std::chrono::milliseconds(500));
    const auto elapsed = std::chrono::steady_clock::now() - startedAt;
    EXPECT_TRUE(shutdownStatus.IsOk()) << shutdownStatus.ToString();
    EXPECT_LT(elapsed, std::chrono::seconds(1));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
    EXPECT_FALSE(TopologyEngineTestPeer::ReadyMembershipPublished(*engine));
    EXPECT_EQ(publication.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);

    proxy.ReleaseBlockedPut();
    DS_ASSERT_OK(publication.get());
    EXPECT_FALSE(TopologyEngineTestPeer::ReadyMembershipPublished(*engine));
    EXPECT_EQ(TopologyEngineTestPeer::RestoreReadyAfterLocalRecovery(*engine).GetCode(), K_NOT_READY);
}

TEST(TopologyEngineTest, ProbeEventInvokesOnlyWorkerProbeHandler)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "probe-event";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    std::mutex mutex;
    std::vector<WorkerProbeRequest> requests;
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, clusterName, [&](WorkerProbeRequest request) {
        std::lock_guard<std::mutex> lock(mutex);
        requests.emplace_back(std::move(request));
        return Status::OK();
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    coordinator::WorkerProbeEventValuePb value;
    value.set_cluster_name(clusterName);
    value.set_probe_round(7);
    value.set_target_address("127.0.0.1:2");
    value.set_target_member_id(std::string(16, 'b'));
    value.set_coordinator_id("coordinator-test");
    std::string encoded;
    ASSERT_TRUE(value.SerializeToString(&encoded));
    const auto probeKey = keys->ProbeTable() + "/" + LOCAL_ADDRESS;
    const auto watchId = FindWatchId(proxy, probeKey);
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::PUT, probeKey, encoded, 1, 1 }));
    value.set_probe_round(8);
    value.set_target_address("127.0.0.1:3");
    value.set_target_member_id(std::string(16, 'c'));
    ASSERT_TRUE(value.SerializeToString(&encoded));
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::PUT, probeKey, encoded, 2, 2 }));
    EXPECT_TRUE(WaitFor([&] {
        std::lock_guard<std::mutex> lock(mutex);
        return requests.size() == 2;
    }));
    {
        std::lock_guard<std::mutex> lock(mutex);
        ASSERT_EQ(requests.size(), 2U);
        EXPECT_EQ(requests.front().probeEpoch, "coordinator-test");
        EXPECT_EQ(requests.front().probeRound, 7U);
        EXPECT_EQ(requests.front().target.address, "127.0.0.1:2");
        EXPECT_EQ(requests.front().target.id, std::string(16, 'b'));
        EXPECT_EQ(requests.back().probeRound, 8U);
        EXPECT_EQ(requests.back().target.address, "127.0.0.1:3");
    }
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, ProbeEventsRemainIndependentAcrossReset)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "probe-reset";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    std::mutex mutex;
    std::vector<WorkerProbeRequest> requests;
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, clusterName, [&](WorkerProbeRequest request) {
        std::lock_guard<std::mutex> lock(mutex);
        requests.emplace_back(std::move(request));
        return Status::OK();
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    coordinator::WorkerProbeEventValuePb value;
    value.set_cluster_name(clusterName);
    value.set_probe_round(7);
    value.set_target_address("127.0.0.1:2");
    value.set_target_member_id(std::string(16, 'b'));
    value.set_coordinator_id("coordinator-test");
    std::string encoded;
    ASSERT_TRUE(value.SerializeToString(&encoded));
    const auto probeKey = keys->ProbeTable() + "/" + LOCAL_ADDRESS;
    const auto watchId = FindWatchId(proxy, probeKey);
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::PUT, probeKey, encoded, 1, 1 }));
    ASSERT_TRUE(WaitFor([&] {
        std::lock_guard<std::mutex> lock(mutex);
        return requests.size() == 1;
    }));

    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId, { CoordinationEventType::RESET, "", "", 0, 0 }));
    value.set_probe_round(8);
    ASSERT_TRUE(value.SerializeToString(&encoded));
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::PUT, probeKey, encoded, 2, 2 }));
    ASSERT_TRUE(WaitFor([&] {
        std::lock_guard<std::mutex> lock(mutex);
        return requests.size() == 2;
    }));
    {
        std::lock_guard<std::mutex> lock(mutex);
        EXPECT_EQ(requests.front().probeRound, 7U);
        EXPECT_EQ(requests.back().probeRound, 8U);
    }
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
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

TEST(TopologyEngineTest, LocalMemberRemovedFromSnapshotRequiresRejoinWithoutSigkill)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "removed-local";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    std::atomic<uint64_t> publishedVersion{ 0 };
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, clusterName);
    builder.SetSnapshotPublishedHandler([&publishedVersion](std::shared_ptr<const TopologySnapshot> snapshot) {
        publishedVersion.store(snapshot->Version());
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    ::testing::internal::CaptureStderr();
    PutTopology(proxy, clusterName, MakeTopologyWithoutLocal(2));
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&engine] { return engine->GetAvailability() == TopologyAvailabilityLevel::ROLE_ISOLATED; }));
    EXPECT_TRUE(engine->RequiresMembershipRejoin());

    PutTopology(proxy, clusterName, MakeTopologyWithoutLocal(3));
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 3));
    ASSERT_TRUE(WaitFor([&publishedVersion] { return publishedVersion.load() == 3; }));
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::ROLE_ISOLATED);
    EXPECT_TRUE(engine->RequiresMembershipRejoin());

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    const auto capturedStderr = ::testing::internal::GetCapturedStderr();
    constexpr char requireRejoinLog[] = "state=local_member_missing action=require_rejoin";
    const auto firstLog = capturedStderr.find(requireRejoinLog);
    ASSERT_NE(firstLog, std::string::npos) << capturedStderr;
    EXPECT_EQ(capturedStderr.find(requireRejoinLog, firstLog + 1), std::string::npos) << capturedStderr;
}

TEST(TopologyEngineTest, VoluntaryExitDoesNotRequireRejoinWhenLocalMemberIsRemoved)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "voluntary-removed-local";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    ASSERT_NE(engine, nullptr);
    DS_ASSERT_OK(engine->Start());
    DS_ASSERT_OK(engine->MarkExiting());

    PutTopology(proxy, clusterName, MakeTopologyWithoutLocal(2));
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&engine] {
        std::shared_ptr<const TopologySnapshot> snapshot;
        return engine->GetSnapshot(snapshot).IsOk() && snapshot->Version() == 2
               && engine->GetAvailability() == TopologyAvailabilityLevel::NOT_READY
               && !engine->RequiresMembershipRejoin();
    }));
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NOT_READY);
    EXPECT_FALSE(engine->RequiresMembershipRejoin());

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
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
    // Snapshot publication happens on the state thread after Start() returns.
    EXPECT_TRUE(WaitFor([&published] { return published.load() > 0U; }));
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, IdempotentExactReadDoesNotRepublishSnapshotCallback)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "idempotent-publish";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    std::atomic<size_t> published{ 0 };
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, clusterName);
    builder.SetSnapshotPublishedHandler(
        [&published](std::shared_ptr<const TopologySnapshot>) { published.fetch_add(1); });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());
    ASSERT_TRUE(WaitFor([&published] { return published.load() == 1; }));

    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 1));
    ASSERT_TRUE(WaitFor([&engine] { return engine->GetDiagnostics().dispatcher.queueDepth == 0; }));

    EXPECT_EQ(published.load(), 1U);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, SnapshotPublicationExceptionDoesNotTerminateStateThread)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "throwing-publish";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, clusterName);
    builder.SetSnapshotPublishedHandler(
        [](std::shared_ptr<const TopologySnapshot>) { throw std::runtime_error("injected callback failure"); });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    PutTopology(proxy, clusterName, MakeTopology(2));
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    std::shared_ptr<const TopologySnapshot> snapshot;
    ASSERT_TRUE(WaitFor([&] { return engine->GetSnapshot(snapshot).IsOk() && snapshot->Version() == 2; }));

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
    invalidTimeout.SetNodeDeadTimeout(std::chrono::seconds(-1));
    EXPECT_EQ(invalidTimeout.Build(output).GetCode(), K_INVALID);

    TopologyEngine::Builder zeroTimeout;
    ConfigureBuilder(zeroTimeout, proxy, ingress, callbacks, "zero-timeout");
    zeroTimeout.SetNodeDeadTimeout(std::chrono::seconds(0));
    DS_ASSERT_OK(zeroTimeout.Build(output));
    ASSERT_NE(output, nullptr);

    TopologyEngine::Builder invalidIsolationTimeout;
    ConfigureBuilder(invalidIsolationTimeout, proxy, ingress, callbacks, "invalid-isolation-timeout");
    invalidIsolationTimeout.SetLocalIsolationTimeout(std::chrono::seconds(-1));
    EXPECT_EQ(invalidIsolationTimeout.Build(output).GetCode(), K_INVALID);
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
    PutTopology(proxy, "global-outage", MakeTopologyWithPeer(1, 3));
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "global-outage");
    std::atomic<uint32_t> probeCalls{ 0 };
    std::atomic<size_t> probedPeers{ 0 };
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetControlBackendProbe([&](const ControlBackendObservation &local, const auto &peers, auto) {
        ++probeCalls;
        probedPeers.store(peers.size());
        std::vector<ControlBackendProbeResult> results;
        for (const auto &target : peers) {
            auto peer = local;
            peer.reporter = target;
            peer.state = ControlBackendState::UNAVAILABLE;
            peer.observedAt = std::chrono::steady_clock::now();
            results.push_back(
                { target, std::move(peer), ControlBackendProbeOutcome::RESPONSE, std::chrono::milliseconds(0) });
        }
        return results;
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::CONTROL_DEGRADED; }));
    ASSERT_TRUE(WaitFor([&] { return probeCalls.load() > 0; }));
    EXPECT_EQ(probedPeers.load(), 3U);
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
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetControlBackendProbe([](const ControlBackendObservation &local, const auto &peers, auto) {
        auto peer = local;
        peer.reporter = peers.front();
        peer.state = ControlBackendState::AVAILABLE;
        peer.observedAt = std::chrono::steady_clock::now();
        const auto target = peer.reporter;
        return std::vector<ControlBackendProbeResult>{
            { target, std::move(peer), ControlBackendProbeOutcome::RESPONSE, std::chrono::milliseconds(0) }
        };
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::CONTROL_DEGRADED; }));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::ROLE_ISOLATED; }));
    proxy.ClearRangeFailures();
    EXPECT_FALSE(engine->RequiresMembershipRejoin());
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 3));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::NORMAL; }));
    EXPECT_FALSE(engine->RequiresMembershipRejoin());
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, MissingPeerQuorumKeepsBackendOutageDegraded)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("missing-quorum");
    PutTopology(proxy, "missing-quorum", MakeTopologyWithPeer());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "missing-quorum");
    std::atomic<uint32_t> probeCalls{ 0 };
    std::atomic<bool> isolated{ false };
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetAvailabilityHandler([&](TopologyAvailabilityLevel level) {
        isolated.store(isolated.load() || level == TopologyAvailabilityLevel::ROLE_ISOLATED);
    });
    builder.SetControlBackendProbe([&](const auto &, const auto &peers, auto) {
        ++probeCalls;
        return std::vector<ControlBackendProbeResult>{
            { peers.front(), std::nullopt, ControlBackendProbeOutcome::DEADLINE_EXCEEDED,
              std::chrono::milliseconds(20) }
        };
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::CONTROL_DEGRADED; }));
    ASSERT_TRUE(WaitFor([&] { return probeCalls.load() >= 4; }));
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::CONTROL_DEGRADED);
    EXPECT_FALSE(isolated.load());
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, BackendRecoveryBeforeThreeLocalConfirmationsStaysAvailable)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("transient-asymmetric");
    PutTopology(proxy, "transient-asymmetric", MakeTopologyWithPeer());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "transient-asymmetric");
    std::atomic<uint32_t> probeCalls{ 0 };
    std::atomic<bool> isolated{ false };
    std::atomic<bool> degradationObserved{ false };
    std::atomic<TopologyAvailabilityLevel> recoveryCallbackObserved{ TopologyAvailabilityLevel::NOT_READY };
    TopologyEngine *engineView = nullptr;
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetAvailabilityHandler([&](TopologyAvailabilityLevel level) {
        isolated.store(isolated.load() || level == TopologyAvailabilityLevel::ROLE_ISOLATED);
        if (level == TopologyAvailabilityLevel::CONTROL_DEGRADED) {
            degradationObserved.store(true);
        } else if (degradationObserved.load() && level == TopologyAvailabilityLevel::NORMAL) {
            recoveryCallbackObserved.store(engineView->GetAvailability());
        }
    });
    builder.SetControlBackendProbe([&](const ControlBackendObservation &local, const auto &peers, auto) {
        if (++probeCalls == 2) {
            proxy.ClearRangeFailures();
        }
        auto peer = local;
        peer.reporter = peers.front();
        peer.state = ControlBackendState::AVAILABLE;
        peer.observedAt = std::chrono::steady_clock::now();
        return std::vector<ControlBackendProbeResult>{
            { peer.reporter, std::move(peer), ControlBackendProbeOutcome::RESPONSE, std::chrono::milliseconds(0) }
        };
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    engineView = engine.get();
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return probeCalls.load() == 2; }));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::NORMAL; }));
    ASSERT_TRUE(WaitFor(
        [&] { return recoveryCallbackObserved.load() != TopologyAvailabilityLevel::NOT_READY; }));
    EXPECT_EQ(recoveryCallbackObserved.load(), TopologyAvailabilityLevel::NORMAL);
    EXPECT_FALSE(isolated.load());
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, PeerHashRingRefreshAcceptsNewerVersionOnly)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("peer-newer-only");
    PutTopology(proxy, "peer-newer-only", MakeTopologyWithPeer(5));
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "peer-newer-only");
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    std::atomic<uint32_t> refreshCalls{ 0 };
    builder.SetPeerTopologyRefresh(
        [&](uint64_t currentVersion, const auto &, auto, std::shared_ptr<const TopologySnapshot> &peerSnapshot) {
            const uint32_t call = ++refreshCalls;
            TopologyState state = MakeTopologyWithPeer(call == 1 ? currentVersion : currentVersion + 1);
            return TopologySnapshot::Create(std::move(state), 0, std::string(64, 'a'), peerSnapshot);
        });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetDiagnostics().peerObservedTopologyVersion == 6; }));
    EXPECT_GE(refreshCalls.load(), 2U);
    EXPECT_FALSE(engine->RequiresMembershipRejoin());
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, PeerHashRingRefreshMissingLocalMemberRequiresRejoin)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("peer-missing-local");
    PutTopology(proxy, "peer-missing-local", MakeTopologyWithPeer(3));
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "peer-missing-local");
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetPeerTopologyRefresh(
        [](uint64_t currentVersion, const auto &, auto, std::shared_ptr<const TopologySnapshot> &peerSnapshot) {
            return TopologySnapshot::Create(MakeTopologyWithoutLocal(currentVersion + 1), 0, std::string(64, 'b'),
                                            peerSnapshot);
        });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->RequiresMembershipRejoin(); }));
    EXPECT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::ROLE_ISOLATED; }));
    EXPECT_EQ(engine->GetDiagnostics().peerObservedTopologyVersion, 4);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, PeerHashRingRefreshMissingLocalMemberDoesNotRequireRejoinDuringVoluntaryExit)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("peer-missing-local-voluntary-exit");
    PutTopology(proxy, "peer-missing-local-voluntary-exit", MakeTopologyWithPeer(3));
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "peer-missing-local-voluntary-exit");
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetPeerTopologyRefresh(
        [](uint64_t currentVersion, const auto &, auto, std::shared_ptr<const TopologySnapshot> &peerSnapshot) {
            return TopologySnapshot::Create(MakeTopologyWithoutLocal(currentVersion + 1), 0, std::string(64, 'b'),
                                            peerSnapshot);
        });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());
    DS_ASSERT_OK(engine->MarkExiting());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] {
        return engine->GetDiagnostics().peerObservedTopologyVersion == 4
               && engine->GetAvailability() == TopologyAvailabilityLevel::NOT_READY;
    }));
    EXPECT_FALSE(engine->RequiresMembershipRejoin());
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NOT_READY);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, PeerHashRingRefreshFailedLocalMemberRequiresRejoin)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("peer-failed-local");
    PutTopology(proxy, "peer-failed-local", MakeTopologyWithPeer(3));
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "peer-failed-local");
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetPeerTopologyRefresh(
        [](uint64_t currentVersion, const auto &, auto, std::shared_ptr<const TopologySnapshot> &peerSnapshot) {
            auto state = MakeTopologyWithPeer(currentVersion + 1);
            state.members.front().state = MemberState::FAILED;
            state.activeBatch = ActiveBatch{ TopologyChangeType::FAILURE, currentVersion + 1 };
            return TopologySnapshot::Create(std::move(state), 0, std::string(64, 'c'), peerSnapshot);
        });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->RequiresMembershipRejoin(); }));
    EXPECT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::ROLE_ISOLATED; }));
    EXPECT_EQ(engine->GetDiagnostics().peerObservedTopologyVersion, 4);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, ConfirmedLocalBackendIsolationKeepsWorkerAliveAfterTimeout)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("isolated-keepalive");
    PutTopology(proxy, "isolated-keepalive", MakeTopologyWithPeer());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "isolated-keepalive");
    builder.SetLocalIsolationTimeout(std::chrono::seconds(0));
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetControlBackendProbe([](const ControlBackendObservation &local, const auto &peers, auto) {
        auto peer = local;
        peer.reporter = peers.front();
        peer.state = ControlBackendState::AVAILABLE;
        peer.observedAt = std::chrono::steady_clock::now();
        return std::vector<ControlBackendProbeResult>{
            { peer.reporter, std::move(peer), ControlBackendProbeOutcome::RESPONSE, std::chrono::milliseconds(0) }
        };
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::ROLE_ISOLATED; }));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, NonAuthoritativeReadFailureDoesNotCancelKeepAliveIsolation)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("isolated-read-failure");
    PutTopology(proxy, "isolated-read-failure", MakeTopologyWithPeer());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "isolated-read-failure");
    builder.SetLocalIsolationTimeout(std::chrono::seconds(0));
    builder.SetFailureScopeProbeInterval(std::chrono::milliseconds(20));
    builder.SetControlBackendProbe([](const ControlBackendObservation &local, const auto &peers, auto) {
        auto peer = local;
        peer.reporter = peers.front();
        peer.state = ControlBackendState::AVAILABLE;
        peer.observedAt = std::chrono::steady_clock::now();
        return std::vector<ControlBackendProbeResult>{
            { peer.reporter, std::move(peer), ControlBackendProbeOutcome::RESPONSE, std::chrono::milliseconds(0) }
        };
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    DS_ASSERT_OK(EmitTopologyEvent(proxy, ingress, *keys, 2));
    ASSERT_TRUE(WaitFor([&] { return engine->GetAvailability() == TopologyAvailabilityLevel::ROLE_ISOLATED; }));
    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_NOT_FOUND, 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    EXPECT_NE(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);
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

TEST(TopologyEngineTest, WorkerWatchStartFailureNeverPublishesHostAdmission)
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
    proxy.FailNextWatchForKey(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE);

    EXPECT_EQ(engine->Start().GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(normalAdmissions.load(), 0U);
}

TEST(TopologyEngineTest, StartRollbackRemovesPublishedCoordinatorMembership)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "start-membership-rollback";
    const auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE);

    EXPECT_EQ(engine->Start().GetCode(), K_RPC_UNAVAILABLE);
    std::vector<KeyValueEntry> entries;
    int64_t revision = 0;
    DS_ASSERT_OK(proxy.Range(keys->MembershipTable() + "/" + LOCAL_ADDRESS, "", entries, revision, 0, nullptr));
    EXPECT_TRUE(entries.empty());
}

TEST(TopologyEngineTest, ServingAvailabilityIsPublishedBeforeAdmissionCallback)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "serving-publication-order", MakeTopology());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "serving-publication-order");
    TopologyEngine *engineView = nullptr;
    std::atomic<TopologyAvailabilityLevel> observed{ TopologyAvailabilityLevel::NOT_READY };
    builder.SetAvailabilityHandler([&](TopologyAvailabilityLevel level) {
        if (level == TopologyAvailabilityLevel::NORMAL || level == TopologyAvailabilityLevel::CONTROL_DEGRADED) {
            observed.store(engineView->GetAvailability());
        }
    });
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    engineView = engine.get();

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(observed.load(), TopologyAvailabilityLevel::NORMAL);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, StartRollbackCleanupFailureRemainsRetryable)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("start-rollback");
    PutTopology(proxy, "start-rollback", MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, "start-rollback");
    proxy.FailNextWatchForKey(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE);
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
