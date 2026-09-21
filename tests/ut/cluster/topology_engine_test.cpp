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

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"
#include "datasystem/cluster/control/topology_task_materializer.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/protos/coordinator.pb.h"
#include "gtest/gtest.h"
#include "ut/cluster/testing/fake_coordinator_service_proxy.h"
#include "ut/common.h"

namespace datasystem::cluster {

class TopologyEngineTestPeer final {
public:
    static Status ReloadTopology(TopologyEngine &engine)
    {
        return engine.ReloadTopology(true);
    }

    static std::chrono::seconds CoordinatorReadyTimeout(const TopologyEngine &engine)
    {
        return engine.options_.coordinatorReadyTimeout;
    }

    static Status ApplyCoordinatorTopologyEvent(TopologyEngine &engine, const CoordinationEvent &event)
    {
        return engine.ApplyCoordinatorTopologyEvent(event);
    }

    static Status HandleRuntimeEvent(TopologyEngine &engine, CoordinationEvent &&event)
    {
        return engine.HandleRuntimeEvent(RuntimeEvent{ RuntimeEventPayload{ std::move(event) } });
    }

    static Status EnqueueCoordinationEvent(TopologyEngine &engine, CoordinationEvent event)
    {
        return engine.EnqueueCoordinationEvent(std::move(event));
    }

    static void InvalidateCoordinatorWatches(TopologyEngine &engine)
    {
        auto *backend = dynamic_cast<DsCoordinationBackend *>(engine.memberBackend_.get());
        ASSERT_NE(backend, nullptr);
        backend->InvalidateWatches();
    }

    static bool OwnsCoordinatorWatch(const TopologyEngine &engine, const std::string &coordinatorId, int64_t watchId)
    {
        const auto *backend = dynamic_cast<const DsCoordinationBackend *>(engine.memberBackend_.get());
        EXPECT_NE(backend, nullptr);
        return backend != nullptr && backend->OwnsWatchIdentity(coordinatorId, watchId);
    }

    static void RecordPeerRpcFailure(TopologyEngine &engine, const HostPort &target,
                                     std::chrono::steady_clock::time_point now)
    {
        auto *backend = dynamic_cast<DsCoordinationBackend *>(engine.memberBackend_.get());
        ASSERT_NE(backend, nullptr);
        backend->RecordPeerRpcFailure(target, now);
    }

    static std::vector<std::string> GetFailedTargets(TopologyEngine &engine, std::chrono::steady_clock::time_point now)
    {
        auto *backend = dynamic_cast<DsCoordinationBackend *>(engine.memberBackend_.get());
        EXPECT_NE(backend, nullptr);
        return backend == nullptr ? std::vector<std::string>{} : backend->GetFailedTargets(now);
    }

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

    static Status SubmitProgressCompletion(TopologyEngine &engine, TopologyCallbackCompletion completion)
    {
        return engine.dispatcher_.SubmitCompletion(std::move(completion));
    }

    static bool HasProgressPool(const TopologyEngine &engine)
    {
        return engine.progressPool_ != nullptr;
    }

    static void PublishCurrentAvailability(TopologyEngine &engine)
    {
        engine.publishedAvailability_.store(engine.availability_.load());
    }

    static uint64_t ExecutorStaleCount(const TopologyEngine &engine)
    {
        return engine.executor_.GetDiagnostics().stale;
    }
};

namespace {

constexpr char LOCAL_ADDRESS[] = "127.0.0.1:10001";
constexpr char LOCAL_ID[] = "aaaaaaaaaaaaaaaa";
constexpr auto TEST_WAIT = std::chrono::seconds(3);

std::vector<uint32_t> MakePersistedTokens(const std::string &address)
{
    constexpr uint32_t tokensPerMember = 4;
    std::vector<uint32_t> tokens;
    tokens.reserve(tokensPerMember);
    for (uint32_t index = 0; index < tokensPerMember; ++index) {
        tokens.emplace_back(HashAlgorithm::MakeToken(address, index, 0));
    }
    return tokens;
}

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
    state.members = { Member{ { LOCAL_ID, LOCAL_ADDRESS }, MemberState::ACTIVE, MakePersistedTokens(LOCAL_ADDRESS) } };
    return state;
}

TopologyState MakeTopologyWithPeer(uint64_t version = 1, size_t peerCount = 1, char firstPeerId = 'b')
{
    auto state = MakeTopology(version);
    for (size_t i = 0; i < peerCount; ++i) {
        const auto address = "127.0.0.1:" + std::to_string(10'002 + i);
        state.members.emplace_back(
            Member{ { std::string(16, static_cast<char>(firstPeerId + i)), address }, MemberState::ACTIVE,
                    MakePersistedTokens(address) });
    }
    return state;
}

TopologyState MakeTopologyWithoutLocal(uint64_t version = 1)
{
    TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    const std::string address = "127.0.0.1:10002";
    state.members = { Member{ { std::string(16, 'b'), address }, MemberState::ACTIVE,
                              MakePersistedTokens(address) } };
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
    auto found = std::find_if(watches.rbegin(), watches.rend(), [&key](const auto &watch) { return watch.key == key; });
    EXPECT_NE(found, watches.rend());
    return found == watches.rend() ? 0 : found->watchId;
}

Status EmitTopologyEvent(testing::FakeCoordinatorServiceProxy &proxy, TestWatchIngress &ingress,
                         const TopologyKeyHelper &keys, uint64_t version)
{
    const auto key = TopologyStorageKey(keys);
    return ingress.Emit("coordinator-test", FindWatchId(proxy, key),
                        { CoordinationEventType::PUT, key, "", static_cast<int64_t>(version),
                          static_cast<int64_t>(version) });
}

Status EmitCompleteTopologyEvent(testing::FakeCoordinatorServiceProxy &proxy, TestWatchIngress &ingress,
                                 const TopologyKeyHelper &keys, const TopologyState &state, int64_t revision)
{
    std::string encoded;
    RETURN_IF_NOT_OK(TopologyRepositoryCodec::EncodeTopology(state, encoded));
    const auto key = TopologyStorageKey(keys);
    return ingress.Emit("coordinator-test", FindWatchId(proxy, key),
                        { CoordinationEventType::PUT, key, std::move(encoded),
                          static_cast<int64_t>(state.version), revision });
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

TopologyCallbackCompletion MakeUnmatchedTaskCompletion(size_t index)
{
    // A distinct target per completion yields distinct deterministic task ids; the task record itself stays absent
    // so revalidation always ends as one bounded stale completion.
    const auto targetAddress = "127.0.0.1:" + std::to_string(10'003 + index);
    TopologyMigrateTask task;
    task.type = TopologyChangeType::SCALE_OUT;
    task.epoch = 1;
    task.executorAddress = LOCAL_ADDRESS;
    task.targetAddress = targetAddress;
    task.sourceRanges = { { LOCAL_ADDRESS, { 0U, 100U }, false } };
    task.taskId = TopologyTaskMaterializer::BuildTaskId(task);
    TopologyExecutionFence fence;
    fence.taskId = task.taskId;
    fence.taskKind = TopologyTaskKind::MIGRATE;
    fence.batchType = TopologyChangeType::SCALE_OUT;
    fence.batchEpoch = 1;
    fence.phase = TopologyCallbackPhase::SCALE_OUT;
    fence.executor = { std::string(16, 'a'), LOCAL_ADDRESS };
    fence.source = fence.executor;
    fence.target = { std::string(16, 'c'), targetAddress };
    fence.ranges = { { 0U, 100U } };
    TopologyCallbackCompletion completion;
    completion.fence = std::move(fence);
    completion.businessOperationId =
        TopologyTaskMaterializer::BuildBusinessOperationId(completion.fence.phase, completion.fence);
    completion.status = Status::OK();
    completion.deadline = std::chrono::steady_clock::now() + TEST_WAIT + TEST_WAIT;
    return completion;
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
    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_INVALID);

    EXPECT_EQ(engine->Start().GetCode(), K_INVALID);
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

TEST(TopologyEngineTest, CoordinatorRecoveringKeepsLastGoodSnapshotAdmitted)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "recovering-last-good";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    ASSERT_NE(engine, nullptr);
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));
    TopologyEngineTestPeer::PublishCurrentAvailability(*engine);

    std::shared_ptr<const TopologySnapshot> beforeRecovery;
    DS_ASSERT_OK(engine->GetSnapshot(beforeRecovery));
    ASSERT_NE(beforeRecovery, nullptr);
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);

    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_NOT_READY);
    EXPECT_EQ(TopologyEngineTestPeer::ReloadTopology(*engine).GetCode(), K_NOT_READY);

    std::shared_ptr<const TopologySnapshot> duringRecovery;
    DS_ASSERT_OK(engine->GetSnapshot(duringRecovery));
    ASSERT_NE(duringRecovery, nullptr);
    EXPECT_EQ(duringRecovery->Version(), beforeRecovery->Version());
    EXPECT_EQ(duringRecovery->CanonicalDigest(), beforeRecovery->CanonicalDigest());
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);

    PutTopology(proxy, clusterName, MakeTopology(2));
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));
    std::shared_ptr<const TopologySnapshot> afterRecovery;
    DS_ASSERT_OK(engine->GetSnapshot(afterRecovery));
    ASSERT_NE(afterRecovery, nullptr);
    EXPECT_EQ(afterRecovery->Version(), 2U);
    EXPECT_EQ(engine->GetAvailability(), TopologyAvailabilityLevel::NORMAL);
}

TEST(TopologyEngineTest, ShutdownCancelsCoordinatorReadyWait)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "cancel-ready-wait";
    auto keys = MakeKeys(clusterName);
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_NOT_READY, 1'000);

    auto start = std::async(std::launch::async, [&] { return engine->Start(); });
    ASSERT_EQ(start.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout);

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(start.get().GetCode(), K_SHUTTING_DOWN);
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
}

TEST(TopologyEngineTest, ShutdownCancelsCoordinatorTransportRetry)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "cancel-transport-retry";
    auto keys = MakeKeys(clusterName);
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    constexpr size_t persistentFailures = 1'000;
    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_DEADLINE_EXCEEDED, persistentFailures);

    auto start = std::async(std::launch::async, [&] { return engine->Start(); });
    ASSERT_EQ(start.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout);

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(start.get().GetCode(), K_SHUTTING_DOWN);
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
}

TEST(TopologyEngineTest, CoordinatorReadyWaitContinuesStartupAfterServing)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "resume-ready-wait";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_NOT_READY, 2);

    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, CoordinatorReadyWaitSurvivesTransientTimeout)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "recovery-timeout-repro";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    ASSERT_NE(engine, nullptr);
    const auto topologyKey = TopologyStorageKey(*keys);
    std::atomic<size_t> reads{ 0 };
    constexpr size_t timeoutRead = 2;
    proxy.SetRangeEntryInterceptor([&](const std::string &key) {
        if (key != topologyKey) {
            return;
        }
        const auto read = ++reads;
        if (read == 1) {
            proxy.FailNextRangeForKey(key, K_NOT_READY);
        } else if (read == timeoutRead) {
            proxy.FailNextRangeForKey(key, K_RPC_DEADLINE_EXCEEDED);
        }
    });

    const auto started = std::chrono::steady_clock::now();
    const auto status = engine->Start();
    const auto elapsed = std::chrono::steady_clock::now() - started;
    proxy.SetRangeEntryInterceptor(nullptr);
    EXPECT_TRUE(status.IsOk()) << "reads=" << reads.load() << " elapsed_ms="
                              << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
                              << " " << status.ToString();
    EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, CoordinatorReadyWaitRetriesInitialTransportFailure)
{
    for (const auto code : { K_RPC_DEADLINE_EXCEEDED, K_RPC_UNAVAILABLE, K_RPC_NETWORK_BLIP }) {
        testing::FakeCoordinatorServiceProxy proxy;
        TestWatchIngress ingress;
        NoopTopologyCallbacks callbacks;
        const std::string clusterName = "initial-transport-retry";
        auto keys = MakeKeys(clusterName);
        PutTopology(proxy, clusterName, MakeTopology());
        auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
        ASSERT_NE(engine, nullptr);
        proxy.FailNextRangeForKey(TopologyStorageKey(*keys), code);

        DS_ASSERT_OK(engine->Start());
        EXPECT_EQ(engine->GetState(), TopologyEngineState::RUNNING);
        DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    }
}

TEST(TopologyEngineTest, CoordinatorReadyWaitBoundsPersistentTransportFailure)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "transport-retry-deadline";
    auto keys = MakeKeys(clusterName);
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, clusterName);
    builder.SetCoordinatorReadyTimeout(std::chrono::seconds::zero());
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    constexpr size_t persistentFailures = 1'000;
    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_DEADLINE_EXCEEDED, persistentFailures);

    const auto started = std::chrono::steady_clock::now();
    const auto status = engine->Start();
    constexpr auto maxWait = std::chrono::seconds(4);
    EXPECT_LT(std::chrono::steady_clock::now() - started, maxWait);
    EXPECT_EQ(status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_NE(status.ToString().find("Worker startup deadline"), std::string::npos);
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
    EXPECT_TRUE(proxy.WatchCalls().empty());
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

TEST(TopologyEngineTest, CoordinatorTopologyReloadRestoresReadyWithExactLocalMembershipRead)
{
    constexpr uint32_t membershipPrefixFailureCount = 100;
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
    proxy.FailRangeForKeyTimes(keys->MembershipTable() + "/", K_RPC_UNAVAILABLE, membershipPrefixFailureCount);
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
    const auto recoveryStatus = recovery.get();
    EXPECT_TRUE(recoveryStatus.IsOk() || recoveryStatus.GetCode() == K_TRY_AGAIN);
    const auto exitingStatus = exiting.get();
    EXPECT_TRUE(exitingStatus.IsOk() || exitingStatus.GetCode() == K_TRY_AGAIN);
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

TEST(TopologyEngineTest, MembershipInitialSnapshotLargerThanEventQueueCompletes)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "large-membership-snapshot";
    auto keys = MakeKeys(clusterName);
    constexpr size_t PEERS = 2000;
    auto topology = MakeTopologyWithPeer(1, PEERS);
    for (size_t i = 1; i < topology.members.size(); ++i) {
        topology.members[i].identity.id = std::string(16, 'x');
        const auto encodedId = std::to_string(i);
        topology.members[i].identity.id.replace(0, encodedId.size(), encodedId);
    }
    PutTopology(proxy, clusterName, topology);
    MembershipValue membership;
    membership.lifecycleState = MemberLifecycleState::READY;
    std::string encoded;
    DS_ASSERT_OK(MembershipValueCodec::Encode(membership, encoded));
    for (size_t i = 1; i < topology.members.size(); ++i) {
        DS_ASSERT_OK(proxy.PutRaw(keys->MembershipTable() + "/" + topology.members[i].identity.address, encoded));
    }
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    DS_ASSERT_OK(engine->Start());
    EXPECT_EQ(engine->Membership().GetWriteCandidates(LOCAL_ADDRESS, "key", 3).size(), 3U);
    EXPECT_EQ(engine->Membership().GetWriteCandidates(LOCAL_ADDRESS, "key", PEERS).size(), PEERS);
    EXPECT_EQ(proxy.WatchCalls().size(), 4U);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, CoordinatorMembershipHintsFenceLateEventsAndRebuildEmptyPeerSnapshot)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "membership-write-hints";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopologyWithPeer());
    const std::string peer = "127.0.0.1:10002";
    const std::string key = keys->MembershipTable() + "/" + peer;
    MembershipValue value;
    value.lifecycleState = MemberLifecycleState::READY;
    std::string ready;
    DS_ASSERT_OK(MembershipValueCodec::Encode(value, ready));
    DS_ASSERT_OK(proxy.PutRaw(key, ready));
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    DS_ASSERT_OK(engine->Start());
    auto candidates = [&] { return engine->Membership().GetWriteCandidates(LOCAL_ADDRESS, "key", 3); };
    ASSERT_TRUE(WaitFor([&] { return candidates() == std::vector<std::string>({ peer }); }));
    const auto watchId = FindWatchId(proxy, keys->MembershipTable() + "/");
    value.lifecycleState = MemberLifecycleState::EXITING;
    std::string exiting;
    DS_ASSERT_OK(MembershipValueCodec::Encode(value, exiting));
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::PUT, key, exiting, 2, 100 }));
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::PUT, key, ready, 1, 99 }));
    EXPECT_TRUE(candidates().empty());
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::DELETE, key, "", 0, 102 }));
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::PUT, key, ready, 3, 101 }));
    EXPECT_TRUE(candidates().empty());
    DS_ASSERT_OK(ingress.Emit("coordinator-test", watchId,
                              { CoordinationEventType::PUT, key, ready, 4, 103 }));
    ASSERT_TRUE(WaitFor([&] { return candidates() == std::vector<std::string>({ peer }); }));
    int64_t revision = 0;
    int64_t deleted = 0;
    DS_ASSERT_OK(proxy.DeleteRange(key, "", deleted, revision, 0, COORDINATOR_NO_MOD_REVISION_CHECK));
    TopologyEngineTestPeer::InvalidateCoordinatorWatches(*engine);
    ASSERT_TRUE(WaitFor([&] { return candidates().empty(); }));
    ASSERT_TRUE(WaitFor([&] {
        const auto current = FindWatchId(proxy, keys->MembershipTable() + "/");
        return current != watchId && TopologyEngineTestPeer::OwnsCoordinatorWatch(*engine, "coordinator-test", current);
    }));
    CoordinationEvent stale{ CoordinationEventType::PUT, key, ready, 5, 104 };
    stale.sourceAuthorityId = "coordinator-test";
    stale.sourceWatchId = watchId;
    EXPECT_EQ(TopologyEngineTestPeer::EnqueueCoordinationEvent(*engine, std::move(stale)).GetCode(), K_NOT_READY);
    EXPECT_TRUE(candidates().empty());
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, CoordinatorMembershipResetClearsCandidatesWithCompletelyEmptySnapshot)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "membership-write-hints";
    auto keys = MakeKeys(clusterName);
    PutTopology(proxy, clusterName, MakeTopologyWithPeer());
    const std::string peer = "127.0.0.1:10002";
    const std::string key = keys->MembershipTable() + "/" + peer;
    MembershipValue value;
    value.lifecycleState = MemberLifecycleState::READY;
    std::string ready;
    DS_ASSERT_OK(MembershipValueCodec::Encode(value, ready));
    DS_ASSERT_OK(proxy.PutRaw(key, ready));
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    DS_ASSERT_OK(engine->Start());
    auto candidates = [&] { return engine->Membership().GetWriteCandidates(LOCAL_ADDRESS, "key", 3); };
    ASSERT_TRUE(WaitFor([&] { return candidates() == std::vector<std::string>({ peer }); }));
    const auto watchId = FindWatchId(proxy, keys->MembershipTable() + "/");
    proxy.ReturnEmptyWatchSnapshotForKey(keys->MembershipTable() + "/");
    TopologyEngineTestPeer::InvalidateCoordinatorWatches(*engine);
    ASSERT_TRUE(WaitFor([&] { return candidates().empty(); }));
    ASSERT_TRUE(WaitFor([&] {
        const auto current = FindWatchId(proxy, keys->MembershipTable() + "/");
        return current != watchId && TopologyEngineTestPeer::OwnsCoordinatorWatch(*engine, "coordinator-test", current);
    }));
    CoordinationEvent stale{ CoordinationEventType::PUT, key, ready, 5, 104 };
    stale.sourceAuthorityId = "coordinator-test";
    stale.sourceWatchId = watchId;
    EXPECT_EQ(TopologyEngineTestPeer::EnqueueCoordinationEvent(*engine, std::move(stale)).GetCode(), K_NOT_READY);
    EXPECT_TRUE(candidates().empty());
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
    int64_t currentWatchId = 0;
    ASSERT_TRUE(WaitFor([&] {
        currentWatchId = FindWatchId(proxy, probeKey);
        return currentWatchId != watchId
               && TopologyEngineTestPeer::OwnsCoordinatorWatch(*engine, "coordinator-test", currentWatchId);
    }));
    value.set_probe_round(8);
    ASSERT_TRUE(value.SerializeToString(&encoded));
    DS_ASSERT_OK(ingress.Emit("coordinator-test", currentWatchId,
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

TEST(TopologyEngineTest, TopologyPublicationPreservesFailuresUntilPeerIncarnationChanges)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;
    Raii restoreNodeTimeout([savedNodeTimeout] { FLAGS_node_timeout_s = savedNodeTimeout; });
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "preserve-peer-rpc-failure";
    PutTopology(proxy, clusterName, MakeTopologyWithPeer(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    ASSERT_NE(engine, nullptr);

    HostPort peer("127.0.0.1", 10'002);
    const auto now = std::chrono::steady_clock::now();
    TopologyEngineTestPeer::RecordPeerRpcFailure(*engine, peer, now - std::chrono::milliseconds(1'600));
    TopologyEngineTestPeer::RecordPeerRpcFailure(*engine, peer, now - std::chrono::milliseconds(800));
    TopologyEngineTestPeer::RecordPeerRpcFailure(*engine, peer, now);
    ASSERT_EQ(TopologyEngineTestPeer::GetFailedTargets(*engine, now), std::vector<std::string>({ peer.ToString() }));

    PutTopology(proxy, clusterName, MakeTopologyWithPeer(2));
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));

    EXPECT_EQ(TopologyEngineTestPeer::GetFailedTargets(*engine, now), std::vector<std::string>({ peer.ToString() }));

    PutTopology(proxy, clusterName, MakeTopologyWithPeer(3, 1, 'c'));
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));

    EXPECT_TRUE(TopologyEngineTestPeer::GetFailedTargets(*engine, now).empty());
}

TEST(TopologyEngineTest, RemovedPeerCannotCarryFailureEvidenceIntoReplacement)
{
    const auto savedNodeTimeout = FLAGS_node_timeout_s;
    FLAGS_node_timeout_s = 3;
    Raii restoreNodeTimeout([savedNodeTimeout] { FLAGS_node_timeout_s = savedNodeTimeout; });
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const std::string clusterName = "discard-removed-peer-rpc-failure";
    PutTopology(proxy, clusterName, MakeTopologyWithPeer(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, clusterName);
    ASSERT_NE(engine, nullptr);
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));

    HostPort peer("127.0.0.1", 10'002);
    const auto now = std::chrono::steady_clock::now();
    TopologyEngineTestPeer::RecordPeerRpcFailure(*engine, peer, now - std::chrono::milliseconds(1'600));
    TopologyEngineTestPeer::RecordPeerRpcFailure(*engine, peer, now - std::chrono::milliseconds(800));
    TopologyEngineTestPeer::RecordPeerRpcFailure(*engine, peer, now);
    ASSERT_EQ(TopologyEngineTestPeer::GetFailedTargets(*engine, now), std::vector<std::string>({ peer.ToString() }));

    PutTopology(proxy, clusterName, MakeTopology(2));
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));
    EXPECT_TRUE(TopologyEngineTestPeer::GetFailedTargets(*engine, now).empty());

    PutTopology(proxy, clusterName, MakeTopologyWithPeer(3, 1, 'c'));
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));
    EXPECT_TRUE(TopologyEngineTestPeer::GetFailedTargets(*engine, now).empty());
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

    TopologyEngine::Builder invalidCoordinatorReadyTimeout;
    ConfigureBuilder(invalidCoordinatorReadyTimeout, proxy, ingress, callbacks, "invalid-coordinator-ready-timeout");
    invalidCoordinatorReadyTimeout.SetCoordinatorReadyTimeout(std::chrono::seconds(-1));
    EXPECT_EQ(invalidCoordinatorReadyTimeout.Build(output).GetCode(), K_INVALID);

    TopologyEngine::Builder zeroTimeout;
    ConfigureBuilder(zeroTimeout, proxy, ingress, callbacks, "zero-timeout");
    zeroTimeout.SetNodeDeadTimeout(std::chrono::seconds(0));
    DS_ASSERT_OK(zeroTimeout.Build(output));
    ASSERT_NE(output, nullptr);
    EXPECT_EQ(TopologyEngineTestPeer::CoordinatorReadyTimeout(*output), std::chrono::seconds(10));

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

TEST(TopologyEngineTest, CoordinatorTopologyWatchPublishesCompletePayloadWithoutTopologyRange)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("direct-topology-watch");
    PutTopology(proxy, "direct-topology-watch", MakeTopology(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, "direct-topology-watch");
    DS_ASSERT_OK(engine->Start());

    proxy.FailRangeForKeyTimes(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE, 100);
    PutTopology(proxy, "direct-topology-watch", MakeTopology(3));
    DS_ASSERT_OK(EmitCompleteTopologyEvent(proxy, ingress, *keys, MakeTopology(3), 10));
    std::shared_ptr<const TopologySnapshot> snapshot;
    ASSERT_TRUE(WaitFor([&] {
        return engine->GetSnapshot(snapshot).IsOk() && snapshot->Version() == 3
               && snapshot->AuthorityRevision() == 10;
    }));
    EXPECT_EQ(snapshot->CoordinatorId(), "coordinator-test");
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, CoordinatorTopologyWatchRejectsStaleAuthority)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("stale-topology-watch");
    PutTopology(proxy, "stale-topology-watch", MakeTopology(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, "stale-topology-watch");
    DS_ASSERT_OK(engine->Start());

    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(MakeTopology(2), encoded));
    CoordinationEvent event{ CoordinationEventType::PUT, TopologyStorageKey(*keys), std::move(encoded), 2, 10,
                             "stale-coordinator", FindWatchId(proxy, TopologyStorageKey(*keys)) };
    EXPECT_EQ(TopologyEngineTestPeer::ApplyCoordinatorTopologyEvent(*engine, event).GetCode(), K_NOT_READY);
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(engine->GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), 1);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, CoordinatorTopologyWatchDoesNotPublishAfterAuthorityInvalidation)
{
    constexpr char injectPoint[] = "TopologyEngine.ApplyCoordinatorTopologyEvent.beforeCommit";
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("invalidated-topology-watch");
    PutTopology(proxy, "invalidated-topology-watch", MakeTopology(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, "invalidated-topology-watch");
    DS_ASSERT_OK(engine->Start());

    std::string encoded;
    DS_ASSERT_OK(TopologyRepositoryCodec::EncodeTopology(MakeTopology(2), encoded));
    CoordinationEvent event{ CoordinationEventType::PUT, TopologyStorageKey(*keys), std::move(encoded), 2, 10,
                             "coordinator-test", FindWatchId(proxy, TopologyStorageKey(*keys)) };
    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Status applyStatus;
    std::thread applyThread([&] {
        applyStatus = TopologyEngineTestPeer::ApplyCoordinatorTopologyEvent(*engine, event);
    });
    Raii releaseApply([&] {
        (void)inject::Clear(injectPoint);
        if (applyThread.joinable()) {
            applyThread.join();
        }
    });
    for (size_t retry = 0; retry < 200 && inject::GetExecuteCount(injectPoint) == 0; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_GT(inject::GetExecuteCount(injectPoint), 0);
    TopologyEngineTestPeer::InvalidateCoordinatorWatches(*engine);
    DS_ASSERT_OK(inject::Clear(injectPoint));
    applyThread.join();

    EXPECT_EQ(applyStatus.GetCode(), K_NOT_READY);
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(engine->GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), 1);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, InvalidCoordinatorTopologyWatchFallsBackToRange)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("invalid-topology-watch");
    PutTopology(proxy, "invalid-topology-watch", MakeTopology(1));
    auto engine = BuildEngine(proxy, ingress, callbacks, "invalid-topology-watch");
    DS_ASSERT_OK(engine->Start());

    PutTopology(proxy, "invalid-topology-watch", MakeTopology(2));
    const auto key = TopologyStorageKey(*keys);
    DS_ASSERT_OK(ingress.Emit("coordinator-test", FindWatchId(proxy, key),
                              { CoordinationEventType::PUT, key, "invalid", 2, 10 }));
    std::shared_ptr<const TopologySnapshot> snapshot;
    ASSERT_TRUE(WaitFor([&] { return engine->GetSnapshot(snapshot).IsOk() && snapshot->Version() == 2; }));
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyEngineTest, WorkerWatchTransientFailureRetriesStartup)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    const auto keys = MakeKeys("start-watch-retry");
    PutTopology(proxy, "start-watch-retry", MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, "start-watch-retry");
    proxy.FailNextWatchForKey(TopologyStorageKey(*keys), K_RPC_UNAVAILABLE);

    DS_ASSERT_OK(engine->Start());
    EXPECT_TRUE(ingress.IsBound());
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
    proxy.FailNextWatchForKey(TopologyStorageKey(*keys), K_INVALID);

    EXPECT_EQ(engine->Start().GetCode(), K_INVALID);
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
    proxy.FailNextRangeForKey(TopologyStorageKey(*keys), K_INVALID);

    EXPECT_EQ(engine->Start().GetCode(), K_INVALID);
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
    proxy.FailNextWatchForKey(TopologyStorageKey(*keys), K_INVALID);
    ingress.FailNextUnbind();

    EXPECT_EQ(engine->Start().GetCode(), K_INVALID);
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

TEST(TopologyEngineTest, GetRoutingHostIdsReadsPublishedSnapshotWithoutCoordinatorAccess)
{
    constexpr char CLUSTER_NAME[] = "host-id-from-snapshot";
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, CLUSTER_NAME, MakeTopology());
    auto keys = MakeKeys(CLUSTER_NAME);
    MembershipValue membership;
    membership.lifecycleState = MemberLifecycleState::READY;
    membership.hostId = "host-a";
    std::string encoded;
    DS_ASSERT_OK(MembershipValueCodec::Encode(membership, encoded));
    DS_ASSERT_OK(proxy.PutRaw(keys->MembershipTable() + "/" + LOCAL_ADDRESS, encoded));

    auto engine = BuildEngine(proxy, ingress, callbacks, CLUSTER_NAME);
    ASSERT_NE(engine, nullptr);
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));

    // After the snapshot carries host ids, a coordinator membership read failure must not affect
    // GetRoutingHostIds, which now serves from the local snapshot instead of re-reading the backend.
    proxy.FailRangeForKeyTimes(keys->MembershipTable() + "/", K_RPC_UNAVAILABLE, 1);
    std::unordered_map<std::string, std::string> hostIds;
    DS_ASSERT_OK(engine->GetRoutingHostIds(hostIds));
    ASSERT_EQ(hostIds.size(), 1UL);
    EXPECT_EQ(hostIds.at(LOCAL_ADDRESS), "host-a");
}

TEST(TopologyEngineTest, HostIdsRecoverAtTheSameTopologyVersion)
{
    constexpr char CLUSTER_NAME[] = "host-id-recovery";
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, CLUSTER_NAME, MakeTopology());
    const auto keys = MakeKeys(CLUSTER_NAME);
    MembershipValue membership;
    membership.lifecycleState = MemberLifecycleState::READY;
    membership.hostId = "host-a";
    std::string encoded;
    DS_ASSERT_OK(MembershipValueCodec::Encode(membership, encoded));
    DS_ASSERT_OK(proxy.PutRaw(keys->MembershipTable() + "/" + LOCAL_ADDRESS, encoded));
    auto engine = BuildEngine(proxy, ingress, callbacks, CLUSTER_NAME);
    ASSERT_NE(engine, nullptr);
    proxy.FailRangeForKeyTimes(keys->MembershipTable() + "/", K_RPC_UNAVAILABLE, 1);
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));
    std::unordered_map<std::string, std::string> hostIds;
    DS_ASSERT_OK(engine->GetRoutingHostIds(hostIds));
    EXPECT_TRUE(hostIds.empty());
    std::shared_ptr<const TopologySnapshot> first;
    DS_ASSERT_OK(engine->GetSnapshot(first));

    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));
    DS_ASSERT_OK(engine->GetRoutingHostIds(hostIds));
    EXPECT_EQ(hostIds.at(LOCAL_ADDRESS), "host-a");
    membership.hostId = "host-b";
    DS_ASSERT_OK(MembershipValueCodec::Encode(membership, encoded));
    DS_ASSERT_OK(proxy.PutRaw(keys->MembershipTable() + "/" + LOCAL_ADDRESS, encoded));
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));
    DS_ASSERT_OK(engine->GetRoutingHostIds(hostIds));
    EXPECT_EQ(hostIds.at(LOCAL_ADDRESS), "host-b");
    std::shared_ptr<const TopologySnapshot> latest;
    DS_ASSERT_OK(engine->GetSnapshot(latest));
    EXPECT_EQ(latest->Version(), first->Version());
    EXPECT_EQ(latest->CanonicalDigest(), first->CanonicalDigest());

    proxy.FailRangeForKeyTimes(keys->MembershipTable() + "/", K_RPC_UNAVAILABLE, 1);
    DS_ASSERT_OK(engine->GetRoutingHostIds(hostIds));
    DS_ASSERT_OK(TopologyEngineTestPeer::ReloadTopology(*engine));
    DS_ASSERT_OK(engine->GetRoutingHostIds(hostIds));
    EXPECT_EQ(hostIds.at(LOCAL_ADDRESS), "host-b");
}

TEST(TopologyEngineTest, CompletionProcessingRunsConcurrentlyWithinPoolBound)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "progress-concurrent", MakeTopology());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "progress-concurrent");
    constexpr size_t poolBound = 4;
    builder.SetProgressThreads(poolBound);
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());
    ASSERT_TRUE(TopologyEngineTestPeer::HasProgressPool(*engine));

    // Submit more completions than the pool bound so every pool thread must pile up inside the interceptor; the
    // observed in-flight high-water mark then proves the pool scales up to the bound and never dispatches past it.
    constexpr size_t completionCount = 8;
    auto keys = MakeKeys("progress-concurrent");
    const std::string migratePrefix = keys->MigrateTaskTable() + "/";
    // The barrier only counts and blocks migrate-task reads driven by completion revalidation; engine background
    // reads use other keys and pass through unblocked.
    std::mutex mutex;
    std::condition_variable arrivedCv;
    std::condition_variable releaseCv;
    size_t entered = 0;
    size_t exited = 0;
    size_t maxInFlight = 0;
    bool release = false;
    proxy.SetRangeEntryInterceptor([&](const std::string &key) {
        if (key.rfind(migratePrefix, 0) != 0) {
            return;
        }
        std::unique_lock<std::mutex> lock(mutex);
        ++entered;
        maxInFlight = std::max(maxInFlight, entered - exited);
        arrivedCv.notify_all();
        releaseCv.wait_for(lock, std::chrono::seconds(2), [&] { return release; });
        ++exited;
    });
    for (size_t index = 0; index < completionCount; ++index) {
        DS_ASSERT_OK(TopologyEngineTestPeer::SubmitProgressCompletion(
            *engine, MakeUnmatchedTaskCompletion(index)));
    }
    {
        std::unique_lock<std::mutex> lock(mutex);
        // With a serial inline consumer the second task read could never arrive while the first one blocks, and an
        // unbounded dispatcher would exceed the bound; wait until the whole pool is inside the barrier.
        EXPECT_TRUE(arrivedCv.wait_for(lock, TEST_WAIT, [&] { return entered >= poolBound; }));
        release = true;
    }
    releaseCv.notify_all();
    ASSERT_TRUE(WaitFor([&] { return TopologyEngineTestPeer::ExecutorStaleCount(*engine) == completionCount; }));
    {
        std::lock_guard<std::mutex> lock(mutex);
        EXPECT_EQ(entered, completionCount);
        // A scheduler-jitter environment may relax this to >= 2 (concurrency exists) and <= poolBound (bounded).
        EXPECT_EQ(maxInFlight, poolBound);
    }
    proxy.SetRangeEntryInterceptor(nullptr);
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
}

TEST(TopologyEngineTest, ProgressDoorbellWakesLoopWithoutBackendRoundTrip)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "progress-doorbell", MakeTopology());
    auto engine = BuildEngine(proxy, ingress, callbacks, "progress-doorbell");
    DS_ASSERT_OK(engine->Start());

    std::atomic<size_t> rangeCalls{ 0 };
    proxy.SetRangeEntryInterceptor([&](const std::string &) { rangeCalls.fetch_add(1); });
    const auto before = rangeCalls.load();
    DS_ASSERT_OK(TopologyEngineTestPeer::HandleRuntimeEvent(
        *engine, { CoordinationEventType::PUT, "topology/progress-doorbell", "", 0, 0 }));
    EXPECT_EQ(rangeCalls.load(), before);
    proxy.SetRangeEntryInterceptor(nullptr);

    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
}

TEST(TopologyEngineTest, ProgressPoolDisabledMatchesLegacyBehavior)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "progress-legacy", MakeTopology());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "progress-legacy");
    builder.SetProgressThreads(0);
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());
    ASSERT_FALSE(TopologyEngineTestPeer::HasProgressPool(*engine));

    DS_ASSERT_OK(TopologyEngineTestPeer::SubmitProgressCompletion(*engine, MakeUnmatchedTaskCompletion(0)));
    ASSERT_TRUE(WaitFor([&] { return TopologyEngineTestPeer::ExecutorStaleCount(*engine) == 1U; }));
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
}

TEST(TopologyEngineTest, ShutdownDrainsProgressPoolWithoutCrash)
{
    testing::FakeCoordinatorServiceProxy proxy;
    TestWatchIngress ingress;
    NoopTopologyCallbacks callbacks;
    PutTopology(proxy, "progress-drain", MakeTopology());
    TopologyEngine::Builder builder;
    ConfigureBuilder(builder, proxy, ingress, callbacks, "progress-drain");
    builder.SetProgressThreads(4);
    std::unique_ptr<TopologyEngine> engine;
    DS_ASSERT_OK(builder.Build(engine));
    DS_ASSERT_OK(engine->Start());

    constexpr size_t completionCount = 4;
    auto keys = MakeKeys("progress-drain");
    const std::string migratePrefix = keys->MigrateTaskTable() + "/";
    std::mutex mutex;
    std::condition_variable arrivedCv;
    std::condition_variable releaseCv;
    size_t arrived = 0;
    bool release = false;
    proxy.SetRangeEntryInterceptor([&](const std::string &key) {
        if (key.rfind(migratePrefix, 0) != 0) {
            return;
        }
        std::unique_lock<std::mutex> lock(mutex);
        ++arrived;
        arrivedCv.notify_all();
        releaseCv.wait_for(lock, std::chrono::seconds(1), [&] { return release; });
    });
    for (size_t index = 0; index < completionCount; ++index) {
        DS_ASSERT_OK(TopologyEngineTestPeer::SubmitProgressCompletion(
            *engine, MakeUnmatchedTaskCompletion(index)));
    }
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(arrivedCv.wait_for(lock, TEST_WAIT, [&] { return arrived >= 1U; }));
    }
    // Shutdown must join the pool while revalidation work is still blocked inside the interceptor.
    DS_ASSERT_OK(engine->Shutdown(std::chrono::steady_clock::now() + TEST_WAIT + TEST_WAIT));
    EXPECT_EQ(engine->GetState(), TopologyEngineState::STOPPED);
}

}  // namespace
}  // namespace datasystem::cluster
