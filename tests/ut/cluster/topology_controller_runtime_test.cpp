/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Single-cluster topology Controller Runtime composition tests.
 */
#include "datasystem/cluster/control/topology_controller_runtime.h"

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/common/util/thread.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr size_t TEST_EVENT_QUEUE_CAPACITY = 32;
constexpr auto TEST_WAIT = std::chrono::seconds(1);
constexpr auto TEST_LONG_WAIT = std::chrono::seconds(3);
constexpr auto TEST_RECONCILE_TICK = std::chrono::milliseconds(10);
constexpr auto TEST_SLOW_RECONCILE_TICK = std::chrono::hours(1);
constexpr auto TEST_JANITOR_INTERVAL = std::chrono::seconds(1);
constexpr auto INVALID_JANITOR_INTERVAL = std::chrono::seconds(0);
constexpr uint64_t TEST_TOPOLOGY_VERSION = 1;
constexpr size_t EXPECTED_EVENT_SOURCE_SHUTDOWN_CALLS = 1;
constexpr size_t EXPECTED_FULL_SHUTDOWN_CALLS = 0;
constexpr size_t EXPECTED_NO_GET_ATTEMPTS = 0;
constexpr size_t EXPECTED_INITIAL_COMPONENT_GET_ATTEMPTS = 2;

class InstrumentedBackend final : public ICoordinationBackend {
public:
    InstrumentedBackend() = default;
    ~InstrumentedBackend() override = default;

    Status GetAll(const std::string &table, std::vector<std::pair<std::string, std::string>> &values) override
    {
        return backend_.GetAll(table, values);
    }

    Status Get(const std::string &table, const std::string &key, std::string &value) override
    {
        return backend_.Get(table, key, value);
    }

    Status Get(const std::string &table, const std::string &key, RangeSearchResult &result, int32_t timeoutMs) override
    {
        return backend_.Get(table, key, result, timeoutMs);
    }

    Status CAS(const std::string &table, const std::string &key, const ProcessFunction &process,
               RangeSearchResult &result) override
    {
        return backend_.CAS(table, key, process, result);
    }

    Status CAS(const std::string &table, const std::string &key, const ProcessFunction &process) override
    {
        return backend_.CAS(table, key, process);
    }

    Status CAS(const std::string &table, const std::string &key, const std::string &oldValue,
               const std::string &newValue) override
    {
        return backend_.CAS(table, key, oldValue, newValue);
    }

    Status Delete(const std::string &table, const std::string &key) override
    {
        return backend_.Delete(table, key);
    }

    Status Delete(const std::string &table, const std::string &key, int timeoutMs) override
    {
        return backend_.Delete(table, key, timeoutMs);
    }

    Status WatchEvents(const std::vector<WatchKey> &watchKeys) override
    {
        return backend_.WatchEvents(watchKeys);
    }

    Status InitKeepAlive(const std::string &table, const std::string &key, bool isRestart,
                         bool isStoreAvailableWhenStart) override
    {
        return backend_.InitKeepAlive(table, key, isRestart, isStoreAvailableWhenStart);
    }

    Status ShutdownEventSources() override
    {
        bool failShutdown = false;
        {
            std::lock_guard<std::mutex> lock(callMutex_);
            ++shutdownEventSourceCalls_;
            failShutdown = failNextEventSourceShutdown_;
            failNextEventSourceShutdown_ = false;
        }
        callCv_.notify_all();
        const auto status = backend_.ShutdownEventSources();
        if (status.IsError()) {
            return status;
        }
        if (failShutdown) {
            return Status(K_RPC_UNAVAILABLE, "injected event-source shutdown failure");
        }
        return Status::OK();
    }

    Status Shutdown() override
    {
        std::lock_guard<std::mutex> lock(callMutex_);
        ++fullShutdownCalls_;
        return backend_.Shutdown();
    }

    Status UpdateNodeState(MemberLifecycleState state) override
    {
        return backend_.UpdateNodeState(state);
    }

    Status GetStorePrefix(const std::string &table, std::string &prefix) override
    {
        return backend_.GetStorePrefix(table, prefix);
    }

    Status InformReconciliationDone(const HostPort &address) override
    {
        return backend_.InformReconciliationDone(address);
    }

    bool IsKeepAliveTimeout() override
    {
        return backend_.IsKeepAliveTimeout();
    }

    bool IsFirstKeepAliveSent() override
    {
        return backend_.IsFirstKeepAliveSent();
    }

    void SetEventHandler(EventHandler &&handler) override
    {
        backend_.SetEventHandler(std::move(handler));
    }

    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override
    {
        backend_.SetCheckStoreStateWhenNetworkFailedHandler(std::move(handler));
    }

    FakeCoordinationBackend &Backend()
    {
        return backend_;
    }

    size_t ShutdownEventSourceCalls() const
    {
        std::lock_guard<std::mutex> lock(callMutex_);
        return shutdownEventSourceCalls_;
    }

    size_t FullShutdownCalls() const
    {
        std::lock_guard<std::mutex> lock(callMutex_);
        return fullShutdownCalls_;
    }

    bool WaitForShutdownEventSourceCalls(size_t expected, std::chrono::steady_clock::time_point deadline)
    {
        std::unique_lock<std::mutex> lock(callMutex_);
        return callCv_.wait_until(lock, deadline, [this, expected] { return shutdownEventSourceCalls_ >= expected; });
    }

    void FailNextShutdownEventSources()
    {
        std::lock_guard<std::mutex> lock(callMutex_);
        failNextEventSourceShutdown_ = true;
    }

private:
    FakeCoordinationBackend backend_;
    // Protects shutdownEventSourceCalls_, fullShutdownCalls_, and failNextEventSourceShutdown_.
    mutable std::mutex callMutex_;
    std::condition_variable callCv_;
    size_t shutdownEventSourceCalls_{ 0 };
    size_t fullShutdownCalls_{ 0 };
    bool failNextEventSourceShutdown_{ false };
};

TopologyControllerRuntime::Options MakeOptions(std::string clusterName)
{
    TopologyControllerRuntime::Options options;
    options.clusterName = std::move(clusterName);
    options.eventQueueCapacity = TEST_EVENT_QUEUE_CAPACITY;
    options.controller.reconcileTick = TEST_RECONCILE_TICK;
    return options;
}

void PutEmptyTopology(InstrumentedBackend &backend, const std::string &clusterName)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create(clusterName, keys));
    TopologyState state;
    state.version = TEST_TOPOLOGY_VERSION;
    state.clusterHasInit = true;
    backend.Backend().PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
}

bool WaitForBlockedGetOrRelease(FakeCoordinationBackend &backend, std::chrono::steady_clock::time_point deadline)
{
    const bool blocked = backend.WaitUntilGetBlocked(deadline);
    if (!blocked) {
        backend.ReleaseBlockedGet();
    }
    return blocked;
}

TEST(TopologyControllerRuntimeTest, ValidatesOptionsAndKeepsOutputUnchanged)
{
    InstrumentedBackend backend;
    HashAlgorithm algorithm;
    std::unique_ptr<TopologyControllerRuntime> runtime;
    DS_ASSERT_OK(TopologyControllerRuntime::Create(MakeOptions("valid"), backend, algorithm, runtime));
    auto *const original = runtime.get();

    auto invalidCluster = MakeOptions("invalid/cluster");
    EXPECT_EQ(TopologyControllerRuntime::Create(std::move(invalidCluster), backend, algorithm, runtime).GetCode(),
              K_INVALID);
    EXPECT_EQ(runtime.get(), original);

    auto zeroCapacity = MakeOptions("valid");
    zeroCapacity.eventQueueCapacity = 0;
    EXPECT_EQ(TopologyControllerRuntime::Create(std::move(zeroCapacity), backend, algorithm, runtime).GetCode(),
              K_INVALID);
    EXPECT_EQ(runtime.get(), original);

    auto invalidController = MakeOptions("valid");
    invalidController.controller.nodeDeadTimeout = std::chrono::seconds(0);
    EXPECT_EQ(TopologyControllerRuntime::Create(std::move(invalidController), backend, algorithm, runtime).GetCode(),
              K_INVALID);
    EXPECT_EQ(runtime.get(), original);

    auto invalidJanitor = MakeOptions("valid");
    invalidJanitor.janitor = TopologyTaskJanitorOptions{};
    invalidJanitor.janitor->deleteBatch = invalidJanitor.janitor->scanLimit + 1;
    EXPECT_EQ(TopologyControllerRuntime::Create(std::move(invalidJanitor), backend, algorithm, runtime).GetCode(),
              K_INVALID);
    EXPECT_EQ(runtime.get(), original);

    std::unique_ptr<TopologyControllerRuntime> unscoped;
    DS_ASSERT_OK(TopologyControllerRuntime::Create(MakeOptions(""), backend, algorithm, unscoped));
}

TEST(TopologyControllerRuntimeTest, DefaultsJanitorOffAndSupportsExplicitJanitor)
{
    auto defaultOptions = MakeOptions("default-no-janitor");
    EXPECT_FALSE(defaultOptions.janitor.has_value());
    InstrumentedBackend defaultBackend;
    HashAlgorithm algorithm;
    PutEmptyTopology(defaultBackend, defaultOptions.clusterName);
    std::unique_ptr<TopologyControllerRuntime> defaultRuntime;
    DS_ASSERT_OK(
        TopologyControllerRuntime::Create(std::move(defaultOptions), defaultBackend, algorithm, defaultRuntime));
    DS_ASSERT_OK(defaultRuntime->Start());
    EXPECT_EQ(defaultRuntime->Start().GetCode(), K_INVALID);
    DS_ASSERT_OK(defaultRuntime->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(defaultBackend.ShutdownEventSourceCalls(), EXPECTED_EVENT_SOURCE_SHUTDOWN_CALLS);
    EXPECT_EQ(defaultBackend.FullShutdownCalls(), EXPECTED_FULL_SHUTDOWN_CALLS);

    auto janitorOptions = MakeOptions("explicit-janitor");
    janitorOptions.janitor = TopologyTaskJanitorOptions{};
    InstrumentedBackend janitorBackend;
    PutEmptyTopology(janitorBackend, janitorOptions.clusterName);
    std::unique_ptr<TopologyControllerRuntime> janitorRuntime;
    DS_ASSERT_OK(
        TopologyControllerRuntime::Create(std::move(janitorOptions), janitorBackend, algorithm, janitorRuntime));
    DS_ASSERT_OK(janitorRuntime->Start());
    DS_ASSERT_OK(janitorRuntime->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(janitorBackend.ShutdownEventSourceCalls(), EXPECTED_EVENT_SOURCE_SHUTDOWN_CALLS);
    EXPECT_EQ(janitorBackend.FullShutdownCalls(), EXPECTED_FULL_SHUTDOWN_CALLS);
}

TEST(TopologyControllerRuntimeTest, ControllerStartFailureDoesNotStartJanitorOrFullShutdown)
{
    InstrumentedBackend backend;
    HashAlgorithm algorithm;
    auto options = MakeOptions("controller-start-failure");
    options.janitor = TopologyTaskJanitorOptions{};
    std::unique_ptr<TopologyControllerRuntime> runtime;
    DS_ASSERT_OK(TopologyControllerRuntime::Create(std::move(options), backend, algorithm, runtime));
    backend.Backend().FailNextWatch();

    EXPECT_EQ(runtime->Start().GetCode(), K_RPC_UNAVAILABLE);
    EXPECT_EQ(backend.Backend().GetAttemptCount(), EXPECTED_NO_GET_ATTEMPTS);
    EXPECT_FALSE(backend.Backend().HasEventHandler());
    EXPECT_EQ(backend.ShutdownEventSourceCalls(), EXPECTED_EVENT_SOURCE_SHUTDOWN_CALLS);
    EXPECT_EQ(backend.FullShutdownCalls(), EXPECTED_FULL_SHUTDOWN_CALLS);
    DS_ASSERT_OK(runtime->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

TEST(TopologyControllerRuntimeTest, InvalidJanitorOptionsDoNotStartController)
{
    InstrumentedBackend backend;
    HashAlgorithm algorithm;
    auto options = MakeOptions("invalid-janitor-options");
    options.janitor = TopologyTaskJanitorOptions{};
    options.janitor->interval = INVALID_JANITOR_INTERVAL;
    std::unique_ptr<TopologyControllerRuntime> runtime;

    EXPECT_EQ(TopologyControllerRuntime::Create(std::move(options), backend, algorithm, runtime).GetCode(), K_INVALID);
    EXPECT_EQ(runtime, nullptr);
    EXPECT_FALSE(backend.Backend().HasEventHandler());
    EXPECT_EQ(backend.ShutdownEventSourceCalls(), 0U);
    EXPECT_EQ(backend.FullShutdownCalls(), EXPECTED_FULL_SHUTDOWN_CALLS);
}

TEST(TopologyControllerRuntimeTest, StopTimeoutRetainsDependenciesAndSecondStopConverges)
{
    InstrumentedBackend backend;
    HashAlgorithm algorithm;
    auto options = MakeOptions("stop-timeout");
    PutEmptyTopology(backend, options.clusterName);
    std::unique_ptr<TopologyControllerRuntime> runtime;
    DS_ASSERT_OK(TopologyControllerRuntime::Create(std::move(options), backend, algorithm, runtime));
    backend.Backend().BlockNextGet();
    DS_ASSERT_OK(runtime->Start());
    ASSERT_TRUE(WaitForBlockedGetOrRelease(backend.Backend(), std::chrono::steady_clock::now() + TEST_WAIT));

    EXPECT_EQ(runtime->Stop(std::chrono::steady_clock::now()).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(backend.ShutdownEventSourceCalls(), EXPECTED_EVENT_SOURCE_SHUTDOWN_CALLS);
    EXPECT_EQ(backend.FullShutdownCalls(), EXPECTED_FULL_SHUTDOWN_CALLS);
    backend.Backend().ReleaseBlockedGet();
    DS_ASSERT_OK(runtime->Stop(std::chrono::steady_clock::now() + TEST_WAIT));

    std::string topology;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("stop-timeout", keys));
    DS_ASSERT_OK(backend.Get(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology));
    EXPECT_EQ(backend.FullShutdownCalls(), EXPECTED_FULL_SHUTDOWN_CALLS);
}

TEST(TopologyControllerRuntimeTest, StopContinuesAfterJanitorTimeoutAndPreservesFirstError)
{
    InstrumentedBackend backend;
    HashAlgorithm algorithm;
    auto options = MakeOptions("stop-first-error");
    options.controller.reconcileTick = TEST_SLOW_RECONCILE_TICK;
    options.janitor = TopologyTaskJanitorOptions{};
    options.janitor->interval = TEST_JANITOR_INTERVAL;
    PutEmptyTopology(backend, options.clusterName);
    std::unique_ptr<TopologyControllerRuntime> runtime;
    DS_ASSERT_OK(TopologyControllerRuntime::Create(std::move(options), backend, algorithm, runtime));
    DS_ASSERT_OK(runtime->Start());
    ASSERT_TRUE(backend.Backend().WaitForGetAttempts(EXPECTED_INITIAL_COMPONENT_GET_ATTEMPTS,
                                                     std::chrono::steady_clock::now() + TEST_LONG_WAIT));
    backend.Backend().BlockNextGet();
    ASSERT_TRUE(WaitForBlockedGetOrRelease(backend.Backend(), std::chrono::steady_clock::now() + TEST_LONG_WAIT));
    backend.FailNextShutdownEventSources();

    EXPECT_EQ(runtime->Stop(std::chrono::steady_clock::now()).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(backend.ShutdownEventSourceCalls(), EXPECTED_EVENT_SOURCE_SHUTDOWN_CALLS);
    backend.Backend().ReleaseBlockedGet();
    DS_ASSERT_OK(runtime->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
    EXPECT_EQ(backend.FullShutdownCalls(), EXPECTED_FULL_SHUTDOWN_CALLS);
}

TEST(TopologyControllerRuntimeTest, ConcurrentLifecycleOperationReturnsTryAgain)
{
    InstrumentedBackend backend;
    HashAlgorithm algorithm;
    auto options = MakeOptions("concurrent-stop");
    PutEmptyTopology(backend, options.clusterName);
    std::unique_ptr<TopologyControllerRuntime> runtime;
    DS_ASSERT_OK(TopologyControllerRuntime::Create(std::move(options), backend, algorithm, runtime));
    backend.Backend().BlockNextGet();
    DS_ASSERT_OK(runtime->Start());
    ASSERT_TRUE(WaitForBlockedGetOrRelease(backend.Backend(), std::chrono::steady_clock::now() + TEST_WAIT));

    Status firstStop;
    Thread stopThread(
        [&runtime, &firstStop] { firstStop = runtime->Stop(std::chrono::steady_clock::now() + TEST_WAIT); });
    const bool stopStarted = backend.WaitForShutdownEventSourceCalls(EXPECTED_EVENT_SOURCE_SHUTDOWN_CALLS,
                                                                     std::chrono::steady_clock::now() + TEST_WAIT);
    if (stopStarted) {
        EXPECT_EQ(runtime->Stop(std::chrono::steady_clock::now() + TEST_WAIT).GetCode(), K_TRY_AGAIN);
    }
    backend.Backend().ReleaseBlockedGet();
    stopThread.join();
    ASSERT_TRUE(stopStarted);
    DS_ASSERT_OK(firstStop);
}

TEST(TopologyControllerRuntimeTest, KeepsClusterWatchPlansIsolated)
{
    InstrumentedBackend firstBackend;
    InstrumentedBackend secondBackend;
    HashAlgorithm algorithm;
    PutEmptyTopology(firstBackend, "runtime-a");
    PutEmptyTopology(secondBackend, "runtime-b");
    std::unique_ptr<TopologyControllerRuntime> first;
    std::unique_ptr<TopologyControllerRuntime> second;
    DS_ASSERT_OK(TopologyControllerRuntime::Create(MakeOptions("runtime-a"), firstBackend, algorithm, first));
    DS_ASSERT_OK(TopologyControllerRuntime::Create(MakeOptions("runtime-b"), secondBackend, algorithm, second));

    DS_ASSERT_OK(first->Start());
    DS_ASSERT_OK(second->Start());
    const auto firstWatches = firstBackend.Backend().WatchKeys();
    const auto secondWatches = secondBackend.Backend().WatchKeys();
    ASSERT_FALSE(firstWatches.empty());
    ASSERT_EQ(firstWatches.size(), secondWatches.size());
    for (const auto &watch : firstWatches) {
        EXPECT_NE(watch.tableName.find("/runtime-a/"), std::string::npos);
    }
    for (const auto &watch : secondWatches) {
        EXPECT_NE(watch.tableName.find("/runtime-b/"), std::string::npos);
    }
    DS_ASSERT_OK(first->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
    DS_ASSERT_OK(second->Stop(std::chrono::steady_clock::now() + TEST_WAIT));
}

}  // namespace
}  // namespace datasystem::cluster
