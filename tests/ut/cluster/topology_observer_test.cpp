/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Standalone cluster topology Observer lifecycle tests.
 */
#include "datasystem/cluster/runtime/topology_observer.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include <thread>

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

TEST(TopologyObserverTest, PublishesInitialSnapshotAndJoinsWithoutClosingBackend)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("observer", keys));
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyObserver observer(reader, *keys);

    DS_ASSERT_OK(observer.Start());
    std::shared_ptr<const TopologySnapshot> snapshot;
    DS_ASSERT_OK(observer.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), 1);
    DS_ASSERT_OK(observer.Shutdown(std::chrono::steady_clock::now() + std::chrono::seconds(5)));
    DS_ASSERT_OK(observer.Shutdown(std::chrono::steady_clock::now()));
    std::string bytes;
    DS_ASSERT_OK(backend.Get(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), bytes));
}

TEST(TopologyObserverTest, FullRebuildsVersionGapAndPreservesLastGoodOnCorruption)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("observer", keys));
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyObserver observer(reader, *keys);
    DS_ASSERT_OK(observer.Start());
    EXPECT_TRUE(backend.WatchKeys().empty());

    state.version = 3;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    std::shared_ptr<const TopologySnapshot> snapshot;
    while (std::chrono::steady_clock::now() < deadline) {
        if (observer.GetSnapshot(snapshot).IsOk() && snapshot->Version() == 3) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->Version(), 3);

    backend.PutBytes(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), "corrupt");
    const auto failureDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < failureDeadline && observer.GetDiagnostics().readFailures == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GT(observer.GetDiagnostics().readFailures, 0);
    DS_ASSERT_OK(observer.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), 3);
    DS_ASSERT_OK(observer.Shutdown(std::chrono::steady_clock::now() + std::chrono::seconds(5)));
}

TEST(TopologyObserverTest, InitialReadFailureAllowsRetry)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("observer", keys));
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyObserver observer(reader, *keys);

    EXPECT_EQ(observer.Start().GetCode(), K_NOT_FOUND);
    EXPECT_FALSE(backend.HasEventHandler());
    EXPECT_FALSE(observer.GetDiagnostics().running);
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    DS_ASSERT_OK(observer.Start());
}

TEST(TopologyObserverTest, PollsForChangesWithoutAWatch)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("observer", keys));
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyObserver observer(reader, *keys);
    DS_ASSERT_OK(observer.Start());
    std::shared_ptr<const TopologySnapshot> initial;
    DS_ASSERT_OK(observer.GetSnapshot(initial));
    ASSERT_TRUE(backend.WaitForGetAttempts(2, std::chrono::steady_clock::now() + std::chrono::seconds(5)));
    std::shared_ptr<const TopologySnapshot> unchanged;
    DS_ASSERT_OK(observer.GetSnapshot(unchanged));
    EXPECT_EQ(unchanged, initial);

    state.version = 2;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    std::shared_ptr<const TopologySnapshot> snapshot;
    while (std::chrono::steady_clock::now() < deadline) {
        if (observer.GetSnapshot(snapshot).IsOk() && snapshot->Version() == 2) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->Version(), 2);
    DS_ASSERT_OK(observer.Shutdown(std::chrono::steady_clock::now() + std::chrono::seconds(5)));
}

TEST(TopologyObserverTest, ShutdownTimeoutKeepsDependenciesAliveAndLaterJoins)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("observer", keys));
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    TopologyObserver observer(reader, *keys);
    DS_ASSERT_OK(observer.Start());

    backend.BlockNextGet();
    auto blocked = backend.WaitUntilGetBlocked(std::chrono::steady_clock::now() + std::chrono::seconds(5));
    if (!blocked) {
        backend.ReleaseBlockedGet();
        FAIL() << "Background poll did not enter Get";
    }
    const auto reads = backend.GetAttemptCount();
    std::shared_ptr<const TopologySnapshot> snapshot;
    EXPECT_TRUE(observer.GetSnapshot(snapshot).IsOk());
    EXPECT_EQ(backend.GetAttemptCount(), reads);
    EXPECT_EQ(observer.Shutdown(std::chrono::steady_clock::now()).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_FALSE(backend.HasEventHandler());
    backend.ReleaseBlockedGet();
    DS_ASSERT_OK(observer.Shutdown(std::chrono::steady_clock::now() + std::chrono::seconds(5)));
}

TEST(TopologyObserverTest, StartedObserverIsSafelyStoppedByDestructor)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("observer-destructor", keys));
    TopologyState state;
    state.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), state);
    TopologyRepository repository(backend, *keys);
    TopologyReader reader(repository);
    {
        TopologyObserver observer(reader, *keys);
        DS_ASSERT_OK(observer.Start());
    }
    EXPECT_FALSE(backend.HasEventHandler());
}

}  // namespace
}  // namespace datasystem::cluster
