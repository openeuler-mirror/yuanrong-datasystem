/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cluster topology bounded Shutdown contract tests.
 */
#include "datasystem/cluster/control/topology_task_janitor.h"

#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/cluster/control/topology_controller.h"
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {
constexpr auto SHUTDOWN_WAIT = std::chrono::seconds(1);

TEST(TopologyShutdownTest, JanitorTimeoutKeepsDependenciesAliveAndLaterJoins)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("janitor-stop", keys));
    TopologyState topology;
    topology.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    TopologyTaskJanitorOptions options;
    options.interval = std::chrono::seconds(1);
    TopologyTaskJanitor janitor(repository, algorithm, materializer, options);
    backend.BlockNextGet();
    DS_ASSERT_OK(janitor.Start());
    ASSERT_TRUE(backend.WaitUntilGetBlocked(std::chrono::steady_clock::now() + SHUTDOWN_WAIT));
    EXPECT_EQ(janitor.Stop(std::chrono::steady_clock::now()).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    backend.ReleaseBlockedGet();
    DS_ASSERT_OK(janitor.Stop(std::chrono::steady_clock::now() + SHUTDOWN_WAIT));
    std::string bytes;
    DS_ASSERT_OK(backend.Get(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), bytes));
}

TEST(TopologyShutdownTest, ControllerTimeoutKeepsDependenciesAliveAndLaterJoins)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("controller-stop", keys));
    TopologyState topology;
    topology.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    TopologyControllerOptions options;
    options.reconcileTick = std::chrono::milliseconds(1);
    TopologyController controller(backend, repository, *keys, algorithm, dispatcher, options);
    backend.BlockNextGet();
    DS_ASSERT_OK(controller.Start());
    const bool blocked = backend.WaitUntilGetBlocked(std::chrono::steady_clock::now() + SHUTDOWN_WAIT);
    EXPECT_TRUE(blocked);
    if (!blocked) {
        backend.ReleaseBlockedGet();
        DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + SHUTDOWN_WAIT));
        return;
    }
    EXPECT_EQ(controller.Stop(std::chrono::steady_clock::now()).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    backend.ReleaseBlockedGet();
    DS_ASSERT_OK(controller.Stop(std::chrono::steady_clock::now() + SHUTDOWN_WAIT));
    std::string bytes;
    DS_ASSERT_OK(backend.Get(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), bytes));
}

TEST(TopologyShutdownTest, StartedJanitorIsSafelyStoppedByDestructor)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("janitor-destructor", keys));
    TopologyState topology;
    topology.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    TopologyTaskMaterializer materializer;
    {
        TopologyTaskJanitor janitor(repository, algorithm, materializer, {});
        DS_ASSERT_OK(janitor.Start());
    }
}

TEST(TopologyShutdownTest, StartedControllerIsSafelyStoppedByDestructor)
{
    FakeCoordinationBackend backend;
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("controller-destructor", keys));
    TopologyState topology;
    topology.version = 1;
    backend.PutRaw(keys->TopologyTable(), TopologyKeyHelper::TopologyKey(), topology);
    TopologyRepository repository(backend, *keys);
    HashAlgorithm algorithm;
    CoordinationEventDispatcher dispatcher(32);
    {
        TopologyController controller(backend, repository, *keys, algorithm, dispatcher, {});
        DS_ASSERT_OK(controller.Start());
    }
    EXPECT_FALSE(backend.HasEventHandler());
}

}  // namespace
}  // namespace datasystem::cluster
