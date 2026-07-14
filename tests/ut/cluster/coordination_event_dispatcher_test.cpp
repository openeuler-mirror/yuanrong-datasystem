/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Bounded cluster runtime event ingress tests.
 */
#include "datasystem/cluster/runtime/coordination_event_dispatcher.h"
#include "datasystem/cluster/runtime/topology_role_watch_plan.h"

#include <algorithm>

#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

TEST(CoordinationEventDispatcherTest, CoalescesDoorbellsAndMarksOverflowForResync)
{
    CoordinationEventDispatcher dispatcher(1);
    DS_ASSERT_OK(dispatcher.Start());
    DS_ASSERT_OK(dispatcher.SubmitCoordination({ CoordinationEventType::PUT, "topology", "ignored", 1, 1 }));
    DS_ASSERT_OK(dispatcher.SubmitCoordination({ CoordinationEventType::PUT, "topology", "ignored", 2, 2 }));
    EXPECT_EQ(dispatcher.SubmitCoordination({ CoordinationEventType::PUT, "notify", "ignored", 1, 3 }).GetCode(),
              K_TRY_AGAIN);
    EXPECT_TRUE(dispatcher.ConsumeResyncRequired());
    EXPECT_FALSE(dispatcher.ConsumeResyncRequired());
    RuntimeEvent event;
    DS_ASSERT_OK(dispatcher.WaitPop(std::chrono::steady_clock::now(), event));
    const auto &doorbell = std::get<CoordinationEvent>(event.payload);
    EXPECT_EQ(doorbell.revision, 2);
    dispatcher.ShutdownIngress();
}

TEST(CoordinationEventDispatcherTest, CompletionOverflowDoesNotMasqueradeAsDoorbellResync)
{
    CoordinationEventDispatcher dispatcher(1);
    DS_ASSERT_OK(dispatcher.Start());
    DS_ASSERT_OK(dispatcher.SubmitCoordination({ CoordinationEventType::PUT, "topology", "ignored", 1, 1 }));
    TopologyCallbackCompletion completion;

    EXPECT_EQ(dispatcher.SubmitCompletion(std::move(completion)).GetCode(), K_TRY_AGAIN);
    EXPECT_FALSE(dispatcher.ConsumeResyncRequired());
    dispatcher.ShutdownIngress();
}

TEST(CoordinationEventDispatcherTest, RestartDropsEventsFromThePreviousExecution)
{
    CoordinationEventDispatcher dispatcher(2);
    DS_ASSERT_OK(dispatcher.Start());
    DS_ASSERT_OK(dispatcher.SubmitCoordination({ CoordinationEventType::PUT, "stale", "ignored", 1, 1 }));
    dispatcher.ShutdownIngress();

    DS_ASSERT_OK(dispatcher.Start());
    RuntimeEvent event;
    EXPECT_EQ(dispatcher.WaitPop(std::chrono::steady_clock::now(), event).GetCode(), K_RPC_DEADLINE_EXCEEDED);
    DS_ASSERT_OK(dispatcher.SubmitCoordination({ CoordinationEventType::PUT, "current", "ignored", 1, 2 }));
    DS_ASSERT_OK(dispatcher.WaitPop(std::chrono::steady_clock::now(), event));
    EXPECT_EQ(std::get<CoordinationEvent>(event.payload).key, "current");
    dispatcher.ShutdownIngress();
}

TEST(TopologyRoleWatchPlanTest, BuildsOnlyRoleRequiredExactAndPrefixWatches)
{
    std::unique_ptr<TopologyKeyHelper> keys;
    DS_ASSERT_OK(TopologyKeyHelper::Create("watch", keys));
    std::vector<WatchKey> watches;
    DS_ASSERT_OK(TopologyRoleWatchPlan::Build(TopologyRuntimeRole::WORKER, "127.0.0.1:1", *keys, 7, watches));
    ASSERT_EQ(watches.size(), 2);
    EXPECT_EQ(watches[0].tableName, keys->TopologyTable());
    EXPECT_EQ(watches[0].key, TopologyKeyHelper::TopologyKey());
    EXPECT_EQ(watches[1].tableName, keys->NotifyTable());
    EXPECT_EQ(watches[1].key, "127.0.0.1:1");
    DS_ASSERT_OK(TopologyRoleWatchPlan::Build(TopologyRuntimeRole::CONTROLLER, "", *keys, 7, watches));
    EXPECT_EQ(watches.size(), 4);
    EXPECT_TRUE(
        std::all_of(watches.begin() + 1, watches.end(), [](const WatchKey &watch) { return watch.key.empty(); }));
}

}  // namespace
}  // namespace datasystem::cluster
