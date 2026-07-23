/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime state manager tests.
 */
#include "datasystem/worker/runtime/worker_runtime_state.h"

#include <chrono>
#include <future>
#include <optional>

#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
WorkerRunningEvidence CompleteEvidence()
{
    WorkerRunningEvidence evidence;
    evidence.membershipReady = true;
    evidence.topologyReady = true;
    evidence.resourceReady = true;
    return evidence;
}

TEST(WorkerRuntimeStateTest, RequiresServingEvidenceBeforeRunning)
{
    WorkerRuntimeState state;
    auto evidence = CompleteEvidence();
    evidence.membershipReady = false;

    EXPECT_FALSE(state.TryMarkRunning(evidence, "membership pending"));
    EXPECT_EQ(state.GetSnapshot().mode, WorkerServiceMode::RECOVERING);

    evidence.membershipReady = true;
    EXPECT_TRUE(state.TryMarkRunning(evidence, "ready"));
    EXPECT_EQ(state.GetSnapshot().mode, WorkerServiceMode::RUNNING);
}

TEST(WorkerRuntimeStateTest, LocalIsolationClosesRunningUntilRecoveryStarts)
{
    WorkerRuntimeState state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));

    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    EXPECT_EQ(state.GetSnapshot().mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_FALSE(state.TryMarkRunning(CompleteEvidence(), "stale evidence"));

    state.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "backend recovered");
    EXPECT_TRUE(state.TryMarkRunning(CompleteEvidence(), "fresh evidence"));
    EXPECT_EQ(state.GetSnapshot().mode, WorkerServiceMode::RUNNING);
}

TEST(WorkerRuntimeStateTest, ReadGuardLinearizesIsolationTransition)
{
    WorkerRuntimeState state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    std::optional<WorkerRuntimeStateReadGuard> guard(state.TryAcquireReadGuard());
    ASSERT_TRUE(guard.has_value());

    auto isolation = std::async(std::launch::async, [&state] {
        state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    });
    EXPECT_EQ(isolation.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    EXPECT_FALSE(state.TryAcquireReadGuard().has_value());

    guard.reset();
    EXPECT_EQ(isolation.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(state.GetSnapshot().mode, WorkerServiceMode::LOCAL_ISOLATED);
}
}  // namespace
}  // namespace datasystem::worker
