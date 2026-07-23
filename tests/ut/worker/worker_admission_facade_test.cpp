/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker admission facade tests.
 */
#include "datasystem/worker/runtime/worker_admission_facade.h"

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

TEST(WorkerAdmissionFacadeTest, GuardHoldsReadWindowForSynchronousRpc)
{
    WorkerRuntimeState state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    WorkerAdmissionFacade facade(state);

    std::optional<WorkerRuntimeStateReadGuard> guard;
    ASSERT_TRUE(facade.AcquireGuard(WorkerAdmissionKind::NORMAL_WRITE, "Create", guard).IsOk());
    ASSERT_TRUE(guard.has_value());

    auto transition = std::async(std::launch::async, [&state] {
        state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local");
    });
    EXPECT_EQ(transition.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    guard.reset();
    EXPECT_EQ(transition.wait_for(std::chrono::seconds(1)), std::future_status::ready);
}

TEST(WorkerAdmissionFacadeTest, TransitionPendingFallbackKeepsControlPlaneOpen)
{
    WorkerRuntimeState state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    WorkerAdmissionFacade facade(state);

    std::optional<WorkerRuntimeStateReadGuard> writeGuard;
    ASSERT_TRUE(facade.AcquireGuard(WorkerAdmissionKind::NORMAL_WRITE, "Create", writeGuard).IsOk());
    ASSERT_TRUE(writeGuard.has_value());

    auto transition = std::async(std::launch::async, [&state] {
        state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local");
    });
    ASSERT_EQ(transition.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);

    for (auto kind :
         { WorkerAdmissionKind::DIAGNOSTIC_RPC, WorkerAdmissionKind::CLEANUP_RPC, WorkerAdmissionKind::RECOVERY_RPC }) {
        std::optional<WorkerRuntimeStateReadGuard> controlGuard;
        EXPECT_TRUE(facade.AcquireGuard(kind, "control", controlGuard).IsOk());
        EXPECT_FALSE(controlGuard.has_value());
    }

    std::optional<WorkerRuntimeStateReadGuard> normalGuard;
    EXPECT_EQ(facade.AcquireGuard(WorkerAdmissionKind::NORMAL_READ, "Get", normalGuard).GetCode(), K_NOT_READY);
    writeGuard.reset();
    EXPECT_EQ(transition.wait_for(std::chrono::seconds(1)), std::future_status::ready);
}
}  // namespace
}  // namespace datasystem::worker
