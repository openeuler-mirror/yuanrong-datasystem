/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker service admission tests.
 */
#include "datasystem/worker/runtime/worker_service_admission.h"

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

TEST(WorkerServiceAdmissionTest, RejectsOrdinaryTrafficWhileLocalIsolated)
{
    WorkerRuntimeState state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    WorkerServiceAdmission admission(state);

    auto rc = admission.Check(WorkerAdmissionKind::NORMAL_WRITE, "Create");
    EXPECT_EQ(rc.GetCode(), K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("mode=LOCAL_ISOLATED"), std::string::npos);
    EXPECT_TRUE(admission.Check(WorkerAdmissionKind::CLEANUP_RPC, "Cleanup").IsOk());
    EXPECT_TRUE(admission.Check(WorkerAdmissionKind::DIAGNOSTIC_RPC, "Inspect").IsOk());
}

TEST(WorkerServiceAdmissionTest, AppliesBasicModeMatrix)
{
    WorkerRuntimeState running;
    ASSERT_TRUE(running.TryMarkRunning(CompleteEvidence(), "ready"));
    WorkerServiceAdmission runningAdmission(running);
    EXPECT_TRUE(runningAdmission.Check(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk());
    EXPECT_TRUE(runningAdmission.Check(WorkerAdmissionKind::NORMAL_WRITE, "Create").IsOk());

    WorkerRuntimeState recovering;
    recovering.MarkRecovering(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "recovering");
    WorkerServiceAdmission recoveringAdmission(recovering);
    EXPECT_EQ(recoveringAdmission.Check(WorkerAdmissionKind::NORMAL_READ, "Get").GetCode(), K_NOT_READY);
    EXPECT_TRUE(recoveringAdmission.Check(WorkerAdmissionKind::RECOVERY_RPC, "Recover").IsOk());

    WorkerRuntimeState oom;
    ASSERT_TRUE(oom.TryMarkRunning(CompleteEvidence(), "ready"));
    oom.MarkOutOfMemory("oom");
    WorkerServiceAdmission oomAdmission(oom);
    EXPECT_TRUE(oomAdmission.Check(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk());
    EXPECT_EQ(oomAdmission.Check(WorkerAdmissionKind::NORMAL_WRITE, "Create").GetCode(), K_OUT_OF_MEMORY);
}

TEST(WorkerServiceAdmissionTest, TransitionPendingKeepsControlPlaneOpen)
{
    WorkerRuntimeState state;
    ASSERT_TRUE(state.TryMarkRunning(CompleteEvidence(), "ready"));
    auto guard = state.TryAcquireReadGuard();
    ASSERT_TRUE(guard.has_value());
    WorkerServiceAdmission admission(state);

    auto isolation = std::async(std::launch::async, [&state] {
        state.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "keepalive failed");
    });
    ASSERT_EQ(isolation.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);

    EXPECT_EQ(admission.Check(WorkerAdmissionKind::NORMAL_WRITE, "Create").GetCode(), K_NOT_READY);
    EXPECT_TRUE(admission.Check(WorkerAdmissionKind::RECOVERY_RPC, "Recover").IsOk());
    EXPECT_TRUE(admission.Check(WorkerAdmissionKind::CLEANUP_RPC, "Cleanup").IsOk());
    EXPECT_TRUE(admission.Check(WorkerAdmissionKind::DIAGNOSTIC_RPC, "Inspect").IsOk());

    guard.reset();
    EXPECT_EQ(isolation.wait_for(std::chrono::seconds(1)), std::future_status::ready);
}
}  // namespace
}  // namespace datasystem::worker
