/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker runtime facade tests.
 */
#include "datasystem/worker/runtime/worker_runtime_facade.h"

#include <chrono>
#include <future>

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

TEST(WorkerRuntimeFacadeTest, AppliesBasicRuntimeStateToAdmission)
{
    WorkerRuntimeFacade runtime;

    runtime.MarkJoining("topology availability is not ready");
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").GetCode(), K_NOT_READY);

    EXPECT_TRUE(runtime.TryMarkRunning(CompleteEvidence(), "topology available"));
    EXPECT_TRUE(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").IsOk());

    runtime.MarkLocalIsolated(WorkerIsolationReason::TOPOLOGY_PASSIVE_SCALE_DOWN,
                              "topology availability is role-isolated");
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_WRITE, "Create").GetCode(), K_NOT_READY);
}

TEST(WorkerRuntimeFacadeTest, JoiningDoesNotOverrideLocalIsolation)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.TryMarkRunning(CompleteEvidence(), "topology available"));

    runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local isolation");
    runtime.MarkJoining("topology availability is not ready");

    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_EQ(runtime.CheckAdmission(WorkerAdmissionKind::NORMAL_READ, "Get").GetCode(), K_NOT_READY);
}

TEST(WorkerRuntimeFacadeTest, TransitionPendingFallbackKeepsDiagnosticsAvailable)
{
    WorkerRuntimeFacade runtime;
    ASSERT_TRUE(runtime.TryMarkRunning(CompleteEvidence(), "ready"));

    WorkerRuntimeFacade::AdmissionGuard writeGuard;
    ASSERT_TRUE(runtime.AcquireAdmissionGuard(WorkerAdmissionKind::NORMAL_WRITE, "Create", writeGuard).IsOk());

    auto transition = std::async(std::launch::async, [&runtime] {
        runtime.MarkLocalIsolated(WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local");
    });
    ASSERT_EQ(transition.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);

    WorkerRuntimeFacade::AdmissionGuard diagnosticGuard;
    EXPECT_TRUE(runtime.AcquireAdmissionGuard(WorkerAdmissionKind::DIAGNOSTIC_RPC, "Inspect", diagnosticGuard).IsOk());

    WorkerRuntimeFacade::AdmissionGuard normalGuard;
    EXPECT_EQ(runtime.AcquireAdmissionGuard(WorkerAdmissionKind::NORMAL_READ, "Get", normalGuard).GetCode(),
              K_NOT_READY);

    writeGuard = WorkerRuntimeFacade::AdmissionGuard();
    EXPECT_EQ(transition.wait_for(std::chrono::seconds(1)), std::future_status::ready);
}
}  // namespace
}  // namespace datasystem::worker
