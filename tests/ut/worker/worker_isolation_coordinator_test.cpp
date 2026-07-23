/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker isolation coordinator tests.
 */
#include "datasystem/worker/runtime/worker_isolation_coordinator.h"

#include <utility>

#include "gtest/gtest.h"

namespace datasystem::worker {
namespace {
TEST(WorkerIsolationCoordinatorTest, LocalIsolationClosesAdmissionAndKeepsProcessAlive)
{
    WorkerRuntimeFacade runtime;
    bool admissionOpen = true;
    WorkerIsolationCoordinatorHooks hooks;
    hooks.setTopologyServingAdmission = [&](bool open) { admissionOpen = open; };
    WorkerIsolationCoordinator coordinator(runtime, std::move(hooks));

    coordinator.OnLocalIsolation(Status(K_RUNTIME_ERROR, "keepalive timeout"));

    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::LOCAL_ISOLATED);
    EXPECT_FALSE(admissionOpen);
}

TEST(WorkerIsolationCoordinatorTest, LocalRecoveryKeepsAdmissionClosed)
{
    WorkerRuntimeFacade runtime;
    bool admissionOpen = true;
    WorkerIsolationCoordinatorHooks hooks;
    hooks.setTopologyServingAdmission = [&](bool open) { admissionOpen = open; };
    WorkerIsolationCoordinator coordinator(runtime, std::move(hooks));

    coordinator.OnLocalRecovery();

    EXPECT_EQ(runtime.GetSnapshot().mode, WorkerServiceMode::RECOVERING);
    EXPECT_FALSE(admissionOpen);
}
}  // namespace
}  // namespace datasystem::worker
