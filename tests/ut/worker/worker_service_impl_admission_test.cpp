/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Worker common service runtime admission tests.
 */

#include "datasystem/worker/worker_service_impl.h"

#include "datasystem/worker/runtime/worker_runtime_facade.h"

#include <gtest/gtest.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace worker {
namespace {
TEST(WorkerServiceImplAdmissionTest, RegisterClientRejectsWhileWorkerIsStarting)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    std::atomic<bool> localExiting{ false };
    WorkerServiceImpl service(HostPort(), HostPort(), 1.0, nullptr, nullptr, "test-worker", membership, localExiting);
    WorkerRuntimeFacade runtime;
    service.SetRuntimeFacade(&runtime);
    RegisterClientReqPb req;
    req.set_version(DATASYSTEM_VERSION);
    RegisterClientRspPb rsp;

    auto rc = service.RegisterClient(req, rsp);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("RegisterClient"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("STARTING"), std::string::npos);
}

TEST(WorkerServiceImplAdmissionTest, GetSocketPathRejectsWhileWorkerIsStarting)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    std::atomic<bool> localExiting{ false };
    WorkerServiceImpl service(HostPort(), HostPort(), 1.0, nullptr, nullptr, "test-worker", membership, localExiting);
    WorkerRuntimeFacade runtime;
    service.SetRuntimeFacade(&runtime);
    GetSocketPathReqPb req;
    GetSocketPathRspPb rsp;

    auto rc = service.GetSocketPath(req, rsp);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("GetSocketPath"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("STARTING"), std::string::npos);
}

TEST(WorkerServiceImplAdmissionTest, GetSocketPathAllowsInternalJoiningWindow)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    std::atomic<bool> localExiting{ false };
    WorkerServiceImpl service(HostPort(), HostPort(), 1.0, nullptr, nullptr, "test-worker", membership, localExiting);
    WorkerRuntimeFacade runtime;
    auto &runtimeState = runtime.RuntimeState();
    runtimeState.MarkJoining("joining");
    service.SetRuntimeFacade(&runtime);
    GetSocketPathReqPb req;
    GetSocketPathRspPb rsp;

    auto rc = service.GetSocketPath(req, rsp);

    EXPECT_NE(rc.GetCode(), StatusCode::K_NOT_READY) << rc.ToString();
    EXPECT_EQ(rc.GetMsg().find("GetSocketPath"), std::string::npos);
    EXPECT_EQ(rc.GetMsg().find("JOINING"), std::string::npos);
}

TEST(WorkerServiceImplAdmissionTest, RegisterClientAllowsInternalJoiningWindow)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    std::atomic<bool> localExiting{ false };
    WorkerServiceImpl service(HostPort(), HostPort(), 1.0, nullptr, nullptr, "test-worker", membership, localExiting);
    WorkerRuntimeFacade runtime;
    auto &runtimeState = runtime.RuntimeState();
    runtimeState.MarkJoining("joining");
    service.SetRuntimeFacade(&runtime);
    RegisterClientReqPb req;
    req.set_version(DATASYSTEM_VERSION);
    RegisterClientRspPb rsp;

    auto rc = service.RegisterClient(req, rsp);

    EXPECT_NE(rc.GetCode(), StatusCode::K_NOT_READY) << rc.ToString();
    EXPECT_EQ(rc.GetMsg().find("RegisterClient"), std::string::npos);
    EXPECT_EQ(rc.GetMsg().find("JOINING"), std::string::npos);
}

TEST(WorkerServiceImplAdmissionTest, GetClientFdRejectsWhileWorkerIsStarting)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    std::atomic<bool> localExiting{ false };
    WorkerServiceImpl service(HostPort(), HostPort(), 1.0, nullptr, nullptr, "test-worker", membership, localExiting);
    WorkerRuntimeFacade runtime;
    service.SetRuntimeFacade(&runtime);
    GetClientFdReqPb req;
    GetClientFdRspPb rsp;

    auto rc = service.GetClientFd(req, rsp);

    EXPECT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    EXPECT_NE(rc.GetMsg().find("GetClientFd"), std::string::npos);
    EXPECT_NE(rc.GetMsg().find("STARTING"), std::string::npos);
}
}  // namespace
}  // namespace worker
}  // namespace datasystem
