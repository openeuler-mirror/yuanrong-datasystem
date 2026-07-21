/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"

#include "datasystem/worker/runtime/worker_runtime_facade.h"
#include "datasystem/worker/worker_health_check.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
class ClientWorkerSCServiceAdmissionTest : public CommonTest {
public:
    void TearDown() override
    {
        SetTopologyServingAdmission(true);
        SetUnhealthy();
        CommonTest::TearDown();
    }

protected:
    cluster::TopologySnapshotState snapshots_;
    cluster::MembershipEndpointView membership_{ snapshots_ };
    worker::MetadataRouteResolver metadataRoute_{ nullptr, worker::MetadataRouteOptions{} };
};

TEST_F(ClientWorkerSCServiceAdmissionTest, WorkerServiceAdmissionRejectsStreamReadWriteDuringIsolation)
{
    worker::stream_cache::ClientWorkerSCServiceImpl service(HostPort(), HostPort(), nullptr, nullptr, nullptr,
                                                            metadataRoute_, membership_);
    worker::WorkerRuntimeFacade runtime;
    runtime.MarkLocalIsolated(worker::WorkerIsolationReason::CONTROL_BACKEND_LOCAL_ISOLATION, "local isolation");
    service.SetRuntimeFacade(&runtime);
    DS_ASSERT_OK(SetHealthProbe());
    SetTopologyServingAdmission(true);

    auto rc = service.CreateProducer(nullptr);

    ASSERT_EQ(rc.GetCode(), StatusCode::K_NOT_READY);
    ASSERT_NE(rc.GetMsg().find("LOCAL_ISOLATED"), std::string::npos);
    ASSERT_NE(rc.GetMsg().find("CONTROL_BACKEND_LOCAL_ISOLATION"), std::string::npos);
}
}  // namespace ut
}  // namespace datasystem
