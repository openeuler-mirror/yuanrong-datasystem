/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <gtest/gtest.h>

#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#define private public
#include "datasystem/worker/worker_service_impl.h"
#undef private

namespace datasystem::worker {
TEST(WorkerServiceUbHealthTest, HeartbeatCarriesOneCompleteSelfSummary)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    std::atomic<bool> localExiting{ false };
    WorkerServiceImpl service(HostPort("127.0.0.1", 18480), HostPort("127.0.0.1", 18481), 1.0, nullptr, nullptr, "",
                              membership, localExiting);
    service.workerStartId_ = "incarnation-a";
    UbHealthSummary source;
    source.worker = HostPort("127.0.0.1", 19999);
    source.writable = false;
    source.state = UbAdmissionState::UNAVAILABLE;
    source.reason = UbFailureClass::PORT_UNAVAILABLE_ERROR4;
    source.lastStatusCode = K_URMA_ERROR;
    source.epoch = 9;
    service.SetUbHealthSummaryProvider([source] { return std::optional<UbHealthSummary>{ source }; });
    HeartbeatRspPb rsp;

    service.PopulateUbHealthSummary(rsp);

    ASSERT_TRUE(rsp.has_ub_health_summary());
    UbHealthSummary decoded;
    ASSERT_TRUE(DecodeUbHealthSummary(rsp.ub_health_summary(), decoded).IsOk());
    EXPECT_EQ(decoded.worker, HostPort("127.0.0.1", 18480));
    EXPECT_EQ(decoded.incarnation, "incarnation-a");
    EXPECT_FALSE(decoded.writable);
    EXPECT_EQ(decoded.reason, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
    EXPECT_EQ(decoded.epoch, 9u);
}

TEST(WorkerServiceUbHealthTest, HeartbeatConsumerFencesIncarnationBeforeNotification)
{
    UbHealthSummary source;
    source.worker = HostPort("127.0.0.1", 18480);
    source.incarnation = "incarnation-a";
    source.writable = false;
    source.epoch = 3;
    HeartbeatRspPb rsp;
    EncodeUbHealthSummary(source, *rsp.mutable_ub_health_summary());
    UbHealthSummaryCache cache;
    size_t notifications = 0;

    EXPECT_TRUE(ApplyHeartbeatUbHealthSummary(
                    rsp, source.worker, "incarnation-b", cache,
                    [&notifications](const UbHealthSummary &) { ++notifications; })
                    .IsOk());
    EXPECT_EQ(notifications, 0u);
    EXPECT_FALSE(cache.Get(source.worker).has_value());

    EXPECT_TRUE(ApplyHeartbeatUbHealthSummary(
                    rsp, source.worker, "incarnation-a", cache,
                    [&notifications](const UbHealthSummary &) { ++notifications; })
                    .IsOk());
    EXPECT_EQ(notifications, 1u);
    ASSERT_TRUE(cache.Get(source.worker).has_value());
}

TEST(WorkerServiceUbHealthTest, HeartbeatConsumerRejectsSummaryFromAnotherWorker)
{
    UbHealthSummary source;
    source.worker = HostPort("127.0.0.1", 18480);
    source.incarnation = "incarnation-a";
    HeartbeatRspPb rsp;
    EncodeUbHealthSummary(source, *rsp.mutable_ub_health_summary());
    UbHealthSummaryCache cache;
    size_t notifications = 0;

    auto rc = ApplyHeartbeatUbHealthSummary(
        rsp, HostPort("127.0.0.1", 18481), source.incarnation, cache,
        [&notifications](const UbHealthSummary &) { ++notifications; });

    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_EQ(notifications, 0u);
    EXPECT_EQ(cache.Size(), 0u);
}
}  // namespace datasystem::worker
