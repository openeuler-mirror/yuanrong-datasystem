/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include <gtest/gtest.h>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#define private public
#include "datasystem/worker/worker_service_impl.h"
#undef private

namespace datasystem::worker {
namespace {
const HostPort SELF("127.0.0.1", 18480);

std::string BinaryIncarnation()
{
    std::string incarnation(16, '\0');
    incarnation[0] = static_cast<char>(0xff);
    incarnation[1] = static_cast<char>(0xfe);
    incarnation[15] = static_cast<char>(0x80);
    return incarnation;
}

void PublishSelf(cluster::TopologySnapshotState &snapshots, const std::string &incarnation)
{
    cluster::TopologyState state;
    state.version = 1;
    state.members = { cluster::Member{ { incarnation, SELF.ToString() }, cluster::MemberState::ACTIVE, { 1 } } };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    ASSERT_TRUE(cluster::TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot).IsOk());
    cluster::SnapshotUpdateOutcome outcome;
    ASSERT_TRUE(snapshots.Publish(std::move(snapshot), outcome).IsOk());
}
}  // namespace

TEST(WorkerServiceUbHealthTest, HeartbeatCarriesOneCompleteSelfSummary)
{
    cluster::TopologySnapshotState snapshots;
    const auto topologyIncarnation = BinaryIncarnation();
    PublishSelf(snapshots, topologyIncarnation);
    cluster::MembershipEndpointView membership(snapshots);
    std::atomic<bool> localExiting{ false };
    WorkerServiceImpl service(SELF, HostPort("127.0.0.1", 18481), 1.0, nullptr, nullptr, "", membership,
                              localExiting);
    service.workerStartId_ = "process-start-id";
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
    std::string wire;
    ASSERT_TRUE(rsp.SerializeToString(&wire));
    HeartbeatRspPb parsed;
    ASSERT_TRUE(parsed.ParseFromString(wire));
    UbHealthSummary decoded;
    ASSERT_TRUE(DecodeUbHealthSummary(parsed.ub_health_summary(), decoded).IsOk());
    EXPECT_EQ(decoded.worker, SELF);
    EXPECT_EQ(decoded.incarnation, topologyIncarnation);
    EXPECT_NE(decoded.incarnation, service.workerStartId_);
    EXPECT_FALSE(decoded.writable);
    EXPECT_EQ(decoded.reason, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
    EXPECT_EQ(decoded.epoch, 9u);
}

TEST(WorkerServiceUbHealthTest, HeartbeatConsumerNotifiesValidatedUbIncarnation)
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
                    rsp, source.worker, cache,
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
        rsp, HostPort("127.0.0.1", 18481), cache,
        [&notifications](const UbHealthSummary &) { ++notifications; });

    EXPECT_EQ(rc.GetCode(), K_INVALID);
    EXPECT_EQ(notifications, 0u);
    EXPECT_EQ(cache.Size(), 0u);
}
}  // namespace datasystem::worker
