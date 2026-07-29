/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include <gtest/gtest.h>

#include "datasystem/cluster/ub_health/ub_health_lease_sync.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "ut/cluster/testing/fake_coordination_backend.h"

namespace datasystem::cluster {
namespace {
const HostPort SELF("127.0.0.1", 18480);
const HostPort GLOBAL_PEER("127.0.0.1", 18481);
const HostPort LOCAL_PEER("127.0.0.1", 18482);
constexpr char TABLE[] = "/datasystem/test/ub_health";

std::string Encode(const UbHealthSummary &summary)
{
    UbHealthSummaryPb pb;
    EncodeUbHealthSummary(summary, pb);
    return pb.SerializeAsString();
}
}  // namespace

TEST(UbHealthLeaseSyncTest, PublishesOneSelfRecordAndConsumesSelfOnlySnapshot)
{
    FakeCoordinationBackend backend;
    std::vector<UbHealthSummary> consumed;
    UbHealthLeaseSync::Config config{
        TABLE,
        SELF,
        "self-1",
        [] {
            UbHealthSummary summary;
            summary.writable = false;
            summary.epoch = 4;
            return summary;
        },
        [&consumed](const auto &snapshot) { consumed = snapshot; }
    };
    UbHealthLeaseSync sync(backend, std::move(config));

    ASSERT_TRUE(sync.SyncOnce().IsOk());

    std::vector<std::pair<std::string, std::string>> records;
    ASSERT_TRUE(backend.GetAll(TABLE, records).IsOk());
    ASSERT_EQ(records.size(), 1u);
    ASSERT_EQ(consumed.size(), 1u);
    EXPECT_EQ(consumed.front().worker, SELF);
    EXPECT_EQ(consumed.front().incarnation, "self-1");
    EXPECT_FALSE(consumed.front().writable);
    EXPECT_EQ(consumed.front().epoch, 4u);
}

TEST(UbHealthLeaseSyncTest, LeaseExpiryDropsGlobalObservationButPreservesLocalEvidence)
{
    FakeCoordinationBackend backend;
    PeerUbAdmission admission;
    UbHealthSummary global;
    global.worker = GLOBAL_PEER;
    global.incarnation = "global-1";
    global.writable = false;
    global.epoch = 2;
    backend.PutBytes(TABLE, GLOBAL_PEER.ToString(), Encode(global));
    UbOpOutcome local(LOCAL_PEER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                      Status(K_URMA_ERROR, "local provider observation"));
    local.cqeStatus = 4;
    admission.ReportOutcome(local);
    UbHealthLeaseSync::Config config{
        TABLE,
        SELF,
        "self-1",
        [] { return UbHealthSummary{}; },
        [&admission](const auto &snapshot) { admission.ReplaceGlobalSummaries(snapshot); }
    };
    UbHealthLeaseSync sync(backend, std::move(config));

    ASSERT_TRUE(sync.SyncOnce().IsOk());
    EXPECT_EQ(admission.CheckReadSource(GLOBAL_PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
    EXPECT_EQ(admission.CheckReadSource(LOCAL_PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);

    ASSERT_TRUE(backend.Delete(TABLE, GLOBAL_PEER.ToString()).IsOk());
    ASSERT_TRUE(sync.SyncOnce().IsOk());
    EXPECT_TRUE(admission.CheckReadSource(GLOBAL_PEER).IsOk());
    EXPECT_EQ(admission.CheckReadSource(LOCAL_PEER).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
}
}  // namespace datasystem::cluster
