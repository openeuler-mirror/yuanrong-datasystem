/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Unit tests for MigrateTransport::ProcessMigrateResponse skip-key propagation.
 */

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <unordered_set>

#include "ut/common.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"

#define private public
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport2.h"
#include "datasystem/worker/object_cache/data_migrator/transport/fast_migrate_transport.h"
#include "datasystem/worker/object_cache/data_migrator/transport/tcp_migrate_transport.h"
#undef private

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {

namespace {
MigrateTransport::Request MakeEmptyRequest()
{
    MigrateTransport::Request req;
    req.type = MigrateType::SPILL;
    req.localAddr = "127.0.0.1:18888";
    return req;
}
}  // namespace

TEST(FastMigrateTransport2Test, ProcessMigrateResponsePopulatesSkipKeys)
{
    NotifyRemoteGetReqPb reqPb;
    reqPb.add_object_keys("k1");
    reqPb.add_object_keys("k2");
    NotifyRemoteGetRspPb rspPb;
    rspPb.add_skipped_object_keys("k1");

    auto req = MakeEmptyRequest();
    MigrateTransport::Response rsp;
    FastMigrateTransport2 transport;
    transport.ProcessMigrateResponse(reqPb, rspPb, req, rsp);

    EXPECT_EQ(rsp.skipKeys.size(), 1u);
    EXPECT_TRUE(rsp.skipKeys.count("k1") > 0);
}

TEST(FastMigrateTransport2Test, ProcessMigrateResponseExcludesSkippedFromSuccess)
{
    NotifyRemoteGetReqPb reqPb;
    reqPb.add_object_keys("k1");
    reqPb.add_object_keys("k2");
    NotifyRemoteGetRspPb rspPb;
    rspPb.add_skipped_object_keys("k1");

    auto req = MakeEmptyRequest();
    MigrateTransport::Response rsp;
    FastMigrateTransport2 transport;
    transport.ProcessMigrateResponse(reqPb, rspPb, req, rsp);

    EXPECT_EQ(rsp.successKeys.size(), 1u);
    EXPECT_TRUE(rsp.successKeys.count("k2") > 0);
    EXPECT_TRUE(rsp.successKeys.count("k1") == 0);
}

TEST(FastMigrateTransportTest, ProcessMigrateResponsePopulatesSkipKeys)
{
    MigrateDataDirectReqPb reqPb;
    auto *obj1 = reqPb.add_objects();
    obj1->set_object_key("k1");
    auto *obj2 = reqPb.add_objects();
    obj2->set_object_key("k2");
    MigrateDataDirectRspPb rspPb;
    rspPb.add_skipped_object_keys("k1");

    auto req = MakeEmptyRequest();
    MigrateTransport::Response rsp;
    FastMigrateTransport transport;
    transport.ProcessMigrateResponse(reqPb, rspPb, req, rsp);

    EXPECT_EQ(rsp.skipKeys.size(), 1u);
    EXPECT_TRUE(rsp.skipKeys.count("k1") > 0);
    EXPECT_EQ(rsp.successKeys.size(), 1u);
    EXPECT_TRUE(rsp.successKeys.count("k2") > 0);
    EXPECT_TRUE(rsp.successKeys.count("k1") == 0);
}

TEST(TcpMigrateTransportTest, ProcessMigrateResponsePreservesSkipAndExpiredClassifications)
{
    MigrateDataRspPb rspPb;
    rspPb.add_success_ids("success");
    rspPb.add_success_ids("skip");
    rspPb.add_skipped_object_keys("skip");
    rspPb.add_expired_ids("expired");

    auto req = MakeEmptyRequest();
    MigrateTransport::Response rsp;
    TcpMigrateTransport transport;
    transport.ProcessMigrateRsp(rspPb, req, rsp);

    EXPECT_EQ(rsp.successKeys.size(), 1u);
    EXPECT_TRUE(rsp.successKeys.count("success") > 0);
    EXPECT_TRUE(rsp.successKeys.count("skip") == 0);
    EXPECT_TRUE(rsp.skipKeys.count("skip") > 0);
    EXPECT_TRUE(rsp.expiredKeys.count("expired") > 0);
}

}  // namespace ut
}  // namespace datasystem
