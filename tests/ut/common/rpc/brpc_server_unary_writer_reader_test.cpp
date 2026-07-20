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

#include <gtest/gtest.h>

#include <brpc/controller.h>
#include <brpc/details/controller_private_accessor.h>
#include <butil/time.h>
#include "datasystem/common/rpc/brpc_server_unary_writer_reader.h"
#include "datasystem/protos/utils.pb.h"

namespace datasystem {
namespace test {

TEST(BrpcServerUnaryWriterReaderTest, UsesDeliveredTimeoutWhenServerDeadlineIsUnavailable)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    HostPortPb request;
    HostPortPb response;

    BrpcServerUnaryWriterReader<HostPortPb, HostPortPb> serverApi(&cntl, &request, &response, nullptr,
                                                                   "test.Unary");

    TimeoutDuration timeout = serverApi.GetScTimeoutDuration();
    const int64_t remainingMs = timeout.CalcRealRemainingTime();
    EXPECT_GT(remainingMs, 0);
    EXPECT_LE(remainingMs, 1000);
}

TEST(BrpcServerUnaryWriterReaderTest, KeepsExpiredServerDeadlineExpired)
{
    brpc::Controller cntl;
    brpc::ControllerPrivateAccessor(&cntl).set_deadline_us(butil::gettimeofday_us() - 1000);
    HostPortPb request;
    HostPortPb response;

    BrpcServerUnaryWriterReader<HostPortPb, HostPortPb> serverApi(&cntl, &request, &response, nullptr,
                                                                   "test.Unary");

    TimeoutDuration timeout = serverApi.GetScTimeoutDuration();
    EXPECT_LE(timeout.CalcRealRemainingTimeUs(), 0);
}

TEST(BrpcServerUnaryWriterReaderTest, PrefersServerDeadlineOverDeliveredTimeout)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(1000);
    brpc::ControllerPrivateAccessor(&cntl).set_deadline_us(butil::gettimeofday_us() + 20 * 1000);
    HostPortPb request;
    HostPortPb response;

    BrpcServerUnaryWriterReader<HostPortPb, HostPortPb> serverApi(&cntl, &request, &response, nullptr,
                                                                   "test.Unary");

    TimeoutDuration timeout = serverApi.GetScTimeoutDuration();
    const int64_t remainingMs = timeout.CalcRealRemainingTime();
    EXPECT_GT(remainingMs, 0);
    EXPECT_LE(remainingMs, 20);
}

TEST(BrpcServerUnaryWriterReaderTest, UsesDefaultTimeoutWithoutServerTimeoutInformation)
{
    brpc::Controller cntl;
    HostPortPb request;
    HostPortPb response;

    BrpcServerUnaryWriterReader<HostPortPb, HostPortPb> serverApi(&cntl, &request, &response, nullptr,
                                                                   "test.Unary");

    TimeoutDuration timeout = serverApi.GetScTimeoutDuration();
    EXPECT_GT(timeout.CalcRealRemainingTime(), 59 * 1000);
}

}  // namespace test
}  // namespace datasystem
