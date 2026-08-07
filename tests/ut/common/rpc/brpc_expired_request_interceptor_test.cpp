/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Unit tests for ExpiredRequestInterceptor. Verifies the drop logic and the
 * edge guards (no deadline, unset received time, disabled flag) without needing a running
 * server: brpc::Controller is constructed directly and timeout_ms / rpc_received_us are set
 * via their setters.
 */
#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include <brpc/controller.h>
#include <butil/time.h>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rpc/brpc_expired_request_interceptor.h"
#include "datasystem/common/rpc/brpc_status_util.h"

namespace datasystem {
namespace test {
namespace {
// Client budgets in milliseconds.
constexpr int64_t kBudgetMs20 = 20;
constexpr int64_t kBudgetMs60s = 60 * 1000;
// Elapsed-since-received offsets in microseconds (butil::cpuwide_time_us clock).
constexpr int64_t kElapsedUs100ms = 100 * 1000;
constexpr int64_t kElapsedUsVeryOld = 999'999'999;
}  // namespace

class BrpcExpiredRequestInterceptorTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        prevDrop_ = FLAGS_brpc_drop_expired_request;
        FLAGS_brpc_drop_expired_request = true;
    }
    void TearDown() override
    {
        FLAGS_brpc_drop_expired_request = prevDrop_;
    }
    bool prevDrop_{ false };
    ExpiredRequestInterceptor interceptor_;
};

// Already expired (elapsed > timeout) -> reject with ERPCTIMEDOUT (1008).
TEST_F(BrpcExpiredRequestInterceptorTest, RejectsAlreadyExpiredRequest)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(kBudgetMs20);  // 20 ms client budget
    cntl.set_rpc_received_us(butil::cpuwide_time_us() - kElapsedUs100ms);  // read 100 ms ago
    int code = 0;
    std::string txt;
    EXPECT_FALSE(interceptor_.Accept(&cntl, code, txt));
    EXPECT_EQ(code, kBrpcErpcTimedOut);
    EXPECT_FALSE(txt.empty());
}

// Fresh request -> accept, and the output args are left untouched.
TEST_F(BrpcExpiredRequestInterceptorTest, AcceptsFreshRequest)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(kBudgetMs60s);  // 60 s budget
    cntl.set_rpc_received_us(butil::cpuwide_time_us());  // just read
    int code = 42;
    std::string txt = "untouched";
    EXPECT_TRUE(interceptor_.Accept(&cntl, code, txt));
    EXPECT_EQ(code, 42);
    EXPECT_EQ(txt, "untouched");
}

// No deadline (timeout_ms = 0) -> accept even if very old, so non-datasystem clients or
// no-deadline requests are never dropped.
TEST_F(BrpcExpiredRequestInterceptorTest, AcceptsWhenNoDeadline)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(0);
    cntl.set_rpc_received_us(butil::cpuwide_time_us() - kElapsedUsVeryOld);
    int code = 0;
    std::string txt;
    EXPECT_TRUE(interceptor_.Accept(&cntl, code, txt));
}

// received_us unset (0) -> accept (defensive guard; baidu-std always sets it, but other
// protocols may not).
TEST_F(BrpcExpiredRequestInterceptorTest, AcceptsWhenReceivedTimeUnset)
{
    brpc::Controller cntl;
    cntl.set_timeout_ms(kBudgetMs20);
    cntl.set_rpc_received_us(0);
    int code = 0;
    std::string txt;
    EXPECT_TRUE(interceptor_.Accept(&cntl, code, txt));
}

// Flag off (escape hatch) -> accept, even for an obviously expired request.
TEST_F(BrpcExpiredRequestInterceptorTest, AcceptsWhenDisabled)
{
    FLAGS_brpc_drop_expired_request = false;
    brpc::Controller cntl;
    cntl.set_timeout_ms(1);
    cntl.set_rpc_received_us(butil::cpuwide_time_us() - kElapsedUsVeryOld);
    int code = 0;
    std::string txt;
    EXPECT_TRUE(interceptor_.Accept(&cntl, code, txt));
}

}  // namespace test
}  // namespace datasystem
