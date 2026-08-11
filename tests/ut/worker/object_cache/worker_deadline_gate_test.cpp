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
 * Description: UT for deadline gates in Get/Set handler that prevent exceeding requestTimeoutMs.
 */

#include <thread>
#include <gtest/gtest.h>

#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace test {

class WorkerDeadlineGateTest : public testing::Test {
protected:
    void SetUp() override
    {
        GetRequestContext()->reqTimeoutDuration.Init(20);
    }
    void TearDown() override
    {
        GetRequestContext()->reqTimeoutDuration.Init();
    }
};

// Verify that CalcRealRemainingTimeUs returns <=0 after the deadline elapses.
TEST_F(WorkerDeadlineGateTest, DeadlineExceededReturnsNegativeRemaining)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    int64_t remaining = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    EXPECT_LE(remaining, 0);
}

// Verify that a fresh 20ms deadline has positive remaining.
TEST_F(WorkerDeadlineGateTest, FreshDeadlineHasPositiveRemaining)
{
    int64_t remaining = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    EXPECT_GT(remaining, 0);
    EXPECT_LE(remaining, 20000);
}

// Verify that an uninitialized reqTimeoutDuration returns the 60s default.
TEST_F(WorkerDeadlineGateTest, UninitializedReturnsPositiveDefault)
{
    GetRequestContext()->reqTimeoutDuration.Init();
    int64_t remaining = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    EXPECT_GT(remaining, 0);
}

// Verify the ProcessShmPut Publish gate: once Create+mmap+memcpy exhausted the budget,
// CheckApiDeadline (called right before the Publish RPC) returns K_RPC_DEADLINE_EXCEEDED so the
// second, non-idempotent RPC is skipped. ApiDeadlineGuard scopes Init so the thread-local deadline
// is restored for the rest of the suite.
TEST_F(WorkerDeadlineGateTest, PublishGateSkipsWhenDeadlineExhausted)
{
    ApiDeadlineGuard deadlineGuard(1);
    EXPECT_TRUE(ApiDeadline::Instance().CheckApiDeadline().IsOk());
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    Status s = ApiDeadline::Instance().CheckApiDeadline();
    EXPECT_EQ(s.GetCode(), K_RPC_DEADLINE_EXCEEDED);
}

}  // namespace test
}  // namespace datasystem
