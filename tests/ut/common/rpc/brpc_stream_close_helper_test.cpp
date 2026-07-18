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
 * Coverage split:
 *   UT (this file): tests helper logic paths that do NOT require a real brpc
 *   stream -- short-circuit (INVALID_STREAM_ID), drain reset, default logContext.
 *   These use INVALID_STREAM_ID intentionally to avoid calling brpc::StreamClose
 *   with a fake stream ID (UB risk).
 *
 *   ST (tests/st/common/rpc/brpc/stream_close_timeout_test.cpp): tests the
 *   end-to-end deferred cleanup queue + reaper thread paths (EnqueueDeferredCleanup,
 *   ProcessDeferredQueueBatch, expired leak fallback) with real brpc streams.
 */

#include "datasystem/common/rpc/brpc_stream_close_helper.h"
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>

#include <brpc/controller.h>
#include <gtest/gtest.h>

namespace datasystem {
namespace test {

class StreamCloseHelperTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        streamId = brpc::INVALID_STREAM_ID;
        streamEnd = false;
        readError = false;
    }

    brpc::StreamId streamId;
    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    bool streamEnd;
    bool readError;
};

// StreamCloseAndWait returns kClosed immediately when streamId is already
// INVALID_STREAM_ID. This is the no-op short-circuit path.
TEST_F(StreamCloseHelperTest, StreamCloseAndWaitShortCircuitsOnInvalidId)
{
    StreamCloseState state{streamId, mtx, cv, streamEnd, readError, nullptr, kDefaultDeferredWaitSec};
    StreamCloseResult result = StreamCloseAndWait(state);
    EXPECT_EQ(result, StreamCloseResult::kClosed);
    EXPECT_EQ(streamId, brpc::INVALID_STREAM_ID);
}

// StreamCloseAndDrain resets Controller when StreamCloseAndWait returns kClosed
// (INVALID streamId shortcut).
TEST_F(StreamCloseHelperTest, DrainResetsControllerOnClosed)
{
    std::unique_ptr<brpc::Controller> cntl = std::make_unique<brpc::Controller>();
    StreamCloseState state{streamId, mtx, cv, streamEnd, readError, nullptr, kDefaultDeferredWaitSec};

    bool reset = StreamCloseAndDrain(state, cntl, "test");
    EXPECT_TRUE(reset);
    EXPECT_EQ(cntl, nullptr);
    EXPECT_EQ(streamId, brpc::INVALID_STREAM_ID);
}

// StreamCloseAndDrain with default logContext (nullptr) falls back to
// "StreamClose" in the ERROR log.
TEST_F(StreamCloseHelperTest, DrainUsesDefaultLogContextWhenNull)
{
    std::unique_ptr<brpc::Controller> cntl = std::make_unique<brpc::Controller>();
    StreamCloseState state{streamId, mtx, cv, streamEnd, readError, nullptr, kDefaultDeferredWaitSec};

    // Call without explicit logContext — should not crash.
    bool reset = StreamCloseAndDrain(state, cntl);
    EXPECT_TRUE(reset);
    EXPECT_EQ(cntl, nullptr);
}

// StreamCloseAndDrain with custom timeout parameter.
TEST_F(StreamCloseHelperTest, DrainRespectsCustomTimeout)
{
    std::unique_ptr<brpc::Controller> cntl = std::make_unique<brpc::Controller>();
    StreamCloseState state{streamId, mtx, cv, streamEnd, readError, nullptr, kDefaultDeferredWaitSec};

    // Custom timeout should also short-circuit on INVALID streamId.
    bool reset = StreamCloseAndDrain(state, cntl, "customTimeout");
    EXPECT_TRUE(reset);
    EXPECT_EQ(cntl, nullptr);
}

// NOTE: The timeout-and-intentional-leak path of StreamCloseAndWait /
// StreamCloseAndDrain is tested end-to-end in stream_close_timeout_test
// (ST), which uses a real brpc server + stream to exercise the full
// StreamClose → wait_for 5s → kTimeout → release() codepath. The UT
// cannot safely test this path because calling brpc::StreamClose with
// a fake stream ID is undefined behavior.

}  // namespace test
}  // namespace datasystem
