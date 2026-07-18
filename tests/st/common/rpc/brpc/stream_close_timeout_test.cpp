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

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <brpc/stream.h>
// brpc headers above override LOG/VLOG/DLOG via butil/logging.h and pull in
// glog-style CHECK macros. Re-include datasystem's log.h to restore the
// spdlog-based macros (otherwise brpc's CHECK_EQ in doubly_buffered_data.h
// fails to parse). Same pattern as rpc_server.cpp.
#include "datasystem/common/log/log.h"
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "datasystem/common/rpc/brpc_stream_close_helper.h"
#include "datasystem/common/rpc/brpc/hello.pb.h"

namespace datasystem {
namespace test {
namespace {

constexpr int kTestPort = 18501;

// Server-streaming handler: accepts stream, sends one response, then closes.
class StreamHelloServiceImpl : public datasystem::rpc::brpc::HelloService {
public:
    void StreamHello(google::protobuf::RpcController *cntl,
                     const datasystem::rpc::brpc::HelloRequest *request,
                     datasystem::rpc::brpc::HelloResponse *response,
                     google::protobuf::Closure *done) override
    {
        // brpc streaming: response is sent via the stream, not this callback.
        // The server must accept the stream and write to it.
        (void)cntl;
        (void)request;
        (void)response;
        (void)done;
        // For brpc server-streaming, the server must use StreamAccept +
        // StreamWrite. This is handled inside brpc's server-side streaming
        // infrastructure. For now, this service is a placeholder for a
        // normal-unary fallback response.
        brpc::ClosureGuard doneGuard(done);
    }

    void SayHello(google::protobuf::RpcController *cntl,
                  const datasystem::rpc::brpc::HelloRequest *request,
                  datasystem::rpc::brpc::HelloResponse *response,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_message("Hello, " + request->name() + "!");
    }

    void BidiStreamHello(google::protobuf::RpcController *cntl,
                         const datasystem::rpc::brpc::HelloRequest *request,
                         datasystem::rpc::brpc::HelloResponse *response,
                         google::protobuf::Closure *done) override
    {
        (void)cntl;
        (void)request;
        (void)response;
        brpc::ClosureGuard doneGuard(done);
    }
};

class StreamCloseTimeoutTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        server_ = std::make_unique<brpc::Server>();
        ASSERT_EQ(server_->AddService(&svc_, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        brpc::ServerOptions opts;
        opts.idle_timeout_sec = -1;
        butil::EndPoint ep(butil::IP_ANY, kTestPort);
        ASSERT_EQ(server_->Start(ep, &opts), 0);
    }

    void TearDown() override
    {
        server_->Stop(0);
        server_->Join();
        server_.reset();
    }

    std::unique_ptr<brpc::Server> server_;
    StreamHelloServiceImpl svc_;
};

// Test that StreamCloseAndWait returns kClosed when on_closed fires normally.
// This verifies the normal fast-path where the peer closes within the timeout.
TEST_F(StreamCloseTimeoutTest, NormalStreamCloseSucceeds)
{
    brpc::ChannelOptions opts;
    opts.timeout_ms = 3000;
    opts.max_retry = 0;

    brpc::Channel channel;
    ASSERT_EQ(channel.Init("localhost", kTestPort, &opts), 0);

    datasystem::rpc::brpc::HelloService_Stub stub(&channel);

    // Use BidiStreamHello: client creates a stream, issues RPC, then closes.
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);

    brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
    brpc::StreamOptions streamOpts;
    streamOpts.handler = nullptr;
    ASSERT_EQ(brpc::StreamCreate(&streamId, cntl, &streamOpts), 0);
    ASSERT_NE(streamId, brpc::INVALID_STREAM_ID);

    datasystem::rpc::brpc::HelloRequest req;
    req.set_name("test");
    datasystem::rpc::brpc::HelloResponse rsp;
    stub.BidiStreamHello(&cntl, &req, &rsp, nullptr);

    // Close the stream and wait for on_closed
    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    bool streamEnd = false;
    bool readError = false;

    std::thread closeThread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::lock_guard<bthread::Mutex> lk(mtx);
        streamEnd = true;
        cv.notify_one();
    });

    StreamCloseState state{streamId, mtx, cv, streamEnd, readError};
    StreamCloseResult result = StreamCloseAndWait(state, 5);
    EXPECT_EQ(result, StreamCloseResult::kClosed);
    EXPECT_EQ(streamId, brpc::INVALID_STREAM_ID);

    closeThread.join();
}

// Test that StreamCloseAndWait returns kTimeout when no callback fires.
// This is the peer-hang scenario that triggers the intentional leak path.
TEST_F(StreamCloseTimeoutTest, StreamCloseTimesOutWhenPeerHangs)
{
    brpc::ChannelOptions opts;
    opts.timeout_ms = 3000;
    opts.max_retry = 0;

    brpc::Channel channel;
    ASSERT_EQ(channel.Init("localhost", kTestPort, &opts), 0);

    datasystem::rpc::brpc::HelloService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);

    brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
    brpc::StreamOptions streamOpts;
    streamOpts.handler = nullptr;
    ASSERT_EQ(brpc::StreamCreate(&streamId, cntl, &streamOpts), 0);
    ASSERT_NE(streamId, brpc::INVALID_STREAM_ID);

    datasystem::rpc::brpc::HelloRequest req;
    req.set_name("test");
    datasystem::rpc::brpc::HelloResponse rsp;
    stub.BidiStreamHello(&cntl, &req, &rsp, nullptr);

    // Don't signal streamEnd/readError — simulate peer hang.
    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    bool streamEnd = false;
    bool readError = false;

    StreamCloseState state{streamId, mtx, cv, streamEnd, readError};

    // Use a short timeout (1s) to keep test fast
    auto start = std::chrono::steady_clock::now();
    StreamCloseResult result = StreamCloseAndWait(state, 1);
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(result, StreamCloseResult::kTimeout);
    EXPECT_GE(elapsed, std::chrono::seconds(1));
    EXPECT_EQ(streamId, brpc::INVALID_STREAM_ID);
}

// Test that StreamCloseAndDrain leaks the Controller on timeout.
TEST_F(StreamCloseTimeoutTest, DrainLeaksControllerOnTimeout)
{
    brpc::ChannelOptions opts;
    opts.timeout_ms = 3000;
    opts.max_retry = 0;

    brpc::Channel channel;
    ASSERT_EQ(channel.Init("localhost", kTestPort, &opts), 0);

    datasystem::rpc::brpc::HelloService_Stub stub(&channel);

    auto testCntl = std::make_unique<brpc::Controller>();
    testCntl->set_timeout_ms(3000);

    brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
    brpc::StreamOptions streamOpts;
    streamOpts.handler = nullptr;
    ASSERT_EQ(brpc::StreamCreate(&streamId, *testCntl, &streamOpts), 0);
    ASSERT_NE(streamId, brpc::INVALID_STREAM_ID);

    datasystem::rpc::brpc::HelloRequest req;
    req.set_name("test");
    datasystem::rpc::brpc::HelloResponse rsp;
    stub.BidiStreamHello(testCntl.get(), &req, &rsp, nullptr);

    // Don't signal streamEnd/readError — simulate peer hang.
    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    bool streamEnd = false;
    bool readError = false;

    brpc::Controller *rawPtr = testCntl.get();
    StreamCloseState state{streamId, mtx, cv, streamEnd, readError};

    // Use 1s timeout for fast test
    bool reset = StreamCloseAndDrain(state, testCntl, "StreamCloseTimeoutTest");

    EXPECT_FALSE(reset);
    EXPECT_EQ(testCntl, nullptr);  // release() leaves unique_ptr empty
    EXPECT_EQ(streamId, brpc::INVALID_STREAM_ID);
    // rawPtr was intentionally leaked — don't delete it
    EXPECT_NE(rawPtr, nullptr);
}

// Test that StreamCloseAndDrain resets the Controller on normal close.
TEST_F(StreamCloseTimeoutTest, DrainResetsControllerOnNormalClose)
{
    brpc::ChannelOptions opts;
    opts.timeout_ms = 3000;
    opts.max_retry = 0;

    brpc::Channel channel;
    ASSERT_EQ(channel.Init("localhost", kTestPort, &opts), 0);

    datasystem::rpc::brpc::HelloService_Stub stub(&channel);

    auto testCntl = std::make_unique<brpc::Controller>();
    testCntl->set_timeout_ms(3000);

    brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
    brpc::StreamOptions streamOpts;
    streamOpts.handler = nullptr;
    ASSERT_EQ(brpc::StreamCreate(&streamId, *testCntl, &streamOpts), 0);
    ASSERT_NE(streamId, brpc::INVALID_STREAM_ID);

    datasystem::rpc::brpc::HelloRequest req;
    req.set_name("test");
    datasystem::rpc::brpc::HelloResponse rsp;
    stub.BidiStreamHello(testCntl.get(), &req, &rsp, nullptr);

    // Signal streamEnd from another thread to simulate on_closed
    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    bool streamEnd = false;
    bool readError = false;

    std::thread signalThread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::lock_guard<bthread::Mutex> lk(mtx);
        streamEnd = true;
        cv.notify_one();
    });

    StreamCloseState state{streamId, mtx, cv, streamEnd, readError};
    bool reset = StreamCloseAndDrain(state, testCntl, "StreamCloseTimeoutTest");

    EXPECT_TRUE(reset);
    EXPECT_EQ(testCntl, nullptr);  // reset() deletes and nullifies
    EXPECT_EQ(streamId, brpc::INVALID_STREAM_ID);

    signalThread.join();
}

// Regression: StreamCloseAndWait must wait for on_closed (streamEnd) and NOT
// return on on_failed (readError). brpc fires on_failed THEN on_closed during
// recycle (stream.cpp:594 -> :596); the old predicate (streamEnd || readError)
// returned on on_failed, letting the caller destroy the StreamInputHandler before
// the guaranteed on_closed -> UAF. This fires readError mid-wait and asserts the
// wait still times out (not satisfied by readError alone).
TEST_F(StreamCloseTimeoutTest, WaitDoesNotReturnOnFailedOnly)
{
    brpc::ChannelOptions opts;
    opts.timeout_ms = 3000;
    opts.max_retry = 0;

    brpc::Channel channel;
    ASSERT_EQ(channel.Init("localhost", kTestPort, &opts), 0);

    datasystem::rpc::brpc::HelloService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);

    brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
    brpc::StreamOptions streamOpts;
    streamOpts.handler = nullptr;
    ASSERT_EQ(brpc::StreamCreate(&streamId, cntl, &streamOpts), 0);
    ASSERT_NE(streamId, brpc::INVALID_STREAM_ID);

    datasystem::rpc::brpc::HelloRequest req;
    req.set_name("test");
    datasystem::rpc::brpc::HelloResponse rsp;
    stub.BidiStreamHello(&cntl, &req, &rsp, nullptr);

    bthread::Mutex mtx;
    bthread::ConditionVariable cv;
    bool streamEnd = false;
    bool readError = false;

    // Fire readError (on_failed analog) at 100ms, but never streamEnd (on_closed).
    std::thread failThread([&mtx, &cv, &readError]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::lock_guard<bthread::Mutex> lk(mtx);
        readError = true;
        cv.notify_one();
    });

    StreamCloseState state{streamId, mtx, cv, streamEnd, readError};
    auto start = std::chrono::steady_clock::now();
    StreamCloseResult result = StreamCloseAndWait(state, 2);  // 2s timeout
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    // readError alone must NOT satisfy the wait -> kTimeout, having waited the
    // full 2s (well past the 100ms readError signal).
    EXPECT_EQ(result, StreamCloseResult::kTimeout);
    EXPECT_GE(elapsedMs, 1500);
    EXPECT_EQ(streamId, brpc::INVALID_STREAM_ID);

    failThread.join();
}

}  // namespace
}  // namespace test
}  // namespace datasystem
