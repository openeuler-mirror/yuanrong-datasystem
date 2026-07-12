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

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include "datasystem/common/rpc/brpc/hello.pb.h"
#include "datasystem/common/rpc/rpc_server.h"

namespace datasystem {
namespace test {
namespace brpc_shutdown {

constexpr int kTestPortNormal = 18501;
constexpr int kTestPortHang = 18502;
constexpr int kJoinTimeoutMs = 3000;  // Short timeout for test verification

class NormalHelloServiceImpl : public rpc::brpc::HelloService {
public:
    void SayHello(google::protobuf::RpcController *cntl,
                  const rpc::brpc::HelloRequest *request,
                  rpc::brpc::HelloResponse *response,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_message("Hello, " + request->name() + "!");
    }
};

// Handler that blocks on a condition variable forever, simulating a stuck
// bthread (OOM, deadlock, etc.). The test signals the CV during cleanup.
class HangingHelloServiceImpl : public rpc::brpc::HelloService {
public:
    void SayHello(google::protobuf::RpcController *cntl,
                  const rpc::brpc::HelloRequest *request,
                  rpc::brpc::HelloResponse *response,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        std::unique_lock<std::mutex> lock(mtx_);
        started_ = true;
        startedCv_.notify_all();
        cv_.wait(lock);  // Block until Unblock() is called
        response->set_message("unblocked");
    }

    void WaitUntilStarted()
    {
        std::unique_lock<std::mutex> lock(mtx_);
        startedCv_.wait(lock, [this] { return started_; });
    }

    void Unblock()
    {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.notify_all();
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    std::condition_variable startedCv_;
    bool started_ = false;
};

// Emulates the StopBrpcServer detach+future pattern (matching rpc_server.cpp)
// but returns the Join thread handle so the caller can join it after cleanup,
// avoiding UAF between the detached thread and a stack-allocated server.
struct JoinResult {
    std::future_status status;
    bool joinCompleted = false;
};

std::thread RunBoundedJoin(brpc::Server *server, int timeoutMs, JoinResult &result)
{
    result = {};
    server->Stop(0);

    brpc::Server *raw = server;
    std::promise<void> joinDone;
    std::future<void> doneFuture = joinDone.get_future();
    std::thread joinThread([raw, p = std::move(joinDone)]() mutable {
        raw->Join();
        p.set_value();
    });

    result.status = doneFuture.wait_for(std::chrono::milliseconds(timeoutMs));
    result.joinCompleted = (result.status == std::future_status::ready);
    return joinThread;
}

class ShutdownTimeoutTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Verify normal shutdown: Join completes well within the timeout window
// when there are no stuck bthreads.
TEST_F(ShutdownTimeoutTest, NormalShutdownCompletesWithinTimeout)
{
    brpc::Server server;
    NormalHelloServiceImpl svc;
    ASSERT_EQ(server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions opts;
    opts.idle_timeout_sec = -1;
    butil::EndPoint ep(butil::IP_ANY, kTestPortNormal);
    ASSERT_EQ(server.Start(ep, &opts), 0);

    // Send a quick RPC to verify the server is live
    brpc::Channel channel;
    brpc::ChannelOptions channelOpts;
    channelOpts.timeout_ms = 3000;
    channelOpts.max_retry = 0;
    ASSERT_EQ(channel.Init("127.0.0.1", kTestPortNormal, &channelOpts), 0);

    rpc::brpc::HelloRequest req;
    req.set_name("test");
    rpc::brpc::HelloResponse rsp;
    brpc::Controller cntl;
    rpc::brpc::HelloService_Stub stub(&channel);
    stub.SayHello(&cntl, &req, &rsp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    EXPECT_EQ(rsp.message(), "Hello, test!");

    // Shutdown with a generous timeout: normal path must complete fast
    JoinResult result;
    auto start = std::chrono::steady_clock::now();
    std::thread joinThread = RunBoundedJoin(&server, kJoinTimeoutMs, result);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    EXPECT_EQ(result.status, std::future_status::ready);
    EXPECT_TRUE(result.joinCompleted);
    EXPECT_LT(elapsed, kJoinTimeoutMs / 2) << "Normal shutdown took " << elapsed
        << "ms, expected < " << (kJoinTimeoutMs / 2) << "ms";

    joinThread.join();  // Already completed, joins immediately
}

// Verify timeout path: when a bthread handler is stuck, Join() blocks
// indefinitely but the bounded wait times out (no crash, no abort).
TEST_F(ShutdownTimeoutTest, JoinTimeoutTriggersErrorOnStuckBthread)
{
    brpc::Server server;
    HangingHelloServiceImpl svc;
    ASSERT_EQ(server.AddService(&svc, brpc::SERVER_DOESNT_OWN_SERVICE), 0);

    brpc::ServerOptions opts;
    opts.idle_timeout_sec = -1;
    butil::EndPoint ep(butil::IP_ANY, kTestPortHang);
    ASSERT_EQ(server.Start(ep, &opts), 0);

    // Fire an RPC on a background thread so the handler blocks on the CV
    std::thread rpcThread([&]() {
        brpc::Channel channel;
        brpc::ChannelOptions channelOpts;
        channelOpts.timeout_ms = 60000;  // Long timeout so RPC doesn't cancel
        channelOpts.max_retry = 0;
        channel.Init("127.0.0.1", kTestPortHang, &channelOpts);
        rpc::brpc::HelloRequest req;
        req.set_name("hang");
        rpc::brpc::HelloResponse rsp;
        brpc::Controller cntl;
        rpc::brpc::HelloService_Stub stub(&channel);
        stub.SayHello(&cntl, &req, &rsp, nullptr);
    });

    // Wait for the handler bthread to enter the CV wait
    svc.WaitUntilStarted();

    // Run bounded Join — the handler is stuck, so Join should time out
    JoinResult result;
    auto start = std::chrono::steady_clock::now();
    std::thread joinThread = RunBoundedJoin(&server, kJoinTimeoutMs, result);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    EXPECT_EQ(result.status, std::future_status::timeout);
    EXPECT_FALSE(result.joinCompleted);
    EXPECT_GE(elapsed, kJoinTimeoutMs - 500) << "Timeout too early: " << elapsed << "ms";
    EXPECT_LT(elapsed, kJoinTimeoutMs + 1000) << "Timeout took too long: " << elapsed << "ms";

    // Unblock the handler so the Join thread and RPC can complete.
    // joinThread.join() synchronizes: Join() returns after handler unblocks,
    // guaranteeing server lifetime covers the Join call.
    svc.Unblock();
    rpcThread.join();
    joinThread.join();
}

// Verify StopBrpcServer() postconditions: completes without abort,
// idempotent on re-call, and ~RpcServer() destructor is clean.
TEST_F(ShutdownTimeoutTest, StopBrpcServerIdempotentNoAbort)
{
    constexpr int kTestPortStop = 18503;

    // Build a real RpcServer in brpc mode (not a raw brpc::Server).
    // This exercises the actual StopBrpcServer() code path.
    std::unique_ptr<RpcServer> rpcServer;
    ASSERT_EQ(RpcServer::Builder()
                  .SetUseBrpc(true)
                  .SetBrpcAddr("127.0.0.1", kTestPortStop)
                  .Init(rpcServer),
              Status::OK());

    NormalHelloServiceImpl svc;
    ASSERT_EQ(rpcServer->AddBrpcService(&svc), Status::OK());
    ASSERT_EQ(rpcServer->StartBrpcServer("127.0.0.1", kTestPortStop), Status::OK());

    // Send a quick RPC to verify the server is live (active bthreads).
    brpc::Channel channel;
    brpc::ChannelOptions channelOpts;
    channelOpts.timeout_ms = 3000;
    channelOpts.max_retry = 0;
    ASSERT_EQ(channel.Init("127.0.0.1", kTestPortStop, &channelOpts), 0);

    rpc::brpc::HelloRequest req;
    req.set_name("stop");
    rpc::brpc::HelloResponse rsp;
    brpc::Controller cntl;
    rpc::brpc::HelloService_Stub stub(&channel);
    stub.SayHello(&cntl, &req, &rsp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

    // Call StopBrpcServer() — must complete without abort.
    // After this call, brpcServer_ is null (unique_ptr reset).
    rpcServer->StopBrpcServer();

    // Idempotency: second call must be a no-op (brpcServer_ is null).
    // This must not crash, abort, or SIGSEGV.
    rpcServer->StopBrpcServer();

    // ~RpcServer() calls StopBrpcServer() a third time via the
    // `if (useBrpc_ && brpcServer_)` guard. Since brpcServer_ is already
    // null, it's a no-op. The destructor must complete without abort.
    // If the fix were reverted to detach, the first StopBrpcServer() would
    // have detached a Join thread, and this reset() → ~RpcServer() would
    // race with it → SIGABRT.
    rpcServer.reset();

    // If we reach here, no SIGABRT occurred. The test framework will
    // report PASSED with exit 0 (not 134).
}

// Two threads call StopBrpcServer() concurrently — mutex must serialize.
TEST_F(ShutdownTimeoutTest, ConcurrentStopBrpcServerSerialized)
{
    constexpr int kTestPortConcurrent = 18504;

    std::unique_ptr<RpcServer> rpcServer;
    ASSERT_EQ(RpcServer::Builder()
                  .SetUseBrpc(true)
                  .SetBrpcAddr("127.0.0.1", kTestPortConcurrent)
                  .Init(rpcServer),
              Status::OK());

    NormalHelloServiceImpl svc;
    ASSERT_EQ(rpcServer->AddBrpcService(&svc), Status::OK());
    ASSERT_EQ(rpcServer->StartBrpcServer("127.0.0.1", kTestPortConcurrent), Status::OK());

    // Verify the server is live with a quick RPC that completes before stop.
    brpc::Channel channel;
    brpc::ChannelOptions channelOpts;
    channelOpts.timeout_ms = 3000;
    channelOpts.max_retry = 0;
    ASSERT_EQ(channel.Init("127.0.0.1", kTestPortConcurrent, &channelOpts), 0);
    rpc::brpc::HelloRequest req;
    req.set_name("concurrent");
    rpc::brpc::HelloResponse rsp;
    brpc::Controller cntl;
    rpc::brpc::HelloService_Stub stub(&channel);
    stub.SayHello(&cntl, &req, &rsp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

    // Two threads call StopBrpcServer() concurrently. The mutex serializes
    // them: the first does Stop+Join+reset, the second sees nullptr and
    // returns immediately. Without the mutex, both pass the guard → both
    // reset() → UAF.
    std::thread t1([&]() { rpcServer->StopBrpcServer(); });
    std::thread t2([&]() { rpcServer->StopBrpcServer(); });
    t1.join();
    t2.join();

    // No crash, no SIGABRT. ~RpcServer() calls StopBrpcServer() again (no-op).
    rpcServer.reset();
}

// StopBrpcServer with an in-flight RPC — Join() must drain the bthread before reset().
TEST_F(ShutdownTimeoutTest, StopBrpcServerWithInFlightRpc)
{
    constexpr int kTestPortInFlight = 18505;

    std::unique_ptr<RpcServer> rpcServer;
    ASSERT_EQ(RpcServer::Builder()
                  .SetUseBrpc(true)
                  .SetBrpcAddr("127.0.0.1", kTestPortInFlight)
                  .Init(rpcServer),
              Status::OK());

    HangingHelloServiceImpl svc;  // Blocks until Unblock() is called
    ASSERT_EQ(rpcServer->AddBrpcService(&svc), Status::OK());
    ASSERT_EQ(rpcServer->StartBrpcServer("127.0.0.1", kTestPortInFlight), Status::OK());

    // Fire an RPC on a background thread. The handler will block on the CV,
    // keeping a bthread in-flight when StopBrpcServer() is called.
    std::thread rpcThread([&]() {
        brpc::Channel channel;
        brpc::ChannelOptions channelOpts;
        channelOpts.timeout_ms = 10000;  // Long timeout; RPC won't cancel
        channelOpts.max_retry = 0;
        channel.Init("127.0.0.1", kTestPortInFlight, &channelOpts);
        rpc::brpc::HelloRequest req;
        req.set_name("inflight");
        rpc::brpc::HelloResponse rsp;
        brpc::Controller cntl;
        rpc::brpc::HelloService_Stub stub(&channel);
        stub.SayHello(&cntl, &req, &rsp, nullptr);
    });

    // Wait for the handler bthread to enter the CV wait (in-flight).
    svc.WaitUntilStarted();

    // Now call StopBrpcServer(). Stop(0) marks the server closing; Join()
    // must wait for the in-flight handler bthread to exit. Since the handler
    // is blocked, Join() will block. We unblock the handler to let Join()
    // proceed. This tests that Join() correctly waits for active bthreads.
    std::thread stopThread([&]() { rpcServer->StopBrpcServer(); });

    // Give Stop+Join a moment to start waiting on the in-flight bthread.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Unblock the handler so Join() can complete.
    svc.Unblock();

    // StopBrpcServer should now return (Join completed, server reset).
    stopThread.join();
    rpcThread.join();

    // ~RpcServer() — StopBrpcServer() no-op (brpcServer_ already null).
    rpcServer.reset();
}

}  // namespace brpc_shutdown
}  // namespace test
}  // namespace datasystem
