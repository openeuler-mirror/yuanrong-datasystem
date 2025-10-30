/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Test ZMQ pluggin
 */
#include <cstdlib>
#include <random>

#include "common.h"
#include "zmq_test.h"
#include "zmq_curve_test_common.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_helper.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/common/rpc/rpc_unary_client_impl.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_service.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/zmq_test.stub.rpc.pb.h"

DS_DECLARE_bool(enable_curve_zmq);
DS_DECLARE_string(curve_key_dir);
DS_DECLARE_bool(log_async);
DS_DECLARE_string(unix_domain_socket_dir);
DS_DECLARE_string(encrypt_kit);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
const int DEFAULT_SERVICE_THREADS = 16;
class ZmqInternalTest : public CommonTest {};

TEST_F(ZmqInternalTest, ParseWorkerId)
{
    ZmqService *service;
    std::string oorWorkerId = std::string(WORKER_PREFIX) + "123:1234567891235";
    int32_t id;
    ASSERT_EQ(ZmqService::ParseWorkerId(oorWorkerId, service, id).GetCode(), StatusCode::K_RUNTIME_ERROR);

    std::string invalidWorkerId = std::string(WORKER_PREFIX) + "123:abc";
    ASSERT_EQ(ZmqService::ParseWorkerId(invalidWorkerId, service, id).GetCode(), StatusCode::K_RUNTIME_ERROR);

    std::string invalidWorkerId2 = std::string(WORKER_PREFIX) + "123";
    ASSERT_EQ(ZmqService::ParseWorkerId(invalidWorkerId2, service, id).GetCode(), StatusCode::K_RUNTIME_ERROR);

    std::string validWorkerId2 = std::string(WORKER_PREFIX) + "123:123";
    ASSERT_EQ(ZmqService::ParseWorkerId(validWorkerId2, service, id), Status::OK());
}

TEST_F(ZmqInternalTest, SetUpSockPath)
{
    UnixSockFd sock;
    ASSERT_EQ(sock.CreateUnixSocket(), Status::OK());
    struct sockaddr_un addr;
    RandomData randomData;
    std::string largeStr = randomData.GetRandomString(sizeof(addr.sun_path) + 1);
    ASSERT_EQ(UnixSockFd::SetUpSockPath(largeStr, addr).GetCode(), StatusCode::K_RUNTIME_ERROR);

    std::string validStr = randomData.GetRandomString(sizeof(addr.sun_path));
    ASSERT_EQ(UnixSockFd::SetUpSockPath(validStr, addr), Status::OK());
    sock.Close();
}

class ZmqTest : public CommonTest {
public:
    void SetUp() override
    {
        auto rc = StartZmqServer();
        if (rc.IsError()) {
            LOG(INFO) << "Fail to bring up zmq server. " << rc.ToString();
        }
        CreateStub();
        opt_.SetTimeout(timeoutMs_);
    }
    void TearDown() override
    {
        // Shut down the stub first because our set up is everything in the same
        // process, and as a result all share the same ZMQ context. The ZMQ
        // context will be blocked on shutdown if the stub is alive.
        if (stub) {
            stub.reset();
        }
        // Now we shutdown the server.
        if (rpc) {
            rpc->Shutdown();
        }
    }

    virtual Status StartZmqServer()
    {
        demo = std::make_unique<arbitrary::workspace::DemoServiceImpl>();
        auto bld = RpcServer::Builder();
        RpcServiceCfg cfg;
        cfg.udsEnabled_ = true;
        cfg.numRegularSockets_ = RPC_NUM_BACKEND;
        cfg.numStreamSockets_ = RPC_NUM_BACKEND;
        bld.SetDebug().AddEndPoint("tcp://127.0.0.1:*").AddService(demo.get(), cfg);
        RETURN_IF_NOT_OK(bld.InitAndStart(rpc));
        return Status::OK();
    }

    virtual void CreateStub()
    {
        HostPort serverAddress("127.0.0.1", GetServerPort());
        auto channel = std::make_shared<datasystem::RpcChannel>(serverAddress, datasystem::RpcCredential());
        channel->SetServiceUdsEnabled(arbitrary::workspace::DemoService_Stub::FullServiceName(),
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
        stub = std::make_unique<arbitrary::workspace::DemoService::Stub>(channel);
    }

    int GetServerPort() const
    {
        std::vector<std::string> ports = rpc->GetListeningPorts();
        std::cout << "Server is listening on port " << ports.front() << "\n";
        return stoi(ports.front());
    }

    static std::mt19937 GetRandomDevice()
    {
        std::mt19937 random_device{ std::random_device("/dev/urandom")() };
        return random_device;
    }

    //  Provide random number from 0..(num-1)
    static int32_t within(int32_t num)
    {
        std::mt19937 random_device = GetRandomDevice();
        std::uniform_int_distribution<int32_t> distribution(0, num - 1);
        return distribution(random_device);
    }

    void SendRecvMsg(const std::string &send)
    {
        std::unique_ptr<
            datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
            stream;
        DS_ASSERT_OK(stub->StreamGreeting(&stream));
        arbitrary::workspace::SayHelloPb req;
        req.set_msg(send);
        DS_ASSERT_OK(stream->Write(req));
        DS_ASSERT_OK(stream->Finish());
        arbitrary::workspace::ReplyHelloPb rsp;
        DS_ASSERT_OK(stream->Read(rsp));
        std::cout << rsp.reply() << std::endl;
        ASSERT_EQ(send, rsp.reply()) << rsp.reply();
    }

protected:
    std::unique_ptr<arbitrary::workspace::DemoService::Stub> stub;
    // Create socket in the class so that we can test the closing order of socket.
    std::unique_ptr<ZmqSocket> sockPtr;
    std::unique_ptr<RpcServer> rpc;
    std::unique_ptr<arbitrary::workspace::DemoServiceImpl> demo;
    const int timeoutMs_ = 2000;
    RpcOptions opt_;
};

class ZmqTcpTest : public ZmqTest {
public:
    void SetUp() override
    {
        auto rc = StartZmqServer();
        if (rc.IsError()) {
            LOG(INFO) << "Fail to bring up zmq server. " << rc.ToString();
        }
        CreateStub();
    }

    Status StartZmqServer() override
    {
        demo = std::make_unique<arbitrary::workspace::DemoServiceImpl>();
        auto bld = RpcServer::Builder();
        RpcServiceCfg cfg;
        cfg.udsEnabled_ = false;
        cfg.numRegularSockets_ = RPC_NUM_BACKEND;
        cfg.numStreamSockets_ = RPC_NUM_BACKEND;
        bld.SetDebug().AddEndPoint("tcp://127.0.0.1:*").AddService(demo.get(), cfg);
        RETURN_IF_NOT_OK(bld.InitAndStart(rpc));
        return Status::OK();
    }

    void CreateStub() override
    {
        HostPort serverAddress("127.0.0.1", GetServerPort());
        auto channel = std::make_shared<datasystem::RpcChannel>(serverAddress, datasystem::RpcCredential());
        stub = std::make_unique<arbitrary::workspace::DemoService::Stub>(channel);
    }

private:
};

class ZmqFallBackTest : public CommonTest {
public:
    void SetUp() override
    {
        auto rc = StartZmqServer();
        if (rc.IsError()) {
            LOG(INFO) << "Fail to bring up zmq server. " << rc.ToString();
        }
        CreateStub();
    }
    void TearDown() override
    {
        // Shut down the stub first because our set up is everything in the same
        // process, and as a result all share the same ZMQ context. The ZMQ
        // context will be blocked on shutdown if the stub is alive.
        if (stub) {
            stub.reset();
        }
        // Now we shutdown the server.
        if (rpc) {
            rpc->Shutdown();
        }
    }

    virtual Status StartZmqServer()
    {
        demo = std::make_unique<arbitrary::workspace::SimpleServiceImpl>();
        auto bld = RpcServer::Builder();
        RpcServiceCfg cfg;
        cfg.udsEnabled_ = false;
        cfg.numRegularSockets_ = RPC_NUM_BACKEND;
        cfg.numStreamSockets_ = RPC_NUM_BACKEND;
        bld.SetDebug().AddEndPoint("tcp://127.0.0.1:*").AddService(demo.get(), cfg);
        RETURN_IF_NOT_OK(bld.InitAndStart(rpc));
        return Status::OK();
    }

    virtual void CreateStub()
    {
        HostPort serverAddress("127.0.0.1", GetServerPort());
        auto channel = std::make_shared<datasystem::RpcChannel>(serverAddress, datasystem::RpcCredential());
        // Force uds but server but force will not support it
        channel->SetServiceUdsEnabled("arbitrary.workspace.SimpleService",
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
        stub = std::make_unique<arbitrary::workspace::SimpleService::Stub>(channel);
    }

    int GetServerPort() const
    {
        std::vector<std::string> ports = rpc->GetListeningPorts();
        std::cout << "Server is listening on port " << ports.front() << "\n";
        return stoi(ports.front());
    }

protected:
    std::unique_ptr<arbitrary::workspace::SimpleService::Stub> stub;
    std::unique_ptr<RpcServer> rpc;
    std::unique_ptr<arbitrary::workspace::SimpleServiceImpl> demo;
};

class ZmqMTPTest : public CommonTest {
public:
    void SetUp() override
    {
        auto rc = StartZmqServer();
        if (rc.IsError()) {
            LOG(INFO) << "Fail to bring up zmq server. " << rc.ToString();
        }
        CreateStub();
    }
    void TearDown() override
    {
        // Shut down the stub first because our set up is everything in the same
        // process, and as a result all share the same ZMQ context. The ZMQ
        // context will be blocked on shutdown if the stub is alive.
        if (stub) {
            stub.reset();
        }
        // Now we shutdown the server.
        if (rpc) {
            rpc->Shutdown();
        }
    }

    virtual Status StartZmqServer()
    {
        demo = std::make_unique<arbitrary::workspace::MTPServiceImpl>();
        auto bld = RpcServer::Builder();
        RpcServiceCfg cfg;
        cfg.udsEnabled_ = false;
        cfg.tcpDirect_ = "*";
        cfg.numRegularSockets_ = RPC_NUM_BACKEND;
        cfg.numStreamSockets_ = RPC_NUM_BACKEND;
        bld.SetDebug().AddEndPoint("tcp://127.0.0.1:*").AddService(demo.get(), cfg);
        RETURN_IF_NOT_OK(bld.InitAndStart(rpc));
        return Status::OK();
    }

    virtual void CreateStub()
    {
        HostPort serverAddress("127.0.0.1", GetServerPort());
        auto channel = std::make_shared<datasystem::RpcChannel>(serverAddress, datasystem::RpcCredential());
        // Force uds but server but force will not support it
        channel->SetServiceTcpDirect("arbitrary.workspace.MTPService");
        stub = std::make_unique<arbitrary::workspace::MTPService::Stub>(channel);
    }

    int GetServerPort() const
    {
        std::vector<std::string> ports = rpc->GetListeningPorts();
        std::cout << "Server is listening on port " << ports.front() << "\n";
        return stoi(ports.front());
    }

protected:
    std::unique_ptr<arbitrary::workspace::MTPService::Stub> stub;
    std::unique_ptr<RpcServer> rpc;
    std::unique_ptr<arbitrary::workspace::MTPServiceImpl> demo;
};

class ZmqForkTest : public ZmqTest {
public:
    void SetUp() override
    {
    }

    void TearDown() override
    {
        ZmqTest::TearDown();
    }
};

class ZmqConnTest : public ZmqTest {
public:
    void SetUp() override
    {
        auto rc = StartZmqServer();
        if (rc.IsError()) {
            LOG(INFO) << "Fail to bring up zmq server. " << rc.ToString();
        }
    }

    void TearDown() override
    {
        ZmqTest::TearDown();
    }
};

// global variable for DFX test purposes

class DfxClient {
public:
    DfxClient() = default;
    ~DfxClient()
    {
        if (dfxThread) {
            dfxExitFlag = 1;
            dfxThread->join();
        }
    }

    volatile sig_atomic_t dfxExitFlag = 0;
    std::unique_ptr<arbitrary::workspace::SimpleService::Stub> dfxStub{ nullptr };
    std::unique_ptr<Thread> dfxThread{ nullptr };
};

static DfxClient dfxClient;
class ZmqDfxTest : public CommonTest {
public:
    void SetUp() override
    {
        auto rc = StartZmqServer();
        if (rc.IsError()) {
            LOG(INFO) << "Fail to bring up zmq server. " << rc.ToString();
        }
        CreateStub();
    }
    void TearDown() override
    {
        // For Dfx purposes, not going to clean up the stubs here
        // Now we shutdown the server.
        if (rpc) {
            rpc->Shutdown();
        }
    }

    virtual Status StartZmqServer()
    {
        demo = std::make_unique<arbitrary::workspace::SimpleServiceImpl>();
        auto bld = RpcServer::Builder();
        RpcServiceCfg cfg;
        cfg.udsEnabled_ = false;
        cfg.numRegularSockets_ = RPC_NUM_BACKEND;
        cfg.numStreamSockets_ = RPC_NUM_BACKEND;
        bld.SetDebug().AddEndPoint("tcp://127.0.0.1:*").AddService(demo.get(), cfg);
        RETURN_IF_NOT_OK(bld.InitAndStart(rpc));
        return Status::OK();
    }

    virtual void CreateStub()
    {
        HostPort serverAddress("127.0.0.1", GetServerPort());
        auto channel = std::make_shared<datasystem::RpcChannel>(serverAddress, datasystem::RpcCredential());
        // Force uds but server but force will not support it
        channel->SetServiceUdsEnabled("arbitrary.workspace.SimpleService",
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
        dfxClient.dfxStub = std::make_unique<arbitrary::workspace::SimpleService::Stub>(channel);
    }

    int GetServerPort() const
    {
        std::vector<std::string> ports = rpc->GetListeningPorts();
        std::cout << "Server is listening on port " << ports.front() << "\n";
        return stoi(ports.front());
    }

protected:
    std::unique_ptr<RpcServer> rpc;
    std::unique_ptr<arbitrary::workspace::SimpleServiceImpl> demo;
};

TEST_F(ZmqTest, NonExistingUnixPath)
{
    HostPort dummy("127.0.0.1", 1234);
    auto sockDir = FormatString("%s/%s", FLAGS_unix_domain_socket_dir, GetStringUuid().substr(0, ZMQ_EIGHT));
    RpcServer::Builder bld;
    auto sockPath = RpcChannel::UnixSocketPath(sockDir, dummy);
    bld.SetDebug().AddEndPoint(sockPath).AddService(demo.get(), RpcServiceCfg());
    // It should fail because the sockDir doesn't exist.
    std::unique_ptr<RpcServer> server;
    auto rc = bld.InitAndStart(server);
    LOG(INFO) << FormatString("Start server using non-existing path %s rc %s", sockPath, rc.ToString());
    EXPECT_TRUE(rc.IsError());
}

TEST_F(ZmqTest, CachingTest)
{
    // Enable connection caching
    stub->CacheSession(true);
    Status rc;
    arbitrary::workspace::SayHelloPb hi;
    hi.set_msg("Hello");
    std::cout << "I said " << hi.msg() << std::endl;
    // First async call and in return we got a tag back. We will use it later
    // to check result.
    int64_t tag1;
    rc = stub->SimpleGreetingAsyncWrite(hi, tag1);
    EXPECT_TRUE(rc.IsOk());
    // Now we check the result.
    arbitrary::workspace::ReplyHelloPb reply;
    rc = stub->SimpleGreetingAsyncRead(tag1, reply);
    EXPECT_TRUE(rc.IsOk());
    std::cout << "Reply from server: " << reply.reply() << std::endl;
    stub->CacheSession(false);
}

TEST_F(ZmqTest, SimpleGreeting)
{
    Status rc;
    arbitrary::workspace::SayHelloPb hi;
    hi.set_msg("Hello");
    std::cout << "I said " << hi.msg() << std::endl;
    // First async call and in return we got a tag back. We will use it later
    // to check result.
    int64_t tag1;
    rc = stub->SimpleGreetingAsyncWrite(hi, tag1);
    EXPECT_TRUE(rc.IsOk());
    // Now we check the result.
    arbitrary::workspace::ReplyHelloPb reply;
    rc = stub->SimpleGreetingAsyncRead(tag1, reply);
    EXPECT_TRUE(rc.IsOk());
    std::cout << "Reply from server: " << reply.reply() << std::endl;
    // Some time out testing.
    hi.clear_msg();
    reply.clear_reply();
    hi.set_msg("World");
    RpcOptions opts;
    opts.SetTimeout(500);
    rc = stub->SimpleGreetingAsyncWrite(opts, hi, tag1);
    EXPECT_TRUE(rc.IsOk());
    rc = stub->SimpleGreetingAsyncRead(tag1, reply);
    EXPECT_TRUE(rc.IsError());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Send a different message. Expect the server will drop the previous result.
    hi.clear_msg();
    reply.clear_reply();
    hi.set_msg("Hello");
    rc = stub->SimpleGreetingAsyncWrite(hi, tag1);
    EXPECT_TRUE(rc.IsOk());
    rc = stub->SimpleGreetingAsyncRead(tag1, reply);
    EXPECT_TRUE(rc.IsOk());
    EXPECT_EQ(reply.reply(), "World");
    // Repeat the same tests by sending two messages but cancel the first request.
    // We expect to get the request of the second message
    hi.clear_msg();
    hi.set_msg("Hello");
    arbitrary::workspace::SayHelloPb world;
    world.set_msg("World");
    int64_t tag2;
    rc = stub->SimpleGreetingAsyncWrite(hi, tag1);
    EXPECT_TRUE(rc.IsOk());
    rc = stub->SimpleGreetingAsyncWrite(world, tag2);
    EXPECT_TRUE(rc.IsOk());
    // Cancel the first one.
    stub->ForgetRequest(tag1);
    // Get the reply from the 2nd one.
    reply.clear_reply();
    rc = stub->SimpleGreetingAsyncRead(tag2, reply);
    EXPECT_TRUE(rc.IsOk());
    EXPECT_EQ(reply.reply(), "Hello");
    // The first request should get error if reuse
    rc = stub->SimpleGreetingAsyncRead(tag1, reply);
    EXPECT_TRUE(rc.IsError());
}

TEST_F(ZmqTest, ClientStreamGreeting)
{
    Status rc;
    // Client side is streaming.
    std::unique_ptr<datasystem::ClientWriter<arbitrary::workspace::SayHelloPb>> writer;
    rc = stub->ClientStreamGreeting(&writer);
    EXPECT_TRUE(rc.IsOk());
    const int numIter = 5;
    for (int i = 0; i < numIter; ++i) {
        arbitrary::workspace::SayHelloPb pb;
        pb.set_msg("Hello");
        rc = writer->Write(pb);
        EXPECT_TRUE(rc.IsOk());
    }
    rc = writer->Finish();
    EXPECT_TRUE(rc.IsOk());
    arbitrary::workspace::ReplyHelloPb reply;
    rc = writer->Read(reply);
    EXPECT_TRUE(rc.IsOk());
    std::cout << reply.reply() << std::endl;
    int v = stoi(reply.reply());
    EXPECT_EQ(v, numIter);
}

TEST_F(ZmqTest, ClientStreamWithPayload)
{
    Status rc;
    // Client side is streaming.
    std::unique_ptr<datasystem::ClientWriter<arbitrary::workspace::SayHelloPb>> writer;
    rc = stub->ClientStreamWithPayload(&writer);
    EXPECT_TRUE(rc.IsOk());
    const int numIter = 5;
    for (int i = 0; i < numIter; ++i) {
        arbitrary::workspace::SayHelloPb pb;
        pb.set_msg("Hello");
        rc = writer->Write(pb);
        EXPECT_TRUE(rc.IsOk());
    }
    rc = writer->Finish();
    EXPECT_TRUE(rc.IsOk());
    arbitrary::workspace::ReplyHelloPb reply;
    rc = writer->Read(reply);
    EXPECT_TRUE(rc.IsOk());
    std::cout << reply.reply() << std::endl;
    int v = stoi(reply.reply());
    EXPECT_EQ(v, numIter);
    std::vector<RpcMessage> recvBuffer;
    // One more payload which should be "ABC"
    rc = writer->ReceivePayload(recvBuffer);
    EXPECT_TRUE(rc.IsOk());
    auto &buf = recvBuffer[0];
    auto n = strncmp(reinterpret_cast<char *>(buf.Data()), "ABC", 3);
    EXPECT_EQ(n, 0);
}

TEST_F(ZmqTest, ServerStreamGreeting)
{
    Status rc;
    // Server side is streaming.
    std::unique_ptr<datasystem::ClientReader<arbitrary::workspace::ReplyHelloPb>> reader;
    arbitrary::workspace::SayHelloPb hi;
    hi.set_msg("Hello");
    rc = stub->ServerStreamGreeting(&reader, hi);
    EXPECT_TRUE(rc.IsOk());
    arbitrary::workspace::ReplyHelloPb pb;
    int count = 0;
    while ((rc = reader->Read(pb)).IsOk()) {
        ++count;
        std::cout << pb.reply() << std::endl;
    }
    EXPECT_TRUE(rc.GetCode() == datasystem::StatusCode(datasystem::K_RPC_STREAM_END));
    EXPECT_EQ(count, 5);
}

TEST_F(ZmqTest, ServerStreamWithPayload)
{
    Status rc;
    // Server side is streaming.
    std::unique_ptr<datasystem::ClientReader<arbitrary::workspace::ReplyHelloPb>> reader;
    arbitrary::workspace::SayHelloPb hi;
    hi.set_msg("Hello");
    char buf[3] = { 0 };
    MemView payload(buf, 3);
    rc = stub->ServerStreamWithPayload(&reader, hi, { payload });
    EXPECT_TRUE(rc.IsOk());
    arbitrary::workspace::ReplyHelloPb pb;
    int count = 0;
    while ((rc = reader->Read(pb)).IsOk()) {
        ++count;
        std::cout << pb.reply() << std::endl;
    }
    EXPECT_TRUE(rc.GetCode() == datasystem::StatusCode(datasystem::K_RPC_STREAM_END));
    EXPECT_EQ(count, 5);
}

TEST_F(ZmqTest, LEVEL1_StreamGreeting)
{
    // The basic both sides streaming case.
    Status rc;
    std::unique_ptr<
        datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
        stream;
    DS_ASSERT_OK(stub->StreamGreeting(&stream));
    const int numIter = 5;
    for (int i = 0; i < numIter; ++i) {
        arbitrary::workspace::SayHelloPb pb;
        pb.set_msg("Hello");
        DS_ASSERT_OK(stream->Write(pb));
    }
    DS_ASSERT_OK(stream->Finish());
    arbitrary::workspace::ReplyHelloPb pb;
    int count = 0;
    while ((rc = stream->Read(pb)).IsOk()) {
        ++count;
        LOG(INFO) << pb.reply();
    }
    EXPECT_EQ(rc.GetCode(), datasystem::K_RPC_STREAM_END);
    EXPECT_EQ(count, numIter);
}

TEST_F(ZmqTest, LEVEL1_StreamGreetingTestIdle)
{
    // Test both side streaming case with 1 minute idle time.
    Status rc;
    std::unique_ptr<
        datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
        stream;
    rc = stub->StreamGreeting(&stream);
    EXPECT_TRUE(rc.IsOk());
    const int numIter = 5;
    for (int i = 0; i < numIter; ++i) {
        arbitrary::workspace::SayHelloPb pb;
        pb.set_msg("Hello");
        rc = stream->Write(pb);
        EXPECT_TRUE(rc.IsOk());
    }
    // Sleep one minute to test idleness
    const int TEN_SEC = 10;
    std::this_thread::sleep_for(std::chrono::seconds(TEN_SEC));
    for (int i = 0; i < numIter; ++i) {
        arbitrary::workspace::SayHelloPb pb;
        pb.set_msg("Hello");
        rc = stream->Write(pb);
        EXPECT_TRUE(rc.IsOk());
    }
    rc = stream->Finish();
    EXPECT_TRUE(rc.IsOk());
    arbitrary::workspace::ReplyHelloPb pb;
    int count = 0;
    while ((rc = stream->Read(pb)).IsOk()) {
        ++count;
        std::cout << pb.reply() << std::endl;
    }
    EXPECT_TRUE(rc.GetCode() == datasystem::StatusCode(datasystem::K_RPC_STREAM_END));
    EXPECT_EQ(count, numIter + numIter);
}

TEST_F(ZmqTest, LEVEL2_StreamGreetingWithTimeout1)
{
    Status rc;
    std::unique_ptr<
        datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
        clientStreamApi;
    rc = stub->StreamGreeting(&clientStreamApi);
    EXPECT_TRUE(rc.IsOk());
    Timer timer;
    // Set timeout basic in 1 second
    const uint64_t timeoutBasicMs = 1000;
    const int normIter = 5;
    for (int i = 0; i < normIter; ++i) {
        if (i == normIter - 1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(30 * timeoutBasicMs));
        }
        std::stringstream ss;
        ss << FormatString("\n============= Round:%d test begin =============\n", i);
        if (timer.ElapsedMilliSecondAndReset() >= RPC_BACKEND_TIMEOUT) {
            ss << FormatString("Before round:%d test, detect stream rpc connection timeout, hence we reset it\n", i);
            rc = clientStreamApi->Finish();
            EXPECT_TRUE(rc.IsOk());
            rc = stub->StreamGreeting(&clientStreamApi);
            EXPECT_TRUE(rc.IsOk());
        }
        arbitrary::workspace::SayHelloPb req;
        req.set_msg(FormatString("Hello:%d\n", i));
        ss << FormatString("Round %d request is:%s\n", i, req.msg());
        rc = clientStreamApi->Write(req);
        EXPECT_TRUE(rc.IsOk());
        arbitrary::workspace::ReplyHelloPb rsp;
        rc = clientStreamApi->Read(rsp);
        if (rc.IsError()) {
            ss << FormatString("Error msg:%s\n", rc.ToString());
        }
        EXPECT_TRUE(rc.IsOk());
        ss << FormatString("Round %d response is:%s\n", i, rsp.reply());
        ss << FormatString("============= Round:%d test end =============\n", i);
        LOG(INFO) << ss.str();
    }
    rc = clientStreamApi->Finish();
    EXPECT_TRUE(rc.IsOk());
}

TEST_F(ZmqTest, LEVEL2_StreamGreetingWithTimeout2)
{
    Status rc;
    std::unique_ptr<
        datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
        clientStreamApi;
    rc = stub->StreamGreeting(&clientStreamApi);
    EXPECT_TRUE(rc.IsOk());
    Timer timer;
    // Set timeout basic in 1 second
    const uint64_t timeoutBasicMs = 1000;
    const int normIter = 5;
    for (int i = 0; i < normIter; ++i) {
        if (i == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2 * timeoutBasicMs));
        }
        if (i == normIter - 1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(30 * timeoutBasicMs));
        }
        std::stringstream ss;
        ss << FormatString("\n============= Round:%d test begin =============\n", i);
        arbitrary::workspace::SayHelloPb req;
        req.set_msg(FormatString("Hello-%d\n", i));
        ss << FormatString("Round %d request is:%s\n", i, req.msg());
        rc = clientStreamApi->Write(req);
        EXPECT_TRUE(rc.IsOk());
        arbitrary::workspace::ReplyHelloPb rsp;
        rc = clientStreamApi->Read(rsp);

        if (rc.IsError()) {
            LOG(ERROR) << FormatString("In Round %d test, Error msg:%s\n", i, rc.ToString());
            auto code = rc.GetCode();
            if (code == K_RPC_CANCELLED || code == K_RPC_UNAVAILABLE) {
                ss << FormatString("In Round %d test, clientStreamApi broken, we refresh it\n", i);
                rc = clientStreamApi->Finish();
                EXPECT_TRUE(rc.IsOk());
                rc = stub->StreamGreeting(&clientStreamApi);
                EXPECT_TRUE(rc.IsOk());
                rc = clientStreamApi->Write(req);
                EXPECT_TRUE(rc.IsOk());
                rc = clientStreamApi->Read(rsp);
            }
        }
        EXPECT_TRUE(rc.IsOk());
        ss << FormatString("Round %d response is:%s\n", i, rsp.reply());
        ss << FormatString("============= Round:%d test end =============\n", i);
        LOG(INFO) << ss.str();
    }
    rc = clientStreamApi->Finish();
    EXPECT_TRUE(rc.IsOk());
}

TEST_F(ZmqTest, EmbeddedPayload)
{
    Status rc;
    auto sz = 5 * 1024 * 1024;
    auto half_sz = sz / 2;
    auto buf = std::make_unique<char[]>(sz);
    std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
    ifs.read(buf.get(), sz);
    ifs.close();
    uint32_t checksum = GetCrc32(buf.get(), sz);
    int64_t tag4;
    MemView payload1(buf.get(), half_sz);
    MemView payload2(buf.get() + half_sz, half_sz);
    rc = stub->SendBigBufAsyncWrite(arbitrary::workspace::SayHelloPb(), tag4, { payload1, payload2 });
    EXPECT_TRUE(rc.IsOk());
    // Check the results.
    arbitrary::workspace::ChecksumPb crc32;
    rc = stub->SendBigBufAsyncRead(tag4, crc32);
    EXPECT_TRUE(rc.IsOk());
    std::cout << "Reply from server: " << crc32.crc32() << std::endl;
    // Verify they match
    ASSERT_EQ(crc32.crc32(), checksum);
}

TEST_F(ZmqTest, StreamSendBigBuf)
{
    Status rc;
    auto sz = 10 * 1024 * 1024;
    auto half_sz = sz / 2;
    auto buf = std::make_unique<char[]>(sz);
    std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
    ifs.read(buf.get(), sz);
    ifs.close();
    uint32_t checksum = GetCrc32(buf.get(), sz);
    MemView payload1(buf.get(), half_sz);
    MemView payload2(buf.get() + half_sz, half_sz);

    auto begin = std::chrono::high_resolution_clock::now();
    std::unique_ptr<datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ChecksumPb>>
        stream;
    rc = stub->StreamSendBigBuf(&stream);
    for (auto i = 0; i < 8; ++i) {
        rc = stream->Write(arbitrary::workspace::SayHelloPb());
        EXPECT_TRUE(rc.IsOk());
        rc = stream->SendPayload({ payload1, payload2 });
        EXPECT_TRUE(rc.IsOk());
        arbitrary::workspace::ChecksumPb crc32;
        rc = stream->Read(crc32);
        EXPECT_TRUE(rc.IsOk());
        std::cout << "Reply from server: " << crc32.crc32() << std::endl;
        // Verify they match
        ASSERT_EQ(crc32.crc32(), checksum);
    }
    rc = stream->Finish();
    EXPECT_TRUE(rc.IsOk());
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    LOG(INFO) << "Time profiling:" << elapsed;
    sleep(1);
}

TEST_F(ZmqTest, StreamSendBigBufAsync)
{
    Status rc;
    auto sz = 10 * 1024 * 1024;
    auto half_sz = sz / 2;
    auto buf = std::make_unique<char[]>(sz);
    std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
    ifs.read(buf.get(), sz);
    ifs.close();
    uint32_t checksum = GetCrc32(buf.get(), sz);
    MemView payload1(buf.get(), half_sz);
    MemView payload2(buf.get() + half_sz, half_sz);

    ThreadPool pool(8);
    auto begin = std::chrono::high_resolution_clock::now();
    std::vector<std::unique_ptr<
        datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ChecksumPb>>>
        streams;
    for (int i = 0; i < 8; ++i) {
        std::unique_ptr<
            datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ChecksumPb>>
            stream;
        rc = stub->StreamSendBigBuf(&stream);
        streams.emplace_back(std::move(stream));
    }
    std::vector<std::future<void>> asyncFutures;
    for (auto i = 0; i < 8; ++i) {
        auto stream = streams[i].get();
        asyncFutures.emplace_back(pool.Submit([&payload1, &payload2, checksum, stream]() {
            DS_ASSERT_OK(stream->Write(arbitrary::workspace::SayHelloPb()));
            DS_ASSERT_OK(stream->SendPayload({ payload1, payload2 }));
            arbitrary::workspace::ChecksumPb crc32;
            DS_ASSERT_OK(stream->Read(crc32));
            std::cout << "Reply from server: " << crc32.crc32() << std::endl;
            // Verify they match
            ASSERT_EQ(crc32.crc32(), checksum);
            DS_ASSERT_OK(stream->Finish());
        }));
    }
    for (int i = 0; i < 5; ++i) {
        asyncFutures[i].get();
    }
    auto end = std::chrono::high_resolution_clock::now();
    uint64_t elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    LOG(INFO) << "Time profiling:" << elapsed;
    sleep(1);
}

TEST_F(ZmqTest, RecvBigBuf)
{
    Status rc;
    // Receive a payload buffer from server
    arbitrary::workspace::SayHelloPb hi;
    hi.set_msg("Receiving a payload");
    int64_t tag;
    rc = stub->RecvBigBufAsyncWrite(hi, tag);
    EXPECT_TRUE(rc.IsOk());
    arbitrary::workspace::ChecksumPb pb;
    std::vector<RpcMessage> recvBuffer;
    rc = stub->RecvBigBufAsyncRead(tag, pb, recvBuffer);
    EXPECT_TRUE(rc.IsOk());
    auto &buf = recvBuffer[0];
    auto sz = buf.Size();
    std::cout << "Receive a payload buffer of size " << sz << std::endl;
    uint32_t checksum = GetCrc32(buf.Data(), sz);
    ASSERT_EQ(pb.crc32(), checksum);
}

TEST_F(ZmqTest, StreamRecvBigBuf)
{
    Status rc;
    std::unique_ptr<datasystem::ClientWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ChecksumPb>>
        stream;
    rc = stub->StreamRecvBigBuf(&stream);
    EXPECT_TRUE(rc.IsOk());
    for (auto i = 0; i < 5; ++i) {
        // Receive a payload buffer from server
        arbitrary::workspace::SayHelloPb hi;
        hi.set_msg("Receiving a payload");
        rc = stream->Write(hi);
        EXPECT_TRUE(rc.IsOk());
        std::vector<RpcMessage> recvBuffer;
        arbitrary::workspace::ChecksumPb pb;
        rc = stream->Read(pb);
        EXPECT_TRUE(rc.IsOk());
        rc = stream->ReceivePayload(recvBuffer);
        EXPECT_TRUE(rc.IsOk());
        auto &payload = recvBuffer[0];
        auto sz = payload.Size();
        std::cout << "Receive a payload buffer of size " << sz << std::endl;
        uint32_t checksum = GetCrc32(payload.Data(), sz);
        ASSERT_EQ(pb.crc32(), checksum);
    }
    rc = stream->Finish();
    EXPECT_TRUE(rc.IsOk());
    sleep(1);
}

TEST_F(ZmqTest, TestSendingJunk)
{
    Status rc;
    auto ctx = std::make_shared<ZmqContext>();
    rc = ctx->Init();
    EXPECT_TRUE(rc.IsOk());
    sockPtr = std::make_unique<ZmqSocket>(ctx, ZmqSocketType::DEALER);
    HostPort serverAddress("127.0.0.1", GetServerPort());
    auto channel = std::make_shared<datasystem::RpcChannel>(serverAddress, datasystem::RpcCredential());
    rc = sockPtr->Connect(*channel);
    EXPECT_TRUE(rc.IsOk());
    // This sends some junk to the server which should ignore it.
    rc = sockPtr->ZmqSendString("Hello");
    EXPECT_TRUE(rc.IsOk());
    // Now redo SimpleGreeting.
    arbitrary::workspace::SayHelloPb hi;
    hi.set_msg("Are you still there ?");
    std::cout << "I said " << hi.msg() << std::endl;
    // First async call and in return we got a tag back. We will use it later
    // to check result.
    int64_t tag1;
    rc = stub->SimpleGreetingAsyncWrite(hi, tag1);
    EXPECT_TRUE(rc.IsOk());
    // Now we check the result.
    arbitrary::workspace::ReplyHelloPb reply;
    rc = stub->SimpleGreetingAsyncRead(tag1, reply);
    EXPECT_TRUE(rc.IsOk());
    std::cout << "Reply from server: " << reply.reply() << std::endl;
    ctx->Close();
}

TEST_F(ZmqTest, MultiThreadStreamGreeting)
{
    int threadNum = 10;
    std::vector<std::thread> threads;
    std::vector<std::pair<std::string, std::string>> inOutMsg(threadNum);
    for (int i = 0; i < threadNum; i++) {
        threads.emplace_back([this]() {
            for (int j = 0; j < 100; j++) {
                SendRecvMsg(GetStringUuid());
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(ZmqTest, MultiThreadSendBigBuf)
{
    auto sz = 1024 * 1024;
    auto buf = std::make_unique<char[]>(sz);
    std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
    ifs.read(buf.get(), sz);
    ifs.close();
    uint32_t checksum = GetCrc32(buf.get(), sz);
    int threadNum = 50;
    std::vector<std::thread> threads;
    std::vector<std::pair<std::string, std::string>> inOutMsg(threadNum);
    for (int i = 0; i < threadNum; i++) {
        threads.emplace_back([this, &buf, &sz, &checksum]() {
            HostPort serverAddress("127.0.0.1", GetServerPort());
            auto channel = std::make_shared<datasystem::RpcChannel>(serverAddress, datasystem::RpcCredential());
            auto myStub = std::make_unique<arbitrary::workspace::DemoService::Stub>(channel);
            for (int j = 0; j < 200; j++) {
                std::string val = GetStringUuid();
                arbitrary::workspace::SayHelloPb hi;
                arbitrary::workspace::ChecksumPb reply;
                hi.set_msg(val);
                int64_t tag;
                MemView buffer(buf.get(), sz);
                EXPECT_EQ(myStub->SendBigBufAsyncWrite(hi, tag, { buffer }), Status::OK());
                EXPECT_EQ(myStub->SendBigBufAsyncRead(tag, reply), Status::OK());
                EXPECT_EQ(reply.crc32(), checksum);
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(ZmqTest, Restart)
{
    std::vector<std::string> ports = rpc->GetListeningPorts();
    rpc->Shutdown();
    rpc.reset();
    RpcOptions opts;
    opts.SetTimeout(1000);
    for (auto i = 0; i < 10; ++i) {
        std::string msg = GetStringUuid();
        arbitrary::workspace::SayHelloPb req;
        arbitrary::workspace::ReplyHelloPb rsp;
        req.set_msg(msg);
        Status rc = stub->SimpleGreeting(opts, req, rsp);
        bool res = (rc.GetCode() == StatusCode::K_RPC_UNAVAILABLE) || (rc.GetCode() == StatusCode::K_RPC_CANCELLED);
        LOG(INFO) << rc.ToString();
        EXPECT_EQ(res, true);
    }
    stub.reset();
    std::this_thread::sleep_for(std::chrono::seconds(5));
    // Bring back up the server
    demo = std::make_unique<arbitrary::workspace::DemoServiceImpl>();
    auto bld = RpcServer::Builder();
    RpcServiceCfg cfg;
    cfg.udsEnabled_ = true;
    cfg.numRegularSockets_ = DEFAULT_SERVICE_THREADS;
    cfg.numStreamSockets_ = DEFAULT_SERVICE_THREADS;
    bld.SetDebug().AddEndPoint("tcp://127.0.0.1:" + ports.front()).AddService(demo.get(), cfg);
    DS_ASSERT_OK(bld.InitAndStart(rpc));
    std::string msg = GetStringUuid();
    arbitrary::workspace::SayHelloPb req;
    req.set_msg(msg);
    int64_t tag;
    CreateStub();
    DS_ASSERT_OK(stub->SimpleGreetingAsyncWrite(req, tag));
    arbitrary::workspace::ReplyHelloPb rsp;
    DS_ASSERT_OK(stub->SimpleGreetingAsyncRead(tag, rsp));
    EXPECT_EQ(msg, rsp.reply());
}

TEST_F(ZmqTest, TimeoutWaitForConnectedTest)
{
    Status rc;
    arbitrary::workspace::SayHelloPb req;
    arbitrary::workspace::ReplyHelloPb rsp;
    DS_ASSERT_OK(datasystem::inject::Set("SockConnEntry.WaitForConnected.SetTimeout", "call(0)"));
    rc = stub->UnarySocketSimpleGreeting(req, rsp);
    EXPECT_TRUE(rc.IsOk());
}

TEST_F(ZmqTest, UnarySocketSimpleGreeting)
{
    Status rc;
    arbitrary::workspace::SayHelloPb req;
    arbitrary::workspace::ReplyHelloPb rsp;
    rc = stub->UnarySocketSimpleGreeting(req, rsp);
    EXPECT_TRUE(rc.IsOk());
}

TEST_F(ZmqTest, UnarySocketSendPayload)
{
    Status rc;
    auto sz = 5 * 1024 * 1024;
    auto half_sz = sz / 2;
    auto buf = std::make_unique<char[]>(sz);
    std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
    ifs.read(buf.get(), sz);
    ifs.close();
    uint32_t checksum = GetCrc32(buf.get(), sz);
    MemView payload1(buf.get(), half_sz);
    MemView payload2(buf.get() + half_sz, half_sz);
    arbitrary::workspace::ChecksumPb crc32;
    rc = stub->UnarySocketSendPayload(arbitrary::workspace::SayHelloPb(), crc32, { payload1, payload2 });
    EXPECT_TRUE(rc.IsOk());
    // Check the results.
    std::cout << "Reply from server: " << crc32.crc32() << std::endl;
    // Verify they match
    ASSERT_EQ(crc32.crc32(), checksum);
}

TEST_F(ZmqTest, UnarySocketRecvPayload)
{
    Status rc;
    std::vector<RpcMessage> recvBuffer;
    arbitrary::workspace::ChecksumPb crc32;
    rc = stub->UnarySocketRecvPayload(arbitrary::workspace::SayHelloPb(), crc32, recvBuffer);
    EXPECT_TRUE(rc.IsOk());
    auto &payload = recvBuffer[0];
    auto sz = payload.Size();
    std::cout << "Receive a payload buffer of size: " << sz << std::endl;
    uint32_t checksum = GetCrc32(payload.Data(), sz);
    // Verify they match
    ASSERT_EQ(crc32.crc32(), checksum);
}

// Test the case where the server returned an error
TEST_F(ZmqTest, UnarySocketSimpleError)
{
    Status rc;
    arbitrary::workspace::SayHelloPb req;
    req.set_msg("Hello");
    arbitrary::workspace::ReplyHelloPb rsp;
    rc = stub->UnarySocketSimpleError(req, rsp);
    EXPECT_TRUE(rc.IsError());
}

TEST_F(ZmqTest, SimpleGreetingCorruptMetaPb1)
{
    Status rc;
    arbitrary::workspace::SayHelloPb req;
    arbitrary::workspace::ReplyHelloPb rsp;
    DS_ASSERT_OK(datasystem::inject::Set("ZmqFrontend.CorruptMetaPb", "call()"));
    rc = stub->UnarySocketSimpleGreeting(req, rsp);
    EXPECT_TRUE(rc.IsError());
}

// Crash happens in ServiceToClient when ClientToService deallocates the FdEvent
TEST_F(ZmqTest, ZmqFdEventCrash1)
{
    LOG(INFO) << "ZmqFdEventCrash1 start";
    const int NUM_THREADS = 2;
    ThreadPool pool(NUM_THREADS);
    auto r1 = pool.Submit([this]() {
        // Thread1 make ServiceToClient wait for accessing MetaPb
        arbitrary::workspace::SayHelloPb req;
        req.set_msg("Hello");
        arbitrary::workspace::ReplyHelloPb rsp;
        DS_ASSERT_OK(datasystem::inject::Set("IOService.ServiceToClient.wait", "call(1000)"));
        DS_ASSERT_NOT_OK(stub->UnarySocketSimpleGreeting(opt_, req, rsp));
    });

    auto r2 = pool.Submit([this]() {
        sleep(1);
        // Thread2 makes ClientToService wait for in RemoveFd()
        // Thread1 will access the metaPb for deleted fdEvent and causes the issue
        DS_ASSERT_OK(datasystem::inject::Set("IOService.ClientToService.SetEvent", "call()"));
        arbitrary::workspace::SayHelloPb req1;
        req1.set_msg("Hello1");
        arbitrary::workspace::ReplyHelloPb rsp;
        DS_ASSERT_NOT_OK(stub->UnarySocketSimpleGreeting(opt_, req1, rsp));
    });

    r1.get();
    r2.get();
}

// Crash happens in ClientToService when ServiceToClient deallocates the FdEvent
TEST_F(ZmqTest, ZmqFdEventCrash2)
{
    const int NUM_THREADS = 2;
    ThreadPool pool(NUM_THREADS);
    auto r1 = pool.Submit([this]() {
        // Thread1 make ServiceToClient wait for accessing MetaPb
        arbitrary::workspace::SayHelloPb req;
        req.set_msg("Hello");
        arbitrary::workspace::ReplyHelloPb rsp;
        DS_ASSERT_OK(datasystem::inject::Set("IOService.ClientToService.wait", "call(1000)"));
        DS_ASSERT_NOT_OK(stub->UnarySocketSimpleGreeting(opt_, req, rsp));
    });

    auto r2 = pool.Submit([this]() {
        sleep(1);
        // Thread2 makes ClientToService wait for in RemoveFd()
        // Thread1 will access the metaPb for deleted fdEvent and causes the issue
        DS_ASSERT_OK(datasystem::inject::Set("IOService.ServiceToClient.SetEvent", "call()"));
        arbitrary::workspace::SayHelloPb req1;
        req1.set_msg("Hello1");
        arbitrary::workspace::ReplyHelloPb rsp;
        DS_ASSERT_NOT_OK(stub->UnarySocketSimpleGreeting(opt_, req1, rsp));
    });

    r1.get();
    r2.get();
}

// Crash happens in ClientToService when ServiceToClient deallocates the FdEvent (K_RPC_CANCELLED)
TEST_F(ZmqTest, ZmqFdEventCrash3)
{
    const int NUM_THREADS = 2;
    ThreadPool pool(NUM_THREADS);
    auto r1 = pool.Submit([this]() {
        // Thread1 make ServiceToClient wait for accessing MetaPb
        arbitrary::workspace::SayHelloPb req;
        req.set_msg("Hello");
        arbitrary::workspace::ReplyHelloPb rsp;
        DS_ASSERT_OK(datasystem::inject::Set("IOService.ClientToService.wait", "call(1000)"));
        DS_ASSERT_OK(stub->UnarySocketSimpleGreeting(opt_, req, rsp));
    });

    auto r2 = pool.Submit([this]() {
        sleep(1);
        // Thread2 makes ClientToService wait for in RemoveFd()
        // Thread1 will access the metaPb for deleted fdEvent and causes the issue
        DS_ASSERT_OK(datasystem::inject::Set("IOService.ServiceToClient.RPC_CANCELLED", "call()"));
        arbitrary::workspace::SayHelloPb req1;
        req1.set_msg("Hello1");
        arbitrary::workspace::ReplyHelloPb rsp;
        DS_ASSERT_NOT_OK(stub->UnarySocketSimpleGreeting(opt_, req1, rsp));
    });

    r1.get();
    r2.get();
}

TEST_F(ZmqTest, SimpleGreetingCorruptMetaPb2)
{
    arbitrary::workspace::SayHelloPb req;
    arbitrary::workspace::ReplyHelloPb rsp;
    DS_ASSERT_OK(datasystem::inject::Set("IOService.CorruptMetaPb", "call()"));
    RpcOptions opt;
    // Set a shorter timeout so it does not wait for the entire RPC_TIMEOUT
    const int DEFAULT_TIMEOUT = 5000;
    opt.SetTimeout(DEFAULT_TIMEOUT);
    DS_ASSERT_NOT_OK(stub->UnarySocketSimpleGreeting(opt, req, rsp));
}

TEST_F(ZmqTest, TestZmqClientLogInitHelper1)
{
    LOG(INFO) << "Test init client log.";
    const std::string LOG_FILENAME = "client1";
    const std::string LOG_DIR = "./client_log1";
    Logging::GetInstance()->Start(LOG_FILENAME, true);
    EXPECT_FALSE(FileExist(LOG_DIR));
}

TEST_F(ZmqTest, TestZmqClientLogInitHelper2)
{
    LOG(INFO) << "Test init client log.";
    const std::string LOG_FILENAME = "client2";
    const std::string LOG_DIR = "./client_log2";

    const std::string logEnv = "DATASYSTEM_CLIENT_LOG_DIR";
    int replace = 1;
    setenv(logEnv.c_str(), LOG_DIR.c_str(), replace);
    Logging::GetInstance()->Start(LOG_FILENAME, true);
    EXPECT_FALSE(FileExist(LOG_DIR));
    unsetenv(logEnv.c_str());
}

TEST_F(ZmqTest, TestZmqClientLogInitHelper3)
{
    LOG(INFO) << "Test init client log.";
    const std::string LOG_FILENAME = "client3";
    const std::string LOG_DIR = GetTestCaseDataDir() + "/client_log3";

    const std::string logEnv = "DATASYSTEM_CLIENT_LOG_DIR";
    int replace = 1;
    setenv(logEnv.c_str(), LOG_DIR.c_str(), replace);

    FLAGS_log_dir = "";
    Logging::GetInstance()->Start(LOG_FILENAME, true);
    EXPECT_TRUE(FileExist(LOG_DIR));
    unsetenv(logEnv.c_str());
}

#ifndef USE_URMA
// Test the case where the server returned an error
TEST_F(ZmqForkTest, ForkNewStubSuccess)
{
    // Here is the plan.
    // Fork  -- parent start the server
    // Child -- Create the stub.
    //       -- Fork one more time.
    // Grandchildren -- Create a stub.
    auto createChildStub = [](int32_t port, std::unique_ptr<arbitrary::workspace::DemoService::Stub> &childStub) {
        HostPort serverAddress("127.0.0.1", port);
        auto channel = std::make_shared<datasystem::RpcChannel>(serverAddress, datasystem::RpcCredential());
        channel->SetServiceUdsEnabled(arbitrary::workspace::DemoService_Stub::FullServiceName(),
                                      GetServiceSockName(ServiceSocketNames::DEFAULT_SOCK));
        childStub = std::make_unique<arbitrary::workspace::DemoService::Stub>(channel);
    };
    constexpr static int TWO = 2;
    int pipeFd[TWO];
    auto rc = pipe(pipeFd);
    ASSERT_TRUE(rc == 0);
    pid_t pid = fork();
    int32_t port = 0;
    // If child, try to send the request
    if (pid == 0) {
        close(pipeFd[1]);
        sleep(5);
        ssize_t n = read(pipeFd[0], &port, sizeof(port));
        ASSERT_EQ(static_cast<size_t>(n), sizeof(port));
        close(pipeFd[0]);
        std::unique_ptr<arbitrary::workspace::DemoService::Stub> childStub;
        createChildStub(port, childStub);
        // Now fork again
        pid = fork();
        if (pid == 0) {
            std::unique_ptr<arbitrary::workspace::DemoService::Stub> grandChildStub;
            createChildStub(port, grandChildStub);
            arbitrary::workspace::SayHelloPb req;
            req.set_msg("Hello");
            arbitrary::workspace::ReplyHelloPb rsp;
            DS_EXPECT_OK(grandChildStub->UnarySocketSimpleGreeting(req, rsp));
            std::cout << rsp.reply() << std::endl;
            exit(testing::Test::HasFailure());
        }
        arbitrary::workspace::SayHelloPb req;
        req.set_msg("Hello");
        arbitrary::workspace::ReplyHelloPb rsp;
        DS_EXPECT_OK(childStub->UnarySocketSimpleGreeting(req, rsp));
        std::cout << rsp.reply() << std::endl;
        int status;
        waitpid(pid, &status, 0);
        int exit_status = WEXITSTATUS(status);
        ASSERT_EQ(0, exit_status);
        exit(testing::Test::HasFailure());
    }
    close(pipeFd[0]);
    DS_ASSERT_OK(StartZmqServer());
    port = GetServerPort();
    // Notify the child the port number
    ssize_t n = write(pipeFd[1], &port, sizeof(port));
    ASSERT_EQ(static_cast<size_t>(n), sizeof(port));
    close(pipeFd[1]);
    int status;
    waitpid(pid, &status, 0);
    int exit_status = WEXITSTATUS(status);
    ASSERT_EQ(0, exit_status);
}
#endif

// Test sending large buffer
TEST_F(ZmqTest, LargePayload)
{
    Status rc;
    size_t sz = 2 * 1024L * 1024L * 1024L;
    std::string a(sz, 'a');
    a[sz - 1] = 'b';
    MemView payload1(a.data(), a.size());
    uint32_t checksum = GetCrc32(a.data(), sz);
    arbitrary::workspace::ChecksumPb crc32;
    rc = stub->UnarySocketLargePayload(arbitrary::workspace::SayHelloPb(), crc32, { payload1 });
    EXPECT_TRUE(rc.IsOk());
    // Check the results.
    std::cout << "Reply from server: " << crc32.crc32() << std::endl;
    // Verify they match
    ASSERT_EQ(crc32.crc32(), checksum);
}

TEST_F(ZmqTcpTest, LargePayload)
{
    Status rc;
    size_t sz = 2 * 1024L * 1024L * 1024L;
    std::string a(sz, 'a');
    a[sz - 1] = 'b';
    MemView payload1(a.data(), a.size());
    uint32_t checksum = GetCrc32(a.data(), sz);
    arbitrary::workspace::ChecksumPb crc32;
    rc = stub->UnarySocketLargePayload(arbitrary::workspace::SayHelloPb(), crc32, { payload1 });
    EXPECT_TRUE(rc.IsOk());
    // Check the results.
    EXPECT_TRUE(rc.IsOk());
    std::cout << "Reply from server: " << crc32.crc32() << std::endl;
    // Verify they match
    ASSERT_EQ(crc32.crc32(), checksum);
}

TEST_F(ZmqFallBackTest, UdsFallBack)
{
    arbitrary::workspace::SayHelloPb hi;
    hi.set_msg("Hello");
    std::cout << "I said " << hi.msg() << std::endl;
    arbitrary::workspace::ReplyHelloPb reply;
    auto rc = stub->HelloWorld(hi, reply);
    EXPECT_TRUE(rc.IsOk());
    std::cout << "Reply from server: " << reply.reply() << std::endl;
}

TEST_F(ZmqMTPTest, TestMTP)
{
    ThreadPool pool(2);
    auto r1 = pool.Submit([this]() {
        for (auto i = 0; i < 100; ++i) {
            arbitrary::workspace::SayHelloPb rq;
            arbitrary::workspace::ReplyHelloPb reply;
            std::vector<RpcMessage> payload;
            std::unique_ptr<
                ClientUnaryWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
                clientApi;
            DS_ASSERT_OK(stub->HelloWorld3(RpcOptions(), &clientApi));
            rq.set_msg("Hello");
            DS_ASSERT_OK(clientApi->Write(rq));
            DS_ASSERT_OK(clientApi->Read(reply));
            ASSERT_EQ(rq.msg(), reply.reply());
            DS_ASSERT_OK(clientApi->ReceivePayload(payload));
            ASSERT_EQ(payload.size(), 1U);
            std::string msg = payload[0].ToString();
            ASSERT_EQ(msg, "ABCDEF");
        }
    });
    auto r2 = pool.Submit([this]() {
        for (auto i = 0; i < 100; ++i) {
            arbitrary::workspace::SayHelloPb rq;
            arbitrary::workspace::ReplyHelloPb reply;
            std::vector<RpcMessage> payload;
            std::unique_ptr<
                ClientUnaryWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
                clientApi;
            DS_ASSERT_OK(stub->HelloWorld4(RpcOptions(), &clientApi));
            rq.set_msg("Hello");
            DS_ASSERT_OK(clientApi->Write(rq));
            std::string msg = "GHIJKL";
            MemView memView(msg.data(), msg.size());
            DS_ASSERT_OK(clientApi->SendPayload({ memView }));
            DS_ASSERT_OK(clientApi->Read(reply));
            ASSERT_EQ(rq.msg(), reply.reply());
            DS_ASSERT_OK(clientApi->ReceivePayload(payload));
            ASSERT_EQ(payload.size(), 1U);
            std::string rMsg = payload[0].ToString();
            ASSERT_EQ(rMsg, msg);
        }
    });
    r1.get();
    r2.get();
}

TEST_F(ZmqMTPTest, TestAsyncPayload)
{
    Status rc;
    auto sz = 5 * 1024 * 1024;
    auto half_sz = sz / 2;
    auto buf = std::make_unique<char[]>(sz);
    std::ifstream ifs("/dev/urandom", std::ios_base::in | std::ios_base::binary);
    ifs.read(buf.get(), sz);
    ifs.close();
    MemView payload1(buf.get(), half_sz);
    MemView payload2(buf.get() + half_sz, half_sz);
    std::unique_ptr<ClientUnaryWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
        clientApi;
    DS_ASSERT_OK(stub->HelloWorld4(RpcOptions(), &clientApi));
    arbitrary::workspace::SayHelloPb rq;
    arbitrary::workspace::ReplyHelloPb reply;
    std::vector<MemView> payload;
    rq.set_msg("Hello");
    DS_ASSERT_OK(clientApi->Write(rq));
    payload.emplace_back(payload1);
    payload.emplace_back(payload2);
    DS_ASSERT_OK(clientApi->AsyncSendPayload(payload));
    // Check the results.
    DS_ASSERT_OK(clientApi->Read(reply));
    ASSERT_EQ(rq.msg(), reply.reply());
    std::vector<RpcMessage> replyPayload;
    DS_ASSERT_OK(clientApi->ReceivePayload(replyPayload));
    ASSERT_EQ(replyPayload.size(), payload.size());
    uint32_t checksum1 = GetCrc32(payload1.Data(), payload1.Size());
    uint32_t checksum2 = GetCrc32(replyPayload[0].Data(), replyPayload[0].Size());
    ASSERT_EQ(checksum1, checksum2);
    checksum1 = GetCrc32(payload2.Data(), payload2.Size());
    checksum2 = GetCrc32(replyPayload[1].Data(), replyPayload[1].Size());
    ASSERT_EQ(checksum1, checksum2);
}

TEST_F(ZmqMTPTest, TestEmptyPayload)
{
    std::unique_ptr<ClientUnaryWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
        clientApi;
    DS_ASSERT_OK(stub->HelloWorld4(RpcOptions(), &clientApi));
    arbitrary::workspace::SayHelloPb rq;
    arbitrary::workspace::ReplyHelloPb reply;
    std::vector<MemView> payload;
    rq.set_msg("Hello");
    DS_ASSERT_OK(clientApi->Write(rq));
    DS_ASSERT_OK(clientApi->AsyncSendPayload(payload));
    DS_ASSERT_OK(clientApi->Read(reply));
    ASSERT_EQ(rq.msg(), reply.reply());
    std::vector<RpcMessage> replyPayload;
    DS_ASSERT_OK(clientApi->ReceivePayload(replyPayload));
}

TEST_F(ZmqDfxTest, LEVEL1_TestDelayedStubDestruction)
{
    // The testcase aims to test that if MsgQueRef outlives ZmqStubConnMgr, the MsgQue weak_ptr logic acts properly
    // Initialize global thread
    // sleep is added so that stub will be destructed later.
    dfxClient.dfxThread = std::make_unique<Thread>([]() {
        // Make sure to run into sleep
        const int SLEEP_TIME = 5;
        arbitrary::workspace::SayHelloPb rq;
        arbitrary::workspace::ReplyHelloPb reply;
        std::unique_ptr<ClientUnaryWriterReader<arbitrary::workspace::SayHelloPb, arbitrary::workspace::ReplyHelloPb>>
            clientApi;
        DS_ASSERT_OK(dfxClient.dfxStub->HelloWorld(RpcOptions(), &clientApi));
        rq.set_msg("Hello");
        sleep(SLEEP_TIME);
        Status rc = clientApi->Write(rq);
        DS_ASSERT_NOT_OK(rc);
        LOG(INFO) << rc.GetMsg();
        ASSERT_TRUE(rc.GetMsg().find("Not connected to MsgQueMgr") != std::string::npos);
        // Do not need to check dfxExitFlag, because loop is not involved in this testcase scenario
    });
    // Add sleep to make sure ClientUnaryWriterReader is created
    sleep(1);
}

TEST_F(ZmqConnTest, TestConnectionFrontendInitFail)
{
    DS_ASSERT_OK(datasystem::inject::Set("ZmqFrontend.WorkerEntry.FailInit", "return(K_RUNTIME_ERROR)"));
    CreateStub();
    Status rc;
    arbitrary::workspace::SayHelloPb req;
    arbitrary::workspace::ReplyHelloPb rsp;
    rc = stub->UnarySocketSimpleGreeting(req, rsp);
    LOG(INFO) << rc.ToString();
    EXPECT_TRUE(rc.IsError());
}
}  // namespace st
}  // namespace datasystem
