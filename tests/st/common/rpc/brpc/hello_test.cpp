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
#include <brpc/server.h>
#include <brpc/channel.h>
#include "datasystem/common/rpc/brpc/hello.pb.h"

namespace datasystem {
namespace test {
namespace brpc_hello {

// V07 note: fixed port for single-test bazel run. If this test ever collides
// with concurrent bazel invocations or CI matrix, switch to ST harness port
// allocation (test_port_allocator). Currently safe because hello_test runs
// in isolation and 18500 is outside the worker/etcd port range.
constexpr int kTestPort = 18500;

class HelloServiceImpl : public datasystem::rpc::brpc::HelloService {
public:
    void SayHello(google::protobuf::RpcController *cntl, const datasystem::rpc::brpc::HelloRequest *request,
                  datasystem::rpc::brpc::HelloResponse *response, google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        response->set_message("Hello, " + request->name() + "!");
    }
};

class HelloTest : public ::testing::Test {
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
    HelloServiceImpl svc_;
};

TEST_F(HelloTest, SayHelloReturnsExpectedMessage)
{
    brpc::ChannelOptions opts;
    opts.timeout_ms = 3000;
    opts.max_retry = 0;

    brpc::Channel channel;
    ASSERT_EQ(channel.Init("localhost", kTestPort, &opts), 0);

    datasystem::rpc::brpc::HelloService_Stub stub(&channel);

    datasystem::rpc::brpc::HelloRequest request;
    request.set_name("world");

    datasystem::rpc::brpc::HelloResponse response;
    brpc::Controller cntl;

    stub.SayHello(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    EXPECT_EQ(response.message(), "Hello, world!");
    EXPECT_NE(response.message().find("Hello"), std::string::npos);
}

}  // namespace brpc_hello
}  // namespace test
}  // namespace datasystem
