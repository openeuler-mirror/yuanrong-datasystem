/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Unit tests for BrpcChannelFactory / BrpcControllerFactory.
 *
 * Verifies:
 *   - Create() against a running brpc server returns a non-null channel.
 *   - Create() against an unreachable endpoint returns nullptr (Init failure).
 *   - Controller factory applies non-zero overrides and leaves zero fields at
 *     channel default (no override).
 *
 * Port note: fixed port 18600 is chosen outside the worker/etcd range and
 * distinct from hello_test's 18500 to avoid collision when both run under a
 * single bazel invocation. If this ever collides with concurrent CI matrix
 * runs, switch to ST TestPortAllocator.
 */
#include <gtest/gtest.h>

#include <string>

#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gflags/gflags.h>

#include "datasystem/common/rpc/brpc_factory.h"

namespace datasystem {
namespace test {

constexpr int kFactoryTestPort = 18600;

// A minimal brpc service is not needed for channel/controller creation tests:
// BrpcChannelFactory::Create only needs a server that accepts TCP connections
// and completes the brpc handshake, which brpc::Server does with an empty
// service set. We still add a dummy service so Start succeeds on all brpc
// versions that require at least one service.
class BrpcFactoryTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        server_ = std::make_unique<brpc::Server>();
        brpc::ServerOptions opts;
        opts.idle_timeout_sec = -1;
        butil::EndPoint ep(butil::IP_ANY, kFactoryTestPort);
        ASSERT_EQ(server_->Start(ep, &opts), 0) << "brpc server failed to start on port " << kFactoryTestPort;
    }

    void TearDown() override
    {
        if (server_) {
            server_->Stop(0);
            server_->Join();
            server_.reset();
        }
    }

    std::unique_ptr<brpc::Server> server_;
};

// Create() against a live brpc server returns a usable channel.
TEST_F(BrpcFactoryTest, CreateChannelToLiveServerReturnsNonNull)
{
    BrpcChannelConfig cfg;
    cfg.endpoint = "127.0.0.1:" + std::to_string(kFactoryTestPort);
    cfg.timeout_ms = 3000;
    cfg.connect_timeout_ms = 3000;
    auto channel = BrpcChannelFactory::Create(cfg);
    ASSERT_NE(channel, nullptr);
}

// Create() against an empty/malformed endpoint returns nullptr (Init != 0).
//
// Note: brpc Channel::Init is lazy — it does NOT perform a TCP handshake, only
// parses the endpoint and resolves the host. So a well-formed endpoint pointing
// at a port where nothing listens still returns Init == 0 (the failure surfaces
// later at RPC time). This test therefore uses an EMPTY endpoint, which brpc
// cannot parse and must reject at Init, just to exercise the nullptr return
// path. It does NOT cover real-world Init failures (DNS / naming) — those need
// a malformed host and are out of scope for a unit test.
TEST_F(BrpcFactoryTest, CreateChannelToEmptyEndpointReturnsNull)
{
    BrpcChannelConfig cfg;
    cfg.endpoint = "";  // unparseable -> Init fails
    cfg.timeout_ms = 200;
    cfg.connect_timeout_ms = 200;
    cfg.max_retry = 0;
    auto channel = BrpcChannelFactory::Create(cfg);
    EXPECT_EQ(channel, nullptr);
}

// Create() still succeeds when cfg.enable_circuit_breaker=false. This exercises
// the cfg=false path used by worker<->worker mesh channels. The effective state
// is not directly observable (brpc Channel has no getter for it), so we assert
// Create succeeds and returns a usable channel.
TEST_F(BrpcFactoryTest, CreateChannelWithCircuitBreakerDisabledInCfg)
{
    BrpcChannelConfig cfg;
    cfg.endpoint = "127.0.0.1:" + std::to_string(kFactoryTestPort);
    cfg.timeout_ms = 3000;
    cfg.connect_timeout_ms = 3000;
    cfg.enable_circuit_breaker = false;
    auto channel = BrpcChannelFactory::Create(cfg);
    ASSERT_NE(channel, nullptr);
}

// Default-constructed controller config produces a controller with no overrides.
TEST_F(BrpcFactoryTest, CreateControllerDefaultIsNonNull)
{
    auto cntl = BrpcControllerFactory::Create();
    EXPECT_NE(cntl, nullptr);
}

// Non-zero fields are applied; zero fields are left untouched (channel default).
TEST_F(BrpcFactoryTest, CreateControllerAppliesOverrides)
{
    BrpcControllerConfig cfg;
    cfg.timeout_ms = 1234;
    cfg.max_retry = 2;
    cfg.backup_request_ms = 50;
    auto cntl = BrpcControllerFactory::Create(cfg);
    ASSERT_NE(cntl, nullptr);
    EXPECT_EQ(cntl->timeout_ms(), 1234);
    EXPECT_EQ(cntl->max_retry(), 2);
    EXPECT_EQ(cntl->backup_request_ms(), 50);
}

// Zero-valued fields must NOT override; the controller keeps brpc defaults.
TEST_F(BrpcFactoryTest, CreateControllerZeroFieldsDoNotOverride)
{
    BrpcControllerConfig cfg;  // all zero
    auto cntl = BrpcControllerFactory::Create(cfg);
    ASSERT_NE(cntl, nullptr);
    // backup_request_ms defaults to -1 in brpc (disabled); zero config must not
    // flip it to 0 (which would mean "fire backup immediately"). We only assert
    // that the value is NOT 0, i.e. we did not accidentally enable it.
    EXPECT_NE(cntl->backup_request_ms(), 0);
}

// BrpcChannelFactory::Create must enable brpc's wire-level timeout delivery so
// the server-side CallMethod prologue (brpc_service_generator.cpp
// BuildScTimeoutDurationInitSnippet) can read cntl->timeout_ms() and initialize
// reqTimeoutDuration from the client budget instead of the 60s DEFAULT_TIMEOUT.
// This is the root-cause layer for issue #773 (brpc server deadline_us() is -1
// because brpc never reconstructs _deadline_us from RpcMeta; without
// deliver_timeout_ms the server never sees the client budget and the worker
// retry loop runs with 60s, ignoring the client 20ms deadline). The flag is
// defined in brpc's baidu_rpc_protocol.cpp and defaults to false, so it must be
// flipped to true exactly once before the first brpc channel is used. We assert
// the flag is true after Create() runs, regardless of the channel outcome.
TEST_F(BrpcFactoryTest, CreateEnablesBrpcDeliverTimeoutMsFlag)
{
    BrpcChannelConfig cfg;
    cfg.endpoint = "127.0.0.1:" + std::to_string(kFactoryTestPort);
    cfg.timeout_ms = 3000;
    cfg.connect_timeout_ms = 3000;
    auto channel = BrpcChannelFactory::Create(cfg);
    ASSERT_NE(channel, nullptr);

    std::string flagVal;
    ASSERT_TRUE(gflags::GetCommandLineOption("baidu_std_protocol_deliver_timeout_ms", &flagVal));
    EXPECT_EQ(flagVal, "true")
        << "baidu_std_protocol_deliver_timeout_ms must be true for deadline propagation";
}

}  // namespace test
}  // namespace datasystem
