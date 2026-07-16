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

#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "datasystem/common/urma_mock/transport/uds_endpoint_service.h"
#include "datasystem/common/urma_mock/inject/fault_inject.h"
#include "datasystem/common/urma_mock/transport/uds_transport.h"

using namespace datasystem::urma_mock;

namespace {

class UrmaMockInjectHandshakeTest : public testing::Test {
protected:
    void SetUp() override
    {
        instance_ = "inject_handshake_" + std::to_string(::getpid());
        setenv("URMA_MOCK_UDS_INSTANCE", instance_.c_str(), 1);
        setenv("URMA_MOCK_UDS_BASE_DIR", "/tmp", 1);
    }

    void TearDown() override
    {
        ResetMockInject();
        unsetenv("URMA_MOCK_UDS_INSTANCE");
        unsetenv("URMA_MOCK_UDS_BASE_DIR");
    }

    std::string instance_;
};

}  // namespace

TEST_F(UrmaMockInjectHandshakeTest, TimeoutModeReturnsAfterInjectedDelay)
{
    auto path = ResolveUdsPath();
    UdsListener listener;
    ASSERT_TRUE(listener.Bind(path));
    int peerFd = -1;
    std::thread acceptTh([&] {
        peerFd = listener.Accept();
        if (peerFd >= 0) {
            UdsConnection server;
            server.AdoptFd(peerFd);
            UdsMsgType type = UdsMsgType::HELLO;
            std::vector<uint8_t> payload;
            std::vector<int> fds;
            int err = 0;
            (void)server.Recv(&type, nullptr, &payload, &fds, nullptr, &err);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    SetMockInjectHandshakeDelay(30, 1);
    uint64_t va = 0;
    uint64_t len = 0;
    auto start = std::chrono::steady_clock::now();
    int fd = UdsEndpointService::Instance().ImportSegViaUds(instance_, 7, &va, &len);
    auto elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    EXPECT_EQ(fd, -1);
    EXPECT_GE(elapsedMs, 25);
    acceptTh.join();
}

TEST_F(UrmaMockInjectHandshakeTest, ModeZeroDoesNotForceTimeout)
{
    SetMockInjectHandshakeDelay(0, 0);
    EXPECT_EQ(GetMockInjectHandshakeDelayMs(), 0u);
    EXPECT_EQ(GetMockInjectHandshakeTimeoutMode(), 0u);
}
