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
#include <condition_variable>
#include <future>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "datasystem/common/rdma/npu/roce_transport.h"

namespace datasystem {
namespace {

constexpr auto TEST_TIMEOUT = std::chrono::seconds(2);

class BlockingRoCETransport : public RoCETransport {
public:
    bool WaitUntilStarted(const std::string &remoteIdentity)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, TEST_TIMEOUT,
                            [this, &remoteIdentity] { return started_.count(remoteIdentity) != 0; });
    }

    void ReleaseAll()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }

    size_t CallCount(const std::string &remoteIdentity)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return callCounts_[remoteIdentity];
    }

protected:
    Status InitializeP2PComm(const std::string &remoteIdentity, P2pKind kind,
                             std::function<int()> *heartbeatCallback, P2PComm &p2pComm, int32_t &devId) override
    {
        (void)kind;
        (void)heartbeatCallback;
        (void)p2pComm;
        (void)devId;

        std::unique_lock<std::mutex> lock(mutex_);
        ++callCounts_[remoteIdentity];
        started_.insert(remoteIdentity);
        cv_.notify_all();
        cv_.wait(lock, [this] { return released_; });
        return Status::OK();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::unordered_map<std::string, size_t> callCounts_;
    std::unordered_set<std::string> started_;
    bool released_ = false;
};

TEST(RoCETransportTest, DifferentIdentifiersInitializeConcurrently)
{
    BlockingRoCETransport transport;
    auto first = std::async(std::launch::async,
                            [&transport] { return transport.Connect("first-identity", P2P_RECEIVER, nullptr); });
    const bool firstStarted = transport.WaitUntilStarted("first-identity");

    auto second = std::async(std::launch::async,
                             [&transport] { return transport.Connect("second-identity", P2P_RECEIVER, nullptr); });
    const bool secondStartedBeforeRelease = transport.WaitUntilStarted("second-identity");

    transport.ReleaseAll();
    EXPECT_TRUE(firstStarted);
    EXPECT_TRUE(secondStartedBeforeRelease);
    EXPECT_TRUE(first.get().IsOk());
    EXPECT_TRUE(second.get().IsOk());
}

TEST(RoCETransportTest, SameIdentifierSharesOneInFlightInitialization)
{
    BlockingRoCETransport transport;
    auto first = std::async(std::launch::async,
                            [&transport] { return transport.Connect("same-identity", P2P_RECEIVER, nullptr); });
    const bool firstStarted = transport.WaitUntilStarted("same-identity");
    auto second = std::async(std::launch::async,
                             [&transport] { return transport.Connect("same-identity", P2P_RECEIVER, nullptr); });

    transport.ReleaseAll();
    Status firstRc = first.get();
    Status secondRc = second.get();

    EXPECT_TRUE(firstStarted);
    EXPECT_EQ(transport.CallCount("same-identity"), 1U);
    EXPECT_EQ(firstRc.GetCode(), secondRc.GetCode());
    EXPECT_EQ(firstRc.GetMsg(), secondRc.GetMsg());
}

}  // namespace
}  // namespace datasystem
