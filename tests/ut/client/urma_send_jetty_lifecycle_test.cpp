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

// This is deliberately a URMA-backed environment test. It covers UrmaResource's real local send-Jetty lifecycle;
// keep the container-only SendJettyPool state machine in urma_send_lane_test.cpp.

#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#ifdef USE_URMA
#define private public
#include "datasystem/common/rdma/urma_manager.h"
#undef private

DS_DECLARE_bool(enable_urma);
DS_DECLARE_uint32(urma_send_jetty_lane_pool_size);
DS_DECLARE_uint32(urma_send_jetty_lane_refill_extra_size);

namespace datasystem {
namespace {
constexpr auto kWaitTimeout = std::chrono::seconds(10);

bool WaitUntil(const std::function<bool()> &predicate)
{
    const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

void ConfigureSingleLaneLifecycleTest()
{
    FLAGS_enable_urma = true;
    FLAGS_urma_send_jetty_lane_pool_size = 1;
    // Keep room for the retiring Jetty and its replacement at the same time, so refill is expected before the old
    // Jetty has completed asynchronous error-state cleanup.
    FLAGS_urma_send_jetty_lane_refill_extra_size = 1;
}

TEST(UrmaSendJettyLifecycleTest, PreFillsAcquiresReusesAndRefillsAfterFailure)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    ConfigureSingleLaneLifecycleTest();
    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(manager.Init(HostPort("127.0.0.1", 0)).IsOk());
    auto &resource = *manager.urmaResource_;

    // Init pre-fills the process-level pool with one real local send Jetty.
    const auto prefillStats = resource.GetSendJettyPoolStats();
    ASSERT_EQ(prefillStats.poolSize, 1u);
    ASSERT_EQ(prefillStats.idleCount, 1u);

    std::shared_ptr<UrmaJetty> firstJetty;
    ASSERT_TRUE(resource.AcquireJetty(firstJetty).IsOk());
    ASSERT_NE(firstJetty, nullptr);
    EXPECT_TRUE(firstJetty->IsValid());
    const auto acquiredStats = resource.GetSendJettyPoolStats();
    EXPECT_EQ(acquiredStats.poolSize, 1u);
    EXPECT_EQ(acquiredStats.idleCount, 0u);
    EXPECT_EQ(acquiredStats.inUseCount, 1u);

    // Capacity is one, so a second acquire must report backpressure rather than create a Jetty on the hot path.
    std::shared_ptr<UrmaJetty> exhaustedJetty;
    EXPECT_EQ(resource.AcquireJetty(exhaustedJetty).GetCode(), K_TRY_AGAIN);

    resource.ReleaseJetty(firstJetty);
    std::shared_ptr<UrmaJetty> reusedJetty;
    ASSERT_TRUE(resource.AcquireJetty(reusedJetty).IsOk());
    EXPECT_EQ(reusedJetty.get(), firstJetty.get()) << "release must return the same local send Jetty to the pool";

    // A real failure invalidates and removes the acquired Jetty. Refill must create a different valid Jetty while
    // the old Jetty is retired asynchronously; ReleaseJetty(old) must remain a no-op after invalidation.
    ASSERT_TRUE(resource.ReCreateJetty(reusedJetty).IsOk());
    EXPECT_FALSE(reusedJetty->IsValid());
    resource.ReleaseJetty(reusedJetty);
    ASSERT_TRUE(WaitUntil([&resource] {
        const auto stats = resource.GetSendJettyPoolStats();
        return stats.poolSize == 1 && stats.idleCount == 1;
    })) << "send Jetty pool was not refilled after local Jetty failure";

    std::shared_ptr<UrmaJetty> replacementJetty;
    ASSERT_TRUE(resource.AcquireJetty(replacementJetty).IsOk());
    EXPECT_NE(replacementJetty.get(), reusedJetty.get());
    EXPECT_TRUE(replacementJetty->IsValid());
    resource.ReleaseJetty(replacementJetty);
}

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaSendJettyLifecycleTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif
