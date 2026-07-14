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

// This is deliberately a URMA-backed fault test. It invokes production CQE, async-event, timeout, and pool-exhaustion
// handlers with real local Jetties; keep SendJettyPool's pure state-machine cases in urma_send_lane_test.cpp.

#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

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
constexpr int kRecoverableCqeStatus = 9;  // Kept in sync with GetUrmaErrorHandlePolicy().

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

void ConfigureFaultTest()
{
    FLAGS_enable_urma = true;
    FLAGS_urma_send_jetty_lane_pool_size = 1;
    // Each fault test retires one Jetty. Keep sufficient headroom for all retired Jetties when this target runs its
    // cases together, while still requiring the active pool itself to be exactly one lane.
    FLAGS_urma_send_jetty_lane_refill_extra_size = 4;
}

Status InitManagerForFaultTest(UrmaManager &manager)
{
    ConfigureFaultTest();
    auto status = manager.Init(HostPort("127.0.0.1", 0));
    if (status.IsError()) {
        return status;
    }
    if (manager.urmaResource_ == nullptr) {
        return Status(K_RUNTIME_ERROR, "UrmaManager initialized without UrmaResource");
    }
    return Status::OK();
}

bool WaitForIdleSendLane(UrmaResource &resource)
{
    return WaitUntil([&resource] {
        const auto stats = resource.GetSendJettyPoolStats();
        return stats.poolSize == 1 && stats.idleCount == 1 && stats.inUseCount == 0;
    });
}

std::shared_ptr<UrmaConnection> MakeTestConnection()
{
    UrmaJfrInfo info;
    info.uniqueInstanceId = "urma-send-jetty-fault-test";
    return std::make_shared<UrmaConnection>(std::unique_ptr<UrmaTargetJetty>(), info);
}

void ExpectReplacementAfterRetire(UrmaResource &resource, const std::shared_ptr<UrmaJetty> &retiredJetty)
{
    ASSERT_TRUE(WaitForIdleSendLane(resource)) << "send Jetty pool was not refilled after retirement";
    std::shared_ptr<UrmaJetty> replacementJetty;
    ASSERT_TRUE(resource.AcquireJetty(replacementJetty).IsOk());
    EXPECT_NE(replacementJetty.get(), retiredJetty.get());
    EXPECT_TRUE(replacementJetty->IsValid());
    resource.ReleaseJetty(replacementJetty);
}

TEST(UrmaSendJettyFaultTest, PoolExhaustionReturnsTryAgainFromManagerAcquirePath)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> heldJetty;
    ASSERT_TRUE(resource.AcquireJetty(heldJetty).IsOk());
    std::shared_ptr<UrmaJetty> unavailableJetty;
    urma_target_jetty_t *targetJetty = nullptr;
    const auto status = manager.AcquireSendLaneFromConnection(MakeTestConnection(), unavailableJetty, targetJetty);
    EXPECT_EQ(status.GetCode(), K_TRY_AGAIN);
    EXPECT_EQ(unavailableJetty, nullptr);
    EXPECT_EQ(targetJetty, nullptr);
    resource.ReleaseJetty(heldJetty);
}

TEST(UrmaSendJettyFaultTest, CqeErrorRetiresAndRefillsJetty)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> failedJetty;
    ASSERT_TRUE(resource.AcquireJetty(failedJetty).IsOk());
    urma_cr_t completion{};
    completion.status = static_cast<urma_cr_status_t>(kRecoverableCqeStatus);
    completion.user_ctx = 1001;
    completion.local_id = failedJetty->GetJettyId();
    std::unordered_set<uint64_t> completedRequests;
    std::unordered_map<uint64_t, int> failedRequests;

    // This is a synthetic CR delivered to the real completion handler. It reaches the status policy, registry lookup,
    // ReCreateJetty, and asynchronous refill path without requiring a device-level data corruption fault.
    const auto status = manager.CheckCompletionRecordStatus(&completion, 1, completedRequests, failedRequests);
    EXPECT_EQ(status.GetCode(), K_URMA_ERROR);
    EXPECT_TRUE(completedRequests.empty());
    ASSERT_EQ(failedRequests.size(), 1u);
    EXPECT_EQ(failedRequests[completion.user_ctx], kRecoverableCqeStatus);
    EXPECT_FALSE(failedJetty->IsValid());
    resource.ReleaseJetty(failedJetty);
    ExpectReplacementAfterRetire(resource, failedJetty);
}

TEST(UrmaSendJettyFaultTest, AsyncJettyErrorRetiresAndRefillsJetty)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> failedJetty;
    ASSERT_TRUE(resource.AcquireJetty(failedJetty).IsOk());
    urma_async_event_t asyncEvent{};
    asyncEvent.event_type = URMA_EVENT_JETTY_ERR;
    asyncEvent.element.jetty = failedJetty->Raw();

    // Route a real Jetty through the async-event dispatcher instead of directly calling UrmaResource::ReCreateJetty.
    ASSERT_TRUE(manager.aeHandler_.HandleUrmaAsyncEvent(asyncEvent).IsOk());
    EXPECT_FALSE(failedJetty->IsValid());
    resource.ReleaseJetty(failedJetty);
    ExpectReplacementAfterRetire(resource, failedJetty);
}

TEST(UrmaSendJettyFaultTest, WaitTimeoutRetiresAndRefillsJetty)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> timedOutJetty;
    ASSERT_TRUE(resource.AcquireJetty(timedOutJetty).IsOk());
    auto lease = std::make_shared<UrmaSendLaneLease>(timedOutJetty);
    constexpr uint64_t kRequestId = 1002;
    ASSERT_TRUE(manager.CreateEvent(kRequestId, MakeTestConnection(), lease, "fault-test", 0,
                                    UrmaEvent::OperationType::WRITE)
                    .IsOk());
    // Seal before waiting, matching the submit path: the timeout then becomes the lease's final retirement action.
    ASSERT_TRUE(manager.SealSendLaneLease(lease).IsOk());
    const auto status = manager.WaitToFinish(kRequestId, -1);
    EXPECT_EQ(status.GetCode(), K_URMA_WAIT_TIMEOUT);
    EXPECT_FALSE(timedOutJetty->IsValid());
    resource.ReleaseJetty(timedOutJetty);
    ExpectReplacementAfterRetire(resource, timedOutJetty);
}

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaSendJettyFaultTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif
