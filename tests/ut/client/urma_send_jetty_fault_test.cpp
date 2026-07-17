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
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <gtest/gtest.h>

#ifdef USE_URMA
#define private public
#include "datasystem/common/rdma/urma_manager.h"
#undef private
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/raii.h"

DS_DECLARE_bool(enable_urma);
DS_DECLARE_uint32(urma_send_jetty_lane_pool_size);
DS_DECLARE_uint32(urma_send_jetty_lane_refill_extra_size);

namespace datasystem {
namespace {
constexpr auto kWaitTimeout = std::chrono::seconds(10);
constexpr int kRecoverableCqeStatus = 9;  // Kept in sync with GetUrmaErrorHandlePolicy().
constexpr int kNonRecoverableCqeStatus = 5;

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
    // UrmaManager is process-global and remains initialized across this target's cases. Keep headroom for every
    // synthetic retirement in the full target while still requiring the active pool itself to be exactly one lane.
    FLAGS_urma_send_jetty_lane_refill_extra_size = 16;
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

size_t GetRegisteredJettyCount(UrmaResource &resource)
{
    std::lock_guard<std::mutex> lock(resource.jettyRegistryMutex_);
    return resource.jettyRegistry_.size();
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

TEST(UrmaSendJettyFaultTest, RepeatedCqeStatus9RefillsWithoutExceedingUniqueJettyHeadroom)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    constexpr uint32_t kFaultCount = 4;
    for (uint32_t i = 0; i < kFaultCount; ++i) {
        std::shared_ptr<UrmaJetty> failedJetty;
        ASSERT_TRUE(resource.AcquireJetty(failedJetty).IsOk());
        urma_cr_t completion{};
        completion.status = static_cast<urma_cr_status_t>(kRecoverableCqeStatus);
        completion.user_ctx = 2000 + i;
        completion.local_id = failedJetty->GetJettyId();
        std::unordered_set<uint64_t> completedRequests;
        std::unordered_map<uint64_t, int> failedRequests;

        EXPECT_EQ(manager.CheckCompletionRecordStatus(&completion, 1, completedRequests, failedRequests).GetCode(),
                  K_URMA_ERROR);
        EXPECT_FALSE(failedJetty->IsValid());
        resource.ReleaseJetty(failedJetty);
        ExpectReplacementAfterRetire(resource, failedJetty);
        // Count unique registry entries, not pool+retiring+pending counters: retiring-to-pending can transiently
        // represent one Jetty in both counters.
        EXPECT_LE(GetRegisteredJettyCount(resource),
                  FLAGS_urma_send_jetty_lane_pool_size + FLAGS_urma_send_jetty_lane_refill_extra_size);
    }
}

TEST(UrmaSendJettyFaultTest, NonRecoverableCqeDoesNotRecreateOrLeakLane)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> jetty;
    ASSERT_TRUE(resource.AcquireJetty(jetty).IsOk());
    const auto registryCount = GetRegisteredJettyCount(resource);
    auto lease = std::make_shared<UrmaSendLaneLease>(jetty);
    constexpr uint64_t kRequestId = 3001;
    ASSERT_TRUE(manager.CreateEvent(kRequestId, MakeTestConnection(), lease, "non-recoverable-cqe", 0,
                                    UrmaEvent::OperationType::WRITE)
                    .IsOk());
    ASSERT_TRUE(manager.SealSendLaneLease(lease).IsOk());

    urma_cr_t completion{};
    completion.status = static_cast<urma_cr_status_t>(kNonRecoverableCqeStatus);
    completion.user_ctx = kRequestId;
    completion.local_id = jetty->GetJettyId();
    std::unordered_set<uint64_t> completedRequests;
    std::unordered_map<uint64_t, int> failedRequests;

    EXPECT_EQ(manager.CheckCompletionRecordStatus(&completion, 1, completedRequests, failedRequests).GetCode(),
              K_URMA_ERROR);
    const auto failedIt = failedRequests.find(kRequestId);
    EXPECT_NE(failedIt, failedRequests.end());
    if (failedIt != failedRequests.end()) {
        EXPECT_EQ(failedIt->second, kNonRecoverableCqeStatus);
    }
    EXPECT_TRUE(jetty->IsValid());
    EXPECT_EQ(GetRegisteredJettyCount(resource), registryCount);

    // CheckCompletionRecordStatus only classifies completions; the running server event thread normally transfers its
    // result into CheckAndNotify. Avoid racing that thread's single-owner finished/failed sets here and drive the same
    // per-event production sequence directly: mark failed, settle the sealed lease, notify the waiter, then let
    // WaitToFinish observe the CQE error and delete the event.
    std::shared_ptr<UrmaEvent> event;
    ASSERT_TRUE(manager.GetEvent(kRequestId, event).IsOk());
    event->SetFailed(kNonRecoverableCqeStatus);
    manager.ReleaseEventLane(event);
    event->NotifyAll();
    EXPECT_EQ(manager.WaitToFinish(kRequestId, 1).GetCode(), K_URMA_ERROR);

    ASSERT_TRUE(WaitForIdleSendLane(resource));
    std::shared_ptr<UrmaJetty> reacquired;
    ASSERT_TRUE(resource.AcquireJetty(reacquired).IsOk());
    EXPECT_EQ(reacquired.get(), jetty.get());
    resource.ReleaseJetty(reacquired);
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

TEST(UrmaSendJettyFaultTest, RepeatedWaitTimeoutsRefillWithoutExceedingUniqueJettyHeadroom)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    constexpr uint32_t kTimeoutCount = 4;
    for (uint32_t i = 0; i < kTimeoutCount; ++i) {
        std::shared_ptr<UrmaJetty> timedOutJetty;
        ASSERT_TRUE(resource.AcquireJetty(timedOutJetty).IsOk());
        auto lease = std::make_shared<UrmaSendLaneLease>(timedOutJetty);
        const uint64_t requestId = 4000 + i;
        ASSERT_TRUE(manager.CreateEvent(requestId, MakeTestConnection(), lease, "fault-storm", 0,
                                        UrmaEvent::OperationType::WRITE)
                        .IsOk());
        ASSERT_TRUE(manager.SealSendLaneLease(lease).IsOk());
        EXPECT_EQ(manager.WaitToFinish(requestId, -1).GetCode(), K_URMA_WAIT_TIMEOUT);
        EXPECT_FALSE(timedOutJetty->IsValid());
        resource.ReleaseJetty(timedOutJetty);
        ExpectReplacementAfterRetire(resource, timedOutJetty);
        EXPECT_LE(GetRegisteredJettyCount(resource),
                  FLAGS_urma_send_jetty_lane_pool_size + FLAGS_urma_send_jetty_lane_refill_extra_size);
    }
}

TEST(UrmaSendJettyFaultTest, RefillCreateFailuresKeepSurvivingLaneAndRecoverAutomatically)
{
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    bool poolExpanded = false;
    Raii restoreGlobalState([&resource, &poolExpanded] {
        (void)inject::Clear("urma.RefillCreateSendJetty");
        (void)inject::Clear("urma.SendJettyPoolRefillAdded");
        FLAGS_urma_send_jetty_lane_pool_size = 1;
        while (poolExpanded && resource.GetSendJettyPoolStats().poolSize > 1) {
            std::shared_ptr<UrmaJetty> extraLane;
            if (resource.AcquireJetty(extraLane).IsError()) {
                break;
            }
            (void)resource.RetireJetty(extraLane);
            resource.ReleaseJetty(extraLane);
        }
    });

    // The process-global fault manager starts with one lane for the earlier tests. Add a second real Jetty so this case
    // can prove that refill failures do not take an unrelated surviving lane out of service.
    FLAGS_urma_send_jetty_lane_pool_size = 2;
    std::shared_ptr<UrmaJetty> extraJetty;
    ASSERT_TRUE(resource.CreateJetty(extraJetty).IsOk());
    {
        std::lock_guard<std::mutex> lock(resource.jettyPoolMutex_);
        resource.sendJettyPool_.Add(extraJetty);
    }
    poolExpanded = true;
    ASSERT_TRUE(WaitUntil([&resource] {
        const auto stats = resource.GetSendJettyPoolStats();
        return stats.poolSize == 2 && stats.idleCount == 2 && stats.inUseCount == 0;
    }));

    constexpr uint64_t kInjectedCreateFailures = 3;
    ASSERT_TRUE(inject::Set("urma.RefillCreateSendJetty", "return(K_URMA_ERROR)").IsOk());
    ASSERT_TRUE(inject::Set("urma.SendJettyPoolRefillAdded", "call()").IsOk());
    std::shared_ptr<UrmaJetty> failedJetty;
    ASSERT_TRUE(resource.AcquireJetty(failedJetty).IsOk());
    ASSERT_TRUE(resource.ReCreateJetty(failedJetty).IsOk());
    resource.ReleaseJetty(failedJetty);

    ASSERT_TRUE(WaitUntil([] {
        return inject::GetExecuteCount("urma.RefillCreateSendJetty") >= kInjectedCreateFailures;
    })) << "refill did not retry the injected CreateJetty failures";
    const auto degradedStats = resource.GetSendJettyPoolStats();
    EXPECT_EQ(degradedStats.poolSize, 1U);
    EXPECT_EQ(degradedStats.idleCount, 1U);
    EXPECT_EQ(degradedStats.inUseCount, 0U);
    EXPECT_LE(GetRegisteredJettyCount(resource),
              FLAGS_urma_send_jetty_lane_pool_size + FLAGS_urma_send_jetty_lane_refill_extra_size);

    std::shared_ptr<UrmaJetty> survivingJetty;
    ASSERT_TRUE(resource.AcquireJetty(survivingJetty).IsOk());
    EXPECT_TRUE(survivingJetty->IsValid());
    EXPECT_NE(survivingJetty.get(), failedJetty.get());
    resource.ReleaseJetty(survivingJetty);

    // Removing the fault is sufficient: the background loop must retry without another retirement notification.
    ASSERT_TRUE(inject::Clear("urma.RefillCreateSendJetty").IsOk());
    ASSERT_TRUE(WaitUntil([] { return inject::GetExecuteCount("urma.SendJettyPoolRefillAdded") >= 1; }));
    ASSERT_TRUE(WaitUntil([&resource] {
        const auto stats = resource.GetSendJettyPoolStats();
        return stats.poolSize == 2 && stats.idleCount == 2 && stats.inUseCount == 0;
    })) << "send Jetty pool did not automatically recover after CreateJetty failures stopped";

    std::shared_ptr<UrmaJetty> first;
    std::shared_ptr<UrmaJetty> second;
    ASSERT_TRUE(resource.AcquireJetty(first).IsOk());
    ASSERT_TRUE(resource.AcquireJetty(second).IsOk());
    EXPECT_NE(first.get(), second.get());
    EXPECT_TRUE(first->IsValid());
    EXPECT_TRUE(second->IsValid());
    resource.ReleaseJetty(first);
    resource.ReleaseJetty(second);
    ASSERT_TRUE(inject::Clear("urma.SendJettyPoolRefillAdded").IsOk());

    // Restore the singleton's original configured capacity so test ordering or repetition cannot inherit two lanes.
    FLAGS_urma_send_jetty_lane_pool_size = 1;
    ASSERT_TRUE(resource.RetireJetty(second).IsOk());
    resource.ReleaseJetty(second);
    ASSERT_TRUE(WaitForIdleSendLane(resource));
    poolExpanded = false;
}

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaSendJettyFaultTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif
