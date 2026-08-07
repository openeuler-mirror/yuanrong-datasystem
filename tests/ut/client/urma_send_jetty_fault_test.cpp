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

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <bthread/bthread.h>
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
constexpr auto kBthreadEventWaitTimeout = std::chrono::seconds(1);
constexpr auto kEventCompletionTimeout = std::chrono::milliseconds(20);
constexpr int kRecoverableCqeStatus = 9;  // Kept in sync with GetUrmaErrorHandlePolicy().
constexpr int kNonRecoverableCqeStatus = 5;
constexpr uint32_t kOrphanWrWarningThreshold = 16;
constexpr uint32_t kOrphanWrRetireThreshold = 32;

bool WaitUntil(const std::function<bool()> &predicate,
               std::chrono::steady_clock::duration timeout = kWaitTimeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
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

bool IsUrmaFaultTestEnvAvailable()
{
#ifdef USE_URMA_MOCK
    return true;
#else
    return std::getenv("DS_URMA_DEV_NAME") != nullptr;
#endif
}

std::shared_ptr<UrmaConnection> MakeTestConnection()
{
    UrmaJfrInfo info;
    info.uniqueInstanceId = "urma-send-jetty-fault-test";
    info.localAddress = HostPort("127.0.0.1", 29100);
    return std::make_shared<UrmaConnection>(std::unique_ptr<UrmaTargetJetty>(), info);
}

void ExpectStatusContains(const Status &status, const std::vector<std::string> &tokens)
{
    const auto statusText = status.ToString();
    for (const auto &token : tokens) {
        EXPECT_NE(statusText.find(token), std::string::npos) << statusText;
    }
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

TEST(UrmaSendJettyFaultTest, EventWaitSupportsBthreadWaiterAndPthreadNotifier)
{
    constexpr uint64_t kRequestId = 1002;
    constexpr auto notifyDelay = std::chrono::milliseconds(10);
    struct WaitArgs {
        std::shared_ptr<UrmaEvent> event;
        std::atomic<bool> started{ false };
        Status status = Status::OK();
    };

    WaitArgs args{ std::make_shared<UrmaEvent>(kRequestId, nullptr, "fault-test", "fault-test", 0,
                                                UrmaEvent::OperationType::WRITE, nullptr) };
    bthread_t tid;
    auto waitEvent = [](void *arg) -> void * {
        auto *waitArgs = static_cast<WaitArgs *>(arg);
        waitArgs->started.store(true, std::memory_order_release);
        waitArgs->status = waitArgs->event->WaitFor(kBthreadEventWaitTimeout);
        return nullptr;
    };
    ASSERT_EQ(bthread_start_background(&tid, nullptr, waitEvent, &args), 0);
    const bool waitStarted = WaitUntil(
        [&args] { return args.started.load(std::memory_order_acquire); }, kBthreadEventWaitTimeout);
    if (waitStarted) {
        std::this_thread::sleep_for(notifyDelay);
        std::thread notifier([&args] {
            args.event->SetFailed(kNonRecoverableCqeStatus);
            args.event->NotifyAll();
        });
        notifier.join();
    } else {
        args.event->NotifyAll();
    }
    const int joinRc = bthread_join(tid, nullptr);

    ASSERT_TRUE(waitStarted);
    ASSERT_EQ(joinRc, 0);
    ASSERT_TRUE(args.status.IsOk()) << args.status.ToString();
    EXPECT_TRUE(args.event->IsFailed());
    EXPECT_EQ(args.event->GetStatusCode(), kNonRecoverableCqeStatus);
    const auto trace = args.event->GetWriteTrace();
    EXPECT_GT(trace.notifyUs, 0U);
    EXPECT_GE(trace.awakeUs, trace.notifyUs);
}

TEST(UrmaSendJettyFaultTest, EventWaitTimeoutPreservesUrmaStatus)
{
    constexpr uint64_t kRequestId = 1003;
    struct WaitArgs {
        std::shared_ptr<UrmaEvent> event;
        Status status = Status::OK();
        std::chrono::steady_clock::duration elapsed{};
    } args{ std::make_shared<UrmaEvent>(kRequestId, nullptr, "fault-test", "fault-test", 0,
                                        UrmaEvent::OperationType::WRITE, nullptr) };

    bthread_t tid;
    auto waitEvent = [](void *arg) -> void * {
        auto *waitArgs = static_cast<WaitArgs *>(arg);
        const auto start = std::chrono::steady_clock::now();
        waitArgs->status = waitArgs->event->WaitFor(kEventCompletionTimeout);
        waitArgs->elapsed = std::chrono::steady_clock::now() - start;
        return nullptr;
    };
    ASSERT_EQ(bthread_start_background(&tid, nullptr, waitEvent, &args), 0);
    ASSERT_EQ(bthread_join(tid, nullptr), 0);

    EXPECT_EQ(args.status.GetCode(), K_URMA_WAIT_TIMEOUT);
    EXPECT_GE(args.elapsed, kEventCompletionTimeout);
    EXPECT_LT(args.elapsed, kBthreadEventWaitTimeout);
}

TEST(UrmaSendJettyFaultTest, PoolExhaustionReturnsUrmaBackpressureFromManagerAcquirePath)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
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
    EXPECT_EQ(status.GetCode(), K_URMA_TRY_AGAIN);
    ExpectStatusContains(status, { "srcAddress=", "targetAddress=127.0.0.1:29100",
                                   "remoteInstanceId=urma-send-jetty-fault-test" });
    EXPECT_EQ(unavailableJetty, nullptr);
    EXPECT_EQ(targetJetty, nullptr);
    resource.ReleaseJetty(heldJetty);
}

TEST(UrmaSendJettyFaultTest, CqeErrorRetiresAndRefillsJetty)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
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
    // RetireJetty, and asynchronous refill path without requiring a device-level data corruption fault.
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
    if (!IsUrmaFaultTestEnvAvailable()) {
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
        // Count unique registry identities. The implementation now installs one pending-retire record
        // synchronously, so a retired Jetty is not double-counted as both "retiring" and "pending".
        EXPECT_LE(GetRegisteredJettyCount(resource),
                  FLAGS_urma_send_jetty_lane_pool_size + FLAGS_urma_send_jetty_lane_refill_extra_size);
    }
}

TEST(UrmaSendJettyFaultTest, NonRecoverableCqeDoesNotRecreateOrLeakLane)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
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
    ASSERT_TRUE(resource.RegisterActiveSendLane(lease).IsOk());
    constexpr uint64_t kRequestId = 3001;
    ASSERT_TRUE(manager.CreateEvent(kRequestId, MakeTestConnection(), lease, "non-recoverable-cqe", 0,
                                    UrmaEvent::OperationType::WRITE, nullptr)
                    .IsOk());
    lease->AddWr();
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

    // CheckCompletionRecordStatus already settles the transport lane from local_id. Avoid racing the running server
    // event thread's single-owner finished/failed sets and drive only the remaining business-notify sequence directly.
    std::shared_ptr<UrmaEvent> event;
    ASSERT_TRUE(manager.GetEvent(kRequestId, event).IsOk());
    event->SetFailed(kNonRecoverableCqeStatus);
    event->NotifyAll();
    EXPECT_EQ(manager.WaitToFinish(kRequestId, 1).GetCode(), K_URMA_ERROR);

    ASSERT_TRUE(WaitForIdleSendLane(resource));
    std::shared_ptr<UrmaJetty> reacquired;
    ASSERT_TRUE(resource.AcquireJetty(reacquired).IsOk());
    EXPECT_EQ(reacquired.get(), jetty.get());
    resource.ReleaseJetty(reacquired);
}

TEST(UrmaSendJettyFaultTest, WaitToFinishReturnsRawCqeStatus)
{
    auto &manager = UrmaManager::Instance();
    constexpr uint64_t kRequestId = 1003;
    auto event = std::make_shared<UrmaEvent>(kRequestId, nullptr, "fault-test", "fault-test", 0,
                                             UrmaEvent::OperationType::WRITE, nullptr);
    // Deliberately bypass CreateEvent: this focused unit verifies raw CQE propagation without requiring a live
    // URMA connection, Jetty, or send-lane lease. Production-path event creation is covered by the lane tests above.
    TbbEventMap::accessor accessor;
    ASSERT_TRUE(manager.tbbEventMap_.insert(accessor, kRequestId));
    accessor->second = event;
    accessor.release();
    event->SetFailed(4);
    event->NotifyAll();
    UrmaWriteFailure failure;

    const auto status = manager.WaitToFinish(kRequestId, 1000, &failure);

    EXPECT_EQ(status.GetCode(), K_URMA_ERROR);
    EXPECT_FALSE(failure.providerStatus.has_value());
    ASSERT_TRUE(failure.cqeStatus.has_value());
    EXPECT_EQ(*failure.cqeStatus, 4);
}

TEST(UrmaSendJettyFaultTest, CleanupWaitPreservesEarlierCqe4WithLaterPostFailure)
{
    const bool originalEnableUrma = FLAGS_enable_urma;
    Raii restoreEnableUrma([originalEnableUrma] { FLAGS_enable_urma = originalEnableUrma; });
    FLAGS_enable_urma = true;
    auto &manager = UrmaManager::Instance();
    constexpr uint64_t kRequestId = 1004;
    auto event = std::make_shared<UrmaEvent>(kRequestId, nullptr, "fault-test", "fault-test", 0,
                                             UrmaEvent::OperationType::WRITE, nullptr);
    // As above, direct insertion isolates failure aggregation from hardware-backed event creation.
    TbbEventMap::accessor accessor;
    ASSERT_TRUE(manager.tbbEventMap_.insert(accessor, kRequestId));
    accessor->second = event;
    accessor.release();
    event->SetFailed(4);
    event->NotifyAll();
    UrmaWriteFailure failure{ .providerStatus = 5, .cqeStatus = std::nullopt };
    std::vector<uint64_t> eventKeys{ kRequestId };
    auto remainingTime = []() { return 1000; };
    auto preserveError = [](Status &status) { return status; };

    const auto status = WaitFastTransportEventWithFailure(eventKeys, remainingTime, preserveError, &failure);

    EXPECT_EQ(status.GetCode(), K_URMA_ERROR);
    ASSERT_TRUE(failure.providerStatus.has_value());
    EXPECT_EQ(*failure.providerStatus, 5);
    ASSERT_TRUE(failure.cqeStatus.has_value());
    EXPECT_EQ(*failure.cqeStatus, 4);
}

TEST(UrmaSendJettyFaultTest, AsyncJettyErrorRetiresAndRefillsJetty)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
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

    // Route a real Jetty through the async-event dispatcher instead of directly calling UrmaResource::RetireJetty.
    ASSERT_TRUE(manager.aeHandler_.HandleUrmaAsyncEvent(asyncEvent).IsOk());
    EXPECT_FALSE(failedJetty->IsValid());
    resource.ReleaseJetty(failedJetty);
    ExpectReplacementAfterRetire(resource, failedJetty);
}

TEST(UrmaSendJettyFaultTest, InFlightWaitTimeoutStatusIncludesPeerContext)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> timedOutJetty;
    ASSERT_TRUE(resource.AcquireJetty(timedOutJetty).IsOk());
    Raii cleanup([&resource, &timedOutJetty] {
        if (timedOutJetty != nullptr) {
            (void)resource.RetireActiveSendLane(timedOutJetty->GetJettyId());
            resource.ReleaseJetty(timedOutJetty);
        }
    });
    constexpr uint64_t kRequestId = 1003;
    constexpr uint64_t kDataSize = 4096;
    auto lease = std::make_shared<UrmaSendLaneLease>(timedOutJetty, kRequestId);
    ASSERT_TRUE(resource.RegisterActiveSendLane(lease).IsOk());
    lease->AddWr();
    ASSERT_TRUE(resource.SealActiveSendLane(lease).IsOk());
    ASSERT_TRUE(manager.CreateEvent(kRequestId, MakeTestConnection(), lease, "127.0.0.1:29100", kDataSize,
                                    UrmaEvent::OperationType::WRITE, nullptr)
                    .IsOk());

    const auto status = manager.WaitToFinish(kRequestId, 0);
    EXPECT_EQ(status.GetCode(), K_URMA_WAIT_TIMEOUT);
    EXPECT_EQ(status.GetMsg().find("RPC deadline exceeded"), std::string::npos) << status.ToString();
    ExpectStatusContains(status, { "requestId=1003", "srcAddress=", "targetAddress=127.0.0.1:29100",
                                   "dataSize=4096", "op=WRITE" });
    EXPECT_TRUE(timedOutJetty->IsValid());
    ASSERT_TRUE(WaitForIdleSendLane(resource));
    EXPECT_TRUE(lease->IsForceReleased());
    EXPECT_EQ(timedOutJetty->GetOrphanWrCount(), 1u);
    ASSERT_TRUE(resource.CompleteActiveSendLane(timedOutJetty->GetJettyId(), kRequestId, URMA_CR_SUCCESS).IsOk());
    EXPECT_EQ(timedOutJetty->GetOrphanWrCount(), 0u);
    timedOutJetty.reset();
}

TEST(UrmaSendJettyFaultTest, TimedOutLaneIsForceReleasedAndLateCqeDoesNotCompleteReplacementLane)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> jetty;
    ASSERT_TRUE(resource.AcquireJetty(jetty).IsOk());
    Raii cleanup([&resource, &jetty] {
        if (jetty != nullptr) {
            (void)resource.RetireActiveSendLane(jetty->GetJettyId());
            resource.ReleaseJetty(jetty);
        }
    });

    constexpr uint64_t kOldRequestId = 1000;
    auto oldLane = std::make_shared<UrmaSendLaneLease>(jetty, kOldRequestId);
    ASSERT_TRUE(resource.RegisterActiveSendLane(oldLane).IsOk());
    oldLane->AddWr();
    ASSERT_TRUE(resource.SealActiveSendLane(oldLane).IsOk());
    resource.ScheduleTimedOutSendLane(oldLane, kOldRequestId, "127.0.0.1:29100", "timed-out-peer");

    ASSERT_TRUE(WaitForIdleSendLane(resource));
    EXPECT_TRUE(oldLane->IsForceReleased());
    EXPECT_EQ(jetty->GetOrphanWrCount(), 1u);

    std::shared_ptr<UrmaJetty> reusedJetty;
    ASSERT_TRUE(resource.AcquireJetty(reusedJetty).IsOk());
    ASSERT_EQ(reusedJetty.get(), jetty.get());
    constexpr uint64_t kNewRequestId = 2000;
    auto newLane = std::make_shared<UrmaSendLaneLease>(reusedJetty, kNewRequestId);
    ASSERT_TRUE(resource.RegisterActiveSendLane(newLane).IsOk());
    newLane->AddWr();
    ASSERT_TRUE(resource.SealActiveSendLane(newLane).IsOk());

    ASSERT_TRUE(resource.CompleteActiveSendLane(jetty->GetJettyId(), kOldRequestId, URMA_CR_SUCCESS).IsOk());
    EXPECT_EQ(newLane->GetPendingWrCount(), 1u);
    EXPECT_EQ(jetty->GetOrphanWrCount(), 0u);
    ASSERT_TRUE(resource.CompleteActiveSendLane(jetty->GetJettyId(), kNewRequestId, URMA_CR_SUCCESS).IsOk());
    EXPECT_TRUE(WaitForIdleSendLane(resource));
    reusedJetty.reset();
    jetty.reset();
}

TEST(UrmaSendJettyFaultTest, TimeoutBeforeSealForceReleasesImmediatelyWhenSealArrives)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> jetty;
    ASSERT_TRUE(resource.AcquireJetty(jetty).IsOk());
    Raii cleanup([&resource, &jetty] {
        if (jetty != nullptr) {
            (void)resource.RetireActiveSendLane(jetty->GetJettyId());
            resource.ReleaseJetty(jetty);
        }
    });

    constexpr uint64_t kRequestId = 2500;
    auto lane = std::make_shared<UrmaSendLaneLease>(jetty, kRequestId);
    ASSERT_TRUE(resource.RegisterActiveSendLane(lane).IsOk());
    lane->AddWr();

    resource.ScheduleTimedOutSendLane(lane, kRequestId, "127.0.0.1:29100", "timeout-before-seal-peer");
    EXPECT_TRUE(lane->IsTimedOut());
    EXPECT_FALSE(lane->IsForceReleased());
    EXPECT_EQ(resource.GetSendJettyPoolStats().inUseCount, 1u);

    ASSERT_TRUE(resource.SealActiveSendLane(lane).IsOk());
    EXPECT_TRUE(lane->IsForceReleased());
    EXPECT_EQ(jetty->GetOrphanWrCount(), 1u);
    EXPECT_EQ(resource.GetSendJettyPoolStats().idleCount, 1u);
    ASSERT_TRUE(resource.CompleteActiveSendLane(jetty->GetJettyId(), kRequestId, URMA_CR_SUCCESS).IsOk());
    EXPECT_EQ(jetty->GetOrphanWrCount(), 0u);
    jetty.reset();
}

TEST(UrmaSendJettyFaultTest, OrphanWrWarningThresholdKeepsJettyReusable)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> jetty;
    ASSERT_TRUE(resource.AcquireJetty(jetty).IsOk());
    Raii cleanup([&resource, &jetty] {
        if (jetty != nullptr) {
            (void)resource.RetireActiveSendLane(jetty->GetJettyId());
            resource.ReleaseJetty(jetty);
        }
    });

    constexpr uint64_t kRequestId = 2800;
    constexpr uint32_t kOrphanWrs = kOrphanWrWarningThreshold + 1;
    auto lane = std::make_shared<UrmaSendLaneLease>(jetty, kRequestId);
    ASSERT_TRUE(resource.RegisterActiveSendLane(lane).IsOk());
    for (uint32_t i = 0; i < kOrphanWrs; ++i) {
        lane->AddWr();
    }
    ASSERT_TRUE(resource.SealActiveSendLane(lane).IsOk());
    resource.ScheduleTimedOutSendLane(lane, kRequestId, "127.0.0.1:29100", "orphan-warning-peer");

    EXPECT_TRUE(lane->IsForceReleased());
    EXPECT_TRUE(jetty->IsValid());
    EXPECT_EQ(jetty->GetOrphanWrCount(), kOrphanWrs);
    EXPECT_EQ(resource.GetSendJettyPoolStats().idleCount, 1u);
    for (uint32_t i = 0; i < kOrphanWrs; ++i) {
        ASSERT_TRUE(resource.CompleteActiveSendLane(jetty->GetJettyId(), kRequestId, URMA_CR_SUCCESS).IsOk());
    }
    EXPECT_EQ(jetty->GetOrphanWrCount(), 0u);
    jetty.reset();
}

TEST(UrmaSendJettyFaultTest, OrphanWrThresholdRetiresJettyAndTriggersRefill)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
        GTEST_SKIP() << "URMA environment test requires DS_URMA_DEV_NAME and a usable local URMA device.";
    }

    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(InitManagerForFaultTest(manager).IsOk());
    auto &resource = *manager.urmaResource_;
    ASSERT_TRUE(WaitForIdleSendLane(resource));

    std::shared_ptr<UrmaJetty> jetty;
    ASSERT_TRUE(resource.AcquireJetty(jetty).IsOk());
    Raii cleanup([&resource, &jetty] {
        if (jetty != nullptr) {
            (void)resource.RetireActiveSendLane(jetty->GetJettyId());
            resource.ReleaseJetty(jetty);
        }
    });

    constexpr uint64_t kRequestId = 3000;
    auto lane = std::make_shared<UrmaSendLaneLease>(jetty, kRequestId);
    ASSERT_TRUE(resource.RegisterActiveSendLane(lane).IsOk());
    for (uint32_t i = 0; i < kOrphanWrRetireThreshold; ++i) {
        lane->AddWr();
    }
    ASSERT_TRUE(resource.SealActiveSendLane(lane).IsOk());
    resource.ScheduleTimedOutSendLane(lane, kRequestId, "127.0.0.1:29100", "orphan-threshold-peer");

    ASSERT_TRUE(WaitUntil([&jetty] { return !jetty->IsValid(); }));
    EXPECT_TRUE(lane->IsForceReleased());
    EXPECT_EQ(jetty->GetOrphanWrCount(), kOrphanWrRetireThreshold);
    ExpectReplacementAfterRetire(resource, jetty);
    jetty.reset();
}

TEST(UrmaSendJettyFaultTest, RefillCreateFailuresKeepSurvivingLaneAndRecoverAutomatically)
{
    if (!IsUrmaFaultTestEnvAvailable()) {
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
    ASSERT_TRUE(resource.RetireJetty(failedJetty).IsOk());
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
