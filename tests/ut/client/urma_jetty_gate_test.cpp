/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

// Deterministic unit coverage for the provider-post admission state machine. These cases use no
// provider handle: they prove the gate's ordering contract independently of hardware timing.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/rdma/urma_resource.h"

namespace datasystem {
namespace {

std::shared_ptr<UrmaJetty> MakeJetty()
{
    return std::make_shared<UrmaJetty>(nullptr, nullptr, nullptr);
}

TEST(UrmaJettyGateTest, AcquireFirstDefersFinalizerUntilLastPermitAndArm)
{
    auto jetty = MakeJetty();
    auto permit = jetty->TryAcquirePostPermit();
    ASSERT_TRUE(permit);
    EXPECT_TRUE(jetty->BeginRetire());
    // BeginRetire must not let the last release win before pool detach/retire-record setup.
    EXPECT_FALSE(jetty->IsFinalizerScheduled());
    EXPECT_FALSE(jetty->ArmRetireFinalizer());
    permit = {};
    EXPECT_TRUE(jetty->IsFinalizerScheduled());
    EXPECT_EQ(jetty->ActivePostCalls(), 0u);
}

TEST(UrmaJettyGateTest, CloseFirstRejectsAllNewPermits)
{
    auto jetty = MakeJetty();
    ASSERT_TRUE(jetty->BeginRetire());
    EXPECT_FALSE(jetty->TryAcquirePostPermit());
    EXPECT_TRUE(jetty->ArmRetireFinalizer());
    EXPECT_TRUE(jetty->IsFinalizerScheduled());
}

TEST(UrmaJettyGateTest, LastPermitReleasedBeforeArmIsFinalizedByArm)
{
    auto jetty = MakeJetty();
    auto permit = jetty->TryAcquirePostPermit();
    ASSERT_TRUE(permit);
    ASSERT_TRUE(jetty->BeginRetire());
    permit = {};
    EXPECT_FALSE(jetty->IsFinalizerScheduled());
    EXPECT_TRUE(jetty->ArmRetireFinalizer());
    EXPECT_TRUE(jetty->IsFinalizerScheduled());
}

TEST(UrmaJettyGateTest, ConcurrentPostsAreAdmittedWithoutSerialization)
{
    constexpr size_t kThreads = 8;
    auto jetty = MakeJetty();
    std::atomic<size_t> admitted{ 0 };
    std::mutex barrierMutex;
    std::condition_variable barrierCv;
    size_t ready = 0;
    bool release = false;
    std::vector<std::thread> threads;
    for (size_t i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            auto permit = jetty->TryAcquirePostPermit();
            if (permit) {
                admitted.fetch_add(1);
            }
            std::unique_lock<std::mutex> lock(barrierMutex);
            ++ready;
            barrierCv.notify_all();
            barrierCv.wait(lock, [&release] { return release; });
        });
    }
    bool allReady = false;
    size_t activeWhileHeld = 0;
    {
        std::unique_lock<std::mutex> lock(barrierMutex);
        allReady = barrierCv.wait_for(lock, std::chrono::seconds(2), [&ready] { return ready == kThreads; });
        activeWhileHeld = jetty->ActivePostCalls();
        release = true;
    }
    barrierCv.notify_all();
    for (auto &thread : threads) {
        thread.join();
    }
    ASSERT_TRUE(allReady);
    EXPECT_EQ(admitted.load(), kThreads);
    EXPECT_EQ(activeWhileHeld, kThreads);
    EXPECT_EQ(jetty->ActivePostCalls(), 0u);
}

TEST(UrmaJettyGateTest, ConcurrentFaultSourcesCloseAndScheduleExactlyOnce)
{
    auto jetty = MakeJetty();
    std::atomic<size_t> closeWinners{ 0 };
    std::vector<std::thread> sources;
    for (size_t i = 0; i < 4; ++i) {
        sources.emplace_back([&] {
            if (jetty->BeginRetire()) {
                closeWinners.fetch_add(1);
            }
        });
    }
    for (auto &source : sources) {
        source.join();
    }
    EXPECT_EQ(closeWinners.load(), 1u);
    EXPECT_TRUE(jetty->ArmRetireFinalizer());
    EXPECT_FALSE(jetty->ArmRetireFinalizer());
    EXPECT_TRUE(jetty->IsFinalizerScheduled());
}

TEST(UrmaJettyGateTest, EarlyAndLateFlushAreBothRetained)
{
    auto early = MakeJetty();
    ASSERT_TRUE(early->BeginRetire());
    ASSERT_TRUE(early->ArmRetireFinalizer());
    ASSERT_TRUE(early->BeginModify());
    EXPECT_FALSE(early->ObserveFlushErrDone());
    EXPECT_TRUE(early->CompleteModify());
    EXPECT_TRUE(early->BeginDelete());

    auto late = MakeJetty();
    ASSERT_TRUE(late->BeginRetire());
    ASSERT_TRUE(late->ArmRetireFinalizer());
    ASSERT_TRUE(late->BeginModify());
    EXPECT_FALSE(late->CompleteModify());
    EXPECT_TRUE(late->ObserveFlushErrDone());
    EXPECT_TRUE(late->BeginDelete());
}

TEST(UrmaJettyGateTest, ModifyFailureStaysQuarantinedAndNeverReopens)
{
    auto jetty = MakeJetty();
    ASSERT_TRUE(jetty->BeginRetire());
    ASSERT_TRUE(jetty->ArmRetireFinalizer());
    ASSERT_TRUE(jetty->BeginModify());
    jetty->Quarantine();
    EXPECT_EQ(jetty->GetLifecycleState(), UrmaJetty::LifecycleState::QUARANTINED);
    EXPECT_FALSE(jetty->TryAcquirePostPermit());
}

}  // namespace
}  // namespace datasystem
