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

#include "datasystem/client/urma_success_rate_tracker.h"

#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"

DS_DECLARE_double(urma_failover_success_rate_ratio);
DS_DECLARE_uint32(urma_failover_min_sample_count);

namespace datasystem {
namespace client {
namespace {
constexpr uint64_t WINDOW_MS = 100;
constexpr uint64_t FIRST_BACKOFF_MS = 1'000;
constexpr uint64_t SECOND_BACKOFF_MS = 2'000;
constexpr uint64_t THIRD_BACKOFF_MS = 4'000;
constexpr uint64_t FOURTH_BACKOFF_MS = 8'000;
constexpr uint64_t MAX_BACKOFF_MS = 10'000;

class UrmaSuccessRateTrackerTest : public testing::Test {
protected:
    void SetUp() override
    {
        FLAGS_urma_failover_success_rate_ratio = 0.5;
        FLAGS_urma_failover_min_sample_count = 10;
    }

    void RecordFailures(UrmaSuccessRateTracker &tracker, uint32_t count, uint64_t startMs)
    {
        for (uint32_t i = 0; i < count; ++i) {
            ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, startMs + i));
        }
    }

    void RecordSuccesses(UrmaSuccessRateTracker &tracker, uint32_t count, uint64_t startMs)
    {
        for (uint32_t i = 0; i < count; ++i) {
            ASSERT_FALSE(tracker.RecordUrmaResult(true, WINDOW_MS, startMs + i));
        }
    }

    bool TriggerFailureWindow(UrmaSuccessRateTracker &tracker, uint64_t startMs, uint64_t &triggerTimeMs)
    {
        // A trigger only happens when a later request settles the previous full window.
        RecordFailures(tracker, FLAGS_urma_failover_min_sample_count, startMs);
        triggerTimeMs = startMs + WINDOW_MS;
        return tracker.RecordUrmaResult(false, WINDOW_MS, triggerTimeMs);
    }
};

TEST_F(UrmaSuccessRateTrackerTest, DoesNotTriggerBeforeFullWindow)
{
    LOG(INFO) << "[URMA tracker test] verify no trigger before a full statistics window";
    UrmaSuccessRateTracker tracker;
    for (uint32_t i = 0; i < 10; ++i) {
        ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, i));
    }
    // The window has enough failures, but no later request has settled it yet.
    EXPECT_FALSE(tracker.IsSwitchInProgress());
}

TEST_F(UrmaSuccessRateTrackerTest, RequiresMinimumSampleCount)
{
    LOG(INFO) << "[URMA tracker test] verify minimum sample count gate";
    FLAGS_urma_failover_min_sample_count = 10;
    UrmaSuccessRateTracker tracker;
    for (uint32_t i = 0; i < 9; ++i) {
        ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, i));
    }
    // The settled window has 0% success rate, but only 9 samples, below the configured floor.
    EXPECT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS));
    EXPECT_FALSE(tracker.IsSwitchInProgress());
}

TEST_F(UrmaSuccessRateTrackerTest, UsesStrictThreshold)
{
    LOG(INFO) << "[URMA tracker test] verify strict less-than threshold semantics";
    UrmaSuccessRateTracker equalTracker;
    for (uint32_t i = 0; i < 5; ++i) {
        ASSERT_FALSE(equalTracker.RecordUrmaResult(true, WINDOW_MS, i));
        ASSERT_FALSE(equalTracker.RecordUrmaResult(false, WINDOW_MS, i));
    }
    // 5 successes out of 10 equals the 0.5 threshold and must not trigger a switch.
    EXPECT_FALSE(equalTracker.RecordUrmaResult(true, WINDOW_MS, WINDOW_MS));

    UrmaSuccessRateTracker lowTracker;
    for (uint32_t i = 0; i < 4; ++i) {
        ASSERT_FALSE(lowTracker.RecordUrmaResult(true, WINDOW_MS, i));
    }
    for (uint32_t i = 0; i < 6; ++i) {
        ASSERT_FALSE(lowTracker.RecordUrmaResult(false, WINDOW_MS, i));
    }
    // 4 successes out of 10 is strictly lower than the 0.5 threshold and should trigger.
    EXPECT_TRUE(lowTracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS));
    EXPECT_TRUE(lowTracker.IsSwitchInProgress());
}

TEST_F(UrmaSuccessRateTrackerTest, ThresholdZeroDisablesAndClearsWindow)
{
    LOG(INFO) << "[URMA tracker test] verify threshold zero disables and clears the window";
    UrmaSuccessRateTracker tracker;
    for (uint32_t i = 0; i < 10; ++i) {
        ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, i));
    }
    FLAGS_urma_failover_success_rate_ratio = 0.0;
    EXPECT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS));
    // Disabled tracking must drop old failures; otherwise restoring the threshold could trigger immediately.
    EXPECT_EQ(tracker.GetCurrentSampleCountForTest(), 0ul);

    FLAGS_urma_failover_success_rate_ratio = 0.5;
    for (uint32_t i = 0; i < 10; ++i) {
        ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS + i + 1));
    }
    // After restore, only the new failure window is evaluated.
    EXPECT_TRUE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS * 2 + 1));
}

TEST_F(UrmaSuccessRateTrackerTest, BacksOffFailedSwitchesAndHealthyWindowResetsBackoff)
{
    LOG(INFO) << "[URMA tracker test] verify failed switch backoff and healthy reset";
    UrmaSuccessRateTracker tracker;
    for (uint32_t i = 0; i < 10; ++i) {
        ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, i));
    }
    EXPECT_TRUE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS));
    tracker.FinishSwitchAttempt(false, WINDOW_MS);
    // The first failed switch attempt should block the next trigger for 1 second.
    EXPECT_EQ(tracker.GetNextAllowedSwitchTimeMs(), WINDOW_MS + 1000);

    for (uint32_t i = 0; i < 10; ++i) {
        ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS + i + 1));
    }
    // Another bad window inside the backoff interval must not enqueue a switch.
    EXPECT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS * 2 + 1));
    EXPECT_FALSE(tracker.IsSwitchInProgress());

    ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS + 1000));
    for (uint32_t i = 1; i < 10; ++i) {
        ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS + 1000 + i));
    }
    EXPECT_TRUE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS + 1200));
    tracker.FinishSwitchAttempt(false, WINDOW_MS + 1200);
    // The second failed switch attempt expands backoff from 1 second to 2 seconds.
    EXPECT_EQ(tracker.GetNextAllowedSwitchTimeMs(), WINDOW_MS + 1200 + 2000);

    for (uint32_t i = 0; i < 10; ++i) {
        ASSERT_FALSE(tracker.RecordUrmaResult(true, WINDOW_MS, WINDOW_MS + 3300 + i));
    }
    EXPECT_FALSE(tracker.RecordUrmaResult(true, WINDOW_MS, WINDOW_MS + 3400));
    // A full healthy window proves the data plane recovered and resets failed-switch backoff.
    EXPECT_EQ(tracker.GetBackoffLevel(), 0u);
}

TEST_F(UrmaSuccessRateTrackerTest, DoesNotDuplicateTriggerWhileSwitchInProgress)
{
    LOG(INFO) << "[URMA tracker test] verify no duplicate trigger while switch is in progress";
    FLAGS_urma_failover_min_sample_count = 2;
    UrmaSuccessRateTracker tracker;
    uint64_t triggerTimeMs = 0;
    ASSERT_TRUE(TriggerFailureWindow(tracker, 0, triggerTimeMs));
    ASSERT_TRUE(tracker.IsSwitchInProgress());
    ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS + 1));
    // FinishSwitchAttempt has not run, so the next unhealthy settled window must be suppressed.
    EXPECT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS * 2));
    EXPECT_TRUE(tracker.IsSwitchInProgress());
}

TEST_F(UrmaSuccessRateTrackerTest, FailedSwitchBackoffIsCapped)
{
    LOG(INFO) << "[URMA tracker test] verify failed switch backoff sequence and cap";
    FLAGS_urma_failover_min_sample_count = 2;
    UrmaSuccessRateTracker tracker;
    const std::vector<uint64_t> expectedBackoffMs{ FIRST_BACKOFF_MS, SECOND_BACKOFF_MS, THIRD_BACKOFF_MS,
                                                   FOURTH_BACKOFF_MS, MAX_BACKOFF_MS, MAX_BACKOFF_MS };
    uint64_t nextWindowStartMs = 0;
    for (uint64_t expectedBackoff : expectedBackoffMs) {
        uint64_t triggerTimeMs = 0;
        ASSERT_TRUE(TriggerFailureWindow(tracker, nextWindowStartMs, triggerTimeMs));
        tracker.FinishSwitchAttempt(false, triggerTimeMs);
        // Each failed switch advances the next allowed time; the last two checks verify the 10s cap.
        EXPECT_EQ(tracker.GetNextAllowedSwitchTimeMs(), triggerTimeMs + expectedBackoff);
        nextWindowStartMs = tracker.GetNextAllowedSwitchTimeMs() + WINDOW_MS;
    }
}

TEST_F(UrmaSuccessRateTrackerTest, HotMinSamplesTakesEffectWhenWindowSettles)
{
    LOG(INFO) << "[URMA tracker test] verify hot minimum sample count applies at window settlement";
    FLAGS_urma_failover_min_sample_count = 10;
    UrmaSuccessRateTracker triggerTracker;
    RecordFailures(triggerTracker, 3, 0);
    FLAGS_urma_failover_min_sample_count = 3;
    // Lowering min samples before settlement makes the already-recorded 3 failures eligible.
    EXPECT_TRUE(triggerTracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS));
    UrmaSuccessRateTracker noTriggerTracker;
    FLAGS_urma_failover_min_sample_count = 10;
    RecordFailures(noTriggerTracker, 3, 0);
    FLAGS_urma_failover_min_sample_count = 4;
    // Raising the settled-window requirement above the recorded sample count keeps the switch closed.
    EXPECT_FALSE(noTriggerTracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS));
    EXPECT_FALSE(noTriggerTracker.IsSwitchInProgress());
}

TEST_F(UrmaSuccessRateTrackerTest, ShortHealthyWindowDoesNotResetBackoff)
{
    LOG(INFO) << "[URMA tracker test] verify short healthy window does not reset backoff";
    FLAGS_urma_failover_min_sample_count = 2;
    UrmaSuccessRateTracker tracker;
    uint64_t triggerTimeMs = 0;
    ASSERT_TRUE(TriggerFailureWindow(tracker, 0, triggerTimeMs));
    tracker.FinishSwitchAttempt(false, triggerTimeMs);
    ASSERT_EQ(tracker.GetBackoffLevel(), 1u);
    ASSERT_EQ(tracker.GetNextAllowedSwitchTimeMs(), triggerTimeMs + FIRST_BACKOFF_MS);
    uint64_t shortHealthyStartMs = tracker.GetNextAllowedSwitchTimeMs() + WINDOW_MS;
    ASSERT_FALSE(tracker.RecordUrmaResult(true, WINDOW_MS, shortHealthyStartMs));
    EXPECT_FALSE(tracker.RecordUrmaResult(true, WINDOW_MS, shortHealthyStartMs + WINDOW_MS));
    // One healthy sample is below min count, so it cannot prove recovery or reset backoff.
    EXPECT_EQ(tracker.GetBackoffLevel(), 1u);
    uint64_t healthyStartMs = shortHealthyStartMs + WINDOW_MS * 2;
    RecordSuccesses(tracker, 2, healthyStartMs);
    EXPECT_FALSE(tracker.RecordUrmaResult(true, WINDOW_MS, healthyStartMs + WINDOW_MS));
    // A full healthy window with enough samples resets both backoff level and next allowed time.
    EXPECT_EQ(tracker.GetBackoffLevel(), 0u);
    EXPECT_EQ(tracker.GetNextAllowedSwitchTimeMs(), 0u);
}

TEST_F(UrmaSuccessRateTrackerTest, ZeroWindowDisablesAndClearsWindow)
{
    LOG(INFO) << "[URMA tracker test] verify zero window length disables and clears state";
    UrmaSuccessRateTracker tracker;
    RecordFailures(tracker, 10, 0);
    EXPECT_FALSE(tracker.RecordUrmaResult(false, 0, WINDOW_MS));
    EXPECT_FALSE(tracker.IsSwitchInProgress());
    // Zero window length is treated as disablement and should leave no stale local state.
    EXPECT_EQ(tracker.GetCurrentSampleCountForTest(), 0ul);
    EXPECT_EQ(tracker.GetBackoffLevel(), 0u);
    EXPECT_EQ(tracker.GetNextAllowedSwitchTimeMs(), 0ul);
}

TEST_F(UrmaSuccessRateTrackerTest, ConcurrentBoundaryTriggersOnlyOnce)
{
    LOG(INFO) << "[URMA tracker test] verify concurrent window boundary triggers only once";
    FLAGS_urma_failover_min_sample_count = 2;
    UrmaSuccessRateTracker tracker;
    ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, 0));
    ASSERT_FALSE(tracker.RecordUrmaResult(false, WINDOW_MS, 1));
    std::atomic<uint32_t> triggerCount{ 0 };
    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < 16; ++i) {
        threads.emplace_back([&tracker, &triggerCount]() {
            if (tracker.RecordUrmaResult(false, WINDOW_MS, WINDOW_MS)) {
                triggerCount.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    // Only one boundary-settling thread may win the switch-in-progress compare-exchange.
    EXPECT_EQ(triggerCount.load(std::memory_order_relaxed), 1u);
}
}  // namespace
}  // namespace client
}  // namespace datasystem
