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

/**
 * Description: Tests for MigrateDataRateController peek-only rate lookup.
 */
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "ut/common.h"
#include "datasystem/worker/object_cache/limiter/data_limiter.h"

using namespace datasystem::object_cache;
using namespace ::testing;

namespace datasystem {
namespace ut {

class MigrateDataRateControllerTest : public CommonTest {};

TEST_F(MigrateDataRateControllerTest, TestPeekAvailableRateDoesNotMutateRateMap)
{
    constexpr uint64_t maxBandwidth = 1000;
    auto controller = std::make_shared<MigrateDataRateController>(maxBandwidth);

    uint64_t expected = MigrateDataRateController::CalculateSmoothedRate(maxBandwidth / 2, maxBandwidth);
    ASSERT_NE(expected, 0u);

    uint64_t first = controller->PeekAvailableRate("workerB");
    ASSERT_EQ(first, expected);

    uint64_t again = controller->PeekAvailableRate("workerB");
    ASSERT_EQ(again, expected);

    uint64_t other = controller->PeekAvailableRate("anotherAddr");
    ASSERT_EQ(other, expected);

    uint64_t onceMore = controller->PeekAvailableRate("workerB");
    ASSERT_EQ(onceMore, expected);
}

// Regression for the rate=20 migration failure: the sliding window only pruned entries inside
// SlidingWindowUpdateRate (write path). After 5afc55ff added the `bytes_send > 0` guard, probe
// requests (bytes_send==0) skipped the write, so stale entries never expired and the remote
// stayed "busy" indefinitely. This test verifies GetAvailableBandwidth prunes on read so the
// window drains within 1 second even without new writes.
TEST_F(MigrateDataRateControllerTest, TestSlidingWindowPrunesOnReadAfterExpiry)
{
    constexpr uint64_t maxBandwidth = 1000;
    MigrateDataRateLimiter limiter(maxBandwidth);

    // Saturate the window: push one entry equal to maxBandwidth.
    limiter.SlidingWindowUpdateRate(maxBandwidth);
    ASSERT_EQ(limiter.GetAvailableBandwidth(), 0u);

    // After the 1-second window expires, a read must prune the stale entry.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    ASSERT_EQ(limiter.GetAvailableBandwidth(), maxBandwidth);
}

// Same regression at the controller level: PeekAvailableRate must return a non-zero rate after
// the sliding window expires, so SelfHealBusyRate probes can recover once the remote drains.
TEST_F(MigrateDataRateControllerTest, TestPeekAvailableRateRecoversAfterWindowExpires)
{
    constexpr uint64_t maxBandwidth = 1000;
    auto controller = std::make_shared<MigrateDataRateController>(maxBandwidth);

    controller->SlidingWindowUpdateRate(maxBandwidth);
    ASSERT_EQ(controller->PeekAvailableRate("workerA"), 0u);

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    uint64_t recovered = controller->PeekAvailableRate("workerA");
    ASSERT_GT(recovered, 0u);
}

// Verifies the PruneExpiredLocked while-loop pops only entries older than 1 second and keeps
// fresh ones, so currentBandwidth is decremented by exactly the popped bytes. This covers the
// multi-entry path that the single-entry tests above do not exercise.
TEST_F(MigrateDataRateControllerTest, TestSlidingWindowPrunesOnlyExpiredEntries)
{
    constexpr uint64_t maxBandwidth = 1000;
    constexpr uint64_t firstBytes = 400;
    constexpr uint64_t secondBytes = 300;
    MigrateDataRateLimiter limiter(maxBandwidth);

    // Entry 1 at t=0.
    limiter.SlidingWindowUpdateRate(firstBytes);
    // Entry 2 at t~800ms: entry 1 still within 1s, not pruned on write. Both in window.
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    limiter.SlidingWindowUpdateRate(secondBytes);
    ASSERT_EQ(limiter.GetAvailableBandwidth(), maxBandwidth - firstBytes - secondBytes);

    // Sleep ~500ms more: entry 1 age ~1300ms (expired), entry 2 age ~500ms (fresh).
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    // Read prunes only entry 1; entry 2 stays. available = max - secondBytes.
    ASSERT_EQ(limiter.GetAvailableBandwidth(), maxBandwidth - secondBytes);
}

}  // namespace ut
}  // namespace datasystem
