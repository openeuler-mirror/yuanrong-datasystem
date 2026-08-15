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

#ifdef USE_URMA
#define private public
#include "datasystem/common/rdma/urma_manager.h"
#undef private

namespace datasystem {
namespace {

TEST(UrmaChipInflightTest, FormatsNonZeroCountsWithRealChipIds)
{
    auto &manager = UrmaManager::Instance();
    EXPECT_EQ(manager.GetSrcChipInflightWrCounter(1), &manager.srcChipInflightWrCounts_.at(1));
    EXPECT_EQ(manager.GetSrcChipInflightWrCounter(2), &manager.srcChipInflightWrCounts_.at(2));
    EXPECT_EQ(manager.GetSrcChipInflightWrCounter(INVALID_CHIP_ID), nullptr);
    manager.srcChipInflightWrCounts_.at(1).store(3, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).store(5, std::memory_order_relaxed);

    EXPECT_STREQ(manager.GetSrcChipInflightWrCountsString(), "{1:3,2:5}");

    manager.srcChipInflightWrCounts_.at(1).store(0, std::memory_order_relaxed);
    manager.srcChipInflightWrCounts_.at(2).store(0, std::memory_order_relaxed);
}

TEST(UrmaChipInflightTest, SelectsSourceChipAccordingToRoundRobinType)
{
    auto &manager = UrmaManager::Instance();
    const uint32_t oldUbNumaRrType = FLAGS_ub_numa_rr_type;
    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);

    FLAGS_ub_numa_rr_type = 0;
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID), 2);
    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(2, true, false, 2), 2);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 0);

    FLAGS_ub_numa_rr_type = 1;
    // Two logical writes with two posts each: type 1 selects once on the first post and reuses that chip.
    auto firstWriteChip = manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID);
    EXPECT_EQ(firstWriteChip, 1);
    firstWriteChip = manager.GetAffinitySrcChipIdForPost(2, true, false, firstWriteChip);
    EXPECT_EQ(firstWriteChip, 1);
    auto secondWriteChip = manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID);
    EXPECT_EQ(secondWriteChip, 2);
    secondWriteChip = manager.GetAffinitySrcChipIdForPost(2, true, false, secondWriteChip);
    EXPECT_EQ(secondWriteChip, 2);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 2);

    manager.affinitySrcChipIdSequence_.store(0, std::memory_order_relaxed);
    FLAGS_ub_numa_rr_type = 2;
    // Two logical writes with two posts each: type 2 selects independently for every post.
    auto chip = manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID);
    EXPECT_EQ(chip, 1);
    chip = manager.GetAffinitySrcChipIdForPost(2, true, false, chip);
    EXPECT_EQ(chip, 2);
    chip = manager.GetAffinitySrcChipIdForPost(2, true, true, INVALID_CHIP_ID);
    EXPECT_EQ(chip, 1);
    chip = manager.GetAffinitySrcChipIdForPost(2, true, false, chip);
    EXPECT_EQ(chip, 2);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 4);

    EXPECT_EQ(manager.GetAffinitySrcChipIdForPost(INVALID_CHIP_ID, false, true, INVALID_CHIP_ID), INVALID_CHIP_ID);
    EXPECT_EQ(manager.affinitySrcChipIdSequence_.load(std::memory_order_relaxed), 4);
    FLAGS_ub_numa_rr_type = oldUbNumaRrType;
}

TEST(UrmaChipInflightTest, NormalizesWorkerRoundRobinType)
{
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(0, "test-worker"), 0u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(1, "test-worker"), 1u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(2, "test-worker"), 2u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(3, "test-worker"), 1u);
    EXPECT_EQ(UrmaManager::NormalizeUbNumaRrType(UINT32_MAX, "test-worker"), 1u);
}

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaChipInflightTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif
