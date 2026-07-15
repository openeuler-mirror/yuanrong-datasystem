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

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaChipInflightTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif
