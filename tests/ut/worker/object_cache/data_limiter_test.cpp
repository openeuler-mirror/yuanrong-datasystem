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
#include <cstdint>
#include <memory>
#include <string>

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

}  // namespace ut
}  // namespace datasystem
