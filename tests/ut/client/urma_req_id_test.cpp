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

#include <array>

#include <gtest/gtest.h>

#ifdef USE_URMA
#define private public
#include "datasystem/common/rdma/urma_manager.h"
#undef private

namespace datasystem {
namespace {

class UrmaReqIdTest : public testing::Test {
protected:
    void SetUp() override
    {
        originalReqId_ = manager_.requestId_.load();
    }

    void TearDown() override
    {
        manager_.requestId_.store(originalReqId_);
    }

    UrmaManager &manager_ = UrmaManager::Instance();
    uint64_t originalReqId_{ 0 };
};

TEST_F(UrmaReqIdTest, GenerateReqIdReturnsContiguousLowFortyBits)
{
    struct TestCase {
        uint64_t rawReqId;
        uint64_t expectedReqId;
    };
    constexpr std::array<TestCase, 4> TEST_CASES = {
        TestCase{ 0, 0 },
        TestCase{ 0xABCDE, 0xABCDE },
        TestCase{ 0xABCDE12345, 0xABCDE12345 },
        TestCase{ 0xFFFFFFABCDE12345, 0xABCDE12345 },
    };

    for (const auto &testCase : TEST_CASES) {
        manager_.requestId_.store(testCase.rawReqId);
        EXPECT_EQ(manager_.GenerateReqId(), testCase.expectedReqId);
    }
}

TEST_F(UrmaReqIdTest, GenerateReqIdIsContiguousAcrossTwentyBitBoundary)
{
    constexpr uint64_t RAW_REQ_ID_BEFORE_CARRY = 0xFFFFF;
    constexpr uint64_t REQ_ID_BEFORE_CARRY = 0xFFFFF;
    constexpr uint64_t REQ_ID_AFTER_CARRY = 0x100000;

    manager_.requestId_.store(RAW_REQ_ID_BEFORE_CARRY);

    EXPECT_EQ(manager_.GenerateReqId(), REQ_ID_BEFORE_CARRY);
    EXPECT_EQ(manager_.GenerateReqId(), REQ_ID_AFTER_CARRY);
}

TEST_F(UrmaReqIdTest, GenerateReqIdWrapsAfterLowFortyBits)
{
    constexpr uint64_t MAX_RAW_REQ_ID = 0xFFFFFFFFFF;

    manager_.requestId_.store(MAX_RAW_REQ_ID);

    EXPECT_EQ(manager_.GenerateReqId(), MAX_RAW_REQ_ID);
    EXPECT_EQ(manager_.GenerateReqId(), 0);
}

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaReqIdTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma.";
}
#endif
