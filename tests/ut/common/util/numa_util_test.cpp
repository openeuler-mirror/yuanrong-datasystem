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
 * Description: test numa util function.
 */
#include "ut/common.h"
#include "datasystem/common/util/numa_util.h"

namespace datasystem {
namespace ut {
class NumaUtilTest : public CommonTest {};

TEST_F(NumaUtilTest, TestParseCpuListSuccess)
{
    std::vector<int> cpus;
    ASSERT_TRUE(ParseCpuList("0,2,4-6", cpus));

    std::vector<int> expect = { 0, 2, 4, 5, 6 };
    ASSERT_EQ(cpus, expect);
}

TEST_F(NumaUtilTest, TestParseCpuListSuccessWithWhitespace)
{
    std::vector<int> cpus;
    ASSERT_TRUE(ParseCpuList(" 1 , 3-4 ", cpus));

    std::vector<int> expect = { 1, 3, 4 };
    ASSERT_EQ(cpus, expect);
}

TEST_F(NumaUtilTest, TestParseCpuListFailOnEmpty)
{
    std::vector<int> cpus;
    ASSERT_FALSE(ParseCpuList("", cpus));
}

TEST_F(NumaUtilTest, TestParseCpuListFailOnReverseRange)
{
    std::vector<int> cpus;
    ASSERT_FALSE(ParseCpuList("5-2", cpus));
}

TEST_F(NumaUtilTest, TestParseCpuListFailOnMalformedSegment)
{
    std::vector<int> cpus;
    ASSERT_FALSE(ParseCpuList("1-", cpus));
    cpus.clear();
    ASSERT_FALSE(ParseCpuList("abc", cpus));
}

TEST_F(NumaUtilTest, TestNumaIdToChipIdWithNumaCount1)
{
    EXPECT_EQ(NumaIdToChipId(0, 1), 1);
}

TEST_F(NumaUtilTest, TestNumaIdToChipIdWithNumaCount2)
{
    EXPECT_EQ(NumaIdToChipId(0, 2), 1);
    EXPECT_EQ(NumaIdToChipId(1, 2), 2);
}

TEST_F(NumaUtilTest, TestNumaIdToChipIdWithNumaCount3)
{
    EXPECT_EQ(NumaIdToChipId(0, 3), 1);
    EXPECT_EQ(NumaIdToChipId(1, 3), 1);
    EXPECT_EQ(NumaIdToChipId(2, 3), 2);
}

TEST_F(NumaUtilTest, TestNumaIdToChipIdWithNumaCount4)
{
    EXPECT_EQ(NumaIdToChipId(0, 4), 1);
    EXPECT_EQ(NumaIdToChipId(1, 4), 1);
    EXPECT_EQ(NumaIdToChipId(2, 4), 2);
    EXPECT_EQ(NumaIdToChipId(3, 4), 2);
}

TEST_F(NumaUtilTest, TestNumaIdToChipIdInvalidWhenNumaIdOutOfRange)
{
    EXPECT_EQ(NumaIdToChipId(4, 4), INVALID_CHIP_ID);
}
}  // namespace ut
}  // namespace datasystem
