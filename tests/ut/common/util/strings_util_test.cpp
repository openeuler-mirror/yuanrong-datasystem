/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: test strings util function
 */
#include <securec.h>

#include "ut/common.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace ut {
class StringsUtilTest : public CommonTest {};

TEST_F(StringsUtilTest, TestStringFormat)
{
    EXPECT_EQ(FormatString("welcome to %s", "beijing"), "welcome to beijing");
    EXPECT_EQ(FormatString("name:%s, arg:%d, price:%.2f", "test", 10, 12.3), "name:test, arg:10, price:12.30");
}

TEST_F(StringsUtilTest, TestStrErr)
{
    EXPECT_EQ(StrErr(9), "Bad file descriptor");
}

TEST_F(StringsUtilTest, TestCopyToUniquePtrChar)
{
    std::string src = "test";
    std::unique_ptr<char[]> dest;
    EXPECT_TRUE(StringToUniquePtrChar(src.c_str(), dest));
    EXPECT_EQ(src, std::string(dest.get()));
}

TEST_F(StringsUtilTest, TestEmptySpillType)
{
    const std::string spillType = "";
    const std::string pattern = "[,\\s+]+";
    std::vector<std::string> types = SplitToUniqueStr(spillType, pattern);
    EXPECT_TRUE(types.empty());
}

TEST_F(StringsUtilTest, TestDuplicatedSpillType)
{
    const std::string spillType = "local_disk, local_disk";
    const std::string pattern = "[,\\s+]+";
    std::vector<std::string> types = SplitToUniqueStr(spillType, pattern);
    size_t size = types.size();
    ASSERT_EQ(size, size_t(1));
    ASSERT_EQ(types[0], "local_disk");
}

TEST_F(StringsUtilTest, TestClearUniqueChar)
{
    std::string origin = "abc";

    // Test clear char[]
    std::unique_ptr<char[]> charPar = std::make_unique<char[]>(origin.size() + 1);
    ASSERT_EQ(memcpy_s(charPar.get(), origin.size() + 1, origin.c_str(), origin.size()), EOK);
    ASSERT_EQ(strcmp(charPar.get(), origin.c_str()), 0);
    ClearUniqueChar(charPar, origin.size() + 1);
    ASSERT_EQ(charPar, nullptr);

    // Test clear unsigned char[]
    std::unique_ptr<unsigned char[]> unsignedCharPar = std::make_unique<unsigned char[]>(origin.size() + 1);
    ASSERT_EQ(memcpy_s(unsignedCharPar.get(), origin.size() + 1, origin.c_str(), origin.size()), EOK);
    ASSERT_EQ(strcmp((const char *)unsignedCharPar.get(), origin.c_str()), 0);
    ClearUniqueChar(unsignedCharPar, origin.size() + 1);
    ASSERT_EQ(unsignedCharPar, nullptr);
}

TEST_F(StringsUtilTest, TestJoinEmptyVec)
{
    std::vector<std::string> vec;
    ASSERT_EQ(Join(vec, ", "), "");
}

TEST_F(StringsUtilTest, TestJoinEmptyConnectStr)
{
    std::vector<std::string> vec { "abc", "def", "ghi" };
    ASSERT_EQ(Join(vec, ""), "abcdefghi");
}

TEST_F(StringsUtilTest, TestJoinNormalConnectStr)
{
    std::vector<std::string> vec { "abc", "def", "ghi" };
    ASSERT_EQ(Join(vec, ", "), "abc, def, ghi");
}

TEST_F(StringsUtilTest, TestRemoveNewlineOfStr)
{
    std::string str = "This is a text.\r\n";
    ASSERT_EQ("This is a text.", RemoveNewlineOfStr(str));

    const int loop = 20;
    for (int i = 0; i < loop; i++) {
        str += "\n\r\n\r";
    }
    str += "0";
    ASSERT_EQ(str, RemoveNewlineOfStr(str));
}

TEST_F(StringsUtilTest, TestIsValidNumber)
{
    std::string num1 = "1740557652";
    std::string num2 = "-123";
    std::string num3 = "0.123";
    std::string num4 = "-1.231244";
    std::string num5 = "00.231244";
    std::string num6 = "+0.231244";
    std::string num7 = ".231244";
    std::string num8 = "--0.231244";

    ASSERT_TRUE(IsValidNumber(num1));
    ASSERT_TRUE(IsValidNumber(num2));
    ASSERT_TRUE(IsValidNumber(num3));
    ASSERT_TRUE(IsValidNumber(num4));
    ASSERT_FALSE(IsValidNumber(num5));
    ASSERT_TRUE(IsValidNumber(num6));
    ASSERT_FALSE(IsValidNumber(num7));
    ASSERT_FALSE(IsValidNumber(num8));
}

TEST_F(StringsUtilTest, TestFormatStringForLog)
{
    // Case1: Test empty strings and null pointers
    EXPECT_EQ(FormatStringForLog(""), "");

    // Case2: Create a long string that will definitely be truncated
    std::string longStr = "This is the beginning of a very long string that should be truncated "
                          "and here is the end part that should remain visible";
    size_t maxDisplayLength1 = 1000;
    EXPECT_EQ(FormatStringForLog(longStr, maxDisplayLength1), longStr);
    EXPECT_EQ(FormatStringForLog(longStr), longStr);

    std::string expectStr = "This... [total: 120]";
    size_t maxDisplayLength2 = 4;
    EXPECT_EQ(FormatStringForLog(longStr, maxDisplayLength2), expectStr);
}

TEST_F(StringsUtilTest, TestAppendSrcDstForLog)
{
    EXPECT_EQ(AppendSrcDstForLog("10.0.0.1:1234", "10.0.0.2:5678"),
              ", src=10.0.0.1:1234, dst=10.0.0.2:5678");
    EXPECT_EQ(AppendSrcDstForLog("", "10.0.0.2:5678"), ", dst=10.0.0.2:5678");
    EXPECT_EQ(AppendSrcDstForLog("prefix", "10.0.0.1:1234", "10.0.0.2:5678"),
              "prefix, src=10.0.0.1:1234, dst=10.0.0.2:5678");
}
}  // namespace ut
}  // namespace datasystem
