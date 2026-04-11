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
 * Description: Type-safed format test.
 */
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/version.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {
class FormatTest : public CommonTest {
protected:
    int varInt = -1;
    std::string varStr = { "testString" };
    uint32_t varUl = 123456789;
    size_t varSize = SIZE_MAX;
    double varDouble = 0.123;
};

TEST_F(FormatTest, FormatInUse)
{
    EXPECT_EQ(FormatString("Int: %d, hex: %08X, size_t: %zu, ul: %llu", varInt, varUl, varSize, varUl),
              "Int: -1, hex: 075BCD15, size_t: 18446744073709551615, ul: 123456789");
    EXPECT_EQ(FormatString("C-String: %s, std::string: %s, Character: %c", varStr.c_str(), varStr, 'A'),
              "C-String: testString, std::string: testString, Character: A");
    EXPECT_EQ(FormatString("double: %.3lf, %.6lf %.2f, %f", varDouble, varDouble, varDouble, varDouble),
              "double: 0.123, 0.123000 0.12, 0.123000");
}

TEST_F(FormatTest, TestSingleFormat)
{
    Format fmt("Test single format, str: %s, int: %d");
    fmt % varStr % varUl;
    EXPECT_EQ(fmt.Str(), "Test single format, str: testString, int: 123456789");
}

// example from std::printf cppreference
TEST_F(FormatTest, OtherFormat)
{
    EXPECT_EQ(FormatString("string align: [%20s],[%-20s]", varStr, varStr),
              "string align: [          testString],[testString          ]");
    EXPECT_EQ(FormatString("Decimal:        %i %d %.6i %i %.0i %+i %i", 1, 2, 3, 0, 0, 4, -4),
              "Decimal:        1 2 3 0 0 +4 -4");
    EXPECT_EQ(FormatString("Rounding:       %f %.0f %.32f", 1.5, 1.5, 1.3),
              "Rounding:       1.500000 2 1.30000000000000004440892098500626");
    EXPECT_EQ(FormatString("Padding:        %05.2f %.2f %5.2f", 1.5, 1.5, 1.5), "Padding:        01.50 1.50  1.50");
    EXPECT_EQ(FormatString("Scientific:     %E %e", 1.5, 1.5), "Scientific:     1.500000E+00 1.500000e+00");
    EXPECT_EQ(FormatString("Hexadecimal:    %a %A", 1.5, 1.5), "Hexadecimal:    0x1.8p+0 0X1.8P+0");
#if defined(__aarch64__)
    EXPECT_EQ(FormatString("Special values: 0/0=%g 1/0=%g", 0.0 / 0.0, 1.0 / 0.0), "Special values: 0/0=nan 1/0=inf");
#else
    EXPECT_EQ(FormatString("Special values: 0/0=%g 1/0=%g", 0.0 / 0.0, 1.0 / 0.0), "Special values: 0/0=-nan 1/0=inf");
#endif
}

TEST_F(FormatTest, TypeMismatch)
{
    EXPECT_EQ(FormatString("Print string in wrong format: %d, %p, %x", varStr, varStr, varStr),
              "Print string in wrong format: testString, testString, testString");
    EXPECT_EQ(FormatString("Print integer in wrong format without truncation: %s, %d", varUl, varSize),
              "Print integer in wrong format without truncation: 123456789, 18446744073709551615");
}

TEST_F(FormatTest, ExceptionWhenInvalid)
{
    EXPECT_THROW(FormatString("format less %d,%d", varInt), std::invalid_argument);
    EXPECT_THROW(FormatString("format more", varInt), std::invalid_argument);
    EXPECT_THROW(FormatString("format wrong %", varInt), std::invalid_argument);
    EXPECT_THROW(FormatString("format wrong %l", varInt), std::invalid_argument);
    EXPECT_THROW(FormatString("format wrong %:", varInt), std::invalid_argument);
}

TEST_F(FormatTest, ClientGitHashFormat)
{
    // Add test for code coverage
    std::string invalidCode = "xxx~!@#$%^&*";
    (void)CheckClientGitHash(invalidCode);
    std::string validCode = "[07738a2ff613cc2aa56c111143abe9278f9][2025-03-10 09:18:24 +0800]";
    (void)CheckClientGitHash(validCode);
}
}  // namespace ut
}  // namespace datasystem
