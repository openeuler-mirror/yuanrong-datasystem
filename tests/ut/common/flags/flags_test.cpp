/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: flags test.
 */

#include "datasystem/common/flags/flags.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

DS_DEFINE_bool(bool_flag, false, "boolean variable test");
DS_DEFINE_uint32(uint32_flag, 32, "uint32 variable test");
DS_DEFINE_int32(int32_flag, 32, "uint32 variable test");
DS_DEFINE_uint64(uint64_flag, 64, "uint32 variable test");
DS_DEFINE_int64(int64_flag, 64, "uint32 variable test");
DS_DEFINE_string(str_flag, "default", "uint32 variable test");

int modifyCount = 0;
int defaultCount = 0;

void ResetCount()
{
    modifyCount = 0;
    defaultCount = 0;
}

static bool ValidateBoolean(const char *name, bool value)
{
    if (std::string(name) != "bool_flag") {
        return false;
    }
    if (value) {
        modifyCount++;
    } else {
        defaultCount++;
    }
    return true;
}

static bool ValidateUint32(const char *name, uint32_t value)
{
    if (std::string(name) != "uint32_flag") {
        return false;
    }
    if (value != 32) {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == 0) {
        return false;
    }
    return true;
}

static bool ValidateInt32(const char *name, int32_t value)
{
    if (std::string(name) != "int32_flag") {
        return false;
    }
    if (value != 32) {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == 0) {
        return false;
    }
    return true;
}

static bool ValidateUint64(const char *name, uint64_t value)
{
    if (std::string(name) != "uint64_flag") {
        return false;
    }
    if (value != 64) {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == 0) {
        return false;
    }
    return true;
}

static bool ValidateInt64(const char *name, int64_t value)
{
    if (std::string(name) != "int64_flag") {
        return false;
    }
    if (value != 64) {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == 0) {
        return false;
    }
    return true;
}

static bool ValidateString(const char *name, const std::string &value)
{
    if (std::string(name) != "str_flag") {
        return false;
    }
    if (value != "default") {
        modifyCount++;
    } else {
        defaultCount++;
    }

    if (value == "xxx") {
        return false;
    }
    return true;
}

DS_DEFINE_validator(bool_flag, &ValidateBoolean);
DS_DEFINE_validator(uint32_flag, &ValidateUint32);
DS_DEFINE_validator(int32_flag, &ValidateInt32);
DS_DEFINE_validator(uint64_flag, &ValidateUint64);
DS_DEFINE_validator(int64_flag, &ValidateInt64);
DS_DEFINE_validator(str_flag, &ValidateString);

#define ASSERT_ABNORMAL_VALUE(name, set_value, expect_value, contain_msg) \
    do {                                                                  \
        std::string errMsg;                                               \
        ASSERT_FALSE(SetCommandLineOption(#name, set_value, errMsg));     \
        ASSERT_TRUE(errMsg.find(contain_msg) != errMsg.npos);             \
        ASSERT_EQ(FLAGS_##name, expect_value);                            \
    } while (0)

namespace datasystem {
namespace ut {
class FlagsTest : public ::testing::Test {
public:
};

TEST_F(FlagsTest, TestDefaultValue)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);
    ASSERT_EQ(FLAGS_bool_flag, false);
    ASSERT_EQ(FLAGS_uint32_flag, 32u);
    ASSERT_EQ(FLAGS_int32_flag, 32);
    ASSERT_EQ(FLAGS_uint64_flag, 64ul);
    ASSERT_EQ(FLAGS_int64_flag, 64l);
    ASSERT_EQ(FLAGS_str_flag, "default");
    ASSERT_EQ(defaultCount, 12);
    ASSERT_EQ(modifyCount, 0);
}

TEST_F(FlagsTest, TestSetValue)
{
    const char *argv[] = { "./program", "-bool_flag",       "true",      "-uint32_flag=64",      "--int32_flag",
                           "-64",       "--int64_flag=-64", "-str_flag", "Hextech Flashtraption" };
    const int setValueFirstDefaultValue = 7;
    ParseCommandLineFlags(9, (char **)argv);
    ASSERT_EQ(FLAGS_bool_flag, true);
    ASSERT_EQ(FLAGS_uint32_flag, 64u);
    ASSERT_EQ(FLAGS_int32_flag, -64);
    ASSERT_EQ(FLAGS_uint64_flag, 64ul);
    ASSERT_EQ(FLAGS_int64_flag, -64l);
    ASSERT_EQ(FLAGS_str_flag, "Hextech Flashtraption");
    ASSERT_EQ(defaultCount, setValueFirstDefaultValue);
    ASSERT_EQ(modifyCount, 5);

    ResetCount();
    std::string errMsg;
    ASSERT_TRUE(SetCommandLineOption("bool_flag", "f", errMsg));
    ASSERT_EQ(FLAGS_bool_flag, false);
    ASSERT_TRUE(errMsg.empty());
    ASSERT_EQ(defaultCount, 1);
    ASSERT_EQ(modifyCount, 0);

    ASSERT_TRUE(SetCommandLineOption("uint32_flag", "123 ", errMsg));
    ASSERT_EQ(FLAGS_uint32_flag, 123UL);
}

TEST_F(FlagsTest, TestOutOfRangeValue)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    // uint32_t
    ASSERT_ABNORMAL_VALUE(uint32_flag, "-1", 32u, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint32_flag, "5294967296", 32u, "illegal value");

    // int32_t
    ASSERT_ABNORMAL_VALUE(int32_flag, "-5294967296", 32, "illegal value");
    ASSERT_ABNORMAL_VALUE(int32_flag, "5294967296", 32, "illegal value");

    // uint64_t
    ASSERT_ABNORMAL_VALUE(uint64_flag, "-1", 64ul, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "18446744073709551616", 64ul, "illegal value");

    // int64_t
    ASSERT_ABNORMAL_VALUE(int64_flag, "9223372036854775809", 64, "illegal value");
    ASSERT_ABNORMAL_VALUE(int64_flag, "-9223372036854775809", 64, "illegal value");
}

TEST_F(FlagsTest, TestSetAbsentFlag)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    std::string errMsg;
    ResetCount();
    ASSERT_FALSE(SetCommandLineOption("absent_flag", "who care", errMsg));
    ASSERT_TRUE(errMsg.find("not found") != errMsg.npos);
    ASSERT_EQ(defaultCount, 0);
    ASSERT_EQ(modifyCount, 0);
}

TEST_F(FlagsTest, TestSetIllegalValue)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    // Boolean
    ASSERT_ABNORMAL_VALUE(bool_flag, "xxx", false, "illegal value");
    ASSERT_ABNORMAL_VALUE(bool_flag, "", false, "illegal value");

    // uint32_t
    ASSERT_ABNORMAL_VALUE(uint32_flag, "xxx", 32u, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint32_flag, "", 32u, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint32_flag, "-100", 32u, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint32_flag, "123abc", 32u, "illegal value");

    // int32_t
    ASSERT_ABNORMAL_VALUE(int32_flag, "xxx", 32, "illegal value");
    ASSERT_ABNORMAL_VALUE(int32_flag, "", 32, "illegal value");
    ASSERT_ABNORMAL_VALUE(int32_flag, "123abc", 32, "illegal value");

    // uint64_t
    ASSERT_ABNORMAL_VALUE(uint64_flag, "xxx", 64ul, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "", 64ul, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "-100", 64ul, "illegal value");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "123abc", 64ul, "illegal value");

    // int64_t
    ASSERT_ABNORMAL_VALUE(int64_flag, "xxx", 64, "illegal value");
    ASSERT_ABNORMAL_VALUE(int64_flag, "", 64, "illegal value");
    ASSERT_ABNORMAL_VALUE(int64_flag, "123abc", 64, "illegal value");
}

TEST_F(FlagsTest, TestFailValidation)
{
    const char *argv[] = { "./program" };
    ParseCommandLineFlags(1, (char **)argv);

    ASSERT_ABNORMAL_VALUE(uint32_flag, "0", 32u, "failed validation");
    ASSERT_ABNORMAL_VALUE(int32_flag, "0", 32, "failed validation");
    ASSERT_ABNORMAL_VALUE(uint64_flag, "0", 64ul, "failed validation");
    ASSERT_ABNORMAL_VALUE(int64_flag, "0", 64, "failed validation");
    ASSERT_ABNORMAL_VALUE(str_flag, "xxx", "default", "failed validation");
}

TEST_F(FlagsTest, TestGetAllFlags)
{
    const char *argv[] = { "./program", "-uint32_flag=32" };
    ParseCommandLineFlags(1, (char **)argv);

    std::vector<FlagInfo> output;
    GetAllFlags(output);
    ASSERT_EQ(output.size(), 8ul);

    for (const auto &info : output) {
        ASSERT_TRUE(info.isDefault);
    }

    std::string errMsg;
    ASSERT_TRUE(SetCommandLineOption("uint32_flag", "1", errMsg));
    ASSERT_FALSE(SetCommandLineOption("uint64_flag", "-1", errMsg));
    ASSERT_TRUE(SetCommandLineOption("bool_flag", "true", errMsg));
    ASSERT_FALSE(SetCommandLineOption("str_flag", "xxx", errMsg));

    output.clear();
    GetAllFlags(output);
    ASSERT_EQ(output.size(), 8ul);

    int modCount = 0;

    for (const auto &info : output) {
        if (!info.isDefault) {
            modCount++;
        }
    }
    ASSERT_EQ(modCount, 2);
}

TEST_F(FlagsTest, TestGetBooleanEnv)
{
    std::vector<std::string> commanlines = {
        "bool1=false", "bool2=true", "bool3=f", "bool4=t",  "bool5=0", "bool6=1",
        "bool7=no",    "bool8=yes",  "bool9=n", "bool10=y", "str=xxx", "empty=",
    };
    for (const auto &commandline : commanlines) {
        ASSERT_EQ(putenv((char *)commandline.c_str()), 0);
    }

    // normal.
    ASSERT_FALSE(GetBoolFromEnv("bool1", true));
    ASSERT_TRUE(GetBoolFromEnv("bool2", false));
    ASSERT_FALSE(GetBoolFromEnv("bool3", true));
    ASSERT_TRUE(GetBoolFromEnv("bool4", false));
    ASSERT_FALSE(GetBoolFromEnv("bool5", true));
    ASSERT_TRUE(GetBoolFromEnv("bool6", false));
    ASSERT_FALSE(GetBoolFromEnv("bool7", true));
    ASSERT_TRUE(GetBoolFromEnv("bool8", false));
    ASSERT_FALSE(GetBoolFromEnv("bool9", true));
    ASSERT_TRUE(GetBoolFromEnv("bool10", false));

    // abnormal
    ASSERT_FALSE(GetBoolFromEnv("str", false));
    ASSERT_TRUE(GetBoolFromEnv("empty", true));
    ASSERT_TRUE(GetBoolFromEnv(nullptr, true));
}

TEST_F(FlagsTest, TestGet32BitEnv)
{
    std::vector<std::string> commanlines = {
        "num1=1",
        "uint32_max=4294967295",
        "uint32_min=0",
        "uint32_max_out=4294967296",
        "uint32_min_out=-1",
        "int32_max=2147483647",
        "int32_min=-2147483648",
        "int32_max_out=2147483648",
        "int32_min_out=-2147483649",
        "str=xxx",
        "empty=",
    };
    for (const auto &commandline : commanlines) {
        ASSERT_EQ(putenv((char *)commandline.c_str()), 0);
    }

    // normal
    ASSERT_EQ(GetUint32FromEnv("num1", 0), 1u);
    ASSERT_EQ(GetUint32FromEnv("uint32_max", 0), UINT32_MAX);
    ASSERT_EQ(GetUint32FromEnv("uint32_min", 1), 0u);

    ASSERT_EQ(GetInt32FromEnv("num1", 0), 1);
    ASSERT_EQ(GetInt32FromEnv("int32_max", 0), INT32_MAX);
    ASSERT_EQ(GetInt32FromEnv("int32_min", 1), INT32_MIN);

    // abnormal
    ASSERT_EQ(GetUint32FromEnv("uint32_max_out", 100u), 100u);
    ASSERT_EQ(GetUint32FromEnv("uint32_min_out", 100u), 100u);
    ASSERT_EQ(GetUint32FromEnv("empty", 0), 0u);
    ASSERT_EQ(GetUint32FromEnv("str", 0), 0u);
    ASSERT_EQ(GetUint32FromEnv("no_exist", 0), 0u);
    ASSERT_EQ(GetUint32FromEnv(nullptr, 0), 0u);

    ASSERT_EQ(GetInt32FromEnv("int32_max_out", 100), 100);
    ASSERT_EQ(GetInt32FromEnv("int32_min_out", 100), 100);
    ASSERT_EQ(GetInt32FromEnv("empty", 0), 0);
    ASSERT_EQ(GetInt32FromEnv("str", 0), 0);
    ASSERT_EQ(GetInt32FromEnv("no_exist", 0), 0);
    ASSERT_EQ(GetInt32FromEnv(nullptr, 0), 0);
}

TEST_F(FlagsTest, TestGet64BitEnv)
{
    std::vector<std::string> commanlines = {
        "num1=1",
        "uint64_max=18446744073709551615",
        "uint64_min=0",
        "uint64_max_out=18446744073709551616",
        "uint64_min_out=-1",
        "int64_max=9223372036854775807",
        "int64_min=-9223372036854775808",
        "int64_max_out=9223372036854775808",
        "int64_min_out=-9223372036854775809",
        "str=xxx",
        "empty=",
    };
    for (const auto &commandline : commanlines) {
        ASSERT_EQ(putenv((char *)commandline.c_str()), 0);
    }

    // normal
    ASSERT_EQ(GetUint64FromEnv("num1", 0), 1u);
    ASSERT_EQ(GetUint64FromEnv("uint64_max", 0), UINT64_MAX);
    ASSERT_EQ(GetUint64FromEnv("uint64_min", 1), 0ul);

    ASSERT_EQ(GetInt64FromEnv("num1", 0), 1);
    ASSERT_EQ(GetInt64FromEnv("int64_max", 0), INT64_MAX);
    ASSERT_EQ(GetInt64FromEnv("int64_min", 1), INT64_MIN);

    // abnormal
    ASSERT_EQ(GetUint64FromEnv("uint64_max_out", 100ul), 100ul);
    ASSERT_EQ(GetUint64FromEnv("uint64_min_out", 100ul), 100ul);
    ASSERT_EQ(GetUint64FromEnv("empty", 0), 0ul);
    ASSERT_EQ(GetUint64FromEnv("str", 0), 0ul);
    ASSERT_EQ(GetUint64FromEnv("no_exist", 0), 0ul);
    ASSERT_EQ(GetUint64FromEnv(nullptr, 0), 0ul);

    ASSERT_EQ(GetInt64FromEnv("int64_max_out", 100), 100);
    ASSERT_EQ(GetInt64FromEnv("int64_min_out", 100), 100);
    ASSERT_EQ(GetInt64FromEnv("empty", 0), 0);
    ASSERT_EQ(GetInt64FromEnv("str", 0), 0);
    ASSERT_EQ(GetInt64FromEnv("no_exist", 0), 0);
    ASSERT_EQ(GetInt64FromEnv(nullptr, 0), 0);
}

TEST_F(FlagsTest, TestGetStringEnv)
{
    std::vector<std::string> commanlines = { "num1=32", "num4=-9223372036854775809", "str=/tmp/xxx1/xxx2/xxx3",
                                             "empty=" };
    for (const auto &commandline : commanlines) {
        ASSERT_EQ(putenv((char *)commandline.c_str()), 0);
    }

    ASSERT_EQ(GetStringFromEnv("num1", "xxx"), "32");
    ASSERT_EQ(GetStringFromEnv("num4", "xxx"), "-9223372036854775809");
    ASSERT_EQ(GetStringFromEnv("str", "xxx"), "/tmp/xxx1/xxx2/xxx3");
    ASSERT_EQ(GetStringFromEnv("empty", "xxx"), "");
    ASSERT_EQ(GetStringFromEnv("no_exist", "xxx"), "xxx");
    ASSERT_EQ(GetStringFromEnv(nullptr, "xxx"), "xxx");
}
}  // namespace ut
}  // namespace datasystem

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
