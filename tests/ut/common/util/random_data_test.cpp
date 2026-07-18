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
 * Description: Random data basic function test.
 */
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/strings_util.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {
class RandomDataTest : public CommonTest {
protected:
    RandomData rand_;
};

TEST_F(RandomDataTest, TestGetRandomUint8)
{
    LOG(INFO) << "Test get random uint8.";
    ASSERT_NE(rand_.GetRandomUint8(), rand_.GetRandomUint8());
}

TEST_F(RandomDataTest, TestGetRandomUint32)
{
    LOG(INFO) << "Test get random uint32.";
    ASSERT_NE(rand_.GetRandomUint32(), rand_.GetRandomUint32());
}

TEST_F(RandomDataTest, TestGetRandomUint64)
{
    LOG(INFO) << "Test get random uint64.";
    ASSERT_NE(rand_.GetRandomUint64(), rand_.GetRandomUint64());
}

TEST_F(RandomDataTest, TestGetRandomUint64WithRange)
{
    LOG(INFO) << "Test get random uint64.";
    uint64_t start = 1;
    uint64_t end = 10'000;
    ASSERT_NE(rand_.GetRandomUint64(start, end), rand_.GetRandomUint64(start, end));
}

TEST_F(RandomDataTest, TestGetRandomString)
{
    LOG(INFO) << "Test get random string.";
    std::string randString = rand_.GetRandomString(100);
    ASSERT_EQ(randString.size(), size_t(100));
}

TEST_F(RandomDataTest, TestGetRandomBytes)
{
    LOG(INFO) << "Test get random bytes.";
    std::vector<uint8_t> bytes = rand_.RandomBytes(100);
    ASSERT_EQ(bytes.size(), size_t(100));
}

TEST_F(RandomDataTest, TestGetRandomLength)
{
    LOG(INFO) << "Test get random length.";
    std::vector<size_t> lens = rand_.RandomLens(0, 100, 1'000);
    ASSERT_EQ(lens.size(), size_t(1'000));
    for (size_t len : lens) {
        ASSERT_GE(len, size_t(0));
        ASSERT_LE(len, size_t(100));
    }
}

TEST_F(RandomDataTest, TestGenBytes)
{
    Timer timer;
    auto bytes = rand_.RandomBytes(2'000'000);
    ASSERT_EQ(bytes.size(), size_t(2'000'000));
    LOG(INFO) << FormatString("elapsed: [%.6lf]s", timer.ElapsedSecond());
}

TEST_F(RandomDataTest, TestGenStr)
{
    Timer timer;
    auto bytes = rand_.GetRandomString(2'000'000);
    ASSERT_EQ(bytes.size(), size_t(2'000'000));
    LOG(INFO) << FormatString("elapsed: [%.6lf]s", timer.ElapsedSecond());
}

TEST_F(RandomDataTest, TestGenPartRandomStr)
{
    Timer timer;
    auto bytes = rand_.GetPartRandomString(2'000'000, 1000);
    ASSERT_EQ(bytes.size(), size_t(2'000'000));
    LOG(INFO) << FormatString("elapsed: [%.3lf]ms", timer.ElapsedMilliSecond());
}

class HRandomDataTest : public CommonTest {
protected:
    HRandomData rand_;
};

TEST_F(HRandomDataTest, TestGetRandomUint64)
{
    LOG(INFO) << "Test get hardware random uint64.";
    ASSERT_NE(rand_.GetRandomUint64(), rand_.GetRandomUint64());
}

TEST_F(HRandomDataTest, TestGetRandomUint64WithRange)
{
    LOG(INFO) << "Test get hardware random uint64.";
    uint64_t start = 1;
    uint64_t end = 10'000;
    ASSERT_NE(rand_.GetRandomUint64(start, end), rand_.GetRandomUint64(start, end));
}
}  // namespace ut
}  // namespace datasystem
