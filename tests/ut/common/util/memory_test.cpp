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
 * Description: Test memory util function.
 */
#include "ut/common.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/random_data.h"

namespace datasystem {
namespace ut {
class MemoryTest : public CommonTest {
public:
    uint8_t *AllocateMemory(int64_t size)
    {
        return (uint8_t *)malloc(size);
    }

    void FreeMemory(uint8_t *point)
    {
        free(point);
    }

    void MemoryCopyTest(std::map<std::string, int64_t> &dataMap)
    {
        for (auto &it : dataMap) {
            LOG(INFO) << "test data size: " << it.first;
            auto src = AllocateMemory(it.second);
            auto dst = AllocateMemory(it.second);
            memset_s(src, it.second, 2, it.second);
            DS_ASSERT_OK(MemoryCopy(dst, it.second, src, it.second, threadPool));
            FreeMemory(src);
            FreeMemory(dst);
        }
    }

    std::shared_ptr<ThreadPool> threadPool = std::make_shared<ThreadPool>(20);
};

TEST_F(MemoryTest, MemoryCopyLessThan1M)
{
    std::map<std::string, int64_t> dataMap = { { "10K", 10 * 1024 }, { "35K", 35 * 1024 }, { "100K", 100 * 1024 } };
    MemoryCopyTest(dataMap);
}

TEST_F(MemoryTest, MemoryCopyGreateThan1M)
{
    std::map<std::string, int64_t> dataMap = { { "2M", 2 * 1024 * 1024 },
                                               { "25M", 25 * 1024 * 1024 },
                                               { "100M", 100 * 1024 * 1024 } };
    MemoryCopyTest(dataMap);
}

TEST_F(MemoryTest, MemoryCopySuccess)
{
    auto dst = AllocateMemory(10);
    std::string src = "abc";
    MemoryCopy(dst, 10, reinterpret_cast<const uint8_t *>(src.data()), src.length(), threadPool);
    std::string result((const char *)dst, src.length());
    ASSERT_EQ(src, result);
    FreeMemory(dst);
}

TEST_F(MemoryTest, MemoryCopyFailed)
{
    int64_t size = 10 * 1024 * 1024;
    auto src = AllocateMemory(size + 20);
    auto dst = AllocateMemory(size);
    memset_s(src, size + 20, 2, size + 20);
    DS_ASSERT_NOT_OK(MemoryCopy(dst, size, src, size + 20, threadPool));
    FreeMemory(dst);
    FreeMemory(src);
}

TEST_F(MemoryTest, MemoryCopyToLargeDestSize)
{
    int64_t size = 3 * 1024L * 1024L * 1024L;
    auto src = AllocateMemory(size);
    auto dst = AllocateMemory(size * 2);
    auto value = 2;
    memset_s(src, size, value, size);
    DS_ASSERT_OK(MemoryCopy(dst, size * 2, src, size, threadPool));
    FreeMemory(dst);
    FreeMemory(src);
}

TEST_F(MemoryTest, CopySmallBlockToLargeBlock)
{
    int64_t dstSize = 2 * 1024L * 1024L * 1024L;
    int64_t srcSize = 8;
    auto src = AllocateMemory(srcSize);
    auto dst = AllocateMemory(dstSize);
    auto value = 2;
    memset_s(src, srcSize, value, srcSize);
    DS_ASSERT_OK(MemoryCopy(dst, dstSize, src, srcSize, threadPool));
    FreeMemory(dst);
    FreeMemory(src);
}
}  // namespace ut
}  // namespace datasystem
