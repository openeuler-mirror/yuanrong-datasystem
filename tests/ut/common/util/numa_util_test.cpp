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

#include <future>
#include <thread>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

#include "datasystem/common/util/numa_util.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace ut {
class NumaUtilTest : public CommonTest {};

namespace {
pid_t ForkForTest(std::function<void()> func)
{
    pid_t child = fork();
    if (child == 0) {
        std::thread thread(std::move(func));
        thread.join();
        exit(0);
    }
    return child;
}

int WaitForChildFork(pid_t pid)
{
    if (pid == 0) {
        return 0;
    }
    int statLoc = 0;
    if (waitpid(pid, &statLoc, 0) < 0) {
        return -1;
    }
    if (WIFEXITED(statLoc)) {
        return WEXITSTATUS(statLoc);
    }
    return -1;
}
}  // namespace

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

TEST_F(NumaUtilTest, BuildsEqualRoundRobinNumaBindingRanges)
{
    constexpr size_t totalSize = 8 * 1024 * 1024;
    using BuildPlanFunction = Status (*)(uint8_t *, size_t, uint32_t, const std::vector<int> &,
                                         std::vector<NumaBindingRange> &);
    BuildPlanFunction buildPlan = &BuildRoundRobinNumaBindingPlan;
    std::vector<uint8_t> memory(totalSize);
    std::vector<NumaBindingRange> ranges;

    DS_ASSERT_OK(buildPlan(memory.data(), totalSize, 4, { 2, 6 }, ranges));

    ASSERT_EQ(ranges.size(), 4u);
    constexpr size_t rangeSize = totalSize / 4;
    for (size_t i = 0; i < ranges.size(); ++i) {
        EXPECT_EQ(ranges[i].pointer, memory.data() + i * rangeSize);
        EXPECT_EQ(ranges[i].size, rangeSize);
        EXPECT_EQ(ranges[i].nodeId, i % 2 == 0 ? 2 : 6);
    }
}

TEST_F(NumaUtilTest, CoversDistinctChipsBeforeReusingNumaNodes)
{
    constexpr size_t totalSize = 8 * 1024 * 1024;
    std::vector<uint8_t> memory(totalSize);
    std::vector<NumaBindingRange> ranges;

    DS_ASSERT_OK(BuildRoundRobinNumaBindingPlan(memory.data(), totalSize, 2, { 0, 1, 2, 3 }, ranges));

    ASSERT_EQ(ranges.size(), 2u);
    EXPECT_EQ(NumaIdToChipId(ranges[0].nodeId, 4), 1);
    EXPECT_EQ(NumaIdToChipId(ranges[1].nodeId, 4), 2);
}

TEST_F(NumaUtilTest, RejectsInvalidNumaBindingPlans)
{
    std::vector<uint8_t> memory(8193);
    std::vector<NumaBindingRange> ranges = { { memory.data(), 4096, 0 } };

    EXPECT_EQ(BuildRoundRobinNumaBindingPlan(nullptr, 8192, 2, { 0, 1 }, ranges).GetCode(), K_INVALID);
    EXPECT_TRUE(ranges.empty());
    EXPECT_EQ(BuildRoundRobinNumaBindingPlan(memory.data(), 0, 2, { 0, 1 }, ranges).GetCode(), K_INVALID);
    EXPECT_EQ(BuildRoundRobinNumaBindingPlan(memory.data(), 8192, 0, { 0, 1 }, ranges).GetCode(), K_INVALID);
    EXPECT_EQ(BuildRoundRobinNumaBindingPlan(memory.data(), 8193, 2, { 0, 1 }, ranges).GetCode(),
              K_INVALID);
    EXPECT_EQ(BuildRoundRobinNumaBindingPlan(memory.data(), 8192, 2, {}, ranges).GetCode(), K_INVALID);
}

TEST_F(NumaUtilTest, TestNumaIdToChipIdConcurrentFirstCallDoesNotCrash)
{
    constexpr int threadCount = 32;
    constexpr int repeatPerThread = 1000;

    pid_t child = ForkForTest([]() {
        std::promise<void> ready;
        std::shared_future<void> start(ready.get_future());
        ThreadPool pool(threadCount);
        std::vector<std::future<void>> futures;
        futures.reserve(threadCount);
        for (int i = 0; i < threadCount; ++i) {
            futures.emplace_back(pool.Submit([start]() mutable {
                start.wait();
                for (int j = 0; j < repeatPerThread; ++j) {
                    const uint8_t chipId = NumaIdToChipId(0);
                    ASSERT_TRUE(chipId == 1 || chipId == INVALID_CHIP_ID);
                }
            }));
        }

        ready.set_value();
        for (auto &future : futures) {
            future.get();
        }
    });

    ASSERT_EQ(WaitForChildFork(child), 0);
}
}  // namespace ut
}  // namespace datasystem
