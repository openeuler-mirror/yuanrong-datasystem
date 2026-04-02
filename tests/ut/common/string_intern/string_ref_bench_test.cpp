/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Testing StringRef.
 */
#include <algorithm>
#include <gtest/gtest.h>
#include <numeric>
#include <random>
#include <unordered_map>

#include "ut/common.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
namespace ut {
const size_t threadCnt8 = 8;
class StringRefBenchTest : public CommonTest {
public:
    std::string GetBenchCost(std::vector<std::vector<double>> &costsVec)
    {
        std::vector<double> costs;
        for (const auto &c : costsVec) {
            for (const auto &cost : c) {
                costs.emplace_back(cost);
            }
        }
        std::stringstream ss;
        std::sort(costs.begin(), costs.end());
        // remove last
        double totalTimeCost = std::accumulate(costs.begin(), costs.end(), 0.0);  // ms
        double avg = totalTimeCost / static_cast<double>(costs.size());
        auto count = costs.size();
        const size_t p90 = 90;
        const size_t p99 = 90;
        const size_t pmax = 100;
        ss << costs.size();
        ss << "," << avg;                        // avg ms
        ss << "," << costs[0];                   // min ms
        ss << "," << costs[count * p90 / pmax];  // p90 ms
        ss << "," << costs[count * p99 / pmax];  // p99 ms
        ss << "," << costs[count - 1];           // max ms
        return ss.str();
    }

    static std::string GenUniqueString(size_t threadIdx, size_t iter)
    {
        const size_t appendSize = 64;
        return "thread_" + std::to_string(threadIdx) + "_iter_" + std::to_string(iter) + "_"
               + std::string(appendSize, 'a');
    }

    static std::string GenDupString(size_t threadIdx, size_t iter)
    {
        (void)threadIdx;
        const size_t appendSize = 64;
        const size_t count = 100;
        return "thread_x_iter_" + std::to_string(iter % count) + "_" + std::string(appendSize, 'a');
    }

    template <typename T, typename G, typename F>
    void PerfIntern(size_t threadCnt, G &&gen, F &&fn)
    {
        const size_t countPerThread = 100'000;
        const size_t batchCnt = 1024;
        std::vector<std::thread> threads;
        // generate string
        std::vector<std::vector<std::string>> datas;
        std::vector<std::vector<double>> costPerThread1;
        std::vector<std::vector<double>> costPerThread2;
        std::vector<std::vector<double>> costPerThread3;
        datas.resize(threadCnt);
        costPerThread1.resize(threadCnt);
        costPerThread2.resize(threadCnt);
        costPerThread3.resize(threadCnt);
        Barrier barrier1(threadCnt);
        Barrier barrier2(threadCnt);
        Barrier barrier3(threadCnt);
        using TbbMap = tbb::concurrent_hash_map<T, uint64_t>;
        TbbMap secondMap;
        for (size_t i = 0; i < threadCnt; i++) {
            auto &data = datas[i];
            auto &costs1 = costPerThread1[i];
            auto &costs2 = costPerThread2[i];
            auto &costs3 = costPerThread3[i];
            data.reserve(countPerThread);
            costs1.reserve(countPerThread / batchCnt);
            costs2.reserve(countPerThread / batchCnt);
            costs3.reserve(countPerThread / batchCnt);

            for (size_t n = 0; n < countPerThread; n++) {
                data.emplace_back(std::move(gen(i, n)));
            }
            std::shuffle(data.begin(), data.end(), std::mt19937{ std::random_device{}() });
            threads.emplace_back([&] {
                std::vector<T> keys;
                keys.reserve(countPerThread);
                barrier1.Wait();
                // test intern
                for (size_t i = 0; i < countPerThread; i += batchCnt) {
                    Timer timer1;
                    for (size_t b = 0; b < batchCnt && i + b < countPerThread; b++) {
                        keys.emplace_back(fn(data[i + b]));
                    }
                    costs1.emplace_back(timer1.ElapsedMilliSecond());
                }
                barrier2.Wait();
                // test intern key insert
                size_t count = 0;
                Timer timer2;
                for (const auto &key : keys) {
                    typename TbbMap::const_accessor accessor;
                    secondMap.insert(accessor, key);
                    if (count == batchCnt) {
                        count = 0;
                        costs2.emplace_back(timer2.ElapsedMilliSecondAndReset());
                    }
                    count += 1;
                }

                barrier3.Wait();
                // test find
                Timer timer3;
                count = 0;
                for (const auto &key : keys) {
                    typename TbbMap::const_accessor accessor;
                    secondMap.find(accessor, key);
                    if (count == batchCnt) {
                        count = 0;
                        costs3.emplace_back(timer3.ElapsedMilliSecondAndReset());
                    }
                    count += 1;
                }
            });
        }
        for (auto &t : threads) {
            t.join();
        }
        std::string caseName;
        std::string name;
        ut::GetCurTestName(caseName, name);
        LOG(INFO) << "BENCHMARK," << caseName << "," << name << ",Thread-" << threadCnt << ", intern,"
                  << GetBenchCost(costPerThread1);
        LOG(INFO) << "BENCHMARK," << caseName << "," << name << ",Thread-" << threadCnt << ", key insert,"
                  << GetBenchCost(costPerThread2);
        LOG(INFO) << "BENCHMARK," << caseName << "," << name << ",Thread-" << threadCnt << ", key find,"
                  << GetBenchCost(costPerThread3);
    }
};

TEST_F(StringRefBenchTest, StringRefInternUnique1)
{
    PerfIntern<ObjectKey>(1, GenUniqueString, ObjectKey::Intern);
}

TEST_F(StringRefBenchTest, StringRefInternDuplicate1)
{
    PerfIntern<ObjectKey>(1, GenDupString, ObjectKey::Intern);
}

TEST_F(StringRefBenchTest, StringRefInternUnique8)
{
    PerfIntern<ObjectKey>(threadCnt8, GenUniqueString, ObjectKey::Intern);
}

TEST_F(StringRefBenchTest, StringRefInternDuplicate8)
{
    PerfIntern<ObjectKey>(threadCnt8, GenDupString, ObjectKey::Intern);
}

TEST_F(StringRefBenchTest, StdStringUnique1)
{
    PerfIntern<std::string>(1, GenUniqueString, [](const std::string &str) { return std::string(str); });
}

TEST_F(StringRefBenchTest, StdStringDuplicate1)
{
    PerfIntern<std::string>(1, GenDupString, [](const std::string &str) { return std::string(str); });
}

TEST_F(StringRefBenchTest, StdStringUnique8)
{
    PerfIntern<std::string>(threadCnt8, GenUniqueString, [](const std::string &str) { return std::string(str); });
}

TEST_F(StringRefBenchTest, StdStringDuplicate8)
{
    PerfIntern<std::string>(threadCnt8, GenDupString, [](const std::string &str) { return std::string(str); });
}

TEST_F(StringRefBenchTest, ImmutableStringInternUnique1)
{
    PerfIntern<ImmutableString>(1, GenUniqueString, [](const std::string &str) { return ImmutableString(str); });
}

TEST_F(StringRefBenchTest, ImmutableStringInternDuplicate1)
{
    PerfIntern<ImmutableString>(1, GenDupString, [](const std::string &str) { return ImmutableString(str); });
}

TEST_F(StringRefBenchTest, ImmutableStringInternUnique8)
{
    PerfIntern<ImmutableString>(threadCnt8, GenUniqueString,
                                [](const std::string &str) { return ImmutableString(str); });
}

TEST_F(StringRefBenchTest, ImmutableStringInternDuplicate8)
{
    PerfIntern<ImmutableString>(threadCnt8, GenDupString, [](const std::string &str) { return ImmutableString(str); });
}
}  // namespace ut
}  // namespace datasystem
