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
 * Description: Datasystem unit test base class for benchmark.
 */
#ifndef DATASYSTEM_TEST_UT_BENCH_HELPER_H
#define DATASYSTEM_TEST_UT_BENCH_HELPER_H

#include <algorithm>
#include <numeric>
#include <random>
#include <thread>
#include <vector>
#include <sstream>
#include "common.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
namespace ut {
class BenchHelper {
public:
    static std::string GetBenchCost(std::vector<std::vector<double>> &costsVec)
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

    template <typename G, typename F1, typename F2>
    void PerfTwoAction(size_t threadCnt, G &&gen, F1 &&fn1, F2 &&fn2)
    {
        const size_t countPerThread = 102'400;
        const size_t batchCnt = 1024;
        std::vector<std::thread> threads;
        // generate string
        std::vector<std::vector<std::string>> datas;
        std::vector<std::vector<double>> costPerThread1;
        std::vector<std::vector<double>> costPerThread2;
        datas.resize(threadCnt);
        costPerThread1.resize(threadCnt);
        costPerThread2.resize(threadCnt);
        Barrier barrier1(threadCnt);
        Barrier barrier2(threadCnt);
        for (size_t i = 0; i < threadCnt; i++) {
            auto &data = datas[i];
            auto &costs1 = costPerThread1[i];
            auto &costs2 = costPerThread2[i];
            data.reserve(countPerThread);
            costs1.reserve(countPerThread / batchCnt);
            costs2.reserve(countPerThread / batchCnt);

            for (size_t n = 0; n < countPerThread; n++) {
                data.emplace_back(std::move(gen(i, n)));
            }
            std::shuffle(data.begin(), data.end(), std::mt19937{ std::random_device{}() });
            threads.emplace_back([&] {
                size_t count = 0;
                barrier1.Wait();
                Timer timer1;
                for (const auto &key : data) {
                    fn1(key);
                    if (count == batchCnt) {
                        count = 0;
                        costs1.emplace_back(timer1.ElapsedMilliSecondAndReset());
                    }
                    count += 1;
                }
                barrier2.Wait();
                Timer timer2;
                count = 0;
                for (const auto &key : data) {
                    fn2(key);
                    if (count == batchCnt) {
                        count = 0;
                        costs2.emplace_back(timer2.ElapsedMilliSecondAndReset());
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
        LOG(ERROR) << "BENCHMARK," << caseName << "," << name << ",Thread-" << threadCnt << ", Action1,"
                   << GetBenchCost(costPerThread1);
        LOG(ERROR) << "BENCHMARK," << caseName << "," << name << ",Thread-" << threadCnt << ", Action2,"
                   << GetBenchCost(costPerThread2);
    }
};
}  // namespace ut
}  // namespace datasystem
#endif
