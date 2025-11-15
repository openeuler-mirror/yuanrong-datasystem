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

#ifndef BENCH_BASE_H
#define BENCH_BASE_H
#include <vector>
#include "args_base.h"
#include "bench_perf.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/utils/status.h"
namespace datasystem {

namespace bench {
class BenchBase {
public:
    static Status Create(std::unique_ptr<ArgsBase> &args, std::unique_ptr<BenchBase> &bench);
    BenchBase(const BenchBase &) = delete;
    BenchBase &operator=(const BenchBase &) = delete;
    virtual ~BenchBase() = default;

    Status Start();

protected:
    BenchBase(ArgsBase &argsBase) : argsBase_(argsBase), perf_(argsBase)
    {
    }
    virtual Status WarmUp() = 0;
    virtual Status Prepare() = 0;
    virtual Status Run(uint64_t threadIndex, Barrier &barrier) = 0;
    virtual Status PrintBenchmarkInfo() = 0;
    Status ParallelRun();

    std::vector<Status> perThreadStatus_;
    std::vector<std::vector<uint64_t>> perThreadCostDetail_;
    std::vector<uint64_t> perThreadCost_;
    ArgsBase &argsBase_;
    PerfManager perf_;
};
}  // namespace bench
}  // namespace datasystem
#endif
