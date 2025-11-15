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

#include "bench_base.h"

#include <thread>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "kv/kv_args.h"
#include "kv/kv_bench.h"

namespace datasystem {
namespace bench {
Status BenchBase::Create(std::unique_ptr<ArgsBase> &args, std::unique_ptr<BenchBase> &bench)
{
    if (args->command == "kv") {
        auto kvArgs = dynamic_cast<KVArgs*>(args.get());
        if (!kvArgs) {
            return Status(K_INVALID, "Invalid KVArgs");
        }
        bench = std::make_unique<KVBench>(*kvArgs);
        return Status::OK();
    }
    return Status(K_INVALID, "Unknown command " + args->command);
}

Status BenchBase::Start()
{
    RETURN_IF_NOT_OK(perf_.Init());
    RETURN_IF_NOT_OK(WarmUp());
    RETURN_IF_NOT_OK(perf_.ResetPerfLog());
    RETURN_IF_NOT_OK(ParallelRun());
    RETURN_IF_NOT_OK(perf_.SaveAllPerfLog());
    RETURN_IF_NOT_OK(perf_.ResetPerfLog());
    RETURN_IF_NOT_OK(PrintBenchmarkInfo());
    return Status::OK();
}

Status BenchBase::ParallelRun()
{
    auto threadNum = argsBase_.threadNum;
    if (threadNum <= 0) {
        return Status(K_INVALID, "Invalid argument: threadNum must be positive");
    }
    perThreadStatus_.clear();
    perThreadStatus_.resize(threadNum);
    perThreadCostDetail_.clear();
    perThreadCostDetail_.resize(threadNum);
    perThreadCost_.clear();
    perThreadCost_.resize(threadNum);

    std::vector<std::thread> threads;
    threads.reserve(threadNum);
    Barrier barrier(threadNum);
    RETURN_IF_NOT_OK(Prepare());
    for (uint32_t t = 0; t < threadNum; t++) {
        threads.emplace_back([this, t, &barrier] { perThreadStatus_[t] = Run(t, barrier); });
    }

    for (auto &t : threads) {
        t.join();
    }

    for (const auto &s : perThreadStatus_) {
        if (s.IsError()) {
            return s;
        }
    }
    return Status::OK();
}
}  // namespace bench
}  // namespace datasystem
