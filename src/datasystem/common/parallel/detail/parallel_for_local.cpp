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
 * Description: A high-performance multi-threaded parallel computing framework.
 */

#include "datasystem/common/parallel/detail/parallel_for_local.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace Parallel {

ParallelThreadPool *ParallelThreadPool::Instance()
{
    static ParallelThreadPool threadPool;
    return &threadPool;
}

void ParallelThreadPool::InitThreadPool(int minThreadNum, int maxThreadNum)
{
    bool expected = false;
    if (isInit_.compare_exchange_strong(expected, true)) {
        threadPool_ = std::make_unique<ThreadPool>(minThreadNum, maxThreadNum, "parallel_for");
        threadPool_->SetWarnLevel(ThreadPool::WarnLevel::NO_WARN);
        threadNum_ = maxThreadNum == 0 ? minThreadNum : std::min(minThreadNum, maxThreadNum);
    }
}

void ParallelThreadPool::LocalSubmit(std::function<void()> &&func)
{
    (void)threadPool_->Submit(func);
}
}
}