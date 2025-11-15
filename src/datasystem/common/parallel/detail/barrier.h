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
 * Description: A generic barrier implementation for thread synchronization.
 */
#ifndef DATASYSTEM_COMMON_PARALLEL_BARRIER_H
#define DATASYSTEM_COMMON_PARALLEL_BARRIER_H

#include <mutex>
#include <atomic>

#define DS_UNLIKELY(x) __builtin_expect(!!(x), 0)

namespace datasystem {
namespace Parallel {

template <typename T> class Barrier {
public:
    Barrier(const Barrier<T> &) = delete;
    Barrier(Barrier<T> &&) = delete;
    Barrier<T> &operator = (const Barrier<T> &) = delete;
    Barrier<T> &operator = (Barrier<T> &&) = delete;

    Barrier()
    {
        semData = new T();
    }

    ~Barrier()
    {
        if (semData != nullptr) {
            semData->SemDestroy();
            delete semData;
            semData = nullptr;
        }
    }

    inline void ForkBarrier(uint32_t initCnt)
    {
        // just master thread call it
        semData->SemInit(0);
        awaited = initCnt;
    }

    inline void JoinBarrier(bool isMaster)
    {
        if (DS_UNLIKELY(isMaster)) {
            semData->SemPend();
            // after wait, master should destroy it
            awaited = 0;
            return;
        }
        // workers meet join barrier point
        if (DS_UNLIKELY(awaited.fetch_add(-1, std::memory_order_relaxed) == 1)) {
            semData->SemPost();
        }
    }

private:
    T *semData;
    std::atomic<int32_t> awaited { 0 };
};
}
}

#endif