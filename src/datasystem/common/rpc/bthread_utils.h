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
 * Description: Thin wrappers for bthread operations.
 *
 * Motivation: brpc uses M:N scheduling — N bthreads multiplexed on M pthread
 * workers. Direct use of std::thread permanently occupies a pthread; direct
 * bthread_yield() is scattered and hard to audit. This header provides
 * searchable, self-documenting wrappers:
 *   - YieldCurrent()     replaces bthread_yield()
 *   - SleepCurrentFor()  replaces sleep_for() in brpc handler retry loops
 *   - StartBackgroundTask() replaces std::thread(...).detach()
 */

#pragma once

#include <bthread/bthread.h>

#include <chrono>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace datasystem {

// Yield the current bthread without blocking the underlying pthread.
// Equivalent to bthread_yield() but greppable and self-documenting.
inline void YieldCurrent()
{
    bthread_yield();
}

template <typename Rep, typename Period>
inline void SleepCurrentFor(std::chrono::duration<Rep, Period> duration)
{
    static constexpr int64_t minSleepUs = 1;
    if (duration <= std::chrono::duration<Rep, Period>::zero()) {
        return;
    }
    auto durationUs = std::chrono::duration_cast<std::chrono::microseconds>(duration);
    if (durationUs < duration) {
        durationUs += std::chrono::microseconds(minSleepUs);
    }
    if (durationUs <= std::chrono::microseconds::zero()) {
        durationUs = std::chrono::microseconds(minSleepUs);
    }
    (void)bthread_usleep(durationUs.count());
}

// Start a background bthread to execute fn. Returns 0 on success,
// non-zero on failure (e.g. bthread creation failed).
// Unlike std::thread(...).detach(), this does NOT permanently occupy a
// pthread — the bthread runs on the brpc worker pool and the underlying
// pthread can be reused for other bthreads.
// Template parameter F supports both copyable and move-only callables
// (e.g. lambdas capturing std::promise by move).
template <typename F>
int StartBackgroundTask(bthread_t* tid, F&& fn)
{
    using FuncType = std::decay_t<F>;
    auto* ctx = new FuncType(std::forward<F>(fn));
    // brpc 1.15 TaskGroup::start_background unconditionally writes *tid after
    // successfully allocating the task — it does NOT check for nullptr. If the
    // caller passes nullptr (doesn't care about the ID), use a local placeholder
    // to avoid SIGSEGV inside brpc internals.
    bthread_t localTid{};
    bthread_t* tidPtr = (tid != nullptr) ? tid : &localTid;
    int rc = bthread_start_background(
        tidPtr, nullptr,
        [](void* arg) -> void* {
            auto* f = static_cast<FuncType*>(arg);
            (*f)();
            delete f;
            return nullptr;
        },
        ctx);
    // If bthread creation fails, the lambda is never invoked — clean up ctx
    // to avoid leaking the heap-allocated callable.
    if (rc != 0) {
        delete ctx;
    }
    return rc;
}

}  // namespace datasystem
