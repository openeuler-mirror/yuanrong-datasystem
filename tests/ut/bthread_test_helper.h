/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Helpers for verifying scheduler progress under bthread lock contention.
 */

#ifndef DATASYSTEM_TESTS_UT_BTHREAD_TEST_HELPER_H
#define DATASYSTEM_TESTS_UT_BTHREAD_TEST_HELPER_H

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <vector>

#include <gtest/gtest.h>
#include "datasystem/common/rpc/bthread_utils.h"

namespace datasystem {
namespace ut {
inline void ExpectBthreadProgressWhileBlocked(const std::function<void(size_t)> &operation,
                                             const std::function<void()> &release)
{
    constexpr auto timeout = std::chrono::seconds(2);
    bthread_t warmup;
    const int warmupStatus = StartBackgroundTask(&warmup, [] {});
    if (warmupStatus != 0) {
        release();
        FAIL() << "Failed to initialize bthread workers: " << warmupStatus;
    }
    bthread_join(warmup, nullptr);
    constexpr size_t waitersPerWorker = 2;
    // Per-tag live worker counts can still grow during startup; use the configured total capacity.
    const auto workerCount = waitersPerWorker * static_cast<size_t>(bthread_getconcurrency());
    std::atomic<size_t> entered{ 0 };
    std::promise<void> allEntered;
    auto enteredFuture = allEntered.get_future();
    std::vector<bthread_t> tasks;
    tasks.reserve(workerCount);
    for (size_t index = 0; index < workerCount; ++index) {
        bthread_t task;
        const int rc = StartBackgroundTask(&task, [&, index] {
            if (entered.fetch_add(1) + 1 == workerCount) {
                allEntered.set_value();
            }
            operation(index);
        });
        EXPECT_EQ(rc, 0);
        if (rc != 0) {
            break;
        }
        tasks.push_back(task);
    }
    const bool enteredAll = enteredFuture.wait_for(timeout) == std::future_status::ready;
    std::promise<void> probe;
    auto probeFuture = probe.get_future();
    bthread_t probeTask;
    const int probeStatus = StartBackgroundTask(&probeTask, [&] { probe.set_value(); });
    const bool progressed = probeStatus == 0 && probeFuture.wait_for(timeout) == std::future_status::ready;
    // Release from the native test thread even on failure so the old blocking implementation can drain.
    release();
    for (auto task : tasks) {
        bthread_join(task, nullptr);
    }
    if (probeStatus == 0) {
        bthread_join(probeTask, nullptr);
    }
    EXPECT_TRUE(enteredAll);
    EXPECT_TRUE(progressed) << "Contended locks exhausted the bthread worker pool";
}
}  // namespace ut
}  // namespace datasystem
#endif
