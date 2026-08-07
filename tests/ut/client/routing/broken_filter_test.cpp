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

#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/client/routing/broken_filter.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {
constexpr auto BROKEN_FILTER_RECOVERY_TIMEOUT = std::chrono::seconds(6);
constexpr auto BROKEN_FILTER_POLL_INTERVAL = std::chrono::milliseconds(20);
// Must match BrokenFilter::EVICT_CONSECUTIVE_FAILURES. A single (or few) disconnect must NOT
// evict a worker globally -- that was the code=37 cascade. Only a sustained burst evicts.
constexpr int EVICT_THRESHOLD = 100;
}  // namespace

class BrokenFilterTest : public CommonTest {
protected:
    HostPort worker_{ "127.0.0.1", 1000 };
};

TEST_F(BrokenFilterTest, DisconnectsBelowThresholdDoNotEvict)
{
    client::BrokenFilter filter;

    EXPECT_TRUE(filter.IsAvailable(worker_));
    // A few transient blips (e.g. jitter-induced K_RPC_PEER_DEAD) must not evict the worker.
    for (int i = 0; i < EVICT_THRESHOLD - 1; ++i) {
        filter.OnWorkerStateChange(worker_, K_CLIENT_WORKER_DISCONNECT);
        EXPECT_TRUE(filter.IsAvailable(worker_)) << "evicted after only " << (i + 1) << " failures";
    }
    // Reaching the threshold within the burst window evicts the worker.
    filter.OnWorkerStateChange(worker_, K_CLIENT_WORKER_DISCONNECT);
    EXPECT_FALSE(filter.IsAvailable(worker_));
}

TEST_F(BrokenFilterTest, OtherStatusCodesAreIgnored)
{
    client::BrokenFilter filter;

    filter.OnWorkerStateChange(worker_, K_RUNTIME_ERROR);
    EXPECT_TRUE(filter.IsAvailable(worker_));

    for (int i = 0; i < EVICT_THRESHOLD; ++i) {
        filter.OnWorkerStateChange(worker_, K_CLIENT_WORKER_DISCONNECT);
    }
    filter.OnWorkerStateChange(worker_, K_NOT_OWNER);  // non-disconnect status is ignored
    EXPECT_FALSE(filter.IsAvailable(worker_));  // still broken from the disconnect burst
}

TEST_F(BrokenFilterTest, WorkerBecomesAvailableAfterTtl)
{
    client::BrokenFilter filter;

    for (int i = 0; i < EVICT_THRESHOLD; ++i) {
        filter.OnWorkerStateChange(worker_, K_CLIENT_WORKER_DISCONNECT);
    }
    EXPECT_FALSE(filter.IsAvailable(worker_));

    const auto deadline = std::chrono::steady_clock::now() + BROKEN_FILTER_RECOVERY_TIMEOUT;
    while (!filter.IsAvailable(worker_) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(BROKEN_FILTER_POLL_INTERVAL);
    }
    EXPECT_TRUE(filter.IsAvailable(worker_));
}

TEST_F(BrokenFilterTest, ConcurrentUpdatesAreNotLost)
{
    client::BrokenFilter filter;
    constexpr int workerCount = 64;
    std::vector<std::thread> threads;
    threads.reserve(workerCount);

    for (int i = 0; i < workerCount; ++i) {
        threads.emplace_back([&filter, i] {
            const HostPort addr("127.0.0.1", 1000 + i);
            for (int n = 0; n < EVICT_THRESHOLD; ++n) {
                filter.OnWorkerStateChange(addr, K_CLIENT_WORKER_DISCONNECT);
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    for (int i = 0; i < workerCount; ++i) {
        EXPECT_FALSE(filter.IsAvailable(HostPort("127.0.0.1", 1000 + i)));
    }
}

}  // namespace ut
}  // namespace datasystem
