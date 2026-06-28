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

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <set>
#include <thread>
#include <vector>

#include "datasystem/common/rpc/brpc_async_context.h"

namespace datasystem {

// Basic lifecycle: AllocateTag produces valid tag, TakeCall succeeds.
TEST(BrpcAsyncContextTest, AllocateAndTake)
{
    BrpcAsyncContext ctx;
    int64_t tag = ctx.AllocateTag();
    EXPECT_GT(tag, 0);
    auto call = ctx.TakeCall(tag);
    EXPECT_NE(call, nullptr);
    call = ctx.TakeCall(tag);  // already consumed
    EXPECT_EQ(call, nullptr);
}

// GetCall: positive and negative lookups.
TEST(BrpcAsyncContextTest, GetCallPositiveAndNegative)
{
    BrpcAsyncContext ctx;
    int64_t tag = ctx.AllocateTag();
    auto call = ctx.GetCall(tag);
    EXPECT_NE(call, nullptr);
    EXPECT_FALSE(call->completed);
    EXPECT_EQ(ctx.GetCall(99999), nullptr);  // never allocated
}

// ForgetCall removes entry; double-forget and never-allocated are safe no-ops.
TEST(BrpcAsyncContextTest, ForgetCall)
{
    BrpcAsyncContext ctx;
    int64_t tag = ctx.AllocateTag();
    ctx.ForgetCall(tag);
    EXPECT_EQ(ctx.GetCall(tag), nullptr);
    ctx.ForgetCall(tag);     // already forgotten, must not crash
    ctx.ForgetCall(99999);   // never allocated, must not crash
}

// Tag IDs are monotonically increasing (not necessarily sequential under concurrency,
// but each call returns a strictly larger value).
TEST(BrpcAsyncContextTest, AllocateTagMonotonic)
{
    BrpcAsyncContext ctx;
    int64_t prev = ctx.AllocateTag();
    for (int i = 0; i < 1000; ++i) {
        int64_t curr = ctx.AllocateTag();
        EXPECT_GT(curr, prev) << "tag IDs must be monotonically increasing";
        prev = curr;
    }
}

// Sequential tags map to round-robin buckets (tagId & 31).
TEST(BrpcAsyncContextTest, TagsSpreadAcrossBuckets)
{
    BrpcAsyncContext ctx;
    // Allocate kShardCount tags; each should land in a distinct bucket.
    std::set<size_t> buckets;
    for (size_t i = 0; i < BrpcAsyncContext::kShardCount; ++i) {
        int64_t tag = ctx.AllocateTag();
        size_t bucket = static_cast<size_t>(tag) & (BrpcAsyncContext::kShardCount - 1);
        EXPECT_EQ(bucket, static_cast<size_t>(tag % BrpcAsyncContext::kShardCount))
            << "bitwise AND should match modulo for power-of-2 shard count";
        buckets.insert(bucket);
    }
    EXPECT_EQ(buckets.size(), BrpcAsyncContext::kShardCount)
        << "first kShardCount sequential tags should hit every bucket";
}

// Concurrent correctness: N threads allocate, read back via GetCall,
// then TakeCall (exactly once) and verify the tag is gone after take.
TEST(BrpcAsyncContextTest, ConcurrentMixedLifecycle)
{
    static constexpr int kNumThreads = 16;
    static constexpr int kOpsPerThread = 500;
    BrpcAsyncContext ctx;
    std::atomic<int> allocOk{0};
    std::atomic<int> getOk{0};
    std::atomic<int> takeOk{0};
    std::atomic<int> goneAfterTakeOk{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&ctx, &allocOk, &getOk, &takeOk, &goneAfterTakeOk]() {
            std::vector<int64_t> localTags;
            localTags.reserve(kOpsPerThread);
            for (int i = 0; i < kOpsPerThread; ++i) {
                int64_t tag = ctx.AllocateTag();
                if (tag > 0) allocOk.fetch_add(1, std::memory_order_relaxed);
                localTags.push_back(tag);
            }
            for (int64_t tag : localTags) {
                auto call = ctx.GetCall(tag);
                if (call) {
                    getOk.fetch_add(1, std::memory_order_relaxed);
                    auto taken = ctx.TakeCall(tag);
                    if (taken) {
                        takeOk.fetch_add(1, std::memory_order_relaxed);
                        if (ctx.GetCall(tag) == nullptr) {
                            goneAfterTakeOk.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(allocOk.load(), kNumThreads * kOpsPerThread);
    EXPECT_EQ(getOk.load(), kNumThreads * kOpsPerThread);
    EXPECT_EQ(takeOk.load(), kNumThreads * kOpsPerThread);
    EXPECT_EQ(goneAfterTakeOk.load(), kNumThreads * kOpsPerThread);
}

// MakeDone produces a non-null Closure; verify cv wait/wake semantics
// by having a waiter thread block on cv.wait() and a notifier thread
// simulate OnRpcDone's signal path.
TEST(BrpcAsyncContextTest, MakeDoneAndCvWaitWake)
{
    BrpcAsyncContext ctx;
    int64_t tag = ctx.AllocateTag();
    auto call = ctx.GetCall(tag);
    ASSERT_NE(call, nullptr);

    auto* done = ctx.MakeDone(call);
    EXPECT_NE(done, nullptr);

    std::atomic<bool> waiterStarted{false};
    std::atomic<bool> waiterWoken{false};

    // Waiter thread: blocks on cv.wait() (simulates AsyncRead)
    std::thread waiter([&call, &waiterStarted, &waiterWoken]() {
        std::unique_lock<std::mutex> lock(call->mtx);
        waiterStarted.store(true);
        call->cv.wait(lock, [&call] { return call->completed; });
        waiterWoken.store(true);
    });

    // Wait for waiter to actually block on cv
    while (!waiterStarted.load()) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Notifier: simulates OnRpcDone (runs on brpc bthread in production)
    {
        std::lock_guard<std::mutex> lock(call->mtx);
        call->completed = true;
        call->cv.notify_one();
    }

    waiter.join();

    EXPECT_TRUE(call->completed);
    EXPECT_TRUE(waiterWoken.load());
    ctx.ForgetCall(tag);
}

}  // namespace datasystem
