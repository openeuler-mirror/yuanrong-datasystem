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

#include "datasystem/common/util/request_context.h"

#include <atomic>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

#include <bthread/bthread.h>

#include "gtest/gtest.h"

namespace datasystem {

// bthread_key_create is called with a nullptr destructor (see request_context.cpp
// InitRequestContext). RequestContext instances are always stack-allocated via
// ScopedRequestContext, which clears the bthread_key (SetRequestContext(nullptr))
// in its destructor BEFORE the stack object goes out of scope. The non-RAII test
// paths below must mirror that contract: clear the key before the stack context
// is destroyed, otherwise the next bthread that reuses this keytable slot would
// see a dangling pointer.
//
// The fixture's TearDown() clears the main thread. Child threads clear
// themselves before exiting.

class RequestContextTest : public ::testing::Test {
protected:
    void TearDown() override
    {
        // Ensure no dangling stack pointer remains in the bthread key
        // when the test's stack-allocated contexts go out of scope.
        SetRequestContext(nullptr);
    }
};

// ============================================================================
// Test 1: Null check -- Get returns null before Set, no crash without Init
// ============================================================================
TEST_F(RequestContextTest, GetWithoutInitReturnsNonNull)
{
    // GetRequestContext() never returns nullptr: falls back to per-pthread
    // static thread_local RequestContext when no handler context is active.
    ASSERT_NE(GetRequestContext(), nullptr);
}

TEST_F(RequestContextTest, GetBeforeSetReturnsNonNull)
{
    InitRequestContext();
    // After Init but before Set, still returns valid pointer via fallback.
    ASSERT_NE(GetRequestContext(), nullptr);
}

// ============================================================================
// Test 2: Basic Set/Get cycle
// ============================================================================
TEST_F(RequestContextTest, BasicSetGet)
{
    InitRequestContext();

    RequestContext ctx;
    SetRequestContext(&ctx);
    ASSERT_EQ(GetRequestContext(), &ctx);
}

TEST_F(RequestContextTest, SetDifferentContextOnSameBthread)
{
    InitRequestContext();

    RequestContext ctx1;
    RequestContext ctx2;

    SetRequestContext(&ctx1);
    ASSERT_EQ(GetRequestContext(), &ctx1);

    SetRequestContext(&ctx2);
    ASSERT_EQ(GetRequestContext(), &ctx2);
}

// ============================================================================
// Test 4: TimeCost isolation between two contexts on same thread
// ============================================================================
TEST_F(RequestContextTest, TimeCostIsolation)
{
    InitRequestContext();

    RequestContext ctxA;
    RequestContext ctxB;

    // Populate ctxA
    SetRequestContext(&ctxA);
    ctxA.workerTimeCost.Append("ctxA_only", 10);
    ctxA.masterTimeCost.Append("ctxA_master", 10);

    // Switch to ctxB, populate it
    SetRequestContext(&ctxB);
    ctxB.workerTimeCost.Append("ctxB_only", 10);

    // ctxB should NOT contain ctxA's entries
    std::string infoB = ctxB.workerTimeCost.GetInfo();
    ASSERT_EQ(infoB.find("ctxA_only"), std::string::npos);
    ASSERT_NE(infoB.find("ctxB_only"), std::string::npos);

    // ctxA should NOT contain ctxB's entries
    SetRequestContext(&ctxA);
    std::string infoA = ctxA.workerTimeCost.GetInfo();
    ASSERT_NE(infoA.find("ctxA_only"), std::string::npos);
    ASSERT_EQ(infoA.find("ctxB_only"), std::string::npos);
}

// ============================================================================
// Test 5: GetWorkerTimeCost / GetMasterTimeCost convenience helpers
// ============================================================================
TEST_F(RequestContextTest, ConvenienceHelpersUseRequestContext)
{
    InitRequestContext();

    RequestContext ctx;
    ctx.workerTimeCost.Append("helper_worker_op", 10);
    ctx.masterTimeCost.Append("helper_master_op", 10);
    SetRequestContext(&ctx);

    TimeCost& wc = GetWorkerTimeCost();
    TimeCost& mc = GetMasterTimeCost();

    ASSERT_NE(wc.GetInfo().find("helper_worker_op"), std::string::npos);
    ASSERT_NE(mc.GetInfo().find("helper_master_op"), std::string::npos);
}

// ============================================================================
// Test 6: Clear semantics on same context
// ============================================================================
TEST_F(RequestContextTest, ClearTimeCost)
{
    InitRequestContext();

    RequestContext ctx;
    SetRequestContext(&ctx);

    ctx.workerTimeCost.Append("before_clear", 10);
    ASSERT_NE(ctx.workerTimeCost.GetInfo().find("before_clear"), std::string::npos);

    ctx.workerTimeCost.Clear();
    ASSERT_EQ(ctx.workerTimeCost.GetInfo().find("before_clear"), std::string::npos);
}

// ============================================================================
// Test 7: Many threads with independent contexts (stress test)
// ============================================================================
TEST_F(RequestContextTest, ManyThreadIsolation)
{
    static constexpr int kNumThreads = 32;
    InitRequestContext();

    std::vector<std::thread> threads;
    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([i]() {
            RequestContext ctx;
            char buf[64];
            snprintf(buf, sizeof(buf), "thread_%d_op", i);
            ctx.workerTimeCost.Append(buf, 10);
            SetRequestContext(&ctx);

            std::string info = GetRequestContext()->workerTimeCost.GetInfo();
            ASSERT_NE(info.find(buf), std::string::npos);

            // Should NOT see other threads' ops
            char other[64];
            for (int j = 0; j < kNumThreads; ++j) {
                if (j == i) continue;
                snprintf(other, sizeof(other), "thread_%d_op", j);
                ASSERT_EQ(info.find(other), std::string::npos);
            }

            SetRequestContext(nullptr);
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

// ============================================================================
// Test 8: ScopedRequestContext RAII clears the key on scope exit
// ============================================================================
// This is the only production entry point used by brpc handlers. The contract:
// regardless of how the handler exits (normal return, RETURN_IF_NOT_OK early
// exit, exception), the destructor must SetRequestContext(nullptr) BEFORE the
// stack RequestContext is destroyed, so the next bthread reusing this
// keytable slot cannot observe a dangling pointer.
TEST_F(RequestContextTest, ScopedRequestContextClearsKeyOnExit)
{
    InitRequestContext();

    // After a ScopedRequestContext scope ends, GetRequestContext() must NOT
    // return the address of the destroyed stack object.
    RequestContext* danglingAddr = nullptr;
    {
        ScopedRequestContext scoped;
        RequestContext* inScope = GetRequestContext();
        ASSERT_NE(inScope, nullptr);
        danglingAddr = inScope;
        // duration=10 >= TimeCost::timeThreshold_(3) so Append records it.
        GetWorkerTimeCost().Append("scoped_op", 10);
        ASSERT_NE(GetWorkerTimeCost().GetInfo().find("scoped_op"), std::string::npos);
    }
    // Out of scope: key must be cleared, so GetRequestContext returns the
    // per-pthread fallback (different address), never the destroyed stack object.
    RequestContext* afterScope = GetRequestContext();
    ASSERT_NE(afterScope, danglingAddr);
}

// Simulate a handler that returns early (RETURN_IF_NOT_OK pattern). The scope
// exit must still clear the key even when control leaves the block abruptly.
TEST_F(RequestContextTest, ScopedRequestContextClearsKeyOnEarlyExit)
{
    InitRequestContext();
    auto runHandler = []() {
        ScopedRequestContext scoped;
        // duration=10 >= TimeCost::timeThreshold_(3) so Append records it
        // into the scoped context's workerTimeCost, not the fallback's.
        GetWorkerTimeCost().Append("before_early_return", 10);
        // Simulate RETURN_IF_NOT_OK(condition) early exit.
        if (true) {
            return;  // destructor must still run and clear the key.
        }
        GetWorkerTimeCost().Append("unreachable", 10);
    };
    runHandler();
    // After early-exit handler returns, key must be cleared.
    RequestContext* fallback = GetRequestContext();
    ASSERT_NE(fallback, nullptr);
    // The fallback's workerTimeCost must not contain the early-exit op,
    // proving the scoped context did not leak into the fallback slot.
    ASSERT_EQ(GetWorkerTimeCost().GetInfo().find("before_early_return"), std::string::npos);
}

// Throw inside a ScopedRequestContext scope: RAII must still clear the bthread
// key during stack unwinding, so the next caller on this bthread does not see
// a dangling pointer to the destroyed stack RequestContext.
TEST_F(RequestContextTest, ScopedRequestContextClearsKeyOnException)
{
    InitRequestContext();
    RequestContext* danglingAddr = nullptr;
    try {
        ScopedRequestContext scoped;
        RequestContext* inScope = GetRequestContext();
        ASSERT_NE(inScope, nullptr);
        danglingAddr = inScope;
        // duration=10 >= TimeCost::timeThreshold_(3) so Append records it.
        GetWorkerTimeCost().Append("before_throw", 10);
        throw std::runtime_error("test exception for RAII unwind");
    } catch (const std::runtime_error&) {
        // Stack unwound; ScopedRequestContext destructor ran during unwinding.
    }
    // After exception handler returns, bthread key must be cleared, so
    // GetRequestContext returns the per-pthread fallback (different address),
    // never the destroyed stack object.
    RequestContext* afterCatch = GetRequestContext();
    ASSERT_NE(afterCatch, danglingAddr);
    // The fallback's workerTimeCost must not contain the thrown-scope op,
    // proving the scoped context did not leak into the fallback slot.
    ASSERT_EQ(GetWorkerTimeCost().GetInfo().find("before_throw"), std::string::npos);
}

// ============================================================================
// Test 9: bthread M:N isolation -- multiple bthreads on shared pthreads
// ============================================================================
// Validates the core PR #1170 invariant under brpc's actual M:N scheduling:
// when multiple bthreads run cooperatively on shared pthreads, each bthread
// must observe its own RequestContext via bthread_getspecific. This covers
// the scenario std::thread-based tests cannot reach (c5 reviewer concern).
//
// Layout: N bthreads are started via bthread_start_background. Each registers
// a unique stack RequestContext, appends a unique marker to its workerTimeCost,
// then loops ROUNDS times: validate own context + own marker, then yield.
// bthread_yield() forces the brpc scheduler to swap to another bthread on
// the same pthread. After all bthreads finish, ASSERT no cross-contamination.
namespace {
struct BthreadMNArg {
    int id;
    std::string marker;
    std::atomic<int>* violations;
};

void* BthreadMNWorkerFn(void* raw)
{
    auto* arg = static_cast<BthreadMNArg*>(raw);
    RequestContext myCtx;
    SetRequestContext(&myCtx);
    // duration=10 >= TimeCost::timeThreshold_(3) so Append records it.
    GetWorkerTimeCost().Append(arg->marker.c_str(), 10);

    constexpr int ROUNDS = 100;
    for (int i = 0; i < ROUNDS; i++) {
        RequestContext* seen = GetRequestContext();
        if (seen != &myCtx) {
            arg->violations->fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        std::string info = GetWorkerTimeCost().GetInfo();
        if (info.find(arg->marker) == std::string::npos) {
            arg->violations->fetch_add(1, std::memory_order_relaxed);
        }
        // Yield forces the brpc scheduler to swap to another bthread.
        bthread_yield();
    }

    SetRequestContext(nullptr);
    return nullptr;
}
}  // namespace

TEST_F(RequestContextTest, BthreadMNIsolation)
{
    InitRequestContext();

    constexpr int NUM_BTHREADS = 16;
    std::atomic<int> violations{ 0 };
    std::vector<BthreadMNArg> args(NUM_BTHREADS);
    std::vector<bthread_t> bids(NUM_BTHREADS);

    for (int i = 0; i < NUM_BTHREADS; i++) {
        args[i].id = i;
        args[i].marker = std::string("bthread_") + std::to_string(i) + "_marker";
        args[i].violations = &violations;
    }

    for (int i = 0; i < NUM_BTHREADS; i++) {
        ASSERT_EQ(bthread_start_background(&bids[i], nullptr, BthreadMNWorkerFn, &args[i]), 0)
            << "bthread_start_background failed for bthread " << i;
    }
    for (int i = 0; i < NUM_BTHREADS; i++) {
        ASSERT_EQ(bthread_join(bids[i], nullptr), 0)
            << "bthread_join failed for bthread " << i;
    }

    // Zero violations = every bthread always saw its own RequestContext + marker,
    // even after bthread_yield forced pthread rescheduling under M:N.
    ASSERT_EQ(violations.load(), 0)
        << "Bthread M:N isolation broken: " << violations.load() << " violations detected";
}

// ScopedRequestContext must be non-copyable and non-movable, so it can never
// be moved out of its handler's stack frame (which would break the RAII
// contract on early return).
static_assert(!std::is_copy_constructible<ScopedRequestContext>::value,
              "ScopedRequestContext must not be copy-constructible");
static_assert(!std::is_move_constructible<ScopedRequestContext>::value,
              "ScopedRequestContext must not be move-constructible");
static_assert(!std::is_copy_assignable<ScopedRequestContext>::value,
              "ScopedRequestContext must not be copy-assignable");
static_assert(!std::is_move_assignable<ScopedRequestContext>::value,
              "ScopedRequestContext must not be move-assignable");

}  // namespace datasystem
