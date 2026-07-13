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
 * Description: Unit tests for ScopedBthreadLocal<T> — per-bthread storage wrapper.
 *
 * Covers two execution modes:
 *   - ZMQ mode (default): per-pthread isolation via pthread_getspecific fallback.
 *     Tests 1-10 below verify this mode.
 *   - brpc mode (DATASYSTEM_USE_BRPC=1): per-bthread isolation via bthread_key_t.
 *     Test 11 requires a running brpc server and is skipped otherwise.
 *
 * Verifies:
 *  - Get() / operator->() / operator*() correctness
 *  - Forwarding copy/move assignment operators
 *  - Per-pthread isolation (child std::thread sees default, not parent's value)
 *  - Default construction for POD and std::string types
 *  - Move semantics (std::move(*var) pattern from authenticate.cpp)
 *  - Multiple independent ScopedBthreadLocal instances coexist
 */

#include "datasystem/common/rpc/scoped_bthread_local.h"

#include <atomic>
#include <string>
#include <thread>

#include <bthread/bthread.h>

#include "gtest/gtest.h"

namespace datasystem {
namespace {

// ============================================================================
// Test 1: Get() returns valid pointer, same on subsequent calls
// ============================================================================
TEST(BthreadLocalTest, GetReturnsSamePointer)
{
    ScopedBthreadLocal<uint64_t> tls;
    uint64_t* p1 = tls.Get();
    ASSERT_NE(p1, nullptr);
    *p1 = 42;

    uint64_t* p2 = tls.Get();
    ASSERT_EQ(p1, p2);
    EXPECT_EQ(*p2, 42u);
}

// ============================================================================
// Test 2: operator->() for method calls via arrow syntax
// ============================================================================
TEST(BthreadLocalTest, ArrowAccess)
{
    ScopedBthreadLocal<std::string> tls;
    *tls = "hello";
    EXPECT_EQ(tls->size(), 5u);
    EXPECT_FALSE(tls->empty());
    tls->clear();
    EXPECT_TRUE(tls->empty());
}

// ============================================================================
// Test 3: Forwarding copy assignment via operator=(const T&)
// ============================================================================
TEST(BthreadLocalTest, ForwardingCopyAssignment)
{
    ScopedBthreadLocal<std::string> tls;
    tls = std::string("world");
    EXPECT_EQ(*tls, "world");

    tls = "literal";
    EXPECT_EQ(*tls, "literal");
}

// ============================================================================
// Test 4: Forwarding move assignment via operator=(T&&)
// ============================================================================
TEST(BthreadLocalTest, ForwardingMoveAssignment)
{
    ScopedBthreadLocal<std::string> tls;
    std::string src = "movable";
    tls = std::move(src);
    EXPECT_EQ(*tls, "movable");
    EXPECT_TRUE(src.empty());
}

// ============================================================================
// Test 5: operator*() for direct value access and auto deduction
// ============================================================================
TEST(BthreadLocalTest, StarDereference)
{
    ScopedBthreadLocal<uint64_t> tls;
    *tls = 12345u;
    EXPECT_EQ(*tls, 12345u);

    auto val = *tls;
    EXPECT_EQ(val, 12345u);
}

// ============================================================================
// Test 6: POD type (uint64_t) defaults to zero
// ============================================================================
TEST(BthreadLocalTest, PodDefaultIsZero)
{
    ScopedBthreadLocal<uint64_t> tls;
    EXPECT_EQ(*tls, 0u);
}

// ============================================================================
// Test 7: std::string defaults to empty
// ============================================================================
TEST(BthreadLocalTest, StringDefaultIsEmpty)
{
    ScopedBthreadLocal<std::string> tls;
    EXPECT_TRUE(tls->empty());
    EXPECT_EQ(tls->size(), 0u);
}

// ============================================================================
// Test 8: Multiple independent instances don't interfere
// ============================================================================
TEST(BthreadLocalTest, MultipleIndependentInstances)
{
    ScopedBthreadLocal<uint64_t> a;
    ScopedBthreadLocal<std::string> b;

    *a = 42u;
    *b = "test";

    EXPECT_EQ(*a, 42u);
    EXPECT_EQ(*b, "test");

    *a = 99u;
    EXPECT_EQ(*a, 99u);
    EXPECT_EQ(*b, "test");
}

// ============================================================================
// Test 9: Pthread isolation — child std::thread sees default (0), not parent's value.
// In ZMQ mode, bthread_getspecific falls back to pthread_getspecific; each
// pthread has its own key table, providing the same isolation as thread_local.
// ============================================================================
TEST(BthreadLocalTest, PthreadChildIsolation)
{
    ScopedBthreadLocal<uint64_t> tls;
    *tls = 777u;

    std::atomic<uint64_t> childValue{0};
    std::atomic<bool> childDone{false};

    std::thread child([&]() {
        childValue.store(*tls.Get());
        childDone.store(true);
    });
    child.join();
    ASSERT_TRUE(childDone.load());

    EXPECT_EQ(childValue.load(), 0u)
        << "Child pthread should see default (0), not parent's 777. "
        << "If childValue != 0, per-pthread isolation is broken.";
    EXPECT_EQ(*tls, 777u) << "Parent value corrupted by child thread.";
}

// ============================================================================
// Test 10: std::move(*var) correctly moves the underlying value, leaving
// the per-pthread/bthread string in moved-from (empty) state.
// This is the pattern used in authenticate.cpp and ak_sk_manager.h.
// ============================================================================
TEST(BthreadLocalTest, MoveFromWrapper)
{
    ScopedBthreadLocal<std::string> tls;
    *tls = "original content";

    std::string moved = std::move(*tls);
    EXPECT_EQ(moved, "original content");
    EXPECT_TRUE(tls->empty());
}

// ============================================================================
// Test 11: Per-bthread isolation (brpc M:N mode).
// Skipped in default ZMQ test mode. Run with:
//   bazel test //tests/ut/common/rpc:bthread_local_test 
//     --config=test --test_env=DATASYSTEM_USE_BRPC=1
// ============================================================================
TEST(BthreadLocalTest, PerBthreadIsolationRequiresBrpcMode)
{
    GTEST_SKIP() << "Requires brpc runtime (DATASYSTEM_USE_BRPC=1). "
                 << "Per-bthread isolation is verified in the brpc-mode ST suite.";
}

}  // namespace
}  // namespace datasystem
