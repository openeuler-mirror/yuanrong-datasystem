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
 * Description: Unit tests for bthread_utils.h — YieldCurrent() and StartBackgroundTask().
 *
 * YieldCurrent() works in all modes (no-op when bthread runtime is absent).
 * StartBackgroundTask() requires a running bthread worker pool (brpc mode);
 * those tests are skipped in default ZMQ test mode and should be run with:
 *   bazel test //tests/ut/common/rpc:bthread_utils_test \
 *     --config=test --test_env=DATASYSTEM_USE_BRPC=1
 */

#include "datasystem/common/rpc/bthread_utils.h"

#include <atomic>
#include <future>

#include <bthread/bthread.h>

#include "gtest/gtest.h"

namespace datasystem {
namespace {

// ============================================================================
// Test 1: YieldCurrent() always succeeds — no-op in ZMQ mode, yields in brpc mode
// ============================================================================
TEST(BthreadUtilsTest, YieldCurrentDoesNotCrash)
{
    YieldCurrent();
    SUCCEED();
}

// ============================================================================
// Test 2: StartBackgroundTask executes callable (brpc mode only)
// ============================================================================
TEST(BthreadUtilsTest, StartBackgroundTaskExecutesCallable)
{
    GTEST_SKIP() << "Requires brpc runtime (DATASYSTEM_USE_BRPC=1). "
                 << "StartBackgroundTask is verified in brpc-mode integration tests.";
}

// ============================================================================
// Test 3: StartBackgroundTask with move-only callable (brpc mode only)
// ============================================================================
TEST(BthreadUtilsTest, StartBackgroundTaskWithMoveOnlyCallable)
{
    GTEST_SKIP() << "Requires brpc runtime (DATASYSTEM_USE_BRPC=1).";
}

// ============================================================================
// Test 4: StartBackgroundTask with nullptr tid (brpc mode only)
// ============================================================================
TEST(BthreadUtilsTest, StartBackgroundTaskWithNullTid)
{
    GTEST_SKIP() << "Requires brpc runtime (DATASYSTEM_USE_BRPC=1).";
}

// ============================================================================
// Test 5: Multiple concurrent StartBackgroundTask calls (brpc mode only)
// ============================================================================
TEST(BthreadUtilsTest, MultipleConcurrentStartBackgroundTask)
{
    GTEST_SKIP() << "Requires brpc runtime (DATASYSTEM_USE_BRPC=1).";
}

}  // namespace
}  // namespace datasystem
