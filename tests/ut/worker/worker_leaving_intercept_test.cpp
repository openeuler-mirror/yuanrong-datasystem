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
 * Description: Unit tests for worker-side write rejection during topology scale-in.
 */

#include "datasystem/worker/object_cache/verify_leaving_state.h"

#include <gtest/gtest.h>

namespace datasystem::worker {
namespace {

TEST(WorkerLeavingInterceptTest, ActiveWorkerAcceptsWrites)
{
    auto rc = VerifyLeavingStateWithEvaluator(true, [] { return false; });

    EXPECT_TRUE(rc.IsOk());
}

TEST(WorkerLeavingInterceptTest, ExitingWorkerRejectsWrites)
{
    auto rc = VerifyLeavingStateWithEvaluator(true, [] { return true; });

    EXPECT_EQ(rc.GetCode(), K_SCALE_DOWN);
}

TEST(WorkerLeavingInterceptTest, DisabledInterceptionSkipsTopologyCheck)
{
    bool evaluated = false;
    auto rc = VerifyLeavingStateWithEvaluator(false, [&evaluated] {
        evaluated = true;
        return true;
    });

    EXPECT_TRUE(rc.IsOk());
    EXPECT_FALSE(evaluated);
}

TEST(WorkerLeavingInterceptTest, MissingExitSignalPreservesLegacyBehavior)
{
    auto rc = VerifyLeavingState(nullptr, true);

    EXPECT_TRUE(rc.IsOk());
}

}  // namespace
}  // namespace datasystem::worker
