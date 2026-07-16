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

/** Description: Tests object-cache service execution policy. */

#include <numeric>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/worker/object_cache/service/service_execution_policy.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr int TASK_COUNT = 3;
constexpr int FAILED_TASK_INDEX = 1;

TEST(ServiceExecutionPolicyTest, BrpcModeDisablesFutureFanout)
{
    EXPECT_FALSE(ShouldUseServiceThreadPoolFanout(true));
    EXPECT_TRUE(ShouldUseServiceThreadPoolFanout(false));
}

TEST(ServiceExecutionPolicyTest, SerialFanoutRunsAllTasksBeforeReturningFirstError)
{
    std::vector<int> tasks(TASK_COUNT);
    std::iota(tasks.begin(), tasks.end(), 0);
    std::vector<int> calls;

    Status rc = RunServiceTasksSerially(tasks.begin(), tasks.end(), [&calls](int taskIndex) {
        calls.emplace_back(taskIndex);
        if (taskIndex == FAILED_TASK_INDEX) {
            return Status(K_RUNTIME_ERROR, "injected failure");
        }
        return Status::OK();
    });

    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(calls, tasks);
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
