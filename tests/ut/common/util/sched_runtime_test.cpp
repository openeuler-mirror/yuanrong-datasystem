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

#include "datasystem/common/util/sched_runtime.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {

TEST(SchedRuntimeTest, DisabledSettingSkipsSchedulerSyscall)
{
    const auto result = SetCurrentThreadSchedRuntime(false);

    EXPECT_TRUE(result.skipped);
    EXPECT_FALSE(result.success);
    EXPECT_EQ(result.err, 0);
}

}  // namespace ut
}  // namespace datasystem
