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

#include "datasystem/common/rdma/fast_transport_base.h"

namespace datasystem {
namespace {

TEST(UrmaRuntimePolicyTest, DeterminesWhetherClientMayAccessNonBoundWorker)
{
    EXPECT_TRUE(ClientMayAccessNonBoundWorker(false, false));
    EXPECT_TRUE(ClientMayAccessNonBoundWorker(false, true));
    EXPECT_TRUE(ClientMayAccessNonBoundWorker(true, true));
    EXPECT_FALSE(ClientMayAccessNonBoundWorker(true, false));
}

TEST(UrmaRuntimePolicyTest, DoesNotRequestRuntimeWithoutWorkerUbCapability)
{
    EXPECT_FALSE(ShouldRequestClientUrmaRuntime(false, false, false));
    EXPECT_FALSE(ShouldRequestClientUrmaRuntime(false, true, false));
    EXPECT_FALSE(ShouldRequestClientUrmaRuntime(false, false, true));
    EXPECT_FALSE(ShouldRequestClientUrmaRuntime(false, true, true));
}

TEST(UrmaRuntimePolicyTest, LocalCacheDisabledRemoteUbRequestsRuntime)
{
    EXPECT_TRUE(ShouldRequestClientUrmaRuntime(true, false, true));
}

TEST(UrmaRuntimePolicyTest, LocalCacheEnabledRemoteUbRequestsRuntime)
{
    EXPECT_TRUE(ShouldRequestClientUrmaRuntime(true, true, true));
}

TEST(UrmaRuntimePolicyTest, LocalCacheDisabledShmFirstRoutedClientRequestsRuntime)
{
    EXPECT_TRUE(ShouldRequestClientUrmaRuntime(true, true, false));
}

TEST(UrmaRuntimePolicyTest, LocalCacheEnabledPureShmClientSkipsRuntime)
{
    EXPECT_FALSE(ShouldRequestClientUrmaRuntime(true, false, false));
}

}  // namespace
}  // namespace datasystem
