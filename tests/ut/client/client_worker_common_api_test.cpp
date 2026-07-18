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

#include "datasystem/client/client_worker_common_api.h"

#include <gtest/gtest.h>
#include <utility>

#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {

class TestClientWorkerRemoteCommonApi : public client::ClientWorkerRemoteCommonApi {
public:
    explicit TestClientWorkerRemoteCommonApi(HostPort hostPort)
        : client::IClientWorkerCommonApi(HostPort(hostPort), HeartbeatType::RPC_HEARTBEAT, false, nullptr),
          client::ClientWorkerRemoteCommonApi(std::move(hostPort))
    {
    }

    void ApplyMemoryAlignment(const RegisterClientRspPb &rsp)
    {
        UpdateMemoryAlignment(rsp);
    }
};

TEST(ClientWorkerCommonApiTest, UsesWorkerMemoryAlignmentAndFallsBackForOldWorker)
{
    TestClientWorkerRemoteCommonApi api(HostPort("127.0.0.1", 1));
    RegisterClientRspPb rsp;

    api.ApplyMemoryAlignment(rsp);
    EXPECT_EQ(api.GetMemoryAlignment(), 64u);

    rsp.set_memory_alignment(4096);
    api.ApplyMemoryAlignment(rsp);
    EXPECT_EQ(api.GetMemoryAlignment(), 4096u);
}

class InvalidWorkerMemoryAlignmentTest : public ::testing::TestWithParam<uint32_t> {
};

TEST_P(InvalidWorkerMemoryAlignmentTest, FallsBackToDefaultAlignment)
{
    TestClientWorkerRemoteCommonApi api(HostPort("127.0.0.1", 1));
    RegisterClientRspPb rsp;
    rsp.set_memory_alignment(GetParam());

    api.ApplyMemoryAlignment(rsp);

    EXPECT_EQ(api.GetMemoryAlignment(), 64u);
}

INSTANTIATE_TEST_SUITE_P(InvalidWorkerAlignments, InvalidWorkerMemoryAlignmentTest,
                         ::testing::Values(3u, 6u, 513u, 8192u));

}  // namespace
}  // namespace ut
}  // namespace datasystem
