/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
#include <unordered_map>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/kv_client.h"
#include "datasystem/perf_client.h"

namespace datasystem {
namespace st {
class PerfClientTest : public ExternalClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        int32_t port = GetFreePort();
        opts.workerConfigs.emplace_back(hostIp, port);
        opts.numEtcd = 1;
        workerAddr_ = HostPort(hostIp, port);
    }

protected:
    HostPort workerAddr_;
};

TEST_F(PerfClientTest, TestGetPerfLogAndReset)
{
    int32_t timeout = 1000;
    ConnectOptions connectOptions = { .host = workerAddr_.Host(),
                                      .port = workerAddr_.Port(),
                                      .connectTimeoutMs = timeout };
    connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
    connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    auto client = std::make_shared<KVClient>(connectOptions);
    DS_ASSERT_OK(client->Init());
    ASSERT_EQ(client->Set("key", "value"), Status::OK());

    auto perfClient = std::make_shared<PerfClient>(connectOptions);
    perfClient->Init();

    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> clientPerfLog;
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> workerPerfLog;
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> clientPerfLog2;
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> workerPerfLog2;

    DS_ASSERT_OK(perfClient->GetPerfLog("client", clientPerfLog));
    DS_ASSERT_OK(perfClient->GetPerfLog("worker", workerPerfLog));
    ASSERT_TRUE(!clientPerfLog.empty());
    ASSERT_TRUE(!workerPerfLog.empty());

    DS_ASSERT_OK(perfClient->ResetPerfLog("client"));
    DS_ASSERT_OK(perfClient->GetPerfLog("client", clientPerfLog2));
    DS_ASSERT_OK(perfClient->ResetPerfLog("worker"));
    DS_ASSERT_OK(perfClient->GetPerfLog("worker", workerPerfLog2));
    ASSERT_TRUE(clientPerfLog.size() != clientPerfLog2.size());
    ASSERT_TRUE(workerPerfLog.size() != workerPerfLog2.size());
}

}  // namespace st
}  // namespace datasystem
