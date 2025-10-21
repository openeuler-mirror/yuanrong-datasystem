/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description:
 */
#include <gtest/gtest.h>
#include <algorithm>

#include "common.h"
#include "client/object_cache/oc_client_common.h"

namespace datasystem {
namespace st {
class KVClientWithNoHeartbeatTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -ipc_through_shared_memory=false";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }
};

TEST_F(KVClientWithNoHeartbeatTest, TestSingleKey)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key = "key1";
    std::string value = "value1";
    ASSERT_EQ(client->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));

    ASSERT_EQ(client->Del(key), Status::OK());
    ASSERT_EQ(client->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);

    ASSERT_EQ(client->Del(key).GetCode(), StatusCode::K_OK);

    ASSERT_EQ(client->Del("key2").GetCode(), StatusCode::K_OK);
}
}  // namespace st
}  // namespace datasystem
