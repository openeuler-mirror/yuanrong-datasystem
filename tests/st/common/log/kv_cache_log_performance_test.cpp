/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Test KV Cache log performance.
 */
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(alsologtostderr);
DS_DECLARE_uint32(max_log_size);

namespace datasystem {
namespace st {
class KVCacheLogPerformanceTest : public OCClientCommon {
public:
    void SetUp() override
    {
        FLAGS_v = 1;
        FLAGS_alsologtostderr = false;
        FLAGS_max_log_size = 1024; // 1024 MB
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ResetClients();
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 2;  // 2 workers
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }

        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -log_monitor=true -max_client_num=2000";
    }

    void InitClients(int num)
    {
        if (num <= 0) {
            return;
        }

        clients_.reserve(num);
        for (int i = 0; i < num; ++i) {
            std::shared_ptr<KVClient> client;
            InitTestKVClient(0, client);
            clients_.push_back(client);
        }
    }

    void ResetClients()
    {
        clients_.clear();
    }

    std::vector<std::string> workerAddress_;
    std::vector<std::shared_ptr<KVClient>> clients_;
};

TEST_F(KVCacheLogPerformanceTest, TestMultiClients)
{
    const int numClients = 32;
    const int64_t kOperations = 100;

    InitClients(numClients);
    std::cout << "Starting " << numClients << " Clients " << kOperations << " Set/Get/Del loops..." << std::endl;

    std::vector<std::thread> threads;
    const std::string kKeyPrefix = "concurrent_op_key_";
    const std::string kValueBase = "value_";

    for (int clientIdx = 0; clientIdx < numClients; ++clientIdx) {
        threads.emplace_back([&, clientIdx]() {
            auto &client = clients_[clientIdx];
            for (int64_t op = 0; op < kOperations; ++op) {
                const std::string key = kKeyPrefix + std::to_string(clientIdx) + "_" + std::to_string(op);
                const std::string value = kValueBase + std::to_string(clientIdx) + "_" + std::to_string(op);

                Status status = client->Set(key, value);
                if (status != Status::OK()) {
                    continue;
                }

                std::string retrievedValue;
                status = client->Get(key, retrievedValue);
                if (status != Status::OK()) {
                    continue;
                }

                client->Del(key);
            }  // end for op
        });  // end lambda
    }  // end for clientIdx

    for (auto &t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

}  // namespace st
}  // namespace datasystem
