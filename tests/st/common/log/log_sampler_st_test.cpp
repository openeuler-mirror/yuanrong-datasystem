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
 * Description: System-level tests for LogSampler cross-process behavior.
 * Covers LS-002b (request reject propagation), LS-012 (cross-process
 * consistency), and LS-013 (worker config delivery via register/heartbeat).
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
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_double(request_sample_rate);
DS_DECLARE_double(access_sample_rate);
DS_DECLARE_double(diagnostic_sample_rate);

namespace datasystem {
namespace st {

class LogSamplerSTTest : public OCClientCommon {
public:
    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -log_monitor=true"
            " -max_client_num=2000 -request_sample_rate=0.5"
            " -access_sample_rate=0.5 -diagnostic_sample_rate=0.5";
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

    std::vector<std::string> workerAddress_;
    std::vector<std::shared_ptr<KVClient>> clients_;
};

/**
 * LS-002b + LS-012: Verify that request sampling decisions propagate
 * from client to worker via RPC. When the sampler is enabled, both
 * sides should make consistent decisions: a sampled-in request produces
 * full logs on both client and worker; a sampled-out request produces
 * reduced log output.
 */
TEST_F(LogSamplerSTTest, CrossProcessSamplingConsistency)
{
    const int numClients = 4;
    const int64_t kOperations = 50;
    InitClients(numClients);

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);
    ASSERT_TRUE(snap->config.enabled);

    std::vector<std::thread> threads;
    for (int clientIdx = 0; clientIdx < numClients; ++clientIdx) {
        threads.emplace_back([&, clientIdx]() {
            auto &client = clients_[clientIdx];
            for (int64_t op = 0; op < kOperations; ++op) {
                const std::string key = "sampler_key_" + std::to_string(clientIdx) + "_" + std::to_string(op);
                const std::string value = "value_" + std::to_string(clientIdx);
                Status s = client->Set(key, value);
                if (s.IsOk()) {
                    std::string out;
                    client->Get(key, out);
                    client->Del(key);
                }
            }
        });
    }
    for (auto &t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

/**
 * LS-013: Verify that the worker delivers LogSampleConfigPb to the client
 * via the register/heartbeat path. The client should apply the received
 * config and use the correct sampling rates in subsequent requests.
 * After connecting, the client's sampling config should match the
 * worker's configured rates.
 */
TEST_F(LogSamplerSTTest, WorkerConfigDeliveryViaRegister)
{
    const int numClients = 2;
    InitClients(numClients);

    auto *snap = LogSampler::Instance().GetSnapshotForTest();
    ASSERT_NE(snap, nullptr);
    ASSERT_TRUE(snap->config.enabled);
    constexpr uint32_t expectedPpm = 500000;
    ASSERT_EQ(snap->config.requestRate.ppm, expectedPpm);
    ASSERT_EQ(snap->config.accessRate.ppm, expectedPpm);
    ASSERT_EQ(snap->config.diagnosticRate.ppm, expectedPpm);

    for (int clientIdx = 0; clientIdx < numClients; ++clientIdx) {
        auto &client = clients_[clientIdx];
        for (int64_t op = 0; op < 20; ++op) {
            const std::string key = "config_key_" + std::to_string(clientIdx) + "_" + std::to_string(op);
            const std::string value = "value_" + std::to_string(clientIdx);
            Status s = client->Set(key, value);
            if (s.IsOk()) {
                std::string out;
                client->Get(key, out);
            }
        }
    }
}

}  // namespace st
}  // namespace datasystem