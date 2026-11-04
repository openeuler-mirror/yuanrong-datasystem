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
 * Description: state cache cross AZ test
 */

#include "datasystem/common/flags/flags.h"
#include <gtest/gtest.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>

#include "common.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

class KVClientLogMonitorTest : public OCClientCommon {
public:
    void SetUp() override
    {
        DS_ASSERT_OK(inject::Set("ObjectClientImpl.ClientWorkerWarmup.skip", "call()"));
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        (void)inject::Clear("ObjectClientImpl.ClientWorkerWarmup.skip");
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numOBS = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerGflagParams =
            FormatString("-shared_memory_size_mb=25 -v=1 -log_monitor=true -log_monitor_interval_ms=%d",
                         logRefreshIntervalS_ * s2ms_);
    }

    void GetObjectSizeFromResourceLog(int index, uint64_t &objectSize)
    {
        std::string fullName = FormatString("%s/../worker%d/log/kv_resource.log", FLAGS_log_dir.c_str(), index);
        std::ifstream ifs(fullName);
        ASSERT_TRUE(ifs.is_open());
        std::string line;
        nlohmann::json snapshot;
        while (std::getline(ifs, line)) {
            auto record = nlohmann::json::parse(line, nullptr, false);
            if (record.is_object()) {
                snapshot = std::move(record);
            }
        }
        ASSERT_TRUE(snapshot.contains("metrics"));
        const auto &metrics = snapshot.at("metrics");
        ASSERT_TRUE(metrics.contains("object_size"));
        ASSERT_TRUE(metrics.at("object_size").is_number_unsigned());
        objectSize = metrics.at("object_size").get<uint64_t>();
    }

protected:
    const int logRefreshIntervalS_ = 1;
    const int s2ms_ = 1'000;
};

TEST_F(KVClientLogMonitorTest, TestObjectSize)
{
    std::string key = "key";
    int val1Size = 4;
    int val2Size = 5;
    int metadataSize = 64;
    int shmQueueSize = 73744;
    uint64_t objectSize = 0;
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    DS_ASSERT_OK(client->Set(key, std::string(val1Size, 'o')));
    sleep(logRefreshIntervalS_ + 1);
    GetObjectSizeFromResourceLog(0, objectSize);
    ASSERT_EQ(objectSize, val1Size + metadataSize + shmQueueSize);

    DS_ASSERT_OK(client->Set(key, std::string(val2Size, 'o')));
    sleep(logRefreshIntervalS_ + 1);
    GetObjectSizeFromResourceLog(0, objectSize);
    ASSERT_EQ(objectSize, val2Size + metadataSize + shmQueueSize);

    DS_ASSERT_OK(client->Del(key));
    sleep(logRefreshIntervalS_ + 1);
    GetObjectSizeFromResourceLog(0, objectSize);
    ASSERT_EQ(objectSize, shmQueueSize);
}

TEST_F(KVClientLogMonitorTest, TestSetDelValResouceLog)
{
    auto dateSize = 20;
    auto logRefreshIntervalS = 1;
    auto key = randomData_.GetRandomString(dateSize);
    auto val = randomData_.GetRandomString(dateSize);
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    DS_ASSERT_OK(client->Set(key, val, param));
    sleep(logRefreshIntervalS + 1);
    uint64_t objectSize = 0;
    GetObjectSizeFromResourceLog(0, objectSize);
    int metadataSize = 64;
    int shmQueueSize = 73744;
    ASSERT_EQ(objectSize, val.size() + metadataSize + shmQueueSize);
    DS_ASSERT_OK(client->Del(key));
    sleep(logRefreshIntervalS + 1);
    GetObjectSizeFromResourceLog(0, objectSize);
    ASSERT_EQ(objectSize, shmQueueSize);
}
}
}
