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
#include <memory>
#include <string>
#include <thread>

#include "common.h"
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
#include "datasystem/object_cache/object_enum.h"
#include "datasystem/kv_cache/kv_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

class KVClientLogMonitorTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.enableDistributedMaster = "true";
        opts.numOBS = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerGflagParams =
            FormatString("-shared_memory_size_mb=25 -v=1 -log_monitor=true -log_monitor_interval_ms=%d",
                         logRefreshIntervalS_ * s2ms_);
    }

    void GetResMonitorLogInfo(int index, const std::string &fileName, std::vector<std::string> &infos)
    {
        std::string fullName = FormatString("%s/../worker%d/log/%s", FLAGS_log_dir.c_str(), index, fileName);
        std::ifstream ifs(fullName);
        ASSERT_TRUE(ifs.is_open());
        std::string line;
        std::streampos prev = ifs.tellg();
        std::streampos pos = ifs.tellg();
        // Get the last line
        while (std::getline(ifs, line)) {
            prev = pos;
            pos = ifs.tellg();
        }
        ifs.clear();
        ifs.seekg(prev);
        std::getline(ifs, line);
        infos = Split(line, " | ");
        const int ignoreCount = 7;
        ASSERT_EQ(infos.size(), static_cast<size_t>(ResMetricName::RES_METRICS_END) + ignoreCount);
    };

protected:
    const int logRefreshIntervalS_ = 1;
    const int s2ms_ = 1'000;
    const int objectSizeIndex_ = 11;
};

TEST_F(KVClientLogMonitorTest, TestObjectSize)
{
    std::string key = "key";
    int val1Size = 4;
    int val2Size = 5;
    int metadataSize = 64;
    int shmQueueSize = 73744;
    std::vector<std::string> infos;
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    DS_ASSERT_OK(client->Set(key, std::string(val1Size, 'o')));
    sleep(logRefreshIntervalS_ + 1);
    GetResMonitorLogInfo(0, "resource.log", infos);
    ASSERT_EQ(std::stoi(infos[objectSizeIndex_]), val1Size + metadataSize + shmQueueSize);

    DS_ASSERT_OK(client->Set(key, std::string(val2Size, 'o')));
    sleep(logRefreshIntervalS_ + 1);
    GetResMonitorLogInfo(0, "resource.log", infos);
    ASSERT_EQ(std::stoi(infos[objectSizeIndex_]), val2Size + metadataSize + shmQueueSize);

    DS_ASSERT_OK(client->Del(key));
    sleep(logRefreshIntervalS_ + 1);
    GetResMonitorLogInfo(0, "resource.log", infos);
    ASSERT_EQ(std::stoi(infos[objectSizeIndex_]), shmQueueSize);
}

TEST_F(KVClientLogMonitorTest, TestSetDelValResouceLog)
{
    auto dateSize = 20;
    auto logRefreshIntervalS = 1;
    auto objectSizeIndex = 11;
    auto key = randomData_.GetRandomString(dateSize);
    auto val = randomData_.GetRandomString(dateSize);
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    DS_ASSERT_OK(client->Set(key, val, param));
    sleep(logRefreshIntervalS + 1);
    std::vector<std::string> infos;
    GetResMonitorLogInfo(0, "resource.log", infos);
    int metadataSize = 64;
    int shmQueueSize = 73744;
    ASSERT_EQ(std::stoi(infos[objectSizeIndex]), val.size() + metadataSize + shmQueueSize);
    DS_ASSERT_OK(client->Del(key));
    sleep(logRefreshIntervalS + 1);
    std::vector<std::string> infos1;
    GetResMonitorLogInfo(0, "resource.log", infos1);
    ASSERT_EQ(std::stoi(infos1[objectSizeIndex]), shmQueueSize);
}
}
}