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
#include <algorithm>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <tbb/concurrent_hash_map.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <ostream>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/flags/flags.h"

DS_DECLARE_bool(log_monitor);

namespace datasystem {
namespace st {
constexpr int64_t SHM_SIZE = 500 * 1024;
class KVCacheClientShmOverTcpTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -log_monitor=true ";

        for (size_t i = 0; i < opts.numWorkers; i++) {
            auto shmWorkerPort = GetFreePort();
            opts.workerSpecifyGflagParams[i] = FormatString("-shared_memory_worker_port=%d", shmWorkerPort);
        }
    }

    void SetUp() override
    {
        if (!SupportScmTcp()) {
            skip_ = true;
            GTEST_SKIP() << "SMC over TCP not supported";
        }
        ExternalClusterTest::SetUp();
        FLAGS_log_monitor = true;
    }

    void TearDown() override
    {
        if (!skip_) {
            ExternalClusterTest::TearDown();
        }
    }

    bool SupportScmTcp()
    {
        const int proto = 518;  // IPPROTO_SCMTCP
        auto fd = socket(AF_INET, SOCK_STREAM, proto);
        bool ret = fd >= 0;
        if (ret) {
            close(fd);
        }
        return ret;
    }

private:
    bool skip_ = false;
};

TEST_F(KVCacheClientShmOverTcpTest, TestSingleKey)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key = "key1";
    std::string value(SHM_SIZE, 'a');
    ASSERT_EQ(client->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    ASSERT_EQ(client->Del(key), Status::OK());
}

class KVCacheClientShmOverTcpDisableUDSTest : public KVCacheClientShmOverTcpTest {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }

        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -log_monitor=true -inject_actions=worker.bind_unix_path:return(K_OK) ";

        for (size_t i = 0; i < opts.numWorkers; i++) {
            auto shmWorkerPort = GetFreePort();
            opts.workerSpecifyGflagParams[i] = FormatString("-shared_memory_worker_port=%d", shmWorkerPort);
        }
    }
};

TEST_F(KVCacheClientShmOverTcpDisableUDSTest, TestSingleKey)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key = "key1";
    std::string value(SHM_SIZE, 'a');
    ASSERT_EQ(client->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    ASSERT_EQ(client->Del(key), Status::OK());
}

}  // namespace st
}  // namespace datasystem
