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
 * Description: brpc cross-worker KV remote-get coverage.
 */

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/kv_client.h"
#include "kv_client_common.h"

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_NUM = 2;
constexpr uint64_t VALUE_SIZE = 8UL * 1024 * 1024;
constexpr int32_t CLIENT_TIMEOUT_MS = 60 * 1000;
}  // namespace

class KVClientBrpcRemoteGetTest : public KVClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams =
            "-shared_memory_size_mb=512 -v=2 -log_monitor=true -enable_perf_trace_log=true -use_brpc=true";
    }

    void SetUp() override
    {
        restoreUseBrpc_ = std::make_unique<Raii>([oldUseBrpc = FLAGS_use_brpc]() { FLAGS_use_brpc = oldUseBrpc; });
        FLAGS_use_brpc = true;
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
        restoreUseBrpc_.reset();
    }

private:
    std::unique_ptr<Raii> restoreUseBrpc_;
};

TEST_F(KVClientBrpcRemoteGetTest, LEVEL1_Get8MBValueAcrossWorkersWithShmEnabled)
{
    std::shared_ptr<KVClient> setterClient;
    std::shared_ptr<KVClient> getterClient;
    InitTestKVClient(0, setterClient, CLIENT_TIMEOUT_MS);
    InitTestKVClient(1, getterClient, CLIENT_TIMEOUT_MS);

    const std::string key = "brpc_remote_get_8mb_" + NewObjectKey();
    const std::string value = GenRandomString(VALUE_SIZE);
    SetParam setParam{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NONE,
                       .cacheType = CacheType::MEMORY };
    DS_ASSERT_OK(setterClient->Set(key, value, setParam));

    std::string valueGet;
    DS_ASSERT_OK(getterClient->Get(key, valueGet));
    ASSERT_EQ(valueGet.size(), value.size());
    ASSERT_EQ(valueGet, value);

    DS_ASSERT_OK(setterClient->Del(key));
}

}  // namespace st
}  // namespace datasystem
