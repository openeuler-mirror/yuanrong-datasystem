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
 * Description: Reproduce concurrent set/get on the same key among workers when worker-worker batch get is enabled.
 */

#include <cstdint>
#include <algorithm>
#include <future>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/kv_client.h"

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_NUM = 4;
constexpr int THREAD_NUM_PER_WORKER = 20;
constexpr int THREAD_POOL_SIZE = 100;
constexpr uint64_t VALUE_SIZE = 600 * 1024;
}  // namespace

class KVClientWorkerWorkerBatchGetTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = WORKER_NUM;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams =
            "-shared_memory_size_mb=100 -v=2 -log_monitor=true -max_client_num=4000 "
            "-enable_worker_worker_batch_get=true";
    }

    Status InitKVClientForWorker(uint32_t workerIndex, std::shared_ptr<KVClient> &client, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        ConnectOptions connectOptions{ .host = workerAddress.Host(), .port = workerAddress.Port(),
                                       .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        client = std::make_shared<KVClient>(connectOptions);
        return client->Init();
    }
};

TEST_F(KVClientWorkerWorkerBatchGetTest, LEVEL1_SameKeyMSetNxConcurrentlyAcrossWorkers)
{
    std::shared_ptr<KVClient> cleanupClient;
    DS_ASSERT_OK(InitKVClientForWorker(0, cleanupClient));

    const std::string key = "batch_get_same_key_mset_nx_" + NewObjectKey();
    const std::string presetValue = GenRandomString(64 * 1024);
    SetParam setParam{ .writeMode = WriteMode::NONE_L2_CACHE };
    DS_ASSERT_OK(cleanupClient->Set(key, presetValue, setParam));

    std::vector<std::future<Status>> results;
    results.reserve(WORKER_NUM * THREAD_NUM_PER_WORKER);
    {
        ThreadPool pool(THREAD_POOL_SIZE);
        for (uint32_t workerIdx = 0; workerIdx < WORKER_NUM; ++workerIdx) {
            for (int threadIdx = 0; threadIdx < THREAD_NUM_PER_WORKER; ++threadIdx) {
                results.emplace_back(pool.Submit([this, workerIdx, threadIdx, key, presetValue]() -> Status {
                    std::shared_ptr<KVClient> client;
                    RETURN_IF_NOT_OK(InitKVClientForWorker(workerIdx, client));

                    std::vector<std::string> keys{ key };
                    std::vector<std::string> rawVals{ presetValue };
                    std::vector<StringView> values;
                    values.reserve(1);
                    values.emplace_back(rawVals[0]);
                    std::vector<std::string> failedKeys;
                    MSetParam msetParam{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0,
                                         .existence = ExistenceOpt::NX, .cacheType = CacheType::MEMORY };
                    Status msetStatus = client->MSet(keys, values, failedKeys, msetParam);
                    if (!msetStatus.IsOk()) {
                        LOG(ERROR) << "MSet NX failed. workerIdx=" << workerIdx << ", threadIdx=" << threadIdx
                                   << ", key=" << key << ", status=" << msetStatus.ToString();
                        return msetStatus;
                    }
                    if (!failedKeys.empty()) {
                        LOG(ERROR) << "MSet NX returned failedKeys unexpectedly. workerIdx=" << workerIdx
                                   << ", threadIdx=" << threadIdx << ", key=" << key
                                   << ", failedKeysSize=" << failedKeys.size();
                        return Status(K_RUNTIME_ERROR, "MSet NX should not return failedKeys when key exists.");
                    }

                    std::string valueGet;
                    RETURN_IF_NOT_OK(client->Get(key, valueGet));
                    if (valueGet != presetValue) {
                        LOG(ERROR) << "Value changed unexpectedly in NX mode. workerIdx=" << workerIdx
                                   << ", threadIdx=" << threadIdx << ", key=" << key;
                        return Status(K_RUNTIME_ERROR, "Value should remain unchanged in NX mode.");
                    }
                    return Status::OK();
                }));
            }
        }
        for (auto &result : results) {
            Status rc = result.get();
            ASSERT_TRUE(rc.IsOk()) << rc.ToString();
        }
    }

    std::string finalValue;
    DS_ASSERT_OK(cleanupClient->Get(key, finalValue));
    ASSERT_EQ(finalValue, presetValue);
    DS_ASSERT_OK(cleanupClient->Del(key));
}

}  // namespace st
}  // namespace datasystem
