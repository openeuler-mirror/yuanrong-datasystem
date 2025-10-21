/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
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
 * Description: Test log basic functions.
 */
#include <cstdlib>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/kv_cache/kv_client.h"

namespace datasystem {
namespace st {

constexpr size_t DEFAULT_WORKER_NUM = 3;

class LoggingFreeTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            "-shared_memory_size_mb=5120 -v=2 -log_async=true -log_async_queue_size=4096 -logbufsecs=15";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }

        EXPECT_EQ(setenv("DATASYSTEM_LOG_ASYNC_ENABLE", "true", 1), 0);
        EXPECT_EQ(setenv("DATASYSTEM_LOG_ASYNC_QUEUE_SIZE", "2048", 1), 0);
        InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
        InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
        InitTestKVClient(2, client2_, 2000);  // Init client2 to worker 2 with 2000ms timeout
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        client2_.reset();
        EXPECT_EQ(setenv("DATASYSTEM_LOG_ASYNC_ENABLE", "", 1), 0);
        EXPECT_EQ(setenv("DATASYSTEM_LOG_ASYNC_QUEUE_SIZE", "", 1), 0);
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client0_, client1_, client2_;
    ExternalCluster *externalCluster_ = nullptr;
};

TEST_F(LoggingFreeTest, FreelogWhenUsingAsyncLog)
{
    int objectCnt = 1000;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);

    for (uint32_t i = 0; i < objectKey.size(); ++i) {
        objectKey[i] = randomData_.GetRandomString(10) + std::to_string(i);  // Generate the data of length 10
        data[i] = randomData_.GetRandomString(10);                           // Generate the data of length 10
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
        DS_ASSERT_OK(client0_->Set(objectKey[i], data[i], param));
    }

    for (uint32_t i = 0; i < objectKey.size(); ++i) {
        std::string getValue;
        DS_ASSERT_OK(client0_->Get(objectKey[i], getValue));
        DS_ASSERT_OK(client0_->Del(objectKey[i]));
    }
}

}  // namespace st
}  // namespace datasystem
