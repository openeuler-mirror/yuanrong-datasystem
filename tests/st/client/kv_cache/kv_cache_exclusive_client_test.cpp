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
 * Description: Test kv client exclusive function
 */

#include "client/object_cache/oc_client_common.h"

namespace datasystem {
namespace st {
class KVCacheExclusiveClientTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-node_timeout_s=5 -shared_memory_size_mb=2048 -v=2";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(0, client1_, timeoutMs_, false, true);
    }

    void TearDown() override
    {
        client1_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<KVClient> client1_;
    const int timeoutMs_ = 2'000;
};

TEST_F(KVCacheExclusiveClientTest, GetTimeout)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Get.delay", "sleep(3000)"));
    DS_ASSERT_OK(client1_->Set("key1", "value1"));
    std::string val;
    DS_ASSERT_NOT_OK(client1_->Get("key1", val));
}

TEST_F(KVCacheExclusiveClientTest, ExclusiveIdOutOfRange)
{
    std::thread t1([&]() {
        int numClients = 130;
        std::shared_ptr<KVClient> client;
        for (int i = 0; i < numClients; ++i) {
            InitTestKVClient(0, client, timeoutMs_, false, true);
            std::string objKey = "objKey_" + std::to_string(i);
            std::string val = "value";
            DS_ASSERT_OK(client->Set(objKey, val));
            DS_ASSERT_OK(client->ShutDown());
        }
    });
    t1.join();
}

TEST_F(KVCacheExclusiveClientTest, TestShutdownAndRestartWorker)
{
    std::string objKey = "objKey_dddd";
    std::string val = "dddd";
    DS_ASSERT_OK(client1_->Set(objKey, val));

    // Shutdown worker
    DS_ASSERT_OK(cluster_->ShutdownNode(ClusterNodeType::WORKER, 0));

    // Get value after worker has shutdown and the call is expected to fail.
    std::string getVal;
    DS_ASSERT_NOT_OK(client1_->Get(objKey, getVal));

    // Restart worker
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));

    // Do operation succesfully after worker is reconnected.
    DS_ASSERT_OK(client1_->Set(objKey, val));
    DS_ASSERT_OK(client1_->Get(objKey, getVal));
    ASSERT_EQ(val, getVal);
}
}
}