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
#include <atomic>

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
    const int timeoutMs_ = 2000;
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

TEST_F(KVCacheExclusiveClientTest, TestMaxWorkAgents)
{
    const int numUsers = 4;
    const int numKeys = numUsers + 1;
    const int spinLockSleep = 500;
    std::atomic<int> threadsIn = 0;
    std::vector<std::thread> userThreads;
    std::vector<std::string> objKeys;
    std::string val("a");
    bool testSucceed = true;
    
    LOG(INFO) << "create the data locally before putting";
    for (int i = 0; i < numKeys; ++i) {
        objKeys.push_back("key_" + std::to_string(i));
    }

    // Fake the maximum number of agents to be the number of user threads we'll create.
    // Later, going 1 more thread will exceed the faked limit
    std::string injectCall("call(");
    injectCall += std::to_string(numUsers) + ")";
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "ZmqService.ProcessAccept.FakeFullPool",
                                           injectCall));

    // spin up user threads. Each one runs a single rpc call and then sleeps.
    int i = 0;
    for (; i < numUsers; ++i) {
        userThreads.emplace_back([this, &objKeys, &val, i, &threadsIn, &spinLockSleep]() {
            LOG(INFO) << "User thread doing put of key " << objKeys[i];
            DS_ASSERT_OK(client1_->Set(objKeys[i], val));
            LOG(INFO) << "User thread put complete. Wait for exit.";
            // Hang the thread in a spin lock until parent releases it so that the connection remains active
            // Parent resets the count to 0 which will unblock us.
            ++threadsIn;
            while (threadsIn != 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
            }
            LOG(INFO) << "User thread quitting now.";
        });
    }

    // Parent wait until all threads have done their work
    while (threadsIn != numUsers) {
        std::this_thread::sleep_for(std::chrono::milliseconds(spinLockSleep));
    }

    // now, using our current thread (its still a new thread for exclusive connection), try to set.
    // Internally, we have injected a fake max agents that will cause this one to exceed the count.
    Status rc = client1_->Set(objKeys[i], val);
    if (rc.GetCode() == K_NO_SPACE) {
        LOG(INFO) << "RPC call correctly failed due to maximum number of work agents: " << rc.ToString();
    } else {
        testSucceed = false;
        LOG(INFO) << "Test failed. Incorrect rc returned: " << rc.ToString();
    }

    threadsIn = 0;  // unblocks the spinning threads to allow them to quit
    for (auto &t : userThreads) {
        t.join();
    }

    ASSERT_TRUE(testSucceed);
}
}
}
