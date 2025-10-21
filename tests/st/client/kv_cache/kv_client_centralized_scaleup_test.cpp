/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Centralized master scenario, worker scale up test.
 */

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

#include "common.h"
#include "client/object_cache/oc_client_common.h"

namespace datasystem {
namespace st {

const std::string HOST_IP_PREFIX = "127.0.0.1";
constexpr size_t DEFAULT_WORKER_NUM = 4;
class KVClientCentralizedScaleupTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams =
            "-shared_memory_size_mb=5120 -v=2 -node_timeout_s=1 -node_dead_timeout_s=2"
            " -auto_del_dead_node=true -heartbeat_interval_ms=500";

        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            std::string hostIp = HOST_IP_PREFIX + std::to_string(i);
            int port = GetFreePort();
            opts.workerConfigs.emplace_back(hostIp, port);
            workerHost_.emplace_back(hostIp, port);
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void WaitWorkerReady(std::vector<uint32_t> indexes, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
    }

    void StartWorkerAndWaitReady(std::vector<uint32_t> indexes, std::string gFlag = "")
    {
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorker(i, workerHost_[0], gFlag).IsOk());
        }
        WaitWorkerReady(indexes);
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void InitCluster(bool withConcurrently = false)
    {
        if (withConcurrently) {
            std::string flag = " -inject_actions=EtcdClusterManager.DelayMessageDeque.test:1*call(5)";
            std::string oldFlag = externalCluster_->AddGFlagForWorkerStart(flag);
            DS_ASSERT_OK(externalCluster_->StartForkWorkerProcess());
            externalCluster_->SetGFlagForWorkerStart(oldFlag);

            DS_ASSERT_OK(externalCluster_->StartWorkerByForkProcess(0));
            DS_ASSERT_OK(externalCluster_->StartWorkerByForkProcess(1));
            WaitWorkerReady({ 0, 1 });
        } else {
            StartWorkerAndWaitReady(std::vector<uint32_t>{ 0, 1 });
        }

        InitTestKVClient(0, client0_);
        InitTestKVClient(1, client1_);
        std::string getValue;
        DS_ASSERT_OK(client0_->Set("key1", "value1"));
        DS_ASSERT_OK(client1_->Get("key1", getValue));
        ASSERT_EQ(getValue, "value1");
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

protected:
    std::vector<std::string> workerAddress_;
    std::vector<HostPort> workerHost_;
    std::shared_ptr<KVClient> client0_, client1_, client2_, client3_;
    ExternalCluster *externalCluster_ = nullptr;
};

TEST_F(KVClientCentralizedScaleupTest, LEVEL1_ScaleUpWorkerSequentially)
{
    InitCluster();

    StartWorkerAndWaitReady(std::vector<uint32_t>{ 2 },
                            " -inject_actions=EtcdClusterManager.DelayMessageDeque.test:1*call(5)");
    InitTestKVClient(2, client2_);  // Connect client to worker 2
    std::string getValue;
    DS_ASSERT_OK(client1_->Set("key2", "value2"));
    DS_ASSERT_OK(client2_->Get("key2", getValue));
    ASSERT_EQ(getValue, "value2");
    DS_ASSERT_OK(client0_->Del("key2"));
    DS_ASSERT_NOT_OK(client1_->Get("key2", getValue));

    StartWorkerAndWaitReady(std::vector<uint32_t>{ 3 },
                            " -inject_actions=EtcdClusterManager.DelayMessageDeque.test:1*call(5)");
    InitTestKVClient(3, client3_);  // Connect client to worker 3
    DS_ASSERT_OK(client3_->Set("key3", "value3"));
    DS_ASSERT_OK(client0_->Get("key3", getValue));
    ASSERT_EQ(getValue, "value3");
    DS_ASSERT_OK(client1_->Del("key3"));
    DS_ASSERT_NOT_OK(client2_->Get("key3", getValue));
}

TEST_F(KVClientCentralizedScaleupTest, ScaleUpWorkerConcurrently)
{
    InitCluster(true);

    std::thread t1([&]() {
        const int index = 2;
        DS_ASSERT_OK(externalCluster_->StartWorkerByForkProcess(index));
        WaitWorkerReady({ index });
        InitTestKVClient(2, client2_);  // Connect client to worker 2
        std::string getValue;
        Status status = client2_->Get("key3", getValue);
        ASSERT_TRUE(status.GetCode() == K_OK || status.GetCode() == K_NOT_FOUND);
        DS_ASSERT_OK(client1_->Set("key2", "value2"));
        DS_ASSERT_OK(client2_->Get("key2", getValue));
        ASSERT_EQ(getValue, "value2");
        DS_ASSERT_OK(client0_->Del("key2"));
        DS_ASSERT_NOT_OK(client1_->Get("key2", getValue));
        status = client2_->Get("key3", getValue);
        ASSERT_TRUE(status.GetCode() == K_OK || status.GetCode() == K_NOT_FOUND);
        if (status.GetCode() == K_OK) {
            ASSERT_EQ(getValue, "value3");
        }
        return;
    });

    std::thread t2([&]() {
        const int index = 3;
        DS_ASSERT_OK(externalCluster_->StartWorkerByForkProcess(index));
        WaitWorkerReady({ index });
        InitTestKVClient(3, client3_);  // Connect client to worker 3
        std::string getValue;
        Status status = client3_->Get("key2", getValue);
        ASSERT_TRUE(status.GetCode() == K_OK || status.GetCode() == K_NOT_FOUND);
        DS_ASSERT_OK(client3_->Set("key3", "value3"));
        DS_ASSERT_OK(client0_->Get("key3", getValue));
        ASSERT_EQ(getValue, "value3");
        DS_ASSERT_OK(client1_->Del("key3"));
        DS_ASSERT_NOT_OK(client3_->Get("key3", getValue));
        status = client3_->Get("key2", getValue);
        ASSERT_TRUE(status.GetCode() == K_OK || status.GetCode() == K_NOT_FOUND);
        if (status.GetCode() == K_OK) {
            ASSERT_EQ(getValue, "value2");
        }
        return;
    });

    t1.join();
    t2.join();

    sleep(3);  // Wait 3 seconds for async proc finish.
}

}  // namespace st
}  // namespace datasystem