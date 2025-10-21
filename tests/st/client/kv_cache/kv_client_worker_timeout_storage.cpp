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
 * Description: Test the behavior after worker timeout.
 */

#include "datasystem/kv_cache/kv_client.h"
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/worker/hash_ring/hash_ring.h"

DS_DECLARE_string(etcd_address);

namespace datasystem {
namespace st {
namespace {
const std::string HOST_IP = "127.0.0.1";
constexpr int WORKER_NUM = 3;
constexpr int CLUSTER_READY_TIMEOUT = 30;
}  // namespace

class KVClientWorkerTimeoutStorage : public OCClientCommon  {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-shared_memory_size_mb=5120 -node_timeout_s=1 -heartbeat_interval_ms=500 -v=2 ";
        opts.waitWorkerReady = false;
    }
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartOBS());
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (int i = 0; i < WORKER_NUM; ++i) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        DS_ASSERT_OK(cluster_->WaitUntilClusterReadyOrTimeout(CLUSTER_READY_TIMEOUT));
        InitTestKVClient(0, client0_); // Init client0 to worker 0
        InitTestKVClient(1, client1_); // Init client0 to worker 0
    }
    void InitTestEtcdInstance()
    {
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        FLAGS_etcd_address = etcdAddress;
        LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
        db_ = std::make_unique<EtcdStore>(etcdAddress);
        if ((db_ != nullptr) && (db_->Init().IsOk())) {
            db_->DropTable(ETCD_RING_PREFIX);
            // We don't check rc here. If table to drop does not exist, it's fine.
            (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
        }
    }
    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        ExternalClusterTest::TearDown();
    }
protected:
    std::unique_ptr<EtcdStore> db_;
    std::shared_ptr<KVClient> client0_, client1_;
};

TEST_F(KVClientWorkerTimeoutStorage, LEVEL1_WorkerTimeoutAndMetaGetFromEtcd)
{
    // 1. Let the meta of data1 and data2 choice worker1 and worker2 respectively.
    std::string objectKey = NewObjectKey();
    std::string data = randomData_.GetRandomString(10);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client0_->Set(objectKey, data, param));

    // 2. Primary worker timeout, get data from l2cache.
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    sleep(5);  // sleep 5s
    std::string getValue;
    DS_ASSERT_OK(client1_->Get(objectKey, getValue));
    ASSERT_EQ(data, getValue);
}

}  // namespace st
}  // namespace datasystem