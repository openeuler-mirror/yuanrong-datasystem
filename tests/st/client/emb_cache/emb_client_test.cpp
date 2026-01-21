/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the Lse at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIONDITIONS OF ANY KIND, either express or implied.
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
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/emb_client.h"
#include "datasystem/utils/status.h"
#include "cluster/external_cluster.h"

// DS_DECLARE_string(etcd_address);
namespace datasystem {
namespace st {
class EmbClientLoadTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        // opts.numRedis = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-shared_memory_size_mb=3000 -v=2 -oc_shm_transfer_threshold_kb=0";
    }

    void GenerateKeyValues(std::vector<uint64_t> &keys, std::vector<std::string> &vals, int num, int valSize)
    {
        // uint32_t minValue = 0;
        // uint32_t maxValue = 100;
        for (int i = 0; i < num; i++) {
            keys.emplace_back(static_cast<uint64_t>(i));
            std::string combined_val = std::to_string(i) + "_" + randomData_.GetRandomString(valSize);
            vals.emplace_back(combined_val.substr(0, valSize));
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        // DS_ASSERT_OK(cluster_->StartRedisCluster());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < 1; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        //   Init client0 to worker 0 with 20000ms timeout
        // InitTestEmbClient(1, client1_, 20000);  // Init client1 to worker 1 with 20000ms timeout
        // InitTestKVClient(2, client_, 20000);  // Init client_ to worker 4 with 20000ms timeout
    }

    void TearDown() override
    {
        // client0_.reset();
        // client1_.reset();
        // client_.reset();
        ExternalClusterTest::TearDown();
    }
protected:
    // std::shared_ptr<EMBClient> client0_, client1_;
    // std::shared_ptr<KVClient> client_;
    RandomData randomData_;  // 定义 RandomData 成员变量
};

TEST_F(EmbClientLoadTest, TestInsert)
{
    InitParams params;
    params.tableKey = "1_table";
    params.tableIndex = 1;
    params.tableName = "table";
    params.dimSize = 16;
    params.tableCapacity = 30;
    params.bucketNum = 10;
    params.bucketCapacity = 10;

    // ConnectOptions connectOptions;
    // connectOptions = { .host = "127.0.0.1", .port = 10086 };
    // std::shared_ptr<EmbClient> client;
    // client = std::make_shared<EmbClient>(connectOptions);

    std::shared_ptr<EmbClient> client;
    std::string etcdAddress;
    std::pair<HostPort, HostPort> addrs;
    cluster_->GetEtcdAddrs(0, addrs);
    etcdAddress = addrs.first.ToString();
    LOG(INFO) << "Etcd address is: " << etcdAddress;
    InitTestEmbClient(0, client, params, etcdAddress, "");
    std::vector<uint64_t> keys;
    std::vector<std::string> vals;
    std::vector<StringView> values;
    GenerateKeyValues(keys, vals, params.tableCapacity, params.dimSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::random_shuffle(keys.begin(), keys.end());

    DS_ASSERT_OK(client->Insert(keys, values));
    std::vector<StringView> buffer;
    DS_ASSERT_OK(client->BuildIndex());
    DS_ASSERT_OK(client->Find(keys, buffer));
    ASSERT_EQ(buffer.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        std::string b(buffer[i].data(), buffer[i].size());
        ASSERT_EQ(b, vals[i]) << "Mismatch at key index " << i;
    }
    // if (!buffer.empty()) {
    //     std::string content(buffer.begin(), buffer.end());
    //     LOG(INFO) << "Buffer Content: " << content;
    // } else {
    //     LOG(WARNING) << "Buffer is empty!";
    // }
}

TEST_F(EmbClientLoadTest, TestFindFromAnother)
{
    InitParams params;
    params.tableKey = "1_table";
    params.tableIndex = 1;
    params.tableName = "table";
    params.dimSize = 16;
    params.tableCapacity = 300;
    params.bucketNum = 10;
    params.bucketCapacity = 10;

    // ConnectOptions connectOptions;
    // connectOptions = { .host = "127.0.0.1", .port = 10086 };
    // std::shared_ptr<EmbClient> client;
    // client = std::make_shared<EmbClient>(connectOptions);

    std::shared_ptr<EmbClient> client1, client2;
    std::string etcdAddress1, etcdAddress2;
    std::pair<HostPort, HostPort> addrs1, addrs2;
    cluster_->GetEtcdAddrs(0, addrs1);
    cluster_->GetEtcdAddrs(0, addrs2);
    etcdAddress1 = addrs1.first.ToString();
    etcdAddress2 = addrs2.first.ToString();
    InitTestEmbClient(0, client1, params, etcdAddress1, "");
    InitTestEmbClient(1, client2, params, etcdAddress2, "");
    std::vector<uint64_t> keys;
    std::vector<std::string> vals;
    std::vector<StringView> values;
    GenerateKeyValues(keys, vals, params.tableCapacity, params.dimSize);
    for (const auto &val : vals) {
        values.emplace_back(val);
    }
    std::random_shuffle(keys.begin(), keys.end());

    DS_ASSERT_OK(client1->Insert(keys, values));
    std::vector<StringView> buffer;
    DS_ASSERT_OK(client1->BuildIndex());
    
    DS_ASSERT_OK(client2->Find(keys, buffer));
    ASSERT_EQ(buffer.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        std::string b(buffer[i].data(), buffer[i].size());
        ASSERT_EQ(b, vals[i]) << "Mismatch at key index " << i;
    }
    // if (!buffer.empty()) {
    //     std::string content(buffer.begin(), buffer.end());
    //     LOG(INFO) << "Buffer Content: " << content;
    // } else {
    //     LOG(WARNING) << "Buffer is empty!";
    // }
}

}  // namespace st
}  // namespace datasystem